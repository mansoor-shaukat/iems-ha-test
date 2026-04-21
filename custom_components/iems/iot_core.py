"""IoT Core MQTT adapter — SigV4 WebSocket publish using temp STS creds.

Implements the Cognito DAI flow wired by auth.py:
  - Credentials come exclusively from auth_provider.get_credentials()
  - ClientId == Cognito Identity sub (enforced by Priya's IoT policy)
  - Topics per contracts/mqtt_topics.md:
      iems/{user_id}/telemetry  QoS 1
      iems/{user_id}/heartbeat  QoS 0
  - On ExpiredTokenException the connection is torn down and rebuilt
    with a fresh credential exchange.
  - awsiot MqttClientConnection + awscrt SigV4 WebSocket signing.

No cert files. No hardcoded endpoints. No hardcoded user IDs.
All routing data flows from auth_provider.get_credentials().
"""
from __future__ import annotations

import asyncio
import json
import logging
from typing import Any

from .auth import IemsAuthProvider
from .const import MQTT_CONNECT_TIMEOUT_SECONDS, MQTT_PUBLISH_TIMEOUT_SECONDS

log = logging.getLogger("iems.iot_core")


class IotCorePublisher:
    """Adapter for AWS IoT Core MQTT publish with auth-provider-driven creds.

    Thread-safety: all public methods are coroutines that run in the HA
    asyncio event loop. awscrt callbacks are dispatched on a thread pool
    and wrapped with asyncio.wrap_future / loop.call_soon_threadsafe so
    we never block the event loop.
    """

    def __init__(self, *, auth_provider: IemsAuthProvider) -> None:
        self._auth = auth_provider
        self._connection: Any | None = None  # awsiot.MqttClientConnection
        self._connected = False
        self._connect_lock = asyncio.Lock()
        # Captured at connect() time; reused by awscrt threadpool callbacks
        # to post state changes back to HA's asyncio loop via call_soon_threadsafe.
        self._event_loop: asyncio.AbstractEventLoop | None = None

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    async def connect(self) -> None:
        """Establish the MQTT-over-WSS connection to IoT Core.

        Uses temporary STS credentials from auth_provider to sign the
        WebSocket URL (SigV4). ClientId == Cognito Identity sub so the
        IoT policy allows the connect.
        """
        async with self._connect_lock:
            if self._connected:
                return
            await self._build_and_connect()

    async def disconnect(self) -> None:
        """Close the MQTT connection if open."""
        self._connected = False
        conn = self._connection
        self._connection = None
        if conn is not None:
            try:
                disconnect_future = conn.disconnect()
                loop = asyncio.get_event_loop()
                await asyncio.wait_for(
                    loop.run_in_executor(None, disconnect_future.result),
                    timeout=MQTT_CONNECT_TIMEOUT_SECONDS,
                )
            except Exception as exc:  # noqa: BLE001 — best-effort teardown
                log.warning("iot_core: disconnect error (ignored): %s", exc)

    async def publish(self, *, topic: str, payload: dict, qos: int) -> bool:
        """Publish a JSON payload to the given topic.

        Returns True on broker ACK. On connection-drop (network blip,
        keep-alive miss, broker-side hangup) silently rebuilds the
        connection with the same credentials before the attempt. On
        ExpiredTokenException tears the connection down and
        re-authenticates.

        Args:
            topic: Full MQTT topic string (must start with iems/{user_id}/).
            payload: Dict that will be JSON-serialised.
            qos:    0 (fire-and-forget) or 1 (at-least-once with ACK).

        Raises:
            RuntimeError: if publish fails for a non-recoverable reason.
            asyncio.TimeoutError: if broker ACK times out after reconnect.
        """
        # If the awscrt on_connection_interrupted callback flipped us to
        # disconnected, transparently re-establish before publishing.
        # This replaces the old "raise RuntimeError" path which dropped
        # every batch between a drop and the next explicit connect() call.
        if not self._connected or self._connection is None:
            log.info("iot_core: connection dropped, reconnecting before publish")
            try:
                await self.connect()
            except Exception as exc:
                # Can't get back online — surface to caller, coordinator
                # will log + drop the batch and retry on the next tick.
                raise RuntimeError(
                    f"IotCorePublisher.publish: connect failed ({type(exc).__name__}: {exc})"
                ) from exc

        payload_bytes = json.dumps(payload, separators=(",", ":")).encode()

        try:
            pub_future, _ = self._connection.publish(
                topic=topic,
                payload=payload_bytes,
                qos=self._qos_enum(qos),
            )
            loop = asyncio.get_event_loop()
            await asyncio.wait_for(
                loop.run_in_executor(None, pub_future.result),
                timeout=MQTT_PUBLISH_TIMEOUT_SECONDS,
            )
            return True
        except Exception as exc:
            exc_name = type(exc).__name__
            # awscrt raises AwsCrtError; check message for token expiry
            if "ExpiredToken" in exc_name or "ExpiredToken" in str(exc):
                log.warning("iot_core: credentials expired — reconnecting")
                await self._reconnect_with_fresh_creds()
            log.error("iot_core: publish failed topic=%s exc=%s: %s", topic, exc_name, exc)
            raise

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    async def _build_and_connect(self) -> None:
        """Build MQTT connection from fresh credentials and connect."""
        try:
            from awsiot import mqtt_connection_builder
            from awscrt import auth as crt_auth, io as crt_io
        except ImportError as exc:
            raise RuntimeError(
                "awsiotsdk / awscrt not installed. "
                "Add 'awsiotsdk' to manifest.json requirements."
            ) from exc

        creds = await self._auth.get_credentials()

        credentials_provider = crt_auth.AwsCredentialsProvider.new_static(
            access_key_id=creds.access_key_id,
            secret_access_key=creds.secret_access_key,
            session_token=creds.session_token,
        )

        event_loop_group = crt_io.EventLoopGroup(1)
        host_resolver = crt_io.DefaultHostResolver(event_loop_group)
        client_bootstrap = crt_io.ClientBootstrap(event_loop_group, host_resolver)

        # ClientId MUST equal Cognito Identity ID (not User Pool sub) — IAM
        # policy condition is `iot:ClientId == cognito-identity.amazonaws.com:sub`
        # which resolves to the Identity Pool identity_id, not user_sub.
        client_id = creds.identity_id

        # Capture the HA asyncio loop so awscrt threadpool callbacks can
        # post state changes back via call_soon_threadsafe without racing.
        self._event_loop = asyncio.get_event_loop()

        def _on_interrupted(connection, error, **_kwargs) -> None:
            """Called on awscrt thread when TCP/MQTT link drops."""
            log.warning("iot_core: connection interrupted: %s", error)
            loop = self._event_loop
            if loop is not None and loop.is_running():
                loop.call_soon_threadsafe(self._mark_disconnected)

        def _on_resumed(connection, return_code, session_present, **_kwargs) -> None:
            """Called on awscrt thread when awscrt auto-reconnects."""
            log.info(
                "iot_core: connection resumed rc=%s session_present=%s",
                return_code, session_present,
            )
            loop = self._event_loop
            if loop is not None and loop.is_running():
                loop.call_soon_threadsafe(self._mark_connected)

        connection = mqtt_connection_builder.websockets_with_default_aws_signing(
            endpoint=creds.iot_endpoint,
            region=creds.region,
            credentials_provider=credentials_provider,
            client_bootstrap=client_bootstrap,
            client_id=client_id,
            clean_session=True,
            keep_alive_secs=30,
            on_connection_interrupted=_on_interrupted,
            on_connection_resumed=_on_resumed,
        )

        loop = asyncio.get_event_loop()
        connect_future = connection.connect()
        await asyncio.wait_for(
            loop.run_in_executor(None, connect_future.result),
            timeout=MQTT_CONNECT_TIMEOUT_SECONDS,
        )

        self._connection = connection
        self._connected = True
        log.info(
            "iot_core: connected endpoint=%s client_id=%s...",
            creds.iot_endpoint,
            client_id[:8],
        )

    async def _reconnect_with_fresh_creds(self) -> None:
        """Tear down stale connection and reconnect with a fresh exchange."""
        self._connected = False
        old_conn = self._connection
        self._connection = None
        if old_conn is not None:
            try:
                loop = asyncio.get_event_loop()
                await loop.run_in_executor(None, old_conn.disconnect().result)
            except Exception:  # noqa: BLE001 — best-effort teardown
                pass
        # Force a fresh credential exchange on next connect
        await self._auth.close()
        async with self._connect_lock:
            await self._build_and_connect()

    def _mark_disconnected(self) -> None:
        """Flip internal state to disconnected. Called on HA loop only."""
        self._connected = False

    def _mark_connected(self) -> None:
        """Flip internal state to connected. Called on HA loop only."""
        self._connected = True

    @staticmethod
    def _qos_enum(qos: int):
        """Convert integer QoS to awscrt enum."""
        from awscrt import mqtt as crt_mqtt
        if qos == 0:
            return crt_mqtt.QoS.AT_MOST_ONCE
        if qos == 1:
            return crt_mqtt.QoS.AT_LEAST_ONCE
        raise ValueError(f"Unsupported QoS value: {qos}. Use 0 or 1.")

    def __repr__(self) -> str:
        # Never expose auth internals
        return f"IotCorePublisher(connected={self._connected})"
