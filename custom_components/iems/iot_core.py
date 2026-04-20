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

        Returns True on broker ACK. On ExpiredTokenException tears down
        the connection and re-authenticates before raising so the caller
        can retry.

        Args:
            topic: Full MQTT topic string (must start with iems/{user_id}/).
            payload: Dict that will be JSON-serialised.
            qos:    0 (fire-and-forget) or 1 (at-least-once with ACK).

        Raises:
            RuntimeError: if not connected.
            asyncio.TimeoutError: if broker ACK times out.
        """
        if not self._connected or self._connection is None:
            raise RuntimeError(
                "IotCorePublisher.publish called before connect(). "
                "Call await adapter.connect() first."
            )

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
        """Build MqttClientConnection from fresh credentials and connect."""
        try:
            from awsiot import mqtt
            from awscrt import auth as crt_auth, io as crt_io, mqtt as crt_mqtt
        except ImportError as exc:
            raise RuntimeError(
                "awsiotsdk / awscrt not installed. "
                "Add 'awsiotsdk' to manifest.json requirements."
            ) from exc

        creds = await self._auth.get_credentials()

        # awscrt credentials provider wrapping our temp STS creds
        credentials = crt_auth.AwsCredentials(
            access_key_id=creds.access_key_id,
            secret_access_key=creds.secret_access_key,
            session_token=creds.session_token,
        )
        credentials_provider = crt_auth.AwsCredentialsProvider.new_static(
            access_key_id=creds.access_key_id,
            secret_access_key=creds.secret_access_key,
            session_token=creds.session_token,
        )

        event_loop_group = crt_io.EventLoopGroup(1)
        host_resolver = crt_io.DefaultHostResolver(event_loop_group)
        client_bootstrap = crt_io.ClientBootstrap(event_loop_group, host_resolver)

        # SigV4 WebSocket signing function
        def _signing_config():
            return crt_auth.AwsSigningConfig(
                algorithm=crt_auth.AwsSigningAlgorithm.V4_ASYMMETRIC
                if hasattr(crt_auth.AwsSigningAlgorithm, "V4_ASYMMETRIC")
                else crt_auth.AwsSigningAlgorithm.V4,
                signature_type=crt_auth.AwsSignatureType.HTTP_REQUEST_QUERY_PARAMS,
                credentials_provider=credentials_provider,
                region=creds.region,
                service="iotdevicegateway",
            )

        def _websocket_handshake_transform(transform_args, **kwargs):
            crt_auth.aws_sign_request(
                http_request=transform_args.http_request,
                signing_config=_signing_config(),
                on_complete=lambda signing_result, error_code, http_headers:
                    transform_args.set_done(error_code),
            )

        mqtt_client = crt_mqtt.Client(
            bootstrap=client_bootstrap,
            tls_ctx=crt_io.ClientTlsContext(crt_io.TlsContextOptions()),
        )

        # ClientId MUST equal Cognito Identity sub — enforced by IAM policy
        client_id = creds.user_id

        connection = mqtt.MqttClientConnection(
            client=mqtt_client,
            host_name=creds.iot_endpoint,
            port=443,
            client_id=client_id,
            websocket_handshake_transform=_websocket_handshake_transform,
            keep_alive_secs=30,
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
