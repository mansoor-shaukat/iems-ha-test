"""IoT Core MQTT adapter — STUB pending Cloud-team spec.

The final implementation depends on the server spec decision (Option X
developer-authenticated Cognito identities vs Option Y HTTP exchange
endpoint — see `docs/priya_spec_ask.md` in the monorepo).

Whichever wins, this class exposes the same interface:

    adapter = IotCorePublisher(auth_provider=auth)
    await adapter.connect()
    ok = await adapter.publish(topic=..., payload=..., qos=...)
    await adapter.disconnect()

and uses credentials ONLY through `auth_provider.get_credentials()` —
the adapter never sees cert files, never hardcodes endpoints, never
stores the raw API key.

Two responsibilities land here when wiring completes:
  1. Build a SigV4-signed MQTT-over-WSS URL using temp STS creds
     (credentials provider from awscrt or from manual SigV4 signing),
     connect to `creds.iot_endpoint` on port 443.
  2. Schedule a background task that refreshes credentials
     CREDENTIAL_REFRESH_LEAD_SECONDS before `creds.expires_at`.

Until Priya's spec lands, the stub raises NotImplementedError. Tests
mock this class; the production integration will not load successfully
until the stub is filled in — that's intentional, it prevents a
half-finished release shipping to HACS.
"""
from __future__ import annotations

import json
import logging

from .auth import IemsAuthProvider

log = logging.getLogger("iems.iot_core")


class IotCorePublisher:
    """Adapter for AWS IoT Core MQTT publish with auth-provider-driven creds.

    DO NOT subclass or instantiate this outside the main entry point
    until the Cloud spec is locked. Unit tests should mock this class
    entirely via `unittest.mock.patch`.
    """

    def __init__(self, *, auth_provider: IemsAuthProvider) -> None:
        self._auth = auth_provider
        self._connected = False
        # Cached creds + the asyncio.Task doing periodic refresh will
        # live here once the spec lands.

    async def connect(self) -> None:
        """Establish the MQTT connection. Uses creds from auth_provider."""
        raise NotImplementedError(
            "IotCorePublisher.connect is pending the Cloud team's "
            "credential-exchange spec. See docs/priya_spec_ask.md in "
            "the monorepo."
        )

    async def disconnect(self) -> None:
        """Stop the MQTT loop and close the connection."""
        self._connected = False

    async def publish(self, *, topic: str, payload: dict, qos: int) -> bool:
        """Publish a JSON payload. Returns True on broker ACK."""
        raise NotImplementedError(
            "IotCorePublisher.publish is pending the Cloud team's "
            "credential-exchange spec."
        )

    def __repr__(self) -> str:
        # Never expose auth internals
        return f"IotCorePublisher(connected={self._connected})"
