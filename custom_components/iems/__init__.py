"""iEMS HACS integration — cloud-push telemetry agent.

async_setup_entry responsibilities:
  1. Pull the API key from the config entry (stored by config_flow).
  2. Build the auth provider (IemsCloudAuthProvider in production).
  3. Fetch temporary IoT credentials via the auth provider.
  4. Build the MQTT adapter using those credentials + the server-
     provided iot_endpoint (NOT hardcoded).
  5. Build entity_index from HA's registries.
  6. Construct IemsCoordinator, subscribe to state_changed, start
     batch + heartbeat timers.
  7. Stash adapter/coordinator/publisher in hass.data for unload.

The auth provider is the ONLY entry point to cloud endpoint routing —
there are no `IOT_ENDPOINT` / `IOT_PORT` / `DEV_USER_ID` hardcodes
anywhere else in the codebase.
"""
from __future__ import annotations

import asyncio
import logging
from typing import Any

from .auth import (
    AuthExchangeError,
    IemsAuthProvider,
    IemsCloudAuthProvider,
    InvalidApiKey,
)
from .const import (
    CONF_API_KEY,
    CONF_IOT_ENDPOINT,
    CONF_REGION,
    CONF_USER_ID,
    DOMAIN,
    VERSION,
)

log = logging.getLogger("iems")

try:
    from homeassistant.config_entries import ConfigEntry
    from homeassistant.core import HomeAssistant, callback
    from homeassistant.exceptions import ConfigEntryAuthFailed, ConfigEntryNotReady
    from homeassistant.helpers import (
        area_registry as ar,
        device_registry as dr,
        entity_registry as er,
    )
    from homeassistant.helpers.event import async_track_state_change_event
    _HA_AVAILABLE = True
except ImportError:  # pragma: no cover - dev env
    _HA_AVAILABLE = False

from .coordinator import IemsCoordinator
from .publisher import TelemetryPublisher


def _build_entity_index(hass) -> dict[str, dict[str, Any]]:
    """Snapshot HA registries into a flat dict consulted on every event."""
    er_reg = er.async_get(hass)
    dr_reg = dr.async_get(hass)
    ar_reg = ar.async_get(hass)

    index: dict[str, dict[str, Any]] = {}
    for ent in er_reg.entities.values():
        device = dr_reg.async_get(ent.device_id) if ent.device_id else None
        area_id = ent.area_id or (device.area_id if device else None)
        area = ar_reg.async_get_area(area_id) if area_id else None
        brand = device.manufacturer if device else None

        index[ent.entity_id] = {
            "platform": ent.platform,
            "domain": ent.domain,
            "device_class": ent.device_class or ent.original_device_class,
            "unit": ent.unit_of_measurement,
            "name": ent.name or ent.original_name or ent.entity_id,
            "area": area.name if area else None,
            "brand": brand,
        }
    return index


if _HA_AVAILABLE:

    async def async_setup_entry(hass: "HomeAssistant", entry: "ConfigEntry") -> bool:
        """Bootstrap the integration from a validated config entry.

        On auth failure we raise ConfigEntryAuthFailed so HA surfaces a
        "repair" button to the user that takes them back through config
        flow. On transient network errors we raise ConfigEntryNotReady
        so HA retries with backoff.
        """
        api_key = entry.data.get(CONF_API_KEY)
        if not api_key:
            # Should be impossible — config_flow enforces it.
            raise ConfigEntryAuthFailed("iEMS: no API key stored in config entry")

        log.info("iEMS %s: starting setup", VERSION)

        # Build the auth provider. Format-validates the key one more time
        # as a defense-in-depth; the real validity check happens at
        # credential exchange.
        try:
            auth: IemsAuthProvider = IemsCloudAuthProvider(api_key=api_key)
        except InvalidApiKey as exc:
            raise ConfigEntryAuthFailed(f"iEMS: API key format invalid: {exc}") from exc

        # First credential exchange. This is the integration's ONE
        # allowed outbound call during setup that can touch the cloud —
        # failure here blocks startup.
        try:
            creds = await auth.get_credentials()
        except AuthExchangeError as exc:
            # Likely revoked or wrong key — user needs to re-enter.
            raise ConfigEntryAuthFailed(f"iEMS auth exchange failed: {exc}") from exc
        except (OSError, TimeoutError) as exc:
            # Network hiccup — HA will retry.
            raise ConfigEntryNotReady(f"iEMS cloud unreachable: {exc}") from exc

        log.info(
            "iems: auth OK; user_id=%s... iot=%s region=%s",
            creds.user_id[:8], creds.iot_endpoint, creds.region,
        )

        # Build the MQTT adapter once we have real credentials.
        # NOTE: IotCorePublisher class currently lives in the monorepo
        # and assumes cert-based auth. A follow-up commit in THIS repo
        # will replace it with a SigV4-signed MQTT-over-WSS client using
        # the temp creds from `creds`. That class is the last thing we
        # wire after Priya's spec lands.
        from .iot_core import IotCorePublisher
        adapter = IotCorePublisher(auth_provider=auth)
        await adapter.connect()

        # Registry snapshot
        entity_index = _build_entity_index(hass)
        log.info("iems: indexed %d entities", len(entity_index))

        publisher = TelemetryPublisher(
            user_id=creds.identity_id,
            publish_fn=adapter.publish,
        )
        coordinator = IemsCoordinator(
            hass=hass,
            user_id=creds.identity_id,
            entity_index=entity_index,
            publisher=publisher,
        )

        @callback
        def _state_changed(event) -> None:
            coordinator.capture_state_change(event.data.get("new_state"))

        unsub = async_track_state_change_event(
            hass,
            list(entity_index.keys()),
            _state_changed,
        )
        coordinator._unsub_state = unsub

        await coordinator.start()

        hass.data.setdefault(DOMAIN, {})[entry.entry_id] = {
            "coordinator": coordinator,
            "adapter": adapter,
            "publisher": publisher,
            "auth": auth,
        }
        return True

    async def async_unload_entry(hass: "HomeAssistant", entry: "ConfigEntry") -> bool:
        record = hass.data.get(DOMAIN, {}).pop(entry.entry_id, None)
        if not record:
            return True
        coord = record.get("coordinator")
        adapter = record.get("adapter")
        auth = record.get("auth")
        if coord:
            await coord.stop()
        if adapter:
            await adapter.disconnect()
        if auth:
            await auth.close()
        return True


__all__ = ["DOMAIN", "VERSION"]
