"""MTronic dispatch event capture — state machine for shed/charge/discharge/import.

Monitors HA state_changed events for MTronic PP (Power Panel) and SP (Smart Plug)
devices. On each transition derives the dispatch state and emits a DispatchEvent.

Dispatch state derivation (Sprint 2 — MTronic only):
  - switch OFF → shed   (load shedding: device cut from circuit)
  - switch ON  → import (device on, importing from grid or battery)
  - charge / discharge reserved for Phase 7 (Deye inverter EMS control loop)

Power reading:
  - Taken from HA state attributes key 'current_power_w' (MTronic PP/SP HA entity
    attribute name as reported by the local mtronic_bridge / HA integration).
  - Falls back to None if the attribute is missing or non-numeric.
  - Per protocol docs: shadow 'p' field retains stale value after switch-OFF;
    we zero it out ourselves when state is 'off'.

Dedup invariant (memory/feedback_ha_dedup_mandatory.md):
  - Before emitting, check whether the device_id appears in the caller-supplied
    direct_entity_ids set. If yes, suppress — the direct integration owns it.

Thread safety:
  - All public methods are sync (called from HA event loop callback).
  - DispatchCapture is stateful: it tracks previous state per device to suppress
    duplicate events (same state repeated without an actual transition).
"""
from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

log = logging.getLogger("iems.mtronic_dispatch")

# Dispatch state literals — lowercase, matches contracts/mqtt_topics.md v0.2.0.
DISPATCH_STATES = frozenset({"shed", "charge", "discharge", "import"})

# HA platforms that carry MTronic switch/plug entities.
# We only capture dispatch events from these platforms — other platforms
# (Deye, Solarman) are handled by their own integrations.
MTRONIC_PLATFORMS: frozenset[str] = frozenset({"mqtt", "mtronic"})

# HA domains that represent controllable switch/plug devices.
SWITCH_DOMAINS: frozenset[str] = frozenset({"switch"})

# HA attribute keys where MTronic power monitoring data may appear.
# The mtronic_bridge publishes 'p' from the shadow as 'current_power_w'.
POWER_ATTR_KEYS: tuple[str, ...] = (
    "current_power_w",   # primary: mtronic_bridge naming
    "power",             # fallback: generic HA device_class=power attribute
    "watt",              # fallback: some MTronic entity variants
)


@dataclass
class DispatchEvent:
    """A single MTronic device dispatch state transition."""
    event_id: str          # UUIDv4
    ts: str                # ISO-8601 UTC with Z
    device_id: str         # HA entity_id
    state: str             # "shed" | "charge" | "discharge" | "import"
    power_w: float | None  # watts at transition time; None if unavailable or OFF
    area: str | None       # HA area name, if known
    suppressed_by: str | None = field(default=None, repr=False)


def _extract_power(attrs: dict[str, Any] | None, is_on: bool) -> float | None:
    """Extract power reading from HA state attributes.

    Returns None when:
    - switch is OFF (shadow p retains stale value — zeroed per protocol docs)
    - attribute is absent
    - value is non-numeric or NaN
    """
    if not is_on or not attrs:
        return None
    for key in POWER_ATTR_KEYS:
        val = attrs.get(key)
        if val is None:
            continue
        try:
            f = float(val)
            # NaN or infinite readings are treated as unavailable
            if f != f or f == float("inf") or f == float("-inf"):
                return None
            return f
        except (TypeError, ValueError):
            continue
    return None


def _iso_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class DispatchCapture:
    """Stateful capture of MTronic dispatch events from HA state changes.

    Parameters
    ----------
    direct_entity_ids
        Set of entity_ids owned by a direct integration (e.g. Solarman).
        Any device_id in this set is suppressed — direct integration wins.
    """

    def __init__(self, direct_entity_ids: frozenset[str] | None = None) -> None:
        self._direct: frozenset[str] = direct_entity_ids or frozenset()
        # Track last emitted state per device_id to suppress identical repeats.
        self._last_state: dict[str, str] = {}

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def process_state_change(
        self,
        *,
        entity_id: str,
        platform: str | None,
        domain: str | None,
        new_state: str,
        attrs: dict[str, Any] | None,
        ts: str | None,
        area: str | None,
    ) -> DispatchEvent | None:
        """Process a single HA state_changed event.

        Returns a DispatchEvent if this transition should be forwarded,
        or None if it should be suppressed.

        Parameters
        ----------
        entity_id
            HA entity_id of the changed entity.
        platform
            HA platform (e.g. 'mqtt', 'mtronic'). Non-MTronic platforms → None.
        domain
            HA domain (e.g. 'switch'). Only 'switch' domain is handled.
        new_state
            The new state value from HA (e.g. 'on', 'off').
        attrs
            HA state attributes dict.
        ts
            ISO-8601 timestamp from HA last_changed, or None to use current time.
        area
            HA area name for this device, if known.
        """
        # Filter: only MTronic platforms, switch domain
        if not self._is_mtronic_switch(platform, domain):
            return None

        # Dedup: direct-integration devices — suppress
        if entity_id in self._direct:
            log.debug(
                "dispatch: suppressed %s — direct integration owns it", entity_id
            )
            return DispatchEvent(
                event_id=str(uuid.uuid4()),
                ts=ts or _iso_now(),
                device_id=entity_id,
                state="shed",  # placeholder — event is suppressed
                power_w=None,
                area=area,
                suppressed_by="direct_integration",
            )

        state_lower = (new_state or "").lower().strip()
        dispatch_state = self._derive_state(state_lower)
        if dispatch_state is None:
            log.debug(
                "dispatch: unknown HA state %r for %s — skipped", new_state, entity_id
            )
            return None

        # Suppress duplicate transitions (same state repeated)
        prev = self._last_state.get(entity_id)
        if prev == dispatch_state:
            log.debug(
                "dispatch: duplicate state %s for %s — suppressed", dispatch_state, entity_id
            )
            return None

        self._last_state[entity_id] = dispatch_state
        is_on = dispatch_state == "import"
        power_w = _extract_power(attrs, is_on=is_on)

        event = DispatchEvent(
            event_id=str(uuid.uuid4()),
            ts=ts or _iso_now(),
            device_id=entity_id,
            state=dispatch_state,
            power_w=power_w,
            area=area,
        )
        log.info(
            "dispatch: %s → %s (power_w=%s)", entity_id, dispatch_state, power_w
        )
        return event

    def reset_device(self, entity_id: str) -> None:
        """Clear cached state for a device (use when entity removed or HA restarted)."""
        self._last_state.pop(entity_id, None)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _is_mtronic_switch(platform: str | None, domain: str | None) -> bool:
        plat = (platform or "").lower()
        dom = (domain or "").lower()
        return plat in MTRONIC_PLATFORMS and dom in SWITCH_DOMAINS

    @staticmethod
    def _derive_state(ha_state: str) -> str | None:
        """Map HA switch state to dispatch state.

        'on'  → import  (device active, drawing power)
        'off' → shed    (device cut from circuit)
        anything else → None (unavailable, unknown — skip)
        """
        if ha_state == "on":
            return "import"
        if ha_state == "off":
            return "shed"
        return None
