"""Device-class inference helper for iEMS HACS integration.

DO NOT confuse with classifier.py (reserved for Sarah's topology research).
This module is a pure-function heuristic layer that maps HA sensor metadata
to a high-level energy role.  No HA dependency — importable anywhere.

Usage::

    from .classifier_helper import infer_role

    role, confidence = infer_role(
        device_class="power",
        state_class="measurement",
        unit_of_measurement="W",
        friendly_name="Ground Master PV Power",
        integration_name="solarman",
    )
    # → ("solar", 0.95)

Roles
-----
solar          — PV generation (inverter, string, panel-level)
battery        — battery storage (SOC, charge/discharge power)
grid           — utility grid (import, export, mains)
high_power_load — high-draw appliances (AC, heat pump, EV charger, …)
other          — everything else

Confidence levels
-----------------
0.95   exact match on device_class AND name keyword
0.75   name keyword match only (device_class absent or irrelevant)
0.5    device_class match only, name gives no role signal (ambiguous power)
0.0    role == 'other', or no signal at all
"""
from __future__ import annotations

# ---------------------------------------------------------------------------
# Keyword sets
# ---------------------------------------------------------------------------

# Name substrings that indicate solar generation.
_SOLAR_NAME_KW: frozenset[str] = frozenset({
    "pv", "solar", "inverter", "string",
})

# Name substrings that indicate battery storage.
_BATTERY_NAME_KW: frozenset[str] = frozenset({
    "battery", "soc",
})

# Name substrings that indicate grid connection.
_GRID_NAME_KW: frozenset[str] = frozenset({
    "grid", "import", "export", "mains", "utility",
})

# Friendly-name substrings for high-power loads (case-insensitive).
_HIGH_POWER_LOAD_KW: frozenset[str] = frozenset({
    "ac",
    "air conditioner",
    "aircon",
    "heat pump",
    "water heater",
    "geyser",
    "ev charger",
    "pool pump",
    "dishwasher",
    "washer",
    "dryer",
    "oven",
    "kettle",
})

# Device classes that carry power/energy signal.
_POWER_DEVICE_CLASSES: frozenset[str] = frozenset({"power", "energy"})

# State classes that indicate an ongoing measurement or accumulation.
_VALID_STATE_CLASSES: frozenset[str] = frozenset({"measurement", "total_increasing"})


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _lower(s: str | None) -> str:
    return (s or "").lower().strip()


def _name_contains(name_lower: str, keywords: frozenset[str]) -> bool:
    """Return True if any keyword is a substring of name_lower."""
    return any(kw in name_lower for kw in keywords)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def infer_role(
    device_class: str | None,
    state_class: str | None,
    unit_of_measurement: str | None,
    friendly_name: str | None,
    integration_name: str | None,
) -> tuple[str, float]:
    """Infer energy role from HA sensor metadata.

    Parameters
    ----------
    device_class
        HA device_class string (e.g. 'power', 'energy', 'battery').
    state_class
        HA state_class string (e.g. 'measurement', 'total_increasing').
    unit_of_measurement
        HA unit string (e.g. 'W', 'kWh', '%').
    friendly_name
        Human-readable sensor name from HA.
    integration_name
        HA integration/platform name (e.g. 'solarman', 'tuya').

    Returns
    -------
    (role, confidence)
        role       : one of 'solar', 'battery', 'grid', 'high_power_load', 'other'
        confidence : float in [0.0, 1.0]
    """
    dc = _lower(device_class)
    name = _lower(friendly_name)
    has_power_dc = dc in _POWER_DEVICE_CLASSES
    has_battery_dc = dc == "battery"

    # ------------------------------------------------------------------
    # Priority 1: grid — checked BEFORE solar/battery so that a sensor
    # called "Grid Power" with device_class=power routes to grid, not solar.
    # ------------------------------------------------------------------
    if _name_contains(name, _GRID_NAME_KW):
        conf = 0.95 if has_power_dc else 0.75
        return ("grid", conf)

    # ------------------------------------------------------------------
    # Priority 2: battery
    # ------------------------------------------------------------------
    # Exact: device_class=battery (SOC sensors) or power+name
    if has_battery_dc or _name_contains(name, _BATTERY_NAME_KW):
        # Prefer exact device_class match
        if has_battery_dc and _name_contains(name, _BATTERY_NAME_KW):
            return ("battery", 0.95)
        if has_battery_dc:
            return ("battery", 0.95)  # device_class alone is strong signal for battery
        # Name-only match
        return ("battery", 0.75)

    # ------------------------------------------------------------------
    # Priority 3: high_power_load
    # Checked before solar so a sensor for an AC unit isn't mislabelled solar.
    # ------------------------------------------------------------------
    if name and _name_contains(name, _HIGH_POWER_LOAD_KW):
        conf = 0.95 if has_power_dc else 0.75
        return ("high_power_load", conf)

    # ------------------------------------------------------------------
    # Priority 4: solar
    # ------------------------------------------------------------------
    if _name_contains(name, _SOLAR_NAME_KW):
        conf = 0.95 if has_power_dc else 0.75
        return ("solar", conf)

    # ------------------------------------------------------------------
    # Priority 5: ambiguous power — device_class=power/energy but name
    # gives no role signal.  Confidence 0.5.
    # ------------------------------------------------------------------
    if has_power_dc:
        return ("other", 0.5)

    # ------------------------------------------------------------------
    # Default: other, no signal
    # ------------------------------------------------------------------
    return ("other", 0.0)
