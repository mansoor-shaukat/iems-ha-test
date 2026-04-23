"""Entity deduplication for the iEMS HACS integration.

Two dedup passes are applied to the entity list before classification:

Pass 1 — Phase/channel collapse (apply_dedup):
    HA inverter integrations (Solarman, Deye, etc.) often publish both an
    aggregate sensor and per-phase / per-string sub-sensors for the same
    physical quantity.  We keep the aggregate and suppress the sub-sensors.

    Rules (mirror portal lib/overview-metrics.ts dedup logic):
      _l[1-9]_        → _     (phase suffix: _l1_, _l2_, _l3_ → drop)
      _(pv|bms|batt|ch)[1-9]_  → _<channel>_  (hw-channel: pv1→pv, bms1→bms …)

    Matching uses the *canonical* form of each entity_id.  If two entities
    share the same canonical id, the one whose original id equals the
    canonical id (i.e. already the aggregate) survives; phase/string variants
    are suppressed with suppressed_by="phase_dedup".

Pass 2 — Direct-integration suppression (suppress_direct_sources):
    Any HA-ingested entity whose unique_id or entity_id substring matches a
    token in a known direct-integration entity set is suppressed with
    suppressed_by="<source>_direct".  This enforces the mandatory-dedup
    invariant: one device, one source (direct wins over HA proxy).

Helper — detect_inverter_groups:
    Cluster entity_ids by common prefix + trailing numeric suffix to infer the
    inverter topology (1, 2, or 4 inverters).  Used by higher-level code that
    wants to know how many inverter instances exist before generating canonical
    names.

Suppression stamp format:
    entity["suppressed_by"] = "<reason>"   e.g. "phase_dedup" | "solarman_direct"
    entity["suppressed_by"] = None         → not suppressed (default)
"""
from __future__ import annotations

import logging
import re
from collections import defaultdict
from typing import Iterable

log = logging.getLogger("iems.dedup")

# ---------------------------------------------------------------------------
# Regex patterns (mirrors portal overview-metrics.ts, translated to Python)
# ---------------------------------------------------------------------------

# Phase suffix: _l1_ _l2_ … _l9_  → collapse to _ (drop phase entirely)
_PHASE_RE = re.compile(r"_l[1-9]_")

# Hardware-channel suffix: _pv1_ _bms2_ _batt3_ _ch4_  → _pv_ _bms_ _batt_ _ch_
_HW_CHANNEL_RE = re.compile(r"_(pv|bms|batt|ch)([1-9])_")

# Trailing numeric suffix on a common prefix: e.g. "inverter_1_" → prefix "inverter"
# Used to detect inverter topology groups.
_INVERTER_SUFFIX_RE = re.compile(r"^(.+?)_([1-9]\d*)$")

# Inverter-relevant domain prefixes — only sensor/binary_sensor entities carry power data.
_INVERTER_DOMAINS = {"sensor", "binary_sensor"}


# ---------------------------------------------------------------------------
# Public: canonicalize_entity_id
# ---------------------------------------------------------------------------

def canonicalize_entity_id(entity_id: str) -> str:
    """Return the canonical form of an entity_id by collapsing phase and
    hardware-channel suffixes.

    Examples:
        sensor.inverter_grid_l1_power  → sensor.inverter_grid_power
        sensor.ground_master_pv1_power → sensor.ground_master_pv_power
        sensor.inv_bms1_voltage        → sensor.inv_bms_voltage
        sensor.grid_power              → sensor.grid_power  (unchanged)
    """
    # Two-pass substitution matches the portal logic.
    # Pass 1: phase — _l1_ → _
    canonical = _PHASE_RE.sub("_", entity_id)
    # Pass 2: hw-channel — _pv1_ → _pv_  (keep the channel token, drop the digit)
    canonical = _HW_CHANNEL_RE.sub(r"_\1_", canonical)
    return canonical


# ---------------------------------------------------------------------------
# Public: detect_inverter_groups
# ---------------------------------------------------------------------------

def detect_inverter_groups(entities: list[dict]) -> dict[str, int]:
    """Cluster entity_ids by common prefix + trailing numeric suffix.

    Returns a dict mapping the common prefix to the count of distinct numeric
    suffixes found under it.

    Only entity_ids in sensor/binary_sensor domain (or with no explicit domain
    stored — relies on the entity_id containing 'sensor.') are considered;
    switch/light/climate entities are ignored.

    Examples:
        [sensor.inverter_1_pv_power, sensor.inverter_2_pv_power]
            → {"inverter": 2}
        [sensor.ground_master_pv_power, sensor.basement_master_pv_power]
            → {"ground_master": 1, "basement_master": 1}
        []
            → {}
    """
    # Maps prefix → set of observed numeric suffixes
    prefix_indices: dict[str, set[str]] = defaultdict(set)
    # Maps prefix → set of entity_ids (to track those WITHOUT numeric suffix)
    prefix_no_suffix: dict[str, int] = defaultdict(int)

    for e in entities:
        eid: str = e.get("entity_id", "")
        # Skip non-sensor domains
        domain_part = eid.split(".")[0] if "." in eid else ""
        if domain_part and domain_part not in _INVERTER_DOMAINS:
            continue

        # Strip the domain prefix (e.g. "sensor.") for name analysis
        name_part = eid.split(".", 1)[1] if "." in eid else eid

        # Check for a common-prefix + trailing numeric suffix pattern.
        # We look at the *last* underscore-delimited segment that is all digits.
        parts = name_part.split("_")
        last_digit_idx = None
        for i in range(len(parts) - 1, -1, -1):
            if parts[i].isdigit():
                last_digit_idx = i
                break

        if last_digit_idx is not None and last_digit_idx > 0:
            # prefix = everything before the numeric segment
            prefix = "_".join(parts[:last_digit_idx])
            suffix_digit = parts[last_digit_idx]
            prefix_indices[prefix].add(suffix_digit)
        else:
            # No trailing numeric suffix — treat the whole name as its own group.
            # This handles named inverters like "ground_master", "basement_slave".
            # We use a heuristic: if the entity contains an inverter-category keyword,
            # count the distinct non-numeric-suffixed prefixes.
            _inverter_kw = {"pv", "grid", "load", "battery", "inverter", "deye", "solarman"}
            if any(kw in name_part for kw in _inverter_kw):
                # Extract the device prefix = all parts that precede the first
                # inverter-category keyword segment.  For "ground_master_pv_power"
                # this yields "ground_master"; for "inv_grid_power" → "inv".
                device_prefix_parts = []
                for part in parts:
                    if part in _inverter_kw:
                        break
                    device_prefix_parts.append(part)
                device_prefix = "_".join(device_prefix_parts) if device_prefix_parts else parts[0]
                prefix_no_suffix[device_prefix] += 1

    result: dict[str, int] = {}
    for prefix, indices in prefix_indices.items():
        result[prefix] = len(indices)
    # Add single-instance named groups (no numeric suffix, appeared once per prefix)
    for prefix, count in prefix_no_suffix.items():
        if prefix not in result:
            result[prefix] = 1  # each named inverter is its own logical group
    return result


# ---------------------------------------------------------------------------
# Public: apply_dedup
# ---------------------------------------------------------------------------

def apply_dedup(entities: list[dict]) -> list[dict]:
    """Apply phase/channel dedup to a list of entity dicts (in-place mutation).

    For each canonical entity_id:
      - The entity whose original entity_id equals the canonical form survives
        (i.e. the aggregate sensor is preferred).
      - If no exact-match aggregate exists, the first entity seen for that
        canonical id survives.
      - All other entities with the same canonical id are suppressed with
        suppressed_by="phase_dedup".

    Entities already stamped with a suppressed_by value (e.g. from a prior
    direct-source suppression step) are left unchanged.

    Returns the same list (mutated in-place) for convenience.
    """
    # First pass: build canonical → list[entity] mapping (skip pre-suppressed)
    canonical_map: dict[str, list[dict]] = defaultdict(list)
    pre_suppressed: list[dict] = []

    for e in entities:
        if e.get("suppressed_by") is not None:
            pre_suppressed.append(e)
            continue
        canonical = canonicalize_entity_id(e["entity_id"])
        canonical_map[canonical].append(e)

    # Second pass: for each canonical group, elect a survivor.
    for canonical, group in canonical_map.items():
        if len(group) == 1:
            # No duplicates — leave as-is
            continue

        # Prefer the entity whose entity_id IS the canonical (the aggregate).
        survivor = None
        for e in group:
            if e["entity_id"] == canonical:
                survivor = e
                break

        # Fallback: first entity seen
        if survivor is None:
            survivor = group[0]

        # Suppress all others
        for e in group:
            if e is not survivor:
                e["suppressed_by"] = "phase_dedup"
                log.debug(
                    "phase_dedup: suppressed %s (canonical: %s)",
                    e["entity_id"],
                    canonical,
                )

    return entities


# ---------------------------------------------------------------------------
# Public: suppress_direct_sources
# ---------------------------------------------------------------------------

def suppress_direct_sources(
    entities: list[dict],
    direct_sets: dict[str, Iterable[str]],
) -> list[dict]:
    """Stamp suppressed_by on any HA entity that matches a direct-integration token.

    Parameters
    ----------
    entities
        List of entity dicts with at least `entity_id` and `unique_id` keys.
    direct_sets
        Mapping from source name (e.g. "solarman_direct") to an iterable of
        match tokens.  A token matches if it appears as a substring in either
        the entity's `unique_id` or `entity_id`.

    Matching rules:
      1. exact `unique_id` match against a token in the set
      2. substring of `unique_id` contains a token
      3. substring of `entity_id` contains a token

    Already-suppressed entities (suppressed_by is not None) are skipped.

    Returns the same list (mutated in-place) for convenience.
    """
    if not direct_sets or not entities:
        return entities

    # Pre-materialise iterables so we don't exhaust generators
    materialised: dict[str, list[str]] = {
        src: list(tokens) for src, tokens in direct_sets.items()
    }

    for e in entities:
        if e.get("suppressed_by") is not None:
            continue  # already stamped — don't overwrite

        unique_id: str = (e.get("unique_id") or "").lower()
        entity_id: str = (e.get("entity_id") or "").lower()

        matched_source: str | None = None
        for source, tokens in materialised.items():
            for token in tokens:
                tok = token.lower()
                if tok and (tok == unique_id or tok in unique_id or tok in entity_id):
                    matched_source = source
                    break
            if matched_source:
                break

        if matched_source:
            e["suppressed_by"] = matched_source
            log.debug(
                "direct_dedup: suppressed %s via %s",
                e.get("entity_id"),
                matched_source,
            )

    return entities
