"""Build telemetry + heartbeat payloads for iEMS cloud.

Schema: contracts/telemetry.schema.json (owned by CTO, read-only here).
Heartbeat shape: hacs_spec.md §3f.
"""
from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any, Iterable

from .classifier import VALID_CATEGORIES
from .const import MAX_ENTITIES_PER_BATCH, SCHEMA_VERSION, VERSION

# Attribute keys we strip on ingestion — HA UI noise, not semantic state.
ATTRIBUTE_STRIP_KEYS = frozenset({
    "friendly_name",
    "icon",
    "entity_picture",
    "supported_features",
    "assumed_state",
    "hidden",
    "editable",
    "restored",
})


class EmptyBatchError(ValueError):
    """Raised when build_batch is called with zero entities.

    Schema requires `entities` minItems: 1, so an empty batch must never
    be shipped.
    """


def _now_iso() -> str:
    """ISO-8601 UTC with trailing 'Z' per schema `format: date-time`."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _clean_attributes(attrs: dict[str, Any] | None) -> dict[str, Any] | None:
    if not attrs:
        return None
    cleaned = {k: v for k, v in attrs.items() if k not in ATTRIBUTE_STRIP_KEYS}
    return cleaned or None


def _validate_entity(e: dict) -> None:
    if e.get("category") not in VALID_CATEGORIES:
        raise ValueError(
            f"Entity {e.get('entity_id')!r} has invalid category "
            f"{e.get('category')!r}"
        )


def build_batch(
    *,
    user_id: str,
    entities: Iterable[dict],
    ha_version: str,
    instance_id: str | None = None,
    country: str | None = None,
    timezone: str | None = None,
) -> dict:
    """Build a `telemetry.schema.json`-conforming payload.

    Parameters
    ----------
    user_id
        Cognito sub (36-char UUID); goes into the payload and topic.
    entities
        Classified entity dicts. Required keys: entity_id, category, ts,
        state. Optional: brand, area, unit, attributes.
    ha_version
        Home Assistant core version string.
    instance_id
        HA instance UUID (optional; multi-install debugging).
    country
        ISO 3166-1 alpha-2 country code from HA core.config.country.
        Optional (v0.5.0); omitted from payload when None or empty.
    timezone
        HA core.config.time_zone string (e.g. 'Asia/Karachi').
        Optional (v0.5.0); omitted from payload when None or empty.

    Raises
    ------
    EmptyBatchError
        When `entities` is empty (schema requires minItems: 1).
    ValueError
        When len(entities) > MAX_ENTITIES_PER_BATCH or a category is not
        in VALID_CATEGORIES.
    """
    entity_list = list(entities)
    if not entity_list:
        raise EmptyBatchError("Cannot build a telemetry batch from zero entities")
    if len(entity_list) > MAX_ENTITIES_PER_BATCH:
        raise ValueError(
            f"Batch size {len(entity_list)} exceeds max {MAX_ENTITIES_PER_BATCH}"
        )

    out_entities: list[dict] = []
    for e in entity_list:
        _validate_entity(e)
        item: dict[str, Any] = {
            "entity_id": e["entity_id"],
            "category": e["category"],
            "ts": e["ts"],
            "state": e["state"],
        }
        if e.get("brand"):
            item["brand"] = e["brand"]
        if e.get("area"):
            item["area"] = e["area"]
        if e.get("unit"):
            item["unit"] = e["unit"]
        cleaned = _clean_attributes(e.get("attributes"))
        if cleaned:
            item["attributes"] = cleaned
        out_entities.append(item)

    source: dict[str, Any] = {
        "integration_version": VERSION,
        "ha_version": ha_version,
    }
    if instance_id:
        source["instance_id"] = instance_id
    # v0.5.0 optional fields — only emit when HA has them configured.
    if country:
        source["country"] = country
    if timezone:
        source["timezone"] = timezone

    return {
        "schema_version": SCHEMA_VERSION,
        "user_id": user_id,
        "batch_id": str(uuid.uuid4()),
        "ts": _now_iso(),
        "source": source,
        "entities": out_entities,
    }


def build_heartbeat(
    *,
    user_id: str,
    ha_version: str,
    uptime_s: int,
    batches_sent: int,
    queue_depth: int,
) -> dict:
    """Heartbeat payload — shape per hacs_spec.md §3f.

    Published to `iems/{user_id}/heartbeat` at QoS 0 every 60s.
    Consumed by Priya's CloudWatch metric filter for liveness monitoring.
    """
    return {
        "schema_version": SCHEMA_VERSION,
        "user_id": user_id,
        "ts": _now_iso(),
        "integration_version": VERSION,
        "ha_version": ha_version,
        "uptime_s": int(uptime_s),
        "batches_sent": int(batches_sent),
        "queue_depth": int(queue_depth),
    }
