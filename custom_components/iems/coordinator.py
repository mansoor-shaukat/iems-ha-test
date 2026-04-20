"""Coordinator — bridges HA state events, classifier, and publisher.

Responsibilities:
  - Hold the entity_index (registry snapshot built at async_setup_entry).
  - Receive state_changed events (via HA's async_track_state_change_event),
    classify, enrich with brand/area/unit, append to `pending`.
  - Flush `pending` every BATCH_WINDOW_SECONDS by building a telemetry
    payload and handing it to the publisher.
  - Emit a heartbeat every HEARTBEAT_INTERVAL_SECONDS.

Pure of HA APIs for the capture/flush/heartbeat paths so unit tests
only need a MagicMock hass. Real HA wiring lives in __init__.py.
"""
from __future__ import annotations

import asyncio
import logging
import time
from typing import Any

from .classifier import classify
from .const import BATCH_WINDOW_SECONDS, HEARTBEAT_INTERVAL_SECONDS
from .telemetry import EmptyBatchError, build_batch, build_heartbeat

log = logging.getLogger("iems.coordinator")


class IemsCoordinator:
    def __init__(
        self,
        *,
        hass,
        user_id: str,
        entity_index: dict[str, dict[str, Any]],
        publisher,
    ) -> None:
        self._hass = hass
        self._user_id = user_id
        self._entity_index = entity_index
        self._publisher = publisher
        self.pending: list[dict] = []
        self._unsub_state = None
        self._batch_task: asyncio.Task | None = None
        self._heartbeat_task: asyncio.Task | None = None
        self._started_at = time.monotonic()

    # ---------------------- State capture ---------------------------------

    def capture_state_change(self, new_state) -> None:
        """Sync handler — called from HA's event bus callback. No I/O."""
        if new_state is None:
            return
        entity_id = new_state.entity_id
        meta = self._entity_index.get(entity_id)
        if not meta:
            return  # not in our registry snapshot → drop

        classified = classify({
            "entity_id": entity_id,
            "domain": meta.get("domain") or entity_id.split(".", 1)[0],
            "platform": meta.get("platform"),
            "device_class": meta.get("device_class"),
            "unit": meta.get("unit"),
            "name": meta.get("name"),
        })
        if not classified.get("surface"):
            return

        ts = self._extract_ts(new_state)

        captured: dict[str, Any] = {
            "entity_id": entity_id,
            "category": classified["category"],
            "ts": ts,
            "state": new_state.state,
        }
        if meta.get("brand"):
            captured["brand"] = meta["brand"]
        if meta.get("area"):
            captured["area"] = meta["area"]
        if meta.get("unit"):
            captured["unit"] = meta["unit"]
        attrs = getattr(new_state, "attributes", None)
        if attrs:
            captured["attributes"] = dict(attrs)

        self.pending.append(captured)

    @staticmethod
    def _extract_ts(new_state) -> str:
        """Normalize HA state.last_changed to ISO-8601 UTC with Z suffix."""
        try:
            iso = new_state.last_changed.isoformat()
        except (AttributeError, TypeError):
            # Fallback — won't validate strictly but keeps pipeline flowing
            from datetime import datetime, timezone
            iso = datetime.now(timezone.utc).isoformat()
        if iso.endswith("+00:00"):
            iso = iso[:-6] + "Z"
        return iso

    # ---------------------- Flush + publish -------------------------------

    async def flush(self) -> None:
        """Drain `pending` into a batch and hand to publisher.

        The publisher owns retry (via its bounded queue). We always clear
        `pending` after handing off, so we never double-ship a batch.
        """
        if not self.pending:
            return
        batch = self.pending[:]
        self.pending.clear()
        try:
            ha_version = getattr(self._hass.config, "version", "unknown")
            payload = build_batch(
                user_id=self._user_id,
                entities=batch,
                ha_version=ha_version,
            )
        except EmptyBatchError:
            return
        except ValueError as exc:
            log.warning("flush: build_batch rejected payload: %s", exc)
            return
        await self._publisher.publish_telemetry(payload)

    async def heartbeat_once(self) -> None:
        ha_version = getattr(self._hass.config, "version", "unknown")
        hb = build_heartbeat(
            user_id=self._user_id,
            ha_version=ha_version,
            uptime_s=int(time.monotonic() - self._started_at),
            batches_sent=getattr(self._publisher, "batches_sent", 0),
            queue_depth=getattr(self._publisher, "queue_depth", 0),
        )
        await self._publisher.publish_heartbeat(hb)

    # ---------------------- Background timers -----------------------------

    async def _batch_loop(self) -> None:
        while True:
            try:
                await asyncio.sleep(BATCH_WINDOW_SECONDS)
                await self.flush()
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # pragma: no cover - safety net
                log.error("batch flush crashed: %s: %s", type(exc).__name__, exc)

    async def _heartbeat_loop(self) -> None:
        while True:
            try:
                await asyncio.sleep(HEARTBEAT_INTERVAL_SECONDS)
                await self.heartbeat_once()
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # pragma: no cover
                log.error("heartbeat crashed: %s: %s", type(exc).__name__, exc)

    async def start(self) -> None:
        self._batch_task = asyncio.create_task(self._batch_loop())
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())

    async def stop(self) -> None:
        for t in (self._batch_task, self._heartbeat_task):
            if t:
                t.cancel()
                try:
                    await t
                except asyncio.CancelledError:
                    pass
        if self._unsub_state:
            self._unsub_state()
            self._unsub_state = None
