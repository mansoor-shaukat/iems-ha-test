"""MQTT publisher for iEMS telemetry + heartbeat.

The publish callable is injected so tests can mock without pulling paho-mqtt.
Production wiring injects `IotCorePublisher.publish` (see iot_core.py).

Signature of `publish_fn`:
    async def publish_fn(*, topic: str, payload: dict, qos: int) -> bool
        Returns True on success. Exceptions are caught here and treated
        as publish failures (payload is enqueued).
"""
from __future__ import annotations

import logging
from collections import deque
from typing import Awaitable, Callable

from .const import (
    BACKOFF_INITIAL_SECONDS,
    BACKOFF_MAX_SECONDS,
    HEARTBEAT_TOPIC_TEMPLATE,
    MAX_QUEUE_DEPTH,
    TELEMETRY_TOPIC_TEMPLATE,
)

log = logging.getLogger("iems.publisher")

PublishFn = Callable[..., Awaitable[bool]]


def backoff_sequence(max_attempts: int = 10):
    """Yield exponential backoff delays capped at BACKOFF_MAX_SECONDS.

    Sequence: 1, 2, 4, 8, 16, 32, 60, 60, ...
    """
    delay = BACKOFF_INITIAL_SECONDS
    for _ in range(max_attempts):
        yield min(delay, BACKOFF_MAX_SECONDS)
        delay *= 2


class TelemetryPublisher:
    """Thin orchestration layer around an injected async `publish_fn`.

    Responsibilities:
      - Format topics (telemetry QoS 1, heartbeat QoS 0).
      - Track batches_sent (for heartbeat metrics).
      - On publish failure, enqueue up to MAX_QUEUE_DEPTH payloads in RAM.
      - drain_queue() drains the queue, preserving FIFO order on partial failure.
    """

    def __init__(self, *, user_id: str, publish_fn: PublishFn) -> None:
        self._user_id = user_id
        self._publish_fn = publish_fn
        # deque(maxlen=N) gives us oldest-drop semantics for free.
        self._queue: deque[dict] = deque(maxlen=MAX_QUEUE_DEPTH)
        self._batches_sent = 0

    # --------------------------- Introspection -------------------------------

    @property
    def queue_depth(self) -> int:
        return len(self._queue)

    @property
    def batches_sent(self) -> int:
        return self._batches_sent

    # ----------------------------- Publish -----------------------------------

    async def _safe_publish(self, *, topic: str, payload: dict, qos: int) -> bool:
        """Call publish_fn, convert any exception to a publish failure."""
        try:
            return bool(await self._publish_fn(topic=topic, payload=payload, qos=qos))
        except (OSError, TimeoutError, ValueError) as exc:
            log.warning("publish error on %s: %s: %s",
                        topic, type(exc).__name__, exc)
            return False

    async def publish_telemetry(self, payload: dict) -> bool:
        """Attempt to publish a telemetry batch. Enqueue on failure."""
        topic = TELEMETRY_TOPIC_TEMPLATE.format(user_id=self._user_id)
        ok = await self._safe_publish(topic=topic, payload=payload, qos=1)
        if ok:
            self._batches_sent += 1
            return True
        # maxlen enforces oldest-drop when full.
        self._queue.append(payload)
        return False

    async def publish_heartbeat(self, payload: dict) -> bool:
        """Heartbeat — QoS 0, fire-and-forget, never enqueued."""
        topic = HEARTBEAT_TOPIC_TEMPLATE.format(user_id=self._user_id)
        return await self._safe_publish(topic=topic, payload=payload, qos=0)

    # ------------------------------ Drain ------------------------------------

    async def drain_queue(self) -> int:
        """Attempt to publish every queued payload. Returns count drained.

        Preserves FIFO order on partial failure: a payload that fails
        during drain is put back at the FRONT of the queue so it's
        retried before newer payloads.
        """
        if not self._queue:
            return 0

        drained = 0
        pending = list(self._queue)
        self._queue.clear()
        topic = TELEMETRY_TOPIC_TEMPLATE.format(user_id=self._user_id)

        for i, payload in enumerate(pending):
            ok = await self._safe_publish(topic=topic, payload=payload, qos=1)
            if ok:
                drained += 1
                self._batches_sent += 1
            else:
                # Re-queue this one + every remaining pending item in order.
                remaining = pending[i:]
                # Insert from the right end so FIFO order is preserved.
                for p in reversed(remaining):
                    self._queue.appendleft(p)
                break
        return drained
