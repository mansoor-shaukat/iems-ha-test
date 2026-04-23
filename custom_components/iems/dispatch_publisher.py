"""Dispatch event publisher — forwards MTronic dispatch events to IoT Core.

Topic: iems/{user_id}/dispatch  (QoS 1, contracts/mqtt_topics.md v0.2.0)

Payload shape (v0.2.0 per contracts/mqtt_topics.md):
  {
    "schema_version": "0.2.0",
    "user_id": "<cognito-sub>",
    "event_id": "<uuid4>",
    "ts": "<ISO-8601-UTC-Z>",
    "device_id": "<entity_id>",
    "state": "shed"|"charge"|"discharge"|"import",
    "power_w": <float|null>,
    "area": <string|null>,
    "source": {"integration_version": "...", "ha_version": "..."}
  }

State enum is LOWERCASE — contracts/mqtt_topics.md v0.2.0 canonical.
Priya's dispatch_ingest Lambda rejects uppercase payloads with HTTP 400.

The publisher uses an injected async publish_fn (same interface as TelemetryPublisher)
so tests never need a real MQTT connection.
"""
from __future__ import annotations

import logging
from typing import Awaitable, Callable

from .const import VERSION
from .mtronic_dispatch import DispatchEvent

log = logging.getLogger("iems.dispatch_publisher")

DISPATCH_TOPIC_TEMPLATE = "iems/{user_id}/dispatch"
DISPATCH_SCHEMA_VERSION = "0.2.0"

PublishFn = Callable[..., Awaitable[bool]]


def build_dispatch_payload(
    *,
    user_id: str,
    event: DispatchEvent,
    ha_version: str,
) -> dict:
    """Build the JSON payload for a single dispatch event.

    Raises
    ------
    ValueError
        If event.state is not one of the valid dispatch states.
    """
    from .mtronic_dispatch import DISPATCH_STATES
    if event.state not in DISPATCH_STATES:
        raise ValueError(
            f"build_dispatch_payload: invalid state {event.state!r}. "
            f"Must be one of {sorted(DISPATCH_STATES)}"
        )

    payload: dict = {
        "schema_version": DISPATCH_SCHEMA_VERSION,
        "user_id": user_id,
        "event_id": event.event_id,
        "ts": event.ts,
        "device_id": event.device_id,
        "state": event.state,
        "power_w": event.power_w,
        "area": event.area,
        "source": {
            "integration_version": VERSION,
            "ha_version": ha_version,
        },
    }
    return payload


class DispatchPublisher:
    """Thin publisher for MTronic dispatch events.

    Suppressed events (event.suppressed_by is not None) are never published.
    Publish failures are logged but not re-queued — dispatch events are
    fire-and-forget at the transport level (QoS 1 covers at-least-once at MQTT).

    Parameters
    ----------
    user_id
        Cognito sub — used to format the MQTT topic.
    publish_fn
        Async callable with signature publish_fn(*, topic, payload, qos) → bool.
        Injected for testability; production wiring passes IotCorePublisher.publish.
    ha_version
        Home Assistant version string (from hass.config.version).
    """

    def __init__(
        self,
        *,
        user_id: str,
        publish_fn: PublishFn,
        ha_version: str = "unknown",
    ) -> None:
        self._user_id = user_id
        self._publish_fn = publish_fn
        self._ha_version = ha_version
        self._events_published = 0

    @property
    def events_published(self) -> int:
        return self._events_published

    async def publish_dispatch(self, event: DispatchEvent) -> bool:
        """Publish a single dispatch event to IoT Core.

        Returns True on successful broker ACK, False on failure or suppression.
        Suppressed events are silently dropped without logging a warning.
        """
        if event.suppressed_by is not None:
            log.debug(
                "dispatch_publisher: suppressed event for %s (reason=%s)",
                event.device_id,
                event.suppressed_by,
            )
            return False

        topic = DISPATCH_TOPIC_TEMPLATE.format(user_id=self._user_id)

        try:
            payload = build_dispatch_payload(
                user_id=self._user_id,
                event=event,
                ha_version=self._ha_version,
            )
        except ValueError as exc:
            log.warning("dispatch_publisher: payload build failed: %s", exc)
            return False

        try:
            ok = bool(
                await self._publish_fn(topic=topic, payload=payload, qos=1)
            )
        except (OSError, TimeoutError, ValueError) as exc:
            log.warning(
                "dispatch_publisher: publish failed for %s: %s: %s",
                event.device_id, type(exc).__name__, exc,
            )
            return False

        if ok:
            self._events_published += 1
            log.info(
                "dispatch_publisher: published %s → %s (event_id=%s)",
                event.device_id, event.state, event.event_id,
            )
        else:
            log.warning(
                "dispatch_publisher: broker rejected publish for %s", event.device_id
            )
        return ok
