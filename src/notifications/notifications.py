"""Notification envelope publisher for FlowFirst.

When NOTIFICATIONS_ENABLED=true, DB Updater (Process 4) calls notify() after each message
is successfully persisted and acked. A lightweight JSON envelope is published to a durable
RabbitMQ fanout exchange; external subscribers bind their own queues to that exchange.

Key design constraints (see AGENTS.md):
- notify() is called AFTER basic_ack — pipeline safety is never risked for a notification.
- mandatory=False — no subscriber bound means the notification is silently discarded, never
  bounced back or raised as an exception.
- All exceptions inside notify() are caught and logged as warnings; the caller is never affected.
- history[] is stripped from the payload field to avoid large envelopes.
- NOTIFICATIONS_FLOWS filters which flow numbers actually emit.
"""

import json
import logging
import time
import uuid

import pika

from src.config.config import (
    NODE_NAME,
    NOTIFICATIONS_ENABLED,
    NOTIFICATIONS_EXCHANGE,
    NOTIFICATIONS_FLOWS,
    NOTIFICATIONS_MAX_PAYLOAD_BYTES,
)

log = logging.getLogger("flowfirst.notifications")

# ---------------------------------------------------------------------------
# Event-type mapping: (flow_number, path_hint) -> dotted event_type string.
# path_hint distinguishes the two Flow 6 paths; all other flows use None.
# ---------------------------------------------------------------------------
_EVENT_TYPES = {
    (1, None):       "telemetry.processed",
    (2, None):       "metric.processed",
    (3, None):       "telemetry.remote_processed",
    (4, None):       "device.created",
    (5, None):       "device.queried",
    (6, None):       "device.updated",
    (6, "remote"):   "device.edge_synced",
    (7, None):       "device.decommissioned",
}

# Pre-computed set of enabled flow numbers (empty = all flows enabled).
def _parse_flows(raw: str) -> "frozenset[int]":
    """Return a frozenset of enabled flow ints, or empty set meaning 'all'."""
    if not raw or raw.strip() in ("*", ""):
        return frozenset()
    result = set()
    for part in raw.split(","):
        part = part.strip()
        if part.isdigit():
            result.add(int(part))
    return frozenset(result)


_ENABLED_FLOWS = _parse_flows(NOTIFICATIONS_FLOWS)


def _flow_enabled(flow: int) -> bool:
    """True if notifications should be emitted for this flow number."""
    return not _ENABLED_FLOWS or flow in _ENABLED_FLOWS


def _safe_payload(data: dict) -> "dict | None":
    """Return a payload subset safe to include in the envelope.

    - Strips the 'history' array (can be large and is already in MariaDB/Swift).
    - Returns None when the serialised result exceeds NOTIFICATIONS_MAX_PAYLOAD_BYTES
      and that limit is > 0, so callers can replace it with a size note.
    """
    subset = {k: v for k, v in data.items() if k != "history"}
    if NOTIFICATIONS_MAX_PAYLOAD_BYTES > 0:
        try:
            serialised = json.dumps(subset)
        except Exception:
            return None
        if len(serialised.encode()) > NOTIFICATIONS_MAX_PAYLOAD_BYTES:
            return None
    return subset


def declare_exchange(channel) -> None:
    """Declare the notifications fanout exchange on an already-open pika channel.

    Safe to call on every channel open / reconnect — RabbitMQ treats a re-declaration
    with identical arguments as a no-op. Must be called before the first notify().
    Only active when NOTIFICATIONS_ENABLED is true.
    """
    if not NOTIFICATIONS_ENABLED:
        return
    channel.exchange_declare(
        exchange=NOTIFICATIONS_EXCHANGE,
        exchange_type="fanout",
        durable=True,
    )


def notify(channel, event_type: str, data: dict, flow: int, path_hint: str = None) -> None:
    """Publish one notification envelope to the fanout exchange.

    Must be called AFTER ch.basic_ack() on the pipeline message.

    Args:
        channel:    The pika channel already held by the P4 handler greenthread.
        event_type: Dotted event string (e.g. "device.created"). Use the constants
                    in _EVENT_TYPES or pass a custom string.
        data:       The full pipeline message dict (post-enrichment, post-history-append).
        flow:       Flow number (1-7), used for filtering and the envelope 'flow' field.
        path_hint:  Optional sub-path discriminator for flows with multiple paths
                    (e.g. "remote" for Flow 6 remote return). Defaults to None.
    """
    if not NOTIFICATIONS_ENABLED:
        return
    if not _flow_enabled(flow):
        return

    try:
        payload = _safe_payload(data)
        envelope = {
            "notification_id": str(uuid.uuid4()),
            "event_type": event_type,
            "flow": flow,
            "entity_id": data.get("device_id") or data.get("item_id"),
            "entity_type": "device" if flow >= 4 else "message",
            "occurred_at": time.strftime("%Y-%m-%d %H:%M:%S"),
            "node": NODE_NAME,
            "message_id": data.get("message_id", ""),
        }
        if payload is not None:
            envelope["payload"] = payload
        else:
            envelope["payload_bytes"] = len(json.dumps(
                {k: v for k, v in data.items() if k != "history"}
            ).encode())

        channel.basic_publish(
            exchange=NOTIFICATIONS_EXCHANGE,
            routing_key="",
            mandatory=False,
            body=json.dumps(envelope),
            properties=pika.BasicProperties(
                delivery_mode=pika.DeliveryMode.Persistent,
                content_type="application/json",
            ),
        )
        log.debug(
            f"[notify] flow={flow} event={event_type} "
            f"entity={envelope.get('entity_id')} msg={envelope['message_id']}"
        )
    except Exception as e:
        log.warning(f"[notify] flow={flow} event={event_type} publish failed — {type(e).__name__}: {e}")
