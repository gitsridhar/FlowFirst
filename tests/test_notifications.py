"""Unit tests for src/notifications/notifications.py.

All tests run without a real RabbitMQ connection. Pika channels are replaced
with MagicMock objects; config module attributes are patched via monkeypatch.
No gt.monkey_patch() and no subprocesses — the module has no import-time side effects.
"""
import importlib
import json
from unittest.mock import MagicMock, call

import pytest

from src.notifications import notifications as notif


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _mock_ch():
    """Return a fresh MagicMock that stands in for a pika channel."""
    return MagicMock()


def _reload(monkeypatch, **cfg):
    """Patch config attributes on the notifications module and reload it so the
    module-level constants (_ENABLED_FLOWS etc.) are recomputed from the new values."""
    from src.config import config
    for key, value in cfg.items():
        monkeypatch.setattr(config, key, value)
        monkeypatch.setattr(notif, key, value)
    # Re-parse _ENABLED_FLOWS from the patched NOTIFICATIONS_FLOWS value
    flows_raw = cfg.get("NOTIFICATIONS_FLOWS", notif.NOTIFICATIONS_FLOWS)
    monkeypatch.setattr(notif, "_ENABLED_FLOWS", notif._parse_flows(flows_raw))


# ---------------------------------------------------------------------------
# Test 1 — notify() is a no-op when NOTIFICATIONS_ENABLED is False
# ---------------------------------------------------------------------------

def test_notify_disabled_when_not_enabled(monkeypatch):
    _reload(monkeypatch, NOTIFICATIONS_ENABLED=False)
    ch = _mock_ch()
    notif.notify(ch, "device.created", {"message_id": "abc", "device_id": "d1"}, flow=4)
    ch.basic_publish.assert_not_called()


# ---------------------------------------------------------------------------
# Test 2 — notify() builds a correct envelope and calls basic_publish once
# ---------------------------------------------------------------------------

def test_notify_builds_correct_envelope(monkeypatch):
    _reload(monkeypatch,
            NOTIFICATIONS_ENABLED=True,
            NOTIFICATIONS_EXCHANGE="flowfirst.notifications",
            NOTIFICATIONS_FLOWS="",
            NOTIFICATIONS_MAX_PAYLOAD_BYTES=0,   # always include payload
            NODE_NAME="node1")
    ch = _mock_ch()
    data = {
        "message_id": "msg-xyz",
        "device_id": "dev-42",
        "device_name": "sensor-a",
        "flow": 4,
        "history": [{"stage": "p1", "timestamp": "2025-01-01"}],
    }
    notif.notify(ch, "device.created", data, flow=4)

    ch.basic_publish.assert_called_once()
    _, kwargs = ch.basic_publish.call_args
    assert kwargs["exchange"] == "flowfirst.notifications"
    assert kwargs["mandatory"] is False

    envelope = json.loads(kwargs["body"])
    assert envelope["event_type"] == "device.created"
    assert envelope["flow"] == 4
    assert envelope["entity_id"] == "dev-42"
    assert envelope["entity_type"] == "device"
    assert envelope["message_id"] == "msg-xyz"
    assert envelope["node"] == "node1"
    assert "notification_id" in envelope
    assert "occurred_at" in envelope


# ---------------------------------------------------------------------------
# Test 3 — notify() never raises even when basic_publish throws
# ---------------------------------------------------------------------------

def test_notify_never_raises_on_publish_failure(monkeypatch):
    _reload(monkeypatch,
            NOTIFICATIONS_ENABLED=True,
            NOTIFICATIONS_EXCHANGE="flowfirst.notifications",
            NOTIFICATIONS_FLOWS="",
            NOTIFICATIONS_MAX_PAYLOAD_BYTES=0)
    ch = _mock_ch()
    ch.basic_publish.side_effect = RuntimeError("broker gone")
    # Must not raise — notification failures are logged and swallowed
    notif.notify(ch, "device.created", {"message_id": "m1", "device_id": "d1"}, flow=4)


# ---------------------------------------------------------------------------
# Test 4 — payload is omitted (replaced by payload_bytes) when over the limit
# ---------------------------------------------------------------------------

def test_payload_omitted_when_over_limit(monkeypatch):
    _reload(monkeypatch,
            NOTIFICATIONS_ENABLED=True,
            NOTIFICATIONS_EXCHANGE="flowfirst.notifications",
            NOTIFICATIONS_FLOWS="",
            NOTIFICATIONS_MAX_PAYLOAD_BYTES=10)   # tiny limit
    ch = _mock_ch()
    data = {
        "message_id": "m1",
        "device_id": "dev-1",
        "device_name": "a-very-long-sensor-name-that-exceeds-ten-bytes",
    }
    notif.notify(ch, "device.created", data, flow=4)

    ch.basic_publish.assert_called_once()
    _, kwargs = ch.basic_publish.call_args
    envelope = json.loads(kwargs["body"])
    assert "payload" not in envelope
    assert "payload_bytes" in envelope
    assert isinstance(envelope["payload_bytes"], int)
    assert envelope["payload_bytes"] > 0


# ---------------------------------------------------------------------------
# Test 5 — history is stripped from the payload field regardless of size
# ---------------------------------------------------------------------------

def test_payload_strips_history(monkeypatch):
    _reload(monkeypatch,
            NOTIFICATIONS_ENABLED=True,
            NOTIFICATIONS_EXCHANGE="flowfirst.notifications",
            NOTIFICATIONS_FLOWS="",
            NOTIFICATIONS_MAX_PAYLOAD_BYTES=0)   # no size limit
    ch = _mock_ch()
    data = {
        "message_id": "m1",
        "item_id": 99,
        "counter": 500,
        "history": [
            {"stage": "p1", "status": "prepared"},
            {"stage": "p2", "status": "reflected"},
            {"stage": "p4", "status": "persisted"},
        ],
    }
    notif.notify(ch, "telemetry.processed", data, flow=1)

    _, kwargs = ch.basic_publish.call_args
    envelope = json.loads(kwargs["body"])
    assert "payload" in envelope
    assert "history" not in envelope["payload"]


# ---------------------------------------------------------------------------
# Test 6 — declare_exchange() calls exchange_declare with correct arguments
# ---------------------------------------------------------------------------

def test_declare_exchange_called_with_correct_args(monkeypatch):
    _reload(monkeypatch,
            NOTIFICATIONS_ENABLED=True,
            NOTIFICATIONS_EXCHANGE="flowfirst.notifications")
    ch = _mock_ch()
    notif.declare_exchange(ch)
    ch.exchange_declare.assert_called_once_with(
        exchange="flowfirst.notifications",
        exchange_type="fanout",
        durable=True,
    )


def test_declare_exchange_no_op_when_disabled(monkeypatch):
    _reload(monkeypatch, NOTIFICATIONS_ENABLED=False)
    ch = _mock_ch()
    notif.declare_exchange(ch)
    ch.exchange_declare.assert_not_called()


# ---------------------------------------------------------------------------
# Test 7 — NOTIFICATIONS_FLOWS filter: only listed flows publish
# ---------------------------------------------------------------------------

def test_notify_flow_filter_respected(monkeypatch):
    _reload(monkeypatch,
            NOTIFICATIONS_ENABLED=True,
            NOTIFICATIONS_EXCHANGE="flowfirst.notifications",
            NOTIFICATIONS_FLOWS="4,7",
            NOTIFICATIONS_MAX_PAYLOAD_BYTES=0)
    ch = _mock_ch()

    # Flow 1 — must NOT publish
    notif.notify(ch, "telemetry.processed", {"message_id": "m1", "item_id": 1}, flow=1)
    ch.basic_publish.assert_not_called()

    # Flow 4 — MUST publish
    notif.notify(ch, "device.created", {"message_id": "m2", "device_id": "d1"}, flow=4)
    ch.basic_publish.assert_called_once()

    ch.reset_mock()

    # Flow 7 — MUST publish
    notif.notify(ch, "device.decommissioned", {"message_id": "m3", "device_id": "d2"}, flow=7)
    ch.basic_publish.assert_called_once()

    ch.reset_mock()

    # Flow 2 — must NOT publish (not in filter)
    notif.notify(ch, "metric.processed", {"message_id": "m4", "item_id": 2}, flow=2)
    ch.basic_publish.assert_not_called()


# ---------------------------------------------------------------------------
# Test 8 — _parse_flows handles blank, "*", and comma-separated values
# ---------------------------------------------------------------------------

def test_parse_flows_blank_means_all():
    assert notif._parse_flows("") == frozenset()
    assert notif._parse_flows("  ") == frozenset()
    assert notif._parse_flows("*") == frozenset()


def test_parse_flows_comma_list():
    result = notif._parse_flows("1,4,7")
    assert result == frozenset({1, 4, 7})


def test_parse_flows_ignores_non_digits():
    # Non-digit tokens are skipped; valid ones are kept
    result = notif._parse_flows("4, 7, abc, 6")
    assert result == frozenset({4, 6, 7})


# ---------------------------------------------------------------------------
# Test 9 — entity_type is "message" for flows 1-3, "device" for flows 4-7
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("flow,expected_type", [
    (1, "message"), (2, "message"), (3, "message"),
    (4, "device"),  (5, "device"),  (6, "device"),  (7, "device"),
])
def test_entity_type_matches_flow(monkeypatch, flow, expected_type):
    _reload(monkeypatch,
            NOTIFICATIONS_ENABLED=True,
            NOTIFICATIONS_EXCHANGE="flowfirst.notifications",
            NOTIFICATIONS_FLOWS="",
            NOTIFICATIONS_MAX_PAYLOAD_BYTES=0)
    ch = _mock_ch()
    data = {"message_id": "m1", "item_id": 1, "device_id": "d1"}
    notif.notify(ch, "some.event", data, flow=flow)
    _, kwargs = ch.basic_publish.call_args
    envelope = json.loads(kwargs["body"])
    assert envelope["entity_type"] == expected_type


# ---------------------------------------------------------------------------
# Test 10 — envelope is valid JSON and all required keys are present
# ---------------------------------------------------------------------------

def test_envelope_contains_all_required_keys(monkeypatch):
    _reload(monkeypatch,
            NOTIFICATIONS_ENABLED=True,
            NOTIFICATIONS_EXCHANGE="flowfirst.notifications",
            NOTIFICATIONS_FLOWS="",
            NOTIFICATIONS_MAX_PAYLOAD_BYTES=0)
    ch = _mock_ch()
    notif.notify(ch, "device.created", {"message_id": "m1", "device_id": "d1"}, flow=4)
    _, kwargs = ch.basic_publish.call_args
    envelope = json.loads(kwargs["body"])
    required = {"notification_id", "event_type", "flow", "entity_id",
                "entity_type", "occurred_at", "node", "message_id"}
    assert required.issubset(envelope.keys())
