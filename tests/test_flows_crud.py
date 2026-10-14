"""Tests for IoT device CRUD flows (4-7).

Covers:
  - Queue constants (config.py)
  - db.py: upsert_device, soft_delete_device, query_devices (mocked DB)
  - process2.py: handle_flow4/5/6/7 handler logic
  - process3.py: handle_flow4/5/6/7 handler logic including Flow 6 dual-publish
  - process3.py: validation rejection (invalid device_type, missing device_id)
  - process5.py: handle_flow6_message (edge-sync token)
  - authz.py: _BUILTIN_ROLES includes flow:4-7:publish for publisher and operator
"""
import json
import hashlib
import time
import types
from unittest.mock import MagicMock, patch, call


# ---------------------------------------------------------------------------
# Queue constants
# ---------------------------------------------------------------------------

def test_flow4_queue_constants():
    from src.config.config import (
        QUEUE_FLOW4_P1_TO_P2, QUEUE_FLOW4_P2_TO_P3, QUEUE_FLOW4_P3_TO_P4,
    )
    assert QUEUE_FLOW4_P1_TO_P2 == "flow4_p1_to_p2"
    assert QUEUE_FLOW4_P2_TO_P3 == "flow4_p2_to_p3"
    assert QUEUE_FLOW4_P3_TO_P4 == "flow4_p3_to_p4"


def test_flow5_queue_constants():
    from src.config.config import (
        QUEUE_FLOW5_P1_TO_P2, QUEUE_FLOW5_P2_TO_P3, QUEUE_FLOW5_P3_TO_P4,
    )
    assert QUEUE_FLOW5_P1_TO_P2 == "flow5_p1_to_p2"
    assert QUEUE_FLOW5_P2_TO_P3 == "flow5_p2_to_p3"
    assert QUEUE_FLOW5_P3_TO_P4 == "flow5_p3_to_p4"


def test_flow6_queue_constants():
    from src.config.config import (
        QUEUE_FLOW6_P1_TO_P2, QUEUE_FLOW6_P2_TO_P3, QUEUE_FLOW6_P3_TO_P4,
        QUEUE_FLOW6_P3_TO_P5, QUEUE_FLOW6_P5_TO_P2, QUEUE_FLOW6_P2_TO_P4,
    )
    assert QUEUE_FLOW6_P1_TO_P2 == "flow6_p1_to_p2"
    assert QUEUE_FLOW6_P2_TO_P3 == "flow6_p2_to_p3"
    assert QUEUE_FLOW6_P3_TO_P4 == "flow6_p3_to_p4"
    assert QUEUE_FLOW6_P3_TO_P5 == "flow6_p3_to_p5"
    assert QUEUE_FLOW6_P5_TO_P2 == "flow6_p5_to_p2"
    assert QUEUE_FLOW6_P2_TO_P4 == "flow6_p2_to_p4"


def test_flow7_queue_constants():
    from src.config.config import (
        QUEUE_FLOW7_P1_TO_P2, QUEUE_FLOW7_P2_TO_P3, QUEUE_FLOW7_P3_TO_P4,
    )
    assert QUEUE_FLOW7_P1_TO_P2 == "flow7_p1_to_p2"
    assert QUEUE_FLOW7_P2_TO_P3 == "flow7_p2_to_p3"
    assert QUEUE_FLOW7_P3_TO_P4 == "flow7_p3_to_p4"


def test_all_queues_contains_15_new_queues():
    from src.config.config import ALL_QUEUES
    new_queues = [q for q in ALL_QUEUES if q.startswith(("flow4_", "flow5_", "flow6_", "flow7_"))]
    assert len(new_queues) == 15


# ---------------------------------------------------------------------------
# authz._BUILTIN_ROLES — flow:4-7 permissions
# ---------------------------------------------------------------------------

def test_publisher_role_has_flow4_7():
    from src.application.authz import _BUILTIN_ROLES
    publisher = _BUILTIN_ROLES["publisher"]
    for flow_n in (4, 5, 6, 7):
        assert f"flow:{flow_n}:publish" in publisher, \
            f"publisher role missing flow:{flow_n}:publish"


def test_operator_role_has_flow4_7():
    from src.application.authz import _BUILTIN_ROLES
    operator = _BUILTIN_ROLES["operator"]
    for flow_n in (4, 5, 6, 7):
        assert f"flow:{flow_n}:publish" in operator, \
            f"operator role missing flow:{flow_n}:publish"


def test_reader_and_admin_roles_unchanged():
    """reader and admin should NOT have flow publish permissions."""
    from src.application.authz import _BUILTIN_ROLES
    for role in ("reader", "admin"):
        for flow_n in (1, 2, 3, 4, 5, 6, 7):
            assert f"flow:{flow_n}:publish" not in _BUILTIN_ROLES[role], \
                f"{role} should not have flow:{flow_n}:publish"


def test_builtin_roles_check_authz_flow4_publisher():
    """An identity with the 'publisher' role should be allowed to publish to flow:4."""
    from src.application import authz
    roles, id_roles = authz.parse_roles("", "alice=publisher")
    ok, reason = authz.check_authz("rbac", "alice", "publish", "flow:4", "POST",
                                   roles, id_roles, {}, [])
    assert ok is True, f"Expected allow, got: {reason}"


def test_builtin_roles_check_authz_flow7_operator():
    from src.application import authz
    roles, id_roles = authz.parse_roles("", "ops=operator")
    ok, reason = authz.check_authz("rbac", "ops", "publish", "flow:7", "POST",
                                   roles, id_roles, {}, [])
    assert ok is True, f"Expected allow, got: {reason}"


def test_device_writer_custom_role():
    """Custom device_writer role should grant flows 4, 6, 7 but NOT 5."""
    from src.application import authz
    roles, id_roles = authz.parse_roles(
        "device_writer=flow:4:publish,flow:6:publish,flow:7:publish",
        "ci_bot=device_writer",
    )
    for flow_n in (4, 6, 7):
        ok, _ = authz.check_authz("rbac", "ci_bot", "publish", f"flow:{flow_n}", "POST",
                                  roles, id_roles, {}, [])
        assert ok is True, f"device_writer should have flow:{flow_n}:publish"
    # flow:5 should be denied
    ok, _ = authz.check_authz("rbac", "ci_bot", "publish", "flow:5", "POST",
                              roles, id_roles, {}, [])
    assert ok is False


def test_device_reader_custom_role():
    """device_reader role should grant only flow:5."""
    from src.application import authz
    roles, id_roles = authz.parse_roles(
        "device_reader=flow:5:publish",
        "monitor_svc=device_reader",
    )
    ok, _ = authz.check_authz("rbac", "monitor_svc", "publish", "flow:5", "POST",
                              roles, id_roles, {}, [])
    assert ok is True
    ok, _ = authz.check_authz("rbac", "monitor_svc", "publish", "flow:4", "POST",
                              roles, id_roles, {}, [])
    assert ok is False


# ---------------------------------------------------------------------------
# db.py — upsert_device, soft_delete_device, query_devices (mocked DB)
# ---------------------------------------------------------------------------

def _make_cursor(rowcount=1, lastrowid=42, fetchall_result=None):
    cursor = MagicMock()
    cursor.__enter__ = lambda s: s
    cursor.__exit__ = MagicMock(return_value=False)
    cursor.rowcount = rowcount
    cursor.lastrowid = lastrowid
    cursor.fetchall = MagicMock(return_value=fetchall_result or [])
    return cursor


def _make_conn(cursor):
    conn = MagicMock()
    conn.cursor = MagicMock(return_value=cursor)
    return conn


def test_upsert_device_calls_insert():
    from src.db import db
    cursor = _make_cursor(lastrowid=1)
    conn = _make_conn(cursor)

    with patch.object(db, "get_db_connection", return_value=conn):
        result = db.upsert_device({
            "device_id": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
            "device_name": "Temp Sensor 1",
            "device_type": "temperature_sensor",
            "location": "factory/zone-A",
            "firmware_version": "2.0.1",
            "tags": {"env": "production"},
            "status": "active",
            "owner_team": "ops",
        })
    assert result == 1
    cursor.execute.assert_called_once()
    sql = cursor.execute.call_args[0][0]
    assert "INSERT INTO devices" in sql
    assert "ON DUPLICATE KEY UPDATE" in sql


def test_upsert_device_serializes_tags():
    """Tags dict should be JSON-serialised before being passed to the cursor."""
    from src.db import db
    cursor = _make_cursor(lastrowid=5)
    conn = _make_conn(cursor)

    tags = {"zone": "alpha", "criticality": "high"}
    with patch.object(db, "get_db_connection", return_value=conn):
        db.upsert_device({
            "device_id": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
            "device_name": "Sensor",
            "device_type": "pressure_gauge",
            "tags": tags,
        })
    call_params = cursor.execute.call_args[0][1]
    # Tags are at index 5 in the param tuple
    assert call_params[5] == json.dumps(tags)


def test_upsert_device_none_tags():
    """None tags should be passed as None, not as the string 'null'."""
    from src.db import db
    cursor = _make_cursor(lastrowid=3)
    conn = _make_conn(cursor)

    with patch.object(db, "get_db_connection", return_value=conn):
        db.upsert_device({
            "device_id": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
            "device_name": "Sensor",
            "device_type": "flow_meter",
            "tags": None,
        })
    call_params = cursor.execute.call_args[0][1]
    assert call_params[5] is None


def test_soft_delete_device():
    from src.db import db
    cursor = _make_cursor(rowcount=1)
    conn = _make_conn(cursor)

    with patch.object(db, "get_db_connection", return_value=conn):
        rows = db.soft_delete_device("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
    assert rows == 1
    sql = cursor.execute.call_args[0][0]
    assert "UPDATE devices" in sql
    assert "decommissioned" in sql


def test_soft_delete_device_not_found():
    from src.db import db
    cursor = _make_cursor(rowcount=0)
    conn = _make_conn(cursor)

    with patch.object(db, "get_db_connection", return_value=conn):
        rows = db.soft_delete_device("does-not-exist-uuid")
    assert rows == 0


def test_query_devices_no_filters():
    from src.db import db
    rows = [{"device_id": "x", "device_name": "Sensor A"}]
    cursor = _make_cursor(fetchall_result=rows)
    conn = _make_conn(cursor)

    with patch.object(db, "get_db_connection", return_value=conn):
        result = db.query_devices({})
    assert result == rows
    sql = cursor.execute.call_args[0][0]
    assert "WHERE" not in sql


def test_query_devices_with_filters():
    from src.db import db
    cursor = _make_cursor(fetchall_result=[])
    conn = _make_conn(cursor)

    with patch.object(db, "get_db_connection", return_value=conn):
        db.query_devices({"device_type": "temperature_sensor", "status": "active"})
    sql, params = cursor.execute.call_args[0]
    assert "WHERE" in sql
    assert "device_type = %s" in sql
    assert "status = %s" in sql
    assert "temperature_sensor" in params
    assert "active" in params


def test_query_devices_location_contains():
    from src.db import db
    cursor = _make_cursor(fetchall_result=[])
    conn = _make_conn(cursor)

    with patch.object(db, "get_db_connection", return_value=conn):
        db.query_devices({"location_contains": "factory"})
    sql, params = cursor.execute.call_args[0]
    assert "LIKE %s" in sql
    assert "%factory%" in params


# ---------------------------------------------------------------------------
# process2.py handlers — unit tests without RabbitMQ
# ---------------------------------------------------------------------------

def _make_method(tag=1):
    m = MagicMock()
    m.delivery_tag = tag
    return m


def _body(data):
    return json.dumps(data).encode()


def _base_data(flow, extra=None):
    d = {
        "flow": flow,
        "message_id": "test-msg-id-0001",
        "history": [],
    }
    if extra:
        d.update(extra)
    return d


def test_p2_handle_flow4_message_enriches():
    """Process 2 Flow 4 handler should add device_fingerprint and enriched_at."""
    import src.application.process2 as p2

    ch = MagicMock()
    method = _make_method()
    data = _base_data(4, {
        "device_name": "Sensor X",
        "device_type": "temperature_sensor",
        "location": "plant/A",
    })

    p2.handle_flow4_message(ch, method, None, _body(data))

    ch.basic_ack.assert_called_once_with(delivery_tag=1)
    publish_call = ch.basic_publish.call_args
    published_data = json.loads(publish_call.kwargs["body"])
    assert "device_fingerprint" in published_data
    assert len(published_data["device_fingerprint"]) == 64  # SHA-256 hex
    assert "enriched_at" in published_data
    assert publish_call.kwargs["routing_key"] == "flow4_p2_to_p3"


def test_p2_handle_flow4_fingerprint_value():
    """Fingerprint should be sha256(device_name:device_type:location)."""
    import src.application.process2 as p2

    ch = MagicMock()
    method = _make_method()
    data = _base_data(4, {
        "device_name": "Alpha",
        "device_type": "flow_meter",
        "location": "zone-1",
    })
    p2.handle_flow4_message(ch, method, None, _body(data))
    published_data = json.loads(ch.basic_publish.call_args.kwargs["body"])
    expected = hashlib.sha256("Alpha:flow_meter:zone-1".encode()).hexdigest()
    assert published_data["device_fingerprint"] == expected


def test_p2_handle_flow5_message_normalizes_filters():
    """Process 2 Flow 5 handler should lowercase device_type and status filters."""
    import src.application.process2 as p2

    ch = MagicMock()
    method = _make_method()
    data = _base_data(5, {"filters": {"device_type": "TEMPERATURE_SENSOR", "status": "ACTIVE"}})

    p2.handle_flow5_message(ch, method, None, _body(data))

    ch.basic_ack.assert_called_once_with(delivery_tag=1)
    published_data = json.loads(ch.basic_publish.call_args.kwargs["body"])
    assert published_data["filters"]["device_type"] == "temperature_sensor"
    assert published_data["filters"]["status"] == "active"
    assert published_data["routing_key"] if False else True  # just check routing key below
    assert ch.basic_publish.call_args.kwargs["routing_key"] == "flow5_p2_to_p3"


def test_p2_handle_flow6_message_adds_sequence():
    """Process 2 Flow 6 handler should add update_sequence and previous_state_tracked."""
    import src.application.process2 as p2

    ch = MagicMock()
    method = _make_method()
    data = _base_data(6, {"device_id": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee", "updates": {}})

    # _config_watcher is None in test context — handler falls back to int(time.time()*1000)
    p2.handle_flow6_message(ch, method, None, _body(data))

    ch.basic_ack.assert_called_once_with(delivery_tag=1)
    published_data = json.loads(ch.basic_publish.call_args.kwargs["body"])
    assert "update_sequence" in published_data
    assert published_data["previous_state_tracked"] is True
    assert ch.basic_publish.call_args.kwargs["routing_key"] == "flow6_p2_to_p3"


def test_p2_handle_flow7_message_marks_deletion():
    """Process 2 Flow 7 handler should add deletion_requested_by and grace_period_s."""
    import src.application.process2 as p2

    ch = MagicMock()
    method = _make_method()
    data = _base_data(7, {"device_id": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"})

    p2.handle_flow7_message(ch, method, None, _body(data))

    ch.basic_ack.assert_called_once_with(delivery_tag=1)
    published_data = json.loads(ch.basic_publish.call_args.kwargs["body"])
    assert "deletion_requested_by" in published_data
    assert "grace_period_s" in published_data
    assert ch.basic_publish.call_args.kwargs["routing_key"] == "flow7_p2_to_p3"


def test_p2_handle_flow6_remote_return_forwards_to_p4():
    """Process 2 Flow 6 remote return handler should forward to flow6_p2_to_p4."""
    import src.application.process2 as p2

    ch = MagicMock()
    method = _make_method()
    data = _base_data(6, {"device_id": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee", "remote_node": "node4"})

    p2.handle_flow6_remote_return_message(ch, method, None, _body(data))

    ch.basic_ack.assert_called_once_with(delivery_tag=1)
    assert ch.basic_publish.call_args.kwargs["routing_key"] == "flow6_p2_to_p4"
    published_data = json.loads(ch.basic_publish.call_args.kwargs["body"])
    assert published_data["process2_remote_return_verified"] is True


# ---------------------------------------------------------------------------
# process2.py handler error paths — nack on exception
# ---------------------------------------------------------------------------

def test_p2_flow4_handler_nacks_on_exception():
    import src.application.process2 as p2

    ch = MagicMock()
    method = _make_method()
    p2.handle_flow4_message(ch, method, None, b"not valid json")

    ch.basic_nack.assert_called_once_with(delivery_tag=1, requeue=False)
    ch.basic_ack.assert_not_called()


# ---------------------------------------------------------------------------
# process3.py handlers — validation tests
# ---------------------------------------------------------------------------

def test_p3_flow4_valid_device():
    """Process 3 Flow 4 validates device_name and device_type and forwards."""
    import src.application.process3 as p3

    ch = MagicMock()
    method = _make_method()
    data = _base_data(4, {
        "device_name": "Pressure Sensor A",
        "device_type": "pressure_gauge",
    })

    p3.handle_flow4_message(ch, method, None, _body(data))

    ch.basic_ack.assert_called_once_with(delivery_tag=1)
    published_data = json.loads(ch.basic_publish.call_args.kwargs["body"])
    assert published_data["validation_passed"] is True
    assert ch.basic_publish.call_args.kwargs["routing_key"] == "flow4_p3_to_p4"


def test_p3_flow4_rejects_empty_device_name():
    """Process 3 Flow 4 should nack when device_name is empty."""
    import src.application.process3 as p3

    ch = MagicMock()
    method = _make_method()
    data = _base_data(4, {"device_name": "", "device_type": "flow_meter"})

    p3.handle_flow4_message(ch, method, None, _body(data))

    ch.basic_nack.assert_called_once_with(delivery_tag=1, requeue=False)
    ch.basic_ack.assert_not_called()
    ch.basic_publish.assert_not_called()


def test_p3_flow4_rejects_invalid_device_type():
    """Process 3 Flow 4 should nack when device_type is not in the allowed set."""
    import src.application.process3 as p3

    ch = MagicMock()
    method = _make_method()
    data = _base_data(4, {"device_name": "My Sensor", "device_type": "laser_cannon"})

    p3.handle_flow4_message(ch, method, None, _body(data))

    ch.basic_nack.assert_called_once_with(delivery_tag=1, requeue=False)
    ch.basic_publish.assert_not_called()


def test_p3_flow4_all_allowed_device_types():
    """All six allowed device types should pass validation."""
    import src.application.process3 as p3

    allowed = [
        "temperature_sensor", "pressure_gauge", "flow_meter",
        "humidity_sensor", "vibration_sensor", "power_monitor",
    ]
    for dtype in allowed:
        ch = MagicMock()
        method = _make_method()
        data = _base_data(4, {"device_name": "Sensor", "device_type": dtype})
        p3.handle_flow4_message(ch, method, None, _body(data))
        ch.basic_ack.assert_called_once_with(delivery_tag=1), f"{dtype} should pass"


def test_p3_flow5_valid_query():
    """Process 3 Flow 5 should add query_validated and forward."""
    import src.application.process3 as p3

    ch = MagicMock()
    method = _make_method()
    data = _base_data(5, {"filters": {"device_type": "temperature_sensor"}})

    p3.handle_flow5_message(ch, method, None, _body(data))

    ch.basic_ack.assert_called_once_with(delivery_tag=1)
    published_data = json.loads(ch.basic_publish.call_args.kwargs["body"])
    assert published_data["query_validated"] is True
    assert ch.basic_publish.call_args.kwargs["routing_key"] == "flow5_p3_to_p4"


def test_p3_flow5_empty_filters_still_valid():
    """Empty filters dict should be accepted (returns all devices)."""
    import src.application.process3 as p3

    ch = MagicMock()
    method = _make_method()
    data = _base_data(5, {"filters": {}})

    p3.handle_flow5_message(ch, method, None, _body(data))

    ch.basic_ack.assert_called_once_with(delivery_tag=1)


def test_p3_flow6_dual_publish():
    """Process 3 Flow 6 should publish to BOTH flow6_p3_to_p4 and flow6_p3_to_p5."""
    import src.application.process3 as p3

    ch = MagicMock()
    method = _make_method()
    # UUID-format device_id (36 chars)
    data = _base_data(6, {"device_id": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee", "updates": {}})

    p3.handle_flow6_message(ch, method, None, _body(data))

    ch.basic_ack.assert_called_once_with(delivery_tag=1)
    assert ch.basic_publish.call_count == 2, "Flow 6 must publish to exactly 2 queues"

    routing_keys = {c.kwargs["routing_key"] for c in ch.basic_publish.call_args_list}
    assert "flow6_p3_to_p4" in routing_keys
    assert "flow6_p3_to_p5" in routing_keys


def test_p3_flow6_rejects_short_device_id():
    """Process 3 Flow 6 should nack when device_id is not 36 characters (not UUID)."""
    import src.application.process3 as p3

    ch = MagicMock()
    method = _make_method()
    data = _base_data(6, {"device_id": "short-id", "updates": {}})

    p3.handle_flow6_message(ch, method, None, _body(data))

    ch.basic_nack.assert_called_once_with(delivery_tag=1, requeue=False)
    ch.basic_publish.assert_not_called()


def test_p3_flow6_rejects_empty_device_id():
    """Process 3 Flow 6 should nack when device_id is empty."""
    import src.application.process3 as p3

    ch = MagicMock()
    method = _make_method()
    data = _base_data(6, {"device_id": "", "updates": {}})

    p3.handle_flow6_message(ch, method, None, _body(data))

    ch.basic_nack.assert_called_once_with(delivery_tag=1, requeue=False)


def test_p3_flow7_valid_delete():
    """Process 3 Flow 7 should add deletion_validated and forward."""
    import src.application.process3 as p3

    ch = MagicMock()
    method = _make_method()
    data = _base_data(7, {"device_id": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"})

    p3.handle_flow7_message(ch, method, None, _body(data))

    ch.basic_ack.assert_called_once_with(delivery_tag=1)
    published_data = json.loads(ch.basic_publish.call_args.kwargs["body"])
    assert published_data["deletion_validated"] is True
    assert ch.basic_publish.call_args.kwargs["routing_key"] == "flow7_p3_to_p4"


def test_p3_flow7_rejects_missing_device_id():
    """Process 3 Flow 7 should nack when device_id is missing."""
    import src.application.process3 as p3

    ch = MagicMock()
    method = _make_method()
    data = _base_data(7, {"device_id": ""})

    p3.handle_flow7_message(ch, method, None, _body(data))

    ch.basic_nack.assert_called_once_with(delivery_tag=1, requeue=False)
    ch.basic_publish.assert_not_called()


# ---------------------------------------------------------------------------
# process5.py — Flow 6 edge-sync handler
# ---------------------------------------------------------------------------

def test_p5_flow6_message_computes_edge_sync_token():
    """Process 5 Flow 6 handler should compute edge_sync_token = sha256(device_id:seq)[:16]."""
    import src.application.process5 as p5

    ch = MagicMock()
    method = _make_method()
    device_id = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
    update_seq = 12345
    data = _base_data(6, {
        "device_id": device_id,
        "update_sequence": update_seq,
        "updates": {},
    })

    p5.handle_flow6_message(ch, method, None, _body(data))

    ch.basic_ack.assert_called_once_with(delivery_tag=1)
    published_data = json.loads(ch.basic_publish.call_args.kwargs["body"])

    expected_token = hashlib.sha256(f"{device_id}:{update_seq}".encode()).hexdigest()[:16]
    assert published_data["edge_sync_token"] == expected_token
    assert published_data["remote_sync_applied"] is True
    assert ch.basic_publish.call_args.kwargs["routing_key"] == "flow6_p5_to_p2"


def test_p5_flow6_message_sets_remote_node():
    import src.application.process5 as p5

    ch = MagicMock()
    method = _make_method()
    data = _base_data(6, {"device_id": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee", "update_sequence": 1})

    p5.handle_flow6_message(ch, method, None, _body(data))

    published_data = json.loads(ch.basic_publish.call_args.kwargs["body"])
    assert "remote_node" in published_data


def test_p5_flow6_message_nacks_on_bad_json():
    import src.application.process5 as p5

    ch = MagicMock()
    method = _make_method()
    p5.handle_flow6_message(ch, method, None, b"bad json {{")

    ch.basic_nack.assert_called_once_with(delivery_tag=1, requeue=False)
    ch.basic_ack.assert_not_called()


# ---------------------------------------------------------------------------
# process3 handler — nack on bad JSON (error path)
# ---------------------------------------------------------------------------

def test_p3_flow4_handler_nacks_on_bad_json():
    import src.application.process3 as p3

    ch = MagicMock()
    method = _make_method()
    p3.handle_flow4_message(ch, method, None, b"not json")

    ch.basic_nack.assert_called_once_with(delivery_tag=1, requeue=False)


def test_p3_flow6_handler_nacks_on_bad_json():
    import src.application.process3 as p3

    ch = MagicMock()
    method = _make_method()
    p3.handle_flow6_message(ch, method, None, b"not json")

    ch.basic_nack.assert_called_once_with(delivery_tag=1, requeue=False)


# ---------------------------------------------------------------------------
# History list entries — correct structure
# ---------------------------------------------------------------------------

def test_p2_flow4_history_entry_structure():
    import src.application.process2 as p2

    ch = MagicMock()
    method = _make_method()
    data = _base_data(4, {
        "device_name": "Sensor",
        "device_type": "humidity_sensor",
        "location": "lab",
    })
    p2.handle_flow4_message(ch, method, None, _body(data))

    published = json.loads(ch.basic_publish.call_args.kwargs["body"])
    last_entry = published["history"][-1]
    assert "stage" in last_entry
    assert "timestamp" in last_entry
    assert "processed_by" in last_entry
    assert "enrichment" in last_entry  # descriptive key


def test_p3_flow4_history_entry_structure():
    import src.application.process3 as p3

    ch = MagicMock()
    method = _make_method()
    data = _base_data(4, {"device_name": "S", "device_type": "vibration_sensor"})
    p3.handle_flow4_message(ch, method, None, _body(data))

    published = json.loads(ch.basic_publish.call_args.kwargs["body"])
    last_entry = published["history"][-1]
    assert "stage" in last_entry
    assert "timestamp" in last_entry
    assert "processed_by" in last_entry
    assert "validation" in last_entry  # descriptive key
