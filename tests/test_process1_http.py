"""HTTP behaviour of the real Process 1 handler (request limits, generic errors, admin gate).
The broker, ZooKeeper and Swift are stubbed (tests/_p1_harness.py); this does NOT prove anything about RabbitMQ."""
import http.client
import json
import os
import subprocess
import sys
import time

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _start(extra_env=None):
    env = dict(os.environ, ALLOW_DEFAULT_SECRETS="true", LOG_LEVEL="WARNING", LOG_DIR=os.devnull + "-none",
               API_MAX_BODY_BYTES="2000", API_MAX_BATCH_COUNT="5", API_REQUEST_TIMEOUT_S="2",
               TLS_API_ENABLED="false", ADMIN_ALLOWED_CNS="", PYTHONPATH=ROOT)
    env.update(extra_env or {})
    proc = subprocess.Popen([sys.executable, "-W", "ignore", os.path.join(ROOT, "tests", "_p1_harness.py")],
                            stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True, env=env, cwd=ROOT)
    deadline = time.time() + 20
    while time.time() < deadline:
        line = proc.stdout.readline()
        if line.startswith("PORT "):
            return proc, int(line.split()[1])
        if proc.poll() is not None:
            break
    proc.kill()
    raise RuntimeError("harness did not start")


@pytest.fixture(scope="module")
def server():
    proc, port = _start()
    yield port
    proc.kill()
    proc.wait()


def call(port, method, path, body=None, headers=None, raw=None):
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=10)
    data = raw if raw is not None else (json.dumps(body) if body is not None else None)
    hdrs = dict(headers or {})
    if data is not None:
        hdrs.setdefault("Content-Type", "application/json")
    conn.request(method, path, body=data, headers=hdrs)
    resp = conn.getresponse()
    text = resp.read().decode()
    conn.close()
    try:
        return resp.status, json.loads(text)
    except ValueError:
        return resp.status, text


def test_valid_flow_request_is_accepted(server):
    status, body = call(server, "POST", "/api/flow1", {"item_id": 7, "counter": 1})
    assert status == 200 and body["payload"]["item_id"] == 7


def test_body_over_limit_is_413(server):
    status, body = call(server, "POST", "/api/flow1", {"initial_data": "x" * 5000})
    assert status == 413 and body["error"] == "Request body too large"


def test_body_just_under_limit_is_accepted(server):
    status, _ = call(server, "POST", "/api/flow1", {"initial_data": "x" * 1500})
    assert status == 200


@pytest.mark.parametrize("count", [0, -3, 6, 100000000, "x"])
def test_batch_count_out_of_range_is_400(server, count):
    status, _ = call(server, "POST", "/api/batch", {"count": count})
    assert status == 400


def test_batch_within_limit(server):
    status, body = call(server, "POST", "/api/batch", {"count": 2})
    assert status == 200 and len(body["flow1_messages"]) == 2


@pytest.mark.parametrize("payload", [{"item_id": "abc"}, {"counter": "x"}])
def test_non_numeric_field_is_400_not_500(server, payload):
    assert call(server, "POST", "/api/flow1", payload)[0] == 400


def test_non_object_json_is_400(server):
    assert call(server, "POST", "/api/flow1", raw="[1,2,3]")[0] == 400


def test_invalid_json_is_400(server):
    assert call(server, "POST", "/api/flow1", raw="{nope")[0] == 400


def test_bad_content_length_is_400(server):
    assert call(server, "POST", "/api/flow1", raw="{}", headers={"Content-Length": "abc"})[0] in (400,)


def test_unknown_path_does_not_echo_it(server):
    status, body = call(server, "GET", "/secret/internal-path")
    assert status == 404 and "internal-path" not in json.dumps(body)


def test_admin_endpoint_refused_without_tls(server):
    status, body = call(server, "POST", "/zk/config", {"key": "flow1_counter_step", "value": 5})
    assert status == 403 and body == {"error": "Forbidden"}


def test_error_body_is_generic_with_request_id():
    proc, port = _start({"HARNESS_BROKER_DOWN": "1"})
    try:
        status, body = call(port, "POST", "/api/flow1", {"item_id": 1})
        assert status == 500
        assert body["error"] == "Internal Server Error" and len(body["request_id"]) == 12
        assert "secret-host" not in json.dumps(body) and "details" not in body
    finally:
        proc.kill()
        proc.wait()


def test_error_details_only_when_explicitly_enabled():
    proc, port = _start({"HARNESS_BROKER_DOWN": "1", "API_EXPOSE_ERROR_DETAILS": "true"})
    try:
        status, body = call(port, "POST", "/api/flow1", {"item_id": 1})
        assert status == 500 and "secret-host" in body["details"]
    finally:
        proc.kill()
        proc.wait()


def test_admin_allowed_only_in_explicit_dev_mode():
    proc, port = _start({"ALLOW_UNAUTHENTICATED_ADMIN": "true"})
    try:
        assert call(port, "POST", "/zk/config", {"key": "flow1_counter_step", "value": 5})[0] == 200
    finally:
        proc.kill()
        proc.wait()


# --- /health is liveness only; /ready and /zk/health carry dependency state -------------------------------------

def test_health_is_local_and_does_not_touch_zookeeper():
    proc, port = _start({"HARNESS_ZK_DEAD": "1"})          # every ZooKeeper call would hang 20 s
    try:
        t0 = time.time()
        status, body = call(port, "GET", "/health")
        assert status == 200 and body["status"] == "healthy" and body["node"]
        assert time.time() - t0 < 3, "/health waited for ZooKeeper"
        assert "zk_registered_workers" not in body and "zk_config" not in body
        assert "GET /ready" in body["endpoints"]
    finally:
        proc.kill()
        proc.wait()


def test_health_still_reports_greenthreads_and_metrics(server):
    status, body = call(server, "GET", "/health")
    assert status == 200 and "greenthreads" in body and "metrics" in body


def test_ready_is_503_without_a_broker_connection(server):
    status, body = call(server, "GET", "/ready")
    assert status == 503 and body["status"] == "not_ready"
    assert body["checks"]["rabbitmq_publisher_connections"]["open"] == 0


def test_ready_is_200_with_a_broker_connection_even_if_zookeeper_is_dead():
    proc, port = _start({"HARNESS_RMQ_OPEN": "1", "HARNESS_ZK_DEAD": "1"})
    try:
        t0 = time.time()
        status, body = call(port, "GET", "/ready")
        assert status == 200 and body["status"] == "ready"
        assert body["checks"]["zookeeper"]["required"] is False
        assert body["checks"]["zookeeper"]["state"] == "not_started"
        assert time.time() - t0 < 3, "/ready waited for ZooKeeper"
        assert "secret-zk-host" not in json.dumps(body)
    finally:
        proc.kill()
        proc.wait()


def test_zk_health_includes_registered_workers(server):
    status, body = call(server, "GET", "/zk/health")
    assert status == 200 and set(body["registered_workers"]) == {f"process{i}" for i in range(1, 6)}
    assert "pipeline_health" in body
