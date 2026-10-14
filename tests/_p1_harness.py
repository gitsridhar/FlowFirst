"""Test harness (not a test): serves the REAL Process 1 request handler with the broker, ZooKeeper and Swift stubbed.

Run as a subprocess by test_process1_http.py, because importing process1 calls gt.monkey_patch(), which must not
happen inside pytest. Prints 'PORT <n>' once listening. Env knobs come from the parent test.
"""
import os
import sys

# eventlet 0.40.x on Python 3.9 + macOS crashes in its kqueue hub (TypeError in kevent). Linux uses epoll and is not
# affected, so only developers on macOS need this. It must be set before eventlet is imported.
if sys.platform == "darwin":
    os.environ.setdefault("EVENTLET_HUB", "selects")

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.application import process1 as p1                      # noqa: E402  (monkey_patch runs inside this import)
from http.server import HTTPServer                              # noqa: E402

PUBLISHED = []


def fake_publish(routing_key, body):
    if os.environ.get("HARNESS_BROKER_DOWN") == "1":
        raise ConnectionError("amqp://secret-host:5672 unreachable")      # text that must never reach a client
    PUBLISHED.append(routing_key)


p1._publish = fake_publish
p1.zklib.write_config = lambda key, value: key == "flow1_counter_step"
p1.zklib.list_registered = lambda name: []
p1.zklib.get_pipeline_health = lambda: {}

if os.environ.get("HARNESS_ZK_DEAD") == "1":
    # ZooKeeper unreachable: every call that would touch it hangs for 20 s and then fails.
    import time as _time

    def _zk_hangs(*a, **k):
        _time.sleep(20)
        raise ConnectionError("zk://secret-zk-host:2181 unreachable")
    p1.zklib.list_registered = _zk_hangs
    p1.zklib.get_pipeline_health = _zk_hangs
    p1.zklib.get_client = _zk_hangs

if os.environ.get("HARNESS_RMQ_OPEN") == "1":
    class _FakeConn:
        is_closed = False
    p1._publish_slots[0].conn = _FakeConn()

server = HTTPServer(("127.0.0.1", 0), p1.RestApiHandler)
print("PORT %d" % server.server_address[1], flush=True)
server.serve_forever()
