"""scripts/greenthread_demo.py: helpers, CLI parsing, and the stages that need no cluster. No broker needed.

The module only calls gt.monkey_patch() under `if __name__ == "__main__"`, so importing it here never patches pytest.
The consume stage runs against an in-memory fake broker (the real pika connection is replaced).
"""
import importlib.util
import os
import subprocess
import sys
from collections import deque

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

TARGETS = {
    "mgmtnode1": ("10.0.0.1", 5672),
    "mgmtnode2": ("10.0.0.2", 5672),
    "mgmtnode3": ("10.0.0.3", 5672),
}


@pytest.fixture(scope="module")
def demo():
    spec = importlib.util.spec_from_file_location("greenthread_demo", os.path.join(ROOT, "scripts", "greenthread_demo.py"))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture(autouse=True)
def _no_signal_handlers(demo, monkeypatch):
    monkeypatch.setattr(demo.gt, "register_signal_handlers", lambda: None)     # never touch pytest's signal handlers


def test_import_does_not_monkey_patch():
    """Checked in a fresh interpreter: other test modules import process2/3/5, which patch the pytest process itself."""
    code = (
        "import importlib.util, sys, eventlet.patcher as p\n"
        "spec = importlib.util.spec_from_file_location('greenthread_demo', %r)\n"
        "m = importlib.util.module_from_spec(spec); spec.loader.exec_module(m)\n"
        "sys.exit(1 if p.is_monkey_patched('socket') or p.is_monkey_patched('time') else 0)\n"
    ) % os.path.join(ROOT, "scripts", "greenthread_demo.py")
    env = dict(os.environ, ALLOW_DEFAULT_SECRETS="true", LOG_LEVEL="WARNING", PYTHONPATH=ROOT)
    r = subprocess.run([sys.executable, "-W", "ignore", "-c", code], env=env, cwd=ROOT, capture_output=True, timeout=60)
    assert r.returncode == 0, r.stderr.decode()[-500:]


def test_resolve_named_and_literal(demo):
    assert demo.resolve_target("mgmtnode2", TARGETS) == ("mgmtnode2", "10.0.0.2", 5672)
    assert demo.resolve_target("192.168.1.50:5671", TARGETS) == ("192.168.1.50:5671", "192.168.1.50", 5671)


def test_resolve_errors(demo):
    with pytest.raises(ValueError):
        demo.resolve_target("  ", TARGETS)
    with pytest.raises(ValueError):
        demo.resolve_targets(" , ", TARGETS)
    with pytest.raises(ValueError, match="invalid port"):
        demo.resolve_target("10.0.0.1:abc", TARGETS)


def test_resolve_targets_drops_duplicates(demo):
    assert [t[0] for t in demo.resolve_targets("mgmtnode1, mgmtnode2,mgmtnode1", TARGETS)] == ["mgmtnode1", "mgmtnode2"]


def test_expected_interleave(demo):
    assert demo.expected_interleave(("A", "B"), 2, True) == ["A1", "B1", "A2", "B2"]
    assert demo.expected_interleave(("A", "B"), 2, False) == ["A1", "A2", "B1", "B2"]


def test_split_sequence(demo):
    assert demo.split_sequence(5, 2) == [[1, 3, 5], [2, 4]]
    parts = demo.split_sequence(30, 3)
    assert sorted(n for p in parts for n in p) == list(range(1, 31))
    with pytest.raises(ValueError):
        demo.split_sequence(5, 0)


def test_demo_queue_never_matches_pipeline_queues(demo):
    name = demo.demo_queue_name("abc12345")
    assert name == "flowfirst_demo_gt.abc12345.work"
    assert not name.startswith(("flow1_", "flow2_", "flow3_"))
    assert demo.demo_queue_arguments("quorum")["x-queue-type"] == "quorum"
    assert demo.demo_queue_arguments()["x-expires"] == demo.QUEUE_EXPIRES_MS


def test_probe_plan_covers_every_service_on_every_node(demo):
    plan = demo.probe_plan([("mgmtnode1", "10.0.0.1", 5672), ("mgmtnode2", "10.0.0.2", 5672)])
    assert len(plan) == 6
    assert {p[1] for p in plan} == {"rabbitmq", "zookeeper", "mariadb"}
    assert {p[0] for p in plan} == {"mgmtnode1", "mgmtnode2"}


def test_render_table(demo):
    out = demo.render_table([{"a": 1, "b": "xx"}], [("a", "A"), ("b", "Bee")])
    assert out.splitlines()[0].split() == ["A", "Bee"]
    assert out.splitlines()[2].split() == ["1", "xx"]


def test_parser_defaults_to_run_and_validates(demo):
    p = demo.build_parser()
    assert p.parse_args(["run"]).func is demo.cmd_run
    assert p.parse_args(["local", "--tasks", "5"]).tasks == 5
    args = p.parse_args(["consume", "--count", "7", "--queue-type", "quorum"])
    assert (args.count, args.queue_type) == (7, "quorum")
    with pytest.raises(SystemExit):
        p.parse_args(["consume", "--queue-type", "bogus"])


def test_usage_errors_exit_2(demo, capsys):
    assert demo.main(["concurrency", "--tasks", "0"]) == 2
    assert demo.main(["consume", "--count", "-1", "--nodes", "mgmtnode1"]) == 2
    assert "ERROR" in capsys.readouterr().err


def test_interleave_stage(demo):
    ok, rows = demo.stage_interleave(demo.Context())
    assert ok
    assert rows[0]["order"] == "A1 B1 C1 A2 B2 C2 A3 B3 C3"
    assert rows[1]["order"] == "A1 A2 A3 B1 B2 B3 C1 C2 C3"


def test_restart_stage(demo):
    ok, rows = demo.stage_restart(demo.Context())
    assert ok
    assert [r["runs"] for r in rows] == [1, 2]


# ----------------------------------------------------------------------------- consume stage against a fake broker

class _Method:
    def __init__(self, tag):
        self.delivery_tag = tag


class _FakeBroker:
    def __init__(self):
        self.queues = {}
        self.deleted = []
        self.published = []


class _FakeChannel:
    def __init__(self, broker, conn):
        self.broker, self.conn, self.consumer, self.queue = broker, conn, None, None

    def queue_declare(self, queue, durable=False, arguments=None):
        self.broker.queues.setdefault(queue, deque())

    def queue_delete(self, queue):
        self.broker.queues.pop(queue, None)
        self.broker.deleted.append(queue)

    def confirm_delivery(self):
        pass

    def basic_qos(self, prefetch_count):
        pass

    def basic_consume(self, queue, on_message_callback):
        self.queue, self.consumer = queue, on_message_callback

    def basic_publish(self, exchange, routing_key, body, mandatory, properties):
        assert mandatory is True and properties.delivery_mode == 2     # project rule: mandatory + persistent
        self.broker.queues[routing_key].append(body)
        self.broker.published.append(routing_key)

    def basic_ack(self, delivery_tag):
        pass


class _FakeConn:
    def __init__(self, broker):
        self.broker, self.is_open, self.ch = broker, True, None

    def channel(self):
        self.ch = _FakeChannel(self.broker, self)
        return self.ch

    def process_data_events(self, time_limit=0):
        import eventlet
        ch = self.ch
        if ch and ch.consumer and self.broker.queues[ch.queue]:
            ch.consumer(ch, _Method(1), None, self.broker.queues[ch.queue].popleft())
        else:
            eventlet.sleep(0.005)

    def close(self):
        self.is_open = False


def test_consume_stage_with_fake_broker(demo, monkeypatch):
    broker = _FakeBroker()
    conns = []

    def fake_connect(host, port, timeout_s=5.0):
        conns.append((host, port))
        return _FakeConn(broker)

    monkeypatch.setattr(demo, "connect_node", fake_connect)
    monkeypatch.setattr(demo, "WORK_SECONDS", 0.002)
    ctx = demo.Context(demo.resolve_targets("mgmtnode1,mgmtnode2,mgmtnode3", TARGETS), timeout_s=5, count=30)
    ok, rows = demo.stage_consume(ctx)

    assert ok, rows
    assert rows[-1]["consumed"] == "30/30"
    assert [r["published"] for r in rows[:3]] == [10, 10, 10]               # published through every node
    assert sum(r["consumed"] for r in rows[:3]) == 30                       # each message handled exactly once
    assert broker.deleted == [ctx.queue]                                     # demo queue removed afterwards
    assert {h for h, _ in conns} == {"10.0.0.1", "10.0.0.2", "10.0.0.3"}    # one connection per node


def test_consume_stage_reports_unreachable_broker(demo, monkeypatch):
    def refuse(host, port, timeout_s=5.0):
        raise ConnectionRefusedError("secret-host")

    monkeypatch.setattr(demo, "connect_node", refuse)
    ctx = demo.Context(demo.resolve_targets("mgmtnode1", TARGETS), timeout_s=1, count=3)
    with pytest.raises(ConnectionRefusedError):
        demo.stage_consume(ctx)


def test_run_stages_hides_exception_text(demo, monkeypatch, capsys):
    def refuse(host, port, timeout_s=5.0):
        raise ConnectionRefusedError("secret-host:5672")

    monkeypatch.setattr(demo, "connect_node", refuse)
    ctx = demo.Context(demo.resolve_targets("mgmtnode1", TARGETS), timeout_s=1, count=3)
    assert demo._run_stages(["consume"], ctx, "t") == 1
    out = capsys.readouterr().out
    assert "FAIL: ConnectionRefusedError" in out and "secret-host" not in out
