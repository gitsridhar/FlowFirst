#!/usr/bin/env python3
"""FlowFirst demo: how to use greenthreads (eventlet) with the project's `gt` helper, locally and across the cluster nodes.

The pipeline processes (src/application/process*.py) run hundreds of tasks on ONE operating-system thread by using
eventlet greenthreads. This script shows the same building blocks in small, readable stages:

  Local stages (no cluster needed)
    interleave   cooperative scheduling: 3 greenthreads take turns ONLY at gt.sleep(0); without it one runs to the end first
    concurrency  N greenthreads that each time.sleep(delay) finish in ~delay, not N*delay (time.sleep is monkey-patched);
                 a gt.periodic() heartbeat keeps ticking meanwhile
    restart      gt.spawn(restart_on_error=True) re-spawns a crashed worker; the default does not

  Cluster stages (talk to mgmtnode1..mgmtnode3 directly, no HAProxy, no REST API, no pipeline process)
    probe        one greenthread per (node, service) opens a TCP connection to RabbitMQ, ZooKeeper and MariaDB at the same
                 time; the same probes run one after the other for comparison
    consume      one CONSUMER greenthread and one PUBLISHER greenthread per RabbitMQ node, all in this one OS thread, each
                 with its own pika connection; messages published through every node are shared by the consumers

Usage:
    python3 scripts/greenthread_demo.py                         full demo (same as 'run')
    python3 scripts/greenthread_demo.py local                   only the stages that need no cluster
    python3 scripts/greenthread_demo.py interleave
    python3 scripts/greenthread_demo.py concurrency --tasks 50 --delay 0.5
    python3 scripts/greenthread_demo.py restart
    python3 scripts/greenthread_demo.py probe [--nodes mgmtnode1,mgmtnode2]
    python3 scripts/greenthread_demo.py consume --count 60 [--queue-type quorum]

Where to run it: the local stages anywhere with the .venv; the cluster stages on mgmtnode1..remotenode1 or any workstation that can
reach NODE1_IP..NODE3_IP and has the FlowFirst .env. RabbitMQ connections honour TLS_RABBITMQ_ENABLED through src.tls.tls
(fails closed). The password is never printed.

Why this file calls gt.monkey_patch() under `if __name__ == "__main__"`: a process entry point must patch BEFORE importing
pika / socket / threading, and this script does. The unit tests import the module as a library and must never patch the
pytest process, hence the guard. Real pipeline entry points patch unconditionally (see AGENTS.md).

The consume stage declares its own queue "flowfirst_demo_gt.<run id>.work" (with x-expires) and deletes it at the end. It
deliberately does NOT call config.setup_queues(), so the nine pipeline queues are never touched.
"""
import os
import sys

# Must be set before eventlet is imported: eventlet 0.40.x on macOS crashes in its kqueue hub; Linux (epoll) is unaffected.
if sys.platform == "darwin":
    os.environ.setdefault("EVENTLET_HUB", "selects")
# A demo must not write to /var/log/flowfirst and should only show problems; both can still be overridden by the caller.
os.environ.setdefault("LOG_DIR", "")
os.environ.setdefault("LOG_LEVEL", "WARNING")

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from src.gt import gt  # noqa: E402

if __name__ == "__main__":
    gt.monkey_patch()       # FIRST: before argparse/socket/time/threading/pika/kazoo/pymysql are imported below

import argparse  # noqa: E402
import json  # noqa: E402
import logging  # noqa: E402
import socket  # noqa: E402
import time  # noqa: E402
import uuid  # noqa: E402
from typing import Dict, List, Optional, Tuple  # noqa: E402

import eventlet  # noqa: E402
import eventlet.event  # noqa: E402
import eventlet.patcher  # noqa: E402
import pika  # noqa: E402

from src.config.config import (  # noqa: E402
    MARIADB_PORT,
    NODE1_IP,
    NODE2_IP,
    NODE3_IP,
    NODE_NAME,
    RABBITMQ_PASSWORD,
    RABBITMQ_PORT,
    RABBITMQ_QUEUE_TYPE,
    RABBITMQ_TLS_PORT,
    RABBITMQ_USER,
    TLS_RABBITMQ_ENABLED,
    TLS_ZK_ENABLED,
    ZK_CLIENT_PORT,
    ZK_TLS_CLIENT_PORT,
    queue_arguments,
)
from src.tls.tls import client_context  # noqa: E402

DEMO_PREFIX = "flowfirst_demo_gt"   # must NOT match the policy pattern ^flow[123]_ (the pipeline queues)
DEFAULT_NODES = "mgmtnode1,mgmtnode2,mgmtnode3"
DEFAULT_TIMEOUT_S = 10.0
PROBE_TIMEOUT_S = 2.0
POLL_INTERVAL_S = 0.05
WORK_SECONDS = 0.05                 # simulated processing time per message in the consume stage
HEARTBEAT_S = 0.05                  # tick of the gt.periodic() heartbeat in the concurrency stage
QUEUE_EXPIRES_MS = 30 * 60 * 1000   # safety net: an unused demo queue deletes itself after 30 minutes
MAX_TASKS = 900                     # below GT_POOL_SIZE (default 1000) so the heartbeat greenthread always gets a slot
INTERLEAVE_NAMES = ("A", "B", "C")
INTERLEAVE_STEPS = 3

Node = Tuple[str, str, int]         # (label, host, port)


# ----------------------------------------------------------------------------- pure helpers (unit-tested)

def get_amqp_port() -> int:
    """AMQP port the brokers listen on for clients: TLS port when TLS_RABBITMQ_ENABLED, else the plain one."""
    return RABBITMQ_TLS_PORT if TLS_RABBITMQ_ENABLED else RABBITMQ_PORT


def get_zk_port() -> int:
    return ZK_TLS_CLIENT_PORT if TLS_ZK_ENABLED else ZK_CLIENT_PORT


def named_targets(port: Optional[int] = None) -> Dict[str, Tuple[str, int]]:
    """Label -> (host, port) for the three cluster members (port = the RabbitMQ client port)."""
    p = port if port is not None else get_amqp_port()
    return {"mgmtnode1": (NODE1_IP, p), "mgmtnode2": (NODE2_IP, p), "mgmtnode3": (NODE3_IP, p)}


def resolve_target(spec: str, targets: Optional[Dict[str, Tuple[str, int]]] = None) -> Node:
    """('mgmtnode2') -> ('mgmtnode2', '<NODE2_IP>', 5672); '10.0.0.1:5672' -> ('10.0.0.1:5672', '10.0.0.1', 5672)."""
    spec = (spec or "").strip()
    if not spec:
        raise ValueError("empty node name")
    targets = targets if targets is not None else named_targets()
    if spec in targets:
        host, port = targets[spec]
        return spec, host, port
    if ":" in spec:
        host, p_str = spec.rsplit(":", 1)
        try:
            return spec, host.strip(), int(p_str.strip())
        except ValueError:
            raise ValueError(f"invalid port in target '{spec}'")
    return spec, spec, get_amqp_port()


def resolve_targets(csv: str, targets: Optional[Dict[str, Tuple[str, int]]] = None) -> List[Node]:
    """'mgmtnode1,mgmtnode2' -> [(label, host, port), ...] with duplicate host:port pairs omitted."""
    out, seen = [], set()
    for part in (csv or "").split(","):
        part = part.strip()
        if not part:
            continue
        label, host, port = resolve_target(part, targets)
        hp = f"{host}:{port}"
        if hp not in seen:
            seen.add(hp)
            out.append((label, host, port))
    if not out:
        raise ValueError("no nodes given")
    return out


def expected_interleave(names, steps: int, yield_each: bool) -> List[str]:
    """The order in which greenthreads that append '<name><step>' `steps` times run.

    With a yield (gt.sleep(0)) after every step they take turns; without one each runs to the end before the next starts,
    because a greenthread is only switched out at a blocking/yielding call."""
    if yield_each:
        return [f"{n}{i}" for i in range(1, steps + 1) for n in names]
    return [f"{n}{i}" for n in names for i in range(1, steps + 1)]


def split_sequence(count: int, parts: int) -> List[List[int]]:
    """Message numbers 1..count dealt round-robin to `parts` publishers: split_sequence(5, 2) -> [[1, 3, 5], [2, 4]]."""
    if parts < 1:
        raise ValueError("parts must be >= 1")
    return [list(range(i + 1, count + 1, parts)) for i in range(parts)]


def demo_queue_name(run_id: str) -> str:
    return f"{DEMO_PREFIX}.{run_id}.work"


def demo_queue_arguments(queue_type: Optional[str] = None) -> Dict[str, object]:
    """Declaration arguments for the demo queue: the configured queue type plus an expiry so a stray cleans itself up."""
    args = dict(queue_arguments(queue_type) or {})
    args["x-expires"] = QUEUE_EXPIRES_MS
    return args


def render_table(rows: List[dict], columns: List[Tuple[str, str]]) -> str:
    """Fixed-width text table. rows: list of dicts, columns: list of (key, heading)."""
    cells = [[str(r.get(k, "")) if r.get(k) is not None else "" for k, _ in columns] for r in rows]
    widths = [max([len(h)] + [len(c[i]) for c in cells]) for i, (_, h) in enumerate(columns)]
    line = "  ".join(h.ljust(widths[i]) for i, (_, h) in enumerate(columns))
    out = [line, "  ".join("-" * w for w in widths)]
    out += ["  ".join(c[i].ljust(widths[i]) for i in range(len(columns))) for c in cells]
    return "\n".join(out)


def share_percent(count: int, total: int) -> str:
    return f"{(100.0 * count / total):.0f}%" if total else "0%"


class Context:
    """Everything a stage needs."""

    def __init__(self, nodes: Optional[List[Node]] = None, timeout_s: float = DEFAULT_TIMEOUT_S, count: int = 0,
                 tasks: int = 20, delay: float = 1.0, queue_type: Optional[str] = None):
        self.nodes = nodes or []
        self.timeout_s = timeout_s
        self.count = count
        self.tasks = tasks
        self.delay = delay
        self.run_id = uuid.uuid4().hex[:8]
        self.queue = demo_queue_name(self.run_id)
        self.qargs = demo_queue_arguments(queue_type)
        self.queue_type = queue_type or RABBITMQ_QUEUE_TYPE


# ----------------------------------------------------------------------------- local stages (no cluster)

def run_interleave(names, steps: int, yield_each: bool) -> List[str]:
    """Spawn one named greenthread per name through gt.spawn(); return the order in which the steps happened."""
    order: List[str] = []

    def worker(name: str) -> None:
        for i in range(1, steps + 1):
            order.append(f"{name}{i}")
            if yield_each:
                gt.sleep(0)         # the mandatory yield of every consumer loop in the pipeline

    handles = [gt.spawn(f"demo_turn_{n}", worker, n) for n in names]
    for h in handles:
        h.wait()                    # wait() hands control to the other greenthreads until this one has finished
    return order


def stage_interleave(ctx: Context) -> Tuple[bool, List[dict]]:
    """Stage: greenthreads are cooperative: they switch only where they yield or block."""
    rows, ok = [], True
    for yield_each, label in ((True, "with gt.sleep(0)"), (False, "no yield")):
        got = run_interleave(INTERLEAVE_NAMES, INTERLEAVE_STEPS, yield_each)
        good = got == expected_interleave(INTERLEAVE_NAMES, INTERLEAVE_STEPS, yield_each)
        ok = ok and good
        rows.append({
            "variant": label,
            "order": " ".join(got),
            "result": ("PASS: took turns" if yield_each else "PASS: first one ran to the end (others starved)")
                      if good else "FAIL: unexpected order",
        })
    return ok, rows


def stage_concurrency(ctx: Context) -> Tuple[bool, List[dict]]:
    """Stage: many tasks that wait run concurrently on one OS thread, and a periodic greenthread keeps running."""
    n, delay = ctx.tasks, ctx.delay
    if not 1 <= n <= MAX_TASKS:
        raise ValueError(f"--tasks must be between 1 and {MAX_TASKS}")
    if delay <= 0:
        raise ValueError("--delay must be positive")
    patched = eventlet.patcher.is_monkey_patched("time")
    ticks = [0]
    done: List[int] = []

    def beat() -> None:
        ticks[0] += 1

    def task(i: int) -> None:
        time.sleep(delay)           # the standard-library call: cooperative ONLY because gt.monkey_patch() ran
        done.append(i)

    gt.periodic("demo_heartbeat", beat, HEARTBEAT_S)
    t0 = time.time()
    try:
        handles = [gt.spawn(f"demo_sleep_{i}", task, i) for i in range(n)]
        for h in handles:
            h.wait()
    finally:
        gt.kill("demo_heartbeat")
    elapsed = time.time() - t0
    sequential = n * delay
    fast = n < 2 or elapsed < sequential / 2
    ok = patched and len(done) == n and fast and ticks[0] > 0
    rows = [
        {"metric": "time.sleep is cooperative (monkey-patched)", "value": "yes" if patched else "NO"},
        {"metric": f"{n} tasks x time.sleep({delay:g}) one after the other", "value": f"{sequential:.2f} s (calculated)"},
        {"metric": f"{n} tasks as greenthreads, measured", "value": f"{elapsed:.2f} s ({len(done)}/{n} finished)"},
        {"metric": f"heartbeat ticks (every {HEARTBEAT_S:g} s) during that time", "value": str(ticks[0])},
        {"metric": "operating-system threads used", "value": "1 (all greenthreads share this thread)"},
        {"metric": "result", "value": "PASS: waits overlap" if ok else "FAIL: no concurrency"},
    ]
    return ok, rows


def stage_restart(ctx: Context) -> Tuple[bool, List[dict]]:
    """Stage: gt.spawn(restart_on_error=True) re-spawns a worker that raised; the default lets it stay dead."""
    runs = {"restarted": 0, "plain": 0}

    def flaky(key: str) -> None:
        runs[key] += 1
        if runs[key] == 1:
            raise RuntimeError("simulated crash")       # not shown to a client; gt logs it (silenced below)

    gt_log = logging.getLogger(gt.__name__)
    previous = gt_log.level
    gt_log.setLevel(logging.CRITICAL)                   # keep the expected traceback out of the demo output
    t0 = time.time()
    try:
        gt.spawn("demo_plain", flaky, "plain", restart_on_error=False).wait()
        gt.spawn("demo_restarted", flaky, "restarted", restart_on_error=True).wait()
    finally:
        gt_log.setLevel(previous)
    elapsed = time.time() - t0
    ok = runs["plain"] == 1 and runs["restarted"] == 2
    rows = [
        {"worker": "demo_plain", "restart_on_error": "False", "runs": runs["plain"],
         "result": "PASS: crashed once, stayed down" if runs["plain"] == 1 else "FAIL"},
        {"worker": "demo_restarted", "restart_on_error": "True", "runs": runs["restarted"],
         "result": f"PASS: crashed, re-spawned after gt's 2 s delay ({elapsed:.1f} s total)" if runs["restarted"] == 2 else "FAIL"},
    ]
    return ok, rows


# ----------------------------------------------------------------------------- cluster stage: probe

def probe_tcp(label: str, service: str, host: str, port: int, timeout_s: float) -> dict:
    """Open (and close) one TCP connection. Cooperative: while this waits, other greenthreads run."""
    row = {"node": label, "service": service, "endpoint": f"{host}:{port}", "status": "", "connect_ms": "-", "result": ""}
    t0 = time.time()
    try:
        s = socket.create_connection((host, port), timeout=timeout_s)
        s.close()
        row["status"] = "reachable"
        row["connect_ms"] = f"{(time.time() - t0) * 1000:.0f}"
    except Exception as e:
        row["status"] = f"unreachable ({type(e).__name__})"
        row["connect_ms"] = f"{(time.time() - t0) * 1000:.0f}"
    return row


def probe_plan(nodes: List[Node]) -> List[Tuple[str, str, str, int]]:
    """(label, service, host, port) for every node and every service the pipeline depends on."""
    services = (("rabbitmq", get_amqp_port()), ("zookeeper", get_zk_port()), ("mariadb", MARIADB_PORT))
    return [(label, svc, host, port) for label, host, _p in nodes for svc, port in services]


def stage_probe(ctx: Context) -> Tuple[bool, List[dict]]:
    """Stage: probe every service on every node at once with a GreenPool, then one by one for comparison."""
    plan = probe_plan(ctx.nodes)
    pool = eventlet.GreenPool(size=len(plan))
    t0 = time.time()
    rows = list(pool.imap(lambda p: probe_tcp(*p, PROBE_TIMEOUT_S), plan))      # results come back in plan order
    concurrent_s = time.time() - t0

    t1 = time.time()
    for p in plan:
        probe_tcp(*p, PROBE_TIMEOUT_S)
    sequential_s = time.time() - t1

    majority = len(ctx.nodes) // 2 + 1
    ok = True
    for svc in dict.fromkeys(p[1] for p in plan):
        up = sum(1 for r in rows if r["service"] == svc and r["status"] == "reachable")
        ok = ok and up >= majority
    for r in rows:
        r["result"] = "PASS" if r["status"] == "reachable" else "down"
    up_total = sum(1 for r in rows if r["status"] == "reachable")
    rows.append({
        "node": "TOTAL", "service": "all", "endpoint": f"{len(plan)} probes", "status": f"{up_total} reachable",
        "connect_ms": "-",
        "result": f"{'PASS' if ok else 'FAIL'}: {len(plan)} greenthreads took {concurrent_s:.2f} s; one by one {sequential_s:.2f} s",
    })
    return ok, rows


# ----------------------------------------------------------------------------- cluster stage: consume

def connect_node(host: str, port: int, timeout_s: float = 5.0) -> pika.BlockingConnection:
    """One BlockingConnection to ONE specific broker node. A BlockingConnection is not greenthread-safe: every
    greenthread opens its own (AGENTS.md). The sockets are cooperative because gt.monkey_patch() ran first."""
    ssl_options = pika.SSLOptions(client_context("rabbitmq")) if TLS_RABBITMQ_ENABLED else None
    params = pika.ConnectionParameters(
        host=host,
        port=port,
        credentials=pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD),
        ssl_options=ssl_options,
        heartbeat=30,
        socket_timeout=timeout_s,
        connection_attempts=1,
        blocked_connection_timeout=60,
    )
    return pika.BlockingConnection(params)


def _close(conn) -> None:
    try:
        if conn is not None and conn.is_open:
            conn.close()
    except Exception:
        pass


def delete_queue(nodes: List[Node], queue: str) -> bool:
    """Delete the demo queue through the first node that answers."""
    for _label, host, port in nodes:
        try:
            conn = connect_node(host, port)
            try:
                conn.channel().queue_delete(queue=queue)
            finally:
                _close(conn)
            return True
        except Exception:
            continue
    return False


def stage_consume(ctx: Context) -> Tuple[bool, List[dict]]:
    """Stage: a consumer greenthread and a publisher greenthread per node, each with its own connection, in ONE thread."""
    count = ctx.count or 30
    nodes = ctx.nodes
    queue = ctx.queue
    stop = eventlet.event.Event()
    # Greenthreads only switch at blocking calls, so these plain dict/list updates need no lock.
    consumed: Dict[str, List[str]] = {label: [] for label, _, _ in nodes}
    published: Dict[str, int] = {label: 0 for label, _, _ in nodes}
    ready: Dict[str, bool] = {label: False for label, _, _ in nodes}
    errors: List[str] = []

    def consumer(label: str, host: str, port: int) -> None:
        conn = None
        try:
            conn = connect_node(host, port, ctx.timeout_s)          # this greenthread's own connection
            ch = conn.channel()
            ch.basic_qos(prefetch_count=1)                         # at most 1 unacknowledged message

            def on_message(c, method, _props, body):
                gt.sleep(WORK_SECONDS)                             # simulated work; other greenthreads run meanwhile
                consumed[label].append(json.loads(body)["id"])
                c.basic_ack(delivery_tag=method.delivery_tag)      # ack only after the work is done

            ch.basic_consume(queue=queue, on_message_callback=on_message)
            ready[label] = True
            while not (stop.ready() or gt.is_stopping()):
                conn.process_data_events(time_limit=0.1)
                gt.sleep(0)                                        # mandatory: lets the sibling greenthreads run
        except Exception as e:
            errors.append(f"{label}: {type(e).__name__}")
            ready[label] = True
        finally:
            _close(conn)

    def publisher(label: str, host: str, port: int, seqs: List[int]) -> None:
        conn = None
        try:
            conn = connect_node(host, port, ctx.timeout_s)          # this greenthread's own connection
            ch = conn.channel()
            ch.confirm_delivery()                                  # publish blocks (cooperatively) for the broker's ack
            for seq in seqs:
                body = json.dumps({"id": f"{ctx.run_id}-{seq}", "seq": seq, "published_via": label}).encode("utf-8")
                ch.basic_publish(exchange="", routing_key=queue, body=body, mandatory=True,
                                 properties=pika.BasicProperties(delivery_mode=2, content_type="application/json"))
                published[label] += 1
                gt.sleep(0)
        except Exception as e:
            errors.append(f"{label}: publish {type(e).__name__}")
        finally:
            _close(conn)

    handles = []
    try:
        decl = connect_node(nodes[0][1], nodes[0][2], ctx.timeout_s)
        try:
            decl.channel().queue_declare(queue=queue, durable=True, arguments=ctx.qargs)
        finally:
            _close(decl)

        consumer_handles = [gt.spawn(f"demo_consumer_{label}", consumer, label, host, port) for label, host, port in nodes]
        handles += consumer_handles
        deadline = time.time() + ctx.timeout_s
        while not all(ready.values()) and time.time() < deadline:
            gt.sleep(POLL_INTERVAL_S)

        t0 = time.time()
        shares = split_sequence(count, len(nodes))
        publisher_handles = [gt.spawn(f"demo_publisher_{label}", publisher, label, host, port, seqs)
                             for (label, host, port), seqs in zip(nodes, shares)]
        handles += publisher_handles
        deadline = time.time() + ctx.timeout_s
        while time.time() < deadline:
            if sum(len(v) for v in consumed.values()) >= count or errors:
                break
            gt.sleep(POLL_INTERVAL_S)
        elapsed = time.time() - t0
    finally:
        if not stop.ready():
            stop.send(True)                     # every consumer loop sees this and returns
        for h in handles:
            h.wait()
        delete_queue(nodes, queue)

    all_ids = [m for v in consumed.values() for m in v]
    total = len(all_ids)
    no_duplicates = len(set(all_ids)) == total
    ok = total == count and no_duplicates and not errors
    rows = [
        {
            "node": label,
            "published": published[label],
            "consumed": len(consumed[label]),
            "share": share_percent(len(consumed[label]), total),
            "result": "PASS: consuming" if consumed[label] else "idle (the others were faster)",
        }
        for label, _, _ in nodes
    ]
    rows.append({
        "node": "TOTAL",
        "published": sum(published.values()),
        "consumed": f"{total}/{count}",
        "share": "-",
        "result": (f"PASS: each message handled exactly once in {elapsed:.2f} s "
                   f"(one consumer would need about {count * WORK_SECONDS:.2f} s)") if ok
                  else f"FAIL: {'duplicates' if not no_duplicates else 'lost or late messages'}"
                       + (f" ({'; '.join(errors)})" if errors else ""),
    })
    return ok, rows


# ----------------------------------------------------------------------------- stage table and commands

STAGES = {
    "interleave": ("Cooperative scheduling: greenthreads switch only where they yield", stage_interleave, [
        ("variant", "variant"), ("order", "order in which the steps ran"), ("result", "result")]),
    "concurrency": ("Concurrency on ONE OS thread: waits overlap, a periodic greenthread keeps ticking", stage_concurrency, [
        ("metric", "metric"), ("value", "value")]),
    "restart": ("Crash handling: gt.spawn(restart_on_error=...)", stage_restart, [
        ("worker", "greenthread"), ("restart_on_error", "restart_on_error"), ("runs", "runs"), ("result", "result")]),
    "probe": ("Across the nodes: probe RabbitMQ, ZooKeeper and MariaDB on every node at once", stage_probe, [
        ("node", "node"), ("service", "service"), ("endpoint", "endpoint"), ("status", "status"),
        ("connect_ms", "connect (ms)"), ("result", "result")]),
    "consume": ("Across the nodes: a consumer and a publisher greenthread per RabbitMQ node", stage_consume, [
        ("node", "node"), ("published", "published"), ("consumed", "consumed"), ("share", "share"), ("result", "result")]),
}
LOCAL_ORDER = ["interleave", "concurrency", "restart"]
CLUSTER_ORDER = ["probe", "consume"]
RUN_ORDER = LOCAL_ORDER + CLUSTER_ORDER


def _make_context(args) -> Context:
    nodes = resolve_targets(args.nodes) if getattr(args, "nodes", None) else []
    count = getattr(args, "count", 0)
    tasks = getattr(args, "tasks", 20)
    delay = getattr(args, "delay", 1.0)
    if count < 0:
        raise ValueError("--count must not be negative")
    return Context(nodes, getattr(args, "timeout", DEFAULT_TIMEOUT_S), count, tasks, delay, getattr(args, "queue_type", None))


def _print_stage(key: str, ctx: Context, indent: str = "") -> bool:
    _title, func, cols = STAGES[key]
    ok, rows = func(ctx)
    print(indent + render_table(rows, cols).replace("\n", "\n" + indent))
    return ok


def _run_stages(order: List[str], ctx: Context, title: str) -> int:
    print(f"== {title} ==")
    print(f"client     : {NODE_NAME}")
    print(f"eventlet   : {eventlet.__version__}, monkey-patched: {'yes' if eventlet.patcher.is_monkey_patched('socket') else 'NO'}")
    if any(k in CLUSTER_ORDER for k in order):
        print(f"nodes      : {', '.join(f'{lbl} ({h})' for lbl, h, _ in ctx.nodes)}")
        print(f"tls mode   : rabbitmq {'ON' if TLS_RABBITMQ_ENABLED else 'OFF'}")
    overall = True
    for n, key in enumerate(order, 1):
        print(f"\n[{n}] {STAGES[key][0]}")
        try:
            overall = _print_stage(key, ctx, "    ") and overall
        except ValueError:
            raise
        except Exception as e:
            print(f"    FAIL: {type(e).__name__}")
            overall = False
    print(f"\n[{len(order) + 1}] Summary & Result")
    print("    PASS: greenthreads verified." if overall
          else "    FAIL: one or more stages failed. For the cluster stages check connectivity and the .env (see ReadMe Scenario 72).")
    return 0 if overall else 1


def cmd_run(args) -> int:
    return _run_stages(RUN_ORDER, _make_context(args), "FlowFirst Greenthread (eventlet) Demo")


def cmd_local(args) -> int:
    return _run_stages(LOCAL_ORDER, _make_context(args), "FlowFirst Greenthread Demo (local stages)")


def _single_stage(key: str):
    def cmd(args) -> int:
        return _run_stages([key], _make_context(args), f"Greenthreads: {STAGES[key][0]}")
    return cmd


# ----------------------------------------------------------------------------- CLI parsing

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="FlowFirst greenthread demo: cooperative scheduling, concurrency, restart, and greenthreads across the cluster nodes."
    )
    sub = p.add_subparsers(dest="cmd")

    def common(sp, local=True, cluster=True):
        if local:
            sp.add_argument("--tasks", type=int, default=20, help="concurrency stage: number of greenthreads (default: %(default)s)")
            sp.add_argument("--delay", type=float, default=1.0, help="concurrency stage: seconds each task waits (default: %(default)s)")
        if cluster:
            sp.add_argument("--nodes", default=DEFAULT_NODES, help="comma list of nodes (default: %(default)s)")
            sp.add_argument("--timeout", type=float, default=DEFAULT_TIMEOUT_S, help="seconds to wait per step")
            sp.add_argument("--count", type=int, default=0, help="consume stage: number of messages (0 = 30)")
            sp.add_argument("--queue-type", choices=("classic", "quorum"), default=None,
                            help="consume stage: queue type (default: RABBITMQ_QUEUE_TYPE from .env)")

    r = sub.add_parser("run", help="full demo: local and cluster stages (default)")
    common(r)
    r.set_defaults(func=cmd_run)

    lo = sub.add_parser("local", help="only the stages that need no cluster")
    common(lo, cluster=False)
    lo.set_defaults(func=cmd_local)

    helps = {
        "interleave": ("greenthreads take turns only where they yield", True, False),
        "concurrency": ("N waits overlap on one OS thread; periodic heartbeat", True, False),
        "restart": ("gt.spawn restart_on_error", False, False),
        "probe": ("probe RabbitMQ, ZooKeeper, MariaDB on every node at once", False, True),
        "consume": ("consumer + publisher greenthread per RabbitMQ node", False, True),
    }
    for key, (text, loc, clu) in helps.items():
        sp = sub.add_parser(key, help=text)
        common(sp, local=loc, cluster=clu)
        sp.set_defaults(func=_single_stage(key))
    return p


def main(argv: Optional[List[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if not args.cmd:
        args = parser.parse_args(["run"] + list(argv if argv is not None else sys.argv[1:]))

    logging.getLogger("pika").setLevel(logging.CRITICAL)    # the demo prints its own one-line failure; pika logs tracebacks
    gt.register_signal_handlers()           # Ctrl-C / SIGTERM set gt.stop_event; the consumer loops then return cleanly
    try:
        return args.func(args)
    except ValueError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return 2
    except pika.exceptions.AMQPError as e:
        print(f"ERROR: RabbitMQ operation failed: {type(e).__name__}", file=sys.stderr)
        return 1
    except Exception as e:
        print(f"ERROR: {type(e).__name__}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
