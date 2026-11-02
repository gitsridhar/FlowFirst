#!/usr/bin/env python3
"""FlowFirst demo: how to use RabbitMQ queues across the cluster nodes.

Talks to the RabbitMQ brokers on mgmtnode1..mgmtnode3 directly (one connection per node, no HAProxy, no REST API, no pipeline
process), so it isolates the messaging layer. Every stage uses its own demo queues/exchange, named
"flowfirst_demo.<run id>.*", so the nine pipeline queues and flowfirst_dead_letters are never touched.

Stages:
  status    reach every broker node, show its version and the cluster name
  work      work queue: publish through EACH node (publisher confirms + mandatory), consume everything through ONE node
  compete   competing consumers: one consumer per node on the same queue, prefetch 1, manual ack, work is shared
  fanout    publish/subscribe: a fanout exchange, one queue bound per node, one publish reaches every queue
  redeliver reliability: a consumer that dies without ack -> the message is redelivered (redelivered=True) to another node

Usage:
    python3 scripts/rabbitmq_queue_demo.py                         full demo (same as 'run')
    python3 scripts/rabbitmq_queue_demo.py run --keep              ...and leave the demo queues in place
    python3 scripts/rabbitmq_queue_demo.py run --nodes mgmtnode1,mgmtnode2 only use these brokers (e.g. mgmtnode3 is down)
    python3 scripts/rabbitmq_queue_demo.py run --queue-type quorum use quorum queues (default: RABBITMQ_QUEUE_TYPE)
    python3 scripts/rabbitmq_queue_demo.py status
    python3 scripts/rabbitmq_queue_demo.py work --count 9
    python3 scripts/rabbitmq_queue_demo.py compete --count 20
    python3 scripts/rabbitmq_queue_demo.py fanout
    python3 scripts/rabbitmq_queue_demo.py redeliver
    python3 scripts/rabbitmq_queue_demo.py cleanup --run-id <id>   delete the queues of a run left behind by --keep

Where to run it: on mgmtnode1..remotenode1 or any workstation that can reach TCP 5672 (5671 with TLS) on NODE1_IP..NODE3_IP and
has the FlowFirst .env. Connections honour TLS_RABBITMQ_ENABLED through src.tls.tls (fails closed). The password is
never printed.

Deliberately NOT calling config.setup_queues(): that declares the nine pipeline queues, which this demo must not touch.
Demo queues carry x-expires, so a run killed halfway cannot leave queues behind for ever.
"""
import argparse
import json
import os
import sys
import threading
import time
import uuid
from typing import Dict, List, Optional, Tuple

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

import pika  # noqa: E402

from src.config.config import (  # noqa: E402
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
    queue_arguments,
)
from src.tls.tls import client_context  # noqa: E402

DEMO_PREFIX = "flowfirst_demo"      # must NOT match the policy pattern ^flow[123]_ (the pipeline queues)
DEFAULT_NODES = "mgmtnode1,mgmtnode2,mgmtnode3"
DEFAULT_TIMEOUT_S = 10.0
POLL_INTERVAL_S = 0.05
WORK_SECONDS = 0.05                 # simulated processing time per message in the competing-consumer stage
QUEUE_EXPIRES_MS = 30 * 60 * 1000   # safety net: an unused demo queue deletes itself after 30 minutes
MAX_FANOUT_QUEUES = 8               # upper bound used by cleanup (one fanout queue per node)

Node = Tuple[str, str, int]         # (label, host, port)


# ----------------------------------------------------------------------------- pure helpers (unit-tested)

def get_amqp_port() -> int:
    """AMQP port the brokers listen on for clients: TLS port when TLS_RABBITMQ_ENABLED, else the plain one."""
    return RABBITMQ_TLS_PORT if TLS_RABBITMQ_ENABLED else RABBITMQ_PORT


def named_targets(port: Optional[int] = None) -> Dict[str, Tuple[str, int]]:
    """Label -> (host, port) for the three RabbitMQ cluster members."""
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


def demo_names(run_id: str) -> Dict[str, str]:
    """Every broker object a run may create. All start with DEMO_PREFIX, none can match the pipeline queue policy."""
    base = f"{DEMO_PREFIX}.{run_id}"
    return {
        "work": f"{base}.work",
        "compete": f"{base}.compete",
        "redeliver": f"{base}.redeliver",
        "exchange": f"{base}.events",
        "fanout_queue": f"{base}.events",       # per-node queues are "<fanout_queue>.n<index>" (see fanout_queue_name)
    }


def fanout_queue_name(run_id: str, index: int) -> str:
    return f"{demo_names(run_id)['fanout_queue']}.n{index}"


def demo_queue_arguments(queue_type: Optional[str] = None) -> Dict[str, object]:
    """Declaration arguments for a demo queue: the configured queue type plus an expiry so strays clean themselves up."""
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


def missing_entries(expected: List[str], seen: List[str]) -> List[str]:
    """Items expected but not yet seen (sorted, for deterministic output)."""
    return sorted(set(expected) - set(seen))


def share_percent(count: int, total: int) -> str:
    return f"{(100.0 * count / total):.0f}%" if total else "0%"


# ----------------------------------------------------------------------------- broker access

def connect_node(host: str, port: int, timeout_s: float = 5.0) -> pika.BlockingConnection:
    """One BlockingConnection to ONE specific broker node (no failover list: the point is to choose the node).

    A BlockingConnection must be used by one thread only, so every thread opens its own."""
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


class Session:
    """A connection to one node plus a channel with publisher confirms on."""

    def __init__(self, label: str, host: str, port: int, timeout_s: float):
        self.label = label
        self.conn = connect_node(host, port, timeout_s)
        self.channel = self.conn.channel()
        self.channel.confirm_delivery()     # basic_publish now blocks for the broker's ack and raises on nack/unroutable

    def close(self) -> None:
        try:
            if self.conn.is_open:
                self.conn.close()
        except Exception:
            pass


def open_sessions(nodes: List[Node], timeout_s: float) -> List[Session]:
    sessions: List[Session] = []
    try:
        for label, host, port in nodes:
            sessions.append(Session(label, host, port, timeout_s))
    except Exception:
        close_sessions(sessions)
        raise
    return sessions


def close_sessions(sessions: List[Session]) -> None:
    for s in sessions:
        s.close()


def publish(session: Session, exchange: str, routing_key: str, payload: dict) -> Tuple[bool, str]:
    """Publish one persistent message with mandatory=True on a confirm channel. Returns (confirmed, reason)."""
    try:
        session.channel.basic_publish(
            exchange=exchange,
            routing_key=routing_key,
            body=json.dumps(payload).encode("utf-8"),
            properties=pika.BasicProperties(delivery_mode=2, message_id=payload["id"], content_type="application/json"),
            mandatory=True,
        )
        return True, ""
    except pika.exceptions.UnroutableError:
        return False, "unroutable"
    except pika.exceptions.NackError:
        return False, "nacked by broker"


def get_one(channel, queue: str, timeout_s: float):
    """basic_get (no auto-ack) with polling. Returns (method, properties, body) or None after timeout_s."""
    deadline = time.time() + timeout_s
    while True:
        method, props, body = channel.basic_get(queue=queue, auto_ack=False)
        if method is not None:
            return method, props, body
        if time.time() >= deadline:
            return None
        time.sleep(POLL_INTERVAL_S)


def _text(value) -> str:
    return value.decode("utf-8", errors="replace") if isinstance(value, bytes) else str(value)


class Context:
    """Everything a stage needs."""

    def __init__(self, nodes: List[Node], run_id: str, queue_type: Optional[str], timeout_s: float, count: int = 0):
        self.nodes = nodes
        self.run_id = run_id
        self.names = demo_names(run_id)
        self.qargs = demo_queue_arguments(queue_type)
        self.queue_type = queue_type or RABBITMQ_QUEUE_TYPE
        self.timeout_s = timeout_s
        self.count = count


# ----------------------------------------------------------------------------- stages

def stage_status(ctx: Context) -> Tuple[bool, List[dict]]:
    """Stage 1: connect to every broker node on its own and report what answers."""
    rows, reachable = [], 0
    for label, host, port in ctx.nodes:
        row = {"node": label, "endpoint": f"{host}:{port}", "status": "", "connect_ms": "n/a", "version": "n/a", "cluster": "n/a"}
        t0 = time.time()
        try:
            conn = connect_node(host, port, ctx.timeout_s)
            try:
                props = getattr(getattr(conn, "_impl", None), "server_properties", None) or {}
                row["connect_ms"] = f"{(time.time() - t0) * 1000:.0f}"
                row["version"] = _text(props.get("version", "n/a"))
                row["cluster"] = _text(props.get("cluster_name", "n/a"))
                row["status"] = "reachable"
                reachable += 1
            finally:
                conn.close()
        except Exception as e:
            row["status"] = f"unreachable ({type(e).__name__})"
        rows.append(row)
    return reachable >= len(ctx.nodes) // 2 + 1, rows


def stage_work_queue(ctx: Context) -> Tuple[bool, List[dict]]:
    """Stage 2: messages published through every node are all consumed through a single (other) node."""
    count = ctx.count or 6
    queue = ctx.names["work"]
    sessions = open_sessions(ctx.nodes, ctx.timeout_s)
    try:
        sessions[0].channel.queue_declare(queue=queue, durable=True, arguments=ctx.qargs)
        sent = []
        for seq in range(1, count + 1):
            pub = sessions[(seq - 1) % len(sessions)]
            payload = {"id": f"{ctx.run_id}-w{seq}", "seq": seq, "published_via": pub.label}
            ok, why = publish(pub, "", queue, payload)
            sent.append((payload, ok, why))

        reader = sessions[-1]
        received: Dict[str, str] = {}
        deadline = time.time() + ctx.timeout_s
        while len(received) < sum(1 for _, ok, _ in sent if ok) and time.time() < deadline:
            got = get_one(reader.channel, queue, max(0.0, deadline - time.time()))
            if got is None:
                break
            method, _props, body = got
            reader.channel.basic_ack(delivery_tag=method.delivery_tag)
            received[json.loads(body)["id"]] = reader.label

        rows, all_ok = [], True
        for payload, ok, why in sent:
            consumed = received.get(payload["id"])
            good = ok and consumed is not None
            all_ok = all_ok and good
            rows.append({
                "seq": payload["seq"],
                "published_via": payload["published_via"],
                "confirmed": "yes" if ok else f"NO ({why})",
                "consumed_via": consumed or "-",
                "result": "PASS: delivered" if good else "FAIL: message lost",
            })
        return all_ok, rows
    finally:
        close_sessions(sessions)


def stage_competing_consumers(ctx: Context) -> Tuple[bool, List[dict]]:
    """Stage 3: one consumer thread per node on the same queue; each message goes to exactly one of them."""
    count = ctx.count or 12
    queue = ctx.names["compete"]
    publisher = open_sessions(ctx.nodes[:1], ctx.timeout_s)[0]
    lock = threading.Lock()
    processed: Dict[str, List[str]] = {label: [] for label, _, _ in ctx.nodes}
    errors: List[str] = []
    stop = threading.Event()
    ready = [threading.Event() for _ in ctx.nodes]

    def consumer(index: int, label: str, host: str, port: int) -> None:
        conn = None
        try:
            conn = connect_node(host, port, ctx.timeout_s)      # each thread owns its connection
            ch = conn.channel()
            ch.basic_qos(prefetch_count=1)                     # at most 1 unacknowledged message per consumer

            def on_message(c, method, _props, body):
                time.sleep(WORK_SECONDS)                       # simulate work
                with lock:
                    processed[label].append(json.loads(body)["id"])
                c.basic_ack(delivery_tag=method.delivery_tag)  # ack only after the work is done

            ch.basic_consume(queue=queue, on_message_callback=on_message)
            ready[index].set()
            while not stop.is_set():
                conn.process_data_events(time_limit=0.1)
        except Exception as e:
            with lock:
                errors.append(f"{label}: {type(e).__name__}")
            ready[index].set()
        finally:
            try:
                if conn is not None and conn.is_open:
                    conn.close()
            except Exception:
                pass

    threads = []
    try:
        publisher.channel.queue_declare(queue=queue, durable=True, arguments=ctx.qargs)
        for i, (label, host, port) in enumerate(ctx.nodes):
            t = threading.Thread(target=consumer, args=(i, label, host, port), daemon=True)
            t.start()
            threads.append(t)
        for evt in ready:
            evt.wait(timeout=ctx.timeout_s)

        for seq in range(1, count + 1):
            publish(publisher, "", queue, {"id": f"{ctx.run_id}-c{seq}", "seq": seq, "published_via": publisher.label})

        deadline = time.time() + ctx.timeout_s
        while time.time() < deadline:
            with lock:
                if sum(len(v) for v in processed.values()) >= count or errors:
                    break
            time.sleep(POLL_INTERVAL_S)
    finally:
        stop.set()
        for t in threads:
            t.join(timeout=3)
        close_sessions([publisher])

    all_ids = [m for v in processed.values() for m in v]
    total = len(all_ids)
    no_duplicates = len(set(all_ids)) == total
    ok = total == count and no_duplicates and not errors
    rows = [
        {
            "consumer": label,
            "processed": len(ids),
            "share": share_percent(len(ids), total),
            "result": "PASS: consuming" if ids else "idle (the others were faster)",
        }
        for label, ids in processed.items()
    ]
    rows.append({
        "consumer": "TOTAL",
        "processed": f"{total}/{count}",
        "share": "-",
        "result": "PASS: each message handled exactly once" if ok else f"FAIL: {'duplicates' if not no_duplicates else 'lost messages'}"
                  + (f" ({'; '.join(errors)})" if errors else ""),
    })
    return ok, rows


def stage_fanout(ctx: Context) -> Tuple[bool, List[dict]]:
    """Stage 4: a fanout exchange with one queue per node; one publish is copied into every queue."""
    exchange = ctx.names["exchange"]
    sessions = open_sessions(ctx.nodes, ctx.timeout_s)
    try:
        sessions[0].channel.exchange_declare(exchange=exchange, exchange_type="fanout", durable=True)
        queues = []
        for i, s in enumerate(sessions):
            q = fanout_queue_name(ctx.run_id, i + 1)
            s.channel.queue_declare(queue=q, durable=True, arguments=ctx.qargs)   # declared THROUGH node i
            s.channel.queue_bind(queue=q, exchange=exchange)
            queues.append(q)

        event = {"id": f"{ctx.run_id}-evt1", "published_via": sessions[0].label, "event": "config.reloaded"}
        confirmed, why = publish(sessions[0], exchange, "", event)

        rows, all_ok = [], confirmed
        for s, q in zip(sessions, queues):
            got = get_one(s.channel, q, ctx.timeout_s) if confirmed else None
            if got is not None:
                s.channel.basic_ack(delivery_tag=got[0].delivery_tag)
            good = got is not None and json.loads(got[2])["id"] == event["id"]
            all_ok = all_ok and good
            rows.append({
                "node": s.label,
                "queue": q.split(".", 2)[2],
                "published_via": event["published_via"],
                "received": "yes" if good else "NO",
                "result": "PASS: copy received" if good else f"FAIL: {why or 'no copy'}",
            })
        return all_ok, rows
    finally:
        close_sessions(sessions)


def stage_redelivery(ctx: Context) -> Tuple[bool, List[dict]]:
    """Stage 5: consumer A takes a message and dies without ack; consumer B, on another node, gets it again."""
    queue = ctx.names["redeliver"]
    node_a = ctx.nodes[0]
    node_b = ctx.nodes[1] if len(ctx.nodes) > 1 else ctx.nodes[0]
    pub = open_sessions(ctx.nodes[:1], ctx.timeout_s)[0]
    rows: List[dict] = []
    try:
        pub.channel.queue_declare(queue=queue, durable=True, arguments=ctx.qargs)
        confirmed, why = publish(pub, "", queue, {"id": f"{ctx.run_id}-r1", "published_via": pub.label})
        rows.append({
            "step": "1. publish", "node": pub.label, "redelivered": "-",
            "result": "PASS: confirmed by broker" if confirmed else f"FAIL: {why}",
        })
        if not confirmed:
            return False, rows

        a = Session(node_a[0], node_a[1], node_a[2], ctx.timeout_s)
        got_a = get_one(a.channel, queue, ctx.timeout_s)
        first_ok = got_a is not None and not got_a[0].redelivered
        rows.append({
            "step": "2. consumer A takes it", "node": node_a[0], "redelivered": "no" if first_ok else "?",
            "result": "PASS: delivered, NOT acknowledged" if first_ok else "FAIL: nothing delivered",
        })
        a.close()       # connection drops with an unacknowledged message = a consumer that crashed

        b = Session(node_b[0], node_b[1], node_b[2], ctx.timeout_s)
        try:
            got_b = get_one(b.channel, queue, ctx.timeout_s)
            again = got_b is not None and bool(got_b[0].redelivered)
            if got_b is not None:
                b.channel.basic_ack(delivery_tag=got_b[0].delivery_tag)
            rows.append({
                "step": "3. consumer B takes it", "node": node_b[0], "redelivered": "yes" if again else "no",
                "result": "PASS: broker redelivered it" if again else "FAIL: message not redelivered",
            })
            left = b.channel.queue_declare(queue=queue, passive=True).method.message_count
        finally:
            b.close()
        rows.append({
            "step": "4. after ack", "node": node_b[0], "redelivered": "-",
            "result": "PASS: queue empty" if left == 0 else f"FAIL: {left} message(s) left",
        })
        return first_ok and again and left == 0, rows
    finally:
        close_sessions([pub])


STAGES = {
    "status": ("Broker nodes reachable one by one", stage_status, [
        ("node", "node"), ("endpoint", "endpoint"), ("status", "status"), ("connect_ms", "connect (ms)"),
        ("version", "rabbitmq"), ("cluster", "cluster name")]),
    "work": ("Work queue: publish through every node, consume through one", stage_work_queue, [
        ("seq", "seq"), ("published_via", "published via"), ("confirmed", "confirmed"),
        ("consumed_via", "consumed via"), ("result", "result")]),
    "compete": ("Competing consumers: one per node, same queue", stage_competing_consumers, [
        ("consumer", "consumer node"), ("processed", "processed"), ("share", "share"), ("result", "result")]),
    "fanout": ("Publish/subscribe: fanout exchange, one queue per node", stage_fanout, [
        ("node", "node"), ("queue", "queue"), ("published_via", "published via"),
        ("received", "received"), ("result", "result")]),
    "redeliver": ("Redelivery after a consumer dies without ack", stage_redelivery, [
        ("step", "step"), ("node", "node"), ("redelivered", "redelivered"), ("result", "result")]),
}
RUN_ORDER = ["status", "work", "compete", "fanout", "redeliver"]


# ----------------------------------------------------------------------------- cleanup

def delete_run_objects(host: str, port: int, run_id: str) -> int:
    """Delete every queue and the exchange of a run through one node. Returns how many objects were deleted."""
    names = demo_names(run_id)
    queues = [names["work"], names["compete"], names["redeliver"]]
    queues += [fanout_queue_name(run_id, i) for i in range(1, MAX_FANOUT_QUEUES + 1)]
    conn = connect_node(host, port)
    deleted = 0
    try:
        ch = conn.channel()
        for q in queues:
            try:
                ch.queue_declare(queue=q, passive=True)      # raises (and closes the channel) if it does not exist
            except pika.exceptions.ChannelClosedByBroker:
                ch = conn.channel()
                continue
            ch.queue_delete(queue=q)
            deleted += 1
        try:
            ch.exchange_declare(exchange=names["exchange"], passive=True)
            ch.exchange_delete(exchange=names["exchange"])
            deleted += 1
        except pika.exceptions.ChannelClosedByBroker:
            pass
    finally:
        conn.close()
    return deleted


def cleanup_run(nodes: List[Node], run_id: str) -> Optional[int]:
    """Try the nodes in order until one answers. None if no node could be reached."""
    for _label, host, port in nodes:
        try:
            return delete_run_objects(host, port, run_id)
        except Exception:
            continue
    return None


# ----------------------------------------------------------------------------- commands

def _make_context(args, count: int = 0) -> Context:
    return Context(resolve_targets(args.nodes), f"{uuid.uuid4().hex[:8]}", args.queue_type, args.timeout,
                   getattr(args, "count", 0) or count)


def _print_stage(key: str, ctx: Context, indent: str = "") -> bool:
    _title, func, cols = STAGES[key]
    ok, rows = func(ctx)
    print(indent + render_table(rows, cols).replace("\n", "\n" + indent))
    return ok


def cmd_run(args) -> int:
    ctx = _make_context(args)
    print("== FlowFirst RabbitMQ Queue Demo ==")
    print(f"run id     : {ctx.run_id}")
    print(f"brokers    : {', '.join(f'{lbl} ({h}:{p})' for lbl, h, p in ctx.nodes)}")
    print(f"client     : {NODE_NAME}")
    print(f"queue type : {ctx.queue_type}"
          + ("  (a classic queue lives on the node that declared it)" if ctx.queue_type == "classic" else "  (replicated across the cluster)"))
    print(f"tls mode   : {'ON (verified CA)' if TLS_RABBITMQ_ENABLED else 'OFF (plain TCP)'}")

    overall = True
    for n, key in enumerate(RUN_ORDER, 1):
        print(f"\n[{n}] {STAGES[key][0]}")
        try:
            overall = _print_stage(key, ctx, "    ") and overall
        except Exception as e:
            print(f"    FAIL: {type(e).__name__}")
            overall = False
            if key == "status":
                break

    print(f"\n[{len(RUN_ORDER) + 1}] Summary & Result")
    print("    PASS: RabbitMQ queues verified across cluster nodes." if overall
          else "    FAIL: one or more stages failed. Check broker connectivity and the cluster (rabbitmqctl cluster_status).")

    if args.keep:
        print(f"\n    Demo queues kept (run id {ctx.run_id}); they expire by themselves after 30 minutes.")
        print(f"    Inspect: rabbitmqctl list_queues name messages consumers | grep {DEMO_PREFIX}")
        print(f"    Remove : python3 scripts/rabbitmq_queue_demo.py cleanup --run-id {ctx.run_id}")
    else:
        n_del = cleanup_run(ctx.nodes, ctx.run_id)
        print(f"\n    Cleaned up {n_del} demo object(s) (use --keep to retain them)." if n_del is not None
              else "\n    WARNING: could not reach any broker to clean up; the queues expire after 30 minutes.")
    return 0 if overall else 1


def _single_stage(key: str):
    def cmd(args) -> int:
        ctx = _make_context(args)
        print(f"== RabbitMQ: {STAGES[key][0]} (run {ctx.run_id}) ==")
        try:
            ok = _print_stage(key, ctx)
        finally:
            if key != "status" and not getattr(args, "keep", False):
                cleanup_run(ctx.nodes, ctx.run_id)
        return 0 if ok else 1
    return cmd


def cmd_cleanup(args) -> int:
    nodes = resolve_targets(args.nodes)
    n_del = cleanup_run(nodes, args.run_id)
    if n_del is None:
        print("ERROR: could not reach any broker", file=sys.stderr)
        return 1
    print(f"Deleted {n_del} demo object(s) of run '{args.run_id}'")
    return 0


# ----------------------------------------------------------------------------- CLI parsing

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="FlowFirst RabbitMQ demo: work queue, competing consumers, fanout and redelivery across the cluster nodes."
    )
    sub = p.add_subparsers(dest="cmd")

    def common(sp, keep=True, count=False):
        sp.add_argument("--nodes", default=DEFAULT_NODES, help="comma list of brokers (default: %(default)s)")
        sp.add_argument("--timeout", type=float, default=DEFAULT_TIMEOUT_S, help="seconds to wait per step")
        sp.add_argument("--queue-type", choices=("classic", "quorum"), default=None,
                        help="queue type for the demo queues (default: RABBITMQ_QUEUE_TYPE from .env)")
        if keep:
            sp.add_argument("--keep", action="store_true", help="leave the demo queues in place")
        if count:
            sp.add_argument("--count", type=int, default=0, help="number of messages (0 = stage default)")

    r = sub.add_parser("run", help="full demo: every stage (default)")
    common(r)
    r.set_defaults(func=cmd_run)

    helps = {
        "status": "reach each broker node and show its version",
        "work": "work queue: publish through every node, consume through one",
        "compete": "competing consumers, one per node",
        "fanout": "fanout exchange with one queue per node",
        "redeliver": "message redelivered after a consumer dies without ack",
    }
    for key, text in helps.items():
        sp = sub.add_parser(key, help=text)
        common(sp, keep=(key != "status"), count=key in ("work", "compete"))
        sp.set_defaults(func=_single_stage(key))

    c = sub.add_parser("cleanup", help="delete the queues/exchange of a run left behind by --keep")
    c.add_argument("--run-id", required=True, help="run id printed by the demo")
    c.add_argument("--nodes", default=DEFAULT_NODES)
    c.set_defaults(func=cmd_cleanup)
    return p


def main(argv: Optional[List[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if not args.cmd:
        args = parser.parse_args(["run"] + list(argv if argv is not None else sys.argv[1:]))

    try:
        if getattr(args, "count", 0) < 0:
            raise ValueError("--count must not be negative")
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
