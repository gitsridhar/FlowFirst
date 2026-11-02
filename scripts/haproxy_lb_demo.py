#!/usr/bin/env python3
"""FlowFirst demo: how HAProxy is used in front of the cluster nodes.

HAProxy listens on the Virtual IP (VIP) and spreads clients over node1..node3. This script talks to the VIP like a real
client and to every node directly, and compares the two, so you SEE the load balancer working: which node answered,
how the requests were shared, what HAProxy's own counters say, and what happens when a node is taken out.

Stages (the numbers in brackets are the VIP ports from .env):
  status    which VIP ports and which node ports accept connections (REST API, MariaDB, RabbitMQ, Swift, stats page)
  backends  HAProxy's own view: state of every server in every backend, from the stats page (needs HAPROXY_STATS_PASS)
  api       REST API [8080]: N x GET /health through the VIP -> which node answered (round-robin), cross-checked with
            HAProxy's per-server session counters, and with a direct probe of each node (a DOWN node must get nothing)
  mariadb   MariaDB [3307]: N connections through the VIP, SELECT @@hostname, shared over the Galera nodes
  rabbitmq  RabbitMQ [5673]: N AMQP connections through the VIP, shared over the brokers (HAProxy counters)
  swift     Swift [8090]: N x GET /healthcheck through the VIP, shared over the Swift proxies (HAProxy counters)
  watch     live view for a failover test: one request per second through the VIP, which node answered, and when
            HAProxy changes a server's state

Usage:
    python3 scripts/haproxy_lb_demo.py                          full demo (same as 'run')
    python3 scripts/haproxy_lb_demo.py run --requests 30        more requests = a clearer distribution
    python3 scripts/haproxy_lb_demo.py status
    python3 scripts/haproxy_lb_demo.py backends
    python3 scripts/haproxy_lb_demo.py api
    python3 scripts/haproxy_lb_demo.py mariadb
    python3 scripts/haproxy_lb_demo.py rabbitmq
    python3 scripts/haproxy_lb_demo.py swift
    python3 scripts/haproxy_lb_demo.py watch --seconds 60       leave it running, then stop a node's API in another shell

Where to run it: on node1..node4 or any workstation that can reach the VIP (and, for the per-node checks, the node
addresses) and has the FlowFirst .env. TLS follows TLS_API_ENABLED / TLS_MARIADB_ENABLED / TLS_RABBITMQ_ENABLED /
TLS_SWIFT_ENABLED through src.tls.tls (fails closed; the VIP certificate must be valid for the VIP address). Passwords
are never printed; errors print only the exception type or the HTTP status.

The demo only reads and connects: it creates no queue, row or object. It is NOT a load test (Scenario 61 is).
"""
import argparse
import base64
import csv
import http.client
import io
import json
import os
import socket
import sys
import time
from collections import Counter
from typing import Callable, Dict, List, NamedTuple, Optional, Tuple

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

import pika  # noqa: E402
import pymysql  # noqa: E402

from src.config.config import (  # noqa: E402
    API_BACKEND_PORT,
    API_PORT,
    FLOWFIRST_VIP,
    MARIADB_PASSWORD,
    MARIADB_PORT,
    MARIADB_USER,
    NODE1_IP,
    NODE2_IP,
    NODE3_IP,
    NODE_NAME,
    RABBITMQ_PASSWORD,
    RABBITMQ_PORT,
    RABBITMQ_TLS_PORT,
    RABBITMQ_USER,
    SWIFT_LB_PORT,
    TLS_API_ENABLED,
    TLS_MARIADB_ENABLED,
    TLS_RABBITMQ_ENABLED,
    TLS_SWIFT_ENABLED,
)
from src.tls.tls import client_context, mysql_ssl  # noqa: E402

DEFAULT_NODES = "node1,node2,node3"
DEFAULT_REQUESTS = 12
DEFAULT_TIMEOUT_S = 5.0
BACKENDS = {      # stage -> backend name in haproxy/haproxy.cfg.template
    "api": "flowfirst_api_back",
    "mariadb": "mariadb_galera_back",
    "rabbitmq": "rabbitmq_amqp_back",
    "swift": "swift_back",
}


def _env_int(name: str, default: int) -> int:
    try:
        return int((os.getenv(name) or "").strip() or default)
    except ValueError:
        return default


class Node(NamedTuple):
    label: str      # what the tables show
    host: str       # address used for direct checks
    name: str       # NODEn_NAME: the name HAProxy uses for the server and API Gateway (Process 1) reports as "node"
    api_port: int   # port for the direct /health probe


class Settings:
    """Everything a stage needs."""

    def __init__(self, args):
        self.vip = args.vip or FLOWFIRST_VIP
        self.api_port = args.api_port or API_PORT
        self.stats_port = args.stats_port or _env_int("HAPROXY_STATS_PORT", 9000)
        self.mariadb_port = args.mariadb_port or _env_int("MARIADB_LB_PORT", 3307)
        self.rabbitmq_port = args.rabbitmq_port or _env_int("RABBITMQ_LB_PORT", 5673)
        self.swift_port = args.swift_port or SWIFT_LB_PORT
        self.swift_node_port = _env_int("SWIFT_PROXY_PORT", 8888)
        self.stats_user = os.getenv("HAPROXY_STATS_USER", "admin").strip()
        self.stats_pass = os.getenv("HAPROXY_STATS_PASS", "").strip()
        self.requests = args.requests
        self.timeout = args.timeout
        self.nodes = resolve_nodes(args.nodes)
        self.seconds = getattr(args, "seconds", 0)


# ----------------------------------------------------------------------------- pure helpers (unit-tested)

def named_nodes() -> Dict[str, Node]:
    ips = {"node1": NODE1_IP, "node2": NODE2_IP, "node3": NODE3_IP}
    return {label: Node(label, ip, os.getenv(f"{label.upper()}_NAME", label), API_BACKEND_PORT) for label, ip in ips.items()}


def resolve_nodes(csv_text: str, named: Optional[Dict[str, Node]] = None) -> List[Node]:
    """'node1,node2' -> Node list; 'host:port' probes that API address directly; duplicates are dropped."""
    named = named if named is not None else named_nodes()
    out: List[Node] = []
    seen = set()
    for part in (csv_text or "").split(","):
        part = part.strip()
        if not part:
            continue
        if part in named:
            node = named[part]
        elif ":" in part:
            host, p_str = part.rsplit(":", 1)
            try:
                node = Node(part, host.strip(), part, int(p_str.strip()))
            except ValueError:
                raise ValueError(f"invalid port in node '{part}'")
        else:
            node = Node(part, part, part, API_BACKEND_PORT)
        if (node.host, node.api_port) not in seen:
            seen.add((node.host, node.api_port))
            out.append(node)
    if not out:
        raise ValueError("no nodes given")
    return out


def parse_stats_csv(text: str) -> List[Dict[str, str]]:
    """HAProxy 'stats;csv' output -> one dict per line. The header line starts with '# '."""
    lines = [ln for ln in text.splitlines() if ln.strip()]
    if not lines:
        return []
    lines[0] = lines[0].lstrip("# ").strip()
    return [dict(row) for row in csv.DictReader(io.StringIO("\n".join(lines)))]


def server_rows(rows: List[Dict[str, str]], backend: str) -> List[Dict[str, str]]:
    """The server lines of one backend (without the FRONTEND/BACKEND summary lines)."""
    return [r for r in rows if r.get("pxname") == backend and r.get("svname") not in ("FRONTEND", "BACKEND")]


def sessions_by_server(rows: List[Dict[str, str]], backend: str) -> Dict[str, int]:
    """Server name -> total sessions HAProxy has handed to it since it started (column 'stot')."""
    out: Dict[str, int] = {}
    for r in server_rows(rows, backend):
        try:
            out[r["svname"]] = int(r.get("stot") or 0)
        except ValueError:
            out[r["svname"]] = 0
    return out


def counter_delta(before: Dict[str, int], after: Dict[str, int]) -> Dict[str, int]:
    return {k: max(0, v - before.get(k, 0)) for k, v in after.items()}


def is_up(status: str) -> bool:
    """HAProxy reports 'UP', 'UP 1/2' (rising), 'DOWN', 'DOWN 1/3' (falling), 'NOLB', 'MAINT'."""
    return (status or "").strip().upper().startswith("UP")


def is_balanced(counts: Dict[str, int], total: int) -> bool:
    """Round-robin shares work evenly. Every listed server got at least one, and no server is more than
    max(2, total/5) ahead of another (a little slack because other clients use the same VIP at the same time)."""
    if not counts or total <= 0:
        return False
    values = list(counts.values())
    return min(values) >= 1 and (max(values) - min(values)) <= max(2, total // 5)


def share_percent(count: int, total: int) -> str:
    return f"{(100.0 * count / total):.0f}%" if total else "0%"


def render_table(rows: List[dict], columns: List[Tuple[str, str]]) -> str:
    """Fixed-width text table. rows: list of dicts, columns: list of (key, heading)."""
    cells = [[str(r.get(k, "")) if r.get(k) is not None else "" for k, _ in columns] for r in rows]
    widths = [max([len(h)] + [len(c[i]) for c in cells]) for i, (_, h) in enumerate(columns)]
    line = "  ".join(h.ljust(widths[i]) for i, (_, h) in enumerate(columns))
    out = [line, "  ".join("-" * w for w in widths)]
    out += ["  ".join(c[i].ljust(widths[i]) for i in range(len(columns))) for c in cells]
    return "\n".join(out)


class HttpStatusError(Exception):
    def __init__(self, status: int):
        super().__init__(f"HTTP {status}")
        self.status = status


def describe_error(e: Exception) -> str:
    """Safe text for a failure: exception type, or the HTTP status. Never the message (it can hold hosts and URLs)."""
    if isinstance(e, HttpStatusError):
        return f"HTTP {e.status}"
    return type(e).__name__


def state_line(rows: List[Dict[str, str]], backend: str) -> str:
    """'node1=UP node2=DOWN node3=UP' for one backend."""
    return " ".join(f"{r['svname']}={(r.get('status') or '?').split()[0]}" for r in server_rows(rows, backend))


# ----------------------------------------------------------------------------- network access

class _TlsConn(http.client.HTTPSConnection):
    """HTTPS to an address with an explicit certificate name (nodes are addressed by IP, certificates by node name)."""

    def __init__(self, host, port, ctx, sni, timeout):
        super().__init__(host, port, timeout=timeout, context=ctx)
        self._ff_ctx = ctx
        self._ff_sni = sni

    def connect(self):
        sock = socket.create_connection((self.host, self.port), self.timeout)
        self.sock = self._ff_ctx.wrap_socket(sock, server_hostname=self._ff_sni or self.host)


_TLS_CTX = []


def _tls_context():
    if not _TLS_CTX:
        _TLS_CTX.append(client_context("haproxy-demo"))
    return _TLS_CTX[0]


def http_get(host: str, port: int, path: str, tls: bool, timeout: float, sni: Optional[str] = None,
             headers: Optional[Dict[str, str]] = None) -> Tuple[int, bytes]:
    """One GET on a fresh connection (a new connection per request, so HAProxy chooses a server every time)."""
    hdrs = {"Connection": "close"}
    hdrs.update(headers or {})
    conn = _TlsConn(host, port, _tls_context(), sni, timeout) if tls else http.client.HTTPConnection(host, port, timeout=timeout)
    try:
        conn.request("GET", path, headers=hdrs)
        resp = conn.getresponse()
        return resp.status, resp.read()
    finally:
        conn.close()


def tcp_check(host: str, port: int, timeout: float) -> Tuple[bool, float]:
    t0 = time.time()
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True, (time.time() - t0) * 1000
    except OSError:
        return False, (time.time() - t0) * 1000


def stats_available(st: Settings) -> bool:
    return bool(st.stats_pass)


def fetch_stats(st: Settings) -> List[Dict[str, str]]:
    """HAProxy's own counters from the stats page (frontends, backends, every server)."""
    token = base64.b64encode(f"{st.stats_user}:{st.stats_pass}".encode("utf-8")).decode("ascii")
    code, body = http_get(st.vip, st.stats_port, "/;csv", TLS_API_ENABLED, st.timeout, headers={"Authorization": f"Basic {token}"})
    if code != 200:
        raise HttpStatusError(code)
    return parse_stats_csv(body.decode("utf-8", errors="replace"))


def safe_stats(st: Settings) -> Optional[List[Dict[str, str]]]:
    """fetch_stats() or None (no password configured, or the stats page does not answer)."""
    if not stats_available(st):
        return None
    try:
        return fetch_stats(st)
    except Exception:
        return None


# ----------------------------------------------------------------------------- stages: each returns (ok, rows, columns)

def stage_status(st: Settings):
    """Which ports accept connections: on the VIP (HAProxy) and on every node (the real service)."""
    rabbit_node = RABBITMQ_TLS_PORT if TLS_RABBITMQ_ENABLED else RABBITMQ_PORT
    services = [
        ("REST API", st.api_port, None),
        ("MariaDB", st.mariadb_port, MARIADB_PORT),
        ("RabbitMQ", st.rabbitmq_port, rabbit_node),
        ("Swift", st.swift_port, st.swift_node_port),
        ("HAProxy stats", st.stats_port, "-"),
    ]
    rows, vip_ok = [], True
    for name, vip_port, node_port in services:
        ok, ms = tcp_check(st.vip, vip_port, st.timeout)
        vip_ok = vip_ok and ok
        row = {"service": name, "vip": f"{st.vip}:{vip_port}", "via_vip": f"open ({ms:.0f} ms)" if ok else "CLOSED"}
        for nd in st.nodes:
            if node_port == "-":
                row[nd.label] = "-"
                continue
            port = nd.api_port if node_port is None else node_port
            nok, nms = tcp_check(nd.host, port, st.timeout)
            row[nd.label] = f"open :{port}" if nok else f"closed :{port}"
        row["result"] = "PASS: HAProxy listens" if ok else "FAIL: nothing listens on the VIP port"
        rows.append(row)
    cols = [("service", "service"), ("vip", "VIP endpoint"), ("via_vip", "via VIP")]
    cols += [(nd.label, f"direct {nd.label}") for nd in st.nodes] + [("result", "result")]
    return vip_ok, rows, cols


def stage_backends(st: Settings):
    """HAProxy's own view of every backend server."""
    cols = [("backend", "backend"), ("server", "server"), ("status", "status"), ("check", "last check"),
            ("sessions", "total sessions"), ("since", "state for (s)"), ("result", "result")]
    if not stats_available(st):
        return True, [{"backend": "-", "server": "-", "status": "-", "check": "-", "sessions": "-", "since": "-",
                       "result": "SKIP: HAPROXY_STATS_PASS is not set in .env"}], cols
    stats = fetch_stats(st)
    rows, ok = [], True
    for backend in BACKENDS.values():
        servers = server_rows(stats, backend)
        up = sum(1 for r in servers if is_up(r.get("status", "")))
        backend_ok = bool(servers) and up >= len(servers) // 2 + 1
        ok = ok and backend_ok
        for r in servers:
            good = is_up(r.get("status", ""))
            rows.append({
                "backend": backend, "server": r["svname"], "status": r.get("status", "?"),
                "check": r.get("check_status") or "-", "sessions": r.get("stot") or "0", "since": r.get("lastchg") or "-",
                "result": "PASS: in rotation" if good else "DOWN: out of rotation (HAProxy sends it nothing)",
            })
        if not servers:
            rows.append({"backend": backend, "server": "-", "status": "-", "check": "-", "sessions": "-", "since": "-",
                         "result": "FAIL: backend not found in HAProxy (wrong stats address or old config)"})
        elif not backend_ok:
            rows.append({"backend": backend, "server": "-", "status": "-", "check": "-", "sessions": "-", "since": "-",
                         "result": f"FAIL: only {up} of {len(servers)} servers up, no majority"})
    return ok, rows, cols


def stage_api(st: Settings):
    """N requests through the VIP: which node answered, cross-checked against HAProxy's counters and a direct probe."""
    n = st.requests
    names = {nd.name: nd.label for nd in st.nodes}
    direct: Dict[str, Tuple[bool, str]] = {}
    for nd in st.nodes:
        t0 = time.time()
        try:
            code, _ = http_get(nd.host, nd.api_port, "/health", TLS_API_ENABLED, st.timeout, sni=nd.name)
            direct[nd.label] = (code == 200, f"{code} ({(time.time() - t0) * 1000:.0f} ms)")
        except Exception as e:
            direct[nd.label] = (False, f"down ({describe_error(e)})")

    stats_before = safe_stats(st)
    counts: Counter = Counter()
    failures = 0
    for _ in range(n):
        try:
            code, body = http_get(st.vip, st.api_port, "/health", TLS_API_ENABLED, st.timeout)
            if code != 200:
                failures += 1
                continue
            node = str(json.loads(body).get("node", "?"))
            counts[names.get(node, "other")] += 1
        except Exception:
            failures += 1
    stats_after = safe_stats(st)

    backend = BACKENDS["api"]
    sessions = counter_delta(sessions_by_server(stats_before, backend), sessions_by_server(stats_after, backend)) \
        if stats_before is not None and stats_after is not None else None
    states = {r["svname"]: r.get("status", "") for r in server_rows(stats_after, backend)} if stats_after else {}

    answered = n - failures
    rows, ok, expected_up = [], failures == 0, {}
    for nd in st.nodes:
        up = is_up(states[nd.name]) if nd.name in states else direct[nd.label][0]     # HAProxy's verdict beats ours
        expected_up[nd.label] = up
        got = counts.get(nd.label, 0)
        if up:
            good, text = got > 0, (f"PASS: served {got} request(s)" if got > 0 else "FAIL: in rotation but received nothing")
        else:
            good, text = got == 0, ("PASS: out of rotation, got nothing" if got == 0 else "FAIL: served requests although down")
        ok = ok and good
        rows.append({"node": nd.label, "direct": direct[nd.label][1], "via_vip": got, "share": share_percent(got, answered),
                     "sessions": sessions.get(nd.name, "n/a") if sessions is not None else "n/a", "result": text})
    if counts.get("other"):
        rows.append({"node": "(unknown)", "direct": "-", "via_vip": counts["other"], "share": share_percent(counts["other"], answered),
                     "sessions": "-", "result": "FAIL: answered by a node that is not in --nodes"})
        ok = False
    balanced = is_balanced({lbl: counts.get(lbl, 0) for lbl, up in expected_up.items() if up}, answered)
    ok = ok and balanced
    if failures:
        total_text = f"FAIL: {failures} of {n} requests got no 200 answer"
    elif not balanced:
        total_text = "FAIL: requests are not shared evenly over the nodes in rotation"
    else:
        total_text = "PASS: round-robin, every request answered" if ok else "FAIL: see the rows above"
    rows.append({"node": "TOTAL", "direct": "-", "via_vip": f"{answered}/{n}", "share": "-", "sessions": "-", "result": total_text})
    cols = [("node", "node"), ("direct", "direct /health"), ("via_vip", "answered via VIP"), ("share", "share"),
            ("sessions", "HAProxy new sessions"), ("result", "result")]
    return ok, rows, cols


def _open_mariadb(st: Settings) -> str:
    conn = pymysql.connect(host=st.vip, port=st.mariadb_port, user=MARIADB_USER, password=MARIADB_PASSWORD,
                           connect_timeout=max(1, int(st.timeout)), ssl=mysql_ssl() if TLS_MARIADB_ENABLED else None)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT @@hostname")
            return str(cur.fetchone()[0])
    finally:
        conn.close()


def _open_rabbitmq(st: Settings) -> str:
    params = pika.ConnectionParameters(
        host=st.vip, port=st.rabbitmq_port, credentials=pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD),
        ssl_options=pika.SSLOptions(client_context("rabbitmq")) if TLS_RABBITMQ_ENABLED else None,
        socket_timeout=st.timeout, connection_attempts=1, heartbeat=30)
    conn = pika.BlockingConnection(params)
    conn.close()
    return ""


def _open_swift(st: Settings) -> str:
    code, _ = http_get(st.vip, st.swift_port, "/healthcheck", TLS_SWIFT_ENABLED, st.timeout)
    if code != 200:
        raise HttpStatusError(code)
    return ""


def _mariadb_hostnames(st: Settings) -> Dict[str, str]:
    """@@hostname -> node label, learned by connecting to each node directly (bypassing HAProxy)."""
    out: Dict[str, str] = {}
    for nd in st.nodes:
        try:
            conn = pymysql.connect(host=nd.host, port=MARIADB_PORT, user=MARIADB_USER, password=MARIADB_PASSWORD,
                                   connect_timeout=max(1, int(st.timeout)), ssl=mysql_ssl() if TLS_MARIADB_ENABLED else None)
            try:
                with conn.cursor() as cur:
                    cur.execute("SELECT @@hostname")
                    out[str(cur.fetchone()[0])] = nd.label
            finally:
                conn.close()
        except Exception:
            continue
    return out


def _stage_tcp(st: Settings, kind: str, opener: Callable[[Settings], str]):
    """N connections through the VIP; the shares are read from HAProxy's counters (and, for MariaDB, from the answers)."""
    n = st.requests
    backend = BACKENDS[kind]
    identities = _mariadb_hostnames(st) if kind == "mariadb" else {}
    stats_before = safe_stats(st)
    answered_by: Counter = Counter()
    failures = 0
    for _ in range(n):
        try:
            who = opener(st)
            if who:
                answered_by[identities.get(who, who)] += 1
        except Exception:
            failures += 1
    stats_after = safe_stats(st)
    ok_conns = n - failures

    cols = [("server", "server"), ("status", "HAProxy status"), ("sessions", "new sessions"), ("answered", "answered (SQL)"),
            ("result", "result")]
    rows: List[dict] = []
    ok = failures == 0
    if stats_before is None or stats_after is None:
        text = f"PASS: {ok_conns}/{n} connections made through the VIP" if ok else f"FAIL: {failures} of {n} connections failed"
        rows.append({"server": "(all)", "status": "n/a", "sessions": "n/a",
                     "answered": json.dumps(dict(answered_by)) if answered_by else "n/a",
                     "result": text + " (no HAProxy counters: set HAPROXY_STATS_PASS)"})
        return ok, rows, cols

    delta = counter_delta(sessions_by_server(stats_before, backend), sessions_by_server(stats_after, backend))
    total = 0
    for r in server_rows(stats_after, backend):
        name = r["svname"]
        got = delta.get(name, 0)
        total += got
        up = is_up(r.get("status", ""))
        if up:
            good, text = got > 0, (f"PASS: received {got} session(s)" if got > 0 else "FAIL: in rotation but received nothing")
        else:
            good, text = got == 0, ("PASS: out of rotation, got nothing" if got == 0 else "FAIL: got sessions although down")
        ok = ok and good
        label = next((nd.label for nd in st.nodes if nd.name == name), name)
        rows.append({"server": name, "status": r.get("status", "?"), "sessions": got,
                     "answered": answered_by.get(label, 0) if kind == "mariadb" else "-", "result": text})
    if not rows:
        ok = False
        rows.append({"server": "-", "status": "-", "sessions": "-", "answered": "-",
                     "result": "FAIL: backend not found in HAProxy"})
    elif total < ok_conns:
        ok = False
    if failures:
        total_text = f"FAIL: {failures} of {n} connection(s) failed"
    elif total < ok_conns:
        total_text = f"FAIL: HAProxy counted only {total} of {ok_conns} connection(s)"
    else:
        total_text = "PASS: every connection was counted by HAProxy"
    rows.append({"server": "TOTAL", "status": "-", "sessions": f"{total} (we made {ok_conns})", "answered": "-",
                 "result": total_text})
    return ok, rows, cols


def stage_mariadb(st: Settings):
    return _stage_tcp(st, "mariadb", _open_mariadb)


def stage_rabbitmq(st: Settings):
    return _stage_tcp(st, "rabbitmq", _open_rabbitmq)


def stage_swift(st: Settings):
    return _stage_tcp(st, "swift", _open_swift)


STAGES = {
    "status": ("VIP ports and node ports reachable", stage_status),
    "backends": ("HAProxy's own view of every backend server", stage_backends),
    "api": ("REST API: round-robin over the nodes", stage_api),
    "mariadb": ("MariaDB: connections shared over the Galera nodes", stage_mariadb),
    "rabbitmq": ("RabbitMQ: connections shared over the brokers", stage_rabbitmq),
    "swift": ("Swift: requests shared over the Swift proxies", stage_swift),
}
RUN_ORDER = ["status", "backends", "api", "mariadb", "rabbitmq", "swift"]


# ----------------------------------------------------------------------------- commands

def _print_stage(key: str, st: Settings, indent: str = "") -> bool:
    ok, rows, cols = STAGES[key][1](st)
    print(indent + render_table(rows, cols).replace("\n", "\n" + indent))
    return ok


def _header(st: Settings) -> None:
    print("== FlowFirst HAProxy Load-Balancing Demo ==")
    print(f"VIP        : {st.vip}   (REST {st.api_port}, MariaDB {st.mariadb_port}, RabbitMQ {st.rabbitmq_port}, "
          f"Swift {st.swift_port}, stats {st.stats_port})")
    print(f"nodes      : {', '.join(f'{n.label} ({n.host})' for n in st.nodes)}")
    print(f"client     : {NODE_NAME}")
    print(f"requests   : {st.requests} per stage")
    print(f"tls mode   : API {'ON' if TLS_API_ENABLED else 'off'}, MariaDB {'ON' if TLS_MARIADB_ENABLED else 'off'}, "
          f"RabbitMQ {'ON' if TLS_RABBITMQ_ENABLED else 'off'}, Swift {'ON' if TLS_SWIFT_ENABLED else 'off'}")
    print(f"stats page : {'credentials set' if stats_available(st) else 'HAPROXY_STATS_PASS not set: per-server counters unavailable'}")


def cmd_run(args) -> int:
    st = Settings(args)
    _header(st)
    overall = True
    for n, key in enumerate(RUN_ORDER, 1):
        print(f"\n[{n}] {STAGES[key][0]}")
        try:
            overall = _print_stage(key, st, "    ") and overall
        except Exception as e:
            print(f"    FAIL: {describe_error(e)}")
            overall = False
    print(f"\n[{len(RUN_ORDER) + 1}] Summary & Result")
    print("    PASS: HAProxy balances the cluster nodes as configured." if overall
          else "    FAIL: one or more stages failed. Check that the VIP is up (Scenario 8), HAProxy is running under "
               "Pacemaker (Scenario 15) and the stats page (Scenario 9).")
    return 0 if overall else 1


def _single_stage(key: str):
    def cmd(args) -> int:
        st = Settings(args)
        print(f"== HAProxy: {STAGES[key][0]} ==")
        return 0 if _print_stage(key, st) else 1
    return cmd


def cmd_watch(args) -> int:
    st = Settings(args)
    print(f"== HAProxy: live view through {st.vip}:{st.api_port} for {st.seconds} s (Ctrl-C to stop) ==")
    print("time      answer  node      ms     HAProxy view of the API backend")
    end = time.time() + st.seconds
    counts: Counter = Counter()
    bad = 0
    last_state = None
    try:
        while time.time() < end:
            t0 = time.time()
            node, answer = "-", ""
            try:
                code, body = http_get(st.vip, st.api_port, "/health", TLS_API_ENABLED, st.timeout)
                answer = str(code)
                if code == 200:
                    node = str(json.loads(body).get("node", "?"))
                    counts[node] += 1
                else:
                    bad += 1
            except Exception as e:
                answer = describe_error(e)
                bad += 1
            ms = (time.time() - t0) * 1000
            stats = safe_stats(st)
            state = state_line(stats, BACKENDS["api"]) if stats is not None else "(no stats)"
            note = ""
            if last_state is not None and state != last_state:
                note = "   <-- HAProxy changed a server state"
            last_state = state
            print(f"{time.strftime('%H:%M:%S')}  {answer:<6}  {node:<8}  {ms:>4.0f}   {state}{note}", flush=True)
            time.sleep(max(0.0, 1.0 - (time.time() - t0)))
    except KeyboardInterrupt:
        print("\ninterrupted")
    total = sum(counts.values()) + bad
    print(f"\n{total} request(s): " + (", ".join(f"{k}={v}" for k, v in sorted(counts.items())) or "none answered")
          + f"; not answered with 200: {bad}")
    return 0 if bad == 0 and counts else 1


# ----------------------------------------------------------------------------- CLI parsing

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="FlowFirst HAProxy demo: see how the VIP spreads REST, MariaDB, RabbitMQ and Swift traffic over the nodes."
    )
    sub = p.add_subparsers(dest="cmd")

    def common(sp):
        sp.add_argument("--vip", default="", help="HAProxy address (default: FLOWFIRST_VIP from .env)")
        sp.add_argument("--nodes", default=DEFAULT_NODES,
                        help="nodes to check directly: node1,node2,node3 or host:api-port (default: %(default)s)")
        sp.add_argument("--requests", type=int, default=DEFAULT_REQUESTS, help="requests/connections per stage (default: %(default)s)")
        sp.add_argument("--timeout", type=float, default=DEFAULT_TIMEOUT_S, help="seconds per network step")
        for name in ("api", "stats", "mariadb", "rabbitmq", "swift"):
            sp.add_argument(f"--{name}-port", type=int, default=0, help=f"override the VIP {name} port from .env")

    r = sub.add_parser("run", help="full demo: every stage (default)")
    common(r)
    r.set_defaults(func=cmd_run)

    helps = {
        "status": "which VIP and node ports accept connections",
        "backends": "state of every backend server according to HAProxy",
        "api": "N requests through the VIP, which node answered",
        "mariadb": "N connections through the VIP, which Galera node answered",
        "rabbitmq": "N AMQP connections through the VIP",
        "swift": "N requests through the VIP to the Swift proxies",
    }
    for key, text in helps.items():
        sp = sub.add_parser(key, help=text)
        common(sp)
        sp.set_defaults(func=_single_stage(key))

    w = sub.add_parser("watch", help="live view for a failover test")
    common(w)
    w.add_argument("--seconds", type=int, default=60, help="how long to watch (default: %(default)s)")
    w.set_defaults(func=cmd_watch)
    return p


def main(argv: Optional[List[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if not args.cmd:
        args = parser.parse_args(["run"] + list(argv if argv is not None else sys.argv[1:]))
    try:
        if args.requests < 1:
            raise ValueError("--requests must be at least 1")
        if getattr(args, "seconds", 1) < 1:
            raise ValueError("--seconds must be at least 1")
        return args.func(args)
    except ValueError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return 2
    except Exception as e:
        print(f"ERROR: {describe_error(e)}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
