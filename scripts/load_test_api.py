#!/usr/bin/env python3
"""
FlowFirst REST API load generator (Scenarios 61 and 62).

Drives many concurrent API invocations at the VIP and reports, per step:
throughput, latency percentiles, error classes, which node answered, and the
RabbitMQ backlog. Python 3.9+ standard library only - nothing to install.

How it works
  * "Concurrency" = virtual users (VUs) that each keep exactly one request in
    flight (closed loop): send, wait for the answer, send the next.
  * The test climbs LOAD_STEPS (e.g. 50,100,250,...). Each step ramps its VUs up
    over LOAD_RAMP_S, then measures for LOAD_STEP_DURATION_S (ramp excluded).
  * A step PASSES when the error rate <= LOAD_MAX_ERROR_PCT and p95 latency of
    successful requests <= LOAD_SLO_P95_MS and the load generator itself kept up.
    The climb stops at the first failing step (use --no-stop to continue).
  * HTTP 200 only means "published to RabbitMQ". After the last step the script
    waits for the queues to drain; the time that takes is the real end-to-end
    capacity. Compare the 200 counts it prints with the MariaDB row counts.

Every parameter comes from the environment / .env (see src/config/.env.example,
"Load generator parameters"); command-line options override them.
"""
import argparse
import asyncio
import base64
import csv
import json
import os
import random
import re
import sys
import time
import urllib.error
import urllib.request
from collections import Counter

try:
    import resource
except ImportError:          # pragma: no cover - non-POSIX
    resource = None

KINDS = ("flow1", "flow2", "flow3", "health")
_NODE_RE = re.compile(rb'"(?:handled_by_node|node)":\s*"([^"]+)"')


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
def _load_env_file(path):
    """Minimal KEY=VALUE reader; never overrides variables already in the environment."""
    try:
        with open(path, encoding="utf-8") as fh:
            for raw in fh:
                line = raw.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, _, val = line.partition("=")
                key = key.strip()
                val = val.strip().strip('"').strip("'")
                if key and key not in os.environ:
                    os.environ[key] = val
    except OSError:
        pass


def _env(name, default):
    val = os.environ.get(name, "").strip()
    return val if val else str(default)


def _bool(text):
    return str(text).strip().lower() in ("1", "true", "yes", "on")


def parse_args():
    here = os.path.dirname(os.path.abspath(__file__))
    for candidate in (os.path.join(here, "..", ".env"), "/opt/flowfirst/.env"):
        _load_env_file(candidate)

    vip = _env("FLOWFIRST_VIP", "127.0.0.1")
    ap = argparse.ArgumentParser(
        description="FlowFirst REST API load generator. Defaults come from LOAD_* variables in .env.")
    ap.add_argument("--target", default=_env("LOAD_TARGET_URL", "http://%s:%s" % (vip, _env("API_PORT", 8080))),
                    help="base URL (LOAD_TARGET_URL; default http://<FLOWFIRST_VIP>:<API_PORT>)")
    ap.add_argument("--steps", default=_env("LOAD_STEPS", "50,100,250,500,1000,2000,4000"),
                    help="comma list of concurrent in-flight requests per step (LOAD_STEPS)")
    ap.add_argument("--duration", type=float, default=float(_env("LOAD_STEP_DURATION_S", 30)),
                    help="measured seconds per step (LOAD_STEP_DURATION_S)")
    ap.add_argument("--ramp", type=float, default=float(_env("LOAD_RAMP_S", 5)),
                    help="seconds over which a step adds its VUs (LOAD_RAMP_S)")
    ap.add_argument("--mix", default=_env("LOAD_FLOW_MIX", "flow1:45,flow2:45,flow3:10"),
                    help="name:weight pairs from flow1,flow2,flow3,health (LOAD_FLOW_MIX)")
    ap.add_argument("--timeout", type=float, default=float(_env("LOAD_TIMEOUT_S", 30)),
                    help="seconds before a request counts as an error (LOAD_TIMEOUT_S)")
    ap.add_argument("--keepalive", default=_env("LOAD_KEEPALIVE", "true"),
                    help="true = reuse one TCP connection per VU, false = connection per request (LOAD_KEEPALIVE)")
    ap.add_argument("--slo-p95-ms", type=float, default=float(_env("LOAD_SLO_P95_MS", 1000)),
                    help="step fails when p95 of successful requests exceeds this (LOAD_SLO_P95_MS)")
    ap.add_argument("--max-error-pct", type=float, default=float(_env("LOAD_MAX_ERROR_PCT", 1.0)),
                    help="step fails when more than this %% of requests fail (LOAD_MAX_ERROR_PCT)")
    ap.add_argument("--think-ms", type=float, default=float(_env("LOAD_THINK_MS", 0)),
                    help="average pause after each response, +/-50%% jitter; 0 = hammer (LOAD_THINK_MS)")
    ap.add_argument("--item-id-base", type=int, default=int(_env("LOAD_ITEM_ID_BASE", 9000000)),
                    help="first item_id; each request gets the next number (LOAD_ITEM_ID_BASE)")
    ap.add_argument("--drain-timeout", type=float, default=float(_env("LOAD_DRAIN_TIMEOUT_S", 300)),
                    help="seconds to wait for RabbitMQ queues to empty after the last step; 0 = skip (LOAD_DRAIN_TIMEOUT_S)")
    ap.add_argument("--interval", type=float, default=float(_env("LOAD_REPORT_INTERVAL_S", 0)),
                    help="also print/save a result line every N seconds inside each step (soak trend); 0 = off "
                         "(LOAD_REPORT_INTERVAL_S)")
    ap.add_argument("--csv", default=None, help="write the per-step summary here (default ./load_test_<time>.csv)")
    ap.add_argument("--no-stop", action="store_true", help="keep climbing after a step fails")
    ap.add_argument("--yes", action="store_true", help="do not ask for confirmation")
    ap.add_argument("--dry-run", action="store_true", help="print the plan and exit")
    args = ap.parse_args()

    args.keepalive = _bool(args.keepalive)
    try:
        args.step_list = [int(x) for x in args.steps.split(",") if x.strip()]
        if not args.step_list or min(args.step_list) < 1:
            raise ValueError
    except ValueError:
        ap.error("--steps must be a comma list of positive integers, e.g. 50,100,500")
    mix = []
    for part in args.mix.split(","):
        if not part.strip():
            continue
        name, _, weight = part.partition(":")
        name = name.strip()
        if name not in KINDS:
            ap.error("--mix: unknown request '%s' (use %s)" % (name, ", ".join(KINDS)))
        try:
            mix.append((name, float(weight or 1)))
        except ValueError:
            ap.error("--mix: bad weight in '%s'" % part)
    if not mix or sum(w for _, w in mix) <= 0:
        ap.error("--mix needs at least one entry with a positive weight")
    args.mix_names = [n for n, _ in mix]
    args.mix_weights = [w for _, w in mix]

    u = re.match(r"^https?://([^/:]+)(?::(\d+))?", args.target)
    if not u:
        ap.error("--target must look like http://host:port")
    args.host = u.group(1)
    args.port = int(u.group(2) or 80)
    args.host_header = "%s:%d" % (args.host, args.port)
    if args.csv is None:
        args.csv = "load_test_%s.csv" % time.strftime("%Y%m%d_%H%M%S")
    args.rmq_user = _env("RABBITMQ_USER", "flowuser")
    args.rmq_password = _env("RABBITMQ_PASSWORD", "flowpassword")
    args.rmq_port = _env("RABBITMQ_MGMT_PORT", 15672)
    args.rmq_hosts = [h for h in (_env("NODE1_IP", ""), _env("NODE2_IP", ""), _env("NODE3_IP", ""),
                                   _env("RABBITMQ_HOST", "")) if h]
    return args


# ---------------------------------------------------------------------------
# Minimal HTTP/1.1 client on asyncio streams
# ---------------------------------------------------------------------------
class Conn:
    __slots__ = ("r", "w")

    def __init__(self, r, w):
        self.r, self.w = r, w

    def close(self):
        try:
            self.w.close()
        except Exception:
            pass


async def _read_chunked(r):
    out = b""
    while True:
        size = int((await r.readline()).split(b";")[0].strip() or b"0", 16)
        if size == 0:
            while (await r.readline()) not in (b"\r\n", b"\n", b""):
                pass
            return out
        out += await r.readexactly(size)
        await r.readline()


async def http_request(conn, args, method, path, body, keep):
    head = ["%s %s HTTP/1.1" % (method, path), "Host: " + args.host_header,
            "User-Agent: flowfirst-load/1", "Accept: application/json"]
    if body is not None:
        head += ["Content-Type: application/json", "Content-Length: %d" % len(body)]
    head.append("Connection: keep-alive" if keep else "Connection: close")
    conn.w.write(("\r\n".join(head) + "\r\n\r\n").encode() + (body or b""))
    await conn.w.drain()

    line = await conn.r.readline()
    if not line:
        raise ConnectionResetError("connection closed before a response")
    parts = line.split(None, 2)
    status = int(parts[1])
    http10 = parts[0].strip().upper() == b"HTTP/1.0"      # Process 1 itself answers HTTP/1.0 (no keep-alive)
    clen, chunked, close = None, False, http10
    while True:
        h = await conn.r.readline()
        if h in (b"\r\n", b"\n", b""):
            break
        k, _, v = h.partition(b":")
        k, v = k.strip().lower(), v.strip().lower()
        if k == b"content-length":
            clen = int(v)
        elif k == b"transfer-encoding" and b"chunked" in v:
            chunked = True
        elif k == b"connection":
            close = b"close" in v or (http10 and b"keep-alive" not in v)
    if chunked:
        data = await _read_chunked(conn.r)
    elif clen is not None:
        data = await conn.r.readexactly(clen)
    else:
        data, close = await conn.r.read(), True
    return status, data, not close


def make_request(kind, n):
    """Return (method, path, body) for one request. n is the unique item id."""
    if kind == "health":
        return "GET", "/health", None
    if kind == "flow1":
        doc = {"item_id": n, "counter": n % 1000, "initial_data": "load-test"}
    elif kind == "flow2":
        doc = {"item_id": n, "value": 20 + n % 30, "initial_data": "load-test"}
    else:
        doc = {"item_id": n, "remote_metric": 40 + n % 50, "initial_data": "load-test"}
    return "POST", "/api/" + kind, json.dumps(doc).encode()


# ---------------------------------------------------------------------------
# Statistics
# ---------------------------------------------------------------------------
def pct(sorted_vals, p):
    if not sorted_vals:
        return 0.0
    return sorted_vals[min(len(sorted_vals) - 1, int(p * (len(sorted_vals) - 1)))]


class StepStats:
    def __init__(self):
        self.active = False
        self.lat_ms = []                 # successful (2xx) requests only
        self.total = 0
        self.ok = 0
        self.status = Counter()
        self.errors = Counter()          # timeout / connect / reset / http_5xx ...
        self.nodes = Counter()
        self.accepted = Counter()        # 2xx per kind (for the DB cross-check)
        self.lag_ms = []

    def record(self, kind, status, err, node, lat_ms):
        if not self.active:
            return
        self.total += 1
        if err:
            self.errors[err] += 1
        else:
            self.status[status] += 1
            if 200 <= status < 300:
                self.ok += 1
                self.lat_ms.append(lat_ms)
                self.accepted[kind] += 1
                if node:
                    self.nodes[node] += 1
            else:
                self.errors["http_%d" % status] += 1


# ---------------------------------------------------------------------------
# RabbitMQ queue depth (management API) - optional
# ---------------------------------------------------------------------------
def fetch_queue_depth(args):
    """Return {queue: messages} or None when the management API is unreachable."""
    token = base64.b64encode(("%s:%s" % (args.rmq_user, args.rmq_password)).encode()).decode()
    for host in args.rmq_hosts:
        url = "http://%s:%s/api/queues/%%2F?columns=name,messages" % (host, args.rmq_port)
        req = urllib.request.Request(url, headers={"Authorization": "Basic " + token})
        try:
            with urllib.request.urlopen(req, timeout=4) as resp:
                return {q["name"]: int(q.get("messages", 0)) for q in json.loads(resp.read())}
        except (urllib.error.URLError, OSError, ValueError, KeyError):
            continue
    return None


class QueueMonitor:
    def __init__(self, args):
        self.args = args
        self.depth = None            # latest {queue: messages}
        self.total = None
        self.max_total = 0
        self.ok = False
        self.first_poll = asyncio.Event()     # set once the first poll attempt has finished

    async def run(self, stop):
        loop = asyncio.get_running_loop()
        while not stop.is_set():
            depth = await loop.run_in_executor(None, fetch_queue_depth, self.args)
            if depth is not None:
                self.ok, self.depth = True, depth
                self.total = sum(depth.values())
                self.max_total = max(self.max_total, self.total)
            self.first_poll.set()
            try:
                await asyncio.wait_for(stop.wait(), 5)
            except asyncio.TimeoutError:
                pass


# ---------------------------------------------------------------------------
# Load generation
# ---------------------------------------------------------------------------
class Counter_:
    """Run-wide totals (ramp and measured windows together)."""
    n = 0
    def __init__(self):
        self.ok = Counter()     # 2xx answers per kind
        self.failed = 0         # requests with an error or non-2xx answer


async def vu(args, stats, stop, delay, ids):
    """One virtual user: one request in flight at a time."""
    await asyncio.sleep(delay)
    conn = None
    rng = random.Random()
    while not stop.is_set():
        kind = rng.choices(args.mix_names, args.mix_weights)[0]
        ids.n += 1
        method, path, body = make_request(kind, args.item_id_base + ids.n)
        t0 = time.monotonic()
        status, err, node, keep = 0, None, None, args.keepalive
        try:
            if conn is None:
                r, w = await asyncio.wait_for(asyncio.open_connection(args.host, args.port), args.timeout)
                conn = Conn(r, w)
            remaining = max(0.1, args.timeout - (time.monotonic() - t0))
            status, data, reusable = await asyncio.wait_for(
                http_request(conn, args, method, path, body, args.keepalive), remaining)
            keep = keep and reusable
            m = _NODE_RE.search(data)
            node = m.group(1).decode("ascii", "replace") if m else None
        except asyncio.TimeoutError:
            err, keep = "timeout", False
        except ConnectionRefusedError:
            err, keep = "refused", False
        except (ConnectionError, asyncio.IncompleteReadError, EOFError):
            err, keep = "reset", False
        except OSError as exc:
            err, keep = "os_%s" % (exc.errno or "error"), False
        except Exception as exc:          # malformed response etc.
            err, keep = "bad_response:%s" % type(exc).__name__, False
        stats.record(kind, status, err, node, (time.monotonic() - t0) * 1000.0)
        if not err and 200 <= status < 300:
            ids.ok[kind] += 1
        else:
            ids.failed += 1
        if not keep and conn is not None:
            conn.close()
            conn = None
        if err:
            await asyncio.sleep(0.05)     # do not spin on a dead target
        elif args.think_ms > 0:
            await asyncio.sleep(args.think_ms / 1000.0 * rng.uniform(0.5, 1.5))
    if conn is not None:
        conn.close()


async def lag_probe(stats, stop):
    """Event-loop lag: if the generator itself is saturated the numbers cannot be trusted."""
    while not stop.is_set():
        t0 = time.monotonic()
        await asyncio.sleep(0.05)
        if stats.active:
            stats.lag_ms.append(max(0.0, (time.monotonic() - t0 - 0.05) * 1000.0))


async def window_reporter(args, stats, monitor, stop, vus, windows, t0):
    """Every args.interval seconds of the measured period, report the numbers for just that window."""
    i_lat, i_total, i_ok, t_prev = 0, 0, 0, t0
    while True:
        try:
            await asyncio.wait_for(stop.wait(), args.interval)
            return                                  # step finished; the partial last window is not reported
        except asyncio.TimeoutError:
            pass
        now = time.monotonic()
        lat = sorted(stats.lat_ms[i_lat:])
        n_total, n_ok = stats.total - i_total, stats.ok - i_ok
        w = {"vus": vus, "t_s": round(now - t0), "rps": round(n_ok / (now - t_prev), 1),
             "p50_ms": round(pct(lat, 0.50), 1), "p95_ms": round(pct(lat, 0.95), 1),
             "p99_ms": round(pct(lat, 0.99), 1),
             "err_pct": round(100.0 * (n_total - n_ok) / n_total, 2) if n_total else 100.0,
             "queue": monitor.total}
        windows.append(w)
        say("    t=%5ds  %8.1f req/s  p50 %6.0f  p95 %6.0f  p99 %6.0f ms  err %5.2f%%  queue %s" % (
            w["t_s"], w["rps"], w["p50_ms"], w["p95_ms"], w["p99_ms"], w["err_pct"], w["queue"]))
        i_lat, i_total, i_ok, t_prev = len(stats.lat_ms), stats.total, stats.ok, now


async def run_step(args, vus, monitor, ids, windows):
    stats = StepStats()
    stop = asyncio.Event()
    q_start = monitor.total
    monitor.max_total = monitor.total or 0
    tasks = [asyncio.ensure_future(vu(args, stats, stop, args.ramp * i / vus, ids)) for i in range(vus)]
    lag_task = asyncio.ensure_future(lag_probe(stats, stop))
    await asyncio.sleep(args.ramp)
    stats.active = True
    t0 = time.monotonic()
    win_task = None
    if args.interval > 0:
        say("    measuring from %s (t=0 below)" % time.strftime("%H:%M:%S"))
        win_task = asyncio.ensure_future(window_reporter(args, stats, monitor, stop, vus, windows, t0))
    await asyncio.sleep(args.duration)
    elapsed = time.monotonic() - t0
    stats.active = False
    stop.set()
    _, pending = await asyncio.wait(tasks, timeout=args.timeout + 2)
    for t in pending:
        t.cancel()
    await asyncio.gather(*pending, lag_task, *([win_task] if win_task else []), return_exceptions=True)

    lat = sorted(stats.lat_ms)
    lag = sorted(stats.lag_ms)
    err_total = stats.total - stats.ok
    row = {
        "vus": vus,
        "requests": stats.total,
        "ok": stats.ok,
        "rps": round(stats.ok / elapsed, 1) if elapsed else 0.0,
        "p50_ms": round(pct(lat, 0.50), 1),
        "p95_ms": round(pct(lat, 0.95), 1),
        "p99_ms": round(pct(lat, 0.99), 1),
        "max_ms": round(lat[-1], 1) if lat else 0.0,
        "err_pct": round(100.0 * err_total / stats.total, 2) if stats.total else 100.0,
        "lag_p95_ms": round(pct(lag, 0.95), 1),
        "q_start": q_start,
        "q_end": monitor.total,
        "q_max": monitor.max_total if monitor.ok else None,
        "errors": dict(stats.errors),
        "status": dict(stats.status),
        "nodes": dict(stats.nodes),
        "accepted": dict(stats.accepted),
    }
    reasons = []
    if row["requests"] == 0:
        reasons.append("no requests completed")
    if row["err_pct"] > args.max_error_pct:
        reasons.append("errors %.2f%% > %.2f%%" % (row["err_pct"], args.max_error_pct))
    if row["ok"] and row["p95_ms"] > args.slo_p95_ms:
        reasons.append("p95 %.0f ms > %.0f ms" % (row["p95_ms"], args.slo_p95_ms))
    if row["lag_p95_ms"] > 100:
        reasons.append("LOAD GENERATOR SATURATED (loop lag p95 %.0f ms) - add client hosts" % row["lag_p95_ms"])
    row["verdict"] = "PASS" if not reasons else "FAIL"
    row["reasons"] = "; ".join(reasons)
    return row


# ---------------------------------------------------------------------------
# Reporting and pre-flight
# ---------------------------------------------------------------------------
def say(msg=""):
    print(msg, flush=True)


def print_step(row):
    say("  %-5s %6d VUs  %8.1f req/s  p50 %6.0f  p95 %6.0f  p99 %6.0f  max %7.0f ms  err %5.2f%%  queue %s -> %s (max %s)" % (
        row["verdict"], row["vus"], row["rps"], row["p50_ms"], row["p95_ms"], row["p99_ms"], row["max_ms"],
        row["err_pct"], row["q_start"], row["q_end"], row["q_max"]))
    if row["errors"]:
        say("        errors : " + ", ".join("%s=%d" % kv for kv in sorted(row["errors"].items(), key=lambda x: -x[1])))
    if row["nodes"]:
        say("        served by: " + ", ".join("%s=%d" % kv for kv in sorted(row["nodes"].items())))
    if row["reasons"]:
        say("        reason : " + row["reasons"])


def check_fd_limit(args):
    need = max(args.step_list) + 200
    if resource is None:
        return
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    if soft != resource.RLIM_INFINITY and soft < need:
        target = need if hard == resource.RLIM_INFINITY else min(hard, need)
        try:
            resource.setrlimit(resource.RLIMIT_NOFILE, (target, hard))
        except (ValueError, OSError):
            pass
        soft, _ = resource.getrlimit(resource.RLIMIT_NOFILE)
    if soft != resource.RLIM_INFINITY and soft < need:
        say("ERROR: open-file limit is %d but %d are needed for %d VUs. Run 'ulimit -n %d' (or raise the hard limit "
            "in /etc/security/limits.d/) and retry." % (soft, need, max(args.step_list), max(need, 65536)))
        sys.exit(2)
    if not args.keepalive:
        try:
            with open("/proc/sys/net/ipv4/ip_local_port_range") as fh:
                lo, hi = [int(x) for x in fh.read().split()]
            say("NOTE: LOAD_KEEPALIVE=false opens one connection per request. If the client side ends up holding closed "
                "connections in TIME_WAIT (check 'ss -s'), the local port range %d-%d (%d ports, ~60 s each) caps the "
                "sustained rate at roughly %d req/s; widen net.ipv4.ip_local_port_range if you see 'Cannot assign "
                "requested address' errors." % (lo, hi, hi - lo, (hi - lo) // 60))
        except (OSError, ValueError):
            pass


def write_windows_csv(path, windows):
    cols = ["vus", "t_s", "rps", "p50_ms", "p95_ms", "p99_ms", "err_pct", "queue"]
    with open(path, "w", newline="", encoding="utf-8") as fh:
        wr = csv.writer(fh)
        wr.writerow(cols)
        for w in windows:
            wr.writerow([w[c] for c in cols])


def write_csv(path, rows):
    cols = ["vus", "requests", "ok", "rps", "p50_ms", "p95_ms", "p99_ms", "max_ms", "err_pct", "lag_p95_ms",
            "q_start", "q_end", "q_max", "verdict", "reasons", "errors", "nodes"]
    with open(path, "w", newline="", encoding="utf-8") as fh:
        wr = csv.writer(fh)
        wr.writerow(cols)
        for r in rows:
            wr.writerow([json.dumps(r[c], sort_keys=True) if isinstance(r[c], dict) else r[c] for c in cols])


async def preflight(args):
    """One GET /health; return the node that answered or raise."""
    r, w = await asyncio.wait_for(asyncio.open_connection(args.host, args.port), 10)
    conn = Conn(r, w)
    try:
        status, data, _ = await asyncio.wait_for(http_request(conn, args, "GET", "/health", None, False), 15)
    finally:
        conn.close()
    if status != 200:
        raise RuntimeError("GET /health returned HTTP %d" % status)
    m = _NODE_RE.search(data)
    return m.group(1).decode() if m else "?"


async def drain(args, monitor):
    if args.drain_timeout <= 0:
        return
    if not monitor.ok:
        say("\nQueue drain check skipped: RabbitMQ management API not reachable "
            "(tried %s port %s). Check queue depth with rabbitmqctl instead." % (",".join(args.rmq_hosts) or "-", args.rmq_port))
        return
    start_depth = monitor.total
    say("\nWaiting for RabbitMQ queues to drain (backlog %d, timeout %.0f s) ..." % (start_depth, args.drain_timeout))
    t0 = time.monotonic()
    while time.monotonic() - t0 < args.drain_timeout:
        if monitor.total == 0:
            took = time.monotonic() - t0
            rate = (start_depth / took) if took > 0 else 0
            say("  Drained in %.0f s (about %.0f queued messages/s across all stages)." % (took, rate))
            return
        await asyncio.sleep(5)
    say("  NOT drained after %.0f s: %d messages still queued. The pipeline cannot keep up with the rate that was "
        "accepted - that is the real limit." % (args.drain_timeout, monitor.total))


async def main_async(args):
    plan_reqs = len(args.step_list)
    say("FlowFirst API load test")
    say("  target        : http://%s  keep-alive=%s  timeout=%.0fs" % (args.host_header, args.keepalive, args.timeout))
    say("  steps (VUs)   : %s   (ramp %.0fs + measure %.0fs each; stops at first FAIL: %s)" % (
        ",".join(str(s) for s in args.step_list), args.ramp, args.duration, not args.no_stop))
    say("  think time    : %s" % ("none (closed loop: each VU re-sends immediately)" if args.think_ms <= 0
                                    else "%.0f ms average per VU" % args.think_ms))
    say("  request mix   : " + ", ".join("%s:%g" % (n, w) for n, w in zip(args.mix_names, args.mix_weights)))
    say("  pass criteria : errors <= %.2f%%, p95 <= %.0f ms" % (args.max_error_pct, args.slo_p95_ms))
    say("  trend lines   : %s" % ("every %.0f s inside each step" % args.interval if args.interval > 0 else "off"))
    say("  item_id range : starts at %d (every request gets the next number)" % (args.item_id_base + 1))
    say("  this test WRITES real rows to MariaDB, objects to Swift and messages through RabbitMQ.")
    if args.dry_run:
        return 0
    if not args.yes and sys.stdin.isatty():
        if input("Proceed? [y/N] ").strip().lower() not in ("y", "yes"):
            say("Aborted.")
            return 1
    check_fd_limit(args)
    try:
        node = await preflight(args)
    except Exception as exc:
        say("ERROR: cannot reach %s/health: %s" % (args.host_header, exc))
        return 2
    say("  pre-flight    : GET /health OK (answered by %s)" % node)

    mon_stop = asyncio.Event()
    monitor = QueueMonitor(args)
    mon_task = asyncio.ensure_future(monitor.run(mon_stop))
    try:
        await asyncio.wait_for(monitor.first_poll.wait(), 15)
    except asyncio.TimeoutError:
        pass
    say("  queue monitor : %s" % ("RabbitMQ management API OK (backlog now %s)" % monitor.total if monitor.ok
                                  else "unavailable (queue columns will show None)"))
    say("")
    rows, ids, windows = [], Counter_(), []
    for vus in args.step_list:
        say("Step %d concurrent ..." % vus)
        row = await run_step(args, vus, monitor, ids, windows)
        rows.append(row)
        print_step(row)
        if row["verdict"] == "FAIL" and not args.no_stop:
            say("  Stopping: this step failed. Raise the relevant scale parameter or fix the bottleneck, then re-run.")
            break
        await asyncio.sleep(2)

    await drain(args, monitor)
    mon_stop.set()
    await asyncio.gather(mon_task, return_exceptions=True)

    passed = [r for r in rows if r["verdict"] == "PASS"]
    say("\n==================== SUMMARY ====================")
    if passed:
        best = max(passed, key=lambda r: r["rps"])
        say("Highest passing concurrency : %d    Peak passing throughput : %.1f req/s (at %d VUs)" % (
            max(r["vus"] for r in passed), best["rps"], best["vus"]))
    else:
        say("No step passed. Lower the first LOAD_STEPS value or fix the failure shown above.")
    writes = {k: v for k, v in ids.ok.items() if k != "health"}
    say("HTTP 2xx for the whole run (ramp included): " + (
        ", ".join("%s=%d" % kv for kv in sorted(ids.ok.items())) or "none") + "    failed/non-2xx: %d" % ids.failed)
    say("item_id range used : %d .. %d" % (args.item_id_base + 1, args.item_id_base + ids.n))
    say("Check end-to-end in MariaDB once the queues are empty. Expected rows per flow = the 2xx counts above "
        "(%s), plus at most a few for requests that timed out client-side but were still accepted:" % (
            ", ".join("flow%s=%d" % (k[-1], v) for k, v in sorted(writes.items())) or "none"))
    say("  SELECT flow_id, COUNT(*) FROM processed_messages WHERE item_id BETWEEN %d AND %d GROUP BY flow_id;" % (
        args.item_id_base + 1, args.item_id_base + ids.n))
    write_csv(args.csv, rows)
    say("Per-step results written to %s" % args.csv)
    if windows:
        wpath = re.sub(r"(\.csv)?$", ".windows.csv", args.csv, count=1)
        write_windows_csv(wpath, windows)
        say("Per-window trend lines written to %s" % wpath)
    return 0 if passed and rows[-1]["verdict"] == "PASS" else 1


def main():
    args = parse_args()
    try:
        sys.exit(asyncio.run(main_async(args)))
    except KeyboardInterrupt:
        say("\nInterrupted.")
        sys.exit(130)


if __name__ == "__main__":
    main()
