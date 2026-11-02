#!/usr/bin/env python3
"""FlowFirst demo: how to use OpenStack Swift object storage across the cluster nodes.

Talks to the Swift proxy on mgmtnode1..mgmtnode3 directly (one python-swiftclient connection per node, no REST API, no pipeline
process), so it isolates the object-store layer and shows which node handles each request. All objects live in a demo
container named "flowfirst_demo_<run id>", so the pipeline container (SWIFT_CONTAINER, default flowfirst_messages) is
never touched.

Stages:
  status     authenticate against every proxy (tempauth) and show the account totals it reports
  write      write one object through EACH node, read every object back through EVERY node (ETag/content compared)
  list       container listing: an object written through one node shows up in the listing of every node
  lifecycle  PUT via node A -> POST metadata via node B -> HEAD via node C -> DELETE via A -> 404 via B and C
  helpers    the FlowFirst helpers (src/swift/swift_storage.py) through the load-balanced VIP, as the pipeline uses them

Usage:
    python3 scripts/swift_object_demo.py                          full demo (same as 'run')
    python3 scripts/swift_object_demo.py run --keep               ...and leave the demo container in place
    python3 scripts/swift_object_demo.py run --nodes mgmtnode1,mgmtnode2  only use these proxies (e.g. mgmtnode3 is down)
    python3 scripts/swift_object_demo.py status
    python3 scripts/swift_object_demo.py write
    python3 scripts/swift_object_demo.py list
    python3 scripts/swift_object_demo.py lifecycle
    python3 scripts/swift_object_demo.py helpers
    python3 scripts/swift_object_demo.py cleanup --run-id <id>    delete the container of a run left behind by --keep

--nodes accepts mgmtnode1, mgmtnode2, mgmtnode3, vip (the HAProxy address SWIFT_AUTH_URL), host[:port] or a full auth URL.

Where to run it: on mgmtnode1..remotenode1 or any workstation that can reach TCP 8888 (proxy; 8090 for the VIP) and has the FlowFirst
.env (SWIFT_USER, SWIFT_KEY). Connections honour TLS_SWIFT_ENABLED through src.tls.tls (fails closed). The key is never
printed and errors print only the exception type and, for Swift, the HTTP status.

Why connect to a node and not the VIP: tempauth returns the storage URL of the host you authenticated against, so after
logging in at http://<node>:8888/auth/v1.0 every later call of that connection stays on that node's proxy.
"""
import argparse
import hashlib
import json
import os
import sys
import time
import uuid
from typing import Callable, Dict, List, Optional, Tuple

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from swiftclient.client import ClientException, Connection  # noqa: E402

from src.config.config import (  # noqa: E402
    NODE1_IP,
    NODE2_IP,
    NODE3_IP,
    NODE_NAME,
    SWIFT_AUTH_URL,
    SWIFT_AUTH_VERSION,
    SWIFT_CONTAINER,
    SWIFT_ENABLED,
    SWIFT_KEY,
    SWIFT_USER,
    TLS_SWIFT_ENABLED,
)
from src.swift import swift_storage  # noqa: E402
from src.tls.tls import swift_kwargs  # noqa: E402

DEMO_PREFIX = "flowfirst_demo"          # container name = DEMO_PREFIX + "_" + run id
DEFAULT_NODES = "mgmtnode1,mgmtnode2,mgmtnode3"
DEFAULT_TIMEOUT_S = 10.0
POLL_INTERVAL_S = 0.2


def _proxy_port() -> int:
    try:
        return int(os.getenv("SWIFT_PROXY_PORT", "8888") or 8888)
    except ValueError:
        return 8888


SWIFT_PROXY_PORT = _proxy_port()

Target = Tuple[str, str]                # (label, auth url)


# ----------------------------------------------------------------------------- pure helpers (unit-tested)

def get_scheme() -> str:
    """https when TLS_SWIFT_ENABLED, else http."""
    return "https" if TLS_SWIFT_ENABLED else "http"


def auth_url_for(host: str, port: Optional[int] = None) -> str:
    return f"{get_scheme()}://{host}:{port if port is not None else SWIFT_PROXY_PORT}/auth/v1.0"


def named_targets() -> Dict[str, str]:
    """Label -> auth URL: the three node-local proxies plus the load-balanced VIP address."""
    return {
        "mgmtnode1": auth_url_for(NODE1_IP),
        "mgmtnode2": auth_url_for(NODE2_IP),
        "mgmtnode3": auth_url_for(NODE3_IP),
        "vip": SWIFT_AUTH_URL,
    }


def endpoint_of(auth_url: str) -> str:
    """'http://10.0.0.1:8888/auth/v1.0' -> '10.0.0.1:8888' (what a table should show)."""
    rest = auth_url.split("://", 1)[-1]
    return rest.split("/", 1)[0]


def resolve_target(spec: str, targets: Optional[Dict[str, str]] = None) -> Target:
    """'mgmtnode2' -> ('mgmtnode2', 'http://<NODE2_IP>:8888/auth/v1.0'); '10.0.0.1:8888' and full URLs are accepted too."""
    spec = (spec or "").strip()
    if not spec:
        raise ValueError("empty node name")
    targets = targets if targets is not None else named_targets()
    if spec in targets:
        return spec, targets[spec]
    if "://" in spec:
        return endpoint_of(spec), spec
    if ":" in spec:
        host, p_str = spec.rsplit(":", 1)
        try:
            return spec, auth_url_for(host.strip(), int(p_str.strip()))
        except ValueError:
            raise ValueError(f"invalid port in target '{spec}'")
    return spec, auth_url_for(spec)


def resolve_targets(csv: str, targets: Optional[Dict[str, str]] = None) -> List[Target]:
    """'mgmtnode1,mgmtnode2' -> [(label, auth url), ...] with duplicate URLs omitted."""
    out, seen = [], set()
    for part in (csv or "").split(","):
        part = part.strip()
        if not part:
            continue
        label, url = resolve_target(part, targets)
        if url not in seen:
            seen.add(url)
            out.append((label, url))
    if not out:
        raise ValueError("no nodes given")
    return out


def container_name(run_id: str) -> str:
    """The one container a run uses. Never equal to the pipeline container."""
    return f"{DEMO_PREFIX}_{run_id}"


def etag_of(data: bytes) -> str:
    """Swift's ETag of a normal object is the MD5 of its content (an integrity checksum, not a security measure)."""
    return hashlib.md5(data, usedforsecurity=False).hexdigest()


def render_table(rows: List[dict], columns: List[Tuple[str, str]]) -> str:
    """Fixed-width text table. rows: list of dicts, columns: list of (key, heading)."""
    cells = [[str(r.get(k, "")) if r.get(k) is not None else "" for k, _ in columns] for r in rows]
    widths = [max([len(h)] + [len(c[i]) for c in cells]) for i, (_, h) in enumerate(columns)]
    line = "  ".join(h.ljust(widths[i]) for i, (_, h) in enumerate(columns))
    out = [line, "  ".join("-" * w for w in widths)]
    out += ["  ".join(c[i].ljust(widths[i]) for i in range(len(columns))) for c in cells]
    return "\n".join(out)


def describe_error(e: Exception) -> str:
    """Safe text for a failure: type and HTTP status only, never the message (it can hold URLs and hosts)."""
    if isinstance(e, ClientException) and e.http_status:
        return f"HTTP {e.http_status}"
    return type(e).__name__


def wait_for(check: Callable[[], Tuple[bool, str]], timeout_s: float, interval_s: float = POLL_INTERVAL_S) -> Tuple[bool, str, float]:
    """Poll check() -> (done, detail) until done or timeout. Returns (done, last detail, elapsed ms).

    Swift is eventually consistent: a replica or a listing may be a moment behind, so reads are polled, not assumed."""
    start = time.time()
    deadline = start + timeout_s
    while True:
        done, detail = check()
        if done or time.time() >= deadline:
            return done, detail, (time.time() - start) * 1000
        time.sleep(interval_s)


# ----------------------------------------------------------------------------- Swift access

class Session:
    """One python-swiftclient Connection to ONE proxy. The connection authenticates on its first request."""

    def __init__(self, label: str, auth_url: str, timeout_s: float):
        self.label = label
        self.auth_url = auth_url
        self.conn = Connection(
            authurl=auth_url,
            user=SWIFT_USER,
            key=SWIFT_KEY,
            auth_version=SWIFT_AUTH_VERSION,
            retries=1,
            timeout=timeout_s,
            **swift_kwargs(),
        )

    def close(self) -> None:
        try:
            self.conn.close()
        except Exception:
            pass


def open_sessions(targets: List[Target], timeout_s: float) -> List[Session]:
    sessions: List[Session] = []
    try:
        for label, url in targets:
            sessions.append(Session(label, url, timeout_s))
    except Exception:
        close_sessions(sessions)
        raise
    return sessions


def close_sessions(sessions: List[Session]) -> None:
    for s in sessions:
        s.close()


class Context:
    """Everything a stage needs."""

    def __init__(self, targets: List[Target], run_id: str, timeout_s: float):
        self.targets = targets
        self.run_id = run_id
        self.container = container_name(run_id)
        self.timeout_s = timeout_s


def ensure_container(ctx: Context) -> None:
    """Create the demo container through the first reachable proxy (PUT of a container is idempotent)."""
    last: Optional[Exception] = None
    for label, url in ctx.targets:
        s = Session(label, url, ctx.timeout_s)
        try:
            s.conn.put_container(ctx.container)
            return
        except Exception as e:
            last = e
        finally:
            s.close()
    raise last if last else RuntimeError("no proxy given")


def read_check(session: Session, container: str, name: str, content: bytes) -> Callable[[], Tuple[bool, str]]:
    """check() for wait_for: GET the object through this session's proxy and compare ETag and bytes."""
    expected = etag_of(content)

    def check() -> Tuple[bool, str]:
        try:
            headers, body = session.conn.get_object(container, name)
        except Exception as e:
            return False, describe_error(e)
        if headers.get("etag", "").strip('"') != expected or body != content:
            return False, "content differs"
        return True, "ok"
    return check


def gone_check(session: Session, container: str, name: str) -> Callable[[], Tuple[bool, str]]:
    """check() for wait_for: True once this session's proxy answers 404 for the object."""
    def check() -> Tuple[bool, str]:
        try:
            session.conn.head_object(container, name)
            return False, "still readable"
        except ClientException as e:
            return (e.http_status == 404), describe_error(e)
        except Exception as e:
            return False, describe_error(e)
    return check


# ----------------------------------------------------------------------------- stages

def stage_status(ctx: Context) -> Tuple[bool, List[dict]]:
    """Stage 1: log in to every proxy on its own and read the account totals it reports."""
    rows, reachable = [], 0
    for label, url in ctx.targets:
        row = {"node": label, "endpoint": endpoint_of(url), "status": "", "auth_ms": "n/a",
               "containers": "n/a", "objects": "n/a", "bytes": "n/a"}
        s = Session(label, url, ctx.timeout_s)
        t0 = time.time()
        try:
            headers = s.conn.head_account()
            row["auth_ms"] = f"{(time.time() - t0) * 1000:.0f}"
            row["containers"] = headers.get("x-account-container-count", "?")
            row["objects"] = headers.get("x-account-object-count", "?")
            row["bytes"] = headers.get("x-account-bytes-used", "?")
            row["status"] = "reachable"
            reachable += 1
        except Exception as e:
            row["status"] = f"unreachable ({describe_error(e)})"
        finally:
            s.close()
        rows.append(row)
    return reachable >= len(ctx.targets) // 2 + 1, rows


def stage_write_read(ctx: Context) -> Tuple[bool, List[dict]]:
    """Stage 2: one object written through each node, every object read back through every node."""
    sessions = open_sessions(ctx.targets, ctx.timeout_s)
    try:
        written = []
        rows: List[dict] = []
        all_ok = True
        for s in sessions:
            name = f"write/written-by-{s.label}.json"
            content = json.dumps({"run": ctx.run_id, "written_via": s.label, "client": NODE_NAME}, indent=2).encode("utf-8")
            try:
                etag = s.conn.put_object(ctx.container, name, contents=content, content_type="application/json",
                                         headers={"x-object-meta-written-via": s.label})
                if str(etag).strip('"') != etag_of(content):
                    raise ValueError("etag mismatch")
                written.append((s, name, content))
            except Exception as e:
                all_ok = False
                rows.append({"written_via": s.label, "read_via": "-", "read_ms": "-",
                             "result": f"FAIL: write failed ({describe_error(e)})"})
        for writer, name, content in written:
            for reader in sessions:
                ok, detail, ms = wait_for(read_check(reader, ctx.container, name, content), ctx.timeout_s)
                all_ok = all_ok and ok
                rows.append({"written_via": writer.label, "read_via": reader.label, "read_ms": f"{ms:.0f}",
                             "result": "PASS: same bytes and ETag" if ok else f"FAIL: {detail}"})
        rows.sort(key=lambda r: (r["written_via"], r["read_via"]))
        return all_ok, rows
    finally:
        close_sessions(sessions)


def stage_listing(ctx: Context) -> Tuple[bool, List[dict]]:
    """Stage 3: objects written through every node are in the container listing of every node."""
    sessions = open_sessions(ctx.targets, ctx.timeout_s)
    try:
        expected = []
        for s in sessions:
            name = f"list/{s.label}.txt"
            s.conn.put_object(ctx.container, name, contents=f"listed {ctx.run_id} via {s.label}".encode("utf-8"),
                              content_type="text/plain")
            expected.append(name)
        rows, all_ok = [], True
        for s in sessions:
            seen: List[str] = []

            def check(s=s) -> Tuple[bool, str]:
                try:
                    _, objects = s.conn.get_container(ctx.container, prefix="list/", full_listing=True)
                except Exception as e:
                    return False, describe_error(e)
                seen[:] = sorted(o["name"] for o in objects)
                missing = sorted(set(expected) - set(seen))
                return (not missing), ("missing " + ", ".join(missing)) if missing else "ok"
            ok, detail, ms = wait_for(check, ctx.timeout_s)
            all_ok = all_ok and ok
            rows.append({"listed_via": s.label, "objects": f"{len(set(seen) & set(expected))}/{len(expected)}",
                         "list_ms": f"{ms:.0f}", "result": "PASS: all objects listed" if ok else f"FAIL: {detail}"})
        return all_ok, rows
    finally:
        close_sessions(sessions)


def stage_lifecycle(ctx: Context) -> Tuple[bool, List[dict]]:
    """Stage 4: create, update metadata, read metadata and delete an object, each step through a different node."""
    sessions = open_sessions(ctx.targets, ctx.timeout_s)
    node_a = sessions[0]
    node_b = sessions[1 % len(sessions)]
    node_c = sessions[2 % len(sessions)]
    name = "lifecycle/object.txt"
    content = f"lifecycle {ctx.run_id}".encode("utf-8")
    rows: List[dict] = []

    def row(step: str, node: str, result: str) -> None:
        rows.append({"step": step, "node": node, "result": result})

    try:
        try:
            node_a.conn.put_object(ctx.container, name, contents=content, content_type="text/plain")
            row("1. PUT object", node_a.label, "PASS: stored")
        except Exception as e:
            row("1. PUT object", node_a.label, f"FAIL: {describe_error(e)}")
            return False, rows

        try:
            node_b.conn.post_object(ctx.container, name, {"x-object-meta-stage": "reviewed"})
            row("2. POST metadata", node_b.label, "PASS: metadata set")
        except Exception as e:
            row("2. POST metadata", node_b.label, f"FAIL: {describe_error(e)}")
            return False, rows

        def meta_check() -> Tuple[bool, str]:
            try:
                headers = node_c.conn.head_object(ctx.container, name)
            except Exception as e:
                return False, describe_error(e)
            return headers.get("x-object-meta-stage") == "reviewed", "metadata not visible yet"
        ok_meta, detail, _ms = wait_for(meta_check, ctx.timeout_s)
        row("3. HEAD metadata", node_c.label, "PASS: x-object-meta-stage=reviewed" if ok_meta else f"FAIL: {detail}")

        try:
            node_a.conn.delete_object(ctx.container, name)
            row("4. DELETE object", node_a.label, "PASS: deleted")
            deleted = True
        except Exception as e:
            row("4. DELETE object", node_a.label, f"FAIL: {describe_error(e)}")
            deleted = False

        ok_gone = deleted
        if deleted:
            for step, s in (("5. GET after delete", node_b), ("6. GET after delete", node_c)):
                ok, detail, _ms = wait_for(gone_check(s, ctx.container, name), ctx.timeout_s)
                ok_gone = ok_gone and ok
                row(step, s.label, "PASS: 404 Not Found" if ok else f"FAIL: {detail}")
        return ok_meta and ok_gone, rows
    finally:
        close_sessions(sessions)


def stage_helpers(ctx: Context) -> Tuple[bool, List[dict]]:
    """Stage 5: the FlowFirst helpers in src/swift/swift_storage.py, through the load-balanced VIP address."""
    if not SWIFT_ENABLED:
        return True, [{"step": "-", "call": "(all)", "result": "SKIP: SWIFT_ENABLED=false in .env, the helpers are switched off"}]
    ctr = ctx.container
    name = f"helpers/{ctx.run_id}.json"
    message = {"message_id": f"{ctx.run_id}-h1", "flow": "demo", "item_id": 1, "note": "stored by swift_object_demo"}
    rows: List[dict] = []

    ok_put, put_result = swift_storage.put_message_object(message, container_name=ctr, object_name=name)
    rows.append({"step": "1", "call": "put_message_object()",
                 "result": f"PASS: stored as {put_result}" if ok_put else "FAIL: write failed (see the log)"})
    if not ok_put:
        return False, rows

    ok_get, data, _headers = swift_storage.get_message_object(name, container_name=ctr)
    same = ok_get and data == message
    rows.append({"step": "2", "call": "get_message_object()",
                 "result": "PASS: payload identical" if same else "FAIL: object missing or different"})

    ok_list, objects, _err = swift_storage.list_message_objects(prefix="helpers/", container_name=ctr)
    listed = ok_list and any(o.get("name") == name for o in objects)
    rows.append({"step": "3", "call": "list_message_objects()",
                 "result": "PASS: object is in the listing" if listed else "FAIL: object not listed"})

    status = swift_storage.get_swift_status(container_name=ctr)
    connected = status.get("status") == "connected"
    rows.append({"step": "4", "call": "get_swift_status()",
                 "result": "PASS: connected" if connected else f"FAIL: status {status.get('status')}"})
    return same and listed and connected, rows


STAGES = {
    "status": ("Proxy nodes reachable one by one (tempauth login)", stage_status, [
        ("node", "node"), ("endpoint", "endpoint"), ("status", "status"), ("auth_ms", "login (ms)"),
        ("containers", "containers"), ("objects", "objects"), ("bytes", "bytes")]),
    "write": ("Write through every node, read back through every node", stage_write_read, [
        ("written_via", "written via"), ("read_via", "read via"), ("read_ms", "read (ms)"), ("result", "result")]),
    "list": ("Container listing as seen by every node", stage_listing, [
        ("listed_via", "listed via"), ("objects", "objects"), ("list_ms", "list (ms)"), ("result", "result")]),
    "lifecycle": ("Object lifecycle, each step through a different node", stage_lifecycle, [
        ("step", "step"), ("node", "node"), ("result", "result")]),
    "helpers": ("FlowFirst helpers (swift_storage.py) through the VIP", stage_helpers, [
        ("step", "step"), ("call", "call"), ("result", "result")]),
}
RUN_ORDER = ["status", "write", "list", "lifecycle", "helpers"]


# ----------------------------------------------------------------------------- cleanup

def delete_run_container(label: str, url: str, container: str, timeout_s: float = 10.0) -> int:
    """Delete every object and then the container of a run through one proxy. Returns how many things were deleted."""
    s = Session(label, url, timeout_s)
    deleted = 0
    try:
        try:
            _, objects = s.conn.get_container(container, full_listing=True)
        except ClientException as e:
            if e.http_status == 404:
                return 0
            raise
        for o in objects:
            try:
                s.conn.delete_object(container, o["name"])
                deleted += 1
            except ClientException as e:
                if e.http_status != 404:
                    raise
        s.conn.delete_container(container)
        return deleted + 1
    finally:
        s.close()


def cleanup_run(targets: List[Target], run_id: str) -> Optional[int]:
    """Try the proxies in order until one answers. None if none could be used."""
    for label, url in targets:
        try:
            return delete_run_container(label, url, container_name(run_id))
        except Exception:
            continue
    return None


# ----------------------------------------------------------------------------- commands

def _make_context(args) -> Context:
    if not SWIFT_USER or not SWIFT_KEY:
        raise ValueError("SWIFT_USER and SWIFT_KEY must be set in .env")
    return Context(resolve_targets(args.nodes), uuid.uuid4().hex[:8], args.timeout)


def _print_stage(key: str, ctx: Context, indent: str = "") -> bool:
    _title, func, cols = STAGES[key]
    ok, rows = func(ctx)
    print(indent + render_table(rows, cols).replace("\n", "\n" + indent))
    return ok


def cmd_run(args) -> int:
    ctx = _make_context(args)
    print("== FlowFirst Swift Object Storage Demo ==")
    print(f"run id     : {ctx.run_id}")
    print(f"proxies    : {', '.join(f'{lbl} ({endpoint_of(u)})' for lbl, u in ctx.targets)}")
    print(f"client     : {NODE_NAME}")
    print(f"container  : {ctx.container}  (the pipeline container {SWIFT_CONTAINER} is not touched)")
    print(f"tls mode   : {'ON (verified CA)' if TLS_SWIFT_ENABLED else 'OFF (plain HTTP)'}")

    overall = True
    created = False
    for n, key in enumerate(RUN_ORDER, 1):
        print(f"\n[{n}] {STAGES[key][0]}")
        try:
            if key != "status" and not created:
                ensure_container(ctx)
                created = True
            overall = _print_stage(key, ctx, "    ") and overall
        except Exception as e:
            print(f"    FAIL: {describe_error(e)}")
            overall = False
            if key == "status":
                break

    print(f"\n[{len(RUN_ORDER) + 1}] Summary & Result")
    print("    PASS: Swift object storage verified across cluster nodes." if overall
          else "    FAIL: one or more stages failed. Check proxy connectivity and the cluster (Scenarios 52 and 53).")

    if args.keep:
        print(f"\n    Demo container kept (run id {ctx.run_id}); Swift does not expire it by itself.")
        print(f"    Inspect: curl -s -H \"X-Auth-Token: <token>\" <storage url>/{ctx.container}")
        print(f"    Remove : python3 scripts/swift_object_demo.py cleanup --run-id {ctx.run_id}")
    else:
        n_del = cleanup_run(ctx.targets, ctx.run_id)
        print(f"\n    Cleaned up {n_del} demo object(s) and container (use --keep to retain them)." if n_del is not None
              else f"\n    WARNING: could not reach any proxy to clean up; remove it with: cleanup --run-id {ctx.run_id}")
    return 0 if overall else 1


def _single_stage(key: str):
    def cmd(args) -> int:
        ctx = _make_context(args)
        print(f"== Swift: {STAGES[key][0]} (run {ctx.run_id}) ==")
        try:
            if key != "status":
                ensure_container(ctx)
            ok = _print_stage(key, ctx)
        finally:
            if key != "status" and not getattr(args, "keep", False):
                cleanup_run(ctx.targets, ctx.run_id)
        return 0 if ok else 1
    return cmd


def cmd_cleanup(args) -> int:
    if not SWIFT_USER or not SWIFT_KEY:
        raise ValueError("SWIFT_USER and SWIFT_KEY must be set in .env")
    n_del = cleanup_run(resolve_targets(args.nodes), args.run_id)
    if n_del is None:
        print("ERROR: could not reach any proxy", file=sys.stderr)
        return 1
    print(f"Deleted {n_del} demo object(s) and container of run '{args.run_id}'")
    return 0


# ----------------------------------------------------------------------------- CLI parsing

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="FlowFirst Swift demo: write/read, listing, object lifecycle and the swift_storage helpers across the cluster nodes."
    )
    sub = p.add_subparsers(dest="cmd")

    def common(sp, keep=True):
        sp.add_argument("--nodes", default=DEFAULT_NODES,
                        help="comma list of proxies: mgmtnode1,mgmtnode2,mgmtnode3,vip,host[:port] (default: %(default)s)")
        sp.add_argument("--timeout", type=float, default=DEFAULT_TIMEOUT_S, help="seconds to wait per step")
        if keep:
            sp.add_argument("--keep", action="store_true", help="leave the demo container in place")

    r = sub.add_parser("run", help="full demo: every stage (default)")
    common(r)
    r.set_defaults(func=cmd_run)

    helps = {
        "status": "log in to each proxy and show the account totals",
        "write": "write through every node, read back through every node",
        "list": "container listing from every node",
        "lifecycle": "PUT, POST metadata, HEAD, DELETE, each through a different node",
        "helpers": "the swift_storage.py helpers through the VIP",
    }
    for key, text in helps.items():
        sp = sub.add_parser(key, help=text)
        common(sp, keep=(key != "status"))
        sp.set_defaults(func=_single_stage(key))

    c = sub.add_parser("cleanup", help="delete the container of a run left behind by --keep")
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
        return args.func(args)
    except ValueError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return 2
    except Exception as e:
        print(f"ERROR: {describe_error(e)}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
