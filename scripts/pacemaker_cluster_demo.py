#!/usr/bin/env python3
"""FlowFirst demo: how Pacemaker is used across the cluster nodes.

Pacemaker (with Corosync) decides WHERE things run on node1..node3: the Virtual IP (VIP) plus HAProxy as one group on a
single node, and the four pipeline processes (API Gateway, Transformer, Forwarder, DB Updater) as clones on every node.
This script asks Pacemaker what it believes (crm_mon), then asks every node what is really true (ssh: systemd units,
interface addresses), and compares the two. Two optional stages then cause a real failure on purpose and show Pacemaker
reacting, always restoring the cluster afterwards.

Read-only stages (the default):
  cluster    nodes: online / standby / which one is the Designated Controller (DC), and quorum
  resources  where every resource runs, against where it should run (VIP + HAProxy on exactly one node, clones on all)
  vip        which node Pacemaker says holds the VIP, which node really has the address, and a request through the VIP
  services   per node: corosync / pacemaker / pcsd, and each flowfirst-processN unit, against Pacemaker's placement
  watch      live view, one line per second, flagged when Pacemaker changes a placement (use it during a failure test)

Stages that change the cluster (need --yes, put everything back in a finally block):
  failover   put the node that holds the VIP into standby, time how long the VIP + HAProxy take to start elsewhere and
             answer again, then unstandby the node       (the same steps as Scenario 8)
  ban        ban one pipeline clone from one node, check that only that instance stopped and the VIP still answers,
             then clear the ban                          (the same steps as Scenario 11)

Usage:
    python3 scripts/pacemaker_cluster_demo.py                         read-only demo (same as 'run')
    python3 scripts/pacemaker_cluster_demo.py cluster | resources | vip | services
    python3 scripts/pacemaker_cluster_demo.py watch --seconds 90
    python3 scripts/pacemaker_cluster_demo.py failover --yes
    python3 scripts/pacemaker_cluster_demo.py ban --yes --process 2 --node node3

Where to run it: on node1, node2 or node3 (it calls 'sudo -n crm_mon' and 'sudo -n pcs' on the machine it runs on), with
the FlowFirst .env, and passwordless ssh + sudo to the other nodes (the same set-up as Phase 0). node4 is not a cluster
member and cannot run it. TLS for the request through the VIP follows TLS_API_ENABLED through src.tls.tls (fails
closed). Errors print only the exception type or an exit code, never a command's output (it can hold hosts). No secret
is read or printed. The commands are run as argument lists, never through a shell on this machine.

It is a demonstration, not a fencing or quorum test: Scenarios 16-26 cover those.
"""
import argparse
import http.client
import json
import os
import re
import subprocess
import sys
import time
import xml.etree.ElementTree as ET  # input is crm_mon's output on this machine, not data from a client
from typing import Callable, Dict, List, NamedTuple, Optional, Set, Tuple

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from src.config.config import (  # noqa: E402
    API_PORT,
    FLOWFIRST_VIP,
    NODE1_IP,
    NODE2_IP,
    NODE3_IP,
    NODE_NAME,
    TLS_API_ENABLED,
)
from src.tls.tls import client_context  # noqa: E402

DEFAULT_TIMEOUT_S = 5.0
DEFAULT_WAIT_S = 90
RES_VIP = "flowfirst-vip"
RES_HAPROXY = "haproxy-res"
GROUP = "vip-haproxy-group"
# process number -> (resource id, clone id, systemd unit, name used in this project)
PROCESSES = {
    1: ("flowfirst-p1-res", "flowfirst-p1-res-clone", "flowfirst-process1", "API Gateway (Process 1)"),
    2: ("flowfirst-p2-res", "flowfirst-p2-res-clone", "flowfirst-process2", "Transformer (Process 2)"),
    3: ("flowfirst-p3-res", "flowfirst-p3-res-clone", "flowfirst-process3", "Forwarder (Process 3)"),
    4: ("flowfirst-p4-res", "flowfirst-p4-res-clone", "flowfirst-process4", "DB Updater (Process 4)"),
}
DAEMONS = ("corosync", "pacemaker", "pcsd")
# One static command for every node: unit states, a separator, then the IPv4 addresses on the interfaces.
NODE_REPORT_CMD = ("systemctl is-active " + " ".join(DAEMONS) + " "
                   + " ".join(p[2] for p in PROCESSES.values()) + "; echo ---; ip -o -4 addr show")
_SAFE_NAME = re.compile(r"^[A-Za-z0-9_][A-Za-z0-9_.@-]*$")      # node names / ssh targets: no leading '-', no spaces

_now = time.time          # replaced by a fake clock in the tests
_sleep = time.sleep


# ----------------------------------------------------------------------------- data model and pure helpers (unit-tested)

class PNode(NamedTuple):
    name: str
    online: bool
    standby: bool
    maintenance: bool
    is_dc: bool


class View(NamedTuple):
    nodes: List[PNode]
    dc: str
    quorum: bool
    running: Dict[str, List[str]]     # resource id -> sorted node names where an instance is active
    failed: List[str]                 # resource ids Pacemaker marks failed
    failures: List[str]               # "op_key@node" from the failed-action list


class NodeReport(NamedTuple):
    units: Dict[str, str]             # systemd unit -> "active" / "inactive" / "failed" / ...
    addresses: Set[str]               # IPv4 addresses on the node's interfaces


class CmdError(Exception):
    def __init__(self, rc: int):
        super().__init__(f"command failed (exit {rc})")
        self.rc = rc


def _truthy(value: Optional[str]) -> bool:
    return (value or "").strip().lower() == "true"


def parse_view(text: str) -> View:
    """crm_mon --output-as=xml -> View. Raises ValueError when the text is not crm_mon XML."""
    try:
        root = ET.fromstring(text)
    except ET.ParseError:
        raise ValueError("crm_mon output is not valid XML") from None
    if root.tag != "pacemaker-result":
        raise ValueError("crm_mon output is not valid XML")
    dc_el = root.find("summary/current_dc")
    dc = quorum = None
    if dc_el is not None and _truthy(dc_el.get("present")):
        dc, quorum = dc_el.get("name") or "", _truthy(dc_el.get("with_quorum"))
    nodes = [PNode(n.get("name", "?"), _truthy(n.get("online")), _truthy(n.get("standby")),
                   _truthy(n.get("maintenance")), _truthy(n.get("is_dc")))
             for n in root.findall("nodes/node")]
    running: Dict[str, Set[str]] = {}
    failed: Set[str] = set()
    for res in root.iter("resource"):
        rid = res.get("id", "?")
        running.setdefault(rid, set())
        if _truthy(res.get("failed")):
            failed.add(rid)
        if _truthy(res.get("active")):
            running[rid].update(n.get("name", "?") for n in res.findall("node"))
    failures = [f"{f.get('op_key', '?')}@{f.get('node', '?')}" for f in root.findall("failures/failure")]
    return View(nodes, dc or "", bool(quorum), {k: sorted(v) for k, v in running.items()}, sorted(failed), failures)


def eligible_nodes(view: View) -> List[str]:
    """Nodes that may run resources: online, not in standby, not in maintenance."""
    return [n.name for n in view.nodes if n.online and not n.standby and not n.maintenance]


def vip_holder(view: View) -> Optional[str]:
    """The one node where the VIP runs, or None (stopped, or wrongly on several)."""
    nodes = view.running.get(RES_VIP, [])
    return nodes[0] if len(nodes) == 1 else None


def expected_placement(view: View, rid: str) -> List[str]:
    """Where the resource should be running: clones on every eligible node, VIP + HAProxy on the VIP holder."""
    if rid in (RES_VIP, RES_HAPROXY):
        holder = vip_holder(view)
        return [holder] if holder else []
    return sorted(eligible_nodes(view))


def placement_signature(view: View) -> Tuple:
    return tuple(sorted((rid, tuple(nodes)) for rid, nodes in view.running.items()))


def preflight(view: View) -> List[str]:
    """Reasons why a stage that changes the cluster must not start now (empty = safe to proceed)."""
    problems: List[str] = []
    if not view.quorum:
        problems.append("the cluster has no quorum")
    for n in view.nodes:
        if not n.online:
            problems.append(f"{n.name} is offline")
        elif n.standby:
            problems.append(f"{n.name} is already in standby")
        elif n.maintenance:
            problems.append(f"{n.name} is in maintenance mode")
    if len(view.nodes) < 3:
        problems.append("fewer than 3 nodes are configured")
    holder = vip_holder(view)
    if holder is None:
        problems.append("the VIP is not running on exactly one node")
    elif view.running.get(RES_HAPROXY, []) != [holder]:
        problems.append("HAProxy is not on the same node as the VIP")
    for num, (rid, _clone, _unit, label) in PROCESSES.items():
        if view.running.get(rid, []) != sorted(eligible_nodes(view)):
            problems.append(f"{label} is not running on every active node")
    if view.failed or view.failures:
        problems.append("Pacemaker lists failed resources or failed actions (clear them first: sudo crm_resource -C)")
    return problems


def parse_node_report(text: str) -> NodeReport:
    """Output of NODE_REPORT_CMD -> unit states and addresses."""
    head, _, tail = text.partition("---")
    names = list(DAEMONS) + [p[2] for p in PROCESSES.values()]
    states = [ln.strip() for ln in head.splitlines() if ln.strip()]
    units = {names[i]: states[i] for i in range(min(len(names), len(states)))}
    addrs = set(re.findall(r"\binet (\d{1,3}(?:\.\d{1,3}){3})/", tail))
    return NodeReport(units, addrs)


def service_mismatches(view: View, node: str, report: NodeReport) -> List[str]:
    """Differences between what Pacemaker says runs on `node` and what systemd on `node` says."""
    out = [f"{d} is {report.units.get(d, 'unknown')}" for d in DAEMONS if report.units.get(d) != "active"]
    for _num, (rid, _clone, unit, _label) in PROCESSES.items():
        should_run = node in view.running.get(rid, [])
        state = report.units.get(unit, "unknown")
        if should_run and state != "active":
            out.append(f"{unit} is {state} but Pacemaker says it runs here")
        elif not should_run and state == "active":
            out.append(f"{unit} is active but Pacemaker says it does not run here")
    return out


def wait_until(predicate: Callable[[], bool], timeout: float, interval: float = 1.0) -> Tuple[bool, float]:
    """Poll `predicate` until it is true or `timeout` seconds pass. Returns (reached, seconds waited)."""
    start = _now()
    while True:
        if predicate():
            return True, _now() - start
        if _now() - start >= timeout:
            return False, _now() - start
        _sleep(interval)


def render_table(rows: List[dict], columns: List[Tuple[str, str]]) -> str:
    """Fixed-width text table. rows: list of dicts, columns: list of (key, heading)."""
    cells = [[str(r.get(k, "")) if r.get(k) is not None else "" for k, _ in columns] for r in rows]
    widths = [max([len(h)] + [len(c[i]) for c in cells]) for i, (_, h) in enumerate(columns)]
    out = ["  ".join(h.ljust(widths[i]) for i, (_, h) in enumerate(columns)), "  ".join("-" * w for w in widths)]
    out += ["  ".join(c[i].ljust(widths[i]) for i in range(len(columns))) for c in cells]
    return "\n".join(out)


def describe_error(e: Exception) -> str:
    """Safe text for a failure: the exit code of a command, or the exception type. Never the message."""
    if isinstance(e, CmdError):
        return f"a cluster command failed (exit {e.rc})"
    return type(e).__name__


# ----------------------------------------------------------------------------- settings and command access

class CNode(NamedTuple):
    label: str     # node1..node3
    host: str      # address for ssh
    name: str      # NODEn_NAME: the node name inside Pacemaker, and what API Gateway (Process 1) reports as "node"


def cluster_nodes() -> List[CNode]:
    ips = {"node1": NODE1_IP, "node2": NODE2_IP, "node3": NODE3_IP}
    return [CNode(label, ip, os.getenv(f"{label.upper()}_NAME", label).strip() or label) for label, ip in ips.items()]


def _safe(value: str, what: str) -> str:
    if not _SAFE_NAME.match(value or ""):
        raise ValueError(f"{what} has characters that are not allowed")
    return value


class Settings:
    def __init__(self, args):
        self.vip = args.vip or FLOWFIRST_VIP
        self.api_port = args.api_port or API_PORT
        self.timeout = args.timeout
        self.wait = args.wait
        self.seconds = getattr(args, "seconds", 0)
        self.ssh_user = _safe(args.ssh_user, "--ssh-user") if args.ssh_user else ""
        self.cluster = os.getenv("CLUSTER_NAME", "flowfirst_cluster").strip()
        self.nodes = cluster_nodes()
        for nd in self.nodes:
            _safe(nd.host, f"{nd.label} address")
            _safe(nd.name, f"{nd.label} name")
        self.process = getattr(args, "process", 1)
        self.target = getattr(args, "node", "") or ""


def run_cmd(args: List[str], timeout: float) -> Tuple[int, str]:
    """Run an argument list (never a shell string on this machine). Returns (exit code, stdout)."""
    try:
        p = subprocess.run(args, capture_output=True, text=True, timeout=timeout)
        return p.returncode, p.stdout
    except FileNotFoundError:
        return 127, ""
    except subprocess.TimeoutExpired:
        return 124, ""


def get_view(st: Settings) -> View:
    rc, out = run_cmd(["sudo", "-n", "crm_mon", "-1", "-r", "--output-as=xml"], 20)
    if rc != 0:
        raise CmdError(rc)
    return parse_view(out)


def pcs(st: Settings, *args: str) -> None:
    for a in args:
        _safe(a, "a pcs argument")
    rc, _ = run_cmd(["sudo", "-n", "pcs", *args], 60)
    if rc != 0:
        raise CmdError(rc)


def crm_cleanup(st: Settings, resource: str) -> None:
    rc, _ = run_cmd(["sudo", "-n", "crm_resource", "-r", _safe(resource, "a resource id"), "-C"], 60)
    if rc != 0:
        raise CmdError(rc)


def fetch_report(st: Settings, node: CNode) -> NodeReport:
    """Unit states and addresses of one node: locally when it is this machine, otherwise over ssh."""
    if node.name == NODE_NAME:
        args = ["sh", "-c", NODE_REPORT_CMD]
    else:
        target = f"{st.ssh_user}@{node.host}" if st.ssh_user else node.host
        args = ["ssh", "-o", "BatchMode=yes", "-o", f"ConnectTimeout={max(1, int(st.timeout))}", target, NODE_REPORT_CMD]
    rc, out = run_cmd(args, st.timeout * 3 + 5)
    if rc not in (0, 3) or "---" not in out:          # systemctl is-active exits 3 when any unit is not active
        raise CmdError(rc)
    return parse_node_report(out)


_TLS_CTX: List = []


def probe_vip(st: Settings) -> Tuple[bool, str]:
    """One GET /health on a fresh connection to the VIP. Returns (answered 200, node that answered)."""
    try:
        if TLS_API_ENABLED:
            if not _TLS_CTX:
                _TLS_CTX.append(client_context("pacemaker-demo"))
            conn = http.client.HTTPSConnection(st.vip, st.api_port, timeout=st.timeout, context=_TLS_CTX[0])
        else:
            conn = http.client.HTTPConnection(st.vip, st.api_port, timeout=st.timeout)
        try:
            conn.request("GET", "/health", headers={"Connection": "close"})
            resp = conn.getresponse()
            body = resp.read()
            if resp.status != 200:
                return False, f"HTTP {resp.status}"
            return True, str(json.loads(body).get("node", "?"))
        finally:
            conn.close()
    except Exception as e:
        return False, type(e).__name__


def say(msg: str) -> None:
    print(f"    {time.strftime('%H:%M:%S')}  {msg}", flush=True)


# ----------------------------------------------------------------------------- read-only stages: each returns (ok, rows, cols)

def stage_cluster(st: Settings):
    view = get_view(st)
    rows, ok = [], True
    for n in view.nodes:
        if not n.online:
            state, text, good = "OFFLINE", "FAIL: not part of the cluster membership", False
        elif n.standby:
            state, text, good = "standby", "NOTE: online but runs nothing (Scenario 8)", False
        elif n.maintenance:
            state, text, good = "maintenance", "NOTE: Pacemaker does not manage it", False
        else:
            state, text, good = "online", "PASS: eligible to run resources", True
        ok = ok and good
        rows.append({"node": n.name, "state": state, "role": "DC (makes the decisions)" if n.is_dc else "", "result": text})
    active = len(eligible_nodes(view))
    majority = len(view.nodes) // 2 + 1
    q_ok = view.quorum and active >= majority
    ok = ok and q_ok
    rows.append({"node": "CLUSTER", "state": f"{active}/{len(view.nodes)} active", "role": f"DC: {view.dc or 'none'}",
                 "result": (f"PASS: partition with quorum (needs {majority})" if q_ok
                            else "FAIL: no quorum, Pacemaker stops resources (Scenarios 19, 22)")})
    return ok, rows, [("node", "node"), ("state", "state"), ("role", "role"), ("result", "result")]


def stage_resources(st: Settings):
    view = get_view(st)
    items = [(GROUP + " / " + RES_VIP, RES_VIP, "one node (group)"), (GROUP + " / " + RES_HAPROXY, RES_HAPROXY, "same node as VIP")]
    items += [(f"{clone} ({label})", rid, "every active node") for _n, (rid, clone, _u, label) in PROCESSES.items()]
    rows, ok = [], True
    for title, rid, rule in items:
        got = view.running.get(rid, [])
        want = expected_placement(view, rid)
        good = got == want and bool(want) and rid not in view.failed
        ok = ok and good
        rows.append({"resource": title, "rule": rule, "running": " ".join(got) or "(stopped)", "expected": " ".join(want) or "?",
                     "result": "PASS" if good else ("FAIL: marked failed" if rid in view.failed else "FAIL: not where Pacemaker should place it")})
    if view.failures:
        ok = False
        rows.append({"resource": "failed actions", "rule": "none", "running": ", ".join(view.failures), "expected": "-",
                     "result": "FAIL: clear with 'sudo crm_resource -C' after repairing"})
    return ok, rows, [("resource", "resource"), ("rule", "placement rule"), ("running", "running on"),
                      ("expected", "expected"), ("result", "result")]


def stage_vip(st: Settings):
    view = get_view(st)
    holder = vip_holder(view)
    rows, owners = [], []
    ok = True
    for nd in st.nodes:
        try:
            has = st.vip in fetch_report(st, nd).addresses
            seen = "has the address" if has else "-"
        except Exception as e:
            has, seen = None, f"unknown ({describe_error(e)})"
        says = nd.name == holder
        if has:
            owners.append(nd.name)
        good = has is not None and has == says
        ok = ok and good
        rows.append({"node": nd.name, "pacemaker": "holds the VIP" if says else "-", "interface": seen,
                     "result": "PASS: Pacemaker and the interface agree" if good else
                               ("FAIL: cannot read the node" if has is None else "FAIL: Pacemaker and the interface disagree")})
    answered, who = probe_vip(st)
    ok = ok and answered and len(owners) == 1
    rows.append({"node": "VIP " + st.vip, "pacemaker": f"holder: {holder or 'none'}", "interface": f"{len(owners)} node(s) have it",
                 "result": (f"PASS: GET /health through the VIP answered by {who}" if answered and len(owners) == 1
                            else "FAIL: " + ("the VIP is not on exactly one node" if len(owners) != 1 else f"GET /health got {who}"))})
    return ok, rows, [("node", "node"), ("pacemaker", "Pacemaker says"), ("interface", "interface (ip addr)"), ("result", "result")]


def stage_services(st: Settings):
    view = get_view(st)
    rows, ok = [], True
    for nd in st.nodes:
        try:
            rep = fetch_report(st, nd)
        except Exception as e:
            ok = False
            rows.append({"node": nd.name, "daemons": "-", "processes": "-", "result": f"FAIL: cannot read the node ({describe_error(e)})"})
            continue
        bad = service_mismatches(view, nd.name, rep)
        ok = ok and not bad
        rows.append({
            "node": nd.name,
            "daemons": " ".join(f"{d}={rep.units.get(d, '?')}" for d in DAEMONS),
            "processes": " ".join(f"p{n}={rep.units.get(PROCESSES[n][2], '?')}" for n in PROCESSES),
            "result": "PASS: systemd matches Pacemaker" if not bad else "FAIL: " + "; ".join(bad),
        })
    return ok, rows, [("node", "node"), ("daemons", "cluster daemons"), ("processes", "pipeline units"), ("result", "result")]


STAGES = {
    "cluster": ("Cluster membership and quorum", stage_cluster),
    "resources": ("Resource placement: Pacemaker's view", stage_resources),
    "vip": ("Virtual IP: Pacemaker's view against the real interfaces", stage_vip),
    "services": ("Services on every node: systemd against Pacemaker", stage_services),
}
RUN_ORDER = ["cluster", "resources", "vip", "services"]


# ----------------------------------------------------------------------------- stages that change the cluster

def _fail_rows(text: str):
    return False, [{"step": "preflight", "result": text}], [("step", "step"), ("result", "result")]


def stage_failover(st: Settings):
    """Standby the VIP holder; time the move and the first answer; always unstandby."""
    cols = [("step", "step"), ("result", "result")]
    view = get_view(st)
    problems = preflight(view)
    if problems:
        return _fail_rows("REFUSED: " + "; ".join(problems))
    holder = _safe(vip_holder(view) or "", "the VIP holder name")
    say(f"VIP and HAProxy run on {holder}; putting it into standby")
    rows: List[dict] = []
    standby_done = moved = served = False
    moved_to, moved_s, served_s, failed_probes = "", 0.0, 0.0, 0
    try:
        pcs(st, "node", "standby", holder)
        standby_done = True
        rows.append({"step": f"pcs node standby {holder}", "result": "PASS: accepted"})
        t0 = _now()
        while _now() - t0 < st.wait:
            ok_probe, who = probe_vip(st)
            if not ok_probe:
                failed_probes += 1
            try:
                v = get_view(st)
            except Exception:
                v = None
            new = vip_holder(v) if v else None
            if not moved and v and new and new != holder and v.running.get(RES_HAPROXY, []) == [new]:
                moved, moved_to, moved_s = True, new, _now() - t0
                say(f"Pacemaker started the VIP and HAProxy on {new} after {moved_s:.0f} s")
            if moved and ok_probe:
                served, served_s = True, _now() - t0
                say(f"GET /health through the VIP answered by {who} after {served_s:.0f} s")
                break
            _sleep(1.0)
    finally:
        if standby_done:
            say(f"restoring: pcs node unstandby {holder}")
            try:
                pcs(st, "node", "unstandby", holder)
                back, secs = wait_until(lambda: _node_back(st, holder), st.wait)
                rows.append({"step": f"pcs node unstandby {holder}",
                             "result": f"PASS: {holder} runs its clones again after {secs:.0f} s" if back
                                       else f"FAIL: {holder} did not become active again within {st.wait} s, check 'sudo pcs status'"})
            except Exception as e:
                rows.append({"step": f"pcs node unstandby {holder}",
                             "result": f"FAIL: {describe_error(e)}. Run by hand NOW: sudo pcs node unstandby {holder}"})
    rows.insert(1, {"step": "VIP and HAProxy move to another node",
                    "result": f"PASS: {holder} -> {moved_to} in {moved_s:.0f} s" if moved
                              else f"FAIL: not moved within {st.wait} s"})
    rows.insert(2, {"step": "VIP answers again",
                    "result": (f"PASS: after {served_s:.0f} s ({failed_probes} request(s) failed on the way: expected, "
                               "the old HAProxy was gone)" if served else "FAIL: no answer through the VIP")})
    ok = all(r["result"].startswith("PASS") for r in rows)
    return ok, rows, cols


def _node_back(st: Settings, node: str) -> bool:
    v = get_view(st)
    return node in eligible_nodes(v) and all(node in v.running.get(p[0], []) for p in PROCESSES.values())


def stage_ban(st: Settings):
    """Ban one pipeline clone from one node, check the others and the VIP, then clear the ban."""
    cols = [("step", "step"), ("result", "result")]
    num = st.process
    rid, clone, unit, label = PROCESSES[num]
    view = get_view(st)
    problems = preflight(view)
    if problems:
        return _fail_rows("REFUSED: " + "; ".join(problems))
    holder = vip_holder(view)
    target = st.target or next((n for n in eligible_nodes(view) if n != holder), "")
    if target not in eligible_nodes(view):
        return _fail_rows(f"REFUSED: '{target or '?'}' is not an active cluster node ({', '.join(eligible_nodes(view))})")
    _safe(target, "the target node")
    others = [n for n in eligible_nodes(view) if n != target]
    cnode = next((c for c in st.nodes if c.name == target), None)
    say(f"banning {label} ({clone}) from {target}")
    rows: List[dict] = []
    banned = False
    try:
        pcs(st, "resource", "ban", clone, target)
        banned = True
        rows.append({"step": f"pcs resource ban {clone} {target}", "result": "PASS: accepted"})
        gone, secs = wait_until(lambda: get_view(st).running.get(rid, []) == sorted(others), st.wait)
        rows.append({"step": f"{label} stops on {target} only",
                     "result": f"PASS: now running on {' '.join(others)} (after {secs:.0f} s)" if gone
                               else f"FAIL: still {' '.join(get_view(st).running.get(rid, []))} after {st.wait} s"})
        if gone and cnode is not None:
            try:
                state = fetch_report(st, cnode).units.get(unit, "unknown")
                rows.append({"step": f"systemd on {target}: {unit}",
                             "result": f"PASS: {state}, Pacemaker stopped it" if state != "active" else "FAIL: still active"})
            except Exception as e:
                rows.append({"step": f"systemd on {target}: {unit}", "result": f"FAIL: cannot read the node ({describe_error(e)})"})
        answered, _s = wait_until(lambda: probe_vip(st)[0], min(st.wait, 20))
        rows.append({"step": "request through the VIP",
                     "result": "PASS: still answered" if answered else "FAIL: no answer while one instance is banned"})
    finally:
        if banned:
            say(f"restoring: pcs resource clear {clone}")
            try:
                pcs(st, "resource", "clear", clone)
                crm_cleanup(st, clone)
                back, secs = wait_until(lambda: get_view(st).running.get(rid, []) == sorted(eligible_nodes(get_view(st))), st.wait)
                rows.append({"step": f"pcs resource clear {clone}",
                             "result": f"PASS: running on every node again after {secs:.0f} s" if back
                                       else f"FAIL: not back on every node within {st.wait} s, check 'sudo pcs constraint'"})
            except Exception as e:
                rows.append({"step": f"pcs resource clear {clone}",
                             "result": f"FAIL: {describe_error(e)}. Run by hand NOW: sudo pcs resource clear {clone}"})
    return all(r["result"].startswith("PASS") for r in rows), rows, cols


CHANGING = {
    "failover": ("Failover: standby the VIP holder, then restore", stage_failover),
    "ban": ("Ban one pipeline instance from one node, then restore", stage_ban),
}


# ----------------------------------------------------------------------------- commands

def _header(st: Settings) -> None:
    print("== FlowFirst Pacemaker Cluster Demo ==")
    print(f"cluster    : {st.cluster}")
    print(f"VIP        : {st.vip}   (REST {st.api_port}, TLS {'ON' if TLS_API_ENABLED else 'off'})")
    print(f"nodes      : {', '.join(f'{n.name} ({n.host})' for n in st.nodes)}")
    print(f"run on     : {NODE_NAME}   (ssh as {st.ssh_user or 'the current user'})")


def _print_stage(table: Dict, key: str, st: Settings, indent: str = "") -> bool:
    ok, rows, cols = table[key][1](st)
    print(indent + render_table(rows, cols).replace("\n", "\n" + indent))
    return ok


def cmd_run(args) -> int:
    st = Settings(args)
    _header(st)
    overall = True
    for n, key in enumerate(RUN_ORDER, 1):
        print(f"\n[{n}] {STAGES[key][0]}")
        try:
            overall = _print_stage(STAGES, key, st, "    ") and overall
        except Exception as e:
            print(f"    FAIL: {describe_error(e)}")
            overall = False
    print(f"\n[{len(RUN_ORDER) + 1}] Summary & Result")
    print("    PASS: Pacemaker's placement matches the real state of every node." if overall
          else "    FAIL: one or more stages failed. See Scenarios 10, 15 and 49 for status, repair and the logs. "
               "A node in standby (Scenario 8) also shows here.")
    return 0 if overall else 1


def _single(table: Dict, key: str):
    def cmd(args) -> int:
        st = Settings(args)
        print(f"== Pacemaker: {table[key][0]} ==")
        try:
            return 0 if _print_stage(table, key, st) else 1
        except Exception as e:
            print(f"FAIL: {describe_error(e)}")
            return 1
    return cmd


def _changing(key: str):
    def cmd(args) -> int:
        if not args.yes:
            print(f"ERROR: '{key}' changes the running cluster ({CHANGING[key][0]}). Everything is restored afterwards, "
                  "but requests can fail for a few seconds. Re-run with --yes to confirm.", file=sys.stderr)
            return 2
        st = Settings(args)
        print(f"== Pacemaker: {CHANGING[key][0]} ==")
        try:
            return 0 if _print_stage(CHANGING, key, st) else 1
        except KeyboardInterrupt:
            print("\ninterrupted: the cluster was restored (check 'sudo pcs status')")
            return 1
        except Exception as e:
            print(f"FAIL: {describe_error(e)}")
            return 1
    return cmd


def cmd_watch(args) -> int:
    st = Settings(args)
    print(f"== Pacemaker: live view for {st.seconds} s (Ctrl-C to stop) ==")
    print("time      VIP on   DC       active   VIP request    clone instances (p1 p2 p3 p4)")
    end, last, bad = _now() + st.seconds, None, 0
    try:
        while _now() < end:
            t0 = _now()
            try:
                v = get_view(st)
                holder = vip_holder(v) or "none"
                sig = placement_signature(v)
                counts = " ".join(str(len(v.running.get(PROCESSES[n][0], []))) for n in PROCESSES)
                active = f"{len(eligible_nodes(v))}/{len(v.nodes)}"
                dc = v.dc or "none"
            except Exception as e:
                holder, dc, active, counts, sig = "?", "?", "?", f"(crm_mon: {describe_error(e)})", ("error",)
            ok, who = probe_vip(st)
            bad += 0 if ok else 1
            note = "   <-- Pacemaker changed a placement" if last is not None and sig != last else ""
            last = sig
            print(f"{time.strftime('%H:%M:%S')}  {holder:<7}  {dc:<7}  {active:<6}   {('ok ' + who) if ok else 'FAILED ' + who:<13}  {counts}{note}", flush=True)
            _sleep(max(0.0, 1.0 - (_now() - t0)))
    except KeyboardInterrupt:
        print("\ninterrupted")
    print(f"\nrequests through the VIP that failed: {bad}")
    return 0 if bad == 0 else 1


# ----------------------------------------------------------------------------- CLI parsing

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="FlowFirst Pacemaker demo: see where the cluster runs the VIP, HAProxy and the "
                                            "pipeline processes, and how it reacts to a failure.")
    sub = p.add_subparsers(dest="cmd")

    def common(sp):
        sp.add_argument("--vip", default="", help="VIP for the request check (default: FLOWFIRST_VIP from .env)")
        sp.add_argument("--api-port", type=int, default=0, help="VIP REST port (default: API_PORT from .env)")
        sp.add_argument("--ssh-user", default=os.getenv("PKI_SSH_USER", "").strip(), help="ssh user for the other nodes "
                        "(default: PKI_SSH_USER from .env, else the current user)")
        sp.add_argument("--timeout", type=float, default=DEFAULT_TIMEOUT_S, help="seconds per network step")
        sp.add_argument("--wait", type=int, default=DEFAULT_WAIT_S, help="seconds to wait for Pacemaker to react (default: %(default)s)")

    r = sub.add_parser("run", help="read-only demo: cluster, resources, vip, services (default)")
    common(r)
    r.set_defaults(func=cmd_run)
    for key, text in {"cluster": "nodes, DC and quorum", "resources": "where each resource runs",
                      "vip": "VIP holder: Pacemaker against the interfaces", "services": "systemd on every node against Pacemaker"}.items():
        sp = sub.add_parser(key, help=text)
        common(sp)
        sp.set_defaults(func=_single(STAGES, key))
    w = sub.add_parser("watch", help="live view for a failure test")
    common(w)
    w.add_argument("--seconds", type=int, default=60, help="how long to watch (default: %(default)s)")
    w.set_defaults(func=cmd_watch)
    f = sub.add_parser("failover", help="CHANGES THE CLUSTER: standby the VIP holder, then restore")
    common(f)
    f.add_argument("--yes", action="store_true", help="confirm that the running cluster may be disturbed")
    f.set_defaults(func=_changing("failover"))
    b = sub.add_parser("ban", help="CHANGES THE CLUSTER: ban one pipeline instance from one node, then restore")
    common(b)
    b.add_argument("--yes", action="store_true", help="confirm that the running cluster may be disturbed")
    b.add_argument("--process", type=int, choices=sorted(PROCESSES), default=1, help="pipeline process 1-4 (default: %(default)s)")
    b.add_argument("--node", default="", help="Pacemaker node name to ban it from (default: a node that does not hold the VIP)")
    b.set_defaults(func=_changing("ban"))
    return p


def main(argv: Optional[List[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if not args.cmd:
        args = parser.parse_args(["run"] + list(argv if argv is not None else sys.argv[1:]))
    try:
        if args.wait < 1:
            raise ValueError("--wait must be at least 1")
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
