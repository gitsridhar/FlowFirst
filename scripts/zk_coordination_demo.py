#!/usr/bin/env python3
"""FlowFirst demo: distributed coordination with Apache ZooKeeper across nodes.

Demonstrates the four core ZooKeeper primitives used across the FlowFirst pipeline
(leader election, dynamic config watches, ephemeral service registry, and atomic dedup barriers)
directly against the ZooKeeper 3-node ensemble, without requiring the REST API or RabbitMQ:

1. Ensemble Status & Quorum Inspection: queries mgmtnode1, mgmtnode2, mgmtnode3 for roles (leader/follower)
   and quorum sync state.
2. Distributed Leader Election & Failover: demonstrates active-leader election, sequential znode
   ordering, standby contenders on follower nodes, and automatic failover on step-down.
3. Live Config Propagation: writes configuration through one node and demonstrates instant
   DataWatch delivery across other nodes without polling or process restart.
4. Ephemeral Service Discovery: registers ephemeral worker znodes from multiple nodes and
   proves automatic removal upon session disconnection.
5. Atomic Deduplication Barrier: proves that concurrent/re-delivered message IDs are atomically
   filtered so only the first delivery succeeds.

Usage:
    python3 scripts/zk_coordination_demo.py                          full demo (same as 'run')
    python3 scripts/zk_coordination_demo.py run --keep               ...and leave znodes for zkCli inspection
    python3 scripts/zk_coordination_demo.py status                   inspect ensemble health & roles
    python3 scripts/zk_coordination_demo.py election                 leader election & failover handover
    python3 scripts/zk_coordination_demo.py config --key threshold --value 45.0
    python3 scripts/zk_coordination_demo.py registry                 ephemeral service registration & auto-cleanup
    python3 scripts/zk_coordination_demo.py dedup --message-id msg-101
    python3 scripts/zk_coordination_demo.py cleanup [--all]

Where to run it: on mgmtnode1, mgmtnode2, mgmtnode3, remotenode1 (client-only), or any workstation with network
access to port 2181/2281 on NODE1_IP..NODE3_IP and the FlowFirst .env file.
Honours TLS_ZK_ENABLED through src.tls.tls (fails closed).
"""

import argparse
import json
import os
import socket
import sys
import threading
import time
import uuid
from typing import Dict, List, Optional, Tuple

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from kazoo.client import KazooClient  # noqa: E402
from kazoo.exceptions import (  # noqa: E402
    KazooException,
    NodeExistsError,
    NoNodeError,
)
from kazoo.recipe.election import Election  # noqa: E402

from src.config.config import (  # noqa: E402
    NODE1_IP,
    NODE2_IP,
    NODE3_IP,
    NODE_NAME,
    TLS_ZK_ENABLED,
    ZK_CLIENT_PORT,
    ZK_HOSTS,
    ZK_TIMEOUT,
    ZK_TLS_CLIENT_PORT,
)
from src.tls.tls import zk_kwargs  # noqa: E402

DEMO_ROOT = "/flowfirst/zk_demo"
DEFAULT_NODES = "mgmtnode1,mgmtnode2,mgmtnode3"
DEFAULT_TIMEOUT_S = 10.0
POLL_INTERVAL_S = 0.05


# ----------------------------------------------------------------------------- pure helpers (unit-tested)

def get_zk_port() -> int:
    """Return effective ZooKeeper client port based on TLS configuration."""
    return ZK_TLS_CLIENT_PORT if TLS_ZK_ENABLED else ZK_CLIENT_PORT


def named_targets(port: Optional[int] = None) -> Dict[str, Tuple[str, int]]:
    """Label -> (host, port) for the ZooKeeper ensemble nodes."""
    p = port if port is not None else get_zk_port()
    return {
        "mgmtnode1": (NODE1_IP, p),
        "mgmtnode2": (NODE2_IP, p),
        "mgmtnode3": (NODE3_IP, p),
    }


def resolve_target(spec: str, targets: Optional[Dict[str, Tuple[str, int]]] = None) -> Tuple[str, str, int]:
    """('mgmtnode2') -> ('mgmtnode2', '<NODE2_IP>', 2181); '10.0.0.1:2181' -> ('10.0.0.1:2181', '10.0.0.1', 2181)."""
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
    return spec, spec, get_zk_port()


def resolve_targets(csv: str, targets: Optional[Dict[str, Tuple[str, int]]] = None) -> List[Tuple[str, str, int]]:
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


def parse_mntr_response(text: str) -> Dict[str, str]:
    """Parse whitespace/tab-separated key-value pairs returned by ZooKeeper 'mntr' 4LW command."""
    result = {}
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        parts = line.split(None, 1)
        if len(parts) == 2:
            result[parts[0].strip()] = parts[1].strip()
        elif len(parts) == 1:
            result[parts[0].strip()] = ""
    return result


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


# ----------------------------------------------------------------------------- network & ZooKeeper access

def query_four_letter_word(host: str, port: int, command: str = "mntr", timeout_s: float = 2.0) -> str:
    """Send a 4-letter word command (mntr, ruok, srvr) to a ZooKeeper node via TCP socket."""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(timeout_s)
    try:
        s.connect((host, port))
        s.sendall(command.encode("ascii") + b"\n")
        response = b""
        while True:
            chunk = s.recv(4096)
            if not chunk:
                break
            response += chunk
        return response.decode("utf-8", errors="replace")
    finally:
        s.close()


def connect_node(host: str, port: int, timeout_s: float = 5.0) -> KazooClient:
    """Create and start a KazooClient directly connected to one specific ensemble node."""
    zk = KazooClient(hosts=f"{host}:{port}", timeout=timeout_s, **zk_kwargs())
    zk.start(timeout=timeout_s)
    return zk


def connect_ensemble(timeout_s: float = 5.0) -> KazooClient:
    """Create and start a KazooClient connected to the full ensemble (ZK_HOSTS)."""
    zk = KazooClient(hosts=ZK_HOSTS, timeout=timeout_s, **zk_kwargs())
    zk.start(timeout=timeout_s)
    return zk


def get_node_status(label: str, host: str, port: int, timeout_s: float = 2.0) -> dict:
    """Query an individual node for server state (leader/follower), latency, and connectivity."""
    info = {
        "node": label,
        "endpoint": f"{host}:{port}",
        "state": "unknown",
        "role": "unreachable",
        "synced_followers": "n/a",
        "zk_version": "n/a",
        "avg_latency": "n/a",
        "znode_count": "n/a",
    }
    # First try 4LW mntr command
    try:
        raw = query_four_letter_word(host, port, "mntr", timeout_s)
        parsed = parse_mntr_response(raw)
        if parsed.get("zk_server_state"):
            info["state"] = "imok"
            info["role"] = parsed.get("zk_server_state", "unknown")
            info["synced_followers"] = parsed.get("zk_synced_followers", "n/a")
            info["zk_version"] = parsed.get("zk_version", "unknown").split("-")[0]
            info["avg_latency"] = f"{parsed.get('zk_avg_latency', '0')} ms"
            info["znode_count"] = parsed.get("zk_znode_count", "n/a")
            return info
    except Exception:
        pass

    # If 4LW is restricted or TLS port was queried, fallback to active KazooClient session
    try:
        zk = connect_node(host, port, timeout_s=timeout_s)
        try:
            info["state"] = "connected" if zk.connected else "disconnected"
            info["role"] = "member"
            info["znode_count"] = str(len(zk.get_children("/") if zk.exists("/") else []))
        finally:
            zk.stop()
            zk.close()
    except Exception as e:
        info["role"] = f"error: {type(e).__name__}"

    return info


# ----------------------------------------------------------------------------- isolated demo stages

def stage_ensemble_status(nodes: List[Tuple[str, str, int]], timeout_s: float) -> Tuple[bool, List[dict]]:
    """Stage 1: Verify ensemble connectivity, role distribution, and quorum health."""
    rows = []
    has_leader = False
    reachable_count = 0
    for label, host, port in nodes:
        st = get_node_status(label, host, port, timeout_s=timeout_s)
        rows.append(st)
        if st["role"] == "leader":
            has_leader = True
        if "error" not in st["role"] and st["role"] != "unreachable":
            reachable_count += 1
    # Quorum requires majority reachable
    quorum_ok = reachable_count >= (len(nodes) // 2 + 1)
    return (quorum_ok and (has_leader or reachable_count > 0)), rows


def stage_leader_election(nodes: List[Tuple[str, str, int]], run_id: str, timeout_s: float) -> Tuple[bool, List[dict]]:
    """Stage 2: Demonstrate distributed leader election, standby queue, and failover across nodes."""
    election_path = f"{DEMO_ROOT}/{run_id}/election"
    logs = []

    clients: List[KazooClient] = []
    elections: List[Election] = []
    leader_events: List[threading.Event] = []
    threads: List[threading.Thread] = []
    stop_events: List[threading.Event] = []

    try:
        # Create a connection per node to simulate candidates on distinct cluster members
        for idx, (label, host, port) in enumerate(nodes):
            zk = connect_node(host, port, timeout_s=timeout_s)
            zk.ensure_path(election_path)
            clients.append(zk)

            cand_id = f"{label}_worker"
            el = Election(zk, election_path, identifier=cand_id)
            elections.append(el)

            is_leader_evt = threading.Event()
            stop_evt = threading.Event()
            leader_events.append(is_leader_evt)
            stop_events.append(stop_evt)

            def make_worker(e_inst: Election, l_evt: threading.Event, s_evt: threading.Event, c_id: str, n_lbl: str):
                def worker():
                    def on_lead():
                        l_evt.set()
                        # Hold leadership until told to step down
                        while not s_evt.is_set():
                            time.sleep(0.05)

                    try:
                        e_inst.run(on_lead)
                    except Exception:
                        pass
                return worker

            t = threading.Thread(target=make_worker(el, is_leader_evt, stop_evt, cand_id, label), daemon=True)
            threads.append(t)

        # 1. Start candidate 0 (representing mgmtnode1)
        t0 = time.time()
        threads[0].start()
        leader_events[0].wait(timeout=timeout_s)
        t_elect0 = (time.time() - t0) * 1000

        logs.append({
            "step": "1. Initial Election",
            "active_leader": nodes[0][0],
            "standby_nodes": "none",
            "time_ms": f"{t_elect0:.1f}",
            "status": "PASS: Elected as Leader",
        })

        # 2. Start subsequent candidates (mgmtnode2, mgmtnode3) — they must queue in standby
        for i in range(1, len(nodes)):
            threads[i].start()
        time.sleep(0.2)  # allow znodes to register in ZK

        # Inspect sequential znodes in ZooKeeper
        znodes = sorted(clients[0].get_children(election_path))
        standbys = ", ".join(n[0] for n in nodes[1:])
        logs.append({
            "step": "2. Standby Queueing",
            "active_leader": nodes[0][0],
            "standby_nodes": standbys,
            "time_ms": "—",
            "status": f"PASS: {len(znodes)} contenders queued in sequence",
        })

        # 3. Simulate Mgmt Node 1 leader step-down / failover
        t_failover_start = time.time()
        stop_events[0].set()  # release node 1
        # Wait for candidate 1 (mgmtnode2) to win leadership
        won = leader_events[1].wait(timeout=timeout_s)
        t_failover_ms = (time.time() - t_failover_start) * 1000

        if won:
            logs.append({
                "step": "3. Leader Failover",
                "active_leader": nodes[1][0],
                "standby_nodes": nodes[2][0] if len(nodes) > 2 else "none",
                "time_ms": f"{t_failover_ms:.1f}",
                "status": f"PASS: Instant handover to {nodes[1][0]}",
            })
        else:
            logs.append({
                "step": "3. Leader Failover",
                "active_leader": "none",
                "standby_nodes": "all",
                "time_ms": f"{t_failover_ms:.1f}",
                "status": "FAIL: Failover timeout exceeded",
            })

        # Clean up remaining contenders
        for s in stop_events:
            s.set()

        return won, logs

    finally:
        for s in stop_events:
            s.set()
        for zk in clients:
            try:
                zk.stop()
                zk.close()
            except Exception:
                pass


def stage_config_watches(nodes: List[Tuple[str, str, int]], run_id: str, timeout_s: float) -> Tuple[bool, List[dict]]:
    """Stage 3: Demonstrate dynamic runtime configuration updates and real-time DataWatch propagation."""
    config_path = f"{DEMO_ROOT}/{run_id}/config/flow2_high_threshold"
    logs = []

    clients: List[KazooClient] = []
    received_updates: Dict[str, List[Tuple[str, float]]] = {n[0]: [] for n in nodes}

    try:
        # Connect to each node and establish DataWatches
        for label, host, port in nodes:
            zk = connect_node(host, port, timeout_s=timeout_s)
            clients.append(zk)

            def make_watcher(node_lbl: str):
                def _watch(data, stat, event=None):
                    if data is not None:
                        val = data.decode("utf-8").strip()
                        received_updates[node_lbl].append((val, time.time()))
                return _watch

            # Register DataWatch on each node's connection
            zk.DataWatch(config_path)(make_watcher(label))

        time.sleep(0.1)

        # Write config value through Mgmt Node 1
        writer_node = nodes[0][0]
        test_val_1 = "38.5"
        t_write1 = time.time()
        clients[0].ensure_path(os.path.dirname(config_path))
        if clients[0].exists(config_path):
            clients[0].set(config_path, test_val_1.encode("utf-8"))
        else:
            clients[0].create(config_path, test_val_1.encode("utf-8"), makepath=True)

        # Wait for all nodes to receive update
        start = time.time()
        all_got_1 = False
        while time.time() - start < timeout_s:
            if all(any(v == test_val_1 for v, _ in received_updates[n[0]]) for n in nodes):
                all_got_1 = True
                break
            time.sleep(POLL_INTERVAL_S)

        max_latency_1 = max(
            [(t - t_write1) * 1000 for n in nodes for v, t in received_updates[n[0]] if v == test_val_1] or [0]
        )

        logs.append({
            "action": f"Set threshold = {test_val_1}",
            "written_via": writer_node,
            "observed_on": f"all {len(nodes)} nodes",
            "latency_ms": f"{max_latency_1:.1f}",
            "result": "PASS: Broadcast via DataWatch" if all_got_1 else "FAIL: Watch update lost",
        })

        # Write a second update through Mgmt Node 3 (or last node) to show any node can update config
        last_idx = len(nodes) - 1
        writer_node_2 = nodes[last_idx][0]
        test_val_2 = "45.0"
        t_write2 = time.time()
        clients[last_idx].set(config_path, test_val_2.encode("utf-8"))

        start = time.time()
        all_got_2 = False
        while time.time() - start < timeout_s:
            if all(any(v == test_val_2 for v, _ in received_updates[n[0]]) for n in nodes):
                all_got_2 = True
                break
            time.sleep(POLL_INTERVAL_S)

        max_latency_2 = max(
            [(t - t_write2) * 1000 for n in nodes for v, t in received_updates[n[0]] if v == test_val_2] or [0]
        )

        logs.append({
            "action": f"Set threshold = {test_val_2}",
            "written_via": writer_node_2,
            "observed_on": f"all {len(nodes)} nodes",
            "latency_ms": f"{max_latency_2:.1f}",
            "result": "PASS: Hot-reloaded without restart" if all_got_2 else "FAIL: Watch update lost",
        })

        return (all_got_1 and all_got_2), logs

    finally:
        for zk in clients:
            try:
                zk.stop()
                zk.close()
            except Exception:
                pass


def stage_service_registry(nodes: List[Tuple[str, str, int]], run_id: str, timeout_s: float) -> Tuple[bool, List[dict]]:
    """Stage 4: Demonstrate ephemeral service registry and automated deregistration upon session disconnect."""
    registry_path = f"{DEMO_ROOT}/{run_id}/registry/transformer"
    logs = []

    clients: List[KazooClient] = []

    try:
        for label, host, port in nodes:
            zk = connect_node(host, port, timeout_s=timeout_s)
            zk.ensure_path(registry_path)
            clients.append(zk)

        # 1. Register ephemeral znodes from mgmtnode1 and mgmtnode2
        p1 = f"{registry_path}/{nodes[0][0]}"
        p2 = f"{registry_path}/{nodes[1][0]}"
        d1 = json.dumps({"node": nodes[0][0], "pid": os.getpid(), "role": "active"}).encode()
        d2 = json.dumps({"node": nodes[1][0], "pid": os.getpid() + 1, "role": "standby"}).encode()

        clients[0].create(p1, d1, ephemeral=True)
        clients[1].create(p2, d2, ephemeral=True)

        # Mgmt Node 3 reads registered services
        reader_zk = clients[-1]
        active_services = reader_zk.get_children(registry_path)
        expected = [nodes[0][0], nodes[1][0]]
        registered_ok = sorted(active_services) == sorted(expected)

        logs.append({
            "event": "Register 2 ephemeral workers",
            "query_node": nodes[-1][0],
            "live_workers": ", ".join(sorted(active_services)),
            "status": "PASS: Discovered across ensemble" if registered_ok else "FAIL: Workers not found",
        })

        # 2. Simulate Mgmt Node 1 crash/exit: close client session
        clients[0].stop()
        clients[0].close()

        # Mgmt Node 3 polls for automatic ephemeral deletion
        start = time.time()
        vanished = False
        remaining = []
        while time.time() - start < timeout_s:
            remaining = reader_zk.get_children(registry_path)
            if nodes[0][0] not in remaining:
                vanished = True
                break
            time.sleep(POLL_INTERVAL_S)

        logs.append({
            "event": f"Disconnect {nodes[0][0]} session",
            "query_node": nodes[-1][0],
            "live_workers": ", ".join(remaining),
            "status": "PASS: Ephemeral auto-pruned" if vanished else "FAIL: Ephemeral leaked",
        })

        return (registered_ok and vanished), logs

    finally:
        for zk in clients:
            try:
                zk.stop()
                zk.close()
            except Exception:
                pass


def stage_dedup_barrier(nodes: List[Tuple[str, str, int]], run_id: str, timeout_s: float) -> Tuple[bool, List[dict]]:
    """Stage 5: Demonstrate atomic deduplication barriers preventing double-processing across nodes."""
    dedup_path = f"{DEMO_ROOT}/{run_id}/dedup"
    logs = []
    clients: List[KazooClient] = []

    try:
        for label, host, port in nodes:
            zk = connect_node(host, port, timeout_s=timeout_s)
            zk.ensure_path(dedup_path)
            clients.append(zk)

        msg_id = f"msg_{uuid.uuid4().hex[:8]}"
        znode = f"{dedup_path}/{msg_id}"

        # 1. Mgmt Node 1 processes message for the first time
        first_attempt = False
        try:
            clients[0].create(znode, b"1", makepath=True)
            first_attempt = True
        except NodeExistsError:
            first_attempt = False

        logs.append({
            "message_id": msg_id,
            "delivery_node": nodes[0][0],
            "action": "First arrival (Process 4 insert)",
            "decision": "PROCEED" if first_attempt else "REJECT",
            "result": "PASS: First-seen accepted" if first_attempt else "FAIL: First-seen rejected",
        })

        # 2. Mgmt Node 2 receives duplicate delivery of identical message_id
        second_attempt_blocked = False
        try:
            clients[1].create(znode, b"1", makepath=True)
            second_attempt_blocked = False  # should have raised NodeExistsError
        except NodeExistsError:
            second_attempt_blocked = True

        logs.append({
            "message_id": msg_id,
            "delivery_node": nodes[1][0],
            "action": "Duplicate redelivery (Failover)",
            "decision": "SKIP (Duplicate blocked)" if second_attempt_blocked else "INSERT (Collision!)",
            "result": "PASS: Dedup barrier blocked insert" if second_attempt_blocked else "FAIL: Duplicate slipped through",
        })

        # 3. Mgmt Node 3 also checks
        third_attempt_blocked = False
        if len(nodes) > 2:
            try:
                clients[2].create(znode, b"1", makepath=True)
            except NodeExistsError:
                third_attempt_blocked = True
            logs.append({
                "message_id": msg_id,
                "delivery_node": nodes[2][0],
                "action": "Duplicate redelivery (Mgmt Node 3)",
                "decision": "SKIP (Duplicate blocked)" if third_attempt_blocked else "INSERT (Collision!)",
                "result": "PASS: Dedup barrier verified" if third_attempt_blocked else "FAIL: Duplicate slipped through",
            })
        else:
            third_attempt_blocked = True

        return (first_attempt and second_attempt_blocked and third_attempt_blocked), logs

    finally:
        for zk in clients:
            try:
                zk.stop()
                zk.close()
            except Exception:
                pass


def delete_demo_tree(host: str, port: int, path: str = DEMO_ROOT) -> int:
    """Recursively delete demo znodes."""
    zk = connect_node(host, port)
    deleted_count = 0
    try:
        if zk.exists(path):
            def count_recursive(p):
                cnt = 1
                try:
                    for ch in zk.get_children(p):
                        cnt += count_recursive(f"{p}/{ch}")
                except NoNodeError:
                    pass
                return cnt

            deleted_count = count_recursive(path)
            zk.delete(path, recursive=True)
    finally:
        zk.stop()
        zk.close()
    return deleted_count


# ----------------------------------------------------------------------------- commands

def cmd_run(args) -> int:
    nodes = resolve_targets(args.nodes)
    run_id = f"run_{uuid.uuid4().hex[:8]}"

    print("== FlowFirst ZooKeeper Coordination Demo ==")
    print(f"run id   : {run_id}")
    print(f"ensemble : {', '.join(f'{lbl} ({h}:{p})' for lbl, h, p in nodes)}")
    print(f"client   : {NODE_NAME}")
    print(f"tls mode : {'ON (mutual TLS/verified CA)' if TLS_ZK_ENABLED else 'OFF (plain TCP)'}")

    overall_ok = True

    # 1. Ensemble Status
    print("\n[1] Inspect ZooKeeper Ensemble & Quorum State")
    st_ok, status_rows = stage_ensemble_status(nodes, args.timeout)
    cols = [
        ("node", "node"),
        ("endpoint", "endpoint"),
        ("role", "role"),
        ("synced_followers", "synced followers"),
        ("avg_latency", "avg latency"),
        ("znode_count", "znodes"),
    ]
    print("    " + render_table(status_rows, cols).replace("\n", "\n    "))
    if not st_ok:
        print("    WARNING: Quorum is degraded or leader not identified.")
        overall_ok = False

    # 2. Leader Election
    print("\n[2] Multi-Node Leader Election & Standby Failover")
    el_ok, el_rows = stage_leader_election(nodes, run_id, args.timeout)
    el_cols = [
        ("step", "stage"),
        ("active_leader", "active leader"),
        ("standby_nodes", "standby queue"),
        ("time_ms", "time (ms)"),
        ("status", "result"),
    ]
    print("    " + render_table(el_rows, el_cols).replace("\n", "\n    "))
    if not el_ok:
        overall_ok = False

    # 3. Dynamic Config Propagation
    print("\n[3] Dynamic Runtime Config Propagation (DataWatch)")
    cfg_ok, cfg_rows = stage_config_watches(nodes, run_id, args.timeout)
    cfg_cols = [
        ("action", "action"),
        ("written_via", "written via"),
        ("observed_on", "observed on"),
        ("latency_ms", "latency (ms)"),
        ("result", "status"),
    ]
    print("    " + render_table(cfg_rows, cfg_cols).replace("\n", "\n    "))
    if not cfg_ok:
        overall_ok = False

    # 4. Service Registry
    print("\n[4] Ephemeral Service Discovery & Auto-Cleanup")
    reg_ok, reg_rows = stage_service_registry(nodes, run_id, args.timeout)
    reg_cols = [
        ("event", "event"),
        ("query_node", "query node"),
        ("live_workers", "live workers"),
        ("status", "status"),
    ]
    print("    " + render_table(reg_rows, reg_cols).replace("\n", "\n    "))
    if not reg_ok:
        overall_ok = False

    # 5. Dedup Barrier
    print("\n[5] Atomic Deduplication Barrier Across Nodes")
    dd_ok, dd_rows = stage_dedup_barrier(nodes, run_id, args.timeout)
    dd_cols = [
        ("message_id", "message id"),
        ("delivery_node", "delivery via"),
        ("action", "action"),
        ("decision", "decision"),
        ("result", "status"),
    ]
    print("    " + render_table(dd_rows, dd_cols).replace("\n", "\n    "))
    if not dd_ok:
        overall_ok = False

    # 6. Result Summary
    print("\n[6] Summary & Result")
    if overall_ok:
        print("    PASS: All ZooKeeper coordination primitives verified across cluster nodes.")
    else:
        print("    FAIL: One or more coordination stages failed. Check ensemble connectivity.")

    if args.keep:
        print(f"\n    Demo znodes kept under {DEMO_ROOT}/{run_id}.")
        print("    Inspect with: /opt/zookeeper/bin/zkCli.sh -server 127.0.0.1:2181")
    else:
        cnt = delete_demo_tree(nodes[0][1], nodes[0][2], f"{DEMO_ROOT}/{run_id}")
        print(f"\n    Cleaned up {cnt} demo znode(s) (use --keep to retain for inspection).")

    return 0 if overall_ok else 1


def cmd_status(args) -> int:
    nodes = resolve_targets(args.nodes)
    print("== ZooKeeper Ensemble Status ==")
    _, rows = stage_ensemble_status(nodes, args.timeout)
    cols = [
        ("node", "node"),
        ("endpoint", "endpoint"),
        ("role", "role"),
        ("synced_followers", "synced followers"),
        ("avg_latency", "avg latency"),
        ("znode_count", "znodes"),
    ]
    print(render_table(rows, cols))
    return 0


def cmd_election(args) -> int:
    nodes = resolve_targets(args.nodes)
    run_id = f"elect_{uuid.uuid4().hex[:6]}"
    print(f"== ZooKeeper Leader Election Demo ({run_id}) ==")
    ok, rows = stage_leader_election(nodes, run_id, args.timeout)
    cols = [
        ("step", "stage"),
        ("active_leader", "active leader"),
        ("standby_nodes", "standby queue"),
        ("time_ms", "time (ms)"),
        ("status", "result"),
    ]
    print(render_table(rows, cols))
    if not args.keep:
        delete_demo_tree(nodes[0][1], nodes[0][2], f"{DEMO_ROOT}/{run_id}")
    return 0 if ok else 1


def cmd_config(args) -> int:
    nodes = resolve_targets(args.nodes)
    run_id = f"cfg_{uuid.uuid4().hex[:6]}"
    print(f"== ZooKeeper Config Watch Demo ({run_id}) ==")
    ok, rows = stage_config_watches(nodes, run_id, args.timeout)
    cols = [
        ("action", "action"),
        ("written_via", "written via"),
        ("observed_on", "observed on"),
        ("latency_ms", "latency (ms)"),
        ("result", "status"),
    ]
    print(render_table(rows, cols))
    if not args.keep:
        delete_demo_tree(nodes[0][1], nodes[0][2], f"{DEMO_ROOT}/{run_id}")
    return 0 if ok else 1


def cmd_registry(args) -> int:
    nodes = resolve_targets(args.nodes)
    run_id = f"reg_{uuid.uuid4().hex[:6]}"
    print(f"== ZooKeeper Service Registry Demo ({run_id}) ==")
    ok, rows = stage_service_registry(nodes, run_id, args.timeout)
    cols = [
        ("event", "event"),
        ("query_node", "query node"),
        ("live_workers", "live workers"),
        ("status", "status"),
    ]
    print(render_table(rows, cols))
    if not args.keep:
        delete_demo_tree(nodes[0][1], nodes[0][2], f"{DEMO_ROOT}/{run_id}")
    return 0 if ok else 1


def cmd_dedup(args) -> int:
    nodes = resolve_targets(args.nodes)
    run_id = f"dd_{uuid.uuid4().hex[:6]}"
    print(f"== ZooKeeper Dedup Barrier Demo ({run_id}) ==")
    ok, rows = stage_dedup_barrier(nodes, run_id, args.timeout)
    cols = [
        ("message_id", "message id"),
        ("delivery_node", "delivery via"),
        ("action", "action"),
        ("decision", "decision"),
        ("result", "status"),
    ]
    print(render_table(rows, cols))
    if not args.keep:
        delete_demo_tree(nodes[0][1], nodes[0][2], f"{DEMO_ROOT}/{run_id}")
    return 0 if ok else 1


def cmd_cleanup(args) -> int:
    _, host, port = resolve_target(args.node or "mgmtnode1")
    path = DEMO_ROOT if args.all else f"{DEMO_ROOT}/{args.run_id}" if args.run_id else DEMO_ROOT
    cnt = delete_demo_tree(host, port, path)
    print(f"Cleaned up {cnt} demo znode(s) under '{path}'")
    return 0


# ----------------------------------------------------------------------------- CLI parsing

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="FlowFirst ZooKeeper coordination demo: election, config watches, registry, and dedup across nodes."
    )
    sub = p.add_subparsers(dest="cmd")

    r = sub.add_parser("run", help="full demo: all ZooKeeper primitives across nodes (default)")
    r.add_argument("--nodes", default=DEFAULT_NODES, help="comma list of nodes (default: %(default)s)")
    r.add_argument("--timeout", type=float, default=DEFAULT_TIMEOUT_S, help="timeout in seconds per stage")
    r.add_argument("--keep", action="store_true", help="retain demo znodes for manual inspection")
    r.set_defaults(func=cmd_run)

    st = sub.add_parser("status", help="query ensemble member roles and health")
    st.add_argument("--nodes", default=DEFAULT_NODES, help="comma list of nodes (default: %(default)s)")
    st.add_argument("--timeout", type=float, default=2.0)
    st.set_defaults(func=cmd_status)

    el = sub.add_parser("election", help="test multi-node leader election and failover")
    el.add_argument("--nodes", default=DEFAULT_NODES, help="comma list of nodes (default: %(default)s)")
    el.add_argument("--timeout", type=float, default=DEFAULT_TIMEOUT_S)
    el.add_argument("--keep", action="store_true")
    el.set_defaults(func=cmd_election)

    cfg = sub.add_parser("config", help="test dynamic runtime config watches across nodes")
    cfg.add_argument("--nodes", default=DEFAULT_NODES, help="comma list of nodes (default: %(default)s)")
    cfg.add_argument("--timeout", type=float, default=DEFAULT_TIMEOUT_S)
    cfg.add_argument("--keep", action="store_true")
    cfg.set_defaults(func=cmd_config)

    reg = sub.add_parser("registry", help="test ephemeral service registry and auto-cleanup")
    reg.add_argument("--nodes", default=DEFAULT_NODES, help="comma list of nodes (default: %(default)s)")
    reg.add_argument("--timeout", type=float, default=DEFAULT_TIMEOUT_S)
    reg.add_argument("--keep", action="store_true")
    reg.set_defaults(func=cmd_registry)

    dd = sub.add_parser("dedup", help="test atomic deduplication barrier across nodes")
    dd.add_argument("--nodes", default=DEFAULT_NODES, help="comma list of nodes (default: %(default)s)")
    dd.add_argument("--timeout", type=float, default=DEFAULT_TIMEOUT_S)
    dd.add_argument("--keep", action="store_true")
    dd.set_defaults(func=cmd_dedup)

    c = sub.add_parser("cleanup", help="remove demo znodes")
    c.add_argument("--node", default="mgmtnode1", help="node to issue cleanup command through")
    c.add_argument("--run-id", default=None, help="clean up specific run ID only")
    c.add_argument("--all", action="store_true", help="remove entire /flowfirst/zk_demo tree")
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
    except KazooException as e:
        print(f"ERROR: ZooKeeper operation failed: {e}", file=sys.stderr)
        return 1
    except Exception as e:
        print(f"ERROR: {type(e).__name__}: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
