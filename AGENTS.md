# AGENTS.md

This file provides guidance to agents when working with code in this repository.

## Project Overview
FlowFirst is a Python 3 distributed pipeline on RHEL 9.6 — no web framework, no build system, no package manager scripts. There is no `Makefile`, `pyproject.toml`, or `setup.py`.

## Environment Setup
```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env   # then edit — set NODE_NAME, CURRENT_NODE_IP, and all node IPs
```

## Running Processes
Processes are run directly. Under Pacemaker, use `systemd/` units. For local dev:
```bash
source .venv/bin/activate
python process1.py   # REST API (listens on API_BACKEND_PORT=8082; HAProxy fronts on 8080)
python process2.py   # Message transformer/examiner
python process3.py   # Acknowledgment sealer
python process4.py   # MariaDB + Swift persistence engine
python process5.py   # Remote worker (Node 4 only)
```

## End-to-End Testing
```bash
# Trigger all flows and inspect DB (requires running cluster):
./scripts/test_flows_and_inspect_db.sh

# Manual single-flow test:
curl -s -X POST http://<VIP>:8080/api/flow1 -H "Content-Type: application/json" \
  -d '{"item_id": 42, "counter": 500}' | jq .
```

## Critical Code Patterns

### `gt.monkey_patch()` MUST be the very first call in every process entry point
```python
import gt
gt.monkey_patch()   # must precede ALL other imports (pika, kazoo, pymysql, etc.)
```
`gt.py` patches stdlib socket/time/threading/select via eventlet. Any import before this breaks cooperative concurrency.

### Each pika consumer greenthread MUST own its own `BlockingConnection`
`pika.BlockingConnection` is not thread-safe or greenthread-safe. Every greenthread that consumes or publishes must call `gt.get_rmq_connection_with_retry(label)` independently — never share a connection across greenthreads.

### Consumer loop pattern (used in all process*.py)
```python
while not gt.is_stopping():
    conn.process_data_events(time_limit=1)
    gt.sleep(0)   # yield to sibling greenthreads
```
The `gt.sleep(0)` yield after each `process_data_events` call is mandatory — without it one greenthread starves the others.

### ZooKeeper dedup barrier (Process 4 only)
Before any DB insert, call `zklib.check_and_mark_processed(msg_id)`. Returns `False` for duplicates (failover re-deliveries). Skip insert and `basic_ack` on `False`.

### ZK leader election gating
Each process entry point wraps its worker in `zklib.LeaderElection("processN", NODE_NAME).run(callback)`. The callback runs only on the elected leader. Falls back to direct call on ZK failure.

## `.env` Rules
- `python-dotenv` does **not** expand `${VAR}` shell references — use **literal values only**
- `RABBITMQ_HOSTS` and `MARIADB_HOSTS` must be **left blank**; `config.py` builds them from `NODE*_IP` at runtime
- Only `NODE_NAME` and `CURRENT_NODE_IP` differ per node; everything else is identical across all nodes
- `config.py` uses `_int_env()` / `_str_env()` guards that warn and fall back to defaults when unexpanded shell references are detected

## Queue Names (from `config.py`)
All queues are declared durable via `setup_queues(channel)` before any publish/consume:
- Flow 1: `flow1_p1_to_p2` → `flow1_p2_to_p3` → `flow1_p3_to_p4`
- Flow 2: `flow2_p1_to_p2` → `flow2_p2_to_p3` → `flow2_p3_reflected`
- Flow 3: `flow3_p1_to_p5` → `flow3_p5_to_p2` → `flow3_p2_to_p4`

## Code Style
- Python 3 with type hints in function signatures (`config.py`, `swift_storage.py`, `zk.py`)
- `from config import (NAME1, NAME2, ...)` — named imports only, never `import config` as namespace except in `zk.py`
- `print(f"[PX][gt] ...")` for all runtime log lines (not `logging.*` in process files; `logging` used only in `gt.py`, `zk.py`, `swift_storage.py`)
- Error handling: `except Exception as e: print(...); ch.basic_nack(..., requeue=False)` — failed messages are dead-lettered, not requeued
- `autocommit=True` on all MariaDB connections (no explicit `COMMIT` calls)
- Swift functions return `(bool, str)` or `(bool, data, headers)` tuples — callers check `[0]` for success

## Infrastructure Notes
- Process 1 listens on `API_BACKEND_PORT` (8082); HAProxy fronts it on `API_PORT` (8080) — never bind to 8080 directly
- MariaDB deadlocks (errno 1213/1205) are retried up to 3× with backoff in `db.py` — this is expected Galera wsrep behaviour
- ZooKeeper `/flowfirst` znode tree is lazily created on first process connection, not by setup scripts
- `config.get_connection()` attaches `conn._connected_to` (non-standard attribute) so callers can log which RabbitMQ node was chosen

---

## Full Cluster Installation Procedure (RHEL 9.6)

All scripts read from `/opt/flowfirst/.env`. Install phases must be followed in order.

### Phase 0a — Time Sync (ALL nodes, before anything else)
> RabbitMQ will not join a cluster if clocks are skewed. Do this first.
```bash
sudo dnf install -y chrony
sudo systemctl enable --now chronyd
sudo chronyc makestep
# Verify: chronyc tracking  →  Leap status: Normal, System time offset < 0.1s
```
For a custom NTP server (air-gapped): `sudo ./scripts/setup_chronyd.sh <ntp-server-ip>`

### Phase 0b — Clone Repo & Python Environment (ALL nodes)
```bash
sudo dnf install -y git python3 python3-pip
sudo git clone <repo-url> /opt/flowfirst
sudo chown -R $USER:$USER /opt/flowfirst
cd /opt/flowfirst
python3 -m venv .venv && source .venv/bin/activate
pip install --upgrade pip && pip install -r requirements.txt
cp .env.example .env
# Edit .env: set NODE_NAME and CURRENT_NODE_IP to THIS node's identity.
# All other variables are identical on every node (literal values only — no ${VAR}).
```

**Per-node `.env` variables** (only these two differ between nodes):

| Variable | node1 | node2 | node3 | node4 |
|---|---|---|---|---|
| `NODE_NAME` | `node1` | `node2` | `node3` | `node4` |
| `CURRENT_NODE_IP` | `<node1-ip>` | `<node2-ip>` | `<node3-ip>` | `<node4-ip>` |

**Shared `.env` variables** (identical on all nodes — use literal IPs, never `${VAR}`):

| Variable | Notes |
|---|---|
| `NODE1_IP` / `NODE2_IP` / `NODE3_IP` / `NODE4_IP` | Actual IPs of each node |
| `FLOWFIRST_VIP` | Pacemaker Virtual IP |
| `RABBITMQ_HOST` | Set to `FLOWFIRST_VIP` (single-host fallback only) |
| `RABBITMQ_HOSTS` | **Leave blank** — `config.py` builds from `NODE*_IP:5672` |
| `MARIADB_HOST` | Set to `FLOWFIRST_VIP` (routes through HAProxy) |
| `MARIADB_HOSTS` | **Leave blank** — `config.py` builds from `NODE*_IP` |
| `VIP_NIC` | Physical cluster NIC name on **this** node (e.g. `ens3`, `enp0s3`) — may differ per node |

### Phase 1 — MariaDB Galera Cluster (Nodes 1–3)
```bash
cd /opt/flowfirst

# On Node 1:
sudo ./mariadb-galera/setup_galera_node.sh
sudo ./mariadb-galera/bootstrap_galera.sh   # bootstraps primary node

# On Nodes 2 and 3 (after Node 1 is bootstrapped):
sudo ./mariadb-galera/setup_galera_node.sh
sudo systemctl enable --now mariadb          # joins the cluster

# Verify on any node (expect wsrep_cluster_size=3, wsrep_local_state_comment=Synced):
./mariadb-galera/check_galera_status.sh
```

### Phase 1b — ZooKeeper Ensemble (Nodes 1–3)
> Install after Galera, before RabbitMQ. ZK must be in quorum before pipeline processes start.
```bash
sudo ./zookeeper/setup_zookeeper.sh
# Downloads ZK 3.9.2, installs Java 17, writes myid from CURRENT_NODE_IP, opens ports 2181/2888/3888

# Verify on each node:
echo ruok | nc 127.0.0.1 2181        # → imok
echo mntr | nc 127.0.0.1 2181 | grep zk_server_state   # → leader or follower
# On the leader node only:
echo mntr | nc 127.0.0.1 2181 | grep zk_synced_followers  # → 2
```
> `zk_synced_followers` is only reported by the ZK leader node, not followers.

### Phase 1c — OS File Descriptor Limit (ALL nodes)
> Default RHEL 9 limit of 1024 is too low for 1000+ eventlet greenthreads.
```bash
echo "flowuser soft nofile 65536" | sudo tee -a /etc/security/limits.d/flowfirst.conf
echo "flowuser hard nofile 65536" | sudo tee -a /etc/security/limits.d/flowfirst.conf
ulimit -n 65536   # apply to current session immediately
# systemd units already include LimitNOFILE=65536 — handled automatically under Pacemaker
```

### Phase 2 — RabbitMQ (Nodes 1–3)
> Chrony must be synced first. The install script checks `chronyd` and warns if not.
```bash
sudo ./scripts/install_rabbitmq_rhel9.sh
# Detects arch at runtime: x86_64 → yum1/yum2.rabbitmq.com, aarch64 → packagecloud.io/rabbitmq
# Downloads GPG keys from github.com/rabbitmq/signing-keys release 3.0 (Cloudsmith URLs are dead)
# Enables rabbitmq_management plugin automatically
```

### Phase 3 — HAProxy (Nodes 1–3)
> RHEL 9 uses predictable NIC names (ens3, ens192, enp0s3) — not eth0. `VIP_NIC` may differ per node.
```bash
# Discover NIC name on each node:
ip route get ${FLOWFIRST_VIP} | awk '{print $5; exit}'
# Set VIP_NIC=<discovered-name> in .env on this node, then:
sudo ./haproxy/setup_haproxy.sh
# Generates /etc/haproxy/haproxy.cfg, sets ip_nonlocal_bind=1, sets SELinux booleans

# Validate config syntax only — DO NOT start haproxy.service manually:
sudo haproxy -c -f /etc/haproxy/haproxy.cfg
```
> **Never start `haproxy.service` directly.** HAProxy is managed by Pacemaker. Manual start conflicts with cluster resource management.

**HAProxy port layout** (no port collisions):
- REST API: HAProxy `:8080` → Process 1 backend `:8082`
- MariaDB LB: HAProxy `:3307` → Galera nodes `:3306`
- RabbitMQ LB: HAProxy `:5673` → RabbitMQ nodes `:5672`
- Stats dashboard: `:9000` (credentials: `HAPROXY_STATS_USER` / `HAPROXY_STATS_PASS`)

**Common HAProxy failures and fixes:**
1. `cannot find interface ens999` → wrong `VIP_NIC` in `.env`; re-detect and re-run setup
2. `cannot bind socket [VIP:8080]` → `ip_nonlocal_bind` not set; setup script does this but run `sudo sysctl -w net.ipv4.ip_nonlocal_bind=1` if needed
3. SELinux AVC denial → run `sudo setsebool -P haproxy_connect_any 1 daemons_enable_cluster_mode 1`
4. `backend 'static' has no server available` → default RHEL haproxy.cfg is still active; re-run `./haproxy/setup_haproxy.sh`

### Phase 4 — Systemd Units
```bash
# Nodes 1–3: install units for P1–P4, then disable direct boot (Pacemaker manages lifecycle)
sudo ./systemd/install_services.sh /opt/flowfirst
sudo systemctl disable flowfirst-process1 flowfirst-process2 flowfirst-process3 flowfirst-process4

# Node 4 only: install and ENABLE P5 directly (no Pacemaker on Node 4)
sudo ./systemd/install_services.sh /opt/flowfirst --remote
sudo systemctl enable --now flowfirst-process5
```

### Phase 5 — Pacemaker Cluster & VIP (Nodes 1–3)
```bash
# Step 5a — Run on ALL THREE nodes first (installs pcs/corosync/haproxy, starts pcsd on port 2224):
sudo ./pacemaker/setup_multinode_cluster.sh --prepare-node

# Step 5b — Run on NODE 1 ONLY after all three nodes are prepared:
sudo ./pacemaker/setup_multinode_cluster.sh           # authenticates nodes, forms cluster
sudo ./pacemaker/configure_multinode_resources.sh     # deploys VIP, HAProxy group, process clones
sudo pcs status                                        # verify all resources started

# Step 5c — Verify HAProxy came up under Pacemaker:
sudo pcs status | grep -A5 "vip-haproxy-group"
sudo ss -tlnp | grep haproxy
# Expected ports: 8080 (REST), 9000 (stats), 3307 (MariaDB LB), 5673 (RabbitMQ LB)
```

### Post-Install Verification
```bash
# Source .env variables into shell first:
cd /opt/flowfirst && set -a && source .env && set +a

# Health check via VIP:
curl -s http://${FLOWFIRST_VIP}:8080/health | jq .

# Trigger Flow 1 end-to-end:
curl -s -X POST http://${FLOWFIRST_VIP}:8080/api/flow1 \
  -H "Content-Type: application/json" \
  -d '{"item_id": 101, "counter": 200}' | jq .

# Inspect DB after ~2 seconds:
mariadb -h ${FLOWFIRST_VIP} -u ${MARIADB_USER} -p"${MARIADB_PASSWORD}" ${MARIADB_DB} \
  -e "SELECT id, flow_id, item_id, counter_value, created_at FROM processed_messages ORDER BY id DESC LIMIT 5;"

# Full automated test (runs flows 1, 2 and inspects DB):
./scripts/test_flows_and_inspect_db.sh
```
