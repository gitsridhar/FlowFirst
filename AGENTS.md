# AGENTS.md

This file provides guidance to agents when working with code in this repository.

## Project Overview
FlowFirst is a Python 3 distributed pipeline on RHEL 9.6 — no web framework, no build system, no package manager scripts. There is no `Makefile`, `pyproject.toml`, or `setup.py`.

## Environment Setup
```bash
python3 -m venv .venv && source .venv/bin/activate
pip install --require-hashes -r requirements.txt
cp src/config/.env.example .env   # then edit — set NODE_NAME, CURRENT_NODE_IP, and all node IPs
```

## Running Processes
Processes are run directly. Under Pacemaker, use `systemd/` units. For local dev:
```bash
source .venv/bin/activate
python -m src.application.process1   # REST API (listens on API_BACKEND_PORT=8082; HAProxy fronts on 8080)
python -m src.application.process2   # Message transformer/examiner
python -m src.application.process3   # Acknowledgment sealer
python -m src.application.process4   # MariaDB + Swift persistence engine
python -m src.application.process5   # Remote worker (Node 4 only)
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
from src.gt import gt
gt.monkey_patch()   # must precede ALL other imports (pika, kazoo, pymysql, etc.)
```
`src/gt/gt.py` patches stdlib socket/time/threading/select via eventlet. Any import before this breaks cooperative concurrency.

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
- `RABBITMQ_HOSTS` and `MARIADB_HOSTS` must be **left blank**; `src/config/config.py` builds them from `NODE*_IP` at runtime
- Only `NODE_NAME` and `CURRENT_NODE_IP` differ per node; everything else is identical across all nodes
- `src/config/config.py` uses `_int_env()` / `_str_env()` guards that warn and fall back to defaults when unexpanded shell references are detected

## Queue Names (from `src/config/config.py`)
All queues are declared durable via `setup_queues(channel)` before any publish/consume:
- Flow 1: `flow1_p1_to_p2` → `flow1_p2_to_p3` → `flow1_p3_to_p4`
- Flow 2: `flow2_p1_to_p2` → `flow2_p2_to_p3` → `flow2_p3_reflected`
- Flow 3: `flow3_p1_to_p5` → `flow3_p5_to_p2` → `flow3_p2_to_p4`

## Code Style
- Python 3 with type hints in function signatures (`src/config/config.py`, `src/swift/swift_storage.py`, `src/zookeeper/zk.py`)
- `from src.config.config import (NAME1, NAME2, ...)` — named imports only, never `import config` as namespace except in `src/zookeeper/zk.py`
- `log = logging.getLogger("flowfirst.pN")` in process files; never `print()` for runtime messages. `log.error(..., exc_info=True)` in `except` blocks. `gt.monkey_patch()` calls `gt.configure_logging()`, which sends every logger to stdout/journal and to a rotating `LOG_DIR/<process>.log` (`LOG_LEVEL` default INFO, `LOG_DIR` default `/var/log/flowfirst`, `LOG_MAX_BYTES`, `LOG_BACKUP_COUNT`) — so never add a second `basicConfig`
- Error handling: `except Exception as e: print(...); ch.basic_nack(..., requeue=False)` — failed messages are dead-lettered, not requeued
- `autocommit=True` on all MariaDB connections (no explicit `COMMIT` calls)
- Swift functions return `(bool, str)` or `(bool, data, headers)` tuples — callers check `[0]` for success

## Infrastructure Notes
- Process 1 listens on `API_BACKEND_PORT` (8082); HAProxy fronts it on `API_PORT` (8080) — never bind to 8080 directly
- MariaDB deadlocks (errno 1213/1205) are retried up to 3× with backoff in `src/db/db.py` — this is expected Galera wsrep behaviour
- ZooKeeper `/flowfirst` znode tree is lazily created on first process connection, not by setup scripts
- `config.get_connection()` attaches `conn._connected_to` (non-standard attribute) so callers can log which RabbitMQ node was chosen
- TLS is optional and off by default (`TLS_*_ENABLED=false`). Client-side TLS lives only in `src/tls/tls.py` and **fails closed**: a missing/unreadable certificate, key or CA (or an `http://` `SWIFT_AUTH_URL` with `TLS_SWIFT_ENABLED=true`) stops the process; there is no insecure fallback or `verify=False` option
- PKI tooling is in `security/` (self-signed CA, 21 leaf certificates, `install_certs.sh`, `enable_{rabbitmq,mariadb,zookeeper,swift}_tls.sh`, `verify_tls.sh`); `pki/`, `*.key` and `*.pem` are gitignored and must never be committed. Full procedure, rollout order and limits: ReadMe Scenario 63
- **Secrets fail closed:** `config.check_secrets()` (run at import) and `scripts/secrets_lib.sh` (install scripts) refuse an empty, short (`SECRET_MIN_LENGTH`, default 12) or well-known sample `RABBITMQ_PASSWORD`, `MARIADB_PASSWORD`, `SWIFT_KEY` (if Swift is enabled) and `HAPROXY_STATS_PASS`. Never print a secret value, never reintroduce a default password in code, and keep the two weak-value lists in step. `ALLOW_DEFAULT_SECRETS=true` is the only opt-out (local Docker dev). Generate values with `./scripts/generate_secrets.sh --write`.
- **Messages must not vanish silently:** publishers use confirms (`setup_queues()` calls `confirm_delivery()`) with `mandatory=True`; rejected messages (`nack requeue=False`) are dead-lettered by the `flowfirst-dlx` policy (`scripts/setup_rabbitmq_dlx.sh`) into `flowfirst_dead_letters`, inspected/replayed with `scripts/replay_dead_letters.py`. Do not add queue arguments to the pipeline queues (it breaks `queue_declare` on running clusters); use policies.
- **Admin authorisation:** `POST /zk/config` is allowed only for certificate CNs in `ADMIN_ALLOWED_CNS` (blank = nobody; no TLS = refused unless `ALLOW_UNAUTHENTICATED_ADMIN=true`, dev only). Identity logic is in `src/application/authz.py`; `X-SSL-Client-CN` is trusted only when the connection itself came from `ADMIN_TRUSTED_PROXY_CNS`. Any new state-changing endpoint must call `self._require_admin()`; never read the header without `authz.resolve_identity`.
- **Dependencies are pinned:** `requirements.txt` is generated (exact versions + hashes) from `requirements.in` by `scripts/update_requirements.sh`; never edit it by hand and install with `pip install --require-hashes -r requirements.txt`. To add or change a dependency edit `requirements.in`, regenerate, and commit both files.
- **Systemd sandbox:** the five unit files keep only the service definition; the sandbox lives in `systemd/flowfirst-hardening.conf`, installed as `<unit>.service.d/10-hardening.conf` by `install_services.sh`. The process may write only `/var/log/flowfirst` (+ private `/tmp`), has no capabilities, and may open only TCP/UDP/unix sockets. Code that needs a new writable path, a new socket family, `exec`, or a privileged call must change that file and be re-tested with `systemd-analyze security` and a real start; never loosen it silently. Per-node exceptions go in a `99-local.conf` drop-in.
- **Metrics:** `src/metrics/metrics.py` exposes `gt.metrics_counter` as Prometheus text on `METRICS_PORT_BASE` + process number (off unless `METRICS_ENABLED=true`). Call `metrics.start("processN")` first thing in `main()`. A new counter that matters operationally must be added to `EXPECTED_COUNTERS` (so it starts at 0) and, if it is a failure, be named `flow<N>_failed` so the alerts see it; `tests/test_metrics.py` checks that every seeded name is incremented somewhere. Metric output must never contain message content, credentials or hostnames of other systems. Alerts live in `monitoring/*.rules.yml` and are tested with `promtool test rules`; changing a metric name means changing them and `ReadMe` (Monitoring and Alerting).
- **CRL lifetime:** with `TLS_CRL_FILE` set, every component that loads the CRL (Process 1, the Python clients, HAProxy, MariaDB `ssl_crl`) refuses ALL certificates once the CRL passes its `nextUpdate` (tested). Never add a revocation path that skips `security/refresh_crl.sh` / `list_certs.sh` monitoring, and keep the preflight `revocation list` check. MariaDB needs `FLUSH SSL` after a new CRL is installed.
- **`/health` is liveness only:** HAProxy probes `GET /health` every 3 s, so it must answer from local state and make NO call to ZooKeeper, RabbitMQ, MariaDB or Swift (a slow dependency would otherwise mark every API node down at once). Dependency state belongs on `/ready` (local state only), `/zk/health`, `/swift/status` and in the metrics. Never point HAProxy at `/ready`: publisher connections are re-opened by the next publish, so a node taken out of rotation would never reconnect. `tests/test_process1_http.py` stubs a hanging ZooKeeper to enforce this.
- **Rejected messages:** a handler that fails does `log.error(...)`, `gt.metrics_counter["flow<N>_failed"] += 1`, then `basic_nack(requeue=False)`, in that order. Without the `flowfirst-dlx` policy RabbitMQ DISCARDS such a message (tested), so never rely on a nack for retry, never add `requeue=True` (poison messages would loop), and never declare the pipeline queues with `x-dead-letter-*` arguments (existing queues would refuse the declaration; the policy is the mechanism). Every publish keeps `mandatory=True` and publisher confirms; `mandatory=False` silently drops unroutable messages (tested).
- **Backups are encrypted and verified:** `scripts/backup_flowfirst.sh` / `restore_flowfirst.sh` (helpers in `backup_lib.sh`, ZooKeeper part in `zk_config_backup.py`) never write plaintext secrets to disk, need `--passphrase-file` (mode 600, 16+ chars), and the exit code is non-zero if any part failed. Back up state, not messages: queue contents are not backed up on purpose. Anything that restores must refuse to overwrite without an explicit flag. Passwords go to the MariaDB client through a mode-600 option file, never on the command line.
- **Production readiness:** `scripts/preflight_check.py` (read-only, prints no secret) is the per-node check; keep it in step when you add a setting that has an unsafe value (add a PASS/WARN/FAIL line). The ReadMe chapter "Production Rollout Run-Book" is the procedure; it states that it has not been run on a real cluster, so update it when it is. Rotating a password means changing it in the service as well as in `.env`.
- **Tests gate changes:** run `./scripts/ci_check.sh` before committing (pytest + ruff are in `requirements-dev.txt`, dev/CI hosts only). Add a test with every new rule in `authz.py`, `config.py` checks, `tls.py` or the Process 1 request handling. Tests that import `process1` must run it in a subprocess (`tests/_p1_harness.py`) because `gt.monkey_patch()` must never run inside pytest. Change dependencies only in `requirements*.in`, then run `scripts/update_requirements.sh`.
- **Publishing from Process 1 goes only through `_publish(routing_key, body)`.** It round-robins over `API_PUBLISH_CONNECTIONS` `_PublishSlot` objects, each holding its own connection, channel (confirms on, via `setup_queues`) and semaphore. Never publish on a shared channel, and never call `basic_publish` directly in `process1.py`. A slot reconnects itself after a broker restart.
- **Request limits and generic errors:** Process 1 enforces `API_MAX_BODY_BYTES` (413), `API_MAX_BATCH_COUNT`, `API_MAX_LIST_LIMIT` and a per-connection `API_REQUEST_TIMEOUT_S`. Read POST bodies only through `_read_json_body()` and return errors only through `_fail()`, which logs the exception under a `request_id` and sends the client a generic message. Never put `str(e)`, a path, a URL or a host into a response body (`API_EXPOSE_ERROR_DETAILS` is the dev-only switch). Client-supplied numbers go through `int()`/`float()` inside the `ValueError`->400 handler.
- With `TLS_API_ENABLED=true`, Process 1 still binds `API_BACKEND_PORT` (8082) but speaks HTTPS; HAProxy on 8080 requires client certificates (mutual TLS) and sets `X-SSL-Client-CN`
- Revocation is enforced by HAProxy and Process 1 only (not RabbitMQ or ZooKeeper; MariaDB unverified); Swift 2.38 hardcodes `CERT_NONE`, so Swift client certificates are enforced at HAProxy only

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
pip install --upgrade pip && pip install --require-hashes -r requirements.txt
cp src/config/.env.example .env
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
| `RABBITMQ_HOSTS` | **Leave blank** — `src/config/config.py` builds from `NODE*_IP:5672` |
| `MARIADB_HOST` | Set to `FLOWFIRST_VIP` (routes through HAProxy) |
| `MARIADB_HOSTS` | **Leave blank** — `src/config/config.py` builds from `NODE*_IP` |
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
# Writes cluster_partition_handling = pause_minority to /etc/rabbitmq/rabbitmq.conf (before first start)
```

### Phase 2b — Swift Object Store (Nodes 1–3)
> Native RPM + systemd install (no containers). Node 4 runs no Swift server. Run after Phase 2, before Phase 3.
```bash
# Generate ONCE, put the SAME values in .env on node1-3 (script refuses placeholders; never change after data is stored):
#   SWIFT_HASH_PATH_PREFIX=$(openssl rand -hex 16)   SWIFT_HASH_PATH_SUFFIX=$(openssl rand -hex 16)
sudo ./swift/setup_swift.sh
# Uses CentOS Cloud SIG OpenStack repo (SWIFT_OPENSTACK_RELEASE, default epoxy; repo written enabled=0)
# proxy :8888, account :6202, container :6201, object :6200 + rsyncd + memcached on every node
# Rings built from NODE1..3_IP (zone per node, 3 replicas, fixed seed) -> byte-identical on all nodes
# HAProxy publishes it on the VIP at SWIFT_LB_PORT (8090): SWIFT_AUTH_URL=http://<VIP>:8090/auth/v1.0
```
> Swift proxy cannot use `:8080` (HAProxy API frontend). Ring changes are manual; the script keeps existing `*.ring.gz`.

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
- Swift: HAProxy `:8090` → swift-proxy `:8888` on each node
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

### Phase 6 — TLS (optional, after everything above works in plain text)
> Off by default. Do it on a test cluster first, one component at a time; full detail, undo steps and limits are in ReadMe Scenario 63.
```bash
# Admin host (once): create the CA and certificates, then copy each node its own files
./security/init_ca.sh && ./security/issue_all.sh
./security/install_certs.sh                       # or --stage DIR to review first

# Nodes 1–3, one node at a time (plain listeners stay on):
sudo ./security/enable_rabbitmq_tls.sh
sudo ./security/enable_zookeeper_tls.sh
sudo ./security/enable_mariadb_tls.sh
sudo ./security/enable_swift_tls.sh

# Then, one switch at a time in .env on every node (literal values), restarting the processes after each:
#   TLS_RABBITMQ_ENABLED / TLS_ZK_ENABLED / TLS_MARIADB_ENABLED / TLS_SWIFT_ENABLED / TLS_API_ENABLED=true
# API last: restart Process 1, then `sudo ./haproxy/setup_haproxy.sh` (HAProxy stays under Pacemaker)
./security/verify_tls.sh
```
> Once `TLS_API_ENABLED=true`, external clients need a client certificate:
> `curl --cacert ca.crt --cert admin-client.crt --key admin-client.key https://<VIP>:8080/health`

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
