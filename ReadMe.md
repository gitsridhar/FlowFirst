# FlowFirst: Enterprise Multi-Process Distributed Data Pipeline
## HA Architecture with Git, Python 3, Pip, Pyenv/Venv, Chrony, MariaDB/Galera Multi-Master, RabbitMQ, HAProxy, Pacemaker/Corosync, Virtual IP (VIP), Apache ZooKeeper, Systemd Services, Eventlet Greenthreads, OpenStack Swift & Remote Worker Node

This project implements an enterprise-grade, high-availability, distributed inter-process communication, object storage, and persistence pipeline across a 4-node topology (3-node core high-availability cluster + 1 remote worker node on RHEL 9.6). It incorporates:
- **Git, Python 3, Pip, & Pyenv/Virtualenv**: Version-controlled modular codebase running isolated Python virtual environments across all nodes
- **Chrony NTP Time Synchronisation**: Sub-millisecond cluster clock synchronisation for Galera writesets and Corosync consensus
- **Virtual IP (VIP) Failover & High Availability**: Managed by **Pacemaker / Corosync** (`IPaddr2`) with zero-downtime VIP migration
- **HAProxy Load Balancing**: Round-Robin Layer-7 load balancing for REST API (:8080), Layer-4 load balancing for MariaDB Galera (:3307 -> :3306), RabbitMQ AMQP (:5673 -> :5672), and the Swift object store (:8090 -> :8888) with live statistics dashboard (:9000)
- **MariaDB Galera Multi-Master Cluster**: Synchronous multi-master replication (`wsrep`), automated Incremental (IST) / State Snapshot Transfers (SST), and quorum consistency across all core nodes
- **RabbitMQ Message Broker Pool**: Durable message queuing, automated reconnect greenthreads, and reliable cross-node message routing
- **Apache ZooKeeper**: Distributed coordinator for runtime leader election, live dynamic configuration hot-reloading (without process restart), worker service registry, and atomic deduplication barriers
- **Eventlet Greenthreads & Non-Blocking I/O**: High-concurrency cooperative multitasking, dedicated per-queue consumer greenpools, cooperative RMQ heartbeats, and live throughput metric collectors
- **OpenStack Swift Object Storage (`python-swiftclient`)**: Dual-persistence layer providing immutable object archiving and direct REST API retrieval. Swift runs as a native 3-node cluster (RPM packages + systemd, installed by `swift/setup_swift.sh`): proxy, account, container and object servers on every node, three replicas (one per node), load-balanced by HAProxy on the VIP
- **Systemd Service Architecture**: Managed systemd service units for `flowfirst-process1` through `flowfirst-process5` with automatic crash recovery
- **Distributed Multi-Process Pipeline (`process1` – `process5`)**:
  - **API Gateway (Process 1)**: REST API producer & health gateway — exposes Flows 1–7 including the IoT device CRUD endpoints (`POST /api/devices`, `POST /api/devices/query`, `PUT /api/devices/{id}`, `DELETE /api/devices/{id}`)
  - **Transformer (Process 2)**: Message transformer, metric examiner & reflection forwarder — enriches device payloads (fingerprint, update sequence) and normalises query filters
  - **Forwarder (Process 3)**: Acknowledgment sealer & reflection forwarder — validates device type and UUID format; dual-publishes Flow 6 to both the DB path and the remote edge-sync path
  - **DB Updater (Process 4)**: Dual MariaDB & Swift persistence engine with ZooKeeper dedup protection — persists `processed_messages` (Flows 1–3) and `devices` (Flows 4–7, including soft-delete)
  - **Remote Worker (Process 5)**: Dedicated remote worker running on Remote Node 4 (`node4`) for cross-network asynchronous task execution — handles Flow 3 pipeline reflection and the Flow 6 remote edge-sync (computes `edge_sync_token`, returns via `flow6_p5_to_p2`)

---

## Multi-Node Cluster Architecture & Network Topology

### Cluster Nodes Configuration
- **Node 1 (`node1`)**: `${NODE1_IP}` (Galera Node 1 / API Node 1 / RabbitMQ Broker / ZooKeeper Peer / HAProxy)
- **Node 2 (`node2`)**: `${NODE2_IP}` (Galera Node 2 / API Node 2 / RabbitMQ Broker / ZooKeeper Peer / HAProxy)
- **Node 3 (`node3`)**: `${NODE3_IP}` (Galera Node 3 / API Node 3 / RabbitMQ Broker / ZooKeeper Peer / HAProxy)
- **Virtual IP (VIP)**: `${FLOWFIRST_VIP}` (Managed by Pacemaker across Nodes 1–3)
- **Remote Node 4 (`node4`)**: `${NODE4_IP}` (Dedicated Remote Worker running **`process5` only**; acts strictly as a lightweight client connecting to the RabbitMQ pool, ZooKeeper ensemble, and Swift storage with no local broker/database daemons)

> 📐 **[View diagram: Multi-Node Cluster Architecture & Network Topology](docs/diagrams.md#1-multi-node-cluster-architecture--network-topology)**

---

## End-to-End Sequence & Galera Replication Flow

> 📐 **[View diagram: End-to-End Sequence & Galera Replication Flow](docs/diagrams.md#2-end-to-end-sequence--galera-replication-flow)**

---

## Step-by-Step Data Flows

### Flow 1: Intermediate Reflection at Transformer (Process 2) & Galera Persistence
1. **Client (`curl`)**: Sends `POST /api/flow1` to the Virtual IP `http://${FLOWFIRST_VIP}:8080`.
2. **HAProxy**: Delivers request to `API Gateway (Process 1)` on one of the active cluster nodes (Round-Robin).
3. **API Gateway (Process 1) ([`process1.py`](src/application/process1.py:1))**: Generates payload with UUID, publishes to `flow1_p1_to_p2`.
4. **Transformer (Process 2) ([`process2.py`](src/application/process2.py:1))**: Consumes message, modifies counter (`+10`), logs audit entry, and **reflects** it to `flow1_p2_to_p3`.
5. **Forwarder (Process 3) ([`process3.py`](src/application/process3.py:1))**: Consumes from `flow1_p2_to_p3`, attaches acknowledgment timestamp, and forwards to `flow1_p3_to_p4`.
6. **DB Updater (Process 4) ([`src/application/process4.py`](src/application/process4.py:1))**: Consumes from `flow1_p3_to_p4` and stores the payload in both **MariaDB Galera Cluster** via [`src/db/db.py`](src/db/db.py:1) and **Swift Object Storage** via [`src/swift/swift_storage.py`](src/swift/swift_storage.py:1).

---

### Flow 2: Threshold Examination at Transformer (Process 2) & Reflection at Forwarder (Process 3)
1. **Client (`curl`)**: Sends `POST /api/flow2` to `http://${FLOWFIRST_VIP}:8080`.
2. **HAProxy**: Delivers request to `API Gateway (Process 1)` on one of the active cluster nodes (Round-Robin).
3. **API Gateway (Process 1) ([`process1.py`](src/application/process1.py:1))**: Generates metric payload, publishes to `flow2_p1_to_p2`.
4. **Transformer (Process 2) ([`process2.py`](src/application/process2.py:1))**: Consumes message, evaluates threshold status (`HIGH`/`NORMAL`), applies 15% scaling factor, and forwards to `flow2_p2_to_p3`.
5. **Forwarder (Process 3) ([`process3.py`](src/application/process3.py:1))**: Consumes from `flow2_p2_to_p3`, seals with verification signature (`verified_by: process3`), and **reflects** to `flow2_p3_reflected`.
6. **DB Updater (Process 4) ([`src/application/process4.py`](src/application/process4.py:1))**: Consumes from `flow2_p3_reflected` and persists full history into both the **MariaDB Galera Cluster** and **Swift Object Storage**.

---

### Flow 3: Remote Node 4 / Remote Worker (Process 5) Pipeline & Persistence
1. **Client (`curl`)**: Sends `POST /api/flow3` to `http://${FLOWFIRST_VIP}:8080`.
2. **HAProxy**: Delivers request to `API Gateway (Process 1)` on any of the first three nodes (Round-Robin).
3. **API Gateway (Process 1) ([`process1.py`](src/application/process1.py:1))**: Prepares data with UUID and initial metric, publishes to `flow3_p1_to_p5` via RabbitMQ pool.
4. **Remote Worker (Process 5) ([`process5.py`](src/application/process5.py:1)) on Remote Node (`node4`)**: Consumes from `flow3_p1_to_p5`, modifies/computes remote transform value, logs audit trail, and **reflects** the modified data back to the RabbitMQ pool on `flow3_p5_to_p2`.
5. **Transformer (Process 2) ([`process2.py`](src/application/process2.py:1)) on any of the first three nodes**: Consumes from `flow3_p5_to_p2`, verifies the remote transformation, appends audit trail, and publishes to `flow3_p2_to_p4` via RabbitMQ pool.
6. **DB Updater (Process 4) ([`src/application/process4.py`](src/application/process4.py:1)) on any of the first three nodes**: Consumes from `flow3_p2_to_p4` and stores the complete history and payload into both the **MariaDB Galera Cluster** via [`src/db/db.py`](src/db/db.py:1) and **Swift Object Storage** via [`src/swift/swift_storage.py`](src/swift/swift_storage.py:1).

---

### IoT Device CRUD Flows (4–7)

Flows 4 through 7 implement a REST-based CRUD interface for IoT device management. All four flows share the same three-stage pipeline (P2 enrich → P3 validate → P4 persist) and write to the `devices` table in the MariaDB Galera cluster; they also archive each processed payload to Swift object storage. The device type must be one of: `temperature_sensor`, `pressure_gauge`, `flow_meter`, `humidity_sensor`, `vibration_sensor`, `power_monitor`.

---

### Flow 4: IoT Device Create

**Endpoint:** `POST /api/devices`
**Required fields:** `device_name`, `device_type` (from allowed set above)
**Optional fields:** `location`, `firmware_version`, `tags` (JSON object), `owner_team`

1. **Client (`curl`)**: Sends `POST /api/devices` with JSON body to `http://${FLOWFIRST_VIP}:8080`.
2. **HAProxy**: Delivers to `API Gateway (Process 1)` on one of the active nodes (Round-Robin).
3. **API Gateway (Process 1) ([`process1.py`](src/application/process1.py:213))**: Generates a fresh UUID `device_id`, builds the create payload with `operation: create` and an initial history entry, and publishes to `flow4_p1_to_p2`.
4. **Transformer (Process 2) ([`process2.py`](src/application/process2.py:1))**: Computes `device_fingerprint` = `SHA-256(device_name:device_type:location)` (hex), appends `enriched_at` timestamp to history, and forwards to `flow4_p2_to_p3`.
5. **Forwarder (Process 3) ([`process3.py`](src/application/process3.py:1))**: Validates that `device_name` is non-empty and `device_type` is in the allowed set; sets `validation_passed: true` and forwards to `flow4_p3_to_p4`.  Rejects (nack, dead-letter) if either check fails.
6. **DB Updater (Process 4) ([`process4.py`](src/application/process4.py:167))**: Calls [`db.upsert_device()`](src/db/db.py:174) (`INSERT … ON DUPLICATE KEY UPDATE`) after a ZooKeeper dedup check, then archives the payload to Swift object storage.

```bash
curl -s -X POST http://${FLOWFIRST_VIP}:8080/api/devices \
  -H "Content-Type: application/json" \
  -d '{"device_name":"Temp Sensor 1","device_type":"temperature_sensor","location":"factory/zone-A","firmware_version":"2.0.1","tags":{"env":"production"},"owner_team":"ops"}' | jq .
```

---

### Flow 5: IoT Device Query

**Endpoint:** `POST /api/devices/query`
**Optional fields:** `filters` object with any of `device_type`, `status`, `owner_team`, `location_contains`

1. **Client (`curl`)**: Sends `POST /api/devices/query` with an optional `filters` JSON object.
2. **HAProxy**: Delivers to `API Gateway (Process 1)` on one of the active nodes (Round-Robin).
3. **API Gateway (Process 1) ([`process1.py`](src/application/process1.py:255))**: Builds a query payload with `operation: query` and the supplied filters, publishes to `flow5_p1_to_p2`.
4. **Transformer (Process 2) ([`process2.py`](src/application/process2.py:1))**: Normalises `device_type` and `status` filter values to lowercase; forwards to `flow5_p2_to_p3`.
5. **Forwarder (Process 3) ([`process3.py`](src/application/process3.py:1))**: Marks `query_validated: true` (empty filter set is valid — returns all devices) and forwards to `flow5_p3_to_p4`.
6. **DB Updater (Process 4) ([`process4.py`](src/application/process4.py:207))**: Calls [`db.query_devices(filters)`](src/db/db.py:251) (builds a dynamic `WHERE` clause with optional `LIKE` for `location_contains`); archives the result payload to Swift.

```bash
# Query all temperature sensors with status active
curl -s -X POST http://${FLOWFIRST_VIP}:8080/api/devices/query \
  -H "Content-Type: application/json" \
  -d '{"filters":{"device_type":"temperature_sensor","status":"active"}}' | jq .

# Query with no filters — returns all devices (up to 1000 rows)
curl -s -X POST http://${FLOWFIRST_VIP}:8080/api/devices/query \
  -H "Content-Type: application/json" \
  -d '{}' | jq .
```

---

### Flow 6: IoT Device Update (with Remote Edge-Sync)

**Endpoint:** `PUT /api/devices/{device_id}`
**Body:** JSON object of fields to update (e.g. `{"firmware_version":"3.0.0","tags":{"env":"staging"}}`)

Flow 6 is the only flow that fans out to **two** downstream queues at Forwarder (Process 3) — one for direct persistence and one for the remote edge-sync on node4.

1. **Client (`curl`)**: Sends `PUT /api/devices/{device_id}` with the update fields.
2. **HAProxy**: Delivers to `API Gateway (Process 1)` on one of the active nodes.
3. **API Gateway (Process 1) ([`process1.py`](src/application/process1.py:279))**: Validates `device_id` is present, builds an update payload with `operation: update`, publishes to `flow6_p1_to_p2`.
4. **Transformer (Process 2) ([`process2.py`](src/application/process2.py:1))**: Generates a monotonic `update_sequence` (ms timestamp from ZooKeeper config or system clock), sets `previous_state_tracked: true`, and forwards to `flow6_p2_to_p3`.
5. **Forwarder (Process 3) ([`process3.py`](src/application/process3.py:1))**: Validates `device_id` is a 36-character UUID; on success **dual-publishes** — to `flow6_p3_to_p4` (direct DB path) **and** to `flow6_p3_to_p5` (remote edge-sync path). Rejects with nack if `device_id` is missing or malformed.
6. **DB Updater (Process 4) — direct path** ([`process4.py`](src/application/process4.py:253)): Consumes from `flow6_p3_to_p4`; calls [`db.upsert_device()`](src/db/db.py:174) after ZK dedup check; archives to Swift.
7. **Remote Worker (Process 5) on node4 — edge-sync path** ([`process5.py`](src/application/process5.py:76)): Consumes from `flow6_p3_to_p5`; computes `edge_sync_token = SHA-256(device_id:update_sequence)[:16]`; sets `remote_sync_applied: true`; publishes result to `flow6_p5_to_p2`.
8. **Transformer (Process 2) — remote return** ([`process2.py`](src/application/process2.py:1)): Consumes from `flow6_p5_to_p2`; sets `process2_remote_return_verified: true`; forwards to `flow6_p2_to_p4`.
9. **DB Updater (Process 4) — remote path** ([`process4.py`](src/application/process4.py:298)): Consumes from `flow6_p2_to_p4`; idempotent upsert after ZK dedup check; archives to Swift.

```bash
curl -s -X PUT http://${FLOWFIRST_VIP}:8080/api/devices/aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee \
  -H "Content-Type: application/json" \
  -d '{"firmware_version":"3.0.0","tags":{"env":"staging"}}' | jq .
```

---

### Flow 7: IoT Device Soft-Delete

**Endpoint:** `DELETE /api/devices/{device_id}`

1. **Client (`curl`)**: Sends `DELETE /api/devices/{device_id}`.
2. **HAProxy**: Delivers to `API Gateway (Process 1)` on one of the active nodes.
3. **API Gateway (Process 1) ([`process1.py`](src/application/process1.py:306))**: Validates `device_id` is present, builds a delete payload with `operation: delete`, publishes to `flow7_p1_to_p2`.
4. **Transformer (Process 2) ([`process2.py`](src/application/process2.py:1))**: Adds `deletion_requested_by` (node name) and `grace_period_s` metadata; forwards to `flow7_p2_to_p3`.
5. **Forwarder (Process 3) ([`process3.py`](src/application/process3.py:1))**: Validates that `device_id` is non-empty; sets `deletion_validated: true`; forwards to `flow7_p3_to_p4`. Rejects with nack on missing `device_id`.
6. **DB Updater (Process 4) ([`process4.py`](src/application/process4.py:346))**: Calls [`db.soft_delete_device(device_id)`](src/db/db.py:225) after ZK dedup check — issues `UPDATE devices SET status = 'decommissioned' WHERE device_id = %s` (the row is **not** physically deleted); archives the payload to Swift.

```bash
curl -s -X DELETE http://${FLOWFIRST_VIP}:8080/api/devices/aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee | jq .
```

---

## Database Schema & ER Diagram

> 📐 **[View diagram: Database Schema & ER Diagram](docs/diagrams.md#3-database-schema--er-diagram)**

`PROCESSED_MESSAGES` stores the audit trail for Flows 1–3. `DEVICES` is the device registry written by Flows 4 (create), 5 (query/archive), 6 (update/upsert), and 7 (soft-delete → `status = decommissioned`). Both tables are replicated synchronously across all three Galera nodes.

---

## Quick Reference

### Installation Phases

| Phase | Runs on | Description | Key script / command |
|---|---|---|---|
| **0 (pre)** | All nodes (1–4) | Create `flowadmin` user, add to `wheel`, grant passwordless sudo, distribute SSH key — **all subsequent phases run as `flowadmin`** | `useradd flowadmin` → `usermod -aG wheel` → `/etc/sudoers.d/flowadmin` → `ssh-keygen` + `ssh-copy-id` |
| **0a** | All nodes (1–4) | Time synchronisation — install & configure Chrony | `dnf install -y chrony` → `chronyc makestep` |
| **0b** | All nodes (1–4) | Clone repo, create Python venv, copy `.env` | `git clone` → `python3 -m venv .venv` → `pip install --require-hashes -r requirements.txt` |
| **1** | Nodes 1–3 only | MariaDB Galera cluster setup (server) | `mariadb-galera/setup_galera_node.sh` |
| **1 bootstrap** | Node 1 only | Bootstrap the primary Galera node | `mariadb-galera/bootstrap_galera.sh` |
| **1b** | Nodes 1–3 only | ZooKeeper 3-node ensemble (servers) | `zookeeper/setup_zookeeper.sh` |
| **1c** | All nodes (1–4) | Raise OS file-descriptor limit for eventlet greenthreads | `ulimit -n 65536` + `/etc/security/limits.d/flowfirst.conf` |
| **2** | Nodes 1–3 only | RabbitMQ cluster & Erlang install (servers) | `scripts/install_rabbitmq_rhel9.sh` |
| **2b** | Nodes 1–3 only | Swift object-store cluster (proxy + account/container/object servers, 3-replica rings, tempauth) | `swift/setup_swift.sh` |
| **3** | Nodes 1–3 only | Identify cluster NIC, set `VIP_NIC` in `.env`, generate HAProxy config | `haproxy/setup_haproxy.sh` |
| **4** | Nodes 1–3 | Install systemd units for API Gateway (Process 1)–4 & target (Pacemaker managed) | `systemd/install_services.sh /opt/flowfirst` |
| **4 (Remote)** | Node 4 only | Phase 0a+0b on node4 first (chrony, repo clone, venv, copy `.env` from node1 and set `NODE_NAME=node4`/`CURRENT_NODE_IP`), then install & enable `flowfirst-process5.service` | `systemd/install_services.sh /opt/flowfirst --remote` |
| **5a** | Nodes 1–3 only | Prepare cluster nodes for Pacemaker (packages, `pcsd`, firewall, `/etc/hosts`) | `pacemaker/setup_multinode_cluster.sh --prepare-node` |
| **5b** | Node 1 only | Initialise cluster, authenticate nodes, deploy VIP + HAProxy + process clones | `pacemaker/setup_multinode_cluster.sh` → `pacemaker/configure_multinode_resources.sh` |
| **5c** | Node 1 only | Verify HAProxy is running under Pacemaker after cluster start | `pcs status` → `ss -tlnp \| grep haproxy` |

---

### Scenarios Quick Reference

| # | Title | Category | What it covers |
|---|---|---|---|
| [1](#scenario-1-invoke-rest-api-via-virtual-ip--verify-round-robin-load-balancing) | Invoke REST API via VIP & Verify Round-Robin | REST API | Health check, Flow 1 & Flow 2 curl, round-robin proof |
| [2](#scenario-2-end-to-end-flow-execution--database-content-examination) | End-to-End Flow Execution & DB Examination | Pipeline | Trigger both flows, inspect MariaDB audit trails |
| [3](#scenario-3-synchronous-multi-master-galera-replication-verification) | Synchronous Galera Replication Verification | Galera | Query all 3 nodes, confirm identical records |
| [4](#scenario-4-galera-node-outage--automatic-quorum-resynchronization) | Galera Node Outage & Quorum Resync | Galera | Stop one node, verify 2-node quorum, rejoin |
| [5](#scenario-5-galera-ist--incremental-state-transfer) | Galera IST — Incremental State Transfer | Galera | Trigger IST, watch `Joiner/Receiving IST` state |
| [6](#scenario-6-galera-sst--state-snapshot-transfer) | Galera SST — State Snapshot Transfer | Galera | Force SST, watch `Donor/Desynced`, confirm full copy |
| [7](#scenario-7-gcachesize-impact-on-ist-vs-sst) | `gcache.size` Impact on IST vs SST | Galera | Resize gcache, demonstrate IST/SST decision boundary |
| [8](#scenario-8-node-failure--virtual-ip-vip-failover) | Node Failure & VIP Failover | Pacemaker | `pcs node standby`, VIP migrates, zero downtime |
| [9](#scenario-9-haproxy-real-time-statistics-dashboard) | HAProxy Statistics Dashboard | HAProxy | Access live stats at `${FLOWFIRST_VIP}:${HAPROXY_STATS_PORT}` |
| [10](#scenario-10-pacemaker-cluster-validation-with-crm_mon--crm_resource) | Pacemaker Validation (`crm_mon`) | Pacemaker | Full status, locate VIP, move resource, standby/recover |
| [11](#scenario-11-stop--start-a-single-pipeline-process-on-one-node) | Stop & Start a Single Process (one node) | Service lifecycle | `pcs resource ban/clear` one clone on one node |
| [12](#scenario-12-stop--start-all-four-pipeline-processes-cluster-wide) | Stop & Start All Processes (cluster-wide) | Service lifecycle | Disable/enable all 4 clones, validate API |
| [13](#scenario-13-stop--start-rabbitmq-on-a-single-node) | Stop & Start RabbitMQ (one node) | Service lifecycle | Verify 2-node RMQ quorum, rejoin |
| [14](#scenario-14-stop--start-mariadb-galera-on-a-single-node) | Stop & Start MariaDB (one node) | Service lifecycle | Verify 2-node Galera, normal rejoin |
| [15](#scenario-15-stop--start-haproxy-on-a-single-node) | Stop & Start HAProxy (one node) | Service lifecycle | Move VIP group via `pcs resource move` |
| [16](#scenario-16-reboot-a-single-node-node3--zero-pipeline-downtime) | Reboot One Node — Zero Downtime | OS reboot | Node3 reboot, cluster continuity, auto-rejoin |
| [17](#scenario-17-reboot-the-node-hosting-the-vip-node1--forced-failover) | Reboot VIP-Holding Node — Forced Failover | OS reboot | Evacuate node1, reboot, confirm VIP migrated |
| [18](#scenario-18-sequential-rolling-reboot-of-all-three-nodes) | Sequential Rolling Reboot (all 3 nodes) | OS reboot | Reboot one at a time, quorum maintained throughout |
| [19](#scenario-19-simultaneous-reboot-of-all-three-nodes-cluster-cold-start) | Simultaneous Reboot — Cluster Cold Start | OS reboot | All 3 nodes down, Galera bootstrap, Pacemaker cold start |
| [20](#scenario-20-soft-network-degradation--latency--packet-loss-tc-netem) | Soft Network Degradation (latency + loss) | Network glitch | `tc netem` 300 ms + 10% loss, observe Corosync/Galera/RMQ |
| [21](#scenario-21-high-bandwidth-throttling--simulating-a-saturated-link) | High Bandwidth Throttling | Network glitch | 1 Mbit/s cap + 2% corruption, Galera IST behaviour |
| [22](#scenario-22-hard-network-partition--iptables-drop-between-two-nodes) | Hard Network Partition (`iptables` DROP) | Network glitch | Full 2-node split, fencing decision, VIP stays up |
| [23](#scenario-23-selective-port-level-block--corosync-ring-only) | Selective Block — Corosync Ring Only | Network glitch | Block UDP 5405, Pacemaker loses heartbeat, app flows |
| [24](#scenario-24-selective-port-level-block--galera-replication-only) | Selective Block — Galera Replication Only | Network glitch | Block TCP 4567, Galera degrades, Corosync stays healthy |
| [25](#scenario-25-selective-port-level-block--rabbitmq-amqp-only) | Selective Block — RabbitMQ AMQP Only | Network glitch | Block TCP 5672, pika reconnects to surviving node |
| [26](#scenario-26-intermittent-flapping--periodic-network-glitch) | Intermittent Flapping — Periodic Glitch | Network glitch | Background glitch loop, Pacemaker failure counters |
| [27](#scenario-27-capture-setup--identify-interfaces-and-start-captures) | Capture Setup — Identify Interfaces | tcpdump | Identify cluster NICs, start background `tcpdump` |
| [28](#scenario-28-amqp-round-trip--flow1--flow2-pipeline-execution) | AMQP Round-Trip — Flow 1 & Flow 2 | tcpdump | TCP handshake, AMQP frames, publish/deliver/ack |
| [29](#scenario-29-rest-api--haproxy-round-trip-verification) | REST API / HAProxy Round-Trip | tcpdump | POST→200 OK, HAProxy SYN→SYN-ACK round-trip |
| [30](#scenario-30-corosync-heartbeat-round-trip-verification) | Corosync Heartbeat Round-Trip | tcpdump | UDP 5405 heartbeat counts, all 6 node pairs |
| [31](#scenario-31-galera-wsrep-replication-round-trip-verification) | Galera wsrep Replication Round-Trip | tcpdump | TCP 4567 writesets, retransmission check |
| [32](#scenario-32-network-round-trip-during-glitch--beforeduringafter-comparison) | Round-Trip During Glitch — Before/During/After | tcpdump | RTT baseline vs. injected latency comparison |
| [33](#scenario-33-tcpdump-based-full-pipeline-flow-trace) | tcpdump-Based Full Pipeline Flow Trace | tcpdump | Trace one message hop-by-hop across all 3 pcap files |
| [34](#scenario-34-zookeeper-ensemble-health--leader-election-verification) | ZooKeeper Ensemble Health & Leader Election | ZooKeeper | `ruok`/`mntr`, confirm leader/follower states |
| [35](#scenario-35-live-config-change-via-zookeeper--no-restart-required) | Live Config Change via ZooKeeper | ZooKeeper | Update threshold/step at runtime, all nodes hot-reload |
| [36](#scenario-36-zookeeper-leader-failover--kill-the-elected-leader-node) | ZooKeeper Leader Failover | ZooKeeper | Stop leader node, watch next candidate elected |
| [37](#scenario-37-zookeeper-dedup-barrier--prevent-double-insert-on-failover) | ZooKeeper Dedup Barrier | ZooKeeper | Re-deliver message, confirm dedup znode blocks double-insert |
| [38](#scenario-38-zookeeper-service-registry--observe-live-process-registration) | ZooKeeper Service Registry | ZooKeeper | Ephemeral znodes, stop process, confirm auto-deregister |
| [39](#scenario-39-verify-greenthread-startup--pool-status) | Verify Greenthread Startup & Pool Status | Greenthreads | `GET /gt/status`, list named greenthreads, pool size |
| [40](#scenario-40-concurrent-batch-publishing--greenthread-parallelism-in-process-1) | Concurrent Batch Publishing | Greenthreads | `POST /api/batch`, concurrent GreenPool publish |
| [41](#scenario-41-per-queue-greenthread-independence--flow-1-blockage-does-not-stall-flow-2) | Per-Queue Greenthread Independence | Greenthreads | Slow one queue, confirm sibling greenthread unaffected |
| [42](#scenario-42-greenthread-auto-restart-on-rabbitmq-consumer-failure) | Greenthread Auto-Restart on RMQ Failure | Greenthreads | Kill RMQ connection, greenthread reconnects automatically |
| [43](#scenario-43-greenthread-metrics-reporter--throughput-monitoring) | Greenthread Metrics Reporter | Greenthreads | Read throughput counters from `/gt/status` and journal |
| [44](#scenario-44-remote-node-4--process-5-end-to-end-pipeline-execution--database-validation) | Remote Node 4 / Remote Worker (Process 5) Pipeline Execution & DB Validation | Remote Worker | P1→P5 (Remote Node 4)→P2→P4, verify MariaDB audit trail & Swift object storage |
| [45](#scenario-45-reading-the-mariadb-galera-error-log-across-scenarios) | Reading the MariaDB Galera Error Log | Galera | Locate log, message phrasebook, per-node timeline, cross-node correlation |
| [46](#scenario-46-reading-the-rabbitmq-broker-log-across-scenarios) | Reading the RabbitMQ Broker Log | RabbitMQ | Locate log, message phrasebook, per-node timeline, cross-node correlation |
| [47](#scenario-47-reading-the-zookeeper-server--client-logs-across-scenarios) | Reading the ZooKeeper Server & Client Logs | ZooKeeper | Locate logs, server & kazoo phrasebook, election timeline, session expiry correlation |
| [48](#scenario-48-reading-and-managing-haproxy-logs-across-scenarios) | Reading and Managing HAProxy Logs | HAProxy | Is anything logged? rsyslog check, line anatomy, up/down events, rotation |
| [49](#scenario-49-reading-pacemaker-and-corosync-logs-across-scenarios) | Reading Pacemaker and Corosync Logs | Pacemaker | Phrasebook, DC scheduler lines, `crm_simulate` replay, fail counts, `crm_report` |
| [50](#scenario-50-reading-swift-logs-and-detecting-missing-objects) | Reading Swift Logs & Detecting Missing Objects | Swift | Client errors, non-fatal failures, MariaDB-vs-Swift reconciliation |
| [51](#scenario-51-reading-varlogmessages-and-the-systemd-journal-across-scenarios) | Reading `/var/log/messages` & the systemd Journal | Linux | Boots, OOM kills, NIC/link changes, SELinux denials, journal retention |
| [52](#scenario-52-swift-cluster-health-ring-consistency--replica-placement) | Swift Cluster Health, Ring Consistency & Replica Placement | Swift | ring/`swift.conf` md5, `swift-ring-builder`, `swift-get-nodes`, one replica per node |
| [53](#scenario-53-swift-node-loss--writes-continue-on-2-of-3-replicas-then-catch-up) | Swift Node Loss & Replication Catch-Up | Swift | 2-of-3 write quorum, `swift_back` DOWN/UP, handoff-free catch-up with the replicator |
| [54](#scenario-54-swift-device-failure--http-507-error-limiting-and-recovery) | Swift Device Failure (HTTP 507) | Swift | Renamed device, proxy error limiting, replicator "unmounted", restore |
| [55](#scenario-55-swift-object-corruption--auditor-quarantine-and-replica-repair) | Swift Object Corruption & Auditor Repair | Swift | Corrupt one replica, auditor quarantine, `swift-recon -q`, repair from good copies |
| [56](#scenario-56-swift-token-cache-memcached-failure--401-re-auth-then-503-when-all-caches-are-down) | Swift Token Cache (memcached) Failure | Swift | One cache down gives 401 + re-auth; all down gives 503 at login; HAProxy cannot see it |
| [57](#scenario-57-swift-credential-failures--wrong-key-key-rotation-and-token-lifetime) | Swift Credential Failures & Key Rotation | Swift | Wrong key 401, non-fatal `Swift store note`, rotating the tempauth key on all proxies |
| [58](#scenario-58-total-swift-outage-during-the-pipeline--detect-and-back-fill-from-mariadb) | Total Swift Outage & Back-Fill from MariaDB | Swift | All proxies down, 7 s retry delay per message, rebuild missing objects from `raw_payload` |
| [59](#scenario-59-reading-the-rest-api-http-request-log-across-scenarios) | Reading the REST API (HTTP) Request Log | REST API | Journal access lines, probe vs real requests, 400/404/500/502 phrasebook, per-node compare, HAProxy correlation |
| [60](#scenario-60-pipeline-process-log-files-varlogflowfirst) | Pipeline Process Log Files | Logging | `/var/log/flowfirst/processN.log`, levels, rotation, tracing one message across processes |
| [61](#scenario-61-scalability-test--thousands-of-concurrent-api-invocations-four-node-cluster) | Scalability Test — Thousands of Concurrent API Invocations | Scale | `scripts/load_test_api.py` step test, scale parameters, bottleneck table, queue drain, DB cross-check |
| [62](#scenario-62-performance-test--measured-latency-throughput-and-freshness-for-client-expectations) | Performance Test — Latency, Throughput & Freshness for Client Expectations | Performance | Fixed workload profiles, warm-up + 3 repeats, end-to-end latency from MariaDB, resource sampling, soak run, load during failover, result template |
| [63](#scenario-63-tls-everywhere--private-ca-mutual-tls-rest-api-and-encrypted-internal-traffic) | TLS Everywhere — Private CA, Mutual-TLS REST API, Encrypted Internal Traffic | Security | `security/` PKI scripts, certificate per use, HTTPS + client certificates on the API, RabbitMQ/MariaDB/ZooKeeper/Swift TLS, node4 identity, rotation, revocation, `verify_tls.sh` |
| [64](#scenario-64-iot-device-crud--flow-4-create-device) | IoT Device CRUD — Flow 4: Create Device | CRUD Pipeline | `POST /api/devices`, device fingerprint, device type validation, `devices` table upsert |
| [65](#scenario-65-iot-device-crud--flow-5-query-devices) | IoT Device CRUD — Flow 5: Query Devices | CRUD Pipeline | `POST /api/devices/query`, filter normalisation, `query_devices()` dynamic WHERE |
| [66](#scenario-66-iot-device-crud--flow-6-update-device-with-remote-edge-sync) | IoT Device CRUD — Flow 6: Update Device with Remote Edge-Sync | CRUD Pipeline | `PUT /api/devices/{id}`, dual-publish P3→P4+P5, edge_sync_token, remote return path |
| [67](#scenario-67-iot-device-crud--flow-7-soft-delete-device) | IoT Device CRUD — Flow 7: Soft-Delete Device | CRUD Pipeline | `DELETE /api/devices/{id}`, `soft_delete_device()`, `status = decommissioned` |
| [68](#scenario-68-notification-listener--receiving-and-printing-live-notifications) | Notification Listener — Receiving & Printing Live Notifications | Notifications | Enable `NOTIFICATIONS_ENABLED`, run `scripts/notification_listener.py`, filter by flow or event type |

> **Putting the production changes on a cluster:** see the [Production Rollout Run-Book](#production-rollout-run-book) at the end of this file (sample-password rotation, dead-letter policy, admin access, limits, TLS order, measurements, sign-off). Run `sudo python3 scripts/preflight_check.py` on each node first.

> **Backups:** see [Backup and Restore](#backup-and-restore) (encrypted, verified, restore-tested on containers only).
>
> **Message safety:** see [Message Safety](#message-safety-publisher-confirms-and-the-dead-letter-queue) (publisher confirms, the dead-letter policy, replaying rejected messages; without the policy a rejected message is **discarded**).
>
> **Monitoring:** see [Monitoring and Alerting](#monitoring-and-alerting) (Prometheus `/metrics` on every process, alert rules, tested against a real Prometheus; no dashboard).

---

## Complete Multi-Node Installation & Setup (RHEL 9.6)

---

### Phase 0: Bootstrap — Time Sync, Repository Clone & Python Environment (All 3 Nodes)

> **Do this first on every node — before any service is installed.**
> Time must be synchronised first: `git clone`, `pip install`, and every subsequent
> script depend on a correct system clock. Chrony is installed without the repo
> because `dnf install chrony` needs no local files.

---

#### Phase 0 (pre-step) — Create a dedicated admin user

> **Run as root on every node (1–4) before anything else.**

Do not perform the installation as `root`. Create a dedicated user (`flowadmin` is used throughout this guide), add it to the `wheel` group (RHEL's sudoers group), and set up passwordless SSH between the cluster nodes. All `sudo` commands in subsequent phases run as `flowadmin`.

**Why a dedicated user, not root:**
- Root login is typically disabled or audited separately; `flowadmin` actions are logged under their own identity in the journal and in `/var/log/secure`.
- The `flowuser` system account that *runs the pipeline processes* is separate and has no login shell — `flowadmin` is your operator/installer identity, `flowuser` is the service identity.
- Passwordless `sudo` is scoped to `flowadmin` only; other users on the nodes are unaffected.
- `install_certs.sh` and `configure_multinode_resources.sh` SSH between nodes using the calling user's identity — a named account with a known key is easier to audit and rotate than root.

```bash
# Run on EVERY node (1–4) as root:

# 1. Create the admin user (no login password — SSH key only)
useradd -m -s /bin/bash -c "FlowFirst admin" flowadmin

# 2. Add to wheel so 'sudo' works
usermod -aG wheel flowadmin

# 3. Grant passwordless sudo (avoids interactive password prompts in scripts)
echo "flowadmin ALL=(ALL) NOPASSWD: ALL" > /etc/sudoers.d/flowadmin
chmod 440 /etc/sudoers.d/flowadmin

# 4. Create the .ssh directory
mkdir -p /home/flowadmin/.ssh
chmod 700 /home/flowadmin/.ssh
chown flowadmin:flowadmin /home/flowadmin/.ssh
```

**Generate an SSH key pair once (on node1 or your admin workstation) and distribute it:**

```bash
# On node1 as flowadmin — generate the key pair (accept defaults, no passphrase for automation):
sudo -u flowadmin ssh-keygen -t ed25519 -C "flowadmin@flowfirst" -N "" -f /home/flowadmin/.ssh/id_ed25519

# Copy the public key to all four nodes (including node1 itself):
for node in ${NODE1_IP} ${NODE2_IP} ${NODE3_IP} ${NODE4_IP}; do
    sudo -u flowadmin ssh-copy-id -i /home/flowadmin/.ssh/id_ed25519.pub flowadmin@${node}
done
```

> **If `ssh-copy-id` requires a password** on first run, use a temporary root session or the cloud console to paste the public key into `/home/flowadmin/.ssh/authorized_keys` on each node (mode 600, owned by `flowadmin`).

**Verify passwordless SSH works between all nodes:**

```bash
for node in ${NODE1_IP} ${NODE2_IP} ${NODE3_IP} ${NODE4_IP}; do
    echo -n "${node}: "
    ssh -o BatchMode=yes -o ConnectTimeout=5 flowadmin@${node} "hostname" || echo "FAILED"
done
```

**From this point on, log in as `flowadmin` and prefix install commands with `sudo`:**

```bash
ssh flowadmin@${NODE1_IP}
# All subsequent phases are run as flowadmin with sudo
```

**Set `PKI_SSH_USER` in `.env`** so `security/install_certs.sh` uses `flowadmin` when pushing certificates to nodes over SSH (Phase 6 / Scenario 63):

```ini
PKI_SSH_USER=flowadmin
```

> **Disabling root SSH (optional but recommended after setup):**
> Once `flowadmin` SSH is confirmed working on all nodes, disable direct root SSH login:
> ```bash
> sudo sed -i 's/^#\?PermitRootLogin.*/PermitRootLogin no/' /etc/ssh/sshd_config
> sudo systemctl reload sshd
> ```
> All subsequent `sudo` operations still work through `flowadmin`. Re-enable only via the cloud console if locked out.

---

#### Phase 0a — Time Synchronisation — Install & Configure Chrony

> **Do this before cloning the repository or installing any packages.**
>
> RabbitMQ **will not start or join a cluster** if the system clock is not synchronised.
> Erlang's distribution protocol and cookie handshake are time-sensitive.
> A drift of more than a few seconds causes node rejection errors, silent queue
> disconnections, and Galera wsrep certification failures. Pacemaker fencing
> decisions can also race incorrectly on nodes with divergent clocks.

```bash
# chrony can be installed directly — no repo clone needed
sudo dnf install -y chrony
sudo systemctl enable --now chronyd
sudo chronyc makestep
```

To use a custom NTP server (recommended for air-gapped environments), run the full
setup script **after** the repo is cloned in Phase 0b:

```bash
# After Phase 0b — re-configure with a custom server if needed
cd /opt/flowfirst
# Pass your internal NTP server IP or use CHRONY_NTP_SERVER from .env:
sudo ./scripts/setup_chronyd.sh "${CHRONY_NTP_SERVER:-time.google.com}"
# Example with an explicit local NTP server IP:
# sudo ./scripts/setup_chronyd.sh <your-ntp-server-ip>
```

The full `setup_chronyd.sh` script performs these steps automatically:
1. Installs `chrony` via `dnf` (skips if already installed)
2. Writes a clean `/etc/chrony.conf` with the specified server + `pool.ntp.org` fallback
3. Enables and restarts `chronyd`
4. Forces an immediate time step with `chronyc makestep` (no waiting for gradual slew)
5. Prints `chronyc tracking` and `chronyc sources` output

**Verify sync on every node before continuing to Phase 0b:**

```bash
chronyc tracking
# Key fields to check:
#   Reference ID  — should show NTP server IP, not 7F7F0101 (local fallback)
#   System time   — offset should be < 0.1 seconds
#   Leap status   — must say "Normal"

# Quick one-liner for all three nodes:
for node in ${NODE1_IP} ${NODE2_IP} ${NODE3_IP}; do
    echo "=== ${node} ==="
    ssh "${node}" "chronyc tracking | grep -E 'Reference|System time|Leap'"
done
```

#### Optional: `CHRONY_NTP_SERVER` in `.env`

If all three nodes use the same internal NTP server, add this to `/opt/flowfirst/.env`
so `setup_chronyd.sh` picks it up automatically without a command-line argument:

```ini
# Set to your internal NTP server IP (leave blank to use pool.ntp.org)
CHRONY_NTP_SERVER=<your-ntp-server-ip>
```

#### Chrony Troubleshooting Reference

**Problem: `Leap status: Not synchronised`**

The daemon is running but cannot reach any NTP server yet. Common on fresh installs.

```bash
# Check which servers are being contacted
chronyc sources -v

# If all show "?" (no response), verify connectivity:
ping pool.ntp.org

# Force a manual query to a specific server:
chronyc -a 'server pool.ntp.org iburst'

# Wait up to 60 seconds, then recheck:
sleep 30 && chronyc tracking | grep "Leap status"
```

**Problem: `Reference ID: 7F7F0101` (local fallback)**

chronyd is not reaching any external server and has fallen back to its own internal clock.
This is **not** synchronised to real time.

```bash
# Confirm firewall allows outbound UDP 123
sudo firewall-cmd --list-all | grep 123

# Open it if missing:
sudo firewall-cmd --permanent --add-service=ntp
sudo firewall-cmd --reload

# Restart and recheck:
sudo systemctl restart chronyd
sleep 10 && chronyc tracking
```

**Problem: chronyd running but RabbitMQ still rejects cluster join**

After fixing the clock, restart Erlang's epmd and RabbitMQ so the new system time
is reflected in the Erlang runtime:

```bash
sudo systemctl stop rabbitmq-server
sudo epmd -kill 2>/dev/null || true
sudo systemctl start rabbitmq-server
sudo rabbitmqctl cluster_status
```

---

#### Phase 0b — Clone the Repository & Set Up the Python Environment

```bash
# 1. Install base utilities and Python
sudo dnf install -y git python3 python3-pip

# 2. Clone repository to /opt/flowfirst
sudo git clone <repo-url> /opt/flowfirst
sudo chown -R $USER:$USER /opt/flowfirst
cd /opt/flowfirst

# 3. Create and activate Python virtual environment
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install --require-hashes -r requirements.txt

# 4. Initialise environment configuration
cp src/config/.env.example .env

# 5. Generate strong secrets ONCE (RabbitMQ / MariaDB / Swift / HAProxy-stats passwords and the Swift hash values).
#    The processes and install scripts refuse to start with the sample passwords from the example file.
./scripts/generate_secrets.sh --write
```

> **Pinned dependencies.** `requirements.txt` is a generated lock file: every package, including indirect ones, at an exact version with sha256 hashes, so every node installs the same code and a tampered or hijacked package is refused (`--require-hashes`). Humans edit only `requirements.in` (the direct, loosely constrained list) and regenerate with `./scripts/update_requirements.sh` (needs [`uv`](https://docs.astral.sh/uv/) on the machine that regenerates; nodes only need pip). One universal file covers Python 3.9 (the RHEL 9 default) to 3.14 and both x86_64 and aarch64, with environment markers choosing the right version per Python. Upgrade deliberately: `./scripts/update_requirements.sh --upgrade`, run the checks and a test cluster, then commit `requirements.in` and `requirements.txt` together. Tested: the lock installs with `--require-hashes` and passes `pip check` on Python 3.9 and 3.12 (macOS arm64) and every project module imports. Not tested: an install on RHEL 9 itself, or on x86_64. Limits: hashes prove the files are the ones that were locked, not that they are free of vulnerabilities (no scanner is wired in), and `eventlet` is in maintenance mode upstream, so the pin will stop receiving features. Wheels that need compiling (none were required on the tested platforms) would need a compiler on the node.

> **Tests and the quality gate.** `./scripts/ci_check.sh` runs six checks and needs no cluster, network or `.env`: (1) every `.py` parses with the Python 3.9 grammar, (2) `bash -n` on every `.sh`, (3) `ruff` with bug-finding rules only (`ruff.toml`), (4) `pytest` (155 tests), (5) each `requirements*.txt` pins every package named in its `.in` file and carries hashes, (6) no `.env`, `.key`, `.pem` or `.p12` file is tracked by git. `--quick` skips the HTTP tests. Set up once on a dev or CI host: `pip install --require-hashes -r requirements.txt -r requirements-dev.txt` (pytest and ruff live only in `requirements-dev.txt`, never on cluster nodes). `.github/workflows/ci.yml` is an example for GitHub Actions on Python 3.9 and 3.12; any other CI system only has to run the same two commands. What the tests cover: admin authorisation (`authz.py`), secret checks and `.env` parsing (`config.py`), TLS context construction with a throw-away CA (`src/tls/tls.py`), and the real API Gateway (Process 1) HTTP handler for request limits, generic errors and the admin gate, `scripts/preflight_check.py`, the Prometheus `/metrics` listener (plain and mutual TLS), the backup scripts, the systemd units, and the CRL scripts. The handler tests start `tests/_p1_harness.py` as a subprocess (importing API Gateway (Process 1) monkey-patches the standard library, which must not happen inside pytest) with the broker, ZooKeeper and Swift stubbed. **Not covered by the automated tests:** anything that needs a real RabbitMQ, MariaDB, ZooKeeper or Swift (publisher confirms, the dead-letter policy, deduplication, failover), the message handlers of Processes 2 to 5, and the shell install scripts beyond a syntax check (the backup, CRL and systemd scripts have tests; `install_*`, `setup_*` and `enable_*_tls.sh` do not). Several of these were checked **by hand** against real containers on one workstation (publisher confirms and the dead-letter queue, mutual TLS, the systemd sandbox, backups, `/metrics` with a real Prometheus, the health probe with a real HAProxy); each chapter says exactly what was run. None of it ran on a four-node cluster. Passing the gate shows these rules still hold; it does not show the cluster works. To check that the tests can fail, deliberately breaking `admin_decision` and the body-size limit made 3 tests fail. The full gate was run on macOS with Python 3.9.22 and 3.12.10, each in a fresh virtual environment built with `pip install --require-hashes` from the lock files (128 tests passed on both); earlier phases also ran it on 3.14. It has not been run on Linux or RHEL. There, eventlet 0.40.x (the Python 3.9 pin) crashes in its kqueue hub, so the harness selects the `selects` hub on macOS. Linux uses a different hub (epoll) and the workflow in `.github/workflows/ci.yml` has never been run, so a first CI run may still need adjustment.

> **Secrets are checked, not trusted.** `RABBITMQ_PASSWORD`, `MARIADB_PASSWORD`, `SWIFT_KEY` (when Swift is enabled) and `HAPROXY_STATS_PASS` must be at least `SECRET_MIN_LENGTH` (12) characters and not a well-known sample value (`flowpassword`, `admin123`, `testing`, anything starting `changeme`, …). Otherwise `config.py` raises at start (the message names the variable, never the value) and `install_rabbitmq_rhel9.sh`, `bootstrap_galera.sh`, `setup_swift.sh` and `setup_haproxy.sh` exit before changing anything. Run `generate_secrets.sh` on **one** node (or the admin host) and copy the resulting `.env` to the others — the values must be identical on every node, and `SWIFT_HASH_PATH_*` must never change once data is stored. The script keeps `.env.bak` (mode 600); delete it after copying. The check cannot tell a strong-looking but leaked password from a good one; it only blocks known-weak and short values. Never put the `.env` in git.

Then **edit `.env`** on each node. The rules are simple:

> **Only two variables differ between nodes. Everything else is identical.**

#### Per-node variables (change these on each node)

| Variable | node1 | node2 | node3 | What it means |
|---|---|---|---|---|
| `NODE_NAME` | `node1` | `node2` | `node3` | The hostname **of this node** — must match `/etc/hostname` and the name passed to `pcs cluster setup` |
| `CURRENT_NODE_IP` | `${NODE1_IP}` | `${NODE2_IP}` | `${NODE3_IP}` | The cluster-interface IP **of this node** — used by Galera's `wsrep_node_address` and Corosync bind address |

#### Shared variables (identical on all three nodes)

| Variable | Value | Notes |
|---|---|---|
| `CLUSTER_NAME` | `flowfirst_cluster` | Same on every node |
| `NODE1_IP` | `${NODE1_IP}` | **Always the IP of node1**, regardless of which node the file is on |
| `NODE2_IP` | `${NODE2_IP}` | **Always the IP of node2** |
| `NODE3_IP` | `${NODE3_IP}` | **Always the IP of node3** |
| `FLOWFIRST_VIP` | `${FLOWFIRST_VIP}` | Pacemaker Virtual IP — same on all nodes |
| `RABBITMQ_HOST` | `${FLOWFIRST_VIP}` | **Set to the VIP IP.** This is only a single-host fallback; in normal cluster operation `src/config/config.py` never reads it — it builds a full three-node host list from `NODE1_IP`…`NODE3_IP` automatically. |
| `RABBITMQ_HOSTS` | *(leave blank)* | **Leave empty.** `src/config/config.py` builds `${NODE1_IP}:5672,${NODE2_IP}:5672,${NODE3_IP}:5672` at runtime from `NODE*_IP`. |
| `RABBITMQ_PORT` | `5672` | Literal integer |
| `MARIADB_HOST` | `${FLOWFIRST_VIP}` | **Set to the VIP IP** — routes through HAProxy to the Galera cluster |
| `MARIADB_HOSTS` | *(leave blank)* | **Leave empty.** `src/config/config.py` builds `${NODE1_IP},${NODE2_IP},${NODE3_IP}` at runtime. |
| `MARIADB_PORT` | `3306` | Literal integer |
| `SWIFT_AUTH_URL` | `http://${FLOWFIRST_VIP}:8090/auth/v1.0` | **Set to the VIP IP.** HAProxy's Swift frontend (`SWIFT_LB_PORT=8090`) load-balances to the Swift proxy on `:8888`. Must be identical on all nodes including node4. Do **not** use a node-direct address (`:8888`) or the REST API port (`8080`). |

#### Concrete `.env` for each node

**node1** (`/opt/flowfirst/.env`):
```
NODE_NAME=node1
CURRENT_NODE_IP=192.168.1.101
# everything else identical — see src/config/.env.example
```

**node2** (`/opt/flowfirst/.env`):
```
NODE_NAME=node2
CURRENT_NODE_IP=192.168.1.102
# everything else identical — see src/config/.env.example
```

**node3** (`/opt/flowfirst/.env`):
```
NODE_NAME=node3
CURRENT_NODE_IP=192.168.1.103
# everything else identical — see src/config/.env.example
```

> **⚠️ `python-dotenv` does not expand `${VAR}` references**
>
> Shell interpolation like `RABBITMQ_HOST=${FLOWFIRST_VIP}` is read as the
> **literal string** `${FLOWFIRST_VIP}` by Python. This causes
> `int("${RABBITMQ_PORT}")` → `ValueError: invalid literal for int()`.
>
> **Rule:** every value in `.env` must be a plain literal — no `$VAR` or
> `${VAR}` anywhere. `src/config/config.py` guards against this with `_int_env` /
> `_str_env` helpers that warn and fall back to defaults, but the `.env`
> file is always the right place to fix it.

---

### Phase 1a: Setup MariaDB Galera Cluster on All 3 Nodes

All scripts automatically read parameters from `/opt/flowfirst/.env`.

#### 1. On Node 1:
```bash
cd /opt/flowfirst
# Configure Galera node settings (reads from .env)
sudo ./mariadb-galera/setup_galera_node.sh

# Bootstrap primary cluster node
sudo ./mariadb-galera/bootstrap_galera.sh
```

#### 2. On Node 2:
```bash
cd /opt/flowfirst
# Configure Galera node settings (reads from .env)
sudo ./mariadb-galera/setup_galera_node.sh

# Join Galera cluster
sudo systemctl enable --now mariadb
```

#### 3. On Node 3:
```bash
cd /opt/flowfirst
# Configure Galera node settings (reads from .env)
sudo ./mariadb-galera/setup_galera_node.sh

# Join Galera cluster
sudo systemctl enable --now mariadb
```

#### 4. Verify Galera Cluster Health on Any Node:
```bash
cd /opt/flowfirst
./mariadb-galera/check_galera_status.sh
```
*Expected Output:*
```text
wsrep_cluster_size: 3
wsrep_cluster_status: Primary
wsrep_connected: ON
wsrep_ready: ON
wsrep_local_state_comment: Synced
```

---

### Phase 1b: Install ZooKeeper Ensemble on All 3 Nodes

> **Install ZooKeeper after Galera and before RabbitMQ.**
> ZooKeeper must be running and in quorum before the pipeline processes start —
> they use it for leader election on startup.

```bash
cd /opt/flowfirst
sudo ./zookeeper/setup_zookeeper.sh
```

The script performs these steps automatically on each node:
1. Installs Java 17 (`java-17-openjdk-headless`) if not already present
2. Downloads Apache ZooKeeper `ZK_VERSION` (default `3.9.2`) from `downloads.apache.org`
3. Creates a dedicated `zookeeper` system user and `/var/lib/zookeeper/data`, `/var/log/zookeeper` directories
4. Writes `myid` from `CURRENT_NODE_IP` matched against `NODE1_IP`/`NODE2_IP`/`NODE3_IP` in `.env`
5. Writes `zoo.cfg` with the full 3-node ensemble and auto-purge settings
6. Installs and enables `zookeeper.service` as a systemd unit
7. Opens firewall ports `2181/tcp` (client), `2888/tcp` (peer), `3888/tcp` (election)
8. Starts ZooKeeper and checks for `imok` response

**Step 1 — Verify the ensemble has quorum across all nodes:**

Run on **each node** to verify basic health and check its server state:

```bash
# Each node should respond "imok"
echo ruok | nc 127.0.0.1 2181

# Check this node's state (one node in the ensemble will be leader, two will be followers)
echo mntr | nc 127.0.0.1 2181 | grep zk_server_state
# Output on one node:   zk_server_state    leader
# Output on other two:  zk_server_state    follower
```

**Step 2 — Verify quorum and full ensemble status (run ONLY on the node where `zk_server_state` is `leader`):**

> **Important:** `zk_synced_followers` is exposed **only by the elected ZooKeeper leader node**.
> Running `echo mntr | grep zk_synced_followers` on a follower node will return empty because followers do not track synced followers.
> Identify which node reported `zk_server_state leader` in Step 1, log in to that node (or query it remotely), and run:

```bash
# Run ON THE LEADER NODE to confirm quorum size (synced followers must be 2 for a 3-node ensemble):
echo mntr | nc 127.0.0.1 2181 | grep zk_synced_followers
# Expected output on leader: zk_synced_followers   2

# Check the state of all 3 nodes from the leader:
for node in ${NODE1_IP} ${NODE2_IP} ${NODE3_IP}; do
    echo -n "${node}: "
    echo mntr | nc -w2 "${node}" 2181 | grep zk_server_state || echo "unreachable"
done
```

#### ZooKeeper `.env` variables

| Variable | Default | Description |
|---|---|---|
| `ZK_VERSION` | `3.9.2` | ZooKeeper release to download |
| `ZK_CLIENT_PORT` | `2181` | Port clients (kazoo) connect to |
| `ZK_PEER_PORT` | `2888` | Inter-node replication port |
| `ZK_ELECTION_PORT` | `3888` | Leader election port |
| `ZK_HEAP_MB` | `256` | JVM heap size (MB) |
| `ZK_TIMEOUT` | `10` | Kazoo session timeout (seconds) |
| `ZK_DEDUP_TTL_MS` | `300000` | How long DB Updater (Process 4) dedup znodes live (ms) |
| `ZK_HOSTS` | *(auto)* | Leave blank — `src/config/config.py` builds from `NODE*_IP` |

#### ZooKeeper znode tree

> **Note on timing:** The `/flowfirst` znode hierarchy is **not** created by `setup_zookeeper.sh`.
> It is lazily initialised by the Python pipeline (`src/zookeeper/zk.py`) the **very first time any FlowFirst process (`process1` – `process4`) connects to ZooKeeper** (in Phase 5 / Scenario 1).
>
> If you connect with `zkCli.sh` immediately after installing ZooKeeper in Phase 1b, `/flowfirst` does not exist yet (`ls /` will only show `[zookeeper]`). This is expected.

The full znode tree populated once the pipeline processes start:

```
/flowfirst
├── election/
│   ├── process1/     ← ephemeral sequential election candidates
│   ├── process2/
│   ├── process3/
│   └── process4/
├── config/           ← persistent, hot-reloadable runtime config
│   ├── flow2_high_threshold    (default: "30.0")
│   ├── flow1_counter_step      (default: "10")
│   └── flow2_scale_factor      (default: "1.15")
├── registry/         ← ephemeral service registration (auto-removed on crash)
│   ├── process1/
│   │   └── node1     {"node":"node1","pid":1234,"started_at":"..."}
│   ├── process2/
│   ├── process3/
│   └── process4/
├── dedup/            ← ephemeral dedup barrier (DB Updater (Process 4) double-insert prevention)
│   └── <message_id>  ← created on first processing, auto-expires after ZK_DEDUP_TTL_MS
└── health/           ← pipeline health dashboard
    ├── process1/
    │   └── node1     {"state":"leader","updated_at":"...","pid":1234}
    ├── process2/
    ├── process3/
    └── process4/
```

#### Inspect the znode tree with zkCli

**Option A — Check ZooKeeper connectivity right now (Phase 1b):**

```bash
# Connect to the local ZooKeeper node
/opt/zookeeper/bin/zkCli.sh -server 127.0.0.1:2181

# Inside zkCli (fresh install before pipeline start):
ls /
# Output: [zookeeper]  (this confirms ZooKeeper is healthy and responding)

quit
```

**Option B — Inspect `/flowfirst` znodes (run AFTER pipeline processes start in Phase 5 / Scenario 1):**

```bash
# Connect to the ZooKeeper ensemble
/opt/zookeeper/bin/zkCli.sh -server 127.0.0.1:2181

# Inside zkCli (once processes are running):
ls /
# Output: [flowfirst, zookeeper]

ls /flowfirst
# Output: [config, dedup, election, health, registry]

ls /flowfirst/election/process1
get /flowfirst/config/flow2_high_threshold
ls /flowfirst/registry/process1
get /flowfirst/registry/process1/node1
ls /flowfirst/health/process1
get /flowfirst/health/process1/node1
quit
```

*(Optional) Pre-seed `/flowfirst` znodes now before starting the pipeline:*

If you want to manually create and seed the base znodes and default config before starting the pipeline:

```bash
cd /opt/flowfirst
source .venv/bin/activate
python3 -c "from src.zookeeper import zk; zk.get_client(); print('Base znodes and default config seeded successfully.')"
```

#### ZooKeeper Troubleshooting Reference

**Problem: `Connection refused` on port 2181**

ZooKeeper needs a quorum (2 of 3 nodes) to serve clients.
If only one node is up, clients cannot connect.

```bash
# Check the service on each node
sudo systemctl status zookeeper

# Confirm the ensemble config is correct
grep "server\." /opt/zookeeper/conf/zoo.cfg

# Check ZooKeeper logs
sudo journalctl -u zookeeper -n 50 --no-pager
```

**Problem: `myid` mismatch — ZooKeeper starts but never joins ensemble**

```bash
cat /var/lib/zookeeper/data/myid
# Must be 1, 2, or 3 matching the server.X line in zoo.cfg
# Re-run setup if wrong:
sudo ./zookeeper/setup_zookeeper.sh
```

**Problem: Java not found or wrong version**

```bash
java -version
# Must be 11, 17, or 21. Install if missing:
sudo dnf install -y java-17-openjdk-headless
```

---

### Phase 1c: Install Greenthread (eventlet) Dependencies on All 3 Nodes

> **Install after ZooKeeper (Phase 1b) and before RabbitMQ (Phase 2).**
> `eventlet` monkey-patches the Python stdlib so all I/O — pika, kazoo, pymysql,
> and the HTTP server — becomes co-operatively concurrent within a single OS process.

The `eventlet` and `dnspython` packages are installed automatically when you run
`pip install --require-hashes -r requirements.txt` in Phase 0b. This phase covers the OS-level
file descriptor limit that eventlet needs and verifies the installation.

#### 1c-a — Raise the OS file descriptor limit (all nodes)

Greenthreads use OS sockets and file handles.  Each process can run up to
`GT_POOL_SIZE` (default 1000) concurrent greenthreads; each needs a file descriptor.
The default RHEL 9 limit of 1024 is too low.

```bash
# Raise the system-wide limit permanently
echo "flowuser soft nofile 65536" | sudo tee -a /etc/security/limits.d/flowfirst.conf
echo "flowuser hard nofile 65536" | sudo tee -a /etc/security/limits.d/flowfirst.conf

# Also set it for the current session to take effect immediately
ulimit -n 65536

# Verify
ulimit -n
# Expected: 65536
```

The systemd service units already include `LimitNOFILE=65536` so this is handled
automatically when processes run under Pacemaker.

#### 1c-b — Verify eventlet is installed correctly

```bash
cd /opt/flowfirst
source .venv/bin/activate

# Confirm version
python -c "import eventlet; print('eventlet', eventlet.__version__)"
# Expected: eventlet 0.37.x (or later)

# Confirm monkey-patch works with pika
python -c "
import eventlet
eventlet.monkey_patch()
import pika
print('monkey-patch OK — pika imported after patch')
"

# Confirm gt.py loads cleanly
python -c "from src.gt import gt; gt.monkey_patch(); print('gt.py OK, pool size:', gt.GT_POOL_SIZE)"
```

#### Greenthread `.env` variables

| Variable | Default | Description |
|---|---|---|
| `GT_POOL_SIZE` | `1000` | Max concurrent greenthreads per process |
| `GT_WORKER_CONCURRENCY` | `4` | Concurrent HTTP handlers in API Gateway (Process 1) |
| `GT_METRICS_INTERVAL_S` | `60` | Seconds between throughput log lines |
| `GT_DEDUP_REAP_INTERVAL_S` | `600` | Seconds between ZK dedup znode reap runs |
| `API_REQUEST_POOL_SIZE` | `0` | HTTP requests one API Gateway (Process 1) serves at once; `0` = `GT_WORKER_CONCURRENCY` × 10 (40). See Scenario 61 |
| `API_LISTEN_BACKLOG` | `50` | Connections the kernel queues for API Gateway (Process 1) while all request slots are busy. See Scenario 61 |
| `METRICS_ENABLED` | `false` | Each process serves Prometheus `/metrics` on `METRICS_PORT_BASE` + its number (9101–9105). See Monitoring and Alerting |
| `METRICS_HOST` | `0.0.0.0` | Bind address of the metrics listener; use the node's own IP and firewall the ports |
| `METRICS_PORT_BASE` | `9100` | Process N listens on this + N (1024–65530). `mysqld_exporter`'s default 9104 collides with DB Updater (Process 4) |
| `GT_CONSUMERS_PER_QUEUE` | `1` | Consumer greenthreads per queue in Processes 2–5, each with its own RabbitMQ connection. See Scenario 61 |
| `GT_PREFETCH_COUNT` | `1` | Unacknowledged messages RabbitMQ hands one consumer at a time. See Scenario 61 |

#### Greenthread layout per process

| Process | Greenthreads | Purpose |
|---|---|---|
| **P1** | `p1_http_server` | eventlet GreenPool HTTP server (concurrent requests) |
| | `p1_rmq_heartbeat` | keeps publish connection alive (process_data_events) |
| | `p1_zk_health` | periodic ZooKeeper health reporter |
| | `process1_metrics` | throughput counter logger |
| **P2** | `p2_flow1_consumer` | dedicated pika consumer for `flow1_p1_to_p2` (own conn) |
| | `p2_flow2_consumer` | dedicated pika consumer for `flow2_p1_to_p2` (own conn) |
| | `p2_zk_health` | ZooKeeper health reporter |
| | `process2_metrics` | throughput counter logger |
| **P3** | `p3_flow1_consumer` | dedicated consumer for `flow1_p2_to_p3` (own conn) |
| | `p3_flow2_consumer` | dedicated consumer for `flow2_p2_to_p3` (own conn) |
| | `p3_zk_health` | ZooKeeper health reporter |
| | `process3_metrics` | throughput counter logger |
| **P4** | `p4_flow1_consumer` | dedicated consumer for `flow1_p3_to_p4` (own conn) |
| | `p4_flow2_consumer` | dedicated consumer for `flow2_p3_reflected` (own conn) |
| | `p4_zk_health` | ZooKeeper health reporter |
| | `p4_dedup_reaper` | periodic ZK dedup znode cleanup |
| | `process4_metrics` | throughput counter logger |

> **Why a separate pika connection per greenthread?**
> `pika.BlockingConnection` and its channels are not thread-safe (or greenthread-safe).
> Each greenthread that consumes or publishes must own its own connection object.
> This is the standard pattern for pika with any concurrency model.

---

### Phase 2a: Install RabbitMQ on All 3 Nodes

> **Prerequisite:** Phase 0b (chrony time sync) must be complete on all nodes before running this.
> The install script checks `chronyd` status at startup and prints a 10-second warning
> if the clock is not synchronised — press **Ctrl-C** and run `setup_chronyd.sh` first.

> **Note — four issues were fixed in the install script (as of 2025):**
> 1. **Cloudsmith baseurls dead** — `dl.cloudsmith.io/public/rabbitmq/…` returns 404.
> 2. **Cloudsmith GPG key URLs dead** — `dl.cloudsmith.io/…/gpg.*.key` also returns 404.
>    Keys now fetched from `github.com/rabbitmq/signing-keys` release 3.0.
> 3. **Server key fingerprint changed** — suffix `2620D4E7` → `26208342` in signing-keys release 3.0.
> 4. **`aarch64` not available on `yum1/yum2.rabbitmq.com`** — those mirrors only carry `x86_64`.
>    The script now detects the architecture at runtime (`uname -m`) and writes different repo stanzas:
>    - `x86_64` → `yum1/yum2.rabbitmq.com` (4 stanzas: x86_64 + noarch for both Erlang and RabbitMQ)
>    - `aarch64` → `packagecloud.io/rabbitmq` (2 stanzas: aarch64 for both Erlang and RabbitMQ)

```bash
cd /opt/flowfirst
sudo ./scripts/install_rabbitmq_rhel9.sh
```

The script performs these steps automatically:
1. **Pre-flight clock check** — warns and pauses 10 s if `chronyd` is not synchronised
2. Downloads all 3 GPG keys to temp files (bypasses `rpm --import /dev/stdin` pipe bug on RHEL 9)
3. Imports keys with `sudo rpm --import <file>`
4. Writes `/etc/yum.repos.d/rabbitmq.repo` with 4 stanzas using `yum1/yum2.rabbitmq.com` baseurls and GitHub `gpgkey=` URLs
5. Runs `dnf clean metadata && dnf makecache && dnf install -y erlang rabbitmq-server`
6. Writes `cluster_partition_handling = pause_minority` to `/etc/rabbitmq/rabbitmq.conf`, enables the `rabbitmq_management` plugin and starts the service

> **Why `pause_minority`:** if the network splits, a broker that can no longer see a majority of the cluster (at least 2 of 3) pauses itself and stops serving clients. It resumes and resyncs automatically when the network heals. The pipeline processes reconnect to the majority side through `src/config/config.py`'s multi-host list. RabbitMQ's default, `ignore`, keeps both sides running independently, so they diverge until an operator restarts one side.
> **Trade-off:** if two brokers are down at the same time, the third pauses as well and the pipeline stops until one of them returns (see [Scenario 19](#scenario-19-simultaneous-reboot-of-all-three-nodes-cluster-cold-start)).
>
> On a cluster installed before this change, apply it on **each of node1–node3**, one node at a time:
> ```bash
> echo 'cluster_partition_handling = pause_minority' | sudo tee -a /etc/rabbitmq/rabbitmq.conf
> sudo systemctl restart rabbitmq-server
> sudo rabbitmqctl environment | grep cluster_partition_handling   # expect pause_minority
> ```

---

### Phase 2b: Install the Swift Object Store Cluster on Nodes 1–3

> **Install after RabbitMQ (Phase 2a) and before HAProxy (Phase 3).** Node 4 does **not** run a Swift server; it only needs `python-swiftclient` (already in `requirements.txt`) and `SWIFT_AUTH_URL` pointing at the VIP.

Swift is installed as a **native service** from RPM packages and managed by plain `systemd` on each node (like MariaDB, RabbitMQ and ZooKeeper), not by Pacemaker. RHEL 9 has no Swift server packages in its own repositories, so the script uses the **CentOS Cloud SIG OpenStack** repository (release set by `SWIFT_OPENSTACK_RELEASE`, default `epoxy` = Swift 2.35). The repo file is written with `enabled=0` and is only enabled for the Swift `dnf install`, so routine `dnf update` runs on the cluster never pull in OpenStack builds of base libraries.

**What runs where (all of it on each of node1, node2 and node3):**

| Component | Port | Purpose |
|---|---|---|
| `openstack-swift-proxy` | `8888` (`SWIFT_PROXY_PORT`) | REST/auth front end (tempauth); 8080 is already the FlowFirst API frontend |
| `openstack-swift-account` / `-container` / `-object` | `6202` / `6201` / `6200` | Storage servers, bound to the node IP |
| replicators, auditors, updaters, reaper | – | Keep the three replicas consistent and repair after an outage |
| `rsyncd` | `873` | Replication transport, limited to the three node IPs |
| `memcached` | `11211` | Token cache shared by **all three** proxies, so a token issued via one node is valid on the others |
| HAProxy `swift_front` (Phase 3) | `8090` (`SWIFT_LB_PORT`) on the VIP | Clients use `http://${FLOWFIRST_VIP}:8090/auth/v1.0`; health check `GET /healthcheck` |

**How the cluster is formed:** the script builds the account, container and object rings from `NODE1_IP..NODE3_IP` with one **zone per node**, `SWIFT_REPLICAS=3` and a fixed seed, so every node generates **byte-identical** `*.ring.gz` files and nothing has to be copied between nodes. Every object is therefore stored on all three nodes, and one node can fail without losing data or availability (writes need 2 of 3 replicas).

**Before you run it**, generate the two cluster-wide hash secrets **once** and put the **same** values in `/opt/flowfirst/.env` on node1, node2 and node3. They must never change after data is stored. The script refuses to run while they still have the placeholder value:

```bash
# On ONE node only, then copy the output into .env on all three nodes
echo "SWIFT_HASH_PATH_PREFIX=$(openssl rand -hex 16)"
echo "SWIFT_HASH_PATH_SUFFIX=$(openssl rand -hex 16)"
```

Also review `SWIFT_USER` / `SWIFT_KEY` (the tempauth login, format `<account>:<user>`) and `SWIFT_AUTH_URL` (`http://<VIP>:8090/auth/v1.0`) in `.env` on **every node including node4**.

```bash
cd /opt/flowfirst
sudo ./swift/setup_swift.sh
```

The script performs these steps automatically:
1. Writes `/etc/yum.repos.d/centos-openstack-<release>.repo` (disabled by default) and imports the CentOS Cloud SIG GPG key
2. Installs `openstack-swift-proxy/-account/-container/-object`, `memcached`, `rsync-daemon`, `xfsprogs`
3. Creates the `swift` user and the data directory `${SWIFT_DEVICES_DIR}/${SWIFT_DEVICE}` (default `/srv/node/flowfirst`), verifies it supports extended attributes, adds a `.ismount` marker when it is a plain directory (so `swift-recon` does not report it as unmounted), and labels it `rsync_data_t` for SELinux
4. Writes `/etc/swift/swift.conf` with the hash prefix/suffix
5. Configures `memcached` (listening on `127.0.0.1` and the node IP) and `/etc/rsyncd.conf`
6. Writes the account, container, object and proxy server configs (originals saved as `*.conf.rpm-orig`)
7. Builds the three rings (existing `*.ring.gz` files are kept)
8. Opens the proxy and ring ports; opens `873` and `11211` **only** to the three node IPs
9. Enables and starts all Swift units, then checks `/healthcheck` and a tempauth login on this node

> **Storage device:** by default the data directory is a plain directory on the root filesystem, which is fine for evaluation. For real data mount a dedicated **XFS** filesystem at `${SWIFT_DEVICES_DIR}/${SWIFT_DEVICE}` on every node and set `SWIFT_MOUNT_CHECK=true` in `.env` before running the script, so Swift refuses to write into an unmounted (empty) mount point.

**Verify** (after the script has run on all three nodes; warnings about unreachable peers on the first nodes are expected):

```bash
# 1. Rings must be identical on all three nodes
for n in ${NODE1_IP} ${NODE2_IP} ${NODE3_IP}; do
  ssh "${n}" "cd /etc/swift && sudo md5sum account.ring.gz container.ring.gz object.ring.gz"
done

# 2. Ring layout: 3 devices, 3 replicas, zones 1-3
sudo swift-ring-builder /etc/swift/object.builder

# 3. All services active on this node
systemctl is-active openstack-swift-proxy openstack-swift-account openstack-swift-container \
  openstack-swift-object openstack-swift-object-replicator memcached rsyncd

# 4. Direct to a node's proxy, then through the VIP once Phase 3/5 is done
curl -s -D - -o /dev/null -H "X-Storage-User: ${SWIFT_USER}" -H "X-Storage-Pass: ${SWIFT_KEY}" \
  http://${CURRENT_NODE_IP}:8888/auth/v1.0 | grep -i x-auth-token
curl -s -D - -o /dev/null -H "X-Storage-User: ${SWIFT_USER}" -H "X-Storage-Pass: ${SWIFT_KEY}" \
  http://${FLOWFIRST_VIP}:8090/auth/v1.0 | grep -i x-auth-token
```

> **Changing the ring later** (adding a node or device, changing weights) is **not** automatic. Build the new ring on one node with `swift-ring-builder` and copy the resulting `*.ring.gz` files to `/etc/swift/` on every node, then restart the Swift services. Do not re-run `setup_swift.sh` for that: it keeps the existing rings.

---

### Phase 3: Identify Your Cluster NIC & Set `VIP_NIC` on Each Node

> **Critical — do this before running `setup_haproxy.sh` or `configure_multinode_resources.sh`.**
> RHEL 9 uses *predictable NIC names* (`ens3`, `ens192`, `enp0s3`) — NOT `eth0`.
> `VIP_NIC` must be the physical cluster interface name on **each individual node**.
> It may differ across nodes if hardware varies.

#### 3a — Discover the correct NIC name on each node

```bash
# Option 1: auto-detect via routing table (recommended)
ip route get ${FLOWFIRST_VIP} | awk '{print $5; exit}'
# Example output:  ens3

# Option 2: list all non-loopback interfaces
ip -o link show | awk -F': ' '{print $2}' | grep -v lo
# Example output:
#   ens3
#   ens192

# Option 3: show interface with IP
ip -4 addr show | grep -v lo
```

#### 3b — Set `VIP_NIC` in `.env` on each node

Edit `/opt/flowfirst/.env` and set `VIP_NIC` to the name found above.
The value may be different on each node:

```ini
# On node1 (e.g. KVM guest with virtio NIC)
VIP_NIC=ens3

# On node2 (e.g. physical server with Broadcom NIC)
VIP_NIC=enp0s3

# On node3 (e.g. VMware guest)
VIP_NIC=ens192
```

#### 3c — Run HAProxy setup on all 3 nodes

```bash
cd /opt/flowfirst
# Generates /etc/haproxy/haproxy.cfg from .env, sets ip_nonlocal_bind, SELinux boolean
sudo ./haproxy/setup_haproxy.sh
```

#### 3d — Verify HAProxy configuration syntax only

> **Do NOT start `haproxy.service` manually.** HAProxy is managed exclusively by
> Pacemaker as part of `vip-haproxy-group`. Starting it directly here will conflict
> with Pacemaker's resource management once the cluster is initialised.
> The only check needed at this stage is that the generated config file is valid.

```bash
# Validate config syntax — no service start
sudo haproxy -c -f /etc/haproxy/haproxy.cfg

# Confirm TCP frontends bind to the VIP, not wildcard
grep "bind" /etc/haproxy/haproxy.cfg
# Expected:
#   bind *:8080               ← REST API
#   bind *:9000               ← stats
#   bind ${FLOWFIRST_VIP}:3306   ← MariaDB (VIP only — no port conflict)
#   bind ${FLOWFIRST_VIP}:5672   ← RabbitMQ (VIP only — no port conflict)
```

HAProxy will be started automatically by Pacemaker in Phase 5 when the VIP is
assigned. Verify it is running at that point — see **Step 5c** below.

---

#### HAProxy Troubleshooting Reference

Five distinct root causes produce `haproxy.service: start job failed`. Each has a different fix:

---

**Failure 1 — Wrong `VIP_NIC` name**

Symptom:
```
[ALERT] cannot find interface ens999 (bind ... dev ens999)
```

Fix:
```bash
# Find the correct name
ip -o link show | awk -F': ' '{print $2}' | grep -v lo

# Update .env on this node
sed -i "s/^VIP_NIC=.*/VIP_NIC=ens3/" /opt/flowfirst/.env

# Re-run haproxy setup
sudo ./haproxy/setup_haproxy.sh
```

---

**Failure 2 — `ip_nonlocal_bind` not set (VIP not yet active)**

HAProxy binds to the VIP address before Pacemaker has assigned it to any node. The kernel rejects binding to an IP that does not currently exist on any interface.

Symptom:
```
[ALERT] cannot bind socket [192.168.1.100:8080]
```

Fix (the setup script already does this, but if running manually):
```bash
# Enable immediately
sudo sysctl -w net.ipv4.ip_nonlocal_bind=1

# Persist across reboots
echo "net.ipv4.ip_nonlocal_bind = 1" | sudo tee /etc/sysctl.d/99-haproxy-nonlocal.conf
sudo sysctl -p /etc/sysctl.d/99-haproxy-nonlocal.conf

# Restart HAProxy
sudo systemctl restart haproxy
```

Verify:
```bash
sysctl net.ipv4.ip_nonlocal_bind
# Expected: net.ipv4.ip_nonlocal_bind = 1
```

---

**Failure 3 — SELinux blocking HAProxy port binding, `dac_override`, or shared memory access**

RHEL 9 ships with SELinux in `Enforcing` mode. Without proper file ownership/contexts and the `haproxy_connect_any` and `daemons_enable_cluster_mode` booleans, HAProxy encounters socket binding denials or `dac_override` permission denials when accessing runtime state directories (`/var/lib/haproxy`, `/run/haproxy`, `/var/log/haproxy`).

Symptom — check the audit log:
```bash
sudo ausearch -m avc -ts recent | grep haproxy
# Example 1:
#   avc:  denied { name_bind } for  comm="haproxy" ... tcontext=system_u:object_r:http_cache_port_t:s0
# Example 2:
#   avc:  denied { dac_override } for  pid=... comm="haproxy" capability=1  scontext=system_u:system_r:haproxy_t:s0
# Example 3:
#   avc:  denied { map } for  comm="haproxy" path="/dev/shm/..." dev="tmpfs" tcontext=system_u:object_r:tmpfs_t:s0
```

Fix (the setup script already does this, but if needed manually):
```bash
# 1. Restore standard SELinux security contexts and directory permissions:
sudo mkdir -p /var/lib/haproxy /run/haproxy /var/log/haproxy
sudo chown -R haproxy:haproxy /var/lib/haproxy /run/haproxy /var/log/haproxy /etc/haproxy
sudo chmod 755 /var/lib/haproxy /run/haproxy /var/log/haproxy
sudo restorecon -Rv /var/lib/haproxy /run/haproxy /var/log/haproxy /etc/haproxy

# 2. Allow HAProxy to connect/bind to custom ports and operate in cluster/shared memory environments
sudo setsebool -P haproxy_connect_any 1
sudo setsebool -P daemons_enable_cluster_mode 1

# 3. If any persistent AVC denial remains, compile a targeted SELinux policy:
sudo ausearch -c 'haproxy' --raw | audit2allow -M haproxy_custom
sudo semodule -i haproxy_custom.pp

# Confirm booleans
getsebool haproxy_connect_any daemons_enable_cluster_mode
# Expected:
#   haproxy_connect_any --> on
#   daemons_enable_cluster_mode --> on
```

---

**Failure 4 — `option forwardfor` in TCP frontends**

`forwardfor` is an HTTP-only directive. Placing it in the `defaults` block causes
HAProxy to reject the config when any TCP frontend or backend inherits it.

Symptom:
```
[WARNING] config : 'option forwardfor' ignored for frontend mariadb_galera_front
           as it requires HTTP mode.
[WARNING] config : 'option forwardfor' ignored for backend mariadb_galera_back
           as it requires HTTP mode.
```

Fix (already corrected in `haproxy.cfg.template`):
`forwardfor` must be removed from `defaults` and placed only inside the HTTP
frontend (`flowfirst_api_front`) and HTTP backend (`flowfirst_api_back`).
Re-run setup to regenerate the config:

```bash
cd /opt/flowfirst
sudo ./haproxy/setup_haproxy.sh
```

---

**Failure 5 — Port Collision between HAProxy and Backend Services**

When HAProxy and backend services run on the same node, port separation prevents any `[Errno 98] Address already in use` collisions:
- **HAProxy REST API Frontend**: Listens on port `8080`, load-balancing requests across backend API Gateway (Process 1) nodes.
- **API Gateway (Process 1) REST API Backend**: Listens on dedicated backend port `8082` (`API_BACKEND_PORT=8082`), avoiding any collision with HAProxy (`8080`) or ZooKeeper AdminServer (`8081`).
- **MariaDB Galera Load Balancer**: HAProxy frontend listens on dedicated LB port `3307` (`MARIADB_LB_PORT=3307`) and load-balances across node daemons on port `3306`.
- **RabbitMQ AMQP Load Balancer**: HAProxy frontend listens on dedicated LB port `5673` (`RABBITMQ_LB_PORT=5673`) and load-balances across node daemons on port `5672`.
- **Swift Load Balancer**: HAProxy frontend listens on dedicated LB port `8090` (`SWIFT_LB_PORT=8090`) and load-balances across the `swift-proxy` daemons on port `8888` (`SWIFT_PROXY_PORT=8888`). The Swift proxy cannot use its conventional port `8080`, which is HAProxy's REST API frontend.

Symptom:
```
[ALERT] : Starting frontend flowfirst_api_front: cannot bind socket
          [0.0.0.0:8080]: Address already in use
# or in API Gateway (Process 1) startup:
OSError: [Errno 98] Address already in use
```

Fix:
1. API Gateway (Process 1) binds to `API_BACKEND_PORT=8082` while HAProxy binds to `API_PORT=8080` and routes to `node:8082`.
2. Ensure no manually launched Python processes (`python -m src.application.process1`) or unmanaged systemd instances are holding port 8082 before Pacemaker starts `flowfirst-p1-res-clone`:
   ```bash
   # Kill manual instances across nodes:
   sudo pkill -f 'python.*process1\.py' || true
   sudo systemctl stop flowfirst-process1 || true
   ```

```bash
cd /opt/flowfirst
sudo ./haproxy/setup_haproxy.sh

# Confirm the configuration
grep "bind" /etc/haproxy/haproxy.cfg
# Expected:
#   bind *:8080                  ← REST API Frontend
#   bind 0.0.0.0:9000            ← Stats dashboard
#   bind *:3307                  ← MariaDB LB Frontend
#   bind *:5673                  ← RabbitMQ LB Frontend
#   bind *:8090                  ← Swift LB Frontend
```

---

**Failure 6 — `backend 'static' has no server available!` / Default RHEL `/etc/haproxy/haproxy.cfg` Active**

When HAProxy is installed on RHEL 9 via `dnf install haproxy`, it creates a default configuration file containing a sample `frontend main`, `backend static`, and `backend app`. The sample `backend static` has no defined servers, causing HAProxy startup failure or warnings.

Symptom:
```
backend 'static' has no server available!
```

Root Cause:
The FlowFirst setup script `./haproxy/setup_haproxy.sh` has not yet been executed (or was executed before `.env` was created), leaving the default RHEL configuration in `/etc/haproxy/haproxy.cfg`.

Fix:
Generate the FlowFirst HAProxy configuration from template:

```bash
cd /opt/flowfirst
sudo ./haproxy/setup_haproxy.sh

# Verify that 'static' is gone and FlowFirst frontends/backends are present:
grep -E "frontend|backend" /etc/haproxy/haproxy.cfg
# Expected output:
#   frontend flowfirst_api_front
#   backend flowfirst_api_back
#   frontend mariadb_galera_front
#   backend mariadb_galera_back
#   frontend rabbitmq_amqp_front
#   backend rabbitmq_amqp_back
#   frontend swift_front
#   backend swift_back
```

---

**Quick all-in-one HAProxy pre-start check** (run after Phase 3, before Phase 5):

```bash
# 1. Kernel flag
sysctl net.ipv4.ip_nonlocal_bind

# 2. SELinux boolean
getsebool haproxy_connect_any

# 3. NIC exists
ip link show "$(grep ^VIP_NIC /opt/flowfirst/.env | cut -d= -f2)"

# 4. Config syntax
sudo haproxy -c -f /etc/haproxy/haproxy.cfg
```

> **Do not run `systemctl status haproxy` or `systemctl start haproxy` here.**
> HAProxy service status is only meaningful after Pacemaker brings it up in Phase 5.
> See **Step 5c** for the post-Pacemaker HAProxy verification.

---

### Phase 4: Install Systemd Services

#### Step 4a — Core Cluster Nodes (node1, node2, node3)
Install unit files for Processes 1–4 and disable direct systemd boot so Pacemaker manages process lifecycles:

```bash
cd /opt/flowfirst
# Install systemd units for cluster processes (API Gateway (Process 1) to 4)
sudo ./systemd/install_services.sh /opt/flowfirst

# Disable standard systemd boot so Pacemaker manages service lifecycles
sudo systemctl disable flowfirst-process1 flowfirst-process2 flowfirst-process3 flowfirst-process4
```

#### Step 4b — Remote Node 4 (`node4` only)

On Remote Node 4, **only Remote Worker (Process 5)** is installed and enabled. No server daemons (ZooKeeper server, RabbitMQ server, Galera server, Swift server, HAProxy, or Pacemaker) run on Node 4; Remote Worker (Process 5) acts strictly as a Python client (`pika`, `kazoo`, `python-swiftclient`).

Node 4 requires the same Phase 0 bootstrap as the cluster nodes — run **Phase 0 (pre-step)**, **Phase 0a** (time sync), and **Phase 0b** (clone repo & Python environment) on node4 before continuing here. Those steps are identical to what was done on nodes 1–3.

##### `.env` for node4

Node 4 does **not** run `generate_secrets.sh`. Copy the `.env` that was generated on one of the cluster nodes — all shared values (passwords, IPs, Swift hash paths) must be identical across all four nodes:

```bash
# From node1 (or any cluster node), copy the .env to node4:
scp /opt/flowfirst/.env <node4-ip>:/opt/flowfirst/.env
```

Then edit the two per-node variables directly on node4:

```bash
# On node4 — set these two values to node4's own identity:
sed -i 's/^NODE_NAME=.*/NODE_NAME=node4/' /opt/flowfirst/.env
sed -i 's/^CURRENT_NODE_IP=.*/CURRENT_NODE_IP=<node4-ip>/' /opt/flowfirst/.env
```

| Variable | node4 value | Notes |
|---|---|---|
| `NODE_NAME` | `node4` | Must be `node4` — Remote Worker (Process 5) uses this for ZooKeeper registration and the `remote_node` field in Flow 6 |
| `CURRENT_NODE_IP` | `<node4 IP>` | node4's own IP — used by ZooKeeper client binding |

All other variables (`NODE1_IP`–`NODE4_IP`, `FLOWFIRST_VIP`, `RABBITMQ_*`, `MARIADB_*`, `SWIFT_*`, passwords) are **identical to the cluster nodes** — do not change them.

> **Firewall on node4:** outbound connections from node4 to the cluster. Ensure node4 can reach:
> - `NODE1_IP`–`NODE3_IP` on port `5672` (RabbitMQ AMQP) — or `5671` when `TLS_RABBITMQ_ENABLED=true`
> - `NODE1_IP`–`NODE3_IP` on port `2181` (ZooKeeper) — or `2281` when `TLS_ZK_ENABLED=true`
> - `FLOWFIRST_VIP` on port `8090` (Swift, if `SWIFT_ENABLED=true`)

##### Install and start Remote Worker (Process 5)

```bash
cd /opt/flowfirst
sudo ./systemd/install_services.sh /opt/flowfirst --remote

# Enable and start Remote Worker (Process 5) on Remote Node 4
sudo systemctl enable --now flowfirst-process5
sudo systemctl status flowfirst-process5
```

#### Step 4c — The service sandbox (installed automatically by 4a and 4b)

`install_services.sh` also installs `systemd/flowfirst-hardening.conf` next to every unit as `/etc/systemd/system/flowfirst-process<N>.service.d/10-hardening.conf`, so there is one copy of the settings for all five processes. Compare the score before and after with `systemd-analyze security flowfirst-process1.service`.

| Setting | Effect on a FlowFirst process |
|---|---|
| `ProtectSystem=strict`, `PrivateTmp`, `PrivateDevices`, `ProtectHome=true` | the whole file system is read-only except `/var/log/flowfirst` (`LogsDirectory=`) and a private `/tmp`; no devices; `/home` and `/root` invisible |
| `ProtectProc=invisible`, `ProtectKernel*`, `ProtectControlGroups`, `ProtectClock`, `ProtectHostname` | other users' processes, kernel tunables, modules, logs, cgroups, the clock and the host name cannot be touched |
| `NoNewPrivileges`, `CapabilityBoundingSet=` (empty), `RestrictSUIDSGID`, `RestrictRealtime`, `RestrictNamespaces`, `LockPersonality` | no privilege gain, no capability at all (port 8082 needs none), no new namespaces |
| `MemoryDenyWriteExecute=true` | memory cannot be writable and executable at once |
| `RestrictAddressFamilies=AF_INET AF_INET6 AF_UNIX` | no raw, packet or netlink sockets |
| `SystemCallFilter=@system-service` minus `@privileged @resources` | only ordinary service system calls; others fail with `EPERM` |
| `UMask=0027` | log files are `0640`, not readable by other users |

**Tested** on Ubuntu 23.10 (systemd 253, aarch64) in a Colima VM, with the real units, a Python 3.11 venv built from the locked wheels, and real RabbitMQ 3, ZooKeeper 3.9 and MariaDB 10.11 containers: all five processes ran, flow 1 went through Processes 1 to 4 into the database, flow 3 went through Remote Worker (Process 5), the REST API answered, logging worked, and no sandbox denial appeared in the journal. `systemd-analyze security` scored each unit **1.2 OK**. Unsandboxed (previous units), API Gateway (Process 1) scored 8.6 EXPOSED and Remote Worker (Process 5) 9.2 UNSAFE. From inside the sandbox, a raw socket, a netlink socket, a packet socket, `setuid(0)`, `chown` and a write to `/etc` all failed; a TCP socket worked. A database outage under the sandbox behaved as the code does without the sandbox: DB Updater (Process 4) logged `Failed to connect to MariaDB`, rejected the message (`nack`, no requeue) and stayed up. In that test the dead-letter policy was **not** applied, so the rejected message was discarded (it never reached the database and `flowfirst_dead_letters` stayed empty); this is exactly why the policy in the Production Rollout Run-Book, R3, matters.

**Things that need your action**
* **`LOG_DIR` outside `/var/log/flowfirst`:** the process still starts but logs "file logging disabled" (read-only file system). Add a drop-in, `sudo systemctl edit flowfirst-process1` (or create `.../flowfirst-process1.service.d/99-local.conf`) containing `[Service]` and `ReadWritePaths=/your/dir`, then `daemon-reload` and restart. Tested.
* **Application directory or TLS files under `/home` or `/root`:** `install_services.sh` switches `ProtectHome` to `read-only` when the install directory is there. It cannot know about `TLS_*` paths; keep them under `/etc/flowfirst/tls` (the default), or set `ProtectHome=read-only` in a `99-local.conf`. The `/home` branch was **not** exercised.
* **Relaxing one setting:** put it in a `99-local.conf` drop-in (for example `MemoryDenyWriteExecute=false`); it sorts after `10-hardening.conf` and wins. `FLOWFIRST_NO_HARDENING=1 sudo ./systemd/install_services.sh ...` installs the units without the sandbox, for finding out whether the sandbox causes a problem.
* **Upgrading an existing node:** re-run `sudo ./systemd/install_services.sh /opt/flowfirst` (add `--remote` on node4), then `sudo systemctl daemon-reload`, and restart **one node at a time** through Pacemaker standby as in the Production Rollout Run-Book. Pacemaker's `systemd:` resources pick up drop-ins; this was **not** run under Pacemaker.

**Limits**
1. Not run on RHEL 9 (systemd 252) or with SELinux enforcing. The directives used all exist in systemd 252, but an SELinux denial would be a separate problem. On RHEL 9 check `ausearch -m avc -ts recent` after the first start.
2. `SystemCallFilter` was tested with what these code paths do on a quiet system: all five processes, the REST API, RabbitMQ, ZooKeeper and MariaDB connections, logging and a database outage. Not tested: TLS to every component, Swift (`SWIFT_ENABLED=false` in the test), a long run, a failover, a high-concurrency load. A blocked call shows up as `Operation not permitted` or a process killed by `SIGSYS`; look at `journalctl -u flowfirst-process1` first.
3. The sandbox covers only the five FlowFirst units. MariaDB, RabbitMQ, ZooKeeper, HAProxy and Swift keep their packages' own units.
4. Exposure 1.2 is the score for this unit; it is a rough guide, not a certificate. The remaining findings are things the process needs (network access, a user).
5. `RemoveIPC=true` and `PrivateTmp=true` mean files other processes leave in `/tmp` are not visible to FlowFirst. Nothing in the code uses `/tmp`.

---

### Phase 5: Pacemaker Cluster & VIP Initialization

> **Note:** The cluster setup is a **two-step process**. `pcsd` must be running and
> reachable on port 2224 on **all three nodes** before `pcs host auth` can succeed.
> The script handles this with a `--prepare-node` flag.

#### Step 5a — Run on ALL THREE nodes first (node1, node2, node3)

```bash
cd /opt/flowfirst
sudo ./pacemaker/setup_multinode_cluster.sh --prepare-node
```

This single command on each node:
1. Enables the HA repo (or adds CentOS Stream 9 HA + AppStream + BaseOS as fallback)
2. Installs `pcs pacemaker corosync fence-agents-all haproxy`
3. Automatically generates the FlowFirst `/etc/haproxy/haproxy.cfg` from `.env` (preventing default RHEL `backend 'static'` errors)
4. Enables and starts `pcsd` (TCP 2224)
4. Sets the `hacluster` password (read from `.env` → `HACLUSTER_PASS`, default `hacluster123`)
5. Opens firewall ports: `2224/tcp` `3121/tcp` `5403/tcp` `5404/udp` `5405/udp`
6. Adds all three node IPs to `/etc/hosts`

#### Step 5b — Run on NODE 1 ONLY after all three nodes are prepared

```bash
cd /opt/flowfirst

# Cluster init — pre-flight checks port 2224 on all nodes before proceeding
sudo ./pacemaker/setup_multinode_cluster.sh

# Deploy Virtual IP (VIP), HAProxy group, and cloned pipeline resources
sudo ./pacemaker/configure_multinode_resources.sh

# Verify cluster and all resources are started
sudo pcs status
```

The script runs a **pre-flight check** before `pcs host auth` — it tests TCP port 2224
on each node and exits with an actionable error if any node is not yet prepared.

#### Step 5c — Verify HAProxy is running under Pacemaker

Once `pcs status` shows all resources started, confirm HAProxy came up correctly
under Pacemaker's control on the node that holds the VIP:

```bash
# Confirm Pacemaker started haproxy-res as part of vip-haproxy-group
sudo pcs status | grep -A5 "vip-haproxy-group"
# Expected: haproxy-res Started on one node alongside flowfirst-vip

# Confirm HAProxy process is running
sudo systemctl status haproxy --no-pager

# Confirm HAProxy is listening on the expected ports
sudo ss -tlnp | grep haproxy
# Expected lines:
#   0.0.0.0:8080    ← REST API frontend
#   0.0.0.0:9000    ← stats dashboard
#   0.0.0.0:3307    ← MariaDB LB frontend (proxies to node 3306)
#   0.0.0.0:5673    ← RabbitMQ LB frontend (proxies to node 5672)
#   0.0.0.0:8090    ← Swift LB frontend (proxies to node swift-proxy 8888)

# Confirm stats page is reachable via the VIP
curl -s -u "${HAPROXY_STATS_USER}:${HAPROXY_STATS_PASS}" http://${FLOWFIRST_VIP}:${HAPROXY_STATS_PORT:-9000}/stats | grep -c "pxname"
# Expected: non-zero (one line per backend server)

# Review HAProxy logs if anything looks wrong
sudo journalctl -u haproxy -n 30 --no-pager
```

### Phase 6 (optional): TLS for the REST API and all internal links

Everything above works in plain text, and TLS is **off by default** (`TLS_*_ENABLED=false`). Once the cluster is healthy you can add a private CA, mutual-TLS access to the REST API and encrypted RabbitMQ, MariaDB, ZooKeeper and Swift connections, including from the remote node4. Do it on a test cluster first, one component at a time. The full procedure (certificate inventory, rollout order with undo steps, client usage, rotation, revocation, troubleshooting) and an honest list of what was and was not tested are in [Scenario 63](#scenario-63-tls-everywhere--private-ca-mutual-tls-rest-api-and-encrypted-internal-traffic). In short:

```bash
./security/init_ca.sh && ./security/issue_all.sh     # admin host, once
./security/install_certs.sh                          # copies each node only its own keys
sudo ./security/enable_rabbitmq_tls.sh               # then the zookeeper, mariadb and swift scripts, one node at a time
./security/verify_tls.sh                             # PASS/FAIL per endpoint
```

---

## Verification & Real-World Usage Scenarios

> **All commands in this section use shell variables that are defined in your `.env` file.**
> Source your `.env` once in the shell where you run these commands:
>
> ```bash
> cd /opt/flowfirst
> set -a && source .env && set +a
>
> # Confirm the key variables are loaded:
> echo "VIP=${FLOWFIRST_VIP}  NODE1=${NODE1_IP}  NODE2=${NODE2_IP}  NODE3=${NODE3_IP}"
> ```
>
> Every `${FLOWFIRST_VIP}`, `${NODE1_IP}`, `${NODE2_IP}`, `${NODE3_IP}`,
> `${MARIADB_USER}`, `${MARIADB_PASSWORD}`, `${MARIADB_DB}` reference below
> resolves from that single `.env` — **no IP addresses are hardcoded**.
>
> **Where to run commands.** Each scenario starts with a **Runs on** line, and a `# On nodeX` comment marks any step that runs somewhere else. General rules:
> - `pcs`, `crm_mon`, `crm_resource`, `corosync-cfgtool`: any cluster member (node1–node3). They act cluster-wide, so `pcs resource ban X node2` need not run on node2.
> - `sudo mysql -u root -p` (no `-h`), `rabbitmqctl`, `rabbitmqadmin`, `zkCli.sh -server 127.0.0.1:2181`: node1–node3 only, and they talk to the **local** service, which must be running.
> - `journalctl -u flowfirst-processN`: local logs only. Only the ZooKeeper-elected leader does the work, so read logs on the leader node (`curl -s http://${FLOWFIRST_VIP}:8080/zk/health | jq .pipeline_health`).
> - `tc`, `iptables`, `tcpdump`: affect or capture only the node they run on.
> - `curl http://${FLOWFIRST_VIP}…` and `mysql -h ${FLOWFIRST_VIP} …`: any machine that can reach the VIP (node1–node4 or a workstation; `mysql` needs a mariadb client).
> - node4 runs only `flowfirst-process5`, with no local MariaDB, RabbitMQ, ZooKeeper or Pacemaker.

### Scenario 1: Invoke REST API via Virtual IP & Verify Round-Robin Load Balancing

> **Runs on:** any machine that can reach the VIP (node1–node4 or a workstation).

Send requests directly to the **Virtual IP (`${FLOWFIRST_VIP}:${API_PORT:-8080}`)**:

#### A. Health Check via VIP
```bash
curl -s http://${FLOWFIRST_VIP}:8080/health | jq .
```

#### B. Trigger Flow 1 via VIP
```bash
curl -X POST http://${FLOWFIRST_VIP}:8080/api/flow1 \
  -H "Content-Type: application/json" \
  -d '{
    "item_id": 101,
    "counter": 200,
    "initial_data": "Multi-node VIP test payload"
  }' | jq .
```

#### C. Trigger Flow 2 via VIP
```bash
curl -X POST http://${FLOWFIRST_VIP}:8080/api/flow2 \
  -H "Content-Type: application/json" \
  -d '{
    "item_id": 202,
    "value": 31.8,
    "initial_data": "High temperature alert"
  }' | jq .
```

#### D. Verify Round-Robin Load Balancing

The loop below includes `Content-Type: application/json` — without it API Gateway (Process 1) rejects the body with a 400 before setting `handled_by_node`, causing every response to print `null` regardless of which node answered.

```bash
for i in {1..6}; do
  curl -s -X POST http://${FLOWFIRST_VIP}:8080/api/flow1 \
    -H "Content-Type: application/json" \
    -d "{\"item_id\": $i, \"counter\": 100}" | jq -r '.handled_by_node'
done
```
*Expected output — each of the three nodes handles two requests:*
```text
node1
node2
node3
node1
node2
node3
```

> **If all six lines show the same node** HAProxy is balancing correctly but the responses are 400s whose JSON has no `handled_by_node` key (jq prints `null`). Confirm with:
> ```bash
> curl -s -X POST http://${FLOWFIRST_VIP}:8080/api/flow1 \
>   -H "Content-Type: application/json" \
>   -d '{"item_id": 1, "counter": 100}'
> ```
> A successful response contains `"status": "success"` and `"handled_by_node": "nodeN"`. A 400 contains `"error"` instead.

Confirm the distribution using the HAProxy stats API (no `jq` required):
```bash
curl -s -u "${HAPROXY_STATS_USER}:${HAPROXY_STATS_PASS}" \
  "http://${FLOWFIRST_VIP}:9000/stats;csv" \
  | grep -E "^api_backend,(node1|node2|node3)" \
  | awk -F',' '{printf "%-10s  sessions: %s\n", $2, $8}'
```
*Each backend (`node1`, `node2`, `node3`) should show a non-zero and roughly equal session count.*

---

### Scenario 2: End-to-End Flow Execution & Database Content Examination

> **Runs on:** `scripts/test_flows_and_inspect_db.sh` and the `mariadb` commands: node1, node2 or node3 (a mariadb client is needed). `curl` commands: any machine that can reach the VIP (node1–node4 or a workstation).

This scenario executes Flow 1 and Flow 2 through the REST API, follows the messages through RabbitMQ and intermediate modifications, and inspects the resulting table rows and JSON audit trails stored in MariaDB Galera.

#### Automated Execution:
```bash
# Run the automated test & inspect script
./scripts/test_flows_and_inspect_db.sh
```

#### Manual Step-by-Step Walkthrough:

##### Step 1: Trigger Flow 1 (Initial Counter: 500)
```bash
curl -s -X POST http://${FLOWFIRST_VIP}:8080/api/flow1 \
  -H "Content-Type: application/json" \
  -d '{
    "item_id": 101,
    "counter": 500,
    "initial_data": "E2E inspection test payload for Flow 1"
  }' | jq .
```

##### Step 2: Trigger Flow 2 (Initial Metric: 35.00)
```bash
curl -s -X POST http://${FLOWFIRST_VIP}:8080/api/flow2 \
  -H "Content-Type: application/json" \
  -d '{
    "item_id": 202,
    "value": 35.00,
    "initial_data": "E2E high temperature alert for Flow 2"
  }' | jq .
```

##### Step 3: Inspect Summary Table in MariaDB
```bash
mariadb -h ${FLOWFIRST_VIP} -u ${MARIADB_USER} -p"${MARIADB_PASSWORD}" ${MARIADB_DB} --table -e "
SELECT
    id,
    flow_id,
    item_id,
    counter_value,
    metric_value,
    examined_status,
    verified_by,
    created_at
FROM processed_messages
ORDER BY id DESC
LIMIT 5;
"
```
*Expected Output:*
```text
+----+---------+---------+---------------+--------------+-----------------+-------------+---------------------+
| id | flow_id | item_id | counter_value | metric_value | examined_status | verified_by | created_at          |
+----+---------+---------+---------------+--------------+-----------------+-------------+---------------------+
| 12 |       2 |     202 |          NULL |        40.25 | HIGH            | process3    | 2025-08-20 10:15:22 |
| 11 |       1 |     101 |           510 |         NULL | NULL            | NULL        | 2025-08-20 10:15:20 |
+----+---------+---------+---------------+--------------+-----------------+-------------+---------------------+
```

##### Step 4: Inspect Flow 1 Audit Trail & Transformation History
```bash
mariadb -h ${FLOWFIRST_VIP} -u ${MARIADB_USER} -p"${MARIADB_PASSWORD}" ${MARIADB_DB} -e "
SELECT
    message_id,
    initial_data,
    counter_value,
    JSON_PRETTY(history_trail) AS audit_trail
FROM processed_messages
WHERE flow_id = 1
ORDER BY id DESC LIMIT 1\G
"
```
*Sample JSON History Output:*
```json
[
  {
    "stage": "process1_created",
    "timestamp": "2025-08-20 10:15:20",
    "status": "prepared",
    "source": "rest_api"
  },
  {
    "stage": "process2_reflected",
    "timestamp": "2025-08-20 10:15:21",
    "modification": "Added +10 to counter and flagged reflected"
  },
  {
    "stage": "process3_received_and_forwarded",
    "timestamp": "2025-08-20 10:15:21",
    "status": "forwarded_to_process4"
  },
  {
    "stage": "process4_saved_to_mariadb",
    "timestamp": "2025-08-20 10:15:22",
    "database_action": "INSERT/UPDATE processed_messages"
  }
]
```

##### Step 5: Inspect Flow 2 Audit Trail (Threshold Examination & Scaling)
```bash
mariadb -h ${FLOWFIRST_VIP} -u ${MARIADB_USER} -p"${MARIADB_PASSWORD}" ${MARIADB_DB} -e "
SELECT
    message_id,
    initial_data,
    metric_value,
    examined_status,
    verified_by,
    JSON_PRETTY(history_trail) AS audit_trail
FROM processed_messages
WHERE flow_id = 2
ORDER BY id DESC LIMIT 1\G
"
```
*Sample JSON History Output:*
```json
[
  {
    "stage": "process1_created",
    "timestamp": "2025-08-20 10:15:22",
    "status": "prepared",
    "source": "rest_api"
  },
  {
    "stage": "process2_examined_and_forwarded",
    "timestamp": "2025-08-20 10:15:22",
    "status_assigned": "HIGH",
    "modification": "Applied 15% scaling factor and assigned status"
  },
  {
    "stage": "process3_reflected",
    "timestamp": "2025-08-20 10:15:23",
    "modification": "Added verification seal and marked completed"
  },
  {
    "stage": "process4_saved_to_mariadb",
    "timestamp": "2025-08-20 10:15:23",
    "database_action": "INSERT/UPDATE processed_messages"
  }
]
```

---

### Scenario 3: Synchronous Multi-Master Galera Replication Verification

> **Runs on:** node1, node2 or node3 (each query targets a specific node IP with `-h`).

#### A. Ensure data exists before querying

This scenario verifies that the **same rows appear on all three Galera nodes simultaneously**. It needs at least one row in `processed_messages` to be meaningful. The pipeline is asynchronous — a message published via the API reaches MariaDB only after travelling through Transformer (Process 2), Forwarder (Process 3), and DB Updater (Process 4), which typically takes 1–3 seconds on a healthy cluster.

If Scenarios 1 and 2 were run just before this one, wait a few seconds and confirm rows have arrived before querying all three nodes:

```bash
# Send a fresh message and wait for it to be persisted
curl -s -X POST http://${FLOWFIRST_VIP}:8080/api/flow1 \
  -H "Content-Type: application/json" \
  -d '{"item_id": 301, "counter": 100}' | jq .

# Wait for the pipeline to process it (P1 → RabbitMQ → P2 → P3 → P4 → MariaDB)
sleep 5

# Confirm at least one row exists via the VIP before querying individual nodes
mariadb -h ${FLOWFIRST_VIP} -u ${MARIADB_USER} -p"${MARIADB_PASSWORD}" ${MARIADB_DB} \
  -e "SELECT COUNT(*) AS row_count FROM processed_messages;"
```

If `row_count` is still `0` after 5 seconds, the pipeline has a problem — check DB Updater (Process 4) logs before continuing:

```bash
sudo journalctl -u flowfirst-process4 -n 50 --no-pager | grep -E "ERROR|Persisted|Failed"
```

#### B. Query all three nodes directly

Once rows exist, query each node **by its own IP** (bypassing HAProxy) to prove Galera has synchronised the write to all members:

```bash
# Check on Node 1:
mariadb -h ${NODE1_IP} -u ${MARIADB_USER} -p"${MARIADB_PASSWORD}" ${MARIADB_DB} \
  -e "SELECT id, message_id, flow_id, item_id, counter_value, metric_value FROM processed_messages ORDER BY id DESC LIMIT 5;"

# Check on Node 2:
mariadb -h ${NODE2_IP} -u ${MARIADB_USER} -p"${MARIADB_PASSWORD}" ${MARIADB_DB} \
  -e "SELECT id, message_id, flow_id, item_id, counter_value, metric_value FROM processed_messages ORDER BY id DESC LIMIT 5;"

# Check on Node 3:
mariadb -h ${NODE3_IP} -u ${MARIADB_USER} -p"${MARIADB_PASSWORD}" ${MARIADB_DB} \
  -e "SELECT id, message_id, flow_id, item_id, counter_value, metric_value FROM processed_messages ORDER BY id DESC LIMIT 5;"
```

*All three nodes return identical rows — same `id` values, same `message_id`s, in the same order. This is Galera synchronous multi-master replication: the write was certified and committed on all three nodes before DB Updater (Process 4) received the acknowledgement.*

---

### Scenario 4: Galera Node Outage & Automatic Quorum Resynchronization

> **Runs on:** stop/start steps on **node3**; `check_galera_status.sh` and local `mysql` checks on **node1 or node2** (node3's MariaDB is down); `curl` from any machine that can reach the VIP (node1–node4 or a workstation).

1. **Simulate failure of Galera Node 3:**
   ```bash
   sudo systemctl stop mariadb  # (on Node 3)
   ```
2. **Verify Cluster Quorum remains healthy (2/3 nodes active):**
   ```bash
   ./mariadb-galera/check_galera_status.sh
   ```
   *Result:* `wsrep_cluster_size` is now `2`, cluster status remains `Primary`.
3. **Execute new API requests:**
   ```bash
   curl -X POST http://${FLOWFIRST_VIP}:8080/api/flow1 -d '{"item_id": 555}'
   ```
4. **Restart Node 3 and observe Automatic State Transfer (IST/SST):**
   ```bash
   sudo systemctl start mariadb  # (on Node 3)
   ```
   *Galera automatically synchronizes missing transactions. Cluster size returns to `3`.*

---

### Scenario 5: Galera IST — Incremental State Transfer

> **Runs on:** stop/start, `grastate.dat` and IST log checks on **node3** (the joiner); donor `wsrep_last_committed` checks on **node1 or node2**; the state-watch loop on **node1**.

#### What IST is and when it is used

IST (Incremental State Transfer) is the **fast path** Galera uses to re-sync a rejoining node. Instead of copying the entire dataset, the donor streams only the **write-sets the joiner missed** directly from its in-memory/disk **gcache ring buffer**.

IST is chosen automatically when **both** conditions are true:
1. The joiner's last committed transaction (`wsrep_last_committed`) is still inside the donor's gcache window — i.e., the donor has retained all write-sets since the joiner fell behind.
2. The joiner presents its `seqno` to the donor during the joining handshake and the donor can satisfy it.

If either condition fails, Galera falls back to SST.

```
gcache ring buffer on donor:
  [seqno 100]─────[seqno 850]────►[seqno 1000]  (current)
               ▲
         joiner last seqno = 850
         gap = 150 write-sets  ← fits in gcache → IST
```

#### 5.1 — Confirm gcache.size in the running cluster

```bash
# On any node
sudo mysql -u root -p -e "SHOW VARIABLES LIKE 'wsrep_provider_options'\G" \
  | grep gcache.size
```
```
wsrep_provider_options: ... gcache.size = 536870912; ...   ← 512 MB
```

#### 5.2 — Trigger a short outage and force IST

```bash
# Step 1: Stop node3
sudo systemctl stop mariadb   # on node3

# Step 2: Generate a small amount of write traffic (stays inside gcache window)
for i in $(seq 1 20); do
  curl -s -X POST http://${FLOWFIRST_VIP}:8080/api/flow1 > /dev/null
done
sleep 3

# Step 3: Check node3's last seqno before restart
sudo cat /var/lib/mysql/grastate.dat   # on node3
```
```
seqno:   892     ← last committed before outage
```

```bash
# Step 4: Check donor's current seqno
sudo mysql -u root -p -e "SHOW STATUS LIKE 'wsrep_last_committed';"
```
```
+---------------------+-------+
| Variable_name       | Value |
+---------------------+-------+
| wsrep_last_committed| 912   |   ← gap = 20 write-sets — well inside 512 MB gcache
+---------------------+-------+
```

#### 5.3 — Restart node3 and watch IST execute

```bash
# On node3
sudo systemctl start mariadb

# On node1 — watch the state progression in real time
sudo watch -n1 "mysql -u root -p -e \
  \"SHOW STATUS WHERE Variable_name IN \
  ('wsrep_local_state_comment','wsrep_cluster_size','wsrep_last_committed');\" 2>/dev/null"
```

Expected state progression on node3:
```
wsrep_local_state_comment: Joiner/Waiting       ← requesting IST from donor
wsrep_local_state_comment: Joiner/Receiving IST ← receiving write-sets
wsrep_local_state_comment: Joined               ← replay complete
wsrep_local_state_comment: Synced               ← fully caught up, rejoined cluster
```

Confirm IST (not SST) was used — IST leaves no SST lock files and the donor **stays `Synced`** throughout:
```bash
# On the donor node (node1 or node2) — state must never leave Synced during IST
sudo mysql -u root -p -e "SHOW STATUS LIKE 'wsrep_local_state_comment';"
```
```
+---------------------------+--------+
| wsrep_local_state_comment | Synced |   ← donor was never paused — IST confirmed
+---------------------------+--------+
```

```bash
# Confirm cluster back to 3 members
sudo mysql -u root -p -e "SHOW STATUS LIKE 'wsrep_cluster_size';"
```
```
+--------------------+-------+
| Variable_name      | Value |
+--------------------+-------+
| wsrep_cluster_size | 3     |
+--------------------+-------+
```

Also verify in the MariaDB error log:
```bash
sudo grep -i "IST\|incremental" /var/log/mariadb/mariadb.log | tail -5
```
```
[Note] WSREP: Prepared IST receiver for seqno 892-912
[Note] WSREP: 0.0 (node3): State transfer from 0.0 (node1) complete
[Note] WSREP: Received IST: 20 write-sets
[Note] WSREP: IST received
```

---

### Scenario 6: Galera SST — State Snapshot Transfer

> **Runs on:** `galera.cnf` edits on **each of node1–node3** (rolling restart node3 → node2 → node1); `grastate.dat` edit, start and SST log checks on **node3**; donor checks (`wsrep_local_state_comment`, `ps … rsync`) on the donor, normally **node1**.

#### What SST is and when it is used

SST (State Snapshot Transfer) is the **slow path** — a full copy of the entire dataset from donor to joiner. Galera uses it when:
1. The joiner has been offline so long that its `seqno` has **fallen outside the donor's gcache window** (the write-sets it missed have been overwritten in gcache).
2. The joiner has `seqno: -1` in `grastate.dat` (unclean shutdown / corrupted state).
3. The joiner is a **brand new node** with no data at all.

During SST with `wsrep_sst_method=rsync` (the default), the **donor is desynced** from the cluster for the duration of the copy. With `wsrep_sst_method=mariabackup` the donor stays online.

```
gcache ring buffer on donor:
  [seqno 1]────────────────────►[seqno 1000]  (current)
  ▲ gcache overwritten
  joiner last seqno = 200  ← outside gcache window → SST required
```

#### 6.1 — Set wsrep_sst_method (choose rsync or mariabackup)

Set `MARIADB_SST_METHOD` in `.env` on **all nodes** and re-run `setup_galera_node.sh`, then do a rolling restart (node3 → node2 → node1). All nodes must use the same method.

**rsync** (default — no extra packages required):
```
MARIADB_SST_METHOD=rsync
```
Simple, but it blocks the donor for the duration of the copy and is **unencrypted**. Acceptable when the cluster network is isolated.

**mariabackup** (non-blocking donor — recommended for production):
```
MARIADB_SST_METHOD=mariabackup
```
`setup_galera_node.sh` installs `mariadb-backup` automatically when this is set. The donor stays online during SST. To also encrypt the stream with the cluster's own TLS certificates (Scenario 63 must be done first):
```
MARIADB_SST_METHOD=mariabackup
MARIADB_SST_ENCRYPT=true
```
`TLS_DIR` / `TLS_CA_FILE` / `TLS_CLIENT_CERT` / `TLS_CLIENT_KEY` must point at the issued certificates. `setup_galera_node.sh` writes a `[sst]` section with `encrypt=4` (OpenSSL TLS over socat). **This path has not been tested on a real cluster.**

After setting the values, apply to all nodes and do the rolling restart:
```bash
# On all nodes (regenerates /etc/my.cnf.d/galera.cnf):
sudo ./mariadb-galera/setup_galera_node.sh

# Rolling restart — never restart all nodes at once:
sudo systemctl restart mariadb   # on node3 first, then node2, then node1
```

#### 6.2 — Force SST by making the joiner fall outside the gcache window

To reliably trigger SST (rather than IST), set `gcache.size` to a very small value **before** the outage so the cache fills quickly:

```bash
# On all nodes — temporarily shrink gcache to force SST
sudo mysql -u root -p -e "
  SET GLOBAL wsrep_provider_options='gcache.size=1M';
"
# Note: gcache.size is applied at startup; live SET GLOBAL may not take effect
# on all Galera versions. Edit galera.cnf and restart for a guaranteed change.
```

Or simply leave node3 offline long enough for heavy write traffic to fill and overwrite the gcache. Then:

```bash
# Confirm node3 is now outside the gcache window
sudo mysql -u root -p -e "SHOW STATUS LIKE 'wsrep_last_committed';"   # on node1
```
```
wsrep_last_committed: 15420    ← node3 last seqno was 892 — gap far exceeds 1M gcache
```

```bash
# Mark node3's state as requiring SST (clear seqno)
sudo systemctl stop mariadb   # on node3
sudo sed -i 's/^seqno:.*/seqno:   -1/' /var/lib/mysql/grastate.dat   # on node3
sudo cat /var/lib/mysql/grastate.dat
```
```
seqno:   -1      ← forces Galera to request SST on next start
safe_to_bootstrap: 0
```

#### 6.3 — Start node3 and observe SST

```bash
sudo systemctl start mariadb   # on node3
```

Watch state on node3:
```bash
sudo watch -n1 "mysql -u root -p -e \
  \"SHOW STATUS WHERE Variable_name IN \
  ('wsrep_local_state_comment','wsrep_cluster_size');\" 2>/dev/null"
```
```
wsrep_local_state_comment: Joiner/Waiting          ← requesting SST
wsrep_local_state_comment: Joiner/Receiving SST    ← full copy in progress (slow)
wsrep_local_state_comment: Joined                  ← SST received, applying write-sets
wsrep_local_state_comment: Synced                  ← rejoined
```

Watch the donor during SST — with `rsync` it will show `Donor/Desynced`:
```bash
# On node1 (donor)
sudo watch -n1 "mysql -u root -p -e \
  \"SHOW STATUS LIKE 'wsrep_local_state_comment';\" 2>/dev/null"
```
```
wsrep_local_state_comment: Donor/Desynced   ← donor is paused (rsync SST)
```

With `mariabackup`:
```bash
wsrep_local_state_comment: Synced   ← donor stays online (mariabackup SST)
```

Confirm SST in the error log:
```bash
sudo grep -i "SST\|snapshot" /var/log/mariadb/mariadb.log | tail -10
```
```
[Note] WSREP: State transfer required: own seqno -1
[Note] WSREP: Initiating SST (rsync) from node1
[Note] WSREP: SST complete, seqno: 15420
[Note] WSREP: 0.0 (node3): State transfer from 0.0 (node1) complete
```

Monitor SST data transfer rate (from a separate terminal on the donor):
```bash
# If using rsync SST, watch the rsync process on the donor
sudo watch -n2 "ps aux | grep rsync | grep -v grep"
# If using mariabackup SST, watch mariadb-backup
sudo watch -n2 "ps aux | grep mariadb-backup | grep -v grep"
```

---

### Scenario 7: `gcache.size` Impact on IST vs SST

> **Runs on:** config changes and restarts on **each of node1–node3** in turn (node3 → node2 → node1); the 7.3 IST/SST test on **node3**; `SET GLOBAL` in 7.4 on **all three nodes**, donor check on **node1**; 7.5 queries on any cluster member (node1, node2 or node3).

#### How gcache.size determines IST or SST

`gcache.size` is a circular ring buffer Galera maintains **per node** on disk (`/var/lib/mysql/galera.cache`). It stores recent write-sets (committed transactions) so that rejoining nodes can be caught up incrementally without a full copy.

```
┌─────────────────────────────────────────────────────┐
│  gcache ring buffer  (gcache.size = N bytes)        │
│                                                     │
│  oldest retained    newest retained   current head  │
│  ┌──────────────────────────────────────────────┐   │
│  │ seqno 800  seqno 850  ...  seqno 990  seqno 1000 │
│  └──────────────────────────────────────────────┘   │
│        ▲                                            │
│  joiner seqno 850 → gap 150 write-sets → IST ✅     │
│                                                     │
│  joiner seqno 200 → outside buffer     → SST ❌     │
└─────────────────────────────────────────────────────┘
```

**Decision rule** (simplified):
- `joiner_seqno >= oldest_seqno_in_gcache` → **IST** (fast, donor stays online)
- `joiner_seqno <  oldest_seqno_in_gcache` → **SST** (slow, full copy)

#### 7.1 — Effect of different gcache.size values

The table below shows how `gcache.size` affects the maximum tolerable outage before SST becomes required, assuming a moderate write rate of **~500 write-sets/second** (typical for the FlowFirst pipeline under load):

| `gcache.size` | Approx. write-sets stored | Max outage before SST | Use case |
|---|---|---|---|
| `128M` (default) | ~640,000 | ~21 minutes | Light workloads, dev/test |
| `512M` | ~2,560,000 | ~85 minutes | Standard production |
| `1G` | ~5,120,000 | ~170 minutes | Write-heavy workloads |
| `2G` | ~10,240,000 | ~5.7 hours | Long maintenance windows |
| `4G` | ~20,480,000 | ~11 hours | High-availability clusters |

> **Rule of thumb:** set `gcache.size` to cover at least your longest expected maintenance window multiplied by your peak write-set rate.

#### 7.2 — Change gcache.size live vs. at startup

`gcache.size` is a Galera provider option. It takes effect **at MariaDB startup** — a running cluster cannot resize gcache live without a restart.

```bash
# Edit on all nodes before rolling restart
sudo tee /etc/my.cnf.d/galera.cnf > /dev/null << 'EOF'
# ... (keep other settings) ...
wsrep_provider_options="gcache.size=2G"
EOF
```

Restart one node at a time (never all simultaneously):
```bash
# Restart node3 first, wait for Synced, then node2, then node1
sudo systemctl restart mariadb   # on node3
```

Wait until node3 is `Synced`:
```bash
sudo mysql -u root -p -e "SHOW STATUS LIKE 'wsrep_local_state_comment';"
# expect: Synced
```

Then restart node2, wait for `Synced`, then restart node1.

Confirm new gcache.size is active:
```bash
sudo mysql -u root -p -e "SHOW VARIABLES LIKE 'wsrep_provider_options'\G" \
  | grep gcache.size
```
```
wsrep_provider_options: ... gcache.size = 2147483648; ...   ← 2 GB
```

Check the actual gcache file size on disk:
```bash
ls -lh /var/lib/mysql/galera.cache
```
```
-rw-r--r-- 1 mysql mysql 2.0G Jun 9 12:05 /var/lib/mysql/galera.cache
```

#### 7.3 — Demonstrate IST succeeding with adequate gcache

```bash
# gcache.size=512M on all nodes (current config)
# Stop node3
sudo systemctl stop mariadb   # node3

# Generate 10,000 write-sets (well under 512M capacity)
for i in $(seq 1 500); do
  curl -s -X POST http://${FLOWFIRST_VIP}:8080/api/flow1 > /dev/null &
done
wait
sleep 5

# Restart node3 — IST will be used
sudo systemctl start mariadb   # node3

# Confirm IST in log
sudo grep -E "IST|Received IST|write-sets" /var/log/mariadb/mariadb.log | tail -5
```
```
[Note] WSREP: Prepared IST receiver
[Note] WSREP: Received IST: 500 write-sets
[Note] WSREP: IST received
```

#### 7.4 — Demonstrate SST triggered by undersized gcache

```bash
# Temporarily set gcache to 1M on all nodes to guarantee overflow
sudo mysql -u root -p -e \
  "SET GLOBAL wsrep_provider_options='gcache.size=1M';"   # on all 3 nodes
# Note: takes effect at next restart — restart node3 first while gcache is tiny

# Stop node3, generate heavy traffic, then restart
sudo systemctl stop mariadb   # node3
for i in $(seq 1 5000); do
  curl -s -X POST http://${FLOWFIRST_VIP}:8080/api/flow2 > /dev/null
done
sleep 5
sudo systemctl start mariadb   # node3

# SST should be triggered — watch donor go Donor/Desynced
sudo mysql -u root -p -e "SHOW STATUS LIKE 'wsrep_local_state_comment';"   # node1
```
```
+---------------------------+----------------+
| wsrep_local_state_comment | Donor/Desynced |   ← SST in progress
+---------------------------+----------------+
```

```bash
# Restore production gcache.size after the demo
sudo mysql -u root -p -e \
  "SET GLOBAL wsrep_provider_options='gcache.size=512M';"   # on all 3 nodes
# Then do a rolling restart to persist the change in galera.cnf
```

#### 7.5 — Monitor gcache usage in real time

```bash
# Check current gcache occupancy and pool size
sudo mysql -u root -p -e "
SHOW STATUS WHERE Variable_name IN (
  'wsrep_local_cached_downtime',
  'wsrep_provider_name'
);
SHOW VARIABLES LIKE 'wsrep_provider_options'\G
" 2>/dev/null | grep -E "gcache|cached"
```

Use the Galera gcache hit rate to assess if your `gcache.size` is adequate:
```bash
sudo mysql -u root -p -e "
SHOW STATUS WHERE Variable_name IN (
  'wsrep_local_state_comment',
  'wsrep_flow_control_sent',
  'wsrep_local_recv_queue_avg',
  'wsrep_last_committed'
);" 2>/dev/null
```

#### 7.6 — gcache.size Decision Guide

```
                    ┌─────────────────────────────────────┐
                    │  Node rejoining the cluster?        │
                    └──────────────┬──────────────────────┘
                                   │
              ┌────────────────────▼──────────────────────┐
              │  joiner_seqno >= oldest_seqno_in_gcache?  │
              └────────┬──────────────────────────┬────────┘
                       │ YES                      │ NO
                       ▼                          ▼
          ┌────────────────────┐      ┌──────────────────────┐
          │  IST               │      │  SST                 │
          │  • Fast            │      │  • Slow (full copy)  │
          │  • Donor stays     │      │  • Donor desynced    │
          │    online          │      │    (rsync) or online │
          │  • Only missing    │      │    (mariabackup)     │
          │    write-sets sent │      │  • seqno reset to -1 │
          └────────────────────┘      └──────────────────────┘
                   ▲                             ▲
          gcache.size LARGER            gcache.size smaller
          → more IST opportunities      → more SST fallbacks
```

**Summary:** increase `gcache.size` to keep more IST windows open; use `mariabackup` as your `wsrep_sst_method` to make unavoidable SSTs non-blocking.

---

### Scenario 8: Node Failure & Virtual IP (VIP) Failover

> **Runs on:** any cluster member (node1, node2 or node3) — preferably **node2 or node3**, since node1 is put into standby.

1. **Put Node 1 into standby mode:**
   ```bash
   sudo pcs node standby node1
   ```
2. **Observe Pacemaker immediately moving the Virtual IP and HAProxy to Node 2:**
   ```bash
   sudo pcs status
   ```
   *Result:* `vip-haproxy-group` is now `Started node2`.
3. **Issue `curl` requests with zero downtime:**
   ```bash
   curl -s http://${FLOWFIRST_VIP}:8080/health | jq .
   ```
4. **Restore Node 1:**
   ```bash
   sudo pcs node unstandby node1
   ```

---

### Scenario 9: HAProxy Real-Time Statistics Dashboard

> **Runs on:** a browser or `curl` on any machine that can reach the VIP (node1–node4 or a workstation).

Access the live stats dashboard to view real-time frontend and backend health metrics for both the REST API and MariaDB Galera cluster:
- **URL:** `http://${FLOWFIRST_VIP}:${HAPROXY_STATS_PORT:-9000}` (from `.env`)
- **Username:** `${HAPROXY_STATS_USER}` (from `.env`, default: `admin`)
- **Password:** `${HAPROXY_STATS_PASS}` (from `.env`, default: `admin123`)

---

### Scenario 10: Pacemaker Cluster Validation with `crm_mon` & `crm_resource`

> **Runs on:** any cluster member (node1, node2 or node3) (`pcs`/`crm_*` commands are cluster-wide). In 10.7 the underlying fault is repaired on the affected node (node3); clean-up can run from any member.

Use these commands on **any cluster node** to confirm that all Pacemaker resources are running, correctly distributed, and healthy.

---

#### 10.1 — Full One-Shot Cluster Status (`crm_mon -1Ar`)

```bash
sudo crm_mon -1Ar
```

**Representative expected output (all 3 nodes healthy):**
```
Cluster Summary:
  * Stack: corosync
  * Current DC: node1 (version 2.1.6-8.el9-6fdc9deea29) - partition with quorum
  * Last updated: Mon Jun  9 12:00:00 2025
  * Last change:  Mon Jun  9 11:50:00 2025 by root via cibadmin on node1
  * 3 nodes configured
  * 14 resource instances configured

Node List:
  * Online: [ node1 node2 node3 ]

Active Resources:
  * Resource Group: vip-haproxy-group:
    * flowfirst-vip (ocf::heartbeat:IPaddr2):        Started node1
    * haproxy-res (systemd:haproxy):               Started node1
  * Clone Set: flowfirst-p1-res-clone [flowfirst-p1-res]:
    * Started: [ node1 node2 node3 ]
  * Clone Set: flowfirst-p2-res-clone [flowfirst-p2-res]:
    * Started: [ node1 node2 node3 ]
  * Clone Set: flowfirst-p3-res-clone [flowfirst-p3-res]:
    * Started: [ node1 node2 node3 ]
  * Clone Set: flowfirst-p4-res-clone [flowfirst-p4-res]:
    * Started: [ node1 node2 node3 ]
```

> Key indicators:
> - `partition with quorum` — 2-of-3 nodes agree; cluster is fully operational
> - `vip` and `haproxy` are **grouped** and always land on the same node together
> - All 4 process clones show `Started: [ node1 node2 node3 ]` — active on every node

---

#### 10.2 — Continuous Live Monitor (`crm_mon -Ar`)

```bash
sudo crm_mon -Ar          # Refreshes every 15 seconds; press Ctrl-C to exit
sudo crm_mon -Ar -i 5     # Refresh every 5 seconds
```

This is the interactive equivalent and is ideal for watching resource migration in real time during a failover test (Scenario 8).

---

#### 10.3 — Resource Constraint & Location Refresh (`crm_resource -C`)

The `-C` flag clears all **failed-action history** and **location constraints** that Pacemaker accumulated when resources failed or were manually migrated. Run this after recovering a node that was put into standby:

```bash
sudo crm_resource -C
```

**Expected output:**
```
Cleaned up all resources on all nodes
```

To clear a **single resource** across all nodes:
```bash
sudo crm_resource -r flowfirst-vip -C
sudo crm_resource -r flowfirst-p1-res-clone -C
```

---

#### 10.4 — Per-Resource Status (`crm_resource --resource … --locate`)

```bash
# Where is the VIP currently running?
sudo crm_resource --resource flowfirst-vip --locate
```
```
resource flowfirst-vip is running on: node1
```

```bash
# Where is each clone instance running?
sudo crm_resource --resource flowfirst-p1-res-clone --locate
```
```
resource flowfirst-p1-res-clone is running on: node1  node2  node3
```

---

#### 10.5 — Move VIP to a Specific Node

```bash
# Force VIP (and HAProxy group) to node2
sudo crm_resource --resource vip-haproxy-group --move --node node2
sudo crm_mon -1Ar | grep vip
```
```
    * flowfirst-vip (ocf::heartbeat:IPaddr2):        Started node2
    * haproxy-res (systemd:haproxy):               Started node2
```

Remove the forced constraint to let Pacemaker re-balance freely:
```bash
sudo crm_resource --resource vip-haproxy-group --clear
```

---

#### 10.6 — Node Standby & Recovery (VIP Failover Validation)

```bash
# 1. Put node1 into standby (simulates node outage)
sudo pcs node standby node1

# 2. Confirm VIP migrated to node2 or node3
sudo crm_mon -1Ar | grep -E "vip|haproxy"
```
```
    * flowfirst-vip (ocf::heartbeat:IPaddr2):        Started node2
    * haproxy-res (systemd:haproxy):               Started node2
```

```bash
# 3. Confirm API still reachable via VIP — no downtime
curl -s http://${FLOWFIRST_VIP}:8080/health | jq .handled_by_node
```
```
"node2"
```

```bash
# 4. Restore node1
sudo pcs node unstandby node1

# 5. Clear any residual constraints
sudo crm_resource -C

# 6. Confirm all nodes back online
sudo crm_mon -1Ar | grep "Online:"
```
```
  * Online: [ node1 node2 node3 ]
```

---

#### 10.7 — Check for Failed Actions

```bash
sudo pcs status | grep -A5 "Failed"
```

If any resource has failed actions, the output will resemble:
```
Failed Resource Actions:
  * flowfirst-p1_start_0 on node3 'not installed' (5): call=12, status='complete',
      exitreason='Could not find Python venv at /opt/flowfirst/.venv',
      last-rc-change='Mon Jun  9 11:45:10 2025', queued=0ms, exec=48ms
```

Fix the underlying issue on `node3`, then clear the failure:
```bash
sudo crm_resource -r flowfirst-p1-res-clone -C
sudo pcs resource refresh flowfirst-p1-res-clone
```

---

#### 10.8 — Full `pcs status` Reference Output (All Healthy)

```bash
sudo pcs status
```
```
Cluster name: flowfirst_cluster
Cluster Summary:
  * Stack: corosync
  * Current DC: node1 (version 2.1.6-8.el9) - partition with quorum
  * Last updated: Mon Jun  9 12:05:00 2025
  * Last change:  Mon Jun  9 11:50:00 2025 by root via cibadmin on node1
  * 3 nodes configured
  * 14 resource instances configured

Node List:
  * Online: [ node1 node2 node3 ]

Full List of Resources:
  * Resource Group: vip-haproxy-group:
    * flowfirst-vip (ocf::heartbeat:IPaddr2):        Started node1
    * haproxy-res (systemd:haproxy):               Started node1
  * Clone Set: flowfirst-p1-res-clone [flowfirst-p1-res] (active, 3 of 3):
    * flowfirst-p1-res (systemd:flowfirst-process1):    Started node1
    * flowfirst-p1-res (systemd:flowfirst-process1):    Started node2
    * flowfirst-p1-res (systemd:flowfirst-process1):    Started node3
  * Clone Set: flowfirst-p2-res-clone [flowfirst-p2-res] (active, 3 of 3):
    * flowfirst-p2-res (systemd:flowfirst-process2):    Started node1
    * flowfirst-p2-res (systemd:flowfirst-process2):    Started node2
    * flowfirst-p2-res (systemd:flowfirst-process2):    Started node3
  * Clone Set: flowfirst-p3-res-clone [flowfirst-p3-res] (active, 3 of 3):
    * flowfirst-p3-res (systemd:flowfirst-process3):    Started node1
    * flowfirst-p3-res (systemd:flowfirst-process3):    Started node2
    * flowfirst-p3-res (systemd:flowfirst-process3):    Started node3
  * Clone Set: flowfirst-p4-res-clone [flowfirst-p4-res] (active, 3 of 3):
    * flowfirst-p4-res (systemd:flowfirst-process4):    Started node1
    * flowfirst-p4-res (systemd:flowfirst-process4):    Started node2
    * flowfirst-p4-res (systemd:flowfirst-process4):    Started node3

Daemon Status:
  corosync: active/enabled
  pacemaker: active/enabled
  pcsd:      active/enabled
```

---

## Service Lifecycle Scenarios

These scenarios cover stopping and starting individual pipeline services, groups of services, and infrastructure services (RabbitMQ, MariaDB, HAProxy) — with full validation at each step.

---

### Scenario 11: Stop & Start a Single Pipeline Process on One Node

> **Runs on:** any cluster member (node1, node2 or node3) (`pcs resource ban/clear` acts cluster-wide; it does not need to run on the target node). `journalctl` shows only the local node's logs.

This is the surgical approach: stop one process instance on one node while the clones on the other two nodes continue to serve traffic.

```bash
# On node2: stop process2 via Pacemaker (recommended — Pacemaker restarts it automatically)
sudo pcs resource ban flowfirst-p2-res-clone node2

# Confirm it is no longer running on node2
sudo crm_mon -1Ar | grep p2
```
```
  * Clone Set: flowfirst-p2-res-clone [flowfirst-p2-res] (active, 2 of 3):
    * flowfirst-p2-res (systemd:flowfirst-process2):    Started node1
    * flowfirst-p2-res (systemd:flowfirst-process2):    Started node3
    * Stopped: [ node2 ]
```

```bash
# Re-allow it on node2 and verify it comes back
sudo pcs resource clear flowfirst-p2-res-clone node2
sudo crm_resource -r flowfirst-p2-res-clone -C
sleep 5
sudo crm_mon -1Ar | grep p2
```
```
  * Clone Set: flowfirst-p2-res-clone [flowfirst-p2-res] (active, 3 of 3):
    * Started: [ node1 node2 node3 ]
```

---

### Scenario 12: Stop & Start All Four Pipeline Processes (Cluster-Wide)

> **Runs on:** any cluster member (node1, node2 or node3).

#### 12.1 — Stop all pipeline processes on all nodes

```bash
sudo pcs resource disable flowfirst-p1-res-clone
sudo pcs resource disable flowfirst-p2-res-clone
sudo pcs resource disable flowfirst-p3-res-clone
sudo pcs resource disable flowfirst-p4-res-clone
```

Confirm they are stopped:
```bash
sudo crm_mon -1Ar | grep -E "p1|p2|p3|p4"
```
```
  * Clone Set: flowfirst-p1-res-clone [flowfirst-p1-res] (disabled, 0 of 3):
    * Stopped: [ node1 node2 node3 ]
  * Clone Set: flowfirst-p2-res-clone [flowfirst-p2-res] (disabled, 0 of 3):
    * Stopped: [ node1 node2 node3 ]
  * Clone Set: flowfirst-p3-res-clone [flowfirst-p3-res] (disabled, 0 of 3):
    * Stopped: [ node1 node2 node3 ]
  * Clone Set: flowfirst-p4-res-clone [flowfirst-p4-res] (disabled, 0 of 3):
    * Stopped: [ node1 node2 node3 ]
```

#### 12.2 — Start all pipeline processes cluster-wide

Re-enable in dependency order: persistence first, then reflectors, then examiner, then producer.

```bash
sudo pcs resource enable flowfirst-p4-res-clone
sudo pcs resource enable flowfirst-p3-res-clone
sudo pcs resource enable flowfirst-p2-res-clone
sudo pcs resource enable flowfirst-p1-res-clone
sleep 10
sudo crm_mon -1Ar | grep -E "p1|p2|p3|p4"
```
```
  * Clone Set: flowfirst-p1-res-clone [flowfirst-p1-res] (active, 3 of 3):
    * Started: [ node1 node2 node3 ]
  * Clone Set: flowfirst-p2-res-clone [flowfirst-p2-res] (active, 3 of 3):
    * Started: [ node1 node2 node3 ]
  * Clone Set: flowfirst-p3-res-clone [flowfirst-p3-res] (active, 3 of 3):
    * Started: [ node1 node2 node3 ]
  * Clone Set: flowfirst-p4-res-clone [flowfirst-p4-res] (active, 3 of 3):
    * Started: [ node1 node2 node3 ]
```

#### 12.3 — Validate the REST API is serving traffic

```bash
curl -s http://${FLOWFIRST_VIP}:8080/health | jq .
```
```json
{
  "status": "ok",
  "handled_by_node": "node1"
}
```

---

### Scenario 13: Stop & Start RabbitMQ on a Single Node

> **Runs on:** stop/start on **node3**; `rabbitmqctl cluster_status` after the stop on **node1 or node2** (node3's broker is down); after the restart, any of node1–node3.

#### 13.1 — Stop RabbitMQ on node3

```bash
# On node3
sudo systemctl stop rabbitmq-server

# On node1 or node2 — verify the remaining 2-node RabbitMQ cluster still has quorum
sudo rabbitmqctl cluster_status
```
```
Cluster name: rabbit@node1
Disk Nodes: rabbit@node1  rabbit@node2  rabbit@node3
Running Nodes: rabbit@node1  rabbit@node2
Versions: ...
Alarms: (none)
```

The pipeline processes use `pika` multi-host connection parameters and will automatically reconnect to `node1` or `node2`.

#### 13.2 — Restart RabbitMQ on node3 and verify it rejoins

```bash
# On node3
sudo systemctl start rabbitmq-server
sleep 5
sudo rabbitmqctl cluster_status | grep "Running Nodes"
```
```
Running Nodes: rabbit@node1  rabbit@node2  rabbit@node3
```

---

### Scenario 14: Stop & Start MariaDB Galera on a Single Node

> **Runs on:** stop/start on **node2**; the `wsrep_cluster_size` check after the stop on **node1 or node3** (node2's MariaDB is down); after the restart, any of node1–node3.

#### 14.1 — Stop MariaDB on node2

```bash
# On node2
sudo systemctl stop mariadb

# On any other node — confirm the cluster is still running with 2 members
sudo mysql -u root -p -e "SHOW STATUS LIKE 'wsrep_cluster_size';"
```
```
+--------------------+-------+
| Variable_name      | Value |
+--------------------+-------+
| wsrep_cluster_size | 2     |
+--------------------+-------+
```

#### 14.2 — Restart MariaDB on node2 (normal rejoin — NOT a bootstrap)

```bash
# On node2
sudo systemctl start mariadb
sleep 10

# Confirm it rejoined
sudo mysql -u root -p -e "SHOW STATUS LIKE 'wsrep_cluster_size';"
```
```
+--------------------+-------+
| Variable_name      | Value |
+--------------------+-------+
| wsrep_cluster_size | 3     |
+--------------------+-------+
```

Verify the node is synced:
```bash
sudo mysql -u root -p -e "SHOW STATUS LIKE 'wsrep_local_state_comment';"
```
```
+---------------------------+--------+
| Variable_name             | Value  |
+---------------------------+--------+
| wsrep_local_state_comment | Synced |
+---------------------------+--------+
```

---

### Scenario 15: Stop & Start HAProxy on a Single Node

> **Runs on:** `pcs`/`crm_*` commands on any cluster member (node1, node2 or node3); the HAProxy stats `curl` on any machine that can reach the VIP (node1–node4 or a workstation).

HAProxy runs under Pacemaker as part of `vip-haproxy-group`. Do **not** manipulate it directly with `systemctl` — use `pcs resource` so Pacemaker stays in control.

#### 15.1 — Disable the VIP + HAProxy group (stops on current node, migrates to another)

```bash
# Move the group away from node1 to node2
sudo pcs resource move vip-haproxy-group node2
sleep 5
sudo crm_mon -1Ar | grep -E "vip|haproxy"
```
```
    * flowfirst-vip (ocf::heartbeat:IPaddr2):        Started node2
    * haproxy-res (systemd:haproxy):               Started node2
```

#### 15.2 — Confirm HAProxy stats endpoint responds on the new node

```bash
curl -s http://${NODE2_IP}:9000/stats -o /dev/null -w "%{http_code}"
```
```
200
```

#### 15.3 — Clear the forced constraint and let Pacemaker decide

```bash
sudo pcs resource clear vip-haproxy-group
```

---

## OS Reboot Scenarios

These scenarios cover rebooting individual nodes, all nodes sequentially, and all nodes simultaneously, with complete validation procedures after each restart.

---

### Scenario 16: Reboot a Single Node (Node3) — Zero Pipeline Downtime

> **Runs on:** 16.1 and 16.4 on **node1**; the 16.2 reboot on **node3**; 16.3 checks on **node1 or node2** (node3 is down).

The cluster retains quorum (2 of 3 nodes remain) and all cloned resources continue on the surviving nodes.

#### 16.1 — Pre-reboot state check

```bash
# From node1 — record the baseline
sudo crm_mon -1Ar
sudo rabbitmqctl cluster_status | grep "Running Nodes"
sudo mysql -u root -p -e "SHOW STATUS LIKE 'wsrep_cluster_size';"
```

Expected: 3 nodes online, 3 RabbitMQ nodes running, `wsrep_cluster_size = 3`.

#### 16.2 — Reboot node3

```bash
# On node3
sudo reboot
```

#### 16.3 — While node3 is rebooting — verify cluster continuity

From **node1** or **node2**:
```bash
sudo crm_mon -1Ar | grep "Online:"
```
```
  * Online: [ node1 node2 ]
  * OFFLINE: [ node3 ]
```

```bash
# Confirm VIP still up (it was not on node3)
curl -s http://${FLOWFIRST_VIP}:8080/health | jq .
```
```json
{ "status": "ok", "handled_by_node": "node1" }
```

```bash
# Confirm pipeline clones still active on the 2 surviving nodes
sudo crm_mon -1Ar | grep "Clone Set"
```
```
  * Clone Set: flowfirst-p1-res-clone [flowfirst-p1-res] (active, 2 of 3):
    * Started: [ node1 node2 ]
    * Stopped: [ node3 ]
```

#### 16.4 — Post-reboot: validate node3 rejoins automatically

After node3 finishes booting (typically 60–90 seconds), Corosync and Pacemaker services start automatically (`enabled` in systemd), and Galera performs IST re-sync.

```bash
# From node1 — poll until node3 reappears
sudo crm_mon -1Ar | grep "Online:"
```
```
  * Online: [ node1 node2 node3 ]
```

```bash
# Confirm all pipeline clones restored to 3/3
sudo crm_mon -1Ar | grep -E "p1|p2|p3|p4"
```
```
  * Clone Set: flowfirst-p1-res-clone [flowfirst-p1-res] (active, 3 of 3):
    * Started: [ node1 node2 node3 ]
  * Clone Set: flowfirst-p2-res-clone [flowfirst-p2-res] (active, 3 of 3):
    * Started: [ node1 node2 node3 ]
  * Clone Set: flowfirst-p3-res-clone [flowfirst-p3-res] (active, 3 of 3):
    * Started: [ node1 node2 node3 ]
  * Clone Set: flowfirst-p4-res-clone [flowfirst-p4-res] (active, 3 of 3):
    * Started: [ node1 node2 node3 ]
```

```bash
# Confirm Galera is synced
sudo mysql -u root -p -e "SHOW STATUS LIKE 'wsrep_cluster_size';"
```
```
+--------------------+-------+
| Variable_name      | Value |
+--------------------+-------+
| wsrep_cluster_size | 3     |
+--------------------+-------+
```

```bash
# Clear any residual failed-action history
sudo crm_resource -C
```

---

### Scenario 17: Reboot the Node Hosting the VIP (Node1) — Forced Failover

> **Runs on:** 17.1–17.2 on any cluster member (node1, node2 or node3) (prefer node2); the 17.3 reboot on **node1**; 17.4 checks on **node2 or node3** while node1 is down.

This is the highest-impact single-node reboot: the Virtual IP, HAProxy, and all process clones on node1 must migrate before node1 goes offline.

#### 17.1 — Identify the current VIP holder

```bash
sudo crm_resource --resource flowfirst-vip --locate
```
```
resource flowfirst-vip is running on: node1
```

#### 17.2 — Gracefully evacuate node1 before rebooting (recommended)

```bash
# Put node1 into standby — Pacemaker migrates VIP to node2 or node3 before reboot
sudo pcs node standby node1
sleep 5
sudo crm_mon -1Ar | grep -E "vip|haproxy|Online"
```
```
  * Online: [ node1 node2 node3 ]
  * Standby: [ node1 ]
  ...
    * flowfirst-vip (ocf::heartbeat:IPaddr2):        Started node2
    * haproxy-res (systemd:haproxy):               Started node2
```

```bash
# Confirm zero downtime during migration
curl -s http://${FLOWFIRST_VIP}:8080/health | jq .handled_by_node
```
```
"node2"
```

#### 17.3 — Reboot node1

```bash
# On node1
sudo pcs node unstandby node1   # (optional — Pacemaker will handle rejoin on boot)
sudo reboot
```

#### 17.4 — Post-reboot validation on node2 or node3

```bash
# Wait for node1 to come back (60–120 s), then:
sudo crm_mon -1Ar
```
```
  * Online: [ node1 node2 node3 ]

  * Resource Group: vip-haproxy-group:
    * flowfirst-vip (ocf::heartbeat:IPaddr2):        Started node2
    * haproxy-res (systemd:haproxy):               Started node2
  * Clone Set: flowfirst-p1-res-clone [flowfirst-p1-res] (active, 3 of 3):
    * Started: [ node1 node2 node3 ]
  ...
```

```bash
# Clear residual constraints
sudo crm_resource -C

# Trigger a test flow to confirm end-to-end pipeline health
curl -s -X POST http://${FLOWFIRST_VIP}:8080/api/flow1 | jq .
```

---

### Scenario 18: Sequential Rolling Reboot of All Three Nodes

> **Runs on:** steps 1–2 from **node1**; step 3 (node1 reboot) from **node2 or node3** — a script cannot poll for the node it is running on to come back. Post-reboot validation on any cluster member (node1, node2 or node3).

Reboot one node at a time, waiting for full rejoin before rebooting the next. Quorum is maintained throughout.

```bash
# === STEP 1: Reboot node3 (run steps 1–2 from node1) ===
ssh node3 'sudo reboot'

# Wait for node3 to come back (poll from node1)
until sudo crm_mon -1 2>/dev/null | grep -q "Online:.*node3"; do
  echo "Waiting for node3 to rejoin..."; sleep 10
done
echo "node3 is back online"

sudo crm_resource -C          # clear failure history from node3 outage
sleep 5

# === STEP 2: Reboot node2 ===
ssh node2 'sudo reboot'

until sudo crm_mon -1 2>/dev/null | grep -q "Online:.*node2"; do
  echo "Waiting for node2 to rejoin..."; sleep 10
done
echo "node2 is back online"

sudo crm_resource -C
sleep 5

# === STEP 3: Reboot node1 (VIP holder — evacuate first) ===
# Run this step from node2 or node3 — node1 is the one being rebooted
sudo pcs node standby node1
sleep 10   # allow VIP migration to complete

ssh node1 'sudo reboot'

until sudo crm_mon -1 2>/dev/null | grep -q "Online:.*node1"; do
  echo "Waiting for node1 to rejoin..."; sleep 10
done
echo "node1 is back online"

sudo pcs node unstandby node1
sudo crm_resource -C
```

#### Post-rolling-reboot validation

```bash
sudo crm_mon -1Ar
```
```
  * Online: [ node1 node2 node3 ]

  * Resource Group: vip-haproxy-group:
    * flowfirst-vip (ocf::heartbeat:IPaddr2):   Started node2
    * haproxy-res (systemd:haproxy):          Started node2
  * Clone Set: flowfirst-p1-res-clone [flowfirst-p1-res] (active, 3 of 3):
    * Started: [ node1 node2 node3 ]
  * Clone Set: flowfirst-p2-res-clone [flowfirst-p2-res] (active, 3 of 3):
    * Started: [ node1 node2 node3 ]
  * Clone Set: flowfirst-p3-res-clone [flowfirst-p3-res] (active, 3 of 3):
    * Started: [ node1 node2 node3 ]
  * Clone Set: flowfirst-p4-res-clone [flowfirst-p4-res] (active, 3 of 3):
    * Started: [ node1 node2 node3 ]
```

```bash
# Full infrastructure health sweep
sudo mysql -u root -p -e "SHOW STATUS LIKE 'wsrep_cluster_size';"  # expect 3
sudo rabbitmqctl cluster_status | grep "Running Nodes"              # expect all 3
curl -s http://${FLOWFIRST_VIP}:8080/health | jq .                     # expect ok
```

---

### Scenario 19: Simultaneous Reboot of All Three Nodes (Cluster Cold Start)

> **Runs on:** 19.1–19.2 on **each of node1–node3**; 19.3 `galera_new_cluster` on the **bootstrap node only**; 19.4 on the **other two nodes**; 19.5 `pcs cluster start` on each node (or `pcs cluster start --all` from one node); 19.6 on any cluster member (node1, node2 or node3); 19.7 `curl` from any machine that can reach the VIP (node1–node4 or a workstation), `rabbitmqctl` on node1–node3.

A complete power-cycle or simultaneous OS reboot causes the Galera cluster to lose quorum. Galera **requires a manual bootstrap** of the primary component on whichever node has the most up-to-date data before the other nodes can rejoin.

> ⚠️ **Warning:** Do not run `systemctl start mariadb` on all nodes simultaneously after a cold start — this will result in a split-brain condition. Bootstrap exactly one node first.

#### 19.1 — Record the Galera position before rebooting

While MariaDB is running, `grastate.dat` shows `seqno: -1` and `safe_to_bootstrap: 0`; Galera writes the real values only when the node **shuts down**. So before the reboot, record the live position instead:

```bash
# On each node before the reboot
sudo mysql -u root -p -e "SHOW STATUS WHERE Variable_name IN ('wsrep_last_committed','wsrep_local_state_uuid');"
```
```
wsrep_last_committed   2847     ← highest value = most up-to-date node
wsrep_local_state_uuid xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
```

After the reboot (19.2) `grastate.dat` holds the saved values and decides the **bootstrap node**:

```
# GALERA saved state
version: 2.1
uuid:    xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
seqno:   2847          ← highest seqno = most up-to-date node
safe_to_bootstrap: 1   ← Galera sets this on the last node to leave the cluster
```

The node with `safe_to_bootstrap: 1` (or the highest `seqno`) is the **bootstrap node**. With a simultaneous reboot all three nodes may show `safe_to_bootstrap: 0`; in that case pick the highest `seqno`, and set `safe_to_bootstrap: 1` in that node's `grastate.dat` before 19.3.

#### 19.2 — Reboot all three nodes simultaneously

```bash
# On each node (or via out-of-band IPMI/iDRAC):
sudo reboot
```

After the nodes are back, MariaDB may already be trying to start: the setup tells node2 and node3 to `systemctl enable --now mariadb`, and with no bootstrapped primary it cannot join anything. Check each node, stop it if needed, and read the saved state:

```bash
# On each node
systemctl is-active mariadb          # 'activating' or 'failed' is expected without a primary
sudo systemctl stop mariadb || true
sudo cat /var/lib/mysql/grastate.dat
```

#### 19.3 — Bootstrap the primary Galera node (run on bootstrap node only)

After all nodes have finished booting, bring up Galera on the bootstrap node **first**:

```bash
# On the bootstrap node (e.g., node1 — the one with safe_to_bootstrap: 1)
sudo galera_new_cluster        # starts mariadb with --wsrep-new-cluster flag
sleep 5

sudo mysql -u root -p -e "SHOW STATUS LIKE 'wsrep_cluster_size';"
```
```
+--------------------+-------+
| Variable_name      | Value |
+--------------------+-------+
| wsrep_cluster_size | 1     |   ← primary component with 1 member — correct
+--------------------+-------+
```

#### 19.4 — Rejoin node2 and node3 (normal start — NOT bootstrap)

```bash
# On node2
sudo systemctl start mariadb
# On node3
sudo systemctl start mariadb
```

Wait for IST/SST to complete (30–120 seconds depending on data volume):

```bash
# From node1
sudo mysql -u root -p -e "SHOW STATUS LIKE 'wsrep_cluster_size';"
```
```
+--------------------+-------+
| Variable_name      | Value |
+--------------------+-------+
| wsrep_cluster_size | 3     |
+--------------------+-------+
```

#### 19.5 — Restart Pacemaker cluster after cold start

After a simultaneous reboot, Pacemaker may be waiting for quorum. Start the cluster on all nodes:

```bash
# On each node
sudo pcs cluster start

# From node1 — wait for quorum
sleep 15
sudo pcs status | grep -E "Online:|quorum"
```
```
  * Stack: corosync
  * Current DC: node1 (version 2.1.6-8.el9) - partition with quorum
  * Online: [ node1 node2 node3 ]
```

#### 19.6 — Re-enable and validate all pipeline resources

Pacemaker remembers resource state from before the cold start. If resources are still enabled, they will auto-start once quorum is restored. Verify:

```bash
sudo crm_mon -1Ar
```
```
  * Online: [ node1 node2 node3 ]

  * Resource Group: vip-haproxy-group:
    * flowfirst-vip (ocf::heartbeat:IPaddr2):   Started node1
    * haproxy-res (systemd:haproxy):          Started node1
  * Clone Set: flowfirst-p1-res-clone [flowfirst-p1-res] (active, 3 of 3):
    * Started: [ node1 node2 node3 ]
  * Clone Set: flowfirst-p2-res-clone [flowfirst-p2-res] (active, 3 of 3):
    * Started: [ node1 node2 node3 ]
  * Clone Set: flowfirst-p3-res-clone [flowfirst-p3-res] (active, 3 of 3):
    * Started: [ node1 node2 node3 ]
  * Clone Set: flowfirst-p4-res-clone [flowfirst-p4-res] (active, 3 of 3):
    * Started: [ node1 node2 node3 ]
```

If any resources did not auto-start:
```bash
sudo pcs resource enable flowfirst-p4-res-clone
sudo pcs resource enable flowfirst-p3-res-clone
sudo pcs resource enable flowfirst-p2-res-clone
sudo pcs resource enable flowfirst-p1-res-clone
sudo crm_resource -C
```

#### 19.7 — End-to-end pipeline smoke test after cold start

```bash
# Send one message through each flow and verify DB persistence
curl -s -X POST http://${FLOWFIRST_VIP}:8080/api/flow1 | jq .
curl -s -X POST http://${FLOWFIRST_VIP}:8080/api/flow2 | jq .

sleep 5   # allow messages to traverse the pipeline

# Check the last 4 rows in the database (from any node via VIP)
mysql -u ${MARIADB_USER} -p"${MARIADB_PASSWORD}" -h ${FLOWFIRST_VIP} ${MARIADB_DB} \
  -e "SELECT id, flow_id, item_id, JSON_PRETTY(history_trail) \
      FROM processed_messages ORDER BY id DESC LIMIT 4\G"
```

```bash
# Confirm RabbitMQ queues are empty (all messages consumed)
sudo rabbitmqctl list_queues name messages consumers
```
```
Listing queues for vhost / ...
name                     messages  consumers
flow1_p1_to_p2           0         1
flow1_p2_to_p3           0         1
flow1_p3_to_p4           0         1
flow2_p1_to_p2           0         1
flow2_p2_to_p3           0         1
flow2_p3_reflected       0         1
flow3_p1_to_p5           0         1
flow3_p5_to_p2           0         1
flow3_p2_to_p4           0         1
```

> Each queue has exactly **one** consumer: only the ZooKeeper-elected leader of each process (and Remote Worker (Process 5) on node4) consumes, so `consumers` is `1`, not 3. A `0` means no leader is consuming that queue.

---

## Network Glitch Simulation Scenarios

These scenarios use Linux traffic-control (`tc netem`) and `iptables` to inject realistic network faults between cluster nodes — packet loss, latency spikes, bandwidth throttling, and hard network partitions — and verify the cluster's response using `pcs`, `crm_mon`, Corosync, Galera, and RabbitMQ commands. A helper script [`scripts/simulate_network_glitch.sh`](scripts/simulate_network_glitch.sh) is provided to automate injection and cleanup.

> **Prerequisites on every node:**
> ```bash
> sudo dnf install -y iproute-tc iptables-nft   # RHEL 9.6
> ```

---

### Network Glitch Tool Overview

| Tool | Fault type | Scope |
|---|---|---|
| `tc qdisc netem` | Delay, loss, corruption, reorder | Per-interface — affects all traffic on that NIC |
| `iptables -j DROP` | Hard packet drop (one-way or both-way) | Per-source/destination — surgical, selective |
| `iptables -j REJECT` | RST response (connection refused) | Per-source/destination |
| `corosync-cfgtool -k` | Kill a Corosync ring | Corosync link only |

All faults are **temporary and fully reversible**. Every sub-scenario includes the matching cleanup command.

---

### Scenario 20: Soft Network Degradation — Latency & Packet Loss (`tc netem`)

> **Runs on:** `tc` commands affect only the local NIC: 20.1, 20.4 and 20.5 on **node2**; observation in 20.2–20.3 on **node1 or node3**; 20.6 `curl` from any machine that can reach the VIP (node1–node4 or a workstation); in 20.7, `tc` on **node2**, `crm_*` on any member.

This simulates a flaky inter-node network link: high latency and packet loss that keeps the node reachable but causes Corosync token timeouts, Galera replication lag, and RabbitMQ connection retries.

#### 20.1 — Inject 300 ms delay + 10 % packet loss on node2's cluster interface

```bash
# On node2 — identify the cluster-facing interface
ip route get ${NODE1_IP} | awk '{print $5; exit}'
# Example output: eth0
IFACE=${VIP_NIC}   # cluster NIC for this node, set in .env (Phase 3)

# Add netem qdisc: 300ms delay ± 50ms jitter, 10% packet loss
sudo tc qdisc add dev ${IFACE} root netem \
    delay 300ms 50ms distribution normal \
    loss 10%

# Confirm it is active
sudo tc qdisc show dev ${IFACE}
```
```
qdisc netem 8001: root refcnt 2 limit 1000
    delay 300ms  50ms  loss 10%
```

#### 20.2 — Observe Corosync detecting the degraded link

From **node1** (within 30–60 s Corosync token timeout):
```bash
sudo corosync-cfgtool -s
```
```
Local node ID 1
RING ID 0
    id      = 192.168.1.101
    status  = ring 0 active with no faults

RING ID 0
    id      = 192.168.1.102     ← node2
    status  = Faulty            ← Corosync flagged the ring as degraded
```

```bash
sudo journalctl -u corosync -n 20 --no-pager | grep -E "token|loss|timeout"
```
```
corosync[1234]: [TOTEM ] A processor failed, forming a new configuration
corosync[1234]: [TOTEM ] token was lost, forming a new configuration
```

#### 20.3 — Observe Pacemaker response

```bash
sudo crm_mon -1Ar | grep -E "Online|OFFLINE|Stopped"
```

With only 300 ms delay Pacemaker usually **tolerates** the degradation (no fencing) unless the token timeout is breached repeatedly. Expected:
```
  * Online: [ node1 node2 node3 ]    ← cluster intact, no fencing triggered
```

If the loss is severe enough to cross the token timeout repeatedly, node2 may be fenced:
```
  * Online: [ node1 node3 ]
  * OFFLINE: [ node2 ]

  * Clone Set: flowfirst-p1-res-clone [flowfirst-p1-res] (active, 2 of 3):
    * Started: [ node1 node3 ]
    * Stopped: [ node2 ]
```

#### 20.4 — Check Galera replication lag on node2

```bash
# On node2
sudo mysql -u root -p -e "SHOW STATUS LIKE 'wsrep_local_recv_queue_avg';"
sudo mysql -u root -p -e "SHOW STATUS LIKE 'wsrep_flow_control_paused';"
```
```
+---------------------------+-------+
| wsrep_local_recv_queue_avg| 4.750 |   ← queue backing up due to latency
+---------------------------+-------+

+---------------------------+-------+
| wsrep_flow_control_paused | 0.320 |   ← 32% of time paused for flow control
+---------------------------+-------+
```

#### 20.5 — Check RabbitMQ connection state

```bash
# On node2
sudo rabbitmqctl list_connections peer_host state | grep -v "^Listing"
```
```
192.168.1.101   running
192.168.1.103   running
```
RabbitMQ uses `net_ticktime` (default 60 s). At 300 ms latency it stays connected but shows elevated send queue depths:
```bash
sudo rabbitmqctl list_connections peer_host send_pend | grep -v "^Listing"
```
```
192.168.1.101   2847
192.168.1.103   312
```

#### 20.6 — Verify the pipeline is still processing

```bash
curl -s -X POST http://${FLOWFIRST_VIP}:8080/api/flow1 | jq .
```
```json
{ "status": "queued", "flow": "flow1", "message_id": "abc123", "handled_by_node": "node1" }
```
Messages may be processed more slowly but the pipeline continues — HAProxy routes around the degraded node2.

#### 20.7 — Remove the degradation (cleanup)

```bash
# On node2
sudo tc qdisc del dev ${IFACE} root

# Confirm cleared
sudo tc qdisc show dev ${IFACE}
```
```
qdisc mq 0: root
```

```bash
# Clear any Pacemaker failure history
sudo crm_resource -C

# Verify full cluster health restored
sudo crm_mon -1Ar | grep "Online:"
```
```
  * Online: [ node1 node2 node3 ]
```

---

### Scenario 21: High Bandwidth Throttling — Simulating a Saturated Link

> **Runs on:** **node3** (the throttled node), except `crm_resource` and `rabbitmqctl list_queues`, which can run on any cluster member (node1, node2 or node3).

#### 21.1 — Limit node3's cluster interface to 1 Mbit/s

```bash
# On node3
IFACE=${VIP_NIC}   # cluster NIC for this node, set in .env (Phase 3)
sudo tc qdisc add dev ${IFACE} root tbf \
    rate 1mbit burst 32kbit latency 400ms

sudo tc qdisc show dev ${IFACE}
```
```
qdisc tbf 8001: root refcnt 2 rate 1Mbit burst 32Kb lat 400ms
```

#### 21.2 — Monitor Galera SST/IST behaviour under bandwidth constraint

```bash
# On node3 — watch wsrep state
watch -n2 "sudo mysql -u root -p -e \"SHOW STATUS LIKE 'wsrep_local_state_comment';\" 2>/dev/null"
```
```
+---------------------------+---------------+
| wsrep_local_state_comment | Donor/Desynced|   ← SST in progress — slow due to throttle
+---------------------------+---------------+
```

#### 21.3 — Inject simultaneous packet corruption (2 %)

```bash
# On node3 — add corruption on top of the throttle
sudo tc qdisc change dev ${IFACE} root netem corrupt 2%

# Check pipeline impact
sudo rabbitmqctl list_queues name messages | grep -v "^Listing"
```
```
flow1_p1_to_p2           12     ← messages backing up — the Transformer (Process 2) leader (if on node3) is struggling
flow1_p2_to_p3           8
```

#### 21.4 — Cleanup

```bash
# On node3
sudo tc qdisc del dev ${IFACE} root
sudo crm_resource -C
```

---

### Scenario 22: Hard Network Partition — `iptables` DROP Between Two Nodes

> **Runs on:** `iptables` rules are local: in 22.2 and 22.6 they run on **node1 and node3** (both sides of the partition); 22.3–22.4 on **node2**; 22.5 on **node1**; in 22.7 `pcs cluster start` on **node3**, then checks on **node1**.

This is the most dangerous fault: node2 can reach node1 and node3, but node1 and node3 **cannot reach each other**. This creates a split-brain candidate situation where Corosync must fence one side.

#### 22.1 — Understand the topology before partitioning

```bash
sudo crm_mon -1Ar | grep "Online:"
```
```
  * Online: [ node1 node2 node3 ]
```

Note the current DC (Designated Coordinator):
```bash
sudo pcs status | grep "Current DC"
```
```
  * Current DC: node1 (version 2.1.6-8.el9) - partition with quorum
```

#### 22.2 — Drop all cluster traffic between node1 and node3 (both directions)

```bash
# On node1 — drop all packets TO node3
sudo iptables -I INPUT  -s ${NODE3_IP} -j DROP
sudo iptables -I OUTPUT -d ${NODE3_IP} -j DROP

# On node3 — drop all packets TO node1
sudo iptables -I INPUT  -s ${NODE1_IP} -j DROP
sudo iptables -I OUTPUT -d ${NODE1_IP} -j DROP
```

> The cluster now has two sides:
> - **Side A**: node1 (can reach node2 only)
> - **Side B**: node3 (can reach node2 only)
> - **node2**: can reach both — it holds quorum

#### 22.3 — Observe Corosync partition detection (within 10–30 s)

```bash
# From node2 — watch Corosync reconfigure
sudo journalctl -u corosync -f --no-pager | grep -E "partition|quorum|fenc|membership"
```
```
corosync[1234]: [QUORUM] Members[2]: 1 2            ← node1+node2 form a partition
corosync[1234]: [QUORUM] Members[2]: 2 3            ← node3+node2 form a partition
corosync[1234]: [QUORUM] This node is within the primary component
```

Corosync uses a **two-node majority rule**: each side has 2 of 3 nodes (node1+node2 and node2+node3). node2 will be in both partitions' majority. The partition that includes node2 **retains quorum**.

#### 22.4 — Observe Pacemaker fencing decision

```bash
# From node2 (within 30–60 s)
sudo crm_mon -1Ar
```
```
Cluster Summary:
  * Stack: corosync
  * Current DC: node2 (version 2.1.6-8.el9) - partition with quorum
  * Online: [ node1 node2 ]
  * OFFLINE: [ node3 ]           ← node3 lost quorum as seen from node1+node2 partition

  * Resource Group: vip-haproxy-group:
    * flowfirst-vip (ocf::heartbeat:IPaddr2):   Started node1
    * haproxy-res (systemd:haproxy):          Started node1
  * Clone Set: flowfirst-p1-res-clone [flowfirst-p1-res] (active, 2 of 3):
    * Started: [ node1 node2 ]
    * Stopped: [ node3 ]
```

```bash
# Check stonith fencing events
sudo pcs status | grep -A5 "Fencing"
```
```
Fencing History:
  * reboot of node3 (via fence_xvm)   succeeded: Mon Jun 9 12:45:01 2025
```

> If STONITH is not configured, Pacemaker will refuse to fence and the cluster will show a warning but resources continue on the quorum side.

#### 22.5 — Verify VIP and pipeline are still serving

```bash
# From node1 — API must still respond via VIP
curl -s http://${FLOWFIRST_VIP}:8080/health | jq .
```
```json
{ "status": "ok", "handled_by_node": "node1" }
```

```bash
# Galera from node1
sudo mysql -u root -p -e "SHOW STATUS LIKE 'wsrep_cluster_size';"
```
```
+--------------------+-------+
| Variable_name      | Value |
+--------------------+-------+
| wsrep_cluster_size | 2     |   ← running with 2/3 — still accepts writes (no minority check)
+--------------------+-------+
```

```bash
# RabbitMQ from node1
sudo rabbitmqctl cluster_status | grep "Running Nodes"
```
```
Running Nodes: rabbit@node1  rabbit@node2
```

> **RabbitMQ partition handling (`pause_minority`):** node2 can still reach both sides, so this is a *partial* partition. RabbitMQ logs `Partial partition detected`, turns it into a full partition, and `pause_minority` pauses **one of node1 or node3**, whichever ends up in the minority. The output above shows node3 paused. Check on each of node1 and node3:
> ```bash
> # On node1 and on node3 — a paused broker has its rabbit app stopped and refuses AMQP clients
> sudo rabbitmq-diagnostics check_running || echo "this broker is paused (expected on one side)"
> ```
> When the DROP rules are removed in 22.6, the paused broker resumes and resyncs automatically. No restart is needed.

#### 22.6 — Restore network connectivity (cleanup)

```bash
# On node1 — remove DROP rules
sudo iptables -D INPUT  -s ${NODE3_IP} -j DROP
sudo iptables -D OUTPUT -d ${NODE3_IP} -j DROP

# On node3 — remove DROP rules
sudo iptables -D INPUT  -s ${NODE1_IP} -j DROP
sudo iptables -D OUTPUT -d ${NODE1_IP} -j DROP
```

#### 22.7 — Re-integrate node3 into the cluster

```bash
# On node3 — restart cluster services (Pacemaker may have shut them down after fencing)
sudo pcs cluster start
sleep 15

# From node1 — confirm node3 rejoined
sudo crm_mon -1Ar | grep "Online:"
```
```
  * Online: [ node1 node2 node3 ]
```

```bash
# Galera rejoins via IST
sudo mysql -u root -p -e "SHOW STATUS LIKE 'wsrep_cluster_size';"
```
```
+--------------------+-------+
| Variable_name      | Value |
+--------------------+-------+
| wsrep_cluster_size | 3     |
+--------------------+-------+
```

```bash
# RabbitMQ node3 rejoins
sudo rabbitmqctl cluster_status | grep "Running Nodes"
```
```
Running Nodes: rabbit@node1  rabbit@node2  rabbit@node3
```

```bash
# Clear all failure history
sudo crm_resource -C

# Confirm all process clones back at 3/3
sudo crm_mon -1Ar | grep -E "p1|p2|p3|p4"
```
```
  * Clone Set: flowfirst-p1-res-clone [flowfirst-p1-res] (active, 3 of 3):
    * Started: [ node1 node2 node3 ]
  * Clone Set: flowfirst-p2-res-clone [flowfirst-p2-res] (active, 3 of 3):
    * Started: [ node1 node2 node3 ]
  * Clone Set: flowfirst-p3-res-clone [flowfirst-p3-res] (active, 3 of 3):
    * Started: [ node1 node2 node3 ]
  * Clone Set: flowfirst-p4-res-clone [flowfirst-p4-res] (active, 3 of 3):
    * Started: [ node1 node2 node3 ]
```

---

### Scenario 23: Selective Port-Level Block — Corosync Ring Only

> **Runs on:** `iptables` on **node1 and node2**; `corosync-cfgtool` and `mysql` checks on **node1**; 23.3 on **node1 or node3**; in 23.4 `pcs cluster start` on **node2**.

Rather than dropping all traffic, this scenario blocks **only Corosync's UDP port 5405** between two nodes — simulating a misconfigured firewall rule that leaves SSH/HTTP/MariaDB/RabbitMQ intact but severs cluster heartbeats.

#### 23.1 — Block Corosync UDP 5405 between node1 and node2

```bash
# On node1 — block Corosync heartbeats to/from node2
sudo iptables -I INPUT  -s ${NODE2_IP} -p udp --dport 5405 -j DROP
sudo iptables -I OUTPUT -d ${NODE2_IP} -p udp --dport 5405 -j DROP

# On node2 — mirror the block
sudo iptables -I INPUT  -s ${NODE1_IP} -p udp --dport 5405 -j DROP
sudo iptables -I OUTPUT -d ${NODE1_IP} -p udp --dport 5405 -j DROP
```

#### 23.2 — Observe: Corosync loses heartbeat but application traffic flows

```bash
# Corosync ring shows fault (from node1)
sudo corosync-cfgtool -s
```
```
RING ID 0
    id      = 192.168.1.101
    status  = ring 0 active with no faults
RING ID 0
    id      = 192.168.1.102
    status  = Faulty
```

```bash
# But RabbitMQ still connects to node2 (AMQP 5672 is not blocked)
sudo rabbitmqctl list_connections peer_host state | grep ${NODE2_IP}
```
```
192.168.1.102   running
```

```bash
# And MariaDB still replicates to node2 (3306/4567 not blocked)
sudo mysql -u root -p -e "SHOW STATUS LIKE 'wsrep_cluster_size';"
```
```
+--------------------+-------+
| Variable_name      | Value |
+--------------------+-------+
| wsrep_cluster_size | 3     |   ← Galera still intact — only Corosync is impacted
+--------------------+-------+
```

#### 23.3 — Pacemaker detects node2 as OFFLINE

```bash
sudo crm_mon -1Ar | grep -E "Online|OFFLINE"
```
```
  * Online: [ node1 node3 ]
  * OFFLINE: [ node2 ]
```

Pacemaker clones on node2 stop; node2's VIP (if held) migrates to node1 or node3. Pipeline continues on 2/3 nodes.

#### 23.4 — Cleanup

```bash
# On node1
sudo iptables -D INPUT  -s ${NODE2_IP} -p udp --dport 5405 -j DROP
sudo iptables -D OUTPUT -d ${NODE2_IP} -p udp --dport 5405 -j DROP

# On node2
sudo iptables -D INPUT  -s ${NODE1_IP} -p udp --dport 5405 -j DROP
sudo iptables -D OUTPUT -d ${NODE1_IP} -p udp --dport 5405 -j DROP

# Restart cluster on node2
sudo pcs cluster start
sleep 15
sudo crm_resource -C
sudo crm_mon -1Ar | grep "Online:"
```
```
  * Online: [ node1 node2 node3 ]
```

---

### Scenario 24: Selective Port-Level Block — Galera Replication Only

> **Runs on:** `iptables` on **node1 and node3**; the 24.2 `mysql` check runs locally on **node3** (a remote `root` login is normally denied); 24.3 queries on any node with a mariadb client.

Block Galera's wsrep replication port (4567) between two nodes while leaving Corosync and RabbitMQ intact.

#### 24.1 — Block Galera replication between node1 and node3

```bash
# On node1
sudo iptables -I INPUT  -s ${NODE3_IP} -p tcp --dport 4567 -j DROP
sudo iptables -I OUTPUT -d ${NODE3_IP} -p tcp --dport 4567 -j DROP
sudo iptables -I INPUT  -s ${NODE3_IP} -p udp --dport 4567 -j DROP
sudo iptables -I OUTPUT -d ${NODE3_IP} -p udp --dport 4567 -j DROP

# On node3 — mirror
sudo iptables -I INPUT  -s ${NODE1_IP} -p tcp --dport 4567 -j DROP
sudo iptables -I OUTPUT -d ${NODE1_IP} -p tcp --dport 4567 -j DROP
sudo iptables -I INPUT  -s ${NODE1_IP} -p udp --dport 4567 -j DROP
sudo iptables -I OUTPUT -d ${NODE1_IP} -p udp --dport 4567 -j DROP
```

#### 24.2 — Observe Galera degradation while Corosync stays healthy

```bash
# Pacemaker cluster is unaffected — all 3 nodes still online
sudo crm_mon -1Ar | grep "Online:"
```
```
  * Online: [ node1 node2 node3 ]
```

```bash
# But Galera flow control activates on node3
sudo mysql -u root -p -h ${NODE3_IP} -e \
  "SHOW STATUS WHERE Variable_name IN
   ('wsrep_cluster_size','wsrep_local_state_comment','wsrep_flow_control_paused');"
```
```
+---------------------------+--------+
| wsrep_cluster_size        | 3      |   ← still 3 — routing through node2
| wsrep_local_state_comment | Synced |
| wsrep_flow_control_paused | 0.418  |   ← 42% pause — replication going via node2 hop
+---------------------------+--------+
```

Galera routes writes through node2 as an intermediary — degraded but functional.

#### 24.3 — Observe write latency increase

```bash
# Time a write through the HAProxy VIP
time mysql -u ${MARIADB_USER} -p"${MARIADB_PASSWORD}" -h ${FLOWFIRST_VIP} ${MARIADB_DB} \
  -e "INSERT INTO processed_messages (message_id,flow_id,item_id,initial_data,history_trail,raw_payload)
      VALUES (UUID(),0,0,'glitch_test','[]','{}');"
```
```
real    0m0.847s     ← normally ~10ms; elevated due to Galera flow control
```

#### 24.4 — Cleanup

```bash
# On node1
for proto in tcp udp; do
  sudo iptables -D INPUT  -s ${NODE3_IP} -p ${proto} --dport 4567 -j DROP
  sudo iptables -D OUTPUT -d ${NODE3_IP} -p ${proto} --dport 4567 -j DROP
done

# On node3
for proto in tcp udp; do
  sudo iptables -D INPUT  -s ${NODE1_IP} -p ${proto} --dport 4567 -j DROP
  sudo iptables -D OUTPUT -d ${NODE1_IP} -p ${proto} --dport 4567 -j DROP
done

sudo crm_resource -C
```

---

### Scenario 25: Selective Port-Level Block — RabbitMQ AMQP Only

> **Runs on:** `iptables` on **node2**; logs, `rabbitmqctl cluster_status` and `list_queues` on **node1 or node3**.

Block AMQP port 5672 on node2 — RabbitMQ stops accepting connections but Corosync and Galera remain intact.

#### 25.1 — Block AMQP traffic to/from node2

```bash
# On node2
sudo iptables -I INPUT  -p tcp --dport 5672 -j DROP
sudo iptables -I OUTPUT -p tcp --sport 5672 -j DROP
```

#### 25.2 — Observe `pika` client reconnection

The pipeline processes use multi-host `ConnectionParameters`. Within `pika`'s heartbeat interval (default 60 s), or immediately on the next connection attempt, they will fail over to `node1` or `node3`.

Check the process logs on the node whose Transformer (Process 2) was connected to node2 (the DROP rule stalls an established connection, so expect this after up to two missed heartbeats, about 2 minutes):
```bash
sudo journalctl -u flowfirst-process2 --since "5 minutes ago" --no-pager | grep -E "raised|Restarting|not ready|RabbitMQ connected|TCP Connection attempt failed"
```
```
ERROR [gt] Greenthread 'p2_flow1_consumer' raised: Stream connection lost: ...
INFO [gt] Restarting 'p2_flow1_consumer' in 2s...
INFO [gt] [p2-flow1] RabbitMQ connected at 192.168.1.101:5672
```
The consumer greenthread restarts itself and `config.get_connection()` offers the broker list in order (`NODE1_IP`, `NODE2_IP`, `NODE3_IP`), so it reconnects to node1 first; the `RabbitMQ connected at` line names the broker it chose. `[gt] [p2-flow1] RabbitMQ not ready (attempt N/20)` appears only when **no** broker answered, and a `pika` line `TCP Connection attempt failed: ... dest=(... '<node2-ip>', 5672)` only when a broker was tried and unreachable (for example when node1 is down).

> These `[gt]` / `[zk]` lines come from Python `logging`, which `gt.configure_logging()` sends to the journal at `LOG_LEVEL` (default `INFO`). `pika` and `kazoo` are held at WARNING. On a node still running units started before that change, those lines are missing; restart the processes. You can also confirm the new broker with `sudo rabbitmqctl list_connections name peer_host | grep <process-2-node-ip>`.

#### 25.3 — Verify RabbitMQ cluster view

```bash
# From node1 — node2 still in cluster config but not accepting AMQP connections
sudo rabbitmqctl cluster_status
```
```
Disk Nodes: rabbit@node1  rabbit@node2  rabbit@node3
Running Nodes: rabbit@node1  rabbit@node2  rabbit@node3
```
> RabbitMQ cluster inter-node communication (port 25672) is separate from AMQP client connections (5672). The RabbitMQ cluster stays intact even though AMQP clients cannot connect to node2.

#### 25.4 — Verify pipeline queues are still being consumed

```bash
sudo rabbitmqctl list_queues name messages consumers
```
```
flow1_p1_to_p2           0    1     ← still 1 consumer (the Transformer (Process 2) leader, now on node1 or node3 if it was on node2)
flow2_p1_to_p2           0    1
flow2_p2_to_p3           0    1
```

#### 25.5 — Cleanup

```bash
# On node2
sudo iptables -D INPUT  -p tcp --dport 5672 -j DROP
sudo iptables -D OUTPUT -p tcp --sport 5672 -j DROP

# Verify every queue still has its single consumer (the leader may stay on its new node)
sleep 10
sudo rabbitmqctl list_queues name messages consumers
```
```
flow1_p1_to_p2           0    1
flow2_p1_to_p2           0    1
flow2_p2_to_p3           0    1
```

---

### Scenario 26: Intermittent Flapping — Periodic Network Glitch

> **Runs on:** the flapping loop on **node3**; the monitor loop on **node1** in a second terminal; failcount checks on any cluster member (node1, node2 or node3).

Simulate a network interface that glitches every 30 seconds — typical of a bad cable or switch port.

#### 26.1 — Start a background glitch loop on node3

```bash
# On node3 — toggle 5% loss on/off every 30 seconds for 5 cycles
IFACE=${VIP_NIC}   # cluster NIC for this node, set in .env (Phase 3)
for i in $(seq 1 5); do
    echo "[$(date)] Injecting packet loss — cycle ${i}"
    sudo tc qdisc add dev ${IFACE} root netem loss 15% delay 200ms 2>/dev/null || \
    sudo tc qdisc change dev ${IFACE} root netem loss 15% delay 200ms

    sleep 30

    echo "[$(date)] Clearing packet loss — cycle ${i}"
    sudo tc qdisc del dev ${IFACE} root 2>/dev/null

    sleep 30
done
echo "Flapping simulation complete"
```

#### 26.2 — Watch Pacemaker react on node1 (run in a separate terminal)

```bash
# Continuous monitor — watch node3 flap
sudo crm_mon -Ar -i 5 2>&1 | grep -E "Online|OFFLINE|Stopped|Started"
```

Expected oscillation:
```
  * Online: [ node1 node2 node3 ]           ← glitch clear
  * Online: [ node1 node2 ] OFFLINE: [node3] ← glitch injected, Pacemaker fences node3
  * Online: [ node1 node2 node3 ]           ← glitch cleared, node3 rejoins
```

#### 26.3 — Check Pacemaker failure counters building up

```bash
sudo pcs resource failcount show flowfirst-p1-res-clone
```
```
Resource Fail Counts:
  flowfirst-p1-res: node3 = 3
```

Once the failcount hits the `migration-threshold`, Pacemaker bans the resource from that node until the count is cleared. The setup scripts never set `migration-threshold`, so it stays at Pacemaker's default of `INFINITY` (shown as `1000000`) and a ban only happens if you set a finite value (`sudo pcs resource meta flowfirst-p1-res-clone migration-threshold=3`). Failures are still counted and visible above:

```bash
# Reset failure counters after flapping stops
sudo crm_resource -r flowfirst-p1-res-clone -C
sudo crm_resource -r flowfirst-p2-res-clone -C
sudo crm_resource -r flowfirst-p3-res-clone -C
sudo crm_resource -r flowfirst-p4-res-clone -C
```

#### 26.4 — Cleanup

```bash
# Ensure netem is cleared
sudo tc qdisc del dev ${IFACE} root 2>/dev/null || true
sudo crm_resource -C
sudo crm_mon -1Ar | grep "Online:"
```
```
  * Online: [ node1 node2 node3 ]
```

---

### Helper Script: `scripts/simulate_network_glitch.sh`

This script automates all glitch injection and cleanup operations described above. Run it on the target node.

```bash
sudo ./scripts/simulate_network_glitch.sh <command> [node_ip] [options]
```

| Command | Description |
|---|---|
| `latency <iface> <delay_ms> <loss_pct>` | Inject netem latency + loss |
| `throttle <iface> <rate_mbit>` | Limit bandwidth with TBF |
| `partition <remote_ip>` | Drop all traffic to a remote IP (both INPUT/OUTPUT) |
| `block-port <remote_ip> <proto> <port>` | Drop specific port traffic |
| `clear-netem <iface>` | Remove all `tc` rules on interface |
| `clear-iptables <remote_ip>` | Remove all DROP rules for a remote IP |
| `clear-all` | Remove all injected faults (tc + iptables) |
| `status` | Show active `tc` rules and `iptables` DROP rules |

---

### Post-Glitch Full Health Validation Checklist

Run this on **node1** (or any other cluster member — all of these commands work on node1–node3) after any glitch scenario to confirm a clean cluster state:

```bash
# 1. Pacemaker — all 3 nodes online, all clones 3/3
sudo crm_mon -1Ar

# 2. No failed actions
sudo pcs status | grep -c "Failed"   # expect 0

# 3. No residual constraints from moves/bans
sudo pcs constraint list --full | grep -v "^Listing"

# 4. Corosync rings fault-free
sudo corosync-cfgtool -s | grep -v "no faults" | grep "status"
# expect all rings to show "ring N active with no faults"

# 5. Galera — all 3 nodes synced
sudo mysql -u root -p -e \
  "SELECT VARIABLE_NAME, VARIABLE_VALUE FROM information_schema.GLOBAL_STATUS
   WHERE VARIABLE_NAME IN
   ('WSREP_CLUSTER_SIZE','WSREP_LOCAL_STATE_COMMENT','WSREP_FLOW_CONTROL_PAUSED');"
# expect: cluster_size=3, state=Synced, flow_control_paused=0.000

# 6. RabbitMQ — all 3 running, queues empty
sudo rabbitmqctl cluster_status | grep "Running Nodes"
sudo rabbitmqctl list_queues name messages consumers

# 7. End-to-end pipeline smoke test
curl -s -X POST http://${FLOWFIRST_VIP}:8080/api/flow1 | jq .
curl -s -X POST http://${FLOWFIRST_VIP}:8080/api/flow2 | jq .
sleep 5
mysql -u ${MARIADB_USER} -p"${MARIADB_PASSWORD}" -h ${FLOWFIRST_VIP} ${MARIADB_DB} \
  -e "SELECT id, flow_id, item_id FROM processed_messages ORDER BY id DESC LIMIT 4;"
```

---

## Network Round-Trip Verification with `tcpdump`

These scenarios capture live packets on the cluster interfaces while executing real pipeline use-cases, then analyse the captures to confirm that **every network protocol completes its full handshake and round-trip** — AMQP, Corosync, Galera wsrep, MariaDB, and REST/HTTP over HAProxy.

> **Note on sample output:** the illustrative packet traces in this section show IP addresses in their raw form as they appear in `tcpdump`/`tshark` output — for example `${NODE1_IP}` appears as a real IP like `10.0.0.1`. These are **example outputs only** — your actual captures will show the IPs set in your `.env` (`${NODE1_IP}`, `${NODE2_IP}`, `${NODE3_IP}`, `${FLOWFIRST_VIP}`). All runnable commands use env vars.

> **Prerequisites on every node:**
> ```bash
> sudo dnf install -y tcpdump wireshark-cli   # wireshark-cli provides tshark
> ```

A helper script [`scripts/tcpdump_flows.sh`](scripts/tcpdump_flows.sh) automates capture startup, use-case execution, and round-trip analysis for all protocols.

---

### tcpdump Protocol & Port Reference

| Protocol | Transport | Port(s) | What to look for |
|---|---|---|---|
| AMQP (RabbitMQ) | TCP | 5672 | `SYN→SYN-ACK→ACK` connect; `AMQP0-9-1` frame headers; `Basic.Publish`, `Basic.Deliver`, `Basic.Ack` |
| RabbitMQ cluster bus | TCP | 25672 | Erlang distribution handshake; `send_challenge` / `send_challenge_reply` |
| Corosync heartbeat | UDP | 5405 | Periodic token-ring messages; `TOTEM` token and join messages |
| Galera wsrep replication | TCP | 4567 (tcp+udp) | `SYN→SYN-ACK→ACK`; wsrep writesets; `ist.send_sync` |
| Galera IST | TCP | 4568 | IST donor→joiner stream |
| Galera SST (xtrabackup) | TCP | 4444 | SST donor stream |
| MariaDB client | TCP | 3306 | MySQL greeting packet; `COM_QUERY`; result set; `EOF` packet |
| REST API / HAProxy | TCP | 8080 | `HTTP/1.1 POST` request; `200 OK` response; `Content-Length` match |
| HAProxy stats | TCP | 9000 | `HTTP/1.1 GET /stats`; `200 OK` |

---

### Scenario 27: Capture Setup — Identify Interfaces and Start Captures

> **Runs on:** **each of node1–node3**, in its own terminal (each capture records only the local NIC).

#### 27.1 — Identify the cluster-facing network interface on each node

```bash
# Run on each node — find the interface on the cluster subnet
# Filters by the node's own IP (NODE1_IP / NODE2_IP / NODE3_IP from .env)
ip -o addr show | awk -v ip="${CURRENT_NODE_IP}" '$0 ~ ip {print $2, $4}'
# Or list all non-loopback interfaces and their IPs:
ip -o addr show | awk '$3 == "inet" && $2 != "lo" {print $2, $4}'
```
```
eth0 ${NODE1_IP}/24    # node1 — example output (IP from .env)
eth0 ${NODE2_IP}/24    # node2
eth0 ${NODE3_IP}/24    # node3
```

Set a shell variable for all subsequent commands:
```bash
IFACE=${VIP_NIC}   # cluster NIC for this node, set in .env (Phase 3)
```

#### 27.2 — Start background captures on all three nodes simultaneously

Open **three SSH sessions** (one per node) and run on each:

```bash
# On node1 — capture all cluster-relevant ports, write to timestamped pcap
IFACE=${VIP_NIC}   # cluster NIC for this node, set in .env (Phase 3)
PCAP_DIR=/var/log/flowfirst/pcap
sudo mkdir -p ${PCAP_DIR}
sudo tcpdump -i ${IFACE} -s 0 -w ${PCAP_DIR}/node1_$(date +%Y%m%d_%H%M%S).pcap \
    'port 5672 or port 25672 or port 5405 or port 4567 or port 4568 or port 4444 or port 3306 or port 8080 or port 9000' \
    -Z root &
TCPDUMP_PID=$!
echo "tcpdump PID: ${TCPDUMP_PID}"
```

```bash
# On node2
IFACE=${VIP_NIC}   # cluster NIC for this node, set in .env (Phase 3)
PCAP_DIR=/var/log/flowfirst/pcap
sudo mkdir -p ${PCAP_DIR}
sudo tcpdump -i ${IFACE} -s 0 -w ${PCAP_DIR}/node2_$(date +%Y%m%d_%H%M%S).pcap \
    'port 5672 or port 25672 or port 5405 or port 4567 or port 4568 or port 4444 or port 3306 or port 8080 or port 9000' \
    -Z root &
TCPDUMP_PID=$!
```

```bash
# On node3
IFACE=${VIP_NIC}   # cluster NIC for this node, set in .env (Phase 3)
PCAP_DIR=/var/log/flowfirst/pcap
sudo mkdir -p ${PCAP_DIR}
sudo tcpdump -i ${IFACE} -s 0 -w ${PCAP_DIR}/node3_$(date +%Y%m%d_%H%M%S).pcap \
    'port 5672 or port 25672 or port 5405 or port 4567 or port 4568 or port 4444 or port 3306 or port 8080 or port 9000' \
    -Z root &
TCPDUMP_PID=$!
```

#### 27.3 — Verify captures are running

```bash
sudo tcpdump -D    # list interfaces
ls -lh ${PCAP_DIR}/   # confirm pcap files are growing
```
```
-rw-r--r-- 1 root root 48K Jun  9 12:00 node1_20250609_120000.pcap
```

---

### Scenario 28: AMQP Round-Trip — Flow1 & Flow2 Pipeline Execution

> **Runs on:** `curl` from any machine that can reach the VIP (node1–node4 or a workstation); stop each capture and analyse its pcap on the **node that captured it** (`$TCPDUMP_PID` exists only in that shell).

With captures running from Scenario 27, trigger both pipeline flows and then examine the AMQP handshake and message delivery round-trips.

#### 28.1 — Execute both flows via the VIP

```bash
# From any workstation or cluster node
curl -s -X POST http://${FLOWFIRST_VIP}:8080/api/flow1 | jq .
curl -s -X POST http://${FLOWFIRST_VIP}:8080/api/flow2 | jq .
sleep 10    # allow full pipeline traversal
```

#### 28.2 — Stop captures and examine AMQP traffic

```bash
sudo kill ${TCPDUMP_PID}    # stop capture on each node
PCAP=$(ls -t /var/log/flowfirst/pcap/node1_*.pcap | head -1)
```

#### 28.3 — Verify TCP three-way handshake for AMQP connections

```bash
sudo tcpdump -r ${PCAP} -nn 'port 5672' | grep -E "Flags \[S\]|Flags \[S\.\]|Flags \[\.\]" | head -20
```
```
12:00:01.123456 IP 192.168.1.101.52341 > 192.168.1.102.5672: Flags [S],  seq 1234567, ...
12:00:01.124001 IP 192.168.1.102.5672  > 192.168.1.101.52341: Flags [S.], seq 9876543, ack 1234568, ...
12:00:01.124050 IP 192.168.1.101.52341 > 192.168.1.102.5672: Flags [.],  ack 9876544, ...
```

> ✅ **SYN → SYN-ACK → ACK** sequence confirms a successful TCP three-way handshake. Every AMQP connection must show this triplet.

#### 28.4 — Verify AMQP 0-9-1 protocol banner

```bash
sudo tcpdump -r ${PCAP} -nn -A 'port 5672' | grep -E "AMQP|amqp" | head -10
```
```
12:00:01.124100 ... "AMQP" 0 0 9 1    ← AMQP 0-9-1 protocol header sent by client
```

#### 28.5 — Examine AMQP frame sequence with `tshark`

```bash
sudo tshark -r ${PCAP} -d tcp.port==5672,amqp \
    -Y 'amqp' \
    -T fields \
    -e frame.time_relative \
    -e ip.src \
    -e ip.dst \
    -e amqp.method.method \
    2>/dev/null | head -40
```
```
0.000000    192.168.1.101  192.168.1.102  connection.start
0.000120    192.168.1.102  192.168.1.101  connection.start-ok
0.000240    192.168.1.102  192.168.1.101  connection.tune
0.000310    192.168.1.101  192.168.1.102  connection.tune-ok
0.000400    192.168.1.101  192.168.1.102  connection.open
0.000480    192.168.1.102  192.168.1.101  connection.open-ok
0.000550    192.168.1.101  192.168.1.102  channel.open
0.000620    192.168.1.102  192.168.1.101  channel.open-ok
0.000700    192.168.1.101  192.168.1.102  basic.publish        ← message sent to flow1_queue
0.000780    192.168.1.102  192.168.1.101  basic.deliver        ← process2 receives it
0.000850    192.168.1.101  192.168.1.102  basic.ack            ← process2 acknowledges
0.001200    192.168.1.101  192.168.1.102  basic.publish        ← process2 re-publishes modified
0.001290    192.168.1.102  192.168.1.101  basic.deliver        ← process3 receives it
0.001360    192.168.1.101  192.168.1.102  basic.ack            ← process3 acknowledges
```

> ✅ **Full AMQP round-trip confirmed**: `connection.start` → `open-ok` → `basic.publish` → `basic.deliver` → `basic.ack` for each hop in the pipeline.

#### 28.6 — Confirm message payload mutation across hops

```bash
# Extract AMQP payload content (ASCII printable)
sudo tcpdump -r ${PCAP} -nn -A 'port 5672' \
    | grep -A2 "basic.publish" \
    | grep -oP '"payload":\{.*?\}' \
    | head -10
```

Look for the `hop_count` field incrementing and `modified_by` being updated at each process — confirming the data mutation design is visible at the wire level.

#### 28.7 — Verify clean TCP connection teardown (FIN-ACK)

```bash
sudo tcpdump -r ${PCAP} -nn 'port 5672' | grep "Flags \[F" | head -10
```
```
12:00:11.450001 IP 192.168.1.101.52341 > 192.168.1.102.5672: Flags [F.], seq ..., ack ...
12:00:11.450200 IP 192.168.1.102.5672  > 192.168.1.101.52341: Flags [F.], seq ..., ack ...
```

> ✅ Graceful `FIN → FIN-ACK` teardown — no abrupt `RST` terminations.

---

### Scenario 29: REST API / HAProxy Round-Trip Verification

> **Runs on:** the **current VIP holder** (`sudo crm_resource --resource flowfirst-vip --locate`), since only it carries VIP traffic.

#### 29.1 — Capture REST API traffic while sending requests

```bash
# On the node currently holding the VIP (e.g., node1)
# Capture both hops: client -> HAProxy (8080) and HAProxy -> API Gateway (Process 1) backend (8082).
# "-i any" is needed because HAProxy reaches this node's own backend over loopback,
# which a capture on ${VIP_NIC} alone would not see.
sudo tcpdump -i any -s 0 -w /tmp/rest_api.pcap \
    'tcp port 8080 or tcp port 8082' &
TCPDUMP_PID=$!

# Send 3 requests to generate traffic
for i in 1 2 3; do
  curl -s -X POST http://${FLOWFIRST_VIP}:8080/api/flow1 | jq .handled_by_node
  sleep 1
done

sudo kill ${TCPDUMP_PID}
```

#### 29.2 — Verify HTTP request/response round-trip

```bash
sudo tcpdump -r /tmp/rest_api.pcap -nn -A 'tcp port 8080' \
    | grep -E "POST|HTTP/1|Content-Length|handled_by" | head -20     # client <-> HAProxy hop
```
```
12:00:05.001000 ... POST /api/flow1 HTTP/1.1
12:00:05.001100 ... Content-Type: application/json
12:00:05.002500 ... HTTP/1.0 200 OK
12:00:05.002600 ... Content-Length: 87
12:00:05.002700 ... {"status":"queued","flow":"flow1","handled_by_node":"node1"}
```

> ✅ HTTP `POST` request and `200 OK` response confirm REST API round-trip.

#### 29.3 — Confirm HAProxy load-balancing across 3 nodes

Send 9 requests (3 per node via round-robin) and capture the `handled_by_node` field. Run this on the VIP holder with a fresh capture, because the 29.1 file holds only its own three requests:
```bash
sudo tcpdump -i any -s 0 -w /tmp/rest_api_rr.pcap 'tcp dst port 8082' &
TCPDUMP_PID=$!
sleep 1
for i in $(seq 1 9); do
  curl -s -X POST http://${FLOWFIRST_VIP}:8080/api/flow1 | jq -r .handled_by_node
done
sleep 1; sudo kill ${TCPDUMP_PID}
```
```
node1
node2
node3
node1
node2
node3
node1
node2
node3
```

Confirm in the pcap that the destination backend IPs rotate. Only the first packet of each connection (SYN without ACK) is counted, so the numbers are connections, not packets (`option http-server-close` gives one backend connection per request):
```bash
sudo tcpdump -r /tmp/rest_api_rr.pcap -nn \
    'tcp dst port 8082 and tcp[tcpflags] & tcp-syn != 0 and tcp[tcpflags] & tcp-ack == 0' \
    | awk '{print $5}' | sort | uniq -c | sort -rn
```
```
   3 192.168.1.101.8082:
   3 192.168.1.102.8082:
   3 192.168.1.103.8082:
```

> ✅ Even distribution confirms HAProxy round-robin is functioning.

#### 29.4 — Verify TCP SYN-SYN/ACK-ACK for every HTTP connection

```bash
sudo tshark -r /tmp/rest_api_rr.pcap \
    -Y 'tcp.flags.syn==1 && tcp.port==8082' \
    -T fields -e frame.time_relative -e ip.src -e ip.dst -e tcp.flags \
    2>/dev/null
```
```
0.000000  192.168.1.100  192.168.1.101  0x0002   ← SYN (HAProxy→backend; the source address is whatever HAProxy connects from)
0.000080  192.168.1.101  192.168.1.100  0x0012   ← SYN-ACK
1.001000  192.168.1.100  192.168.1.102  0x0002   ← SYN (next round-robin)
1.001070  192.168.1.102  192.168.1.100  0x0012   ← SYN-ACK
```

---

### Scenario 30: Corosync Heartbeat Round-Trip Verification

> **Runs on:** **node1** (capture and analysis).

#### 30.1 — Capture Corosync UDP heartbeats

```bash
# On node1 — capture only Corosync UDP 5405
sudo tcpdump -i ${IFACE} -s 0 -w /tmp/corosync.pcap \
    'udp port 5405' &
TCPDUMP_PID=$!

# Let it run for 30 seconds to capture several token rotations
sleep 30
sudo kill ${TCPDUMP_PID}
```

#### 30.2 — Count token messages per source node

```bash
sudo tcpdump -r /tmp/corosync.pcap -nn 'udp port 5405' \
    | awk '{print $3}' | cut -d. -f1-4 | sort | uniq -c | sort -rn
```
```
 148 192.168.1.101    ← node1 sent 148 Corosync messages
 147 192.168.1.102    ← node2 sent 147
 149 192.168.1.103    ← node3 sent 149
```

> ✅ Roughly equal message counts from all 3 nodes confirms active token-ring participation by every node.

#### 30.3 — Verify bidirectional flow between every node pair

```bash
sudo tcpdump -r /tmp/corosync.pcap -nn 'udp port 5405' \
    | awk '{print $3, "->", $5}' \
    | sed 's/\.[0-9]*://g' \
    | sort -u | head -12
```
```
192.168.1.101 -> 192.168.1.102    ← node1→node2
192.168.1.101 -> 192.168.1.103    ← node1→node3
192.168.1.102 -> 192.168.1.101    ← node2→node1
192.168.1.102 -> 192.168.1.103    ← node2→node3
192.168.1.103 -> 192.168.1.101    ← node3→node1
192.168.1.103 -> 192.168.1.102    ← node3→node2
```

> ✅ All 6 directed pairs present — every node is communicating with every other node in the ring.

#### 30.4 — Measure Corosync token inter-arrival time (heartbeat interval)

```bash
sudo tcpdump -r /tmp/corosync.pcap -nn -tt "udp port 5405 and src ${NODE1_IP}" \
    | awk '{print $1}' \
    | awk 'NR>1{printf "%.3fms\n", ($1-prev)*1000} {prev=$1}' \
    | sort -n | awk 'BEGIN{n=0;s=0} {n++;s+=$1} END{printf "avg=%.1fms  n=%d\n",s/n,n}'
```
```
avg=999.8ms  n=147     ← token circulates approximately every 1 second (Corosync default)
```

> ✅ Token interval close to 1000 ms matches Corosync's default `token` parameter.

---

### Scenario 31: Galera wsrep Replication Round-Trip Verification

> **Runs on:** **node1** (capture and analysis); the write traffic can come from any node.

#### 31.1 — Capture Galera replication traffic during a DB write

```bash
# On node1 — capture Galera ports, plus MariaDB client port 3306 (used by 31.4)
sudo tcpdump -i ${IFACE} -s 0 -w /tmp/galera.pcap \
    'tcp port 4567 or tcp port 4568 or tcp port 4444 or tcp port 3306' &
TCPDUMP_PID=$!

# Trigger a write through the pipeline (process4 will INSERT into MariaDB)
curl -s -X POST http://${FLOWFIRST_VIP}:8080/api/flow1 | jq .
curl -s -X POST http://${FLOWFIRST_VIP}:8080/api/flow2 | jq .
sleep 5

sudo kill ${TCPDUMP_PID}
```

#### 31.2 — Verify Galera TCP connections established on port 4567

```bash
sudo tcpdump -r /tmp/galera.pcap -nn 'tcp port 4567 and tcp[tcpflags] & tcp-syn != 0' \
    | grep -E "Flags \[S\b"
```
```
12:00:10.100001 IP 192.168.1.101.43210 > 192.168.1.102.4567: Flags [S],  ...
12:00:10.100150 IP 192.168.1.102.4567  > 192.168.1.101.43210: Flags [S.], ...
12:00:10.100200 IP 192.168.1.101.43210 > 192.168.1.102.4567: Flags [.],  ...
```

> ✅ TCP handshake on Galera wsrep port 4567.

#### 31.3 — Count Galera writeset packets per node pair

```bash
sudo tcpdump -r /tmp/galera.pcap -nn 'tcp port 4567 and not tcp[tcpflags] & tcp-syn != 0' \
    | awk '{print $3, "->", $5}' \
    | sed 's/\.[0-9]*://g' \
    | sort | uniq -c | sort -rn | head -10
```
```
  42 192.168.1.101 -> 192.168.1.102    ← writesets from node1 to node2
  41 192.168.1.101 -> 192.168.1.103    ← writesets from node1 to node3
  41 192.168.1.102 -> 192.168.1.101
  40 192.168.1.102 -> 192.168.1.103
  42 192.168.1.103 -> 192.168.1.101
  41 192.168.1.103 -> 192.168.1.102
```

> ✅ Bidirectional writeset replication between all node pairs confirms Galera multi-master is active.

#### 31.4 — Correlate Galera replication with MariaDB query volume

```bash
# Count MariaDB client segments captured alongside Galera replication
# (port 3306 is included in the 31.1 capture filter; without it this count is 0)
sudo tcpdump -r /tmp/galera.pcap -nn 'tcp port 3306' | wc -l
```
```
24     ← 24 TCP segments on port 3306 (query + result + EOF packets)
```

> The number depends on which node DB Updater (Process 4) writes to. Only traffic that crosses node1's interface is captured, so a leader on node2 or node3 that writes to its own local MariaDB shows `0` here; the Galera replication packets in 31.2–31.3 still appear.

#### 31.5 — Examine MariaDB protocol greeting and query round-trip

```bash
# Run a separate short capture focused on port 3306
sudo tcpdump -i ${IFACE} -s 0 -w /tmp/mysql.pcap 'tcp port 3306' &
TCPDUMP_PID=$!

mysql -u ${MARIADB_USER} -p"${MARIADB_PASSWORD}" -h ${FLOWFIRST_VIP} ${MARIADB_DB} \
  -e "SELECT COUNT(*) FROM processed_messages;"

sudo kill ${TCPDUMP_PID}
```

```bash
sudo tcpdump -r /tmp/mysql.pcap -nn -A 'tcp port 3306' \
    | grep -E "mysql|MariaDB|COM_QUERY|SELECT|COUNT|localhost" | head -20
```
```
12:00:20.001000 ... server version: 10.11.x-MariaDB   ← greeting packet
12:00:20.002000 ... COM_QUERY: SELECT COUNT(*) ...     ← client sends query
12:00:20.003000 ... result: 8                          ← server returns result
```

Or use `tshark` for structured output:
```bash
sudo tshark -r /tmp/mysql.pcap -d tcp.port==3306,mysql \
    -Y 'mysql' \
    -T fields \
    -e frame.time_relative \
    -e ip.src \
    -e mysql.query \
    -e mysql.num_fields \
    2>/dev/null | grep -v "^$"
```
```
0.000000  192.168.1.100  (server greeting, version 10.11)
0.001200  192.168.1.101  SELECT COUNT(*) FROM processed_messages
0.002100  192.168.1.100  (result: 1 field, 1 row)
```

> ✅ `COM_QUERY` → result set → `EOF` packet confirms full MariaDB round-trip.

---

### Scenario 32: Network Round-Trip During Glitch — Before/During/After Comparison

> **Runs on:** captures and analysis on **node1**; `tc` impairment on **node2**.

This scenario combines Scenario 20 (latency injection) with live tcpdump to measure the actual impact of a network glitch on every protocol's round-trip time (RTT).

#### 32.1 — Establish baseline RTTs (clean network)

```bash
# On node1 — capture all protocols for 30 seconds baseline
sudo tcpdump -i ${IFACE} -s 0 -w /tmp/baseline.pcap \
    'port 5672 or port 5405 or port 4567 or port 3306 or port 8080' &
TCPDUMP_PID=$!

curl -s -X POST http://${FLOWFIRST_VIP}:8080/api/flow1 | jq .
curl -s -X POST http://${FLOWFIRST_VIP}:8080/api/flow2 | jq .
sleep 20

sudo kill ${TCPDUMP_PID}

echo "=== Baseline AMQP SYN→SYN-ACK RTT ==="
sudo tshark -r /tmp/baseline.pcap -Y 'tcp.analysis.initial_rtt and tcp.port==5672' \
    -T fields -e tcp.analysis.initial_rtt 2>/dev/null | \
    awk '{sum+=$1;n++} END{printf "avg RTT: %.3fms (n=%d)\n", sum*1000/n, n}'
```
```
avg RTT: 0.312ms (n=6)     ← sub-millisecond baseline
```

#### 32.2 — Inject latency on node2 and capture during glitch

```bash
# On node2 — inject 200ms delay
sudo tc qdisc add dev ${IFACE} root netem delay 200ms 20ms

# On node1 — capture during glitch
sudo tcpdump -i ${IFACE} -s 0 -w /tmp/glitch.pcap \
    'port 5672 or port 5405 or port 4567 or port 3306 or port 8080' &
TCPDUMP_PID=$!

curl -s -X POST http://${FLOWFIRST_VIP}:8080/api/flow1 | jq .
sleep 20

sudo kill ${TCPDUMP_PID}
```

#### 32.3 — Compare RTTs baseline vs. glitch

```bash
# AMQP RTT during glitch
echo "=== Glitch AMQP SYN→SYN-ACK RTT ==="
sudo tshark -r /tmp/glitch.pcap -Y 'tcp.analysis.initial_rtt and tcp.port==5672' \
    -T fields -e tcp.analysis.initial_rtt 2>/dev/null | \
    awk '{sum+=$1;n++} END{printf "avg RTT: %.3fms (n=%d)\n", sum*1000/n, n}'
```
```
avg RTT: 201.4ms (n=4)     ← 200ms injected delay is clearly visible
```

```bash
# Corosync token timing during glitch — compare with Scenario 30.4 baseline
echo "=== Glitch Corosync token inter-arrival ==="
sudo tcpdump -r /tmp/glitch.pcap -nn -tt "udp port 5405 and src ${NODE2_IP}" \
    | awk '{print $1}' \
    | awk 'NR>1{printf "%.1fms\n",($1-prev)*1000}{prev=$1}' \
    | sort -n | tail -5
```
```
1198.2ms     ← tokens from node2 arriving ~200ms late
1203.5ms
```

> ✅ `tcpdump` directly measures the 200 ms injected delay in both AMQP and Corosync protocols.

#### 32.4 — Cleanup and verify RTT returns to baseline

```bash
# On node2 — remove latency injection
sudo tc qdisc del dev ${IFACE} root

# Re-run baseline measurement
sudo tcpdump -i ${IFACE} -s 0 -w /tmp/post_glitch.pcap \
    'port 5672 or port 8080' &
TCPDUMP_PID=$!

curl -s -X POST http://${FLOWFIRST_VIP}:8080/api/flow1 | jq .
sleep 10

sudo kill ${TCPDUMP_PID}

sudo tshark -r /tmp/post_glitch.pcap -Y 'tcp.analysis.initial_rtt and tcp.port==5672' \
    -T fields -e tcp.analysis.initial_rtt 2>/dev/null | \
    awk '{sum+=$1;n++} END{printf "avg RTT: %.3fms (n=%d)\n", sum*1000/n, n}'
```
```
avg RTT: 0.308ms (n=6)     ← back to sub-millisecond baseline
```

---

### Scenario 33: tcpdump-Based Full Pipeline Flow Trace

> **Runs on:** captures on **each of node1–node3**; copy the pcaps to one node before running the cross-node loop (it reads only local files); 33.5 on **node3**, as shown.

This scenario captures a single Flow 1 message from HTTP POST all the way to MariaDB INSERT and traces every network hop in the pcap files.

#### 33.1 — Start captures on all 3 nodes simultaneously

```bash
# Run this on node1, node2, node3 in parallel (3 SSH sessions)
NODE=$(hostname)
IFACE=${VIP_NIC}   # cluster NIC for this node, set in .env (Phase 3)
PCAP_DIR=/var/log/flowfirst/pcap
sudo mkdir -p ${PCAP_DIR}
sudo tcpdump -i ${IFACE} -s 0 \
    -w ${PCAP_DIR}/${NODE}_flow_trace_$(date +%H%M%S).pcap \
    'port 5672 or port 3306 or port 8080' &
echo "Capture started on ${NODE}, PID=$!"
```

#### 33.2 — Trigger one Flow 1 message

```bash
# From node1 or workstation
MSG_ID=$(curl -s -X POST http://${FLOWFIRST_VIP}:8080/api/flow1 | jq -r .message_id)
echo "Tracking message: ${MSG_ID}"
sleep 8    # allow full pipeline: P1→P2→P3→P4→DB
```

#### 33.3 — Stop captures and locate the message in pcaps

```bash
# Stop on all nodes
sudo pkill -f "tcpdump.*flow_trace" 2>/dev/null || true

# On each node — search pcap for the message_id in AMQP payload
for PCAP in /var/log/flowfirst/pcap/*flow_trace*.pcap; do
    echo "=== ${PCAP} ==="
    sudo tcpdump -r ${PCAP} -nn -A 'port 5672' 2>/dev/null \
        | grep -c "${MSG_ID}" || true
done
```
```
=== node1_flow_trace_120010.pcap ===
4     ← message seen 4 times on node1 (publish flow1_queue + flow1_reflected_queue)
=== node2_flow_trace_120010.pcap ===
2     ← message transited node2's AMQP broker
=== node3_flow_trace_120010.pcap ===
2     ← final delivery to process4 and DB write
```

#### 33.4 — Reconstruct the full hop timeline

```bash
# On each node — extract timestamps for this message_id
for PCAP in /var/log/flowfirst/pcap/*flow_trace*.pcap; do
    NODE=$(basename ${PCAP} | cut -d_ -f1)
    sudo tcpdump -r ${PCAP} -nn -tt -A 'port 5672' 2>/dev/null \
        | grep -B1 "${MSG_ID}" \
        | grep "^[0-9]" \
        | awk -v node="${NODE}" '{printf "%s  %s  AMQP hop\n", $1, node}'
done | sort -n
```
```
1749470410.123456  node1  AMQP hop    ← process1 publishes to flow1_queue
1749470410.124800  node2  AMQP hop    ← process2 receives, modifies, republishes
1749470410.126200  node1  AMQP hop    ← process3 receives on flow1_reflected_queue
1749470410.127100  node3  AMQP hop    ← process4 receives on final queue
1749470410.128500  node3  AMQP hop    ← DB INSERT via MariaDB (port 3306)
```

> ✅ The full pipeline message journey — from REST POST to DB INSERT — is traced at the packet level with sub-millisecond timestamps.

#### 33.5 — Verify DB INSERT round-trip in the pcap

```bash
PCAP=$(ls /var/log/flowfirst/pcap/node3_flow_trace*.pcap | head -1)

# Extract MySQL COM_QUERY and OK response
sudo tcpdump -r ${PCAP} -nn -A 'tcp port 3306' 2>/dev/null \
    | grep -E "INSERT|affected|last_insert_id" | head -5
```
```
12:00:10.128500 ... INSERT INTO processed_messages ...
12:00:10.129100 ... affected_rows=1, last_insert_id=42
```

> ✅ `INSERT` + `affected_rows=1` confirms the DB write completed successfully.

---

### Helper Script: `scripts/tcpdump_flows.sh`

This script automates capture startup, flow execution, and round-trip analysis for all protocols.

```bash
# Start captures on the current node
sudo ./scripts/tcpdump_flows.sh start [iface]

# Run all use-cases and wait for pipeline traversal
sudo ./scripts/tcpdump_flows.sh run-flows [vip_ip]

# Stop captures
sudo ./scripts/tcpdump_flows.sh stop

# Analyse captured pcap files — print round-trip summaries for all protocols
sudo ./scripts/tcpdump_flows.sh analyse [pcap_file]

# Full automated sequence: start + run-flows + stop + analyse
sudo ./scripts/tcpdump_flows.sh all [iface] [vip_ip]
```

---

### Post-Capture Round-Trip Verification Checklist

After running any capture scenario, use this checklist to confirm complete network round-trips for every protocol. Run it on the **node that holds the pcap** (the example uses node1's capture):

```bash
PCAP=/var/log/flowfirst/pcap/node1_*.pcap   # adjust to your latest file

# ── 1. AMQP TCP three-way handshakes ─────────────────────────────────────────
echo "=== 1. AMQP SYN/SYN-ACK/ACK triplets ==="
sudo tcpdump -r ${PCAP} -nn 'port 5672 and tcp[tcpflags] & tcp-syn != 0' \
    | grep -c "Flags \[S" || true
# expect: pairs of SYN + SYN-ACK entries (each connection shows 2)

# ── 2. AMQP full method sequence ──────────────────────────────────────────────
echo "=== 2. AMQP connection.open-ok present ==="
sudo tshark -r ${PCAP} -d tcp.port==5672,amqp \
    -Y 'amqp.method.method == "connection.open-ok"' \
    -T fields -e frame.number 2>/dev/null | wc -l
# expect: >= 1 per pipeline process

# ── 3. AMQP basic.publish and basic.ack pairs ────────────────────────────────
echo "=== 3. AMQP publish→ack pairs ==="
PUBLISHES=$(sudo tshark -r ${PCAP} -d tcp.port==5672,amqp \
    -Y 'amqp.method.method == "basic.publish"' \
    -T fields -e frame.number 2>/dev/null | wc -l)
ACKS=$(sudo tshark -r ${PCAP} -d tcp.port==5672,amqp \
    -Y 'amqp.method.method == "basic.ack"' \
    -T fields -e frame.number 2>/dev/null | wc -l)
echo "  publishes=${PUBLISHES}  acks=${ACKS}"
# expect: acks >= publishes (acks include publisher confirms)

# ── 4. Corosync bidirectional coverage ───────────────────────────────────────
echo "=== 4. Corosync all-pairs coverage ==="
sudo tcpdump -r ${PCAP} -nn 'udp port 5405' \
    | awk '{print $3, $5}' | sed 's/\.[0-9]*://g' | sort -u
# expect: all 6 directed pairs (node1↔node2, node1↔node3, node2↔node3)

# ── 5. Galera TCP SYN→SYN-ACK on port 4567 ──────────────────────────────────
echo "=== 5. Galera replication connections ==="
sudo tcpdump -r ${PCAP} -nn 'tcp port 4567 and tcp[tcpflags] & tcp-syn != 0' \
    | grep -c "Flags \[S\b" || true
# expect: >= 2 (one per peer connection)

# ── 6. MariaDB greeting + query + result ────────────────────────────────────
echo "=== 6. MariaDB COM_QUERY round-trips ==="
sudo tshark -r ${PCAP} -d tcp.port==3306,mysql \
    -Y 'mysql.command == 3' \
    -T fields -e mysql.query 2>/dev/null | head -5
# expect: SELECT or INSERT statements visible

# ── 7. HTTP POST 200 OK pairs ────────────────────────────────────────────────
echo "=== 7. HTTP POST→200 OK round-trips ==="
POSTS=$(sudo tcpdump -r ${PCAP} -nn -A 'tcp port 8080' 2>/dev/null \
    | grep -c "POST /api/" || true)
OKS=$(sudo tcpdump -r ${PCAP} -nn -A 'tcp port 8080' 2>/dev/null \
    | grep -c "200 OK" || true)
echo "  POST_requests=${POSTS}  200_OK_responses=${OKS}"
# expect: OKs == POSTs

# ── 8. No RST (abrupt termination) on AMQP ──────────────────────────────────
echo "=== 8. AMQP RST check (expect 0) ==="
sudo tcpdump -r ${PCAP} -nn 'port 5672 and tcp[tcpflags] & tcp-rst != 0' \
    | wc -l
# expect: 0 — any RST indicates abnormal connection termination

# ── 9. No retransmissions on Galera ─────────────────────────────────────────
echo "=== 9. Galera TCP retransmissions ==="
sudo tshark -r ${PCAP} -Y 'tcp.analysis.retransmission and tcp.port==4567' \
    -T fields -e frame.number 2>/dev/null | wc -l
# expect: 0 on a healthy network; > 0 during glitch scenarios
```

---

## Cluster & Network Operations Quick Reference

| Goal | Command |
|---|---|
| Full one-shot cluster status | `sudo crm_mon -1Ar` |
| Live continuous monitor | `sudo crm_mon -Ar` |
| Clear all failed-action history | `sudo crm_resource -C` |
| Clear failures for one resource | `sudo crm_resource -r <resource-id> -C` |
| Show resource fail counts | `sudo pcs resource failcount show <resource-id>` |
| Locate where a resource runs | `sudo crm_resource --resource <id> --locate` |
| Force resource to a node | `sudo crm_resource --resource <id> --move --node <node>` |
| Remove forced constraint | `sudo crm_resource --resource <id> --clear` |
| Disable a resource cluster-wide | `sudo pcs resource disable <resource-id>` |
| Enable a resource cluster-wide | `sudo pcs resource enable <resource-id>` |
| Ban a resource from one node | `sudo pcs resource ban <resource-id> <nodename>` |
| Clear ban on a resource | `sudo pcs resource clear <resource-id> <nodename>` |
| Put a node into standby | `sudo pcs node standby <nodename>` |
| Restore a node from standby | `sudo pcs node unstandby <nodename>` |
| Refresh resource state | `sudo pcs resource refresh <resource-id>` |
| List all constraints | `sudo pcs constraint list --full` |
| Show Corosync ring status | `sudo corosync-cfgtool -s` |
| Show quorum status | `sudo corosync-quorumtool -l` |
| Verify cluster config syntax | `sudo crm_verify -L -V` |
| Show CIB (cluster config XML) | `sudo pcs cluster cib` |
| Start cluster on a node | `sudo pcs cluster start` |
| Stop cluster on a node | `sudo pcs cluster stop` |
| Bootstrap Galera primary | `sudo galera_new_cluster` |
| Check Galera cluster size | `SHOW STATUS LIKE 'wsrep_cluster_size';` |
| Check Galera sync state | `SHOW STATUS LIKE 'wsrep_local_state_comment';` |
| Check Galera flow control | `SHOW STATUS LIKE 'wsrep_flow_control_paused';` |
| Check Galera recv queue | `SHOW STATUS LIKE 'wsrep_local_recv_queue_avg';` |
| Check RabbitMQ cluster status | `sudo rabbitmqctl cluster_status` |
| List RabbitMQ queue depths | `sudo rabbitmqctl list_queues name messages consumers` |
| Inject netem latency + loss | `sudo tc qdisc add dev <iface> root netem delay <ms> loss <pct>%` |
| Remove netem fault | `sudo tc qdisc del dev <iface> root` |
| Show active tc rules | `sudo tc qdisc show dev <iface>` |
| Drop all traffic to remote IP | `sudo iptables -I INPUT/OUTPUT -s/-d <ip> -j DROP` |
| Remove iptables DROP rule | `sudo iptables -D INPUT/OUTPUT -s/-d <ip> -j DROP` |
| List all iptables rules | `sudo iptables -L -n -v --line-numbers` |
| Flush all iptables DROP rules | `sudo iptables -F` *(flushes entire chain — use carefully)* |
| Start tcpdump capture to file | `sudo tcpdump -i <iface> -s 0 -w <file.pcap> '<filter>'` |
| Read and display pcap | `sudo tcpdump -r <file.pcap> -nn -A '<filter>'` |
| Count packets matching filter | `sudo tcpdump -r <file.pcap> -nn '<filter>' \| wc -l` |
| Decode AMQP methods (tshark) | `sudo tshark -r <file> -d tcp.port==5672,amqp -Y 'amqp'` |
| Decode MySQL queries (tshark) | `sudo tshark -r <file> -d tcp.port==3306,mysql -Y 'mysql'` |
| Measure TCP initial RTT | `sudo tshark -r <file> -Y 'tcp.analysis.initial_rtt' -T fields -e tcp.analysis.initial_rtt` |
| Find TCP retransmissions | `sudo tshark -r <file> -Y 'tcp.analysis.retransmission'` |
| Find TCP RST packets | `sudo tcpdump -r <file> -nn 'tcp[tcpflags] & tcp-rst != 0'` |

---

## Local Single-Node / Development Mode (Docker Compose)

Run the full pipeline on a single developer machine using Docker Compose for
RabbitMQ, MariaDB, and ZooKeeper — no cluster hardware required.

> **Port note:** ZooKeeper's internal admin server runs on container port `8080`.
> The `src/docker/docker-compose.yml` maps it to host port **`8081`** to avoid collision with
> API Gateway (Process 1)'s REST API which listens on host port `8080`.

### Step 1 — Create a local `.env`

Copy the example and override for localhost / Docker defaults:

```bash
cp src/config/.env.example .env
```

Edit `.env` — change **only** these values for local dev (everything else stays
as-is from `src/config/.env.example`):

```ini
# Per-node identity — use your local hostname
NODE_NAME=dev
CURRENT_NODE_IP=127.0.0.1

# Point all three node IPs to localhost for single-node mode
NODE1_IP=127.0.0.1
NODE2_IP=127.0.0.1
NODE3_IP=127.0.0.1

# ZooKeeper — single local container
ZK_HOSTS=127.0.0.1:2181

# The Docker containers below use the sample password "flowpassword", which the
# secret check rejects. For THIS throw-away local setup only, opt out explicitly
# (it logs a warning). Never set this on a real node.
ALLOW_DEFAULT_SECRETS=true

# RabbitMQ — matches src/docker/docker-compose.yml credentials
RABBITMQ_HOST=127.0.0.1
RABBITMQ_USER=flowuser
RABBITMQ_PASSWORD=flowpassword

# MariaDB — matches src/docker/docker-compose.yml credentials
MARIADB_HOST=127.0.0.1
MARIADB_USER=flowuser
MARIADB_PASSWORD=flowpassword
MARIADB_DB=flowfirst_db

# Swift — there is no Swift container. Swift is installed natively on the cluster
# (Phase 2b), so single-node development runs with object storage switched off.
# MariaDB persistence is unaffected; DB Updater (Process 4) prints "Swift store note: Swift
# storage is disabled" for each message.
SWIFT_ENABLED=false
```

> **Testing Swift on a development machine** needs a real Swift server: either
> point `SWIFT_AUTH_URL`, `SWIFT_USER` and `SWIFT_KEY` at the cluster's Swift
> (`http://<VIP>:8090/auth/v1.0`, reachable from your machine), or install Swift
> on a RHEL 9 VM with `swift/setup_swift.sh`.

### Step 2 — Start infrastructure containers

```bash
# Start RabbitMQ, MariaDB, and ZooKeeper containers
docker compose -f src/docker/docker-compose.yml up -d

# Wait for all containers to be healthy (up to ~30 s)
docker compose -f src/docker/docker-compose.yml ps
# All three services must show: health: healthy
```

Verify each service is reachable:

```bash
# RabbitMQ management UI — http://localhost:15672 (flowuser / flowpassword)
curl -s -o /dev/null -w "%{http_code}" http://localhost:15672
# Expected: 200

# ZooKeeper — single-node responds imok
echo ruok | nc -w2 127.0.0.1 2181
# Expected: imok

# MariaDB
mysql -h 127.0.0.1 -u "${MARIADB_USER}" -p"${MARIADB_PASSWORD}" -e "SELECT 1;"
# Expected: row with 1
```

### Step 3 — Activate the virtual environment

```bash
source .venv/bin/activate
# If you haven't installed dependencies yet:
pip install --require-hashes -r requirements.txt
```

### Step 4 — Start the four pipeline processes

Start in reverse order (consumers before producers) so no messages are lost:

```bash
# Terminal 1 — DB Updater (Process 4) (final consumer / DB persister)
python -m src.application.process4

# Terminal 2 — Forwarder (Process 3) (reflector / forwarder)
python -m src.application.process3

# Terminal 3 — Transformer (Process 2) (examiner / reflector)
python -m src.application.process2

# Terminal 4 — API Gateway (Process 1) (REST API producer)
python -m src.application.process1
```

Or run all four in the background from a single terminal:

```bash
source .venv/bin/activate
python -m src.application.process4 > /tmp/p4.log 2>&1 &
python -m src.application.process3 > /tmp/p3.log 2>&1 &
python -m src.application.process2 > /tmp/p2.log 2>&1 &
python -m src.application.process1 > /tmp/p1.log 2>&1 &
echo "All four processes started (PIDs: $(pgrep -f 'python process[1-4]' | tr '\n' ' '))"
```

### Step 5 — Send test messages

```bash
# Health check
curl -s http://localhost:8080/health | jq .

# Flow 1: P1 → P2 (counter +10, reflected) → P3 (ack) → P4 (DB persist)
curl -s -X POST http://localhost:8080/api/flow1 \
  -H "Content-Type: application/json" \
  -d '{"item_id": 1, "counter": 100, "initial_data": "local dev test"}' | jq .

# Flow 2: P1 → P2 (threshold examine + 1.15x scale) → P3 (verification seal) → P4
curl -s -X POST http://localhost:8080/api/flow2 \
  -H "Content-Type: application/json" \
  -d '{"item_id": 2, "value": 35.0, "initial_data": "high temp alert"}' | jq .

# Batch: publish 3 messages to both flows concurrently
curl -s -X POST http://localhost:8080/api/batch \
  -H "Content-Type: application/json" \
  -d '{"count": 3}' | jq .

# Greenthread / metrics status
curl -s http://localhost:8080/gt/status | jq .

# ZooKeeper pipeline health
curl -s http://localhost:8080/zk/health | jq .

# Live ZK runtime config
curl -s http://localhost:8080/zk/config | jq .

# Swift Object Storage status & stored objects query
# (with SWIFT_ENABLED=false this reports "enabled": false; objects need a real Swift server)
curl -s http://localhost:8080/swift/status | jq .
curl -s http://localhost:8080/swift/objects | jq .
# Query specific stored object payload from Swift
# curl -s http://localhost:8080/swift/object/flow1/<message_id>.json | jq .
```

### Step 6 — Inspect the database

```bash
# Summary of last 10 processed messages
mysql -h 127.0.0.1 -u ${MARIADB_USER} -p"${MARIADB_PASSWORD}" ${MARIADB_DB} \
  -e "SELECT id, flow_id, item_id, counter_value, metric_value,
             examined_status, verified_by, created_at
      FROM processed_messages
      ORDER BY id DESC LIMIT 10;"

# Full audit trail for a Flow 2 message
mysql -h 127.0.0.1 -u ${MARIADB_USER} -p"${MARIADB_PASSWORD}" ${MARIADB_DB} -e "
SELECT message_id, metric_value, examined_status, verified_by,
       JSON_PRETTY(history_trail) AS audit_trail
FROM processed_messages
WHERE flow_id = 2
ORDER BY id DESC LIMIT 1\G"
```

### Step 7 — Stop everything

```bash
# Kill background processes
pkill -f 'python process[1-4].py' 2>/dev/null || true

# Stop containers
docker compose -f src/docker/docker-compose.yml down

# Or stop and delete all data volumes (full reset)
docker compose -f src/docker/docker-compose.yml down -v
```

---

## ZooKeeper Scenarios

> **Changing runtime configuration is restricted (admin authorisation).** `GET /zk/config` is open, but every `POST /zk/config` in this chapter needs an allow-listed identity, otherwise it returns `403 Forbidden`. Set `ADMIN_ALLOWED_CNS` (certificate Common Names allowed to write, default `admin-client`) and `ADMIN_TRUSTED_PROXY_CNS` (`node1-pipeline,node2-pipeline,node3-pipeline`) in `.env`; with `TLS_API_ENABLED=true` call it with the matching client certificate:
>
> ```bash
> curl -s --cacert ca.crt --cert admin-client.crt --key admin-client.key \
>      -X POST https://${FLOWFIRST_VIP}:8080/zk/config -H "Content-Type: application/json" \
>      -d '{"key": "flow2_high_threshold", "value": 20.0}' | jq .
> ```
>
> In the plain-HTTP examples below (written before TLS existed) the call returns `403` unless you are on a throw-away setup with `ALLOW_UNAUTHENTICATED_ADMIN=true`. Without TLS the caller cannot be identified, so the default is to refuse. A blank `ADMIN_ALLOWED_CNS` allows nobody. Denials are logged at WARNING (`[authz] DENIED …`) with the identity and the reason, never any secret. Details and the identity rules: Scenario 63.

### Scenario 34: ZooKeeper Ensemble Health & Leader Election Verification

> **Runs on:** the `nc` checks from any of node1–node4 (ZooKeeper's client port must be reachable); `curl` from any machine that can reach the VIP (node1–node4 or a workstation).

Verify the ensemble is healthy and identify which node holds the ZooKeeper leader role.

```bash
# 1. Check each node responds "imok"
for node in ${NODE1_IP} ${NODE2_IP} ${NODE3_IP}; do
    echo -n "${node}: "
    echo ruok | nc -w2 "${node}" 2181 || echo "NO RESPONSE"
done
# Expected: all three print "imok"

# 2. Identify the ZooKeeper leader
for node in ${NODE1_IP} ${NODE2_IP} ${NODE3_IP}; do
    state=$(echo mntr | nc -w2 "${node}" 2181 | grep zk_server_state | awk '{print $2}')
    echo "${node}: ${state}"
done
# Expected: one "leader", two "follower"

# 3. Confirm quorum (synced followers = 2)
ZK_LEADER=$(for n in ${NODE1_IP} ${NODE2_IP} ${NODE3_IP}; do
    echo mntr | nc -w2 "$n" 2181 | grep -q "zk_server_state.*leader" && echo "$n"
done)
echo mntr | nc -w2 "${ZK_LEADER}" 2181 | grep zk_synced_followers
# Expected: zk_synced_followers   2

# 4. Check pipeline leader elections via REST API
curl -s http://${FLOWFIRST_VIP}:8080/zk/health | jq .pipeline_health
# Expected: each process shows one node as "leader", others as "follower"
```

*Expected output from `/zk/health`:*
```json
{
  "pipeline_health": {
    "api_gateway": { "node1": {"state": "leader", ...}, "node2": {"state": "follower", ...} },
    "transformer": { "node2": {"state": "leader", ...}, "node1": {"state": "follower", ...} },
    "forwarder": { "node3": {"state": "leader", ...} },
    "db_updater": { "node1": {"state": "leader", ...} }
  }
}
```

---

### Scenario 35: Live Config Change via ZooKeeper — No Restart Required

> **Runs on:** `curl` from any machine that can reach the VIP (node1–node4 or a workstation); `zkCli.sh` on any of node1–node3; `journalctl` on the **current Transformer (Process 2) leader node** (see `/zk/health`).

Change the Flow 2 HIGH threshold and the Flow 1 counter step at runtime. All three running
Transformer (Process 2) instances pick up the change within seconds without any restart.

#### A. Inspect current config

```bash
curl -s http://${FLOWFIRST_VIP}:8080/zk/config | jq .
# Expected:
# {
#   "zk_config": {
#     "flow2_high_threshold": 30.0,
#     "flow1_counter_step": 10,
#     "flow2_scale_factor": 1.15
#   }
# }
```

#### B. Lower the HIGH threshold to 20.0

```bash
curl -s -X POST http://${FLOWFIRST_VIP}:8080/zk/config \
  -H "Content-Type: application/json" \
  -d '{"key": "flow2_high_threshold", "value": 20.0}' | jq .
```

#### C. Send a Flow 2 message with a value of 25.0 (was NORMAL, now HIGH)

```bash
curl -s -X POST http://${FLOWFIRST_VIP}:8080/api/flow2 \
  -H "Content-Type: application/json" \
  -d '{"item_id": 901, "value": 25.0, "initial_data": "ZK config test"}' | jq .
```

Watch Transformer (Process 2) logs on any node — it should now assign `status=HIGH`:

```bash
sudo journalctl -u flowfirst-process2 -f --no-pager | grep "status=\|threshold="
# Expected: status=HIGH, threshold=20.0
```

#### D. Increase the counter step to 50

```bash
curl -s -X POST http://${FLOWFIRST_VIP}:8080/zk/config \
  -H "Content-Type: application/json" \
  -d '{"key": "flow1_counter_step", "value": 50}' | jq .
```

Send a Flow 1 message and verify the counter increment in the MariaDB audit trail:

```bash
curl -s -X POST http://${FLOWFIRST_VIP}:8080/api/flow1 \
  -H "Content-Type: application/json" \
  -d '{"item_id": 902, "counter": 100}' | jq .

# Counter after Transformer (Process 2): should be 100 + 50 = 150
mysql -h ${FLOWFIRST_VIP} -u ${MARIADB_USER} -p"${MARIADB_PASSWORD}" ${MARIADB_DB} \
  -e "SELECT item_id, counter_value, history_trail FROM processed_messages WHERE item_id=902\G"
```

#### E. Restore defaults

```bash
curl -s -X POST http://${FLOWFIRST_VIP}:8080/zk/config -H "Content-Type: application/json" \
  -d '{"key": "flow2_high_threshold", "value": 30.0}' | jq .
curl -s -X POST http://${FLOWFIRST_VIP}:8080/zk/config -H "Content-Type: application/json" \
  -d '{"key": "flow1_counter_step", "value": 10}' | jq .
```

#### F. Verify via zkCli directly

```bash
/opt/zookeeper/bin/zkCli.sh -server 127.0.0.1:2181 \
  get /flowfirst/config/flow2_high_threshold
# Expected: 30.0
```

---

### Scenario 36: ZooKeeper Leader Failover — Kill the Elected Leader Node

> **Runs on:** A, B, E and F `curl`/`mysql` from any machine that can reach the VIP (node1–node4 or a workstation) (`mysql` needs a mariadb client); C via `ssh ${NODE2_IP}` from **node1 or node3** (or run the quoted command directly on node2); D on **node1 or node3** (the surviving candidates).

Verify that when the node holding the Transformer (Process 2) leader election crashes, the standby
on another node automatically wins and takes over consuming without message loss.

#### A. Identify the current Transformer (Process 2) leader

```bash
curl -s http://${FLOWFIRST_VIP}:8080/zk/health | jq '.pipeline_health.process2'
# Note which node shows "state": "leader" — assume it is node2
```

#### B. Send 5 Flow 1 messages to build up a queue backlog

```bash
for i in $(seq 1 5); do
  curl -s -X POST http://${FLOWFIRST_VIP}:8080/api/flow1 \
    -H "Content-Type: application/json" \
    -d "{\"item_id\": $((2000 + i)), \"initial_data\": \"leader-failover-test\"}" | jq -r .payload.message_id
done
```

#### C. Kill Transformer (Process 2) on the leader node (node2)

```bash
# From node1 or node3 — stop only process2 on node2
ssh ${NODE2_IP} "sudo pcs resource ban flowfirst-p2-res-clone node2"
# Or forcefully kill the process:
ssh ${NODE2_IP} "sudo systemctl stop flowfirst-process2"
```

#### D. Watch the ZK election on node1 or node3

```bash
sudo journalctl -u flowfirst-process2 -f --no-pager | grep -E "LEADER|election|flow[12]-consumer"
# Expected within ~10 seconds (the first line is the INFO log from zk.py):
# INFO [zk] [process2] *** NODE node1 IS NOW LEADER ***
# INFO flow1-consumer greenthread started — consuming 'flow1_p1_to_p2'
# INFO flow2-consumer greenthread started — consuming 'flow2_p1_to_p2'
```

#### E. Verify all 5 messages were processed (no loss, no duplicates)

```bash
mysql -h ${FLOWFIRST_VIP} -u ${MARIADB_USER} -p"${MARIADB_PASSWORD}" ${MARIADB_DB} \
  -e "SELECT item_id, counter_value, history_trail
      FROM processed_messages
      WHERE item_id BETWEEN 2001 AND 2005
      ORDER BY item_id\G"
# Expected: 5 rows, each with a complete 4-stage history trail
# Each row's history should show process2_reflected with the new leader's node name
```

#### F. Restore the banned node

```bash
ssh ${NODE2_IP} "sudo pcs resource clear flowfirst-p2-res-clone"
curl -s http://${FLOWFIRST_VIP}:8080/zk/health | jq '.pipeline_health.process2'
# All three nodes now show registered (leader on one, follower on the others)
```

---

### Scenario 37: ZooKeeper Dedup Barrier — Prevent Double-Insert on Failover

> **Runs on:** `mysql` from any node with a mariadb client; `rabbitmqadmin` and `zkCli.sh` on node1–node3; `journalctl` on the **current DB Updater (Process 4) leader node**.

Simulate a Pacemaker failover that re-delivers an already-processed message to
DB Updater (Process 4) and verify the ZooKeeper dedup barrier prevents a duplicate MariaDB row.

#### A. Record a message_id that was already processed

```bash
PROCESSED_ID=$(mysql -h ${FLOWFIRST_VIP} -u ${MARIADB_USER} -p"${MARIADB_PASSWORD}" ${MARIADB_DB} \
  -sNe "SELECT message_id FROM processed_messages ORDER BY id DESC LIMIT 1;")
echo "Already processed: ${PROCESSED_ID}"
```

#### B. Manually re-publish the same message to the final queue

```bash
# Get the raw payload that was already stored
RAW=$(mysql -h ${FLOWFIRST_VIP} -u ${MARIADB_USER} -p"${MARIADB_PASSWORD}" ${MARIADB_DB} \
  -sNe "SELECT raw_payload FROM processed_messages WHERE message_id='${PROCESSED_ID}';")

# Re-publish it directly to the flow1_p3_to_p4 queue using rabbitmqadmin
sudo rabbitmqadmin -u "${RABBITMQ_USER}" -p "${RABBITMQ_PASSWORD}" publish \
  exchange='' routing_key='flow1_p3_to_p4' \
  properties='{"content_type":"application/json"}' \
  payload="${RAW}"
```

#### C. Watch DB Updater (Process 4) logs — expect the dedup barrier to fire

```bash
sudo journalctl -u flowfirst-process4 -f --no-pager | grep -i "dedup\|duplicate"
# Expected (one pair of lines per redelivered message):
# WARNING [zk] Dedup: duplicate detected for <message_id> — skipping insert.
# WARNING [Flow 1] DUPLICATE via ZK dedup — skipping insert for <message_id>
```

#### D. Verify no duplicate row was inserted

```bash
mysql -h ${FLOWFIRST_VIP} -u ${MARIADB_USER} -p"${MARIADB_PASSWORD}" ${MARIADB_DB} \
  -e "SELECT COUNT(*) as row_count FROM processed_messages WHERE message_id='${PROCESSED_ID}';"
# Expected: row_count = 1  (not 2)
```

#### E. Inspect the dedup znode in ZooKeeper

```bash
/opt/zookeeper/bin/zkCli.sh -server 127.0.0.1:2181 \
  ls /flowfirst/dedup
# Expected: [<message_id>, ...]

/opt/zookeeper/bin/zkCli.sh -server 127.0.0.1:2181 \
  stat /flowfirst/dedup/${PROCESSED_ID}
# Shows ctime, mtime — the node was created when DB Updater (Process 4) first processed it
```

---

### Scenario 38: ZooKeeper Service Registry — Observe Live Process Registration

> **Runs on:** `zkCli.sh` on any of node1–node3; `pcs` steps from **any cluster node**; `curl` from any machine that can reach the VIP (node1–node4 or a workstation).

Verify the ephemeral service registry updates in real time as processes start and stop.

#### A. Read the current service registry via REST and zkCli

```bash
# Via REST API (the live registry is on /zk/health; /health no longer calls ZooKeeper)
curl -s http://${FLOWFIRST_VIP}:8080/zk/health | jq .registered_workers
# Expected: all 4 processes, all 3 nodes listed

# Via zkCli
/opt/zookeeper/bin/zkCli.sh -server 127.0.0.1:2181 <<'EOF'
ls /flowfirst/registry/process1
ls /flowfirst/registry/process2
ls /flowfirst/registry/process3
ls /flowfirst/registry/process4
quit
EOF
```

#### B. Stop Forwarder (Process 3) on node2 and observe the ephemeral znode disappear

Forwarder (Process 3) is managed by Pacemaker, which would restart a unit stopped with `systemctl stop` within seconds (the znode would barely disappear). Ban the clone from node2 instead; Pacemaker then stops it there and does not restart it:

```bash
# Stop process3 on node2 (kept down until the ban is cleared in C)
sudo pcs resource ban flowfirst-p3-res-clone node2

# Watch the registry — node2 should disappear within the ZK session timeout (~10s)
sleep 12
/opt/zookeeper/bin/zkCli.sh -server 127.0.0.1:2181 \
  ls /flowfirst/registry/process3
# Expected: [node1, node3]  — node2 is gone

# Confirm via REST
curl -s http://${FLOWFIRST_VIP}:8080/zk/health | jq '.registered_workers.process3'
# Expected: ["node1", "node3"]
```

#### C. Restart Forwarder (Process 3) on node2 and watch it re-register

```bash
sudo pcs resource clear flowfirst-p3-res-clone node2
sleep 5

/opt/zookeeper/bin/zkCli.sh -server 127.0.0.1:2181 \
  ls /flowfirst/registry/process3
# Expected: [node1, node2, node3]

# Inspect the registration payload
/opt/zookeeper/bin/zkCli.sh -server 127.0.0.1:2181 \
  get /flowfirst/registry/process3/node2
# Expected JSON: {"node":"node2","pid":XXXX,"started_at":"..."}
```

#### D. Verify the pipeline health dashboard reflects the re-registration

```bash
curl -s http://${FLOWFIRST_VIP}:8080/zk/health | jq '.pipeline_health.process3'
# Expected: node2 back with "state": "follower" (or "leader" if it won election)
```

---

## Greenthread Scenarios

### Scenario 39: Verify Greenthread Startup & Pool Status

> **Runs on:** `curl` from any machine that can reach the VIP (node1–node4 or a workstation); `journalctl` and `/proc/<pid>/limits` checks on **each of node1–node3** (each node runs its own P1–P4 instances).

Confirm all expected greenthreads are running in each process immediately after startup.

```bash
# 1. Check API Gateway (Process 1) greenthread status via the new /gt/status endpoint
curl -s http://${FLOWFIRST_VIP}:8080/gt/status | jq .
```

*Expected response:*
```json
{
  "active_greenthreads": [
    "p1_http_server",
    "p1_rmq_heartbeat",
    "p1_zk_health",
    "process1_metrics"
  ],
  "metrics": {
    "flow1_published": 0,
    "flow2_published": 0
  },
  "pool_size": 1000,
  "is_stopping": false
}
```

```bash
# 2. Check /health — includes the greenthread list and metrics
curl -s http://${FLOWFIRST_VIP}:8080/health | jq '{node: .node, greenthreads: .greenthreads, metrics: .metrics}'
```

```bash
# 3. Confirm all four processes are running with expected greenthreads via journalctl
for svc in flowfirst-process1 flowfirst-process2 flowfirst-process3 flowfirst-process4; do
    echo "=== ${svc} ==="
    sudo journalctl -u "${svc}" -n 5 --no-pager | grep -E "\[gt\]|greenthread"
done
# Expected per process:
# INFO All greenthreads started: ['p2_flow1_consumer', 'p2_flow2_consumer', ...]
# INFO flow1-consumer greenthread started — consuming 'flow1_p1_to_p2'
# INFO flow2-consumer greenthread started — consuming 'flow2_p1_to_p2'
```

```bash
# 4. Verify OS-level file descriptor limit is in effect
# (Pacemaker-managed processes use LimitNOFILE=65536 from the unit file)
sudo cat /proc/$(pgrep -f src.application.process1 | head -1)/limits | grep "open files"
# Expected: Max open files   65536   65536   files
```

---

### Scenario 40: Concurrent Batch Publishing — Greenthread Parallelism in API Gateway (Process 1)

> **Runs on:** `curl` from any machine that can reach the VIP (node1–node4 or a workstation); `rabbitmqctl` on node1–node3; `journalctl` on the **current Transformer (Process 2) leader node**.

The `/api/batch` endpoint spawns one greenthread per message, publishing Flow 1 and
Flow 2 messages in parallel rather than sequentially.  This scenario measures the
throughput improvement.

#### A. Sequential baseline (single message timing)

```bash
time curl -s -X POST http://${FLOWFIRST_VIP}:8080/api/flow1 \
  -H "Content-Type: application/json" \
  -d '{"item_id": 1, "initial_data": "gt-timing-test"}' > /dev/null
# Note the elapsed time — this is single-message latency
```

#### B. Concurrent batch of 20 messages (greenthread parallelism)

```bash
time curl -s -X POST http://${FLOWFIRST_VIP}:8080/api/batch \
  -H "Content-Type: application/json" \
  -d '{"count": 20}' | jq '{flow1_count: (.flow1_messages | length), flow2_count: (.flow2_messages | length)}'
# Expected: flow1_count=20, flow2_count=20
# Elapsed time should be much less than 20x single-message time
```

#### C. Verify all 20 Flow 1 messages reached Transformer (Process 2)

```bash
# Check RabbitMQ queue depth (should be draining toward 0)
sudo rabbitmqctl list_queues name messages | grep flow1_p1_to_p2

# Check metrics counter on API Gateway (Process 1)
curl -s http://${FLOWFIRST_VIP}:8080/gt/status | jq .metrics.flow1_published
# Expected: ≥ 20
```

#### D. Confirm all 20 were persisted to MariaDB (end-to-end)

```bash
# Wait a few seconds for the pipeline to drain
sleep 5

mysql -h ${FLOWFIRST_VIP} -u ${MARIADB_USER} -p"${MARIADB_PASSWORD}" ${MARIADB_DB} \
  -e "SELECT COUNT(*) as total, MAX(created_at) as latest
      FROM processed_messages
      WHERE flow_id = 1
      AND created_at > NOW() - INTERVAL 60 SECOND;"
# Expected: total ≥ 20
```

#### E. Check Transformer (Process 2) throughput metrics

```bash
sudo journalctl -u flowfirst-process2 --since "2 minutes ago" --no-pager \
  | grep "Throughput Report"
# Expected lines like:
# INFO [gt][process2] ── Throughput Report ──
#   flow1_processed: total=20  +20 in last 60s
#   flow2_processed: total=20  +20 in last 60s
#   active_greenthreads: ['p2_flow1_consumer', 'p2_flow2_consumer', ...]
```

---

### Scenario 41: Per-Queue Greenthread Independence — Flow 1 Blockage Does Not Stall Flow 2

> **Runs on:** `curl` from any machine that can reach the VIP (node1–node4 or a workstation); `rabbitmqctl` on node1–node3; `journalctl` on the **current Transformer (Process 2) leader node**.

Each queue in Transformer (Process 2), 3, and 4 runs in its own greenthread with its own pika
connection.  A slow or blocked Flow 1 handler should not delay Flow 2 messages.

#### A. Inject a slow Flow 1 message by temporarily raising the counter step

```bash
# Set counter_step to a value that causes visible processing (ZK config, live)
curl -s -X POST http://${FLOWFIRST_VIP}:8080/zk/config \
  -H "Content-Type: application/json" \
  -d '{"key": "flow1_counter_step", "value": 999}' | jq .
```

#### B. Flood 10 Flow 1 and 10 Flow 2 messages simultaneously

```bash
# Send all 20 in parallel from the client side
for flow in flow1 flow2; do
    for i in $(seq 1 10); do
        curl -s -X POST "http://${FLOWFIRST_VIP}:8080/api/${flow}" \
          -H "Content-Type: application/json" \
          -d "{\"item_id\": $((3000 + i)), \"initial_data\": \"greenthread-independence-test\"}" \
          > /dev/null &
    done
done
wait
echo "All 20 requests sent"
```

#### C. Watch Transformer (Process 2) logs — both queues processed concurrently

```bash
sudo journalctl -u flowfirst-process2 -f --no-pager | grep -E "\[Flow [12]\]"
# Expected: Flow 1 and Flow 2 log lines are interleaved — not Flow 1 completes
# before Flow 2 starts.  The `gt.sleep(0)` yield after each message allows
# the sibling greenthread to run between messages.
```

#### D. Verify both queues drain to 0 depth in parallel

```bash
watch -n1 "sudo rabbitmqctl list_queues name messages | grep -E 'flow[12]_p1_to_p2'"
# Expected: both queues drain simultaneously, not sequentially
```

#### E. Restore default counter step

```bash
curl -s -X POST http://${FLOWFIRST_VIP}:8080/zk/config \
  -H "Content-Type: application/json" \
  -d '{"key": "flow1_counter_step", "value": 10}' | jq .
```

---

### Scenario 42: Greenthread Auto-Restart on RabbitMQ Consumer Failure

> **Runs on:** A and C on the **current Transformer (Process 2) leader** (assumed node1 below); B from node1 via `ssh` to node2 — `close_all_connections` drops every connection on node2's broker, not just Transformer (Process 2)'s.

Consumer greenthreads are spawned with `restart_on_error=True`.  If a consumer
greenthread crashes (e.g. RabbitMQ connection dropped), it automatically restarts
after 2 seconds without operator intervention or process restart.

#### A. Identify the RabbitMQ node that Transformer (Process 2) on node1 is connected to

```bash
sudo journalctl -u flowfirst-process2 --since "1 hour ago" --no-pager | grep "RabbitMQ connected at" | tail -3
# Example: INFO [gt] [p2-flow1] RabbitMQ connected at 192.168.1.102:5672
# (config.get_connection() tries NODE1_IP first, so this is normally node1; it names node2 only
#  if node1's broker was unreachable when the greenthread connected.)
```

#### B. Force-drop the RabbitMQ connection on that node

```bash
# On the node Transformer (Process 2) connected to (e.g. node2) — close all connections from node1
ssh ${NODE2_IP} "sudo rabbitmqctl close_all_connections 'greenthread restart test'"
```

#### C. Watch Transformer (Process 2) on node1 detect the drop and auto-restart consumers

```bash
sudo journalctl -u flowfirst-process2 -f --no-pager | grep -E "restart|Reconnect|consumer greenthread|not ready"
# Expected sequence (within ~10 seconds):
# ERROR [gt] Greenthread 'p2_flow1_consumer' raised: <connection error>
# INFO [gt] Restarting 'p2_flow1_consumer' in 2s...
# INFO [gt] [p2-flow1] RabbitMQ connected at 192.168.1.101:5672
# INFO flow1-consumer greenthread started — consuming 'flow1_p1_to_p2'
# (a WARNING "RabbitMQ not ready (attempt 1/20): ... — retrying in 5s..." appears before the
#  'connected' line only if no broker answered the first attempt)
```

#### D. Confirm no messages were lost during the restart window

```bash
# Send 5 Flow 1 messages while the restart is in progress
for i in $(seq 4001 4005); do
    curl -s -X POST http://${FLOWFIRST_VIP}:8080/api/flow1 \
      -H "Content-Type: application/json" \
      -d "{\"item_id\": ${i}, \"initial_data\": \"auto-restart-test\"}" | jq -r .payload.message_id
done

# Wait for pipeline to drain
sleep 15

mysql -h ${FLOWFIRST_VIP} -u ${MARIADB_USER} -p"${MARIADB_PASSWORD}" ${MARIADB_DB} \
  -e "SELECT item_id, counter_value FROM processed_messages
      WHERE item_id BETWEEN 4001 AND 4005 ORDER BY item_id;"
# Expected: all 5 rows present (messages queued in RabbitMQ during restart window,
# consumed once the greenthread reconnected)
```

---

### Scenario 43: Greenthread Metrics Reporter — Throughput Monitoring

> **Runs on:** `curl` from any machine that can reach the VIP (node1–node4 or a workstation); `ssh`/`sed`/`pcs` steps on any cluster member (node1, node2 or node3); `journalctl` on each process's **current leader node**. node4's `.env` is not changed by these steps.

Each process emits a structured throughput report every `GT_METRICS_INTERVAL_S` seconds
(default 60s).  This scenario triggers activity and then reads the metrics.

#### A. Set the metrics interval to 15 seconds for fast observation

```bash
# This requires restarting the processes since GT_METRICS_INTERVAL_S is read at startup.
# On each node, update .env and restart via Pacemaker:
ssh ${NODE1_IP} "sed -i 's/^GT_METRICS_INTERVAL_S=.*/GT_METRICS_INTERVAL_S=15/' /opt/flowfirst/.env"
ssh ${NODE2_IP} "sed -i 's/^GT_METRICS_INTERVAL_S=.*/GT_METRICS_INTERVAL_S=15/' /opt/flowfirst/.env"
ssh ${NODE3_IP} "sed -i 's/^GT_METRICS_INTERVAL_S=.*/GT_METRICS_INTERVAL_S=15/' /opt/flowfirst/.env"

sudo pcs resource restart flowfirst-p1-res-clone
sudo pcs resource restart flowfirst-p2-res-clone
sudo pcs resource restart flowfirst-p3-res-clone
sudo pcs resource restart flowfirst-p4-res-clone
```

#### B. Generate pipeline activity

```bash
# Send 50 messages across both flows
for i in $(seq 1 25); do
    curl -s -X POST http://${FLOWFIRST_VIP}:8080/api/flow1 \
      -H "Content-Type: application/json" \
      -d "{\"item_id\": $((5000 + i)), \"initial_data\": \"metrics-test\"}" > /dev/null
    curl -s -X POST http://${FLOWFIRST_VIP}:8080/api/flow2 \
      -H "Content-Type: application/json" \
      -d "{\"item_id\": $((5000 + i)), \"value\": 28.5}" > /dev/null
done
echo "50 messages sent"
```

#### C. Read metrics from /gt/status (API Gateway (Process 1) leader node)

```bash
# Wait for metrics interval
sleep 16

curl -s http://${FLOWFIRST_VIP}:8080/gt/status | jq .metrics
# Expected:
# {
#   "flow1_published": 25,
#   "flow2_published": 25
# }
```

#### D. Read per-process metrics from journal

```bash
# Transformer (Process 2) metrics (shows flow1_processed and flow2_processed)
sudo journalctl -u flowfirst-process2 --since "1 minute ago" --no-pager \
  | grep -A 10 "Throughput Report"

# Expected output on the leader node:
# INFO [gt][process2] ── Throughput Report ──
#   flow1_processed: total=25  +25 in last 15s
#   flow2_processed: total=25  +25 in last 15s
#   active_greenthreads: ['p2_flow1_consumer', 'p2_flow2_consumer', ...]

# DB Updater (Process 4) metrics (shows flow1_persisted, flow2_persisted, dedup_reaped)
sudo journalctl -u flowfirst-process4 --since "1 minute ago" --no-pager \
  | grep -A 10 "Throughput Report"
```

#### E. Restore metrics interval to default

```bash
for node in ${NODE1_IP} ${NODE2_IP} ${NODE3_IP}; do
    ssh "${node}" "sed -i 's/^GT_METRICS_INTERVAL_S=.*/GT_METRICS_INTERVAL_S=60/' /opt/flowfirst/.env"
done
sudo pcs resource restart flowfirst-p1-res-clone
sudo pcs resource restart flowfirst-p2-res-clone
sudo pcs resource restart flowfirst-p3-res-clone
sudo pcs resource restart flowfirst-p4-res-clone
```

---

### Scenario 44: Remote Node 4 / Remote Worker (Process 5) End-to-End Pipeline Execution & Database Validation

> **Runs on:** A and C on **node4**; B, D and E from any machine that can reach the VIP (node1–node4 or a workstation) (D needs a mariadb client).

Demonstrates cross-node asynchronous inter-process communication spanning the core 3-node cluster and **Remote Node 4 (`node4`)**:
1. `API Gateway (Process 1)` on one of the cluster nodes receives `POST /api/flow3` and prepares the initial message payload.
2. `API Gateway (Process 1)` pushes the payload onto the RabbitMQ pool queue `flow3_p1_to_p5`.
3. `Remote Worker (Process 5)` running on **Remote Node 4 (`node4`)** consumes the message from the RabbitMQ pool, applies remote transformation/computation, records audit details, and **reflects** the modified payload back to the RabbitMQ pool on `flow3_p5_to_p2`.
4. `Transformer (Process 2)` on one of the cluster nodes consumes the reflected message from `flow3_p5_to_p2`, verifies the remote node processing, logs the verification step, and pushes it to `flow3_p2_to_p4`.
5. `DB Updater (Process 4)` consumes the message, passes the ZooKeeper dedup barrier, and persists the complete audit history into both the **MariaDB Galera Cluster** (`processed_messages` table) and **Swift Object Storage**.

#### A. Pre-requisite: Verify Remote Worker (Process 5) is running on Remote Node 4

On Remote Node 4 (`node4`), only the lightweight Python environment and `process5` run — no broker or database server daemons:

```bash
# On Remote Node 4 (node4):
sudo systemctl status flowfirst-process5
# Confirm no cluster daemons are running on Node 4 (returns nothing):
pgrep -a mariadbd || pgrep -a beam.smp || pgrep -a zookeeper || echo "Clean client-only node"
```

#### B. Trigger Flow 3 via Virtual IP REST API

```bash
curl -s -X POST http://${FLOWFIRST_VIP}:8080/api/flow3 \
     -H "Content-Type: application/json" \
     -d '{"item_id": 9001, "remote_metric": 64.0, "initial_data": "Remote telemetry batch 9001"}' | jq .
```

*Expected API response:*
```json
{
  "status": "success",
  "flow": 3,
  "handled_by_node": "node1",
  "target_queue": "flow3_p1_to_p5",
  "target_node": "remote_node4",
  "payload": {
    "flow": 3,
    "message_id": "7b8f9a2c-d4e5-4a6b-8c1e-9f0a1b2c3d4e",
    "item_id": 9001,
    "initial_data": "Remote telemetry batch 9001",
    "remote_metric": 64.0,
    "history": [
      {
        "stage": "process1_created",
        "timestamp": "2025-06-15 14:00:00",
        "status": "prepared_for_remote_node4",
        "source": "rest_api",
        "published_by": "node1"
      }
    ]
  }
}
```

#### C. Inspect Remote Node 4 (Remote Worker (Process 5)) Logs

```bash
# On Remote Node 4:
sudo journalctl -u flowfirst-process5 --since "1 minute ago" --no-pager
# Expected output:
# INFO [Remote Node] Consumed Flow 3 item #9001 (msg_id=7b8f9a2c...)
# INFO [Remote Node] Reflected to 'flow3_p5_to_p2' (computed=135.5)
```

#### D. Validate Database Record & Full Audit Trail in MariaDB

Query the Galera cluster via the Virtual IP:

```bash
mariadb -h ${FLOWFIRST_VIP} -u ${MARIADB_USER} -p"${MARIADB_PASSWORD}" ${MARIADB_DB} -e "
SELECT id, message_id, flow_id, item_id, initial_data, created_at
FROM processed_messages
WHERE flow_id = 3 AND item_id = 9001\G"
```

*Expected result:*
```
*************************** 1. row ***************************
          id: 1
  message_id: 7b8f9a2c-d4e5-4a6b-8c1e-9f0a1b2c3d4e
     flow_id: 3
     item_id: 9001
initial_data: Remote telemetry batch 9001
  created_at: 2025-06-15 14:00:01
```

Examine the full 4-stage audit trail spanning Node 1-3 and Remote Node 4:

```bash
mariadb -h ${FLOWFIRST_VIP} -u ${MARIADB_USER} -p"${MARIADB_PASSWORD}" ${MARIADB_DB} -e "
SELECT JSON_PRETTY(history_trail) AS full_audit_trail
FROM processed_messages
WHERE flow_id = 3 AND item_id = 9001\G"
```

*Expected audit trail JSON:*
```json
[
  {
    "stage": "process1_created",
    "timestamp": "2025-06-15 14:00:00",
    "status": "prepared_for_remote_node4",
    "source": "rest_api",
    "published_by": "node1"
  },
  {
    "stage": "process5_remote_transformed_and_reflected",
    "timestamp": "2025-06-15 14:00:00",
    "processed_by": "node4",
    "node_type": "remote_node4",
    "modification": "Applied remote transform (multiplier=2, computed_value=135.5)"
  },
  {
    "stage": "process2_received_from_remote_and_forwarded_to_p4",
    "timestamp": "2025-06-15 14:00:01",
    "processed_by": "node2",
    "status": "remote_flow3_verified",
    "action": "Received from node4 (computed_val=135.5) and forwarded to DB Updater (Process 4)"
  },
  {
    "stage": "process4_saved_to_mariadb",
    "timestamp": "2025-06-15 14:00:01",
    "persisted_by": "node3",
    "database_action": "INSERT/UPDATE processed_messages (flow3 remote complete)"
  }
]
```

#### E. Verify Swift Object Storage Persistence & REST API Query

```bash
# Query Swift stored object via API Gateway (Process 1) REST API
MSG_ID=$(mariadb -h ${FLOWFIRST_VIP} -u ${MARIADB_USER} -p"${MARIADB_PASSWORD}" ${MARIADB_DB} -N -e "
SELECT message_id FROM processed_messages WHERE flow_id = 3 AND item_id = 9001;")

curl -s http://${FLOWFIRST_VIP}:8080/swift/object/flow3/${MSG_ID}.json | jq .
```

*Expected Swift API response:*
```json
{
  "status": "success",
  "object_name": "flow3/7b8f9a2c-d4e5-4a6b-8c1e-9f0a1b2c3d4e.json",
  "payload": {
    "flow": 3,
    "item_id": 9001,
    "remote_node": "node4",
    "remote_computed_value": 135.5,
    "process2_verified_remote": true
  }
}
```

---

## MariaDB Log Analysis

### Scenario 45: Reading the MariaDB Galera Error Log Across Scenarios

> **Runs on:** **each of node1–node3**. Each node writes its own error log, so it shows only that node's view of the cluster. The cross-node loop in D runs from any of node1–node3 over `ssh`. node4 has no MariaDB log.

Use this scenario alongside Scenarios 4–7, 14, 16–24 and 31 to work out from the log what Galera did and why. Exact wording varies between Galera versions, so the commands below grep for stable keywords rather than whole lines.

#### A. Locate the log

```bash
# On node1, node2 or node3
sudo mysql -u root -p -N -e "SELECT @@log_error;"
# Empty or relative value → RHEL default: /var/log/mariadb/mariadb.log

# Startup / shutdown and the pre-start position recovery (galera_recovery) go to the journal
sudo journalctl -u mariadb --since "30 min ago" --no-pager | grep -E "WSREP|Recovered position|Started|Stopped"
```

#### B. Message phrasebook — what each line means

| Log keyword (grep for) | Meaning | Typical scenarios |
|---|---|---|
| `Shifting OPEN -> PRIMARY`, `PRIMARY -> JOINER`, `JOINER -> JOINED`, `JOINED -> SYNCED` | Node state machine during start/rejoin. Ends at `SYNCED` when the node is ready. | 4, 14, 16, 18 |
| `Shifting SYNCED -> DONOR/DESYNCED`, `DONOR/DESYNCED -> JOINED` | This node is acting as donor for another node's state transfer | 5, 6, 7 (on donor) |
| `New COMPONENT: primary = yes ... memb_num = N` | New cluster membership formed; `memb_num` is the cluster size | 4, 14, 16–19, 22–24 |
| `New COMPONENT: primary = no`, `non-Primary` | This node lost quorum and stopped accepting writes | 22, 24 (isolated side) |
| `suspecting node`, `forgetting`, `inactive` / `went down` | Group communication lost contact with a peer (crash, reboot or network block) | 16, 17, 20–24 |
| `requested state transfer from`, `State transfer required` | Joiner asked for IST/SST; the line names the joiner and the chosen donor | 5, 6, 7 |
| `Prepared IST receiver`, `Receiving IST`, `IST received` | IST used — only missing write-sets were streamed | 5, 7.3, 21 |
| `async IST sender` | Donor side of an IST | 5, 7.3 (on donor) |
| `Initiating SST`, `SST complete`, `wsrep_sst_rsync` | Full SST used (gap exceeded donor `gcache.size`, or `grastate.dat` reset) | 6, 7.4 |
| `IST will be unavailable`, `not found in gcache`, `Failed to prepare for incremental state transfer` | Why IST was rejected and SST used instead | 6, 7.4 |
| `SST failed`, `State transfer to ... failed` | State transfer aborted (often a firewall on 4444/4568 or full disk) — node will not reach `SYNCED` | 6, 21, 24 |
| `It may not be safe to bootstrap`, `safe_to_bootstrap` | Refused bootstrap on a node that was not the last to leave | 19 |
| `gcomm: bootstrapping new group` | `galera_new_cluster` was run on this node | 19 |
| `Recovered position` | Last committed seqno found during startup (journal) — use it to choose the bootstrap node | 19 |
| `Flow-control`, `flow control` | Replication throttled because a slow node's receive queue filled | 20, 21 |
| `WSREP: cluster conflict`, `BF-BF`, `certification failed` (needs `wsrep_log_conflicts`, see F) | Multi-master write conflict; the app sees deadlock error 1213, which `src/db/db.py` retries | 2, 31, concurrent load |
| `[ERROR]`, `Aborting` | Fatal — mariadb exits; read the lines just above it | any |

#### C. Build a timeline on one node

```bash
# On the node being investigated (e.g. node3 after Scenario 5/6/16)
sudo grep -E "Shifting|New COMPONENT|non-Primary|suspecting|forgetting|state transfer|IST|SST|bootstrap|Flow-control|\[ERROR\]|Aborting" \
  /var/log/mariadb/mariadb.log | tail -60
```

Read top to bottom: peer loss (`suspecting`/`forgetting`) → new component (`memb_num` drops) → … → rejoin (`requested state transfer`) → `IST`/`SST` → `JOINED -> SYNCED`.

#### D. Correlate the same event across all three nodes

```bash
# From any of node1–node3 (uses ssh to each node, including itself)
SINCE="$(date -d '-15 min' '+%Y-%m-%d %H:%M')"
for n in ${NODE1_IP} ${NODE2_IP} ${NODE3_IP}; do
  echo "===== ${n} ====="
  ssh "${n}" "sudo awk -v s='${SINCE}' '\$0 >= s' /var/log/mariadb/mariadb.log \
    | grep -E 'Shifting|New COMPONENT|non-Primary|state transfer|IST|SST|\[ERROR\]'"
done
```

Lines start with a `YYYY-MM-DD HH:MM:SS` timestamp, which is what the `awk` time filter compares against. Expected for Scenario 5 (IST to node3): node3 shows `JOINER` → `IST received` → `SYNCED`, the donor shows `SYNCED -> DONOR/DESYNCED` → `JOINED` → `SYNCED`, and the third node shows only `New COMPONENT ... memb_num = 3`.

#### E. Watch the log live while running another scenario

```bash
# On the node under test, in a second terminal, before starting the other scenario
sudo tail -F /var/log/mariadb/mariadb.log \
  | grep --line-buffered -E "Shifting|New COMPONENT|non-Primary|suspecting|state transfer|IST|SST|\[ERROR\]"
```

#### F. Temporarily increase verbosity (optional)

```bash
# On each of node1–node3 — runtime only, lost on restart
sudo mysql -u root -p -e "SET GLOBAL wsrep_log_conflicts=ON;"   # log certification conflicts (see table)

# Revert when finished
sudo mysql -u root -p -e "SET GLOBAL wsrep_log_conflicts=OFF;"
```

---

## RabbitMQ Log Analysis

### Scenario 46: Reading the RabbitMQ Broker Log Across Scenarios

> **Runs on:** **each of node1–node3**. Each broker writes its own log, so it shows only that node's view of the cluster. The cross-node loop in D runs from any of node1–node3 over `ssh`. node4 has no broker; its Remote Worker (Process 5) client-side errors are in `journalctl -u flowfirst-process5` on node4.

Use this scenario alongside Scenarios 13, 16–25, 40–42 and 44 to work out from the log what the broker did and why. Exact wording varies between RabbitMQ versions, so the commands below grep for stable keywords rather than whole lines.

#### A. Locate the log

```bash
# On node1, node2 or node3 (the local broker must be running)
sudo rabbitmq-diagnostics log_location
# RHEL default: /var/log/rabbitmq/rabbit@<short-hostname>.log

# If the broker is down, look in the directory and the journal instead
sudo ls -lt /var/log/rabbitmq/
sudo journalctl -u rabbitmq-server --since "30 min ago" --no-pager | tail -40

# Last lines without knowing the file name (broker must be running)
sudo rabbitmq-diagnostics log_tail -N 50
```

#### B. Message phrasebook — what each line means

| Log keyword (grep for) | Meaning | Typical scenarios |
|---|---|---|
| `Starting RabbitMQ`, `Server startup complete` | Broker boot began / finished; the node accepts AMQP after the second line | 13, 16–19 |
| `RabbitMQ is asked to stop`, `Successfully stopped RabbitMQ` | Clean shutdown (`systemctl stop`, reboot) | 13, 16–18 |
| `Waiting for Mnesia tables`, `Successfully synced tables from a peer` | Boot is waiting for a peer that holds the cluster metadata, then got it | 13.2, 16, 18, 19 |
| `Error while waiting for Mnesia tables`, `timeout_waiting_for_tables` | No peer answered within the retry window — boot will fail; start a peer, or start the node that stopped last first | 19 |
| `node 'rabbit@nodeX' up`, `rabbit on node 'rabbit@nodeX' up` | A peer broker joined/rejoined this node's view | 13.2, 16, 18, 19 |
| `node 'rabbit@nodeX' down`, `nodedown`, `net_tick_timeout` | Lost contact with a peer on 25672 (stop, reboot, partition or heavy latency) | 13, 16, 20–22 |
| `Mnesia(...): ** ERROR ** mnesia_event got {inconsistent_database, running_partitioned_network, ...}` | **Network partition detected.** With `cluster_partition_handling = pause_minority` (set by the install script) the minority side pauses itself | 22 |
| `Partial partition detected` | One node can see a peer that another node cannot; `pause_minority` pauses one side | 22, 23 |
| `minority` (e.g. `Cluster minority/secondary status detected - awaiting recovery`) | This broker could not see a majority and **paused**: AMQP listeners are closed and clients fail over to the other brokers | 22, 19 (2 nodes down) |
| `minority` … `cleared`, followed by `Starting RabbitMQ` / `Server startup complete` | Majority visible again; the paused broker restarts its app and resyncs | 22.6 |
| `accepting AMQP connection <...> (<client-ip>:<port> -> <node-ip>:5672)` | A pipeline process (or `rabbitmqadmin`) connected; the client IP shows which node's process it is | 13, 25, 42, 44 |
| `user 'flowuser' authenticated and granted access to vhost '/'` | Login succeeded for that connection | any |
| `closing AMQP connection ... (... vhost: '/', user: 'flowuser')` | Clean client disconnect (process stopped, or Pacemaker moved it) | 11, 12 |
| `client unexpectedly closed TCP connection` | Client vanished without closing (process killed, client node rebooted, network drop) | 16–18, 22, 25 |
| `missed heartbeats from client, timeout: 60s` | Client was silent for longer than `heartbeat=60` (set in `src/config/config.py`) — usually a network glitch or a stalled process | 20, 21, 41 |
| `Closing all connections`, `CONNECTION_FORCED` | An operator ran `rabbitmqctl close_all_connections` / `close_connection` | 42 |
| `memory resource limit alarm set`, `disk resource limit alarm set` | Broker is **blocking publishers**; clients see blocked connections (`blocked_connection_timeout=300` in `src/config/config.py`) | 40, heavy load |
| `alarm cleared` | Publishers unblocked | 40 |
| `invalid credentials`, `ACCESS_REFUSED`, `PLAIN login refused` | Wrong `RABBITMQ_USER` / `RABBITMQ_PASSWORD` in that node's `.env` | install, 43 |
| `Connection attempt from disallowed node`, `Cookie mismatch` | `/var/lib/rabbitmq/.erlang.cookie` differs between nodes — the node cannot join the cluster | install |
| `[error]`, `crash`, `CRASH REPORT` | Read the lines just above for the cause | any |

#### C. Build a timeline on one node

```bash
# On the node being investigated (e.g. node3 after Scenario 13)
LOG=$(sudo rabbitmq-diagnostics log_location 2>/dev/null | grep '\.log$' | head -1)
LOG=${LOG:-/var/log/rabbitmq/rabbit@$(hostname -s).log}
sudo grep -E -i "Starting RabbitMQ|startup complete|asked to stop|Mnesia|node .* (up|down)|partition|minority|accepting AMQP|closing AMQP|unexpectedly closed|missed heartbeats|alarm|\[error\]" \
  "${LOG}" | tail -60
```

Read top to bottom: peer `down` → clients `unexpectedly closed` / reconnect to the surviving brokers (`accepting AMQP` from new client IPs) → peer `up` / `Successfully synced tables` → `Server startup complete` on the rejoining node.

#### D. Correlate the same event across all three nodes

```bash
# From any of node1–node3 (uses ssh to each node, including itself)
SINCE="$(date -d '-15 min' '+%Y-%m-%d %H:%M')"
for n in ${NODE1_IP} ${NODE2_IP} ${NODE3_IP}; do
  echo "===== ${n} ====="
  ssh "${n}" "sudo sh -c 'cat /var/log/rabbitmq/rabbit@*.log' \
    | awk -v s='${SINCE}' '\$0 >= s' \
    | grep -E -i 'startup complete|asked to stop|node .* (up|down)|partition|minority|unexpectedly closed|missed heartbeats|alarm|\[error\]'"
done
```

Lines start with a `YYYY-MM-DD HH:MM:SS` timestamp, which is what the `awk` time filter compares against. Expected for Scenario 13 (stop/start on node3): node1 and node2 each show `node 'rabbit@node3' down`, then `accepting AMQP connection` from the processes that had been on node3's broker, then `node 'rabbit@node3' up`; node3 shows `asked to stop` → `Starting RabbitMQ` → `Successfully synced tables` → `Server startup complete`.

#### E. Watch the log live while running another scenario

```bash
# On the node under test, in a second terminal, before starting the other scenario
sudo tail -F /var/log/rabbitmq/rabbit@$(hostname -s).log \
  | grep --line-buffered -E -i "node .* (up|down)|partition|minority|accepting AMQP|closing AMQP|unexpectedly closed|missed heartbeats|alarm|\[error\]"
```

#### F. Temporarily increase verbosity (optional)

```bash
# On each of node1–node3 — runtime only, lost on broker restart
sudo rabbitmqctl set_log_level debug

# Revert when finished
sudo rabbitmqctl set_log_level info
```

Debug level is very noisy under pipeline load; keep it on only for the duration of the test.

---

## ZooKeeper Log Analysis

### Scenario 47: Reading the ZooKeeper Server & Client Logs Across Scenarios

> **Runs on:** server logs on **each of node1–node3**. Each ZooKeeper server writes its own log, so it shows only that server's view of the ensemble. Client-side logs (`journalctl -u flowfirst-processN`) are on whichever node runs that process, including **node4** for `flowfirst-process5`. The cross-node loop in D runs from any of node1–node3 over `ssh`.

Use this scenario alongside Scenarios 16–22 and 34–38 to work out from the logs which server led the ensemble, which clients lost their sessions, and why leaders, registrations or config watches changed. Exact wording varies between ZooKeeper and kazoo versions, so the commands below grep for stable keywords rather than whole lines.

#### A. Locate the logs

```bash
# On node1, node2 or node3 — server log (ZOO_LOG_DIR set by zookeeper/setup_zookeeper.sh)
sudo ls -lt /var/log/zookeeper/
# Text log:  zookeeper-zookeeper-server-<hostname>.out
# version-2/ holds the BINARY transaction log (dataLogDir points at the same directory) — do not grep it

# Startup failures before the JVM writes its log (wrong JAVA_HOME, port in use, bad myid)
sudo journalctl -u zookeeper --since "30 min ago" --no-pager | tail -40

# Current role of this server, without reading the log
echo srvr | nc -w2 127.0.0.1 2181 | grep -E "Mode|Zxid"
```

Client-side (pipeline processes, via kazoo):

```bash
# On the node running the process (node1–node3 for P1–P4, node4 for P5)
sudo journalctl -u flowfirst-process2 --since "30 min ago" --no-pager | grep -E -i "zk|zookeeper|kazoo|connection dropped|expired|leader"
```

> **Note:** `src/zookeeper/zk.py` and kazoo log through Python `logging`, which `gt.configure_logging()` sends to the journal at `LOG_LEVEL` (default `INFO`; `kazoo` is held at WARNING). So `INFO [zk] ... *** NODE ... IS NOW LEADER ***` and `Registered ...` lines do appear, alongside warnings such as `Config ... unreadable`, `Dedup: duplicate detected` and kazoo's `Connection dropped`. `DEBUG` lines (for example per-message dedup first-seen) need `LOG_LEVEL=DEBUG` in `.env` and a process restart. The processes' own lines (`INFO ZK ready ...`, `WARNING ZK election failed ...`) are also there, and `curl -s http://${FLOWFIRST_VIP}:8080/zk/health | jq .pipeline_health` shows leaders and registrations.

#### B. Message phrasebook — what each line means

**Server log (`/var/log/zookeeper/*.out`, node1–node3):**

| Log keyword (grep for) | Meaning | Typical scenarios |
|---|---|---|
| `LOOKING`, `New election`, `Notification:` | This server has no leader and is voting. Short bursts are normal at start-up and after a peer loss | 16–19, 22, 34 |
| `LEADING`, `LEADER ELECTION TOOK` | This server won the ensemble election (`TOOK - N MS` is how long it took) | 16–19, 34 |
| `FOLLOWING`, `FOLLOWING - LEADER ELECTION TOOK` | This server joined an elected leader as a follower | 16–19, 34 |
| `Have quorum of supporters` | Leader has a majority (2 of 3) and starts serving | 16–19 |
| `Getting a diff from the leader`, `DIFF` / `SNAP` / `TRUNC` | Follower catching up after a restart: DIFF = missing transactions only, SNAP = full snapshot, TRUNC = discard a divergent tail | 16, 18, 19 |
| `Cannot open channel to N at election address` | Server `N` (myid) is unreachable on 3888 (down, rebooting, or firewall/partition) | 16–18, 22 |
| `Exception when following the leader`, `shutdown called`, `Shutting down` | Follower lost the leader and goes back to `LOOKING` | 17, 20–22 |
| `Established session 0x... with negotiated timeout` | A client (pipeline process, `zkCli.sh`) opened a session; the client IP shows which node | 34–38 |
| `Expiring session 0x..., timeout of ...ms exceeded` | A client went silent past its session timeout. **All its ephemeral znodes are deleted**: the process's registry entry and its election node, so another node takes over leadership | 36, 38, 16–22 |
| `Closed socket connection`, `Closing session 0x...` | Clean client disconnect (process stopped); its ephemerals are deleted immediately | 11, 12, 38 |
| `Too many connections from` | A client IP exceeded `maxClientCnxns=120` | heavy load, 39 |
| `fsync-ing the write ahead log ... took ...ms` | Slow disk on this server — causes client timeouts and elections | 20, 21, load |
| `Snapshotting:`, `Purge task completed` | Routine snapshot / `autopurge` housekeeping — not an error | any |
| `Address already in use`, `myid file is missing`, `Invalid configuration` | Start-up failure — see the Phase 1b troubleshooting section | install |
| `ERROR`, `FATAL` | Read the lines just above for the cause | any |

**Client side (journal of `flowfirst-processN`):**

| Log keyword (grep for) | Meaning | Typical scenarios |
|---|---|---|
| `Connection dropped`, `socket connection broken` | kazoo lost its server; it reconnects to another one in `ZK_HOSTS` and keeps the same session if it does so within the timeout | 16–22 |
| `expired`, `EXPIRED_SESSION`, `Session has expired` | Reconnect came too late — ephemerals are gone; the process re-registers (`register_process` falls back to `set` on `NodeExistsError`) and re-enters election | 20–22, 36 |
| `Config ... unreadable`, `Config watch parse error` | Bad value in `/flowfirst/config/*`; the process keeps the default or the previous value | 35 |
| `Dedup: duplicate detected` | DB Updater (Process 4) skipped a redelivered message because its dedup znode still exists | 37 |
| `ZK unavailable ... degraded mode`, `ZK election failed ... single-node mode` | Process started without ZooKeeper; check the ensemble before trusting leadership | 19, 34 |

#### C. Build a timeline on one server

```bash
# On the server being investigated (e.g. node3 after Scenario 16)
sudo grep -h -E "LOOKING|LEADING|FOLLOWING|ELECTION TOOK|quorum of supporters|DIFF|SNAP|TRUNC|Cannot open channel|Exception when following|Expiring session|ERROR|FATAL" \
  /var/log/zookeeper/zookeeper-*.out | tail -60
```

Read top to bottom: peer loss (`Cannot open channel` / `Exception when following`) → `LOOKING` → `LEADING` or `FOLLOWING` with `ELECTION TOOK` → `Expiring session` for clients that were on the lost node → the restarted peer catches up via `DIFF`/`SNAP`.

#### D. Correlate the same event across all three servers

```bash
# From any of node1–node3 (uses ssh to each node, including itself)
SINCE="$(date -d '-15 min' '+%Y-%m-%d %H:%M')"
for n in ${NODE1_IP} ${NODE2_IP} ${NODE3_IP}; do
  echo "===== ${n} ====="
  ssh "${n}" "sudo sh -c 'cat /var/log/zookeeper/zookeeper-*.out' \
    | awk -v s='${SINCE}' '\$0 >= s' \
    | grep -E 'LOOKING|LEADING|FOLLOWING|ELECTION TOOK|Cannot open channel|Expiring session|ERROR'"
done
```

Lines start with a `YYYY-MM-DD HH:MM:SS,mmm [myid:N]` prefix; the `awk` time filter compares against the timestamp, and `myid` tells you which server wrote the line. Expected for Scenario 16 (reboot node3 while it is a follower): node1 and node2 show `Cannot open channel to 3`, then `Expiring session` for node3's pipeline clients, and stay `LEADING`/`FOLLOWING` without a new election; node3, after the reboot, shows `LOOKING` → `FOLLOWING` → `DIFF`. If node3 was the ZooKeeper **leader**, node1 and node2 both show `LOOKING` and one of them then shows `LEADING`.

> The ZooKeeper **ensemble** leader (this log) is unrelated to the **pipeline** leader for each process (the kazoo `Election` under `/flowfirst/election/*`). A pipeline leader changes only when its session expires or closes, which appears in the server log as `Expiring session` / `Closing session` for that client's IP.

#### E. Watch the log live while running another scenario

```bash
# On the server under test, in a second terminal, before starting the other scenario
sudo tail -F /var/log/zookeeper/zookeeper-zookeeper-server-$(hostname).out \
  | grep --line-buffered -E "LOOKING|LEADING|FOLLOWING|ELECTION TOOK|Cannot open channel|Expiring session|Closing session|ERROR"
```

#### F. Temporarily increase verbosity (optional)

ZooKeeper's log level cannot be changed at runtime. Change it one server at a time so the ensemble keeps its quorum:

```bash
# On one of node1–node3 at a time — ZooKeeper 3.9 uses logback
sudo cp /opt/zookeeper/conf/logback.xml /opt/zookeeper/conf/logback.xml.bak
sudo sed -i 's/<root level="INFO">/<root level="DEBUG">/' /opt/zookeeper/conf/logback.xml
sudo systemctl restart zookeeper
echo ruok | nc -w2 127.0.0.1 2181    # expect imok before moving to the next server

# Revert when finished
sudo mv /opt/zookeeper/conf/logback.xml.bak /opt/zookeeper/conf/logback.xml
sudo systemctl restart zookeeper
```

Debug level logs every client request; keep it on only for the duration of the test. If the `sed` changes nothing, open `logback.xml` and change the root logger's level by hand.

---

## HAProxy Log Analysis

### Scenario 48: Reading and Managing HAProxy Logs Across Scenarios

> **Runs on:** the **current VIP holder** (`sudo crm_resource --resource flowfirst-vip --locate`). Pacemaker runs HAProxy on one node at a time (`vip-haproxy-group`), so only that node writes request logs. After a failover, the older lines stay on the previous holder; to see a whole incident, read the log on **both** nodes (the loop in D runs from any of node1–node3 over `ssh`). The `curl` traffic generators can run from any machine that can reach the VIP.

Use this scenario alongside Scenarios 1, 8–9, 15, 17, 20–25, 29 and 40–41 to work out which backend served a request, how long each stage took, and when HAProxy marked a node up or down. Exact wording varies between HAProxy versions, so the commands below grep for stable keywords rather than whole lines.

#### A. Find out where the logs go (and whether they go anywhere)

[`haproxy/haproxy.cfg.template`](haproxy/haproxy.cfg.template) sends logs with `log 127.0.0.1 local2 info`, which is **UDP to 127.0.0.1:514**. HAProxy does not write a log file itself. If `rsyslog` is not listening on UDP 514, the lines are **silently discarded**, and `journalctl -u haproxy` shows only start, stop and reload messages. [`haproxy/setup_haproxy.sh`](haproxy/setup_haproxy.sh) creates `/var/log/haproxy/` but does not configure `rsyslog`.

```bash
# On the VIP holder
sudo grep -n "^\s*log " /etc/haproxy/haproxy.cfg                         # expect: log 127.0.0.1 local2 info
sudo ss -ulnp | grep ':514 '                                              # is rsyslog listening for HAProxy?
sudo grep -rn "local2\|haproxy" /etc/rsyslog.conf /etc/rsyslog.d/ 2>/dev/null
sudo ls -l /var/log/haproxy.log /var/log/haproxy/ 2>/dev/null
sudo journalctl -u haproxy --since "30 min ago" --no-pager | tail -20     # start/stop/reload only
```

If there is no listener on UDP 514 and no rsyslog rule for `local2`, add one. Run this on **each of node1–node3**, because any of them can become the VIP holder:

```bash
sudo tee /etc/rsyslog.d/49-haproxy.conf > /dev/null << 'EOF'
module(load="imudp")
input(type="imudp" address="127.0.0.1" port="514")
if $syslogfacility-text == 'local2' then /var/log/haproxy/haproxy.log
& stop
EOF
sudo systemctl restart rsyslog
sudo ss -ulnp | grep ':514 '
```

The rest of this scenario writes the log path as `HLOG`. Set it once on the VIP holder:

```bash
HLOG=/var/log/haproxy/haproxy.log      # or /var/log/haproxy.log if your package already ships an rsyslog rule
```

Generate a request and find it:

```bash
# From any machine that can reach the VIP
curl -s http://${FLOWFIRST_VIP}:8080/health | jq -r .node

# On the VIP holder
sudo grep flowfirst_api_front ${HLOG} | tail -3
```
```
Oct 12 14:17:31 node1 haproxy[2211]: 192.168.1.50:51234 [12/Oct/2026:14:17:31.123] flowfirst_api_front flowfirst_api_back/node2 0/0/1/5/6 200 345 - - ---- 3/3/0/0/0 0/0 "GET /health HTTP/1.1"
```

#### B. Anatomy of a log line

HTTP lines (`flowfirst_api_front`) read, left to right: `client:port [accept time] frontend backend/server Tq/Tw/Tc/Tr/Tt status bytes - - termination-flags conns srv_q/be_q "request"`.

| Field | Meaning | What a large value tells you |
|---|---|---|
| `backend/server` | Which backend node served the request (`<NOSRV>` means none was available) | Count per server to prove round-robin (Scenario 1) |
| `Tq` | Time to receive the full request from the client | Slow client or network |
| `Tw` | Time queued in HAProxy waiting for a free backend slot | Backends saturated |
| `Tc` | Time to connect to the backend | Network latency or packet loss to that node (Scenario 20) |
| `Tr` | Time the backend took to answer | Slow API Gateway (Process 1) on that node |
| `Tt` | Total time | Sum of the above plus data transfer |
| `status` | HTTP status; `-1` means no response was received | `503` with `<NOSRV>`: no healthy backend; `502`: backend closed early |
| termination flags (4 characters) | Who ended the session, and in what phase. `----` is a normal end | See the table in C |

TCP lines (`mariadb_galera_front`, `rabbitmq_amqp_front`) are shorter: `client:port [accept time] frontend backend/server Tw/Tc/Tt bytes termination-flags conns srv_q/be_q`. Each line is written **when the connection closes**, so a long-lived AMQP connection appears only after it ends, with `Tt` equal to its whole lifetime.

#### C. Message phrasebook — what each line means

| Log keyword (grep for) | Meaning | Typical scenarios |
|---|---|---|
| `Server flowfirst_api_back/nodeX is DOWN` | Health checks (`GET /health` every 3 s, `fall 2`) failed twice, so about 6 s after the fault. The text after `reason:` says why: `Layer4 connection problem` (port closed or host down), `Layer4 timeout`, `Layer7 wrong status, code: 503` (API Gateway (Process 1) is up but answering with an error) | 8, 11, 12, 16–25 |
| `Server flowfirst_api_back/nodeX is UP` | Two consecutive passed checks (`rise 2`) | 11.2, 12, 16–19 |
| `X active and Y backup servers left` | Servers still in rotation after a state change; `0 active` means the next request gets a 503 | 12 (all processes stopped) |
| `backend flowfirst_api_back has no server available!` | All three backends are down | 12, 19 |
| `Server ... is going DOWN for maintenance`, `is NOLB` | Set through the stats socket or dashboard (`MAINT`, `DRAIN`) | 9 |
| `Proxy flowfirst_api_front started`, `Stopping frontend`, `Stopping backend`, `Proxy ... stopped` | HAProxy started, was reloaded, or was stopped (including by Pacemaker moving the group) | 8, 15, 17 |
| status `503` with `<NOSRV>` in the request line | No backend was selectable when the request arrived | 12, 19 |
| status `504`, flags `sH` | Backend did not answer within `timeout server 1m` | 20, 21 |
| status `502`, flags `SH` | Backend closed the connection before sending a full response (for example API Gateway (Process 1) was stopped mid-request) | 11, 16 |
| flags `cD` / `CD` | Client side timeout (`c`) or abort (`C`) while data was flowing | 20, 21 |
| flags `SC` / `sC` | Backend refused (`SC`) or did not accept (`sC`) the connection | 11, 16–25 |
| flags `----` | Normal end of session | any |
| `Connect() failed ... Connection refused` | Not written to this log; the request line shows the `SC` flags instead | 11 |

The flags are read left to right: the first letter is **who** ended it (`C` client, `S` server, `s` server timeout, `c` client timeout, `P` proxy), the second is **which phase** (`C` connecting, `H` waiting for headers, `D` data, `R` waiting for the request), and the last two describe cookies and persistence (`--` in this setup).

#### D. Build a timeline and compare the nodes

```bash
# Server state changes only, on the VIP holder
sudo grep -E "Server .* is (UP|DOWN)|no server available|NOLB|MAINT|Proxy .* (started|stopped)|Stopping (frontend|backend)" ${HLOG} | tail -40

# Requests per backend server and status (round-robin proof; error-rate check)
sudo grep -o 'flowfirst_api_back/[^ ]* [-0-9/]* [0-9-]*' ${HLOG} \
  | awk '{split($1,a,"/"); n[a[2]" "$3]++} END{for(k in n) print n[k], k}' | sort -k2
```
```
118 node1 200
121 node2 200
120 node3 200
```

Compare both nodes after a failover (Scenarios 8, 15, 17). The loop runs from any of node1–node3:

```bash
SINCE="$(date -d '-15 min' '+%b %e %H:%M')"
for n in ${NODE1_IP} ${NODE2_IP} ${NODE3_IP}; do
  echo "===== ${n} ====="
  ssh "${n}" "sudo test -f ${HLOG} && sudo awk -v s='${SINCE}' '\$0 >= s' ${HLOG} \
    | grep -E 'Server .* is (UP|DOWN)|no server available|Proxy .* (started|stopped)| 50[0-9] '"
done
```

HAProxy's syslog timestamps look like `Oct 12 14:17:31` (no year), which is what the `awk` filter compares against; it works within a single day.

Expected for Scenario 17 (reboot of the VIP holder): the old holder logs `Stopping frontend` / `Proxy ... stopped` (if it shut down cleanly) and nothing after it; the new holder logs `Proxy flowfirst_api_front started` and, since its backends were already healthy, no `is UP` lines unless a backend changed.

> **Noise from the health checks:** API Gateway (Process 1) logs one `[http]` line per HTTP request, including HAProxy's own `GET /health` checks every 3 s per backend. When reading `journalctl -u flowfirst-process1`, filter them out with `grep -v "GET /health"`.

#### E. Ask HAProxy directly (live state, no log needed)

```bash
# On the VIP holder — needs socat (sudo dnf install -y socat); the stats socket is /var/lib/haproxy/stats
echo "show stat" | sudo socat stdio /var/lib/haproxy/stats \
  | cut -d, -f1,2,18,22,23,24 | column -s, -t
# Columns: proxy, server, status, check failures, times marked down, seconds since last change
echo "show info" | sudo socat stdio /var/lib/haproxy/stats | grep -E "Uptime|CurrConns|Idle_pct"
```

A server whose "seconds since last change" is small just flipped state; match that time against `is UP` / `is DOWN` in the log.

#### F. Watch live while running another scenario

```bash
# On the VIP holder, in a second terminal
sudo tail -F ${HLOG} | grep --line-buffered -E "Server .* is (UP|DOWN)|no server available| 50[0-9] |Stopping (frontend|backend)|started"
```

#### G. Manage the log: volume, rotation and retention

HAProxy writes one line per HTTP request and one per closed TCP connection. Health checks themselves are not logged (only state changes are), and `option dontlognull` suppresses empty connections.

```bash
# On the node(s) that have been VIP holder
sudo ls -lh ${HLOG}*
sudo test -f /etc/logrotate.d/haproxy && sudo cat /etc/logrotate.d/haproxy || echo "no logrotate rule for HAProxy"
```

If there is no rotation rule, add one (on each of node1–node3). The path in the first line must match `HLOG`:

```bash
sudo tee /etc/logrotate.d/haproxy-flowfirst > /dev/null << 'EOF'
/var/log/haproxy/haproxy.log {
    daily
    rotate 14
    missingok
    notifempty
    compress
    delaycompress
    postrotate
        /usr/bin/systemctl kill -s HUP rsyslog.service >/dev/null 2>&1 || true
    endscript
}
EOF
sudo logrotate -d /etc/logrotate.d/haproxy-flowfirst     # dry run: prints what it would do
```

Raising the level from `info` to `debug` adds little for request logs and is not recommended. If you want per-health-check lines, add `option log-health-checks` to the `defaults` section of `haproxy/haproxy.cfg.template`, regenerate with `haproxy/setup_haproxy.sh`, validate with `sudo haproxy -c -f /etc/haproxy/haproxy.cfg`, and let Pacemaker reload it. Remove it again afterwards.

---

## Pacemaker & Corosync Log Analysis

### Scenario 49: Reading Pacemaker and Corosync Logs Across Scenarios

> **Runs on:** log files and `journalctl` on **each of node1–node3** (each node logs its own view; **scheduler decisions are logged only on the current DC**, find it with `sudo crm_mon -1 | grep "Current DC"`). `crm_mon`, `crm_resource`, `crm_simulate` and `pcs` on **any cluster member**. Resource-agent lines (`IPaddr2`, systemd start/stop) appear on the node where the action ran. node4 is not in the cluster and has no Pacemaker or Corosync logs.

Use this scenario alongside Scenarios 8, 10–12, 15–26 and 38 to work out why Pacemaker moved a resource, restarted a process, or declared a node lost. Exact wording varies between Pacemaker and Corosync versions, so the commands below grep for stable keywords rather than whole lines.

#### A. Locate the logs and the resource names

```bash
# On node1, node2 or node3
sudo grep -E "^\s*(to_logfile|logfile|to_syslog)" /etc/corosync/corosync.conf   # RHEL/pcs default: /var/log/cluster/corosync.log
sudo ls -l /var/log/pacemaker/pacemaker.log /var/log/cluster/corosync.log

# The same daemons also log to the journal (all Pacemaker daemons run under pacemaker.service)
sudo journalctl -u corosync -u pacemaker -u pcsd --since "30 min ago" --no-pager | tail -30

# Which node is the DC (it writes the scheduler lines)?
sudo crm_mon -1 | grep "Current DC"

# Real resource IDs, as created by pacemaker/configure_multinode_resources.sh
sudo pcs resource status
```

Resource IDs used below: `flowfirst-vip` and `haproxy-res` (grouped as `vip-haproxy-group`), and `flowfirst-p1-res` … `flowfirst-p4-res` with clones `flowfirst-p1-res-clone` … `flowfirst-p4-res-clone`. Always confirm with `pcs resource status` before grepping for an ID.

#### B. Message phrasebook — what each line means

**Pacemaker (`pacemaker-controld`, `-schedulerd`, `-execd`, `-attrd`, `-based` in `pacemaker.log`):**

| Log keyword (grep for) | Meaning | Typical scenarios |
|---|---|---|
| `Node nodeX state is now lost`, `state is now member` | Pacemaker's membership view changed | 16–19, 22–24 |
| `Our peer on the DC (nodeX) is dead`, `Set DC to nodeY` | The DC was lost and a new one was elected; scheduler lines now appear on the new DC | 17, 18, 22 |
| `State transition S_IDLE -> S_POLICY_ENGINE` … `-> S_TRANSITION_ENGINE -> S_IDLE` | One decision cycle (DC only). Every state change triggers one; cycles with no visible cause point to flapping | any |
| `Calculated transition N`, `saving inputs in /var/lib/pacemaker/pengine/pe-input-N.bz2` | The scheduler computed a plan. `(with warnings)` / `(with errors)` mean it found a problem. The saved file can be replayed (see D) | any |
| `Initiating start\|stop\|monitor\|migrate_to operation X on nodeY` | An action was sent to a node | 8, 11, 12, 15 |
| `Result of start operation for X on nodeY: ok` | Action finished. Other results: `not running`, `error`, `Timed Out`, `Not installed` | 8, 11, 12, 16–18 |
| `Result of monitor operation for flowfirst-pN-res on nodeY: not running` | The process is down. The fail count goes up and Pacemaker restarts it | 11, 16, 26 |
| `Unexpected result (error) was recorded for monitor of X on nodeY` | A recorded failure that triggers recovery | 26 |
| `Forcing X away from nodeY after N failures (max=1000000)` | The fail count reached `migration-threshold`. `1000000` is Pacemaker's INFINITY, which is the value in effect because the setup scripts never set `migration-threshold` | 26 |
| `Resource X is active on 2 nodes (attempting recovery)` | A resource that must run once (the VIP, HAProxy) was seen on two nodes — a split-brain symptom | 19, 22 |
| `unclean`, `will be fenced`, `fenc` | Appear only when STONITH is enabled. Here `stonith-enabled=false` (set in `pacemaker/setup_multinode_cluster.sh`), so a lost node is simply assumed dead and **never fenced** | 22–24 |
| `ocf-exit-reason:` | The resource agent's human-readable cause of a failure (for example the NIC given in `VIP_NIC` does not exist) | 8, 17, install |

**Resource agents and systemd resources:**

| Log keyword (grep for) | Meaning | Typical scenarios |
|---|---|---|
| `IPaddr2(flowfirst-vip)`, `ip -f inet addr add`, `gratuitous ARP` | The VIP was added to this node's NIC and neighbours were told | 8, 15, 17 |
| `systemd[1]: Started FlowFirst Process N` / `Stopped ...` | Pacemaker's systemd resource started or stopped the process unit on this node | 11, 12, 16 |
| `Scheduled restart job, restart counter is at N` | **systemd** restarted the process itself (the units use `Restart=always`) before Pacemaker's next 15 s monitor, so Pacemaker may never record a failure | 11, 38 |

**Corosync (`corosync.log` or journal, tags in brackets):**

| Log keyword (grep for) | Meaning | Typical scenarios |
|---|---|---|
| `[KNET  ] link: host: N link: 0 is down`, `rx: host: N link: 0 is up` | Heartbeat link to node `N` (node ID, not name) failed / recovered | 16–25 |
| `[TOTEM ] Token has not been received in N ms`, `A processor failed, forming new configuration` | A node stopped answering; the ring is re-forming | 16–19, 22–24 |
| `[TOTEM ] Retransmit List:` | Packet loss or high latency on the ring | 20, 21 |
| `[QUORUM] Members[N]: 1 2` | The new member list after a change | 16–19, 22–24 |
| `[VOTEQ ] quorum lost, blocking activity`, `quorum regained, resuming activity` | The partition lost / regained majority. With no quorum Pacemaker stops resources on the minority side | 19, 22 |
| `[MAIN  ] Completed service synchronization, ready to provide service.` | Membership change finished | any |

#### C. Build a timeline on one node

```bash
# On the node being investigated
sudo journalctl -u corosync -u pacemaker --since "15 min ago" -o short-iso --no-pager \
  | grep -E "KNET|TOTEM|QUORUM|VOTEQ|state is now|peer on the DC|Set DC|State transition|Calculated transition|Initiating|Result of|Unexpected result|Forcing|active on 2 nodes|unclean|fenc|ocf-exit-reason|error" \
  | tail -80
```

Read top to bottom: `link ... is down` / `Token has not been received` → `Members[N]` (smaller) → `state is now lost` → `State transition` → `Calculated transition` → `Initiating` / `Result of` actions. A resource moves only after the `Calculated transition` line for it.

#### D. Explain a decision: replay the scheduler input

The line `Calculated transition N, saving inputs in .../pe-input-N.bz2` names a file on the **DC that made the decision**. Replay it to see exactly what Pacemaker planned and why:

```bash
# On the DC that logged the transition
sudo ls -t /var/lib/pacemaker/pengine/pe-input-*.bz2 | head -3
sudo crm_simulate -S -x /var/lib/pacemaker/pengine/pe-input-N.bz2 -V     # N from the log line; prints the planned actions
sudo crm_simulate -Ls                                                    # live cluster: allocation scores (why a resource prefers a node)
```

#### E. Failure history and leftover constraints

```bash
# On any cluster member
sudo crm_mon -1Arf                                   # -f shows fail counts, -r inactive resources, -A node attributes
sudo pcs resource failcount show
sudo crm_resource --list-operations --resource flowfirst-p1-res

# Constraints left behind by 'pcs resource ban' / 'move' (cli-ban-*, cli-prefer-*) keep a resource off a node
sudo pcs constraint --full | grep -E "cli-ban|cli-prefer" || echo "no leftover CLI constraints"
sudo pcs resource clear flowfirst-p2-res-clone      # remove a leftover ban (Scenarios 11, 36)
sudo pcs resource cleanup flowfirst-p1-res-clone    # reset fail counts after the cause is fixed
```

#### F. Correlate the same event across all three nodes

```bash
# From any of node1–node3 (uses ssh to each node, including itself); sorts all nodes into one timeline
for n in ${NODE1_IP} ${NODE2_IP} ${NODE3_IP}; do
  ssh "${n}" "sudo journalctl -u corosync -u pacemaker --since '15 min ago' -o short-iso --no-pager \
    | grep -E 'link: host|Token has not|Members\[|quorum (lost|regained)|state is now|peer on the DC|Set DC|Calculated transition|Result of (start|stop)|Forcing|active on 2 nodes' \
    | sed 's/^/${n} /'"
done | sort -k2,2
```

This relies on synchronised clocks (chrony, Phase 0a) and the same time zone on every node. Expected for Scenario 16 (reboot node3): on node1 and node2, `link: host: 3 ... is down` → `Members[2]` → `Node node3 state is now lost` → one `Calculated transition` on the DC; later `link: host: 3 ... is up` → `Members[3]` → `Result of start operation for flowfirst-pN-res on node3: ok` for each clone. On node3 itself: boot, `Members[3]`, then the starts.

#### G. Watch live and collect a support bundle

```bash
# On the node under test, in a second terminal
sudo journalctl -u corosync -u pacemaker -f -o short-iso --no-pager \
  | grep --line-buffered -E "link: host|Token has not|Members\[|quorum (lost|regained)|state is now|Calculated transition|Result of|Forcing|ocf-exit-reason|error"

# After an incident, collect logs, CIB and scheduler inputs from every node into one archive
sudo crm_report --from "2026-10-12 14:00:00" --to "2026-10-12 14:30:00" /tmp/flowfirst-incident
```

`crm_report` connects to the other nodes over `ssh`, so run it from a node that can `ssh` to the others without a password. Pacemaker rotates `pacemaker.log` itself (check `ls /etc/logrotate.d/`), but the `pe-input-*.bz2` files accumulate in `/var/lib/pacemaker/pengine/` until Pacemaker prunes them; check `du -sh /var/lib/pacemaker/pengine/` after long flapping tests such as Scenario 26.

---

## Swift Log Analysis

### Scenario 50: Reading Swift Logs and Detecting Missing Objects

> **Runs on:** client-side logs (`journalctl -u flowfirst-process4`) on the **current DB Updater (Process 4) leader node** (find it with `curl -s http://${FLOWFIRST_VIP}:8080/zk/health | jq .pipeline_health.process4`); for Flow 3, Remote Worker (Process 5) only forwards the message, so Swift errors still appear on the DB Updater (Process 4) leader. The `curl` and `mariadb` reconciliation checks in D run from **any machine that can reach the VIP** (the `mariadb` client is installed on node1–node3). Swift **server** logs (proxy, account, container and object servers, installed by Phase 2b) are in the journal of **each of node1–node3**: a request through the VIP is handled by whichever node HAProxy chose, and the object itself is written to all three nodes, so check all three.

Use this scenario alongside Scenarios 2, 37 and 44, and any scenario that stops RabbitMQ, MariaDB or the network, to find out whether DB Updater (Process 4) stored each payload in Swift, and why not when it did not. Swift failures are **non-fatal by design**: DB Updater (Process 4) prints a note, still writes the MariaDB row and still acknowledges the message, so a Swift outage never shows up as a stuck queue.

#### A. Find where Swift is and whether it is enabled

```bash
# On the node you are investigating
grep -E "^SWIFT_" /opt/flowfirst/.env            # SWIFT_ENABLED, SWIFT_AUTH_URL, SWIFT_USER, SWIFT_CONTAINER (the key is secret; do not paste it into tickets)

# Ask API Gateway (Process 1) (any node, via the VIP) — this runs get_swift_status() from the node that answers
curl -s http://${FLOWFIRST_VIP}:8080/swift/status | jq .
```
```json
{ "enabled": true, "status": "connected", "container": "flowfirst_messages", "account_objects": "412", "account_bytes": "1048576" }
```

`"status": "error"` carries an `error` field with the underlying exception text. `"enabled": false` means `SWIFT_ENABLED` is false on the node that answered, and every object write is skipped. Because HAProxy round-robins `/swift/status`, run it a few times to see all three nodes.

> **Check the URL, it is easy to get wrong:** `SWIFT_AUTH_URL` must be `http://${FLOWFIRST_VIP}:8090/auth/v1.0` (HAProxy's Swift frontend, `SWIFT_LB_PORT`), the same on every node including node4. Port `8080` on the VIP is HAProxy in front of **API Gateway (Process 1)**, which has no `/auth/v1.0` route, and a node-local proxy on `:8888` would bypass the load balancer. Confirm that `curl -sI -H "X-Auth-User: ${SWIFT_USER}" -H "X-Auth-Key: <key>" "${SWIFT_AUTH_URL}"` returns `X-Auth-Token` and `X-Storage-Url` headers before trusting any other result here.

#### B. Message phrasebook — what each line means

**Client side (`journalctl -u flowfirst-process4`, produced by `src/application/process4.py` and `src/swift/swift_storage.py`):**

The `Auth GET failed`, `Object PUT failed`, `Container PUT failed` and `Account HEAD failed` texts below are **exception messages from `python-swiftclient`**. They are not separate log lines: they appear inside the `Swift store note: ...` and `Failed to put object ... : ...` lines.

| Log text (grep for) | Meaning | Typical scenarios |
|---|---|---|
| `Stored in Swift object storage: flowN/<message_id>.json` | The object write succeeded; the name is `flow<flow>/<message_id>.json` | 2, 44 |
| `Swift store note: Swift storage is disabled` | `SWIFT_ENABLED` is false on this node. Printed for **every** message; nothing is stored | install |
| `Swift store note: <error text>` | The write failed (see the error rows below). The message was still acknowledged and the MariaDB row was written | any Swift outage |
| `Failed to put object '<name>' in container '<c>': <error>` | The same failure, logged by `src/swift/swift_storage.py` with its logger (an ERROR line; `Could not ensure container` is a WARNING) | any Swift outage |
| `Could not ensure container '<c>' exists: <error>` | Container create/check failed, usually auth or connectivity | install |
| `Auth GET failed: <url> 401 Unauthorized` | Wrong `SWIFT_USER` / `SWIFT_KEY` for that node's `.env` | install |
| `Auth GET failed: <url> 404 Not Found` | `SWIFT_AUTH_URL` points at something that is not a Swift auth endpoint (for example HAProxy to API Gateway (Process 1); see the note in A) | install |
| `Object PUT failed: <url> 503 Service Unavailable` | Swift is up but unhealthy (disk, replication or backend failure) | storage outage |
| `Object PUT failed: ... 507 Insufficient Storage` | A Swift device is full or unmounted | storage outage |
| `Container PUT failed ... 404` / `Account HEAD failed ... 404` | The account is missing or not auto-created for that user | install |
| `[Errno 111] Connection refused`, `Max retries exceeded with url`, `Read timed out`, `Name or service not known` | Swift is not reachable from this node (service down, wrong host, firewall, DNS, partition) | 22–25 |
| `DUPLICATE via ZK dedup — skipping insert` | The message was already handled, so **no** Swift write is attempted for the redelivery | 37 |

**REST API (`GET /swift/...` on API Gateway (Process 1)):**

| Response | Meaning |
|---|---|
| `GET /swift/status` returns `"status": "connected"` | Account and container are reachable from the answering node |
| `GET /swift/status` returns `"status": "error"` with `error` | Auth or connectivity failed from the answering node |
| `GET /swift/object/<name>` returns `404` with `details.error` containing `ClientException 404` | No such object (never written, or written under a different name) |
| `GET /swift/objects` returns `502` with `error` | Listing failed (same causes as above) |

**Server side (Swift daemons on node1–node3, `journalctl` on each node):**

| Where | What to look for |
|---|---|
| `journalctl -t proxy-server -t object-server -t container-server -t account-server` | Request lines from each daemon: status code, `tx<id>` transaction id and the object path. The Swift daemons log to **syslog** (facility `LOCAL0` unless changed), which `rsyslog` and the journal both receive |
| `journalctl -u openstack-swift-proxy -u openstack-swift-object -u openstack-swift-object-replicator -u rsyncd -u memcached` | Service start/stop, crashes and tracebacks that are not request lines. Replicator output shows whether a returned node is catching up |
| `/var/log/rsyncd.log` | Replication transfers between nodes (written by `rsyncd`, configured in `/etc/rsyncd.conf`) |
| `sudo swift-ring-builder /etc/swift/object.builder` and `md5sum /etc/swift/*.ring.gz` | Ring layout, and whether the ring files are identical on all three nodes (a mismatch causes odd 404/503 results) |
| `401` / `403` on the auth line | Credential or ACL problem |
| `503` from `proxy-server` with `Timeout` / `ConnectionTimeout` | The proxy could not reach an object server in time |
| `507` | A device is unmounted or full (`swift-recon -d` shows disk usage, on a node with the recon middleware enabled) |

To correlate a client error with the server log, match the **time** and the **object path** (`/v1/AUTH_<account>/flowfirst_messages/flow1/<message_id>.json`); the client text does not include the server's `tx` id.

#### C. Build a timeline on the DB Updater (Process 4) leader

```bash
# On the DB Updater (Process 4) leader node
sudo journalctl -u flowfirst-process4 --since "30 min ago" --no-pager \
  | grep -E "Swift|swift|Auth GET|Object PUT|Container (PUT|HEAD)|Account HEAD|Errno|Max retries" | tail -60
```

Read top to bottom: `Stored in Swift object storage` lines stop → first `Swift store note:` / `Failed to put object` → (fault cleared) → `Stored in Swift object storage` resumes. The messages between the first failure and the first success are the ones **missing from Swift** (see D).

> **Output delay:** the unit files set `Environment=PYTHONUNBUFFERED=1`, so log output reaches the journal immediately. On a node installed **before** that line was added, the lines from `src/application/process4.py` can appear in bursts (Python block-buffers stdout when it is not a terminal). Re-run `sudo ./systemd/install_services.sh /opt/flowfirst` (add `--remote` on node4), then `sudo systemctl daemon-reload` and restart the processes (`sudo pcs resource restart flowfirst-p4-res-clone` for the clones). Check with `systemctl show flowfirst-process4 -p Environment`.

#### D. Reconcile MariaDB and Swift (find objects that were never stored)

```bash
# From any machine with a mariadb client that can reach the VIP
# 1. Row count per flow in MariaDB
mariadb -h ${FLOWFIRST_VIP} -u ${MARIADB_USER} -p"${MARIADB_PASSWORD}" ${MARIADB_DB} \
  -e "SELECT flow_id, COUNT(*) AS rows_in_db FROM processed_messages GROUP BY flow_id ORDER BY flow_id;"

# 2. Object count per flow in Swift (listing is capped by &limit; Swift itself caps one listing at 10000)
for f in 1 2 3; do
  echo -n "flow${f} objects in Swift: "
  curl -s "http://${FLOWFIRST_VIP}:8080/swift/objects?prefix=flow${f}/&limit=10000" | jq -r .count
done
```

The two numbers should match. Rows without objects were written while Swift was failing. Name them, comparing the 20 newest rows for Flow 1 as an example:

```bash
for id in $(mariadb -h ${FLOWFIRST_VIP} -u ${MARIADB_USER} -p"${MARIADB_PASSWORD}" ${MARIADB_DB} -N \
    -e "SELECT message_id FROM processed_messages WHERE flow_id = 1 ORDER BY id DESC LIMIT 20;"); do
  code=$(curl -s -o /dev/null -w '%{http_code}' "http://${FLOWFIRST_VIP}:8080/swift/object/flow1/${id}.json")
  echo "${code} ${id}"
done | sort | uniq -c -w3
```

`200` means the object exists; `404` means it is missing. **There is no automatic back-fill:** `put_message_object()` runs only once per delivery, and a redelivered message is skipped by the ZooKeeper dedup barrier. To restore a missing object, republish the stored payload from MariaDB (`raw_payload` column) into Swift by hand, or accept the gap.

Rows can legitimately exceed objects if `SWIFT_ENABLED=false` was set on some nodes (each such node skips every write; see the `disabled` note in B), so check `/swift/status` on every node before blaming an outage.

#### E. Watch live while running another scenario

```bash
# On the DB Updater (Process 4) leader node, in a second terminal, then run Scenario 2 or 44
sudo journalctl -u flowfirst-process4 -f --no-pager | grep --line-buffered -E "Swift|Auth GET|Object PUT|Errno|Max retries"
```

#### F. Controlled failure test (optional)

This shows the failure path without touching Swift itself. Run it from **any of node1–node3**. It changes `.env` on **all three** nodes, because the DB Updater (Process 4) leader may be re-elected on a different node when the clones restart:

```bash
# 1. Disable Swift on every node (the units read /opt/flowfirst/.env on start), then restart the P4 clone
for n in ${NODE1_IP} ${NODE2_IP} ${NODE3_IP}; do
  ssh "${n}" "sudo sed -i 's/^SWIFT_ENABLED=.*/SWIFT_ENABLED=false/' /opt/flowfirst/.env"
done
sudo pcs resource restart flowfirst-p4-res-clone

# 2. Send one Flow 1 message, then look for the note on the current DB Updater (Process 4) leader node
curl -s -X POST http://${FLOWFIRST_VIP}:8080/api/flow1 -H "Content-Type: application/json" \
  -d '{"item_id": 9501, "initial_data": "swift-disabled-test"}' | jq -r .payload.message_id
sudo journalctl -u flowfirst-process4 --since "2 minutes ago" --no-pager | grep "Swift store note"

# 3. Restore on every node and restart the clone again
for n in ${NODE1_IP} ${NODE2_IP} ${NODE3_IP}; do
  ssh "${n}" "sudo sed -i 's/^SWIFT_ENABLED=.*/SWIFT_ENABLED=true/' /opt/flowfirst/.env"
done
sudo pcs resource restart flowfirst-p4-res-clone
```

Item 9501 now exists in MariaDB but not in Swift; use D to confirm. There is no back-fill, so that gap remains.

---

## Linux System Log Analysis

### Scenario 51: Reading `/var/log/messages` and the systemd Journal Across Scenarios

> **Runs on:** **each of node1–node3, and node4** for its own host-level events (reboots, OOM kills, SELinux denials, NIC changes, time steps). System logs are strictly **per host**: a reboot, kernel message or `tc`/`iptables` change appears only on the node where it happened. The cross-node loop in D runs from any of node1–node3 over `ssh` (add `${NODE4_IP}` if node4 is reachable from there).

Use this scenario alongside Scenarios 16–26 (reboots and network faults), and whenever a service stops without an obvious cause, to work out what the operating system itself did: why a node rebooted, whether the kernel killed a process, which firewall rule dropped traffic, and whether SELinux denied an action. Exact wording varies between kernel, systemd and RHEL minor versions, so the commands below grep for stable keywords rather than whole lines.

#### A. Know where each log lives

| Source | Where to read it | What it holds |
|---|---|---|
| systemd journal | `journalctl` | Everything: kernel, systemd, every service's stdout/stderr (including `flowfirst-processN`, MariaDB, RabbitMQ, ZooKeeper, HAProxy, Corosync, Pacemaker). For `flowfirst-processN` this is the Python `logging` output (`INFO [Flow 1] ...`, `WARNING ZK unavailable ...`), see the note below. The same records are also written to `/var/log/flowfirst/processN.log` (Scenario 60) |
| `/var/log/messages` | `sudo less /var/log/messages` | rsyslog's copy of the journal's general messages (kernel, systemd, sshd, chronyd, NetworkManager, firewalld, most daemons). Authentication lines go to `/var/log/secure` and cron to `/var/log/cron` instead |
| `/var/log/audit/audit.log` | `sudo ausearch ...` | SELinux denials (`type=AVC`) and audit events, written by `auditd`, **not** by rsyslog |
| `dmesg` / `journalctl -k` | `sudo dmesg -T` | Kernel ring buffer: NIC link changes, OOM killer, disk errors, `iptables` LOG targets |
| Per-service files | See Scenarios 45–50 | `/var/log/mariadb/`, `/var/log/rabbitmq/`, `/var/log/zookeeper/`, `/var/log/pacemaker/`, `/var/log/cluster/` |

**FlowFirst process output in the journal and in `/var/log/messages`.** Each process unit sets `SyslogIdentifier=flowfirst-pN` (`flowfirst-p1` … `flowfirst-p5`; Remote Worker (Process 5) runs on node4). A node installed before Remote Worker (Process 5)'s unit had this line tags its lines `python`: re-run `systemd/install_services.sh /opt/flowfirst --remote` and restart the unit, or use `-u flowfirst-process5`. `gt.configure_logging()` sends every FlowFirst logger (the five processes, `gt`, `zk` and `swift_storage`) to stdout, so the lines go into the journal, and from there into `/var/log/messages` via rsyslog. It also writes them to a rotating file per process (Scenario 60). Three consequences:

- **Filter by identifier or unit.** `sudo journalctl -t flowfirst-p4 --since "30 min ago"` (or `-u flowfirst-process4`) works on the journal; in `/var/log/messages` grep for `flowfirst-p4\[`.
- **Severity is not preserved.** journald stores every stdout line as priority `info`, so `journalctl -p warning` does **not** list FlowFirst `WARNING`/`ERROR` lines. Search for the level prefix instead: `journalctl -u flowfirst-process4 --no-pager | grep -E "(WARNING|ERROR) "`. The journal line carries only a level prefix and the message (no timestamp or logger name; the file in `/var/log/flowfirst/` has both). The prefix comes from `LOG_LEVEL` (default `INFO`; set `DEBUG` in `.env` and restart the processes for more, `WARNING` for less).
- **Multi-line records are split in the text file.** The 60-second `Throughput Report` is one multi-line message. `journalctl` shows it as one entry, while rsyslog writes control characters escaped (`#012` instead of a newline) onto a single long line in `/var/log/messages`. Use `journalctl` for those reports.

`/var/log/messages` and the journal overlap, but they are **not identical**: `/var/log/messages` is a text file that `logrotate` rotates weekly, and the journal can be size-limited or discarded at reboot. When a line is missing from one, check the other.

```bash
# On any node: is the journal kept across reboots? This matters for every reboot scenario (16-19).
sudo ls -ld /var/log/journal 2>/dev/null || echo "NO /var/log/journal: the journal is VOLATILE and is lost at every reboot"
sudo journalctl --list-boots | tail -5          # one line per boot still on disk; a single line after a reboot means history was lost
sudo journalctl --disk-usage
sudo ls -lh /var/log/messages*                  # current file and rotated copies
sudo systemctl is-active rsyslog auditd chronyd
```

If `/var/log/journal` does not exist, make the journal persistent **on each node** so that reboot scenarios leave evidence behind:

```bash
sudo mkdir -p /var/log/journal
sudo systemd-tmpfiles --create --prefix /var/log/journal
sudo systemctl restart systemd-journald
sudo journalctl --flush
sudo journalctl --list-boots | tail -2          # survives the next reboot
```

#### B. Message phrasebook — what each line means

| Log keyword (grep for) | Meaning | Typical scenarios |
|---|---|---|
| `-- Boot` / `journalctl --list-boots` entries, `systemd[1]: Started ...` after a gap, `Linux version ... (mockbuild` | The host booted. Lines before the gap are the previous boot | 16–19 |
| `Reached target Shutdown`, `systemd-shutdown`, `reboot: Restarting system` | A **clean** shutdown or reboot. Without these lines before the boot gap, the host lost power or crashed | 16–19 |
| `Out of memory: Killed process N (name)`, `oom-kill:`, `invoked oom-killer` | The kernel killed a process for lack of RAM. The victim is named in the line (often `java` for ZooKeeper, `mariadbd`, `beam.smp` for RabbitMQ, or a `python` pipeline process) | heavy load, 40 |
| `systemd[1]: <unit>.service: Main process exited, code=killed, status=9/KILL` | A unit was SIGKILLed (the OOM killer or `kill -9`); check for the OOM line just above | 38, 42 |
| `<unit>.service: Failed with result 'exit-code'` / `'timeout'` / `'signal'` | The unit stopped abnormally. `Scheduled restart job, restart counter is at N` follows when `Restart=` applies (the FlowFirst units use `Restart=always`) | 11, 38 |
| `flowfirst-pN[pid]: ERROR [gt] Greenthread '<name>' raised: ...` followed by `INFO [gt] Restarting '<name>' in 2s...` | A pipeline consumer crashed and restarted itself (Scenario 42). Repeats mean the cause is still there (RabbitMQ, MariaDB or ZooKeeper unreachable). The lines come from the process, not from systemd, so `Failed with result` is **not** logged for them | 22–25, 42 |
| `<unit>.service: Start request repeated too quickly`, `Failed with result 'start-limit-hit'` | systemd gave up restarting a crashing unit; clear with `sudo systemctl reset-failed <unit>` | 38 |
| `NetworkManager[...]: <info> device (ens3): state change`, `kernel: ... NIC Link is Down` / `Up` | A NIC link changed (cable, hypervisor, `ip link set down`). VIP moves, Corosync `link is down` and Galera suspicion usually follow within seconds | 16–25 |
| `kernel: IN=... OUT=... SRC=... DST=... PROTO=... DPT=...` | A packet matched an `iptables ... -j LOG` rule. The `DPT` shows which port. The README's `DROP` rules do **not** log; add a LOG rule before them to see drops (see F) | 22–25 |
| `firewalld[...]: ... ERROR`, `COMMAND_FAILED` | A `firewall-cmd` change failed (wrong zone, missing service) | install |
| `avc:  denied  { ... } for  pid=N comm="name" ... scontext=... tcontext=...` | **SELinux blocked an action.** `comm` is the process; `{ name_bind }` is a port, `{ read }`/`{ write }` a file, `{ connectto }` a socket. See D | install, 8, 15 |
| `setroubleshoot: SELinux is preventing` | The same denial, with a suggested fix (needs `setroubleshoot-server`) | install |
| `chronyd[...]: System clock wrong by N seconds`, `Selected source`, `Can't synchronise: no selectable sources` | A time step or a lost time source. Large steps break RabbitMQ/Erlang joins, Galera and Pacemaker timestamps | 16–19, install |
| `sshd[...]: Failed password`, `Accepted publickey for ... from <ip>` (in `/var/log/secure`) | Who logged in, from where; failed lines mean `ssh` between nodes (used by several scenarios and `crm_report`) is not set up | 36, 49 |
| `sudo[...]: ... COMMAND=...` (in `/var/log/secure`) | Who ran which privileged command, when — useful to confirm which operator action preceded an incident | any |
| `EXT4-fs error`, `XFS ... metadata I/O error`, `I/O error, dev`, `blk_update_request` | Disk problems. MariaDB, RabbitMQ and ZooKeeper stall or crash | storage outage |
| `Remounting filesystem read-only`, `No space left on device` | Disk full or failed. Check `df -h /var /var/lib` | long tests, 26 |
| `pam_unix(sudo:session)`, `systemd-logind` | Session and login housekeeping — routine | any |

#### C. Build a timeline for one boot, one unit, or one time window

```bash
# Everything at warning level or worse in the current boot
sudo journalctl -b -p warning --no-pager | tail -60

# The previous boot (what happened just before a reboot or crash?). -b -1 needs a persistent journal (see A).
sudo journalctl -b -1 -p warning --no-pager | tail -60
sudo journalctl -b -1 --no-pager | tail -30             # the last lines before the node went down

# One unit, with ISO timestamps
sudo journalctl -u mariadb -u rabbitmq-server -u zookeeper --since "30 min ago" -o short-iso --no-pager

# A time window across all units, kernel included
sudo journalctl --since "2026-10-12 14:00:00" --until "2026-10-12 14:30:00" -o short-iso --no-pager \
  | grep -E -i "Out of memory|oom-kill|Main process exited|Failed with result|Link is|state change|avc: +denied|System clock|error|fail" | tail -80

# Kernel only
sudo journalctl -k --since "30 min ago" --no-pager

# FlowFirst pipeline processes only: warnings and errors by prefix (journald stores them as priority info, so -p cannot filter them)
sudo journalctl -t flowfirst-p1 -t flowfirst-p2 -t flowfirst-p3 -t flowfirst-p4 -t flowfirst-p5 --since "30 min ago" --no-pager \
  | grep -E "(WARNING|ERROR|CRITICAL) "
```

Read a reboot from the bottom of the old boot (`journalctl -b -1`): clean shutdowns end with `Reached target Shutdown` and `systemd-shutdown`; a crash or power loss ends abruptly with no shutdown lines. On a node you rebooted with `sudo reboot`, expect the clean ending.

```bash
# Search /var/log/messages and its rotated copies, including compressed ones
sudo grep -h -E "Out of memory|oom-kill|Main process exited|Failed with result" /var/log/messages* 2>/dev/null | tail -20
sudo zgrep -h "Out of memory" /var/log/messages-*.gz 2>/dev/null | tail -5
```

#### D. SELinux denials in detail

RHEL 9 runs SELinux in **Enforcing** mode, and a denial usually looks like an application bug (a service that starts but cannot bind a port, read a file or reach a socket).

```bash
sudo getenforce                                              # expect: Enforcing
sudo ausearch -m AVC,USER_AVC -ts recent 2>/dev/null | tail -30
sudo ausearch -m AVC -ts today -c haproxy 2>/dev/null | head -20     # limit to one command name (comm=)

# Explain one denial in plain language
sudo ausearch -m AVC -ts recent 2>/dev/null | audit2why | head -30
```

If the denial is for a FlowFirst component, first check the booleans and file contexts the install scripts set (`haproxy_connect_any`, `daemons_enable_cluster_mode`, `restorecon -R` on `/var/lib/haproxy` and `/var/log/haproxy`). Only as a last resort build a local policy module with `audit2allow -M <name>` (see the HAProxy troubleshooting section in Phase 3). **Do not** run `setenforce 0` as a fix: it hides the evidence and does not survive the next reboot or `restorecon`.

#### E. Correlate the same event across hosts

```bash
# From any of node1–node3 (uses ssh to each node, including itself); one merged timeline
for n in ${NODE1_IP} ${NODE2_IP} ${NODE3_IP}; do
  ssh "${n}" "sudo journalctl --since '15 min ago' -o short-iso --no-pager \
    | grep -E 'Reached target Shutdown|-- Boot|Out of memory|oom-kill|Main process exited|Failed with result|Link is (Up|Down)|NIC Link|System clock wrong|avc: +denied' \
    | sed 's/^/${n} /'"
done | sort -k2,2
```

This relies on synchronised clocks (chrony, Phase 0a) and the same time zone on every node. Expected for Scenario 16 (reboot node3): node3 shows `Reached target Shutdown`, then (in the **next** boot, so read it after the node is back) `-- Boot` lines and a `chronyd ... Selected source`; node1 and node2 show no OS-level event at all, which proves the others did not restart.

#### F. Watch live, and add temporary packet logging

```bash
# In a second terminal, before running a fault scenario: errors, OOM kills, link changes, denials
sudo journalctl -f -o short-iso --no-pager \
  | grep --line-buffered -E "Out of memory|oom-kill|Main process exited|Failed with result|Link is|state change|avc: +denied|System clock wrong|I/O error"
```

To see **what an `iptables` DROP rule is dropping** during Scenarios 22–25, insert a LOG rule just above the DROP rule, then remove it afterwards (run on the node that has the rule):

```bash
# Example for Scenario 24 (Galera port 4567 blocked); put the LOG rule before the DROP rule
sudo iptables -I INPUT 1 -p tcp --dport 4567 -m limit --limit 5/second -j LOG --log-prefix "FF-DROP-4567: "
sudo journalctl -k -f --no-pager | grep --line-buffered "FF-DROP-4567"

# Remove it when finished
sudo iptables -D INPUT -p tcp --dport 4567 -m limit --limit 5/second -j LOG --log-prefix "FF-DROP-4567: "
```

The `limit` match keeps a busy port from flooding the journal.

#### G. Manage the logs: size, retention and rotation

```bash
# Journal size and retention (applies per node)
sudo journalctl --disk-usage
sudo journalctl --vacuum-time=14d          # keep two weeks
sudo journalctl --vacuum-size=500M         # or cap the size
sudo journalctl --verify                   # check the journal files for corruption

# Make the limits permanent (journald drop-in, on each node)
sudo mkdir -p /etc/systemd/journald.conf.d
sudo tee /etc/systemd/journald.conf.d/flowfirst.conf > /dev/null << 'EOF'
[Journal]
Storage=persistent
SystemMaxUse=1G
MaxRetentionSec=14day
RateLimitBurst=100000
EOF
sudo systemctl restart systemd-journald
```

The `RateLimitBurst` line matters for FlowFirst: journald's default is 10000 messages per 30 s per service, so a burst of process output (Scenario 40 or 43 at high volume) can be silently dropped. The drop-in raises it to 100000. A dropped-message notice looks like `Suppressed N messages from flowfirst-process2.service`; if you still see it, set `RateLimitIntervalSec=0` to disable the limit. The processes also log `INFO` lines (greenthread starts and restarts, RabbitMQ and ZooKeeper connects, leader changes), which adds to the volume during reconnect storms; `LOG_LEVEL=WARNING` in `.env` removes them.

```bash
# Look for suppressed lines after a load test
sudo journalctl --since "1 hour ago" --no-pager | grep -E "Suppressed [0-9]+ messages"

# rsyslog / logrotate for /var/log/messages
sudo cat /etc/logrotate.d/syslog              # which files are rotated, how often
sudo logrotate -d /etc/logrotate.conf 2>&1 | grep -A3 "/var/log/messages"      # dry run
sudo du -sh /var/log /var/log/journal          # who is using the space
```

---

## Swift Cluster Scenarios

These scenarios exercise the Swift cluster installed by Phase 2b. They assume `SWIFT_ENABLED=true`, Swift running on node1–node3 and HAProxy serving `swift_front` on the VIP. Each one can leave the cluster changed, so the last step of every scenario restores it. Scenario 50 explains how to read the client-side symptoms.

Set these once in each shell (`/opt/flowfirst/.env` provides the rest):

```bash
cd /opt/flowfirst && set -a && source .env && set +a
export SWIFT_VIP_URL="http://${FLOWFIRST_VIP}:${SWIFT_LB_PORT:-8090}"
# Get a token through the VIP (tempauth, auth v1.0). Tokens live 24 h; re-run this block after that or after Scenario 56.
TOKEN=$(curl -s -D - -o /dev/null -H "X-Storage-User: ${SWIFT_USER}" -H "X-Storage-Pass: ${SWIFT_KEY}" \
  "${SWIFT_VIP_URL}/auth/v1.0" | tr -d '\r' | awk -F': ' 'tolower($1)=="x-auth-token"{print $2}')
ACCT="AUTH_${SWIFT_USER%%:*}"          # tempauth storage account: AUTH_<part of SWIFT_USER before the colon>
echo "token=${TOKEN:0:12}...  account=${ACCT}"
```

Scenarios 52–58 use `${SWIFT_VIP_URL}`, `${TOKEN}` and `${ACCT}` from this block. Objects created by the tests go under test-only prefixes (`placement/`, `outage/`, `diskfail/`, `corrupt/`; Scenario 58 deliberately keeps its three restored `flow1/` objects) in `${SWIFT_CONTAINER}` and are deleted at the end of each scenario, so they do not disturb the `flow<N>/` objects checked in Scenario 50.

---

### Scenario 52: Swift Cluster Health, Ring Consistency & Replica Placement

> **Runs on:** **any of node1–node3** for the `swift-ring-builder`, `swift-get-nodes` and `swift-recon` commands (they read `/etc/swift/*.ring.gz` locally, and `swift-recon` queries the other nodes' object servers over HTTP). The `ssh` loops run from any of node1–node3. The `curl` checks run from **any machine that can reach the VIP**.

Use this after Phase 2b, after any node reboot (Scenarios 16–19), and before any other Swift scenario, to prove the cluster is healthy and that every object really has one copy on each node.

#### 52.1 — Services, ring files and `swift.conf` agree on all three nodes

```bash
# All Swift units, memcached and rsyncd active on every node
for n in ${NODE1_IP} ${NODE2_IP} ${NODE3_IP}; do
  echo "== ${n}"
  ssh "${n}" "systemctl is-active openstack-swift-proxy openstack-swift-account openstack-swift-container \
    openstack-swift-object openstack-swift-object-replicator openstack-swift-object-auditor memcached rsyncd | sort | uniq -c"
done

# Rings and swift.conf (hash secrets) identical everywhere
for n in ${NODE1_IP} ${NODE2_IP} ${NODE3_IP}; do
  ssh "${n}" "cd /etc/swift && sudo md5sum account.ring.gz container.ring.gz object.ring.gz swift.conf"
done

# The same comparison done by Swift itself (the object servers' recon middleware answers /recon/ringmd5 and /recon/swiftconfmd5)
sudo swift-recon object --md5
```

*Expected:* `active` is reported once per unit on every node (`8 active` per node); all `md5sum` lines match across nodes; `swift-recon` prints `3/3 hosts matched, 0 error[s] while checking hosts.` once for the object ring and once for `swift.conf`. A `!! ... doesn't match on disk md5sum` line names the node whose ring or `swift.conf` differs. A different `swift.conf` hash prefix or suffix on one node is the worst case: that node computes different object locations, so some GETs return 404 even though the data exists.

#### 52.2 — Ring layout

```bash
sudo swift-ring-builder /etc/swift/object.builder
```

*Expected (header line, per the defaults in `src/config/.env.example`):* `1024 partitions, 3.000000 replicas, 1 regions, 3 zones, 3 devices`, followed by one device line per node in zones 1, 2 and 3, all on port `6200` with device `flowfirst` and weight `100.00`. The `balance` and `dispersion` values should be near `0.00`.

#### 52.3 — Auth and health through the VIP and through each node directly

```bash
# HAProxy's own health probe target, through the VIP and on each proxy
curl -s -o /dev/null -w "VIP   %{http_code}\n" "${SWIFT_VIP_URL}/healthcheck"
for n in ${NODE1_IP} ${NODE2_IP} ${NODE3_IP}; do
  curl -s -o /dev/null -w "${n} %{http_code}\n" "http://${n}:${SWIFT_PROXY_PORT:-8888}/healthcheck"
done

# A token issued by node1's proxy must be accepted by every proxy (shared memcached)
TOKEN1=$(curl -s -D - -o /dev/null -H "X-Storage-User: ${SWIFT_USER}" -H "X-Storage-Pass: ${SWIFT_KEY}" \
  "http://${NODE1_IP}:${SWIFT_PROXY_PORT:-8888}/auth/v1.0" | tr -d '\r' | awk -F': ' 'tolower($1)=="x-auth-token"{print $2}')
for n in ${NODE1_IP} ${NODE2_IP} ${NODE3_IP}; do
  curl -s -o /dev/null -w "${n} account HEAD %{http_code}\n" -I -H "X-Auth-Token: ${TOKEN1}" \
    "http://${n}:${SWIFT_PROXY_PORT:-8888}/v1/${ACCT}"
done
```

*Expected:* every `/healthcheck` returns `200`, and the account `HEAD` returns `204` on all three nodes. A `401` on node2 or node3 for a token issued by node1 means the proxies are not sharing the same memcached servers (compare `memcache_servers` in `/etc/swift/proxy-server.conf` on each node, and see Scenario 56).

#### 52.4 — Store one object, then find all three copies

```bash
# Store a test object through the VIP
echo "swift placement test $(date +%s)" > /tmp/placement.txt
curl -s -o /dev/null -w "PUT %{http_code}\n" -X PUT -H "X-Auth-Token: ${TOKEN}" \
  -H "Content-Type: text/plain" --data-binary @/tmp/placement.txt \
  "${SWIFT_VIP_URL}/v1/${ACCT}/${SWIFT_CONTAINER}/placement/test1.txt"

# Where does the ring say it lives?
sudo swift-get-nodes /etc/swift/object.ring.gz "${ACCT}" "${SWIFT_CONTAINER}" placement/test1.txt
```

*Expected:* `PUT 201`. `swift-get-nodes` prints a `Partition` and `Hash` line and then three `Server:Port Device` lines, one per node IP (port `6200`, device `flowfirst`), followed by a `curl -g -I -XHEAD ...` line per node and an `ssh <ip> "ls -lah ${DEVICE:-/srv/node*}/flowfirst/objects/<partition>/<suffix>/<hash>"` line per node. Run each of the three `ssh ... ls` lines (add `sudo`): each node holds a `.data` file of the same size.

To check the replicas directly, paste the three `curl -g -I -XHEAD ...` lines that `swift-get-nodes` printed: they talk to each object server on port `6200`, bypassing the proxy, and all three should return `200 OK` with the same `Etag`.

#### 52.5 — Cluster-wide device checks and clean-up

> **Plain-directory installs report "Not mounted".** The recon middleware always runs a real mount-point check (`check_mount`), even when `mount_check = false` is set for the object server. The default install keeps `/srv/node/flowfirst` on the root filesystem, so without help `swift-recon -u` would list `Not mounted: flowfirst on <ip>:6200` for all three nodes and `-d` would print no usage for them (the objects are stored normally either way). `setup_swift.sh` therefore creates the stub file `/srv/node/flowfirst/.ismount`, which Swift accepts as a mount marker, whenever `SWIFT_MOUNT_CHECK` is not `true` and the directory is not a mount point. On a node installed before that change, create it by hand: `sudo -u swift touch /srv/node/flowfirst/.ismount`. With a dedicated XFS filesystem (`SWIFT_MOUNT_CHECK=true`) no stub is created and a missing mount is reported as it should be.

```bash
sudo swift-recon object -d -u -q        # disk usage, unmounted devices, quarantined objects
curl -s -o /dev/null -w "DELETE %{http_code}\n" -X DELETE -H "X-Auth-Token: ${TOKEN}" \
  "${SWIFT_VIP_URL}/v1/${ACCT}/${SWIFT_CONTAINER}/placement/test1.txt"
```

*Expected (with the `.ismount` stub or a real mount):* disk usage lines for 3 hosts, no `Not mounted:` or `Device errors:` lines, and `quarantined_objects` showing `total: 0`. `DELETE 204`. `swift-recon` takes its host list from the ring, so it works from any node. `No hosts returned valid data.` means the object servers' recon middleware is not reachable: check that port `6200` is open between the nodes and `systemctl is-active openstack-swift-object`.

---

### Scenario 53: Swift Node Loss — Writes Continue on 2 of 3 Replicas, Then Catch-Up

> **Runs on:** stop and start on **node3** (the node being failed); the `curl`, `swift-get-nodes` and `swift-recon` checks run from **node1 or node2** (node3's Swift is down) and `curl` from any machine that can reach the VIP. The DB Updater (Process 4) log check runs on the **current DB Updater (Process 4) leader**.

Swift needs `floor(3/2)+1 = 2` replica writes to accept a PUT, so one node can be lost without an outage. This scenario takes node3's Swift down **without** stopping the rest of node3 (Pacemaker, MariaDB and the pipeline keep running), writes objects while it is down, brings it back, and confirms the replicators copy the missing objects to it. Scenarios 17 and 18 already cover whole-node reboots; use this one to study Swift's own behaviour.

#### 53.1 — Stop Swift on node3 and watch HAProxy remove it

```bash
# On node3 — stop the proxy and the storage servers (keep memcached and rsyncd running)
sudo systemctl stop openstack-swift-proxy openstack-swift-object openstack-swift-container openstack-swift-account

# On the VIP holder — swift_back should mark node3 DOWN within about 6 s (inter 3s, fall 2)
echo "show stat" | sudo socat stdio /var/lib/haproxy/stats | awk -F, '$1=="swift_back"{print $1,$2,$18}'
```

*Expected:* `swift_back node1 UP`, `swift_back node2 UP`, `swift_back node3 DOWN` (and the `BACKEND` line stays `UP`).

#### 53.2 — The pipeline and manual PUTs still work

```bash
# Manual PUTs through the VIP while node3's Swift is down
for i in 1 2 3 4 5; do
  curl -s -o /dev/null -w "PUT $i %{http_code}\n" -X PUT -H "X-Auth-Token: ${TOKEN}" --data-binary "outage-$i" \
    "${SWIFT_VIP_URL}/v1/${ACCT}/${SWIFT_CONTAINER}/outage/obj${i}.txt"
done

# Send pipeline messages; each one is stored by DB Updater (Process 4) via swiftclient
for i in 1 2 3; do
  curl -s -X POST http://${FLOWFIRST_VIP}:8080/api/flow1 -H "Content-Type: application/json" \
    -d "{\"item_id\": 53${i}, \"initial_data\": \"swift-node-loss-${i}\"}" | jq -r .payload.message_id
done
sleep 10
# On the DB Updater (Process 4) leader
sudo journalctl -u flowfirst-process4 --since "2 minutes ago" --no-pager | grep -E "Stored in Swift|Swift store note"
```

*Expected:* all `PUT` lines return `201`; DB Updater (Process 4) prints `Stored in Swift object storage: flow1/<message_id>.json` for each message and no `Swift store note:`. A request that reaches node3's stopped proxy before HAProxy marks it down is a refused connection, which HAProxy re-dispatches to another node (`option redispatch`, `retries 3` in the template defaults); if a client still sees a `5xx`, `swiftclient` retries it (`retries=3`).

#### 53.3 — Only two copies exist while node3 is down

```bash
sudo swift-get-nodes /etc/swift/object.ring.gz "${ACCT}" "${SWIFT_CONTAINER}" outage/obj1.txt
# Run the printed "ssh <ip> ls" lines for node1 and node2 (each has a .data file) and for node3 (no such directory)
```

*Expected:* node1 and node2 list the `.data` file; on node3 `ls` reports `No such file or directory` for that object's directory. The proxy would send a missing copy to a **handoff** node, but a 3-node ring with 3 replicas has no spare device, so the object stays at 2 copies until node3 returns. The container listing update for node3's container server is queued as an async pending on the other nodes (`sudo swift-recon object -a`) and is delivered by the `object-updater` after node3 is back (its pass interval is 300 s by default).

#### 53.4 — Start node3 and wait for the replicators

```bash
# On node3
sudo systemctl start openstack-swift-account openstack-swift-container openstack-swift-object openstack-swift-proxy

# Replicator passes run every 30 s by default (object-replicator interval); to force one pass now, on node1 AND node2:
sudo -u swift swift-object-replicator /etc/swift/object-server.conf once -v

# Then check node3 received them
sudo swift-recon object -r
ssh ${NODE3_IP} "sudo find /srv/node/flowfirst/objects -name '*.data' | wc -l"
```

*Expected:* with `-v` the replicator logs to the console and ends with `Object replication complete (once)`. Replication is push-based, which is why it runs on both nodes that hold the objects. `sudo swift-recon object -r` reports `[replication_failure]` and `[replication_attempted]` counters; the failure total can still include the pass during which node3 was down, so judge it after a later full pass. The `.data` count on node3 now includes the `outage/obj1..5.txt` objects. Re-run the `ssh ... ls` lines from 53.3 to see all three copies. HAProxy marks node3 `UP` again after 2 passed checks (`rise 2`, about 6 s).

#### 53.5 — Clean up

```bash
for i in 1 2 3 4 5; do
  curl -s -o /dev/null -w "DELETE $i %{http_code}\n" -X DELETE -H "X-Auth-Token: ${TOKEN}" \
    "${SWIFT_VIP_URL}/v1/${ACCT}/${SWIFT_CONTAINER}/outage/obj${i}.txt"
done
```

Items 531–533 exist in both MariaDB and Swift (Scenario 50 D finds no gap), because Swift never stopped accepting writes.

---

### Scenario 54: Swift Device Failure — HTTP 507, Error Limiting and Recovery

> **Runs on:** the device fault is created and removed on **node3**; the `curl` calls run from any machine that can reach the VIP; `swift-get-nodes` and the proxy and replicator log checks run on **node1 or node2** (read the journal on each of node1–node3, as the proxy that served a request may be any of them).

A Swift device that disappears is answered with `507 Insufficient Storage` by that node's storage servers. The proxy then **error-limits** the node (skips it for a while), so writes carry on with the remaining two replicas. With the default install (`SWIFT_MOUNT_CHECK=false`) Swift only checks that `/srv/node/flowfirst` is a directory, so renaming it is a safe way to simulate a lost disk. With `SWIFT_MOUNT_CHECK=true` (dedicated XFS mount) unmounting the filesystem has the same effect.

#### 54.1 — Take the device away on node3

```bash
# On node3
sudo mv /srv/node/flowfirst /srv/node/flowfirst.off
```

#### 54.2 — The object server answers 507 directly

```bash
# On node1 or node2 — find the object server URL for an existing object and HEAD node3 directly
sudo swift-get-nodes /etc/swift/object.ring.gz "${ACCT}" "${SWIFT_CONTAINER}" diskfail/obj1.txt | grep "XHEAD"
# Run the printed curl line whose address is ${NODE3_IP}:6200
```

*Expected:* `HTTP/1.1 507 Insufficient Storage` from node3 (the object may not exist yet; a missing device is checked before the object is looked up). The same command against node1 or node2 returns `404 Not Found`.

#### 54.3 — Writes still succeed, and the proxy limits node3

```bash
for i in 1 2 3 4 5; do
  curl -s -o /dev/null -w "PUT $i %{http_code}\n" -X PUT -H "X-Auth-Token: ${TOKEN}" --data-binary "diskfail-$i" \
    "${SWIFT_VIP_URL}/v1/${ACCT}/${SWIFT_CONTAINER}/diskfail/obj${i}.txt"
done

# On each of node1-node3 (whichever proxies served the PUTs)
sudo journalctl -t proxy-server --since "2 minutes ago" --no-pager | grep -E "error limited|Insufficient Storage"
```

*Expected:* every `PUT` returns `201`. The first PUT served by each proxy logs a line of the form `ERROR Node will be error limited for 60.00s: <NODE3_IP>:6200/flowfirst, error: ERROR Insufficient Storage` (`error_suppression_interval` defaults to 60 s). While the node is limited the proxy skips it without trying, so there is no further log line and no added latency. After 60 s the proxy tries node3 again and gets another `507`, so the line repeats at most once a minute per proxy, and only while requests keep arriving.

Pipeline messages sent now (`/api/flow1`) are stored by DB Updater (Process 4) as usual: the `Stored in Swift object storage` line appears and no `Swift store note:` is printed.

#### 54.4 — The replicators notice

```bash
# On node1 and node2: force a replication pass, then look for the unmounted-device message
sudo -u swift swift-object-replicator /etc/swift/object-server.conf once -v 2>&1 | grep -E "responded as unmounted|complete"
```

*Expected:* `<NODE3_IP>:6200/flowfirst responded as unmounted` followed by `Object replication complete (once)`. Nothing can be pushed to node3 until the device returns, and no handoff is used because the 3-node ring has no spare device.

> **Do not use `swift-recon -u` or `-d` to confirm this test.** The recon middleware lists the directories under `/srv/node`, not the devices in the ring. The renamed directory `flowfirst.off` (which still contains the `.ismount` stub) is listed as a healthy extra device and the missing `flowfirst` is not mentioned at all, so `-u` stays silent. On a dedicated XFS mount (`SWIFT_MOUNT_CHECK=true`) a real unmount is reported correctly as `Not mounted: flowfirst on <ip>:6200`. The `507` from 54.2 and the log lines in 54.3 and 54.4 are the reliable signals here.

#### 54.5 — Restore the device and let the replicators catch up

```bash
# On node3
sudo mv /srv/node/flowfirst.off /srv/node/flowfirst

# On node1 and node2
sudo -u swift swift-object-replicator /etc/swift/object-server.conf once -v 2>&1 | grep complete

# Check node3 now holds the objects written during the fault, then delete the test objects
sudo swift-get-nodes /etc/swift/object.ring.gz "${ACCT}" "${SWIFT_CONTAINER}" diskfail/obj1.txt | grep "ls -lah"   # run the node3 line
for i in 1 2 3 4 5; do
  curl -s -o /dev/null -w "DELETE $i %{http_code}\n" -X DELETE -H "X-Auth-Token: ${TOKEN}" \
    "${SWIFT_VIP_URL}/v1/${ACCT}/${SWIFT_CONTAINER}/diskfail/obj${i}.txt"
done
```

*Expected:* the node3 `ls` shows the `.data` file for `diskfail/obj1.txt` after the replication pass; `DELETE` returns `204`. The proxies stop limiting node3 on their own within 60 s of the next successful request, so no restart is needed. The renamed directory must keep its original name, owner (`swift`) and SELinux label (`rsync_data_t`); a `mv` back preserves all three.

---

### Scenario 55: Swift Object Corruption — Auditor Quarantine and Replica Repair

> **Runs on:** the corruption and the auditor run on **node3**; `swift-get-nodes` and the replicator passes run on **node1 and node2** (the replicators on the nodes holding the good copies push the repair); the `curl` calls run from any machine that can reach the VIP.

Swift detects a damaged replica in two ways: the **object auditor** scans the disk on a schedule, and a GET compares the stored metadata with the file it reads. A damaged file is moved to `<device>/quarantined/` (not deleted), and the replicators then copy a good version back from another node. This scenario damages one replica on purpose.

#### 55.1 — Store an object and locate the node3 copy

```bash
# On node1 or node2
curl -s -o /dev/null -w "PUT %{http_code}\n" -X PUT -H "X-Auth-Token: ${TOKEN}" --data-binary "corruption test payload" \
  "${SWIFT_VIP_URL}/v1/${ACCT}/${SWIFT_CONTAINER}/corrupt/obj1.txt"
sudo swift-get-nodes /etc/swift/object.ring.gz "${ACCT}" "${SWIFT_CONTAINER}" corrupt/obj1.txt > /tmp/corrupt-nodes.txt
PART=$(awk '/^Partition/{print $2}' /tmp/corrupt-nodes.txt); HASH=$(awk '/^Hash/{print $2}' /tmp/corrupt-nodes.txt)
echo "partition=${PART} hash=${HASH}"
# Directory of the replica on every node: /srv/node/flowfirst/objects/<partition>/<last 3 hash chars>/<hash>
OBJDIR="/srv/node/flowfirst/objects/${PART}/${HASH: -3}/${HASH}"
ssh ${NODE3_IP} "sudo ls -l ${OBJDIR}"
```

*Expected:* `PUT 201` and one `<timestamp>.data` file in `${OBJDIR}` on node3 (it is the same path on all three nodes).

#### 55.2 — Corrupt the node3 replica

```bash
# Append one byte: the file no longer matches the Content-Length stored in its extended attributes
ssh ${NODE3_IP} "sudo sh -c 'printf x >> ${OBJDIR}/*.data'; sudo ls -l ${OBJDIR}"
```

*Expected:* the `.data` file is now one byte larger than before (24 bytes instead of 23 for the payload above).

#### 55.3 — Run the auditor on node3

```bash
# On node3
sudo -u swift swift-object-auditor /etc/swift/object-server.conf once -v 2>&1 | grep -E "quarantined|completed"
sudo ls /srv/node/flowfirst/quarantined/objects/
sudo ls "${OBJDIR}" 2>&1 | head -2          # the object directory is gone from objects/
```

*Expected:* an `ERROR Object ... failed audit and was quarantined: metadata content-length 23 does not match actual object size 24` line, then an `Object audit (...) "once" mode completed: ... Total quarantined: 1` line. The quarantine directory holds a directory named after the object hash, and `${OBJDIR}` no longer lists the `.data` file. The continuous `openstack-swift-object-auditor` service does the same on its own schedule; the `once` run just does not wait for it.

```bash
# From node1 or node2
sudo swift-recon object -q
```

*Expected:* `[quarantined_objects] low: 0, high: 1, avg: 0.3, total: 1 ...` (node3 reports 1).

#### 55.4 — Repair from the good replicas

```bash
# On node1 and node2
sudo -u swift swift-object-replicator /etc/swift/object-server.conf once -v 2>&1 | grep complete
ssh ${NODE3_IP} "sudo ls -l ${OBJDIR}"
curl -s -H "X-Auth-Token: ${TOKEN}" "${SWIFT_VIP_URL}/v1/${ACCT}/${SWIFT_CONTAINER}/corrupt/obj1.txt"; echo
```

*Expected:* node3 again holds a 23-byte `.data` file (restored by node1's or node2's replicator) and the GET returns `corruption test payload`. Reads were never affected during this test: the proxy returned the data from node1 or node2 regardless of node3's state.

#### 55.5 — Clean up

```bash
curl -s -o /dev/null -w "DELETE %{http_code}\n" -X DELETE -H "X-Auth-Token: ${TOKEN}" \
  "${SWIFT_VIP_URL}/v1/${ACCT}/${SWIFT_CONTAINER}/corrupt/obj1.txt"
ssh ${NODE3_IP} "sudo sh -c 'rm -rf /srv/node/flowfirst/quarantined/objects/${HASH}*'"
```

Quarantined files are kept until you remove them. In a real incident leave them until the cause (disk errors in `dmesg`, Scenario 51) is understood, and watch `swift-recon -q` for a count that keeps growing, which points to failing hardware rather than a one-off event.

---

### Scenario 56: Swift Token Cache (memcached) Failure — 401 Re-Auth, Then 503 When All Caches Are Down

> **Runs on:** `systemctl stop|start memcached` on **node3** (then on all three nodes for 56.3); the `curl` calls run from any machine that can reach the VIP; the client-side check runs on the **current DB Updater (Process 4) leader**; the proxy log check runs on **each of node1–node3**.

tempauth keeps its tokens **only in memcached**, and all three proxies share the same three memcached servers (`memcache_servers` in `/etc/swift/proxy-server.conf`). Token lifetime is 24 hours. This scenario shows what a lost cache server does to clients: one lost server costs a re-login, losing all of them stops logins.

#### 56.1 — Baseline: one token works on every proxy

```bash
# Uses ${TOKEN} and ${ACCT} from the Swift Cluster Scenarios header
for n in ${NODE1_IP} ${NODE2_IP} ${NODE3_IP}; do
  curl -s -o /dev/null -w "${n} %{http_code}\n" -I -H "X-Auth-Token: ${TOKEN}" "http://${n}:${SWIFT_PROXY_PORT:-8888}/v1/${ACCT}"
done
```

*Expected:* `204` from all three.

#### 56.2 — Stop memcached on node3 only

```bash
# On node3
sudo systemctl stop memcached

# From any machine: the same token, three proxies
for n in ${NODE1_IP} ${NODE2_IP} ${NODE3_IP}; do
  curl -s -o /dev/null -w "${n} %{http_code}\n" -I -H "X-Auth-Token: ${TOKEN}" "http://${n}:${SWIFT_PROXY_PORT:-8888}/v1/${ACCT}"
done

# Logging in again still works: the new token is stored on a surviving cache server
curl -s -o /dev/null -w "login %{http_code}\n" -H "X-Storage-User: ${SWIFT_USER}" -H "X-Storage-Pass: ${SWIFT_KEY}" "${SWIFT_VIP_URL}/auth/v1.0"
```

*Expected:* the old token returns `204` on every proxy **if its cache entry lived on node1 or node2** and `401` on every proxy if the entry was held by node3's memcached (the entry is found by hashing the token, so all proxies agree). The login returns `200`. Run the loop with a few different tokens (log in again, repeat) to see both outcomes. A `401` here is not an error in Swift: the token was lost with the cache server. The proxies log `Error connecting to memcached: <NODE3_IP>:11211` lines (`sudo journalctl -t proxy-server --since "2 minutes ago" | grep memcached`); after 10 errors in 60 s a proxy stops trying that server for 60 s.

**What the pipeline sees:** `python-swiftclient` handles a `401` itself: it logs in again once and repeats the request, so DB Updater (Process 4) keeps writing and prints `Stored in Swift object storage` with no `Swift store note:`.

```bash
for i in 1 2 3; do
  curl -s -X POST http://${FLOWFIRST_VIP}:8080/api/flow1 -H "Content-Type: application/json" \
    -d "{\"item_id\": 56${i}, \"initial_data\": \"memcached-one-down-${i}\"}" | jq -r .payload.message_id
done
sleep 10
# On the DB Updater (Process 4) leader
sudo journalctl -u flowfirst-process4 --since "2 minutes ago" --no-pager | grep -E "Stored in Swift|Swift store note"
```

*Expected:* three `Stored in Swift object storage: flow1/...` lines.

#### 56.3 — Stop memcached on all three nodes

```bash
# On each of node1, node2, node3
sudo systemctl stop memcached

curl -s -D - -o /dev/null -H "X-Storage-User: ${SWIFT_USER}" -H "X-Storage-Pass: ${SWIFT_KEY}" "${SWIFT_VIP_URL}/auth/v1.0" | head -1
curl -s -o /dev/null -w "old token: %{http_code}\n" -I -H "X-Auth-Token: ${TOKEN}" "${SWIFT_VIP_URL}/v1/${ACCT}"
```

*Expected:* the login returns `HTTP/1.1 503 Service Unavailable` (tempauth cannot store a new token) and requests with any existing token return `401` (it cannot be looked up). `/healthcheck` still returns `200`, so **HAProxy keeps all three Swift proxies in `swift_back`**: nothing in the load balancer tells you the cache is gone. Send pipeline messages now:

```bash
for i in 1 2; do
  curl -s -X POST http://${FLOWFIRST_VIP}:8080/api/flow1 -H "Content-Type: application/json" \
    -d "{\"item_id\": 56${i}9, \"initial_data\": \"memcached-all-down-${i}\"}" | jq -r .payload.message_id
done
sleep 30
# On the DB Updater (Process 4) leader
sudo journalctl -u flowfirst-process4 --since "2 minutes ago" --no-pager | grep -E "Stored in Swift|Swift store note"
```

*Expected:* `Swift store note: Auth GET failed: http://<VIP>:8090/auth/v1.0 503 Service Unavailable  [first 60 chars of response] b'<html><h1>Service Unavailable</h1><p>The server is currently'` (`python-swiftclient` appends the first 60 bytes of the reply body to its error text; the client retries a `5xx` with 1 s, 2 s and 4 s pauses first, so each message waits about 7 s on Swift before it is acknowledged). DB Updater (Process 4) also logs the same text in an `ERROR Failed to put object 'flow1/<message_id>.json' ...` line just before it. The MariaDB rows are still written. These two items (5619 and 5629) are missing from Swift; Scenario 58 shows how to back-fill.

#### 56.4 — Restore

```bash
# On each of node1, node2, node3
sudo systemctl start memcached
curl -s -o /dev/null -w "login %{http_code}\n" -H "X-Storage-User: ${SWIFT_USER}" -H "X-Storage-Pass: ${SWIFT_KEY}" "${SWIFT_VIP_URL}/auth/v1.0"
```

*Expected:* `login 200`. Memcached starts empty, so every client logs in again once (a `401` followed by the automatic re-login). Refresh `${TOKEN}` with the header block before the next scenario.

---

### Scenario 57: Swift Credential Failures — Wrong Key, Key Rotation and Token Lifetime

> **Runs on:** the `curl` and `swift_storage` checks (A, B) on **any node** that has `/opt/flowfirst` and its `.venv` (node1–node3; node4 has them too but does not call Swift); the `.env` change in C on **all of node1–node3** (DB Updater (Process 4) can run on any of them); the `proxy-server.conf` change in D on **all of node1–node3**.

Credentials sit in two places: `user_<account>_<user> = <key> .admin` in `/etc/swift/proxy-server.conf` on each node (server side), and `SWIFT_USER` / `SWIFT_KEY` in `.env` on each node (client side). A mismatch gives `401` and, because Swift writes are non-fatal, only a `Swift store note:` line, so nothing alerts you.

#### A. Wrong key is refused at login

```bash
curl -s -D - -o /dev/null -H "X-Storage-User: ${SWIFT_USER}" -H "X-Storage-Pass: wrong-key" "${SWIFT_VIP_URL}/auth/v1.0" | tr -d '\r' | head -3
```

*Expected:* `HTTP/1.1 401 Unauthorized` with a `Www-Authenticate: Swift realm="<account>"` header. The same happens for an unknown user.

#### B. What the application reports

```bash
cd /opt/flowfirst
SWIFT_KEY=wrong-key .venv/bin/python -c "
from src.swift import swift_storage
print(swift_storage.put_message_object({'message_id': 'cred-test', 'flow': 9}))
print(swift_storage.get_swift_status())"
```

*Expected:* `(False, "Auth GET failed: http://<VIP>:8090/auth/v1.0 401 Unauthorized  [first 60 chars of response] b'<html><h1>Unauthorized</h1><p>This server could not verify t'")` and a status dict with `'status': 'error'` and the same text in `error`. The same text is also printed to stderr as `Failed to put object 'flow9/cred-test.json' in container 'flowfirst_messages': ...` (a plain script has no logging set-up, so there is no `ERROR` prefix; the pipeline processes add it through `gt.configure_logging()`). A `401` costs one extra login attempt after a 1 s pause and is then raised, so it fails after about 1 s, unlike the 7 s of retries for the `503` in Scenario 56. The environment variable overrides `.env` because `python-dotenv` does not replace values that are already set.

#### C. Break and fix the client key on the DB Updater (Process 4) side

```bash
# On each of node1-node3 (the P4 leader can move when the clone restarts)
for n in ${NODE1_IP} ${NODE2_IP} ${NODE3_IP}; do
  ssh "${n}" "sudo sed -i 's/^SWIFT_KEY=.*/SWIFT_KEY=wrong-key/' /opt/flowfirst/.env"
done
sudo pcs resource restart flowfirst-p4-res-clone
sudo pcs resource restart flowfirst-p1-res-clone      # so /swift/status also uses the changed key
sleep 10

curl -s -X POST http://${FLOWFIRST_VIP}:8080/api/flow1 -H "Content-Type: application/json" \
  -d '{"item_id": 5701, "initial_data": "bad-swift-key"}' | jq -r .payload.message_id
sleep 8
# On the DB Updater (Process 4) leader
sudo journalctl -u flowfirst-process4 --since "2 minutes ago" --no-pager | grep -E "Swift store note|Persisted to MariaDB"
curl -s http://${FLOWFIRST_VIP}:8080/swift/status | jq -r '.status, .error'

# Restore the real key (use the value you had; shown here from the shell's loaded copy)
for n in ${NODE1_IP} ${NODE2_IP} ${NODE3_IP}; do
  ssh "${n}" "sudo sed -i 's|^SWIFT_KEY=.*|SWIFT_KEY=${SWIFT_KEY}|' /opt/flowfirst/.env"
done
sudo pcs resource restart flowfirst-p4-res-clone
sudo pcs resource restart flowfirst-p1-res-clone
```

*Expected:* `Swift store note: Auth GET failed: ... 401 Unauthorized ...` followed by `Persisted to MariaDB` for item 5701, and `/swift/status` printing `error` and the same `Auth GET failed` text. Item 5701 exists in MariaDB but not in Swift (Scenario 58 shows the back-fill). The processes read `.env` only at start, which is why both clones are restarted.

#### D. Rotating the key on the server side

```bash
NEWKEY="$(openssl rand -hex 12)"
ACCOUNT="${SWIFT_USER%%:*}"; SUSER="${SWIFT_USER#*:}"
for n in ${NODE1_IP} ${NODE2_IP} ${NODE3_IP}; do
  ssh "${n}" "sudo sed -i 's/^user_${ACCOUNT}_${SUSER} = [^ ]* /user_${ACCOUNT}_${SUSER} = ${NEWKEY} /' /etc/swift/proxy-server.conf && sudo systemctl restart openstack-swift-proxy"
done

curl -s -o /dev/null -w "old key: %{http_code}\n" -H "X-Storage-User: ${SWIFT_USER}" -H "X-Storage-Pass: ${SWIFT_KEY}" "${SWIFT_VIP_URL}/auth/v1.0"
curl -s -o /dev/null -w "new key: %{http_code}\n" -H "X-Storage-User: ${SWIFT_USER}" -H "X-Storage-Pass: ${NEWKEY}" "${SWIFT_VIP_URL}/auth/v1.0"
curl -s -o /dev/null -w "old token: %{http_code}\n" -I -H "X-Auth-Token: ${TOKEN}" "${SWIFT_VIP_URL}/v1/${ACCT}"
```

*Expected:* `old key: 401`, `new key: 200`, and `old token: 204`. **A token issued before the rotation keeps working** until it expires (24 hours) or its cache entry is lost, because tempauth only checks the key when a token is issued. So a rotation does not log existing clients out; it only blocks new logins with the old key. After the rotation, put `NEWKEY` into `SWIFT_KEY` in `.env` on every node and restart DB Updater (Process 4) and API Gateway (Process 1), as in C, or the next login by the pipeline will get a `401`. To undo the test, set `NEWKEY="${SWIFT_KEY}"` and run the same `for` loop again.

> The proxies read `proxy-server.conf` only at start. Editing it on one node leaves the other two accepting the old key, so a load-balanced login may succeed or fail depending on which proxy answers. Always change and restart all three.

---

### Scenario 58: Total Swift Outage During the Pipeline — Detect and Back-Fill from MariaDB

> **Runs on:** the stop and start of the proxies on **each of node1–node3**; the message sends and the reconciliation from **any machine with a `mariadb` client that can reach the VIP** (node1–node3 have it); the log check on the **current DB Updater (Process 4) leader**.

Swift writes are best-effort (Scenario 50), so a Swift outage never stops the pipeline: MariaDB rows are written, messages are acknowledged, and objects are missing afterwards. `put_message_object()` is not retried later and a redelivered message is skipped by the ZooKeeper dedup barrier, so the gap persists until someone fills it. DB Updater (Process 4) stores the same JSON in MariaDB (`raw_payload`) as in Swift, so the missing objects can be rebuilt from the table.

#### 58.1 — Stop every Swift proxy

```bash
# On each of node1, node2, node3
sudo systemctl stop openstack-swift-proxy

# HAProxy needs about 6 s (inter 3s, fall 2) to mark all three servers DOWN
sleep 8
curl -s -o /dev/null -w "VIP swift: %{http_code}\n" "${SWIFT_VIP_URL}/healthcheck"
```

*Expected:* `VIP swift: 503` (HAProxy has no healthy server in `swift_back`; the HAProxy log shows `backend swift_back has no server available!`). The storage servers (`6200/6201/6202`) are still running, so no data is lost.

#### 58.2 — Send traffic during the outage

```bash
for i in 1 2 3; do
  curl -s -X POST http://${FLOWFIRST_VIP}:8080/api/flow1 -H "Content-Type: application/json" \
    -d "{\"item_id\": 58${i}, \"initial_data\": \"swift-outage-${i}\"}" | jq -r .payload.message_id
done
sleep 40
# On the DB Updater (Process 4) leader
sudo journalctl -u flowfirst-process4 --since "3 minutes ago" --no-pager | grep -E "Swift store note|Persisted to MariaDB" | tail -8
```

*Expected:* for each item, `Swift store note: Auth GET failed: http://<VIP>:8090/auth/v1.0 503 Service Unavailable  [first 60 chars of response] b'<html><body><h1>503 Service Unavailable</h1>\nNo server is av'` followed by `Persisted to MariaDB`. The body here is HAProxy's built-in 503 page (the request never reaches a Swift proxy), unlike Scenario 56 where tempauth itself answers. Each message spends about 7 s in the Swift client's retries (1 s + 2 s + 4 s pauses, four login attempts) before the note is printed, and DB Updater (Process 4) handles one message at a time per flow (`prefetch_count=1`), so a long outage slows the pipeline by that amount per message even though nothing fails.

#### 58.3 — Restore Swift

```bash
# On each of node1, node2, node3
sudo systemctl start openstack-swift-proxy
sleep 8
curl -s -o /dev/null -w "VIP swift: %{http_code}\n" "${SWIFT_VIP_URL}/healthcheck"      # 200 once two checks pass
curl -s http://${FLOWFIRST_VIP}:8080/swift/status | jq -r .status                        # connected
```

#### 58.4 — Find the gap and back-fill it from MariaDB

```bash
M() { mariadb -h "${FLOWFIRST_VIP}" -u "${MARIADB_USER}" -p"${MARIADB_PASSWORD}" "${MARIADB_DB}" "$@"; }
# -r (raw) is required: without it the client escapes backslashes inside the JSON column
M -N -B -r -e "SELECT message_id, flow_id, item_id, raw_payload FROM processed_messages WHERE item_id BETWEEN 581 AND 583 AND flow_id = 1" \
| while IFS=$'\t' read -r mid fid iid payload; do
    code=$(curl -s -o /dev/null -w '%{http_code}' -H "X-Auth-Token: ${TOKEN}" "${SWIFT_VIP_URL}/v1/${ACCT}/${SWIFT_CONTAINER}/flow${fid}/${mid}.json")
    if [ "${code}" = "404" ]; then
      curl -s -o /dev/null -w "restored flow${fid}/${mid}.json -> %{http_code}\n" -X PUT -H "X-Auth-Token: ${TOKEN}" \
        -H "Content-Type: application/json" -H "X-Object-Meta-Flow: ${fid}" -H "X-Object-Meta-Message-Id: ${mid}" \
        -H "X-Object-Meta-Item-Id: ${iid}" --data-binary "${payload}" \
        "${SWIFT_VIP_URL}/v1/${ACCT}/${SWIFT_CONTAINER}/flow${fid}/${mid}.json"
    else
      echo "flow${fid}/${mid}.json already present (${code})"
    fi
  done
```

*Expected:* `restored flow1/<message_id>.json -> 201` for each of items 581–583 (and the same pass over the items from Scenarios 56 and 57 if you widen the `WHERE` clause: `item_id IN (5619, 5629, 5701)`). Re-running the loop prints `already present (200)` for all of them, so it is safe to repeat. Refresh `${TOKEN}` first if the Scenario 56 or 57 steps invalidated it.

```bash
# Confirm with the Scenario 50 D count, which should match again
curl -s "http://${FLOWFIRST_VIP}:8080/swift/object/flow1/$(M -N -B -e 'SELECT message_id FROM processed_messages WHERE item_id = 581 AND flow_id = 1').json" | jq -r .payload.initial_data
```

*Expected:* `swift-outage-1`. The restored object holds the JSON that DB Updater (Process 4) stored in MariaDB (DB Updater (Process 4) added its `process4_saved_to_mariadb` history step before both writes), formatted compactly rather than with the original indentation. For a larger gap, widen the `WHERE` clause by `created_at` and compare against `/swift/objects?prefix=flow1/` as in Scenario 50 D. A stored object is a copy, not the source of truth: MariaDB remains authoritative, and nothing reads Swift back into the pipeline.

---

## HTTP / REST API Log Analysis

### Scenario 59: Reading the REST API (HTTP) Request Log Across Scenarios

> **Runs on:** the `curl` traffic generators from any machine that can reach the VIP. The log itself lives on **each of node1–node3**: every node runs its own API Gateway (Process 1), and HAProxy sends each request to one of them, so a single node's journal holds only the requests that node served. The cross-node loop in D runs from any of node1–node3 over `ssh`. node4 runs no API Gateway (Process 1) and has no REST API log.

API Gateway (Process 1) is the only FlowFirst component that speaks HTTP to clients. It has no access-log file: each request becomes one `INFO [http]` line, written by `RestApiHandler.log_message` in [`src/application/process1.py`](src/application/process1.py). Scenario 48 covers the HAProxy side of the same requests. Use this scenario alongside Scenarios 1, 11, 12, 29 and 40–41, and 56–58 for the Swift endpoints, to see which node served a request, what status it returned, and why a request failed. Exact wording of Python's own error lines varies between Python versions, so the commands below grep for stable keywords.

#### A. Locate the log and generate some traffic

```bash
# On node1, node2 or node3. -o cat prints the bare message without the journal prefix.
sudo journalctl -u flowfirst-process1 -o cat --since "5 min ago" --no-pager | grep -F '[http]' | tail -5
```
```
INFO [http] 192.168.1.11 — "GET /health HTTP/1.0" 200 -
INFO [http] 192.168.1.11 — "GET /health HTTP/1.0" 200 -
```

Each line is `INFO [http] <client ip> — "<request line>" <status> <size>`. Three things to know before reading it:

- **The client IP is HAProxy, not the real client.** Requests that came through the VIP arrive from the VIP holder's node address (normally its primary IP, not the VIP itself). The real client address is in the HAProxy log (Scenario 48, field 1) and in the `X-Forwarded-For` header that HAProxy adds; API Gateway (Process 1) does not log that header.
- **The size is always `-`.** The handler does not report a response size.
- **There is no duration.** To get timing, use the `Tr`/`Tt` fields in the HAProxy log, or `curl -w '%{time_total}'`.

Generate one example of each outcome from any machine that can reach the VIP:

```bash
V=http://${FLOWFIRST_VIP}:8080
curl -s -o /dev/null -w "GET  /health        -> %{http_code}\n" ${V}/health
curl -s -o /dev/null -w "GET  /nope          -> %{http_code}\n" ${V}/nope
curl -s -X POST ${V}/api/flow1 -H "Content-Type: application/json" -d '{"item_id": 5901, "counter": 10}' | jq -r .handled_by_node
curl -s -o /dev/null -w "POST bad JSON       -> %{http_code}\n" -X POST ${V}/api/flow1 -H "Content-Type: application/json" -d '{bad json'
curl -s -o /dev/null -w "POST item_id=abc    -> %{http_code}\n" -X POST ${V}/api/flow1 -H "Content-Type: application/json" -d '{"item_id": "abc"}'
curl -s -o /dev/null -w "GET  limit=abc      -> %{http_code}\n" "${V}/swift/objects?limit=abc"
```

*Expected output:* `200`, `404`, (a node name), `400`, `500`, `502`. The last one is produced by HAProxy, not by API Gateway (Process 1): the `limit=abc` handler raises an uncaught `ValueError`, so API Gateway (Process 1) prints a traceback and closes the connection without answering (see C). Then read the log on the node named by `handled_by_node` for the `POST`, and on every node for the rest:

```bash
sudo journalctl -u flowfirst-process1 -o cat --since "2 min ago" --no-pager | grep -v "GET /health HTTP/1.0"
```
```
INFO [http] 192.168.1.11 — "GET /health HTTP/1.1" 200 -
INFO [http] 192.168.1.11 — "GET /nope HTTP/1.1" 404 -
INFO [Flow 1] Published #5901 (counter=10, step=...)
INFO [http] 192.168.1.11 — "POST /api/flow1 HTTP/1.1" 200 -
INFO [http] 192.168.1.11 — "POST /api/flow1 HTTP/1.1" 400 -
ERROR POST /api/flow1 error: invalid literal for int() with base 10: 'abc'
Traceback (most recent call last):
  ...
ValueError: invalid literal for int() with base 10: 'abc'
INFO [http] 192.168.1.11 — "POST /api/flow1 HTTP/1.1" 500 -
```

The access line is written **after** the response is sent, so an `INFO [Flow N] Published ...` line or a `POST ... error:` line appears just *before* the access line of the same request. The requests are spread round-robin over three nodes (Scenario 1), so each node shows only about a third of them.

#### B. Health checks versus real requests

HAProxy probes `GET /health` on every backend every 3 s (`inter 3s`), and only from the current VIP holder. That is about 28,800 lines per day on every node, and they will bury everything else. HAProxy's probe normally identifies itself as `HTTP/1.0`; a real client such as `curl` uses `HTTP/1.1`. Confirm on your node before relying on this:

```bash
sudo journalctl -u flowfirst-process1 -o cat --since "1 min ago" --no-pager | grep -F 'GET /health' | awk -F'"' '{print $2}' | sort | uniq -c
```
```
     20 GET /health HTTP/1.0
```

Filter out only the probes with `grep -v "GET /health HTTP/1.0"`. Use `grep -v "GET /health"` to drop both probes and manual health checks (the form used in Scenario 48). Because the probes come from the VIP holder, the client IP of the `HTTP/1.0` lines on node2 and node3 tells you which node holds HAProxy right now; it should match `sudo crm_resource --resource flowfirst-vip --locate`.

To see a request that **bypassed** HAProxy, call a node directly on its backend port. The client IP in the log is then your own machine:

```bash
curl -s -o /dev/null -w "%{http_code}\n" http://${NODE2_IP}:8082/health         # from a workstation; no HAProxy involved
# On node2
sudo journalctl -u flowfirst-process1 -o cat --since "1 min ago" --no-pager | grep -F 'GET /health HTTP/1.1'
```

#### C. Message phrasebook — what each line means

| Log text (grep for) | Meaning | Typical scenarios |
|---|---|---|
| `"GET /health HTTP/1.0" 200 -` | HAProxy health probe; API Gateway (Process 1) is up | any |
| `"GET /health HTTP/1.1" 200 -` | A real client called `/health` (through the VIP or directly) | 1, 29 |
| `"POST /api/flow1" 200 -` (also `flow2`, `flow3`, `batch`) | Message(s) published to RabbitMQ; the response body names `handled_by_node` | 1, 2, 40 |
| `[Flow N] Published #...` just before a `200` | Confirms the publish happened before the response | 2, 44 |
| `... 400 -` on a `POST` | Body is not valid JSON, or `/zk/config` is missing `key`/`value` or uses an unknown key | 35 |
| `... 404 -` | Unknown path or method, or `/swift/object/<name>` not found in Swift | 50, 55 |
| `ERROR POST <path> error: ...` (with a traceback) followed by `... 500 -` | The publish failed or a field has the wrong type; the text after `error:` is the exception | 13, 25, 42 |
| `"GET /swift/objects..." 502 -` | Swift list failed (proxy down, bad credentials); the body holds the error | 56, 57, 58 |
| `"OPTIONS ..." 204 -` | Browser CORS pre-flight request | any |
| `code 501, message Unsupported method ('PUT')` | A method other than GET, POST or OPTIONS (for example `PUT`, `DELETE`) was sent; Python's server answers it itself | any |
| `code 400, message Bad request syntax`, `Bad request version`, `Bad HTTP/0.9 request type` | Something that is not HTTP reached port 8082, for example a TLS handshake or a port scanner. The request line shows as unreadable bytes. | any |
| A bare `Traceback (most recent call last):` with no `[http]` line after it | A handler raised an exception that API Gateway (Process 1) did not catch (the eventlet worker prints it to stderr), so no response was written. The client sees a dropped connection and HAProxy typically logs `502` with flags `SH`. Example: `limit=abc` on `/swift/objects`. | 48 |
| `BrokenPipeError`, `ConnectionResetError` in a traceback | The client (or HAProxy) closed the connection before the response was written. HAProxy shows flags `cD`/`CD`. | 20, 21 |
| `HTTP accept error:` | The listening socket failed (not an idle timeout, which is filtered out) | any |
| `HTTP server listening on 0.0.0.0:8082`, `HTTP server stopped.` | API Gateway (Process 1) started or stopped serving on the backend port | 11, 12, 16–19 |
| `All greenthreads started`, `Shutdown signal` | API Gateway (Process 1) start and stop (same journal) | 11, 12 |

The tracebacks go to stderr, which the unit also sends to the journal, so they appear under the same `-u flowfirst-process1`. Because the handler runs inside the process, a `500` with `error:` means API Gateway (Process 1) caught the problem and answered, whereas a traceback with no access line means it did not.

#### D. Build a timeline and compare the nodes

```bash
# Requests by client, method, path and status on this node (probes removed; query strings stripped)
sudo journalctl -u flowfirst-process1 -o cat --since "15 min ago" --no-pager \
  | grep -F '[http]' | grep -v "GET /health HTTP/1.0" \
  | sed -E 's/.*\[http\] ([^ ]+) — "([A-Z]+) ([^ ]+) [^"]*" ([0-9]+).*/\1 \2 \3 \4/' \
  | awk '{sub(/\?.*/,"",$3); n[$1" "$2" "$3" "$4]++} END{for(k in n) print n[k], k}' | sort -k4
```
```
  1 192.168.1.11 GET /nope 404
  2 192.168.1.11 GET /health 200
  3 192.168.1.11 POST /api/flow1 200
  1 192.168.1.11 POST /api/flow1 400
  1 192.168.1.11 POST /api/flow1 500
```

Compare the three nodes. This shows whether round-robin is even, and which node served a failing request. The loop runs from any of node1–node3:

```bash
for n in ${NODE1_IP} ${NODE2_IP} ${NODE3_IP}; do
  echo "===== ${n} ====="
  ssh "${n}" "sudo journalctl -u flowfirst-process1 -o cat --since '15 min ago' --no-pager \
    | grep -F '[http]' | grep -v 'GET /health HTTP/1.0' \
    | grep -cE '\" [0-9]{3} '; \
    sudo journalctl -u flowfirst-process1 -o cat --since '15 min ago' --no-pager \
    | grep -E 'error:|Traceback| (4|5)[0-9]{2} -'"
done
```

For each node the first number is the count of non-probe requests; the lines after it are the errors. To follow one incident across a stop and start of a process (Scenarios 11 and 12), look for a gap in the `HTTP/1.0` probe lines on that node and for `HTTP server stopped.` followed by `HTTP server listening`:

```bash
sudo journalctl -u flowfirst-process1 -o short-iso --since "30 min ago" --no-pager | grep -E "HTTP server (listening|stopped)|Shutdown signal|All greenthreads started"
```

#### E. Line up one request with the HAProxy log

The two logs have no shared request ID. Match them by time, path and status:

```bash
# On the VIP holder (HLOG as in Scenario 48): the request as HAProxy saw it, including the real client and the chosen backend
sudo grep -E 'POST /api/flow1' ${HLOG} | tail -3
# On the backend node it names: the same request as API Gateway (Process 1) saw it
sudo journalctl -u flowfirst-process1 -o short-iso --since "2 min ago" --no-pager | grep -F 'POST /api/flow1'
```

HAProxy's `flowfirst_api_back/node2` field and the `handled_by_node` value in the response must name the same node as the journal you are reading. Read the two sides together:

| HAProxy log | API Gateway (Process 1) journal | Likely cause |
|---|---|---|
| `200` | `200` | Healthy |
| `500` | `500` with `POST ... error:` | API Gateway (Process 1) answered with an error; the cause is in the `error:` text |
| `502 ... SH` | traceback, no access line | Handler crashed before answering |
| `503` with `<NOSRV>` | nothing | No backend was up, so the request never reached API Gateway (Process 1) (Scenarios 12, 19) |
| `SC` flags, no backend | nothing | Connection refused; API Gateway (Process 1) is stopped on that node (Scenarios 11, 16) |
| `504 ... sH` | access line shows a late `200`, or nothing | Backend took longer than `timeout server 1m` (Scenarios 20, 21) |

#### F. Watch live while running another scenario

```bash
# On the node under test, in a second terminal (use '-f' on the journal, not a log file)
sudo journalctl -u flowfirst-process1 -o cat -f \
  | grep --line-buffered -v "GET /health HTTP/1.0" \
  | grep --line-buffered -E '\[http\]|error:|Traceback|HTTP server'
```

#### G. Manage the log: volume and retention

The request lines are `INFO` records, so `LOG_LEVEL=WARNING` in `.env` (and a restart) turns the access lines off together with the other `INFO` messages; do that only if you accept losing them. The same lines are also in `/var/log/flowfirst/api_gateway.log` with a timestamp, which is easier to grep by time than the journal (Scenario 60). Their volume is set by the health probes (about 28,800 lines per day per node) plus your own traffic, and by `/api/batch` (one access line per call, not per message). Retention and rate limits are the journald settings in Scenario 51 G: if a load test produces more than the journal's rate limit, you will see `Suppressed N messages from flowfirst-process1.service` and the missing lines will not be in the journal.

```bash
# Total journal size on this node, and whether messages were dropped
sudo journalctl --disk-usage
sudo journalctl --since "1 hour ago" --no-pager | grep -E "Suppressed [0-9]+ messages.*flowfirst-process1"
```

---

## Pipeline Process Log Files

### Scenario 60: Pipeline Process Log Files (`/var/log/flowfirst`)

> **Runs on:** **each node that runs a pipeline process**: node1–node3 for Processes 1–4, and node4 for Remote Worker (Process 5). Each node's files hold only what that node's processes did. The cross-node trace in D runs from any of node1–node4 over `ssh`.

Every pipeline process logs through Python's `logging` module and writes each record twice: to stdout (the systemd journal) and to its own rotating file, `<LOG_DIR>/processN.log`. The file adds what the journal line lacks: a timestamp, the logger name, and the full traceback of every caught error. Scenarios 47–51 and 59 read the journal; this scenario reads the files.

Settings in `.env` (see `src/config/.env.example`), all optional:

| Variable | Default | Meaning |
|---|---|---|
| `LOG_LEVEL` | `INFO` | `DEBUG`, `INFO`, `WARNING` or `ERROR`. `pika`, `kazoo`, `urllib3` and `swiftclient` are held at `WARNING` regardless. |
| `LOG_DIR` | `/var/log/flowfirst` | Directory for the files. Blank turns file logging off (journal only). |
| `LOG_MAX_BYTES` | `52428800` (50 MiB) | A file is rotated when it reaches this size. |
| `LOG_BACKUP_COUNT` | `7` | Rotated files kept per process (`processN.log.1` … `.7`), so at most 8 files per process. |

The unit files set `LogsDirectory=flowfirst`, so systemd creates `/var/log/flowfirst` owned by `flowuser` when a process starts. Under the service sandbox (Phase 4, Step 4c) this is the only directory the process can write, and new log files are mode `0640`: a user who is not `flowuser` needs `sudo` or the `flowuser` group to read them. Re-run `sudo ./systemd/install_services.sh /opt/flowfirst` (add `--remote` on node4), then `sudo systemctl daemon-reload` and restart the processes on a node installed before this change.

#### A. Find the files and confirm logging started

```bash
# On each node
sudo ls -lh /var/log/flowfirst/
sudo head -3 /var/log/flowfirst/api_gateway.log
```
```
2026-06-15 14:00:01,214 INFO     flowfirst: [gt] Logging to /var/log/flowfirst/api_gateway.log (level=INFO, rotate at 52428800 bytes, keep 7)
2026-06-15 14:00:01,215 INFO     src.gt.gt: [gt] eventlet monkey-patch applied.
2026-06-15 14:00:01,300 INFO     flowfirst.p1: Starting (node=node1, pool=1000) ...
```

If the directory is missing, empty, or the first line is absent, check in this order:

```bash
sudo journalctl -u flowfirst-process1 -o cat --since "10 min ago" --no-pager | grep -F "file logging disabled"
grep -E "^LOG_" /opt/flowfirst/.env
sudo systemctl show flowfirst-process1 -p LogsDirectory
```

*`file logging disabled — cannot use LOG_DIR`* is a warning, not a failure: the process keeps running and logs to the journal only. The text after it gives the reason (usually permission denied, or `LogsDirectory` missing from an old unit file). A process run by hand as another user needs write access to `LOG_DIR`, or `LOG_DIR=` set to a directory it can write.

#### B. Line format and levels

`<date time,ms> <LEVEL> <logger>: <message>`. The logger name tells you which process wrote it: `flowfirst.p1` … `flowfirst.p5` for the pipeline code, `src.gt.gt`, `src.zookeeper.zk` and `flowfirst.swift` for the shared modules.

| Level | Used for | Example |
|---|---|---|
| `INFO` | Normal operation: process start/stop, each message consumed or forwarded, persisted rows, HTTP access lines | `[Flow 1] Persisted to MariaDB (row_id=..., counter=..., steps=...)` |
| `WARNING` | Degraded but working: ZooKeeper unavailable, election failed, MariaDB init failed, duplicate skipped by the dedup barrier, Swift store failed (the message is still saved to MariaDB) | `[Flow 2] DUPLICATE via ZK dedup — skipping insert for <id>` |
| `ERROR` | A message or request failed. The record is followed by a traceback. The message is rejected with `requeue=False`: kept in `flowfirst_dead_letters` if the dead-letter policy is applied, **discarded** if not (see Message Safety). | `[Flow 1] Error: ...` |
| `CRITICAL` | An uncaught exception ended the process. The traceback is logged before it exits, so a crash is no longer visible only in the journal. | `Uncaught exception` |
| `DEBUG` | Per-message detail such as the DB Updater (Process 4) history steps and ZK dedup first-seen | `  [process2_examined] @ ...` |

```bash
# Everything that went wrong on this node today, with tracebacks
sudo grep -E " (WARNING|ERROR|CRITICAL) " /var/log/flowfirst/process*.log | tail -30
# Only failed messages, with the first line of the exception
sudo grep -A3 " ERROR " /var/log/flowfirst/process4.log | tail -20
```

Unlike the journal (Scenario 51 A), the file has a real level on every line, so `grep " ERROR "` is reliable and does not depend on the `LOG_LEVEL` prefix style.

#### C. Message phrasebook — what each line means

| Log text (grep for) | Meaning | Typical scenarios |
|---|---|---|
| `Logging to ... (level=...)` | File logging started; shows the active settings | any |
| `Starting (node=..., pool=...)` / `All greenthreads started` | Process start-up | 11, 12, 39 |
| `Shutdown signal` / `... greenthread stopped.` | Clean stop (SIGTERM from systemd or Pacemaker) | 11, 12, 16–19 |
| `ZK ready` / `ZK service registered` | ZooKeeper connected | 34, 38 |
| `WARNING ZK unavailable ... degraded mode` / `standalone mode` | ZooKeeper could not be reached; the process runs without leader election or dedup | 36 |
| `WARNING ZK election failed ... single-node mode` | Election raised an error; the process runs on this node without the leader gate | 36 |
| `WARNING MariaDB init failed` | DB Updater (Process 4) could not create or check the schema at start-up | 14, 24 |
| `DUPLICATE via ZK dedup` | Failover re-delivery was caught before the insert | 37 |
| `Swift store note` | Swift write failed; the row is still in MariaDB | 56–58 |
| `ERROR [Flow N] Error:` + traceback | One message failed and was dead-lettered | 13, 25, 42 |
| `ERROR POST ... error:` + traceback | A REST request failed with HTTP 500 | 59 |
| `ERROR dedup-reaper error` | DB Updater (Process 4) could not clean expired dedup znodes | 37 |
| `CRITICAL Uncaught exception` | The process crashed; systemd will restart it (`Restart=always`) | any |

#### D. Trace one message across all processes

Every message carries a `message_id`, and DB Updater (Process 4) and Remote Worker (Process 5) log it as `msg_id=`. The earlier hops log the `item_id`, so start from the item you sent (Scenario 2) and find its id:

```bash
# On the node that handled the POST (handled_by_node in the response)
sudo grep -h "#9001" /var/log/flowfirst/api_gateway.log

# From any of node1–node4: every log line that mentions the item, in time order
for n in ${NODE1_IP} ${NODE2_IP} ${NODE3_IP} ${NODE4_IP}; do
  ssh "${n}" "sudo grep -H 'item #9001' /var/log/flowfirst/process*.log 2>/dev/null"
done | sed -E 's#^/var/log/flowfirst/##' | sort -t: -k2
```

Expected for a Flow 1 item: lines from `api_gateway.log` (`Published`), `process2.log` (`Received` and `Reflected`), `process3.log` (`Picked up` and `Forwarded`) and `process4.log` (`Consumed` and `Persisted to MariaDB`), each on whichever node held that process's leader role. A hop that is missing from the list shows where the message stopped. Use `grep -B1 -A5` on the `ERROR` line of the next process to read the traceback.

#### E. Watch live while running another scenario

```bash
# On the node under test, in a second terminal
sudo tail -F /var/log/flowfirst/process*.log | grep --line-buffered -E " (WARNING|ERROR|CRITICAL) |Traceback"
```

`tail -F` (capital F) keeps following after rotation.

#### F. Change verbosity or retention

```bash
# Example: more detail on one node
sudo sed -i 's/^LOG_LEVEL=.*/LOG_LEVEL=DEBUG/' /opt/flowfirst/.env
sudo pcs resource restart flowfirst-p4-res-clone      # on node4: sudo systemctl restart flowfirst-process5
# Revert when finished
sudo sed -i 's/^LOG_LEVEL=.*/LOG_LEVEL=INFO/' /opt/flowfirst/.env
```

Settings are read at process start, so a restart is required. `.env` is identical across nodes except for the node identity; change `LOG_*` on every node you want to affect.

#### G. Manage the files: size, rotation and retention

The application rotates its own files, so no `logrotate` rule is needed (and adding one for these files would conflict with the built-in rotation). With the defaults, one process uses at most 8 × 50 MiB = 400 MiB; Processes 1–4 together at most 1.6 GiB per node.

```bash
sudo du -sh /var/log/flowfirst
sudo ls -lh /var/log/flowfirst/process4.log*
# Reduce the footprint: smaller files and fewer backups, then restart the processes (F)
#   LOG_MAX_BYTES=10485760   LOG_BACKUP_COUNT=3
```

Rotation renames `process4.log` to `process4.log.1`, shifts the older ones up, and deletes the oldest beyond `LOG_BACKUP_COUNT`. Old data is therefore **lost** once the limit is reached; copy a file off the node before it rotates away if you need it for an incident review. `INFO` volume is mostly one or two lines per message plus the API Gateway (Process 1) access lines, including HAProxy's health probes (about 28,800 per day, Scenario 59 B). If space matters more than detail, `LOG_LEVEL=WARNING` keeps only problems.

---

## Scale & Capacity Testing

### Scenario 61: Scalability Test — Thousands of Concurrent API Invocations (Four-Node Cluster)

> **Runs on:** the load generator on a **separate client host** that can reach the VIP (not a cluster node: it would use the CPU you are trying to measure; if you have no spare host, use node4, which runs only Remote Worker (Process 5)). It needs Python 3.9+ and a copy of this repository, but **no `pip install`** (`scripts/load_test_api.py` uses only the standard library). Observation commands run on **node1–node3** (HAProxy on the VIP holder; RabbitMQ, MariaDB and ZooKeeper on any of them) and on **node4** for Remote Worker (Process 5). Tuning changes are made on **all four nodes**.

This scenario answers one question: *how many API calls can the four-node deployment accept at the same time, how many messages per second can the pipeline actually finish, and which setting limits each?* It uses the other scenarios as building blocks: Scenario 1 (REST API), 9 (HAProxy dashboard), 40 (batch publishing), 43 (metrics), 45–51 and 60 (logs).

#### What "concurrent" means here, and why there are two limits

The load generator keeps a fixed number of **virtual users (VUs)**. Each VU sends one request, waits for the answer, then sends the next (a closed loop), so *N* VUs means *N* requests in flight. Throughput follows from that: `requests/s ≈ VUs ÷ (latency + think time)`. To model a real population rather than a hammer, set `LOAD_THINK_MS`: 5000 users who each call once every 10 s is `LOAD_STEPS=5000` with `LOAD_THINK_MS=10000` (about 500 req/s).

A `200` from `POST /api/flow1` only means *API Gateway (Process 1) published the message to RabbitMQ*. The message is finished when DB Updater (Process 4) has stored it in MariaDB and Swift. The pipeline therefore has two capacities, and the test measures both:

| Capacity | What it is | How the test shows it |
|---|---|---|
| **Intake** | How many API calls HAProxy and API Gateway (Process 1) accept per second at acceptable latency | Per-step `req/s`, `p95`, `err` |
| **Completion** | How many messages per second Processes 2–5, MariaDB and Swift finish | Queue backlog during the run, and the **drain time** afterwards |

If intake is higher than completion, every request still succeeds, but the RabbitMQ backlog grows without limit. A test that only looks at HTTP status would report success for a system that is falling behind.

#### Scale parameters

All have defaults that reproduce the previous fixed behaviour, so doing nothing changes nothing. They live in `.env` (documented in `src/config/.env.example`), are identical on every node, and are read at process start (`HAPROXY_*` when `haproxy/setup_haproxy.sh` is run).

| Variable | Default | Limits | Raise it when |
|---|---|---|---|
| `API_REQUEST_POOL_SIZE` | `0` (= 40) | Requests **one** API Gateway (Process 1) serves at the same time. When all slots are busy the accept loop pauses and new connections wait in the kernel queue. | Latency rises with VUs while `req/s` stays flat (Little's law: the slots are full) |
| `API_LISTEN_BACKLOG` | `50` | Connections queued for API Gateway (Process 1) while all slots are busy. The kernel caps it at `net.core.somaxconn` (4096 on RHEL 9). | HAProxy shows connect errors / `503` / `504` under bursts |
| `GT_CONSUMERS_PER_QUEUE` | `1` | Parallel consumers per queue in P2–P5. Each is a greenthread with its **own** RabbitMQ connection. | Queues grow during a step and drain slowly afterwards |
| `GT_PREFETCH_COUNT` | `1` | Messages in flight per consumer. `1` means a network round trip per message. | Same as above, once consumers are no longer the limit |
| `HAPROXY_MAXCONN` | `4000` | `global maxconn` of HAProxy: concurrent client connections for the whole process. Once reached, HAProxy stops accepting and new clients wait in the kernel queue. | Peak VUs (or client connections, with `LOAD_KEEPALIVE=true` one per VU) approach this |
| `HAPROXY_FRONTEND_MAXCONN` | `3000` | `maxconn` per frontend (`defaults` section). It is the lower of the two for the API frontend, so it is the first one you hit. | Peak VUs approach this |
| `GT_POOL_SIZE` | `1000` | Greenthreads per process; must exceed consumers + helper threads | Only if you raise `GT_CONSUMERS_PER_QUEUE` very far |
| `ZK_DEDUP_TTL_MS`, `GT_DEDUP_REAP_INTERVAL_S` | `300000`, `600` | How long DB Updater (Process 4) dedup znodes live and how often they are cleaned | `zk_znode_count` keeps climbing (see D) |
| `SWIFT_ENABLED` | `true` | `false` skips the Swift write so you can measure the MariaDB-only ceiling | To separate Swift from MariaDB (see B) |
| `LOG_LEVEL` | `INFO` | `INFO` writes at least one line per request and per message to the journal and to `/var/log/flowfirst/` | `WARNING` removes that overhead for the final capacity run |

The load generator has its own `LOAD_*` variables (target, `LOAD_STEPS`, `LOAD_STEP_DURATION_S`, `LOAD_RAMP_S`, `LOAD_FLOW_MIX`, `LOAD_THINK_MS`, `LOAD_TIMEOUT_S`, `LOAD_KEEPALIVE`, `LOAD_SLO_P95_MS`, `LOAD_MAX_ERROR_PCT`, `LOAD_ITEM_ID_BASE`, `LOAD_DRAIN_TIMEOUT_S`, `LOAD_REPORT_INTERVAL_S`, `RABBITMQ_MGMT_PORT`). Each is explained next to its default in `src/config/.env.example`, and each can be overridden on the command line (`python3 scripts/load_test_api.py --help`).

> **What the code implies for scale-out (check it, do not assume it).** Each process wraps its work in a ZooKeeper leader election (`LeaderElection(...).run(callback)`), and kazoo's `Election.run` holds a lock for the whole callback. Read literally, that means **only the elected leader of each of Processes 1–4 does the work**, and the other nodes wait as standbys; Remote Worker (Process 5) on node4 has no election. If that is what your cluster does, adding nodes adds failover, not capacity, and the levers are the per-process parameters above. Section A measures which case you have.

#### A. Pre-flight: limits that cap the test before the application does

```bash
# 1. Client host: the generator needs about one file descriptor per VU
ulimit -n                      # needs to exceed your largest LOAD_STEPS value + ~200; the script raises the soft limit up to the hard limit
ulimit -n 65536                # or raise it in /etc/security/limits.d/ for a permanent change

# 2. On node1, node2, node3: kernel accept queue (caps API_LISTEN_BACKLOG) and open-file limit of API Gateway (Process 1)
sysctl net.core.somaxconn
sudo cat /proc/$(pgrep -f 'src.application.process1' | head -1)/limits | grep "open files"     # 65536 from the unit file

# 3. Which nodes actually answer API calls right now?
curl -s http://${FLOWFIRST_VIP}:8080/zk/health | jq -c '.pipeline_health | map_values(keys)'
for n in ${NODE1_IP} ${NODE2_IP} ${NODE3_IP}; do echo -n "${n}: "; ssh "${n}" "sudo ss -tln | grep -c ':8082 '"; done      # 1 = API Gateway (Process 1) is listening, 0 = not
echo "show stat" | ssh ${NODE1_IP} "sudo socat stdio /var/lib/haproxy/stats" | cut -d, -f1,2,18 | grep flowfirst_api_back   # UP / DOWN per backend (run on the VIP holder)

# 4. MariaDB connection ceiling (DB Updater (Process 4) opens a connection per insert)
mariadb -h ${FLOWFIRST_VIP} -u ${MARIADB_USER} -p"${MARIADB_PASSWORD}" -e "SHOW VARIABLES LIKE 'max_connections'; SHOW GLOBAL STATUS LIKE 'Max_used_connections';"

# 5. Record the starting point: queues must be empty and the stack healthy
ssh ${NODE1_IP} "sudo rabbitmqctl list_queues name messages messages_unacknowledged consumers"
curl -s http://${FLOWFIRST_VIP}:8080/health | jq -r '.status, .node'
```

*Expected:* `somaxconn` 4096; API Gateway (Process 1) listening on port 8082 on every node you intend to serve the API (the count in step 3); every `flowfirst_api_back` server `UP`; all queues at 0 messages. If step 3 shows only one node listening and the other two backends `DOWN` in HAProxy, you have the single-leader case described above, and the `served by` line in C will show a single node. Record which case you have; it decides how to read every later result.

The test writes real data. Choose an unused `LOAD_ITEM_ID_BASE` (the default `9000000` is well above the item ids used by the other scenarios), and expect a few hundred thousand rows, objects and ZooKeeper znodes for a full run.

#### B. Baseline at defaults, then find where it breaks

Run the climb with the defaults first, so every later change has something to be compared with. On the client host, from the repository root (it reads `.env` from the repository root or `/opt/flowfirst/.env` for the VIP, node IPs and RabbitMQ credentials):

```bash
# Quick sanity run first: 3 small steps, short, and a dry run that prints the plan without sending anything
python3 scripts/load_test_api.py --dry-run
python3 scripts/load_test_api.py --steps 10,40,100 --duration 15 --ramp 3 --yes

# The full climb (defaults: 50,100,250,500,1000,2000,4000 concurrent; ~35 s per step; stops at the first failing step)
python3 scripts/load_test_api.py --csv baseline.csv
```

Each step prints one line; the format is shown below with **illustrative numbers** (your values will differ):

```
Step 250 concurrent ...
  FAIL    250 VUs     312.4 req/s  p50    712  p95   1840  p99   2210  max   3105 ms  err  0.00%  queue 0 -> 2150 (max 2150)
        served by: node1=10931
        reason : p95 1840 ms > 1000 ms
```

A step **passes** when the error rate is at most `LOAD_MAX_ERROR_PCT` (default 1 %), p95 latency of successful requests is at most `LOAD_SLO_P95_MS` (default 1000 ms), and the generator itself kept up. The last criterion is checked through event-loop lag: if the line says `LOAD GENERATOR SATURATED`, the client host, not the cluster, is the limit. Add client hosts and run the script on each with a different `LOAD_ITEM_ID_BASE` (for example `9000000`, `19000000`, `29000000`), then add the throughput figures.

The script exits `0` when the last step passed, `1` when a step failed, and `2` when it could not start (target unreachable, file-descriptor limit too low). `--csv` saves one row per step for comparing runs.

**Reading the result.** Find the first failing step and match its symptom:

| Symptom in the step line | Probable bottleneck | Confirm with | Lever |
|---|---|---|---|
| `req/s` stops growing while `p50`/`p95` grow in proportion to VUs, no errors | API Gateway (Process 1) request slots full (accept loop paused) | HAProxy `qcur` > 0 on backends, API Gateway (Process 1) near 100 % of one core | `API_REQUEST_POOL_SIZE` |
| `errors : timeout`, HAProxy `scur`/`qcur` high, `econ` or `hrsp_5xx` rising | Backlog full or backends saturated | Step D; `ss -ltn` `Recv-Q` on port 8082 | `API_LISTEN_BACKLOG`, `API_REQUEST_POOL_SIZE` |
| `req/s` flat, latency rising, API Gateway (Process 1) **below** 100 % of a core, RabbitMQ healthy | API Gateway (Process 1) publishes through **one shared channel**, one message at a time (a semaphore around `basic_publish`), and builds each message with `json.dumps(indent=2)` | Compare `req/s` of `POST` mixes with `health:1` (`--mix health:1`, which skips publishing) | No setting changes this; it is a code limit. Report it as the per-node intake ceiling |
| `errors : reset` or `refused` at a round VU count (about 2000, 3000) | HAProxy `maxconn` reached | HAProxy `scur` equal to `slim`, `Connection refused` from the VIP | `HAPROXY_MAXCONN`, `HAPROXY_FRONTEND_MAXCONN` |
| `http_503` | No backend available, or HAProxy queue timeout | HAProxy log `<NOSRV>` or `sQ` flags (Scenario 48) | Fix the down backend; see the two rows above |
| `http_500` | Publish failed inside API Gateway (Process 1) | `grep " ERROR " /var/log/flowfirst/api_gateway.log` | Usually RabbitMQ; see Scenario 46 |
| `served by:` lists one node although three are `UP` | Not a bug in the test: only one API Gateway (Process 1) receives traffic | Step A.3 | See the note on leader election above |
| All HTTP 200, **queue max keeps growing**, drain takes minutes | Completion capacity lower than intake | Step D (queues), then E (drain time) | `GT_CONSUMERS_PER_QUEUE`, `GT_PREFETCH_COUNT` |
| Queue grows only in `flow1_p3_to_p4` / `flow2_p3_reflected` / `flow3_p2_to_p4` | DB Updater (Process 4): MariaDB or Swift | Step D; repeat with `SWIFT_ENABLED=false` | Consumers for DB Updater (Process 4); see below |
| `LOAD GENERATOR SATURATED` | Client host | `top` on the client | More client hosts |

With [Monitoring and Alerting](#monitoring-and-alerting) enabled, `FlowFirstQueueBacklog`, `FlowFirstQueueNoConsumers` and `FlowFirstApi5xxRate` raise the same symptoms outside a load test; the levers below are the same.

To tell MariaDB from Swift as the DB Updater (Process 4) limit, run the same step twice: once normally and once after setting `SWIFT_ENABLED=false` on node1–node3 and restarting DB Updater (Process 4) (restart as in C). DB Updater (Process 4) does a ZooKeeper write, a MariaDB insert on a new connection, and, with Swift enabled, a container check plus an object upload through a fresh Swift connection for every message, so Swift is a likely cost. Set it back to `true` afterwards; with it off nothing is written to Swift.

#### C. Apply a tuning change and re-run

Change parameters on **every** node (node4 too, for Remote Worker (Process 5)), restart, and repeat B. The helper below replaces an existing `KEY=` line or appends the key when it is missing, so running it twice does not create duplicates. Run it from any of node1–node4 with passwordless `ssh` and `sudo`:

```bash
setenv() {   # usage: setenv KEY VALUE   — applies to all four nodes
  for n in ${NODE1_IP} ${NODE2_IP} ${NODE3_IP} ${NODE4_IP}; do
    ssh "${n}" "grep -q '^$1=' /opt/flowfirst/.env && sudo sed -i.bak 's|^$1=.*|$1=$2|' /opt/flowfirst/.env || echo '$1=$2' | sudo tee -a /opt/flowfirst/.env >/dev/null"
  done
}

# Example: a first attempt for roughly 2000-4000 concurrent callers. These are STARTING POINTS to be tuned by measurement.
setenv API_REQUEST_POOL_SIZE 2000
setenv API_LISTEN_BACKLOG 2048
setenv GT_CONSUMERS_PER_QUEUE 8
setenv GT_PREFETCH_COUNT 10
setenv HAPROXY_MAXCONN 20000
setenv HAPROXY_FRONTEND_MAXCONN 10000

# Restart the pipeline processes (Pacemaker manages P1-P4; P5 on node4 is a plain systemd unit)
for p in 1 2 3 4; do sudo pcs resource restart flowfirst-p${p}-res-clone; done
ssh ${NODE4_IP} "sudo systemctl restart flowfirst-process5"

# HAProxy limits are baked into /etc/haproxy/haproxy.cfg: regenerate on each of node1-node3, validate, then restart on the VIP holder
for n in ${NODE1_IP} ${NODE2_IP} ${NODE3_IP}; do ssh "${n}" "cd /opt/flowfirst && sudo ./haproxy/setup_haproxy.sh >/dev/null && sudo haproxy -c -f /etc/haproxy/haproxy.cfg"; done
sudo pcs resource restart haproxy-res          # brief interruption for new connections; see Scenarios 15 and 17
```

Confirm the new values took effect before the next run: `journalctl -u flowfirst-process1 -o cat | grep "HTTP server listening"` should show the new `GreenPool size` and `listen backlog`, and `grep maxconn /etc/haproxy/haproxy.cfg` the new limits. Re-run B with a different `--csv` name and compare the two files.

Know what each lever costs:

- **`GT_CONSUMERS_PER_QUEUE` > 1** lets several consumers read the same queue, so messages of one queue may finish **out of order**. Nothing in this pipeline depends on order across items; check that this holds for your use before raising it.
- **`GT_PREFETCH_COUNT` > 1** means that after a consumer crash up to that many unacknowledged messages are redelivered. DB Updater (Process 4) is protected by the ZooKeeper dedup barrier; Processes 2, 3 and 5 are not, so a crash can produce a duplicate downstream message (DB Updater (Process 4) still stores each `message_id` once).
- **One process is one CPU core.** Eventlet runs all greenthreads of a process on a single OS thread. If `top` shows a pipeline process at 100 % of one core, more consumers or request slots will not help.
- **Failures lose messages unless the dead-letter policy is applied.** Consumers reject failed messages with `requeue=False`; without the `flowfirst-dlx` policy ([Message Safety](#message-safety-publisher-confirms-and-the-dead-letter-queue)) RabbitMQ then discards them. With it they wait in `flowfirst_dead_letters`. Under overload the difference between the `200` count and the MariaDB row count (step E) is the number of messages lost or dead-lettered; check the dead-letter queue before calling them lost.

#### D. Observe the cluster during a run

Open these in separate terminals while a step is running.

```bash
# HAProxy (on the VIP holder): current and peak connections, queue, errors and 5xx per server
echo "show stat" | sudo socat stdio /var/lib/haproxy/stats | awk -F, 'NR==1{sub(/^# /,""); for(i=1;i<=NF;i++) c[$i]=i; next}
  $1 ~ /^flowfirst_api_(front|back)$/ {printf "%-20s %-9s %-5s scur=%s smax=%s slim=%s qcur=%s qmax=%s ereq=%s econ=%s 5xx=%s\n",$1,$2,$c["status"],$c["scur"],$c["smax"],$c["slim"],$c["qcur"],$c["qmax"],$c["ereq"],$c["econ"],$c["hrsp_5xx"]}'

# RabbitMQ (any of node1-node3): backlog per queue; 'unacked' is messages currently being worked on
watch -n 5 "sudo rabbitmqctl list_queues name messages messages_ready messages_unacknowledged consumers"

# CPU of the pipeline processes (every node): a process pinned at ~100% is out of single-core headroom.
# Use pidstat (sudo dnf install -y sysstat): it reports the CPU used during the last 5 s. 'ps -o pcpu' is NOT suitable,
# because it averages over the whole lifetime of the process and hides a process that is busy right now.
watch -n 6 "pidstat -u -l 5 1 | grep -E 'Average:.*(src.application|beam.smp|mariadbd|haproxy)'"

# MariaDB / Galera: connections and replication back-pressure
mariadb -h ${FLOWFIRST_VIP} -u ${MARIADB_USER} -p"${MARIADB_PASSWORD}" -e "SHOW GLOBAL STATUS WHERE Variable_name IN ('Threads_connected','Max_used_connections','wsrep_flow_control_paused','wsrep_local_recv_queue_avg');"

# ZooKeeper: DB Updater (Process 4) creates one dedup znode per message, kept for ZK_DEDUP_TTL_MS
echo mntr | nc ${NODE1_IP} 2181 | grep -E "zk_znode_count|zk_outstanding_requests|zk_avg_latency"

# API Gateway (Process 1) request log rate (journal) and errors (file; see Scenarios 59 and 60)
sudo grep -c " ERROR " /var/log/flowfirst/process*.log
```

How to read them:

- **HAProxy `scur` approaching `slim`**: you are at the `maxconn` limit. **`qcur` > 0 on a backend**: API Gateway (Process 1) cannot take requests as fast as HAProxy sends them.
- **A RabbitMQ queue that keeps rising** while the other queues are near zero names the slow stage; the queue *before* a stage is that stage's input.
- **`wsrep_flow_control_paused` well above 0** (fraction of time replication was throttled) means Galera, not DB Updater (Process 4), is the limit: one slow node holds back all writers.
- **`Max_used_connections` close to `max_connections`** will start refusing DB Updater (Process 4) inserts, which then appear as `[Flow N] Error` lines and lost messages.
- **`zk_znode_count`** grows by one per stored message and falls only when the reaper runs (`GT_DEDUP_REAP_INTERVAL_S`, default every 10 minutes, removing znodes older than `ZK_DEDUP_TTL_MS`, default 5 minutes). At several hundred messages per second this reaches hundreds of thousands of znodes; the reaper deletes them one by one and a very large child list is expensive in ZooKeeper. If the count keeps climbing, lower `GT_DEDUP_REAP_INTERVAL_S` (for example to `60`) and keep `ZK_DEDUP_TTL_MS` longer than the longest redelivery window you need.

#### E. After the run: drain time and an end-to-end check

The generator waits up to `LOAD_DRAIN_TIMEOUT_S` (default 300 s) for the queues to empty and prints either `Drained in N s` or `NOT drained`. The drain time is the honest measure of completion capacity: `backlog ÷ drain time` is the sustained messages-per-second the pipeline can finish. If it reports that the RabbitMQ management API is not reachable, read the backlog with `rabbitmqctl list_queues` instead.

When the queues are empty, compare what was **accepted** with what was **stored**. The script prints the exact query and the expected counts at the end of the run:

```bash
mariadb -h ${FLOWFIRST_VIP} -u ${MARIADB_USER} -p"${MARIADB_PASSWORD}" ${MARIADB_DB} -e "
  SELECT flow_id, COUNT(*) AS stored FROM processed_messages
  WHERE item_id BETWEEN 9000001 AND <last item_id printed by the script> GROUP BY flow_id;"
```

*Expected:* `stored` for each flow equals the `2xx` count printed for that flow. If `stored` is **lower**, messages were lost between the API and MariaDB: look for `ERROR` lines in the process logs, and for the reason in `/var/log/flowfirst/process4.log`. If it is slightly **higher**, some requests timed out on the client but were still accepted by API Gateway (Process 1). Duplicates do not occur, because `message_id` is unique.

Cross-check Swift the same way as in Scenario 50 D (object count against row count). Under overload Swift writes can fail without failing the message: DB Updater (Process 4) logs `Swift store note` as a warning and still stores the row in MariaDB, so a Swift shortfall does not show up in the MariaDB count. Back-fill it with Scenario 58.

#### F. Choosing a result to report

Do not report the last step that happened to run. Report, for the configuration you tested:

1. The **highest passing concurrency** and the **peak passing throughput** printed in the summary (intake).
2. The **drain time** and implied completion rate (end-to-end).
3. The **limiting resource** from the table in B (for example "DB Updater (Process 4) at 100 % of one core" or "HAProxy `maxconn`").
4. The **parameter values** used (copy the `LOAD_*` and scale lines from `.env`) and the `--csv` file.
5. Whether the **`stored` count matched the `2xx` count**. A run that loses messages has not passed, whatever the HTTP numbers say.

For a final capacity run, set `LOG_LEVEL=WARNING` (removes per-request and per-message log lines), use the same `LOAD_STEPS` as the baseline, and repeat once to see run-to-run variation. A single run on a shared environment can be off by tens of percent.

#### G. Clean up

```bash
# Delete the test rows in small batches: one huge DELETE is a single Galera write-set and can fail or stall the cluster
for i in $(seq 1 1000); do
  n=$(mariadb -h ${FLOWFIRST_VIP} -u ${MARIADB_USER} -p"${MARIADB_PASSWORD}" ${MARIADB_DB} -N -B -e \
      "DELETE FROM processed_messages WHERE item_id BETWEEN 9000001 AND <last item_id> LIMIT 5000; SELECT ROW_COUNT();" | tail -1)
  [ "${n}" = "0" ] && break
  sleep 1
done

# Put the settings back to the defaults (or to the values you decided to keep) and restart as in C
setenv API_REQUEST_POOL_SIZE 0 ; setenv API_LISTEN_BACKLOG 50 ; setenv GT_CONSUMERS_PER_QUEUE 1 ; setenv GT_PREFETCH_COUNT 1
setenv HAPROXY_MAXCONN 4000 ; setenv HAPROXY_FRONTEND_MAXCONN 3000 ; setenv LOG_LEVEL INFO
```

ZooKeeper dedup znodes expire on their own. The Swift objects from the run stay in the container (named `flow<N>/<message_id>.json`); on a dedicated test cluster you can leave them, and they will show up as extra objects in the Scenario 50 D comparison.

---

## Performance Testing

### Scenario 62: Performance Test — Measured Latency, Throughput and Freshness for Client Expectations

> **Runs on:** the same **separate client host** as Scenario 61 (Python 3.9+, a copy of this repository, `.env` with the VIP and node IPs, passwordless `ssh` and `sudo` to node1–node4, and a `mariadb` client). Resource sampling runs on **node1–node4**; `pidstat` and `sar` come from the `sysstat` package (`sudo dnf install -y sysstat` on each node). Run it only after Scenario 61, because the profiles below must sit **below** the capacity found there.

> **Status of the numbers in this document.** Nothing in this scenario is a measurement. The procedure was assembled from the code and its helper commands were checked against a mock HTTP server and sample text only. Nothing was run on a real four-node cluster, and `pidstat`, `sar`, `ssh` and `mariadb` were not available where it was prepared. The result table in H is a template: every number that goes to a client must come from your own runs on your hardware, stated together with the conditions it was measured under.

Scenario 61 finds *where the system breaks*. This scenario answers a different question: *at the load a client actually expects, what response times, throughput and data freshness will they experience, and how much does it vary?* It therefore uses fixed, repeatable workloads, repeats them, and reports conditions together with results.

#### A. What to measure, and what to promise

| Measure | Meaning for the client | Where it comes from | Statistic to report |
|---|---|---|---|
| **API response time** | How long `POST /api/flowN` takes to return `200` ("accepted into the pipeline") | `scripts/load_test_api.py` | p50, p95, p99 (never only the average) |
| **Throughput** | Accepted requests per second, compared with the rate the profile should produce | same | req/s, expected vs achieved |
| **Error rate** | Share of calls that fail or time out | same | % per run |
| **Freshness (end-to-end latency)** | Seconds from the API call until the record is stored in MariaDB. This is what a user waiting for data to appear experiences, and it is **not** the API response time | MariaDB (section D) | p50, p95, p99, max, per flow |
| **Backlog and drain time** | Whether the pipeline keeps up, or accepts work faster than it finishes it | script (`queue`, `Drained in`) | max queue, seconds to empty |
| **Resource use** | CPU, memory, network and disk per node, which shows headroom | `pidstat` / `sar` (section E) | average and maximum per process |
| **Repeatability** | Whether the figure can be quoted at all | 3 repeated runs | spread between runs |
| **Stored equals accepted** | No message was lost | script summary and MariaDB | must match exactly |

Freshness is the figure clients usually care about most, and the one a plain HTTP benchmark cannot show.

#### B. Prepare: production settings, healthy cluster, recorded conditions

Measure the configuration you intend to ship. In particular do **not** switch `LOG_LEVEL` to `WARNING` just to look better unless production will run that way; use the tuning values you settled on in Scenario 61 and write them down. Then confirm the starting state and capture the conditions in one place:

```bash
cd /opt/flowfirst && set -a && source .env && set +a

# Healthy starting state (all must hold, otherwise the run measures a fault, not performance)
for n in ${NODE1_IP} ${NODE2_IP} ${NODE3_IP}; do ssh "$n" "chronyc tracking | grep -E 'Leap|System time'"; done     # freshness needs synchronised clocks, offset well below 1 s
mariadb -h ${FLOWFIRST_VIP} -u ${MARIADB_USER} -p"${MARIADB_PASSWORD}" -e "SHOW STATUS LIKE 'wsrep_cluster_size'; SHOW STATUS LIKE 'wsrep_local_state_comment';"   # 3, Synced
ssh ${NODE1_IP} "sudo rabbitmqctl list_queues name messages | awk '\$2>0'"                                          # prints nothing when every queue is empty
curl -s http://${FLOWFIRST_VIP}:8080/zk/health | jq -c '.pipeline_health | map_values(keys)'

# Conditions record: save it next to the results
{
  echo "date: $(date -Is)   commit: $(git -C /opt/flowfirst rev-parse --short HEAD)"
  for ip in ${NODE1_IP} ${NODE2_IP} ${NODE3_IP} ${NODE4_IP}; do
    ssh "$ip" "echo \"== \$(hostname -s): \$(nproc) vCPU, \$(free -g | awk '/Mem:/{print \$2}') GiB RAM, \$(uname -r)\""
  done
  grep -E '^(API_|GT_|HAPROXY_|LOG_LEVEL|SWIFT_ENABLED|ZK_DEDUP|LOAD_)[A-Z_]*=' /opt/flowfirst/.env
  echo "client host: $(hostname -s) -> VIP ${FLOWFIRST_VIP}"
} | tee perf_conditions.txt
```

Also record whether the nodes are virtual machines, the disk type behind `/var/lib/mysql`, `/srv/node` and RabbitMQ, the network between the client host and the VIP, and how many rows `processed_messages` already holds (a larger table makes inserts and the freshness query slower). Run the client host **where the real clients are**, or state that the response times exclude their network.

#### C. The workload profiles and the run protocol

A profile is a number of virtual users and a think time. The request rate follows from `req/s ≈ users ÷ (response time + think time)`. Take the profiles from the client's forecast (users × calls per user per minute). The three below are **examples to replace**, with the rate each should produce when the system keeps up:

| Profile | Users | Think time | Expected rate | Represents |
|---|---|---|---|---|
| `light` | 50 | 5000 ms | about 10 req/s | quiet period |
| `typical` | 250 | 2500 ms | about 100 req/s | normal business load |
| `peak` | 1000 | 2000 ms | about 500 req/s | busiest hour |

The flow mix is `LOAD_FLOW_MIX` (default flow1 45 %, flow2 45 %, flow3 10 %); set it to the client's real mix and record it. Each profile is run once as a **warm-up that is discarded** (connection pools, first Swift container creation, caches) followed by **three measured runs**. The helper below runs one of them, gives it its own non-overlapping `item_id` block so runs never collide, and returns non-zero when a step fails or the queues do not drain. Define it on the client host, in the repository root:

```bash
perf_run() {   # usage: perf_run <profile> <profile-index 0-4> <users> <think_ms> <run 0=warm-up|1..3> [measured-seconds]
  local base=$((9000000 + ($2 * 4 + $5) * 10000000)) dur=${6:-300}
  python3 scripts/load_test_api.py --steps "$3" --think-ms "$4" --ramp ${PERF_RAMP:-10} --duration "${dur}" \
      --interval ${PERF_INTERVAL:-30} --item-id-base "${base}" --drain-timeout 600 \
      --csv "perf_$1_run$5.csv" --yes | tee "perf_$1_run$5.log"
  ! grep -qE "NOT drained|^  FAIL " "perf_$1_run$5.log"
}

export PERF_RAMP=10 PERF_INTERVAL=30
for spec in "light 0 50 5000" "typical 1 250 2500" "peak 2 1000 2000"; do
  read -r name idx users think <<< "${spec}"
  perf_run "${name}" "${idx}" "${users}" "${think}" 0 120                    # warm-up, discarded
  for r in 1 2 3; do perf_run "${name}" "${idx}" "${users}" "${think}" "$r" 300; done
done
```

Each measured run is ten seconds of ramp plus five minutes of measurement, plus the drain wait. It writes `perf_<profile>_run<N>.csv` (end-of-run summary), `perf_<profile>_run<N>.windows.csv` (one row every `PERF_INTERVAL` seconds) and a `.log`. Run the sampling in E **at the same time**, and the freshness query in D **after** each run. Between runs the script waits for the queues to empty, so each run starts from an idle pipeline.

*Expected output of one run (illustrative values, not measurements):*

```
Step 250 concurrent ...
    measuring from 14:02:11 (t=0 below)
    t=   30s       98.7 req/s  p50     14  p95     41  p99     90 ms  err  0.00%  queue 3
    ...
  PASS     250 VUs      99.2 req/s  p50     14  p95     43  p99     95  max     310 ms  err  0.00%  queue 0 -> 6 (max 9)
```

Compare the achieved rate with the expected rate from the table. If `typical` shows 60 req/s where about 100 was expected, the system is already saturated at that profile (Scenario 61 symptoms), and the response-time figures describe an overloaded system, not the intended load.

#### D. Freshness: seconds from API call to a stored record

The first entry of `history_trail` is stamped by API Gateway (Process 1) when the request arrives (`process1_created`); `created_at` is set by MariaDB when DB Updater (Process 4) inserts the row. Their difference is the end-to-end latency, using only data that is already stored. Run it after the queues are empty, for each run:

```bash
perf_fresh() {   # usage: perf_fresh <perf_*.log>  — freshness in seconds per flow, for the item_id range of that run
  local lo hi; read -r lo hi < <(awk '/item_id range used/ {print $5, $7}' "$1")
  mariadb -h ${FLOWFIRST_VIP} -u ${MARIADB_USER} -p"${MARIADB_PASSWORD}" ${MARIADB_DB} -N -B -e "
    SELECT flow_id, TIMESTAMPDIFF(SECOND,
             STR_TO_DATE(JSON_VALUE(history_trail,'\$[0].timestamp'),'%Y-%m-%d %H:%i:%s'), created_at)
    FROM processed_messages WHERE item_id BETWEEN ${lo} AND ${hi};" |
  sort -k1,1n -k2,2n | awk -F'\t' '{ v[$1, ++n[$1]]=$2; s[$1]+=$2; if ($2<0) neg[$1]++ }
    function p(k,q,  i){ i=int(q*(n[k]-1))+1; return v[k,i] }
    END{ for(k in n) printf "flow%s rows=%d avg=%.1f p50=%d p95=%d p99=%d max=%d negative=%d (seconds)\n", k, n[k], s[k]/n[k], p(k,.50), p(k,.95), p(k,.99), v[k,n[k]], neg[k] }' | sort
}

perf_fresh perf_typical_run1.log
```

*Expected shape of the output (illustrative):* `flow1 rows=14980 avg=1.2 p50=1 p95=3 p99=5 max=9 negative=0 (seconds)`, one line per flow.

Read it with these limits in mind:

- **Resolution is one second, with up to ±1 s error.** Both timestamps are truncated to whole seconds (`time.strftime`, and `created_at` is a plain `TIMESTAMP`). Quote freshness in whole seconds and never claim sub-second accuracy.
- **`negative` must be 0.** A negative value means clock skew between the node that stamped the message and the node that inserted it (check `chronyc`), or a MariaDB `time_zone` that differs from the nodes' operating-system time zone (`SELECT @@global.time_zone, @@system_time_zone;`). Do not report freshness until this is resolved.
- It covers the whole path: RabbitMQ hops, Processes 2–4, the ZooKeeper dedup write and the MariaDB insert. It does **not** include the Swift upload, because the row is inserted before DB Updater (Process 4) writes to Swift. Use Scenario 50 D to confirm Swift keeps up.
- Per-stage timings are in `history_trail` too (`process2_*`, `process3_*`, `process4_saved_to_mariadb`), with the same one-second resolution. They are useful for finding which stage grew when freshness is poor, not for measuring it.

#### E. Resource use on every node

Sample CPU and memory of the pipeline processes, plus system CPU, memory, network and disk, during the whole run. Start it just before `perf_run`, with a length that covers the ramp, the measurement and the drain (below: 12 minutes for a 5-minute run):

```bash
perf_sample_start() {   # usage: perf_sample_start <label> <seconds>  — samples every 10 s on node1-node4
  local n=$(( $2 / 10 )) ip
  for ip in ${NODE1_IP} ${NODE2_IP} ${NODE3_IP} ${NODE4_IP}; do
    ssh "${ip}" "nohup sudo pidstat -u -r -h -l 10 ${n} > /tmp/perf_$1_pid.txt 2>&1 < /dev/null &
                 nohup sudo sar -u -r -d -n DEV 10 ${n} > /tmp/perf_$1_sar.txt 2>&1 < /dev/null &"
  done
}

perf_sample_collect() {   # usage: perf_sample_collect <label>  — summary per process on each node; raw files kept as perf_<label>_<node>_{pid,sar}.txt
  local ip
  for ip in ${NODE1_IP} ${NODE2_IP} ${NODE3_IP} ${NODE4_IP}; do
    scp -q "${ip}:/tmp/perf_$1_pid.txt" "perf_$1_${ip}_pid.txt"; scp -q "${ip}:/tmp/perf_$1_sar.txt" "perf_$1_${ip}_sar.txt"
    echo "== ${ip}"
    awk '/^#/ && /%CPU/ { for(i=1;i<=NF;i++){ if($i=="%CPU")c=i-1; if($i=="RSS")r=i-1; if($i=="Command")m=i-1 } next }
         c && $1 ~ /^[0-9]/ { k=$m; for(i=m+1;i<=NF;i++) k=k" "$i
           if (k !~ /src\.application|beam\.smp|mariadbd|haproxy|java|memcached|swift/) next
           n[k]++; s[k]+=$c; if($c>x[k]) x[k]=$c; if(!(k in f)) f[k]=$r; l[k]=$r }
         END{ printf "%8s %8s %10s %10s  %s\n","avg%CPU","max%CPU","RSS_kB_1st","RSS_kB_end","command"
              for(k in n) printf "%8.1f %8.1f %10d %10d  %s\n", s[k]/n[k], x[k], f[k], l[k], substr(k, length(k)-60) }' "perf_$1_${ip}_pid.txt" | { read -r hdr; echo "$hdr"; sort -k1,1nr; }
  done
}

perf_sample_start typical1 720          # then, immediately:  perf_run typical 1 250 2500 1 300
perf_sample_collect typical1            # after the run and the drain
```

How to read it:

- A process at an average near **100 % of one core** has no headroom: eventlet runs all greenthreads of a process on one core, so more users will only add latency. Compare the profile you are quoting with that figure. A reasonable (but arbitrary) convention is to want the `typical` profile well under 70 % average on the busiest pipeline process; your own policy decides.
- Remember the leader-election note in Scenario 61: if only one node's API Gateway (Process 1) and DB Updater (Process 4) work, the other nodes will show idle pipeline processes, and the headroom of that one node is the headroom of the system. If that node fails, the new leader inherits the whole load, so judge headroom with that in mind (see G).
- `RSS_kB_1st` against `RSS_kB_end` shows memory growth during the run; a short run cannot prove there is no leak, see F.
- `sar` files: read system CPU (`%idle`, `%iowait`), memory (`%memused`), network (`rxkB/s`, `txkB/s`) and disk (`%util`) per node with `less`. A disk at `%util` near 100 or an `%iowait` that stays high points at MariaDB, RabbitMQ or Swift storage. These `pidstat` and `sar` commands and the `ssh` wrappers were not exercised on a real node; the summary `awk` was run on sample `pidstat` text only. Check the first run's raw files before trusting the summary.

#### F. Repeatability and stability

**Run-to-run variation.** After three measured runs per profile, summarise them:

```bash
for f in perf_typical_run[1-3].csv; do
  awk -F, -v f="$f" 'NR>1{printf "%-22s %8.1f req/s  p50 %5.0f  p95 %5.0f  p99 %5.0f ms  err %s%%  %s\n", f,$4,$5,$6,$7,$9,$14}' "$f"
done
cat perf_typical_run[1-3].csv | awk -F, '$1 ~ /^[0-9]+$/ {n++; if(n==1||$4<rmin)rmin=$4; if($4>rmax)rmax=$4; if(n==1||$6<pmin)pmin=$6; if($6>pmax)pmax=$6}
  END{printf "runs=%d  req/s %.1f..%.1f (spread %.1f%%)  p95 %.0f..%.0f ms\n", n,rmin,rmax,100*(rmax-rmin)/rmax,pmin,pmax}'
```

Report the **worst** of the three for latency, and the range. As a working convention, a spread of more than about 10 % between runs means the environment is noisy (shared hosts, other workloads, a background Galera or Swift job) and the figure should not be quoted until it is explained. This 10 % is a suggestion, not a standard.

**Trend inside a run.** The `.windows.csv` file shows whether latency or backlog creeps up during the run:

```bash
awk -F, 'NR>1{n++; p[n]=$5; r[n]=$3; q[n]=$8} END{k=int(n/3); for(i=1;i<=k;i++){a+=p[i];ra+=r[i]} for(i=n-k+1;i<=n;i++){b+=p[i];rb+=r[i]}
  printf "windows=%d  p95 first third %.0f ms -> last third %.0f ms (%+.1f%%)   req/s %.1f -> %.1f   queue at end=%s\n", n,a/k,b/k,100*(b-a)/a,ra/k,rb/k,q[n]}' perf_typical_run1.windows.csv
```

A p95 that is clearly higher in the last third, falling `req/s`, or a queue that never returns to near zero between windows is a stability problem even when the run-level figures pass.

**Soak test (stability over time).** A 5-minute run cannot show slow problems. Run the `typical` profile for at least 30 minutes, ideally 1–2 hours, with one trend line per minute, and sample resources for the same period:

```bash
export PERF_INTERVAL=60
perf_sample_start soak 4200                       # 70 minutes of sampling
perf_run soak 3 250 2500 1 3600                   # 1 hour at the typical profile
perf_sample_collect soak
```

During and after the soak, look at things that only grow slowly. Each is a command used elsewhere in this README:

| Watch | Command | Healthy |
|---|---|---|
| Process memory | `RSS_kB_1st` / `RSS_kB_end` from `perf_sample_collect`; or `ps -o rss= -p <pid>` hourly | Flat after warm-up; steady growth is a leak |
| Queue depth | `.windows.csv` `queue` column | Stays near its earlier level |
| Latency trend | drift command above on `perf_soak_run1.windows.csv` | No upward drift |
| ZooKeeper znodes | `echo mntr \| nc ${NODE1_IP} 2181 \| grep zk_znode_count` | Rises and falls with the dedup reaper (every `GT_DEDUP_REAP_INTERVAL_S`, 600 s by default, removes znodes older than `ZK_DEDUP_TTL_MS`, 5 min by default); a steady climb means it is not keeping up |
| Log growth and rotation | `sudo du -sh /var/log/flowfirst; ls -l /var/log/flowfirst` | Rotates at `LOG_MAX_BYTES` (50 MiB) and stays within `LOG_BACKUP_COUNT` files; an hour at 100 req/s writes several per-message lines per request (Scenario 60) |
| Database size | `SELECT table_rows, ROUND(data_length/1048576) AS data_mb FROM information_schema.tables WHERE table_name='processed_messages';` | Grows in proportion to rows stored |
| Errors | `sudo grep -c " ERROR " /var/log/flowfirst/process*.log` on each node | 0 |

#### G. Performance while something fails

Clients also need to know what performance looks like during a fault, because high availability is the point of this architecture. Run the `typical` profile and trigger **one** fault part-way through, using the procedure of an existing scenario. Use a 10-second trend interval so that the dip is visible:

| Fault | Procedure | What you learn |
|---|---|---|
| VIP and HAProxy fail over | Scenario 8 (`pcs node standby node1`) | How long the API is unreachable or slow |
| One pipeline process stopped on one node | Scenario 11 (`pcs resource ban …`) | Effect on throughput and freshness while the leader changes |
| A node reboots | Scenario 16 | The full-node case, including Galera and RabbitMQ rejoining |
| ZooKeeper leader killed | Scenario 36 | Effect of re-election on all four processes |

```bash
export PERF_INTERVAL=10
perf_sample_start fault1 720
perf_run fault 4 250 2500 1 480 &                # 8 minutes of measurement, in the background
sleep 150; date +%T                              # note this time, then trigger the fault (for example: sudo pcs node standby node1)
wait                                             # for the run and drain to finish
grep "measuring from" perf_fault_run1.log      # t=0 of the windows; fault time minus this = fault offset in seconds

# Which windows were outside the pass criteria (SLO from .env: LOAD_SLO_P95_MS, LOAD_MAX_ERROR_PCT) - first and last one
awk -F, -v slo=${LOAD_SLO_P95_MS:-1000} -v maxerr=${LOAD_MAX_ERROR_PCT:-1} \
  'NR>1 && ($5>slo || $7>maxerr){bad++; if(!first)first=$2; last=$2} END{ if(bad) printf "%d window(s) outside SLO, first at t=%ss, last at t=%ss\n", bad, first, last; else print "no window outside SLO" }' perf_fault_run1.windows.csv
```

Subtract the fault offset from the first and last bad windows to get *time to degrade* and *time to recover*; the resolution is the window length (10 s). Then run the stored-versus-accepted check from the end of the run summary. **Do not assume no message is lost during a failure**: consumers reject failed messages with `requeue=False` and the queues have no dead-letter exchange (Scenario 61 C), so the comparison decides. Restore the fault (`pcs node unstandby …`) and wait for a healthy cluster (section B) before the next run.

#### H. Reporting to the client

For each profile, fill in the template with your own figures, using the worst of the three runs for latency and the lowest throughput. Leave a cell as `—` until measured; do not copy illustrative values.

| Measure | light | typical | peak | Agreed target |
|---|---|---|---|---|
| Users / think time / expected req/s | 50 / 5 s / ~10 | 250 / 2.5 s / ~100 | 1000 / 2 s / ~500 | — |
| Achieved req/s (lowest of 3) | — | — | — | — |
| API response p50 / p95 / p99 (ms) | — | — | — | — |
| Error rate (%) | — | — | — | — |
| Freshness p50 / p95 / max (s, ±1 s) | — | — | — | — |
| Max backlog / drain time (s) | — | — | — | — |
| Busiest process, average CPU (%) | — | — | — | — |
| Run-to-run spread (%) | — | — | — | — |
| Stored = accepted | — | — | — | — |

Add a second table for the soak result (memory, queue and latency trend, log size) and a third for the failure runs (time to degrade, time to recover, messages lost). Always attach the conditions record `perf_conditions.txt` and say, in the same sentence as the number, what it applies to, for example:

> *"On four nodes of the specification in `perf_conditions.txt`, with the flow mix and settings recorded there, at 250 concurrent users each calling roughly every 2.5 seconds (about 100 requests per second), 95 % of API calls were answered within ___ ms and 95 % of records were stored within ___ seconds (±1 s). Figures are the worst of three five-minute runs; results varied by ___ % between runs."*

#### I. What these results do not tell you

- **Closed-loop users.** Each simulated user waits for an answer before sending the next call. When the system slows down, the offered load drops with it, which makes overload look milder than it would be for independent clients arriving at a fixed rate. If the client's traffic arrives regardless of response time, treat the `peak` figures as optimistic and keep a safety margin. An open-loop (fixed-arrival-rate) mode is not implemented.
- **Synthetic requests.** Payloads are tiny and generated from a pattern. Larger payloads change API Gateway (Process 1), RabbitMQ, MariaDB and Swift costs.
- **Timestamp resolution** is one second (section D), so freshness cannot resolve sub-second behaviour.
- **One environment.** Hardware, virtualisation, neighbours and network all change the figures. Use the numbers only for the environment in `perf_conditions.txt`.
- **API acceptance is not completion.** The `200` response times in the first rows of the table are real, but a client who needs the stored record has to wait for the freshness figure.
- **The leader-election question from Scenario 61 still applies**, and it decides whether the quoted capacity belongs to one node or to three.

#### J. Clean up

The test writes real rows, objects and ZooKeeper znodes. Every `perf_run` uses its own block of ten million item ids between `9000001` and `209999999`. That range **includes Scenario 61's default `LOAD_ITEM_ID_BASE`**, so run Scenario 61 G first (or change its base) to avoid counting its rows. Delete the rows in small batches exactly as in Scenario 61 G, with `BETWEEN 9000001 AND 209999999`, and clear the temporary files on the nodes:

```bash
for ip in ${NODE1_IP} ${NODE2_IP} ${NODE3_IP} ${NODE4_IP}; do ssh "${ip}" "sudo rm -f /tmp/perf_*_pid.txt /tmp/perf_*_sar.txt"; done
```

Keep the `perf_*.csv`, `perf_*.log`, `perf_conditions.txt` and the `perf_*_pid.txt` / `perf_*_sar.txt` files with the report so any number can be traced back to its run. The swift objects remain, as in Scenario 61.

---

## Security

### Scenario 63: TLS Everywhere — Private CA, Mutual-TLS REST API and Encrypted Internal Traffic

> **Runs on:** a trusted **admin/CA host** (the laptop or jump host that holds `security/` and the CA key; Bash 3.2+, OpenSSL 1.1.1+ or 3.x, passwordless `ssh` and `sudo` to node1–node4 if you want `install_certs.sh` to push files) and then **each cluster node** (node1–3 for the server-side `enable_*_tls.sh` scripts, node4 for its client identity only).

> **Status — read this first.** Everything below was built and tested in pieces on a workstation, with containers for RabbitMQ, MariaDB, ZooKeeper, HAProxy and Swift. **It was never run on a real four-node RHEL cluster.** Section L lists exactly what was tested and what was not. Do the rollout on a test cluster first, one component at a time, with the plain-text listeners left on until each step is proven.

TLS is **off by default** (every `TLS_*_ENABLED` is `false`), so an existing deployment behaves as before until you switch components on. When a switch is on the code **fails closed**: a missing certificate, an unreadable key or an `http://` Swift URL stops the process at start with a clear message instead of quietly falling back to plain text.

#### Certificate lifecycle and runtime usage

> 📐 **[View diagram: SSL Certificate Creation, Distribution and Runtime Usage](docs/diagrams.md#4-ssl-certificate-creation-distribution-and-runtime-usage)**

The diagram covers four stages: ① Create (admin/CA host), ② Distribute (SSH+sudo to each node), ③ Runtime (which cert each component presents), ④ Maintain (CRL refresh, revocation, expiry checks).

The following two diagrams give the same information split into focused views.

**PKI lifecycle — creation, distribution and renewal**

> 📐 **[View diagram: PKI Lifecycle](docs/diagrams.md#5-pki-lifecycle--creation-distribution-and-renewal)**

**Runtime TLS connections — who presents which certificate**

> 📐 **[View diagram: Runtime TLS Connections](docs/diagrams.md#6-runtime-tls-connections--who-presents-which-certificate)**

> **CRL enforcement:** when `TLS_CRL_FILE` is set, HAProxy (`crl-file`), API Gateway (Process 1), and the Python client contexts ([`src/tls/tls.py`](src/tls/tls.py)) each load `ca.crl` at start and reject any certificate whose serial appears in it. RabbitMQ and ZooKeeper do not enforce the CRL. MariaDB enforces it only with `ssl_crl` set in `my.cnf`. A CRL past its `nextUpdate` causes every component to refuse **all** certificates — run `refresh_crl.sh` weekly before that date.

#### A. What gets encrypted, and who must present a certificate

| Link | Port | Server certificate | Client must present | Enforced by |
|---|---|---|---|---|
| External client → REST API (VIP) | 8080 (HTTPS) | `flowfirst-vip` on HAProxy | **Yes** — a certificate from the FlowFirst CA (mutual TLS) | HAProxy `verify required` |
| HAProxy → API Gateway (Process 1) | 8082 (HTTPS) | `<node>-api` | `<node>-pipeline` (HAProxy's own identity) | API Gateway (Process 1) `CERT_REQUIRED` |
| Processes 1–4 and node4's Remote Worker (Process 5) → RabbitMQ | 5671 (AMQPS) | `<node>-rabbitmq` | `pipeline` identity | RabbitMQ `fail_if_no_peer_cert` |
| Processes → MariaDB (through the VIP/HAProxy TCP pass-through) | 3306 / 3307 | `<node>-mariadb` | `pipeline` identity if `--require-client-cert` was run | MariaDB `REQUIRE X509` |
| Processes → ZooKeeper | 2281 (secure client port) | `<node>-zookeeper` | `pipeline` identity | ZooKeeper `clientAuth=need` |
| Processes → Swift (via HAProxy) | 8090 → 8888 | `flowfirst-vip`, `<node>-swift` | `pipeline` identity **at HAProxy only** | HAProxy (see limit L-3) |
| RabbitMQ / Galera / ZooKeeper node ↔ node | 25672, 4567–4568, 2888/3888 | `peer` certificates | peer certificate | optional flags `--inter-node`, `--galera`, `--quorum` |

The three certificate profiles: **server** (`serverAuth`: something clients connect to), **client** (`clientAuth`: an identity that connects, such as an API user, a pipeline process or node4) and **peer** (both: node-to-node traffic).

`issue_all.sh` creates 21 certificates: `flowfirst-vip`; for each of node1–3 `<node>-api`, `-rabbitmq`, `-mariadb`, `-zookeeper`, `-swift` and `-pipeline`; `node4-pipeline`; and `admin-client`. The MariaDB and RabbitMQ node certificates also carry the VIP name and IP in their SAN, because `MARIADB_HOST` and `RABBITMQ_HOST` point at the VIP.

#### B. Components added to the repository

| Path | Purpose |
|---|---|
| `security/init_ca.sh`, `issue_cert.sh`, `issue_all.sh`, `install_certs.sh`, `revoke_cert.sh`, `list_certs.sh`, `lib.sh` | Self-signed root CA, leaf certificates, per-node distribution, revocation, expiry report |
| `security/enable_rabbitmq_tls.sh`, `enable_mariadb_tls.sh`, `enable_zookeeper_tls.sh`, `enable_swift_tls.sh` | Server-side TLS configuration for each product (run on node1–3) |
| `security/verify_tls.sh` | Probes every TLS endpoint and prints PASS/FAIL |
| `src/tls/tls.py` | Python TLS contexts for all clients; fails closed |
| `haproxy/setup_haproxy.sh`, `haproxy/haproxy.cfg.template` | HTTPS frontend with client certificates, re-encrypted backends, TLS pass-through for MariaDB and RabbitMQ |
| `src/application/process1.py` | HTTPS listener with optional client-certificate check |
| `scripts/load_test_api.py`, `test_flows_and_inspect_db.sh`, `tcpdump_flows.sh` | Updated to use HTTPS, the CA and a client certificate when TLS is on |

The `pki/` directory (CA key, issued keys) and every `*.key` / `*.pem` are in `.gitignore`. **Never commit them.**

#### C. Create the CA and issue the certificates (admin host, once)

```bash
cd /opt/flowfirst            # or your checkout on the admin host
cp src/config/.env.example .env      # NODE1..4_IP and FLOWFIRST_VIP must be literal values
./security/init_ca.sh                # asks for a passphrase; creates pki/ca/ca.key (AES-256), ca.crt, ca.crl
./security/issue_all.sh              # 21 certificates under pki/issued/<name>/
./security/list_certs.sh             # profile, expiry and status of each
```

Optional environment overrides: `PKI_VIP_DNS` (default `flowfirst.local`, the DNS name clients use for the VIP), `PKI_CA_DAYS` (default 3650), `PKI_LEAF_BITS` (default 2048), `PKI_DIR`.

Key handling rules:

- **`ca.key` stays on the admin host.** It is encrypted with your passphrase. Nodes only ever receive `ca.crt` (public) and their own leaf keys.
- Leaf keys are **not** passphrase-protected, because services must start unattended. They are mode 600 and owned by the service account.
- Add more external API users with `./security/issue_cert.sh --name alice --profile client --cn "alice"` and hand `alice.crt`, `alice.key` and `ca.crt` to that person over a secure channel.
- Leaf certificates last 365 days by default (`PKI_LEAF_DAYS`, or `--days N` on `issue_cert.sh`). Short lifetimes are the main control for the revocation limits in section L.

#### D. Install the certificates on the nodes

```bash
./security/install_certs.sh --stage /tmp/flowfirst-stage   # review the per-node trees first, no ssh
./security/install_certs.sh                                # push to node1..4 over ssh + sudo
./security/install_certs.sh --node 4                       # or one node at a time
```

Layout on each node (`TLS_DIR`, default `/etc/flowfirst/tls`):

| File | Owner / mode | Nodes |
|---|---|---|
| `ca.crt`, `ca.crl` | `root` 644 | all |
| `pipeline/pipeline.{crt,key}` | `flowuser` (key 600) | all — this is the client identity of Processes 1–5 |
| `api/api.{crt,key}` | `flowuser` | node1–3 |
| `haproxy/vip.pem`, `haproxy/backend.pem` | `haproxy` | node1–3 |
| `rabbitmq/…`, `mariadb/…`, `zookeeper/…`, `swift/…` | the service account | node1–3 |

If a service account does not exist yet, that file stays `root:root` 600 and the script prints a notice. Install the product first, then **re-run `install_certs.sh`**. The `systemd` units run as `flowuser`, so Processes 1–5 read `pipeline/` and `api/` through ownership; the units themselves need no change.

> **Not tested:** the ssh push in `install_certs.sh`. Only `--stage` output was checked. Use `--stage` and copy by hand if you prefer.

#### E. Switch the pipeline processes to TLS (`.env` on every node)

All values are literals, as always. The two per-node values (`NODE_NAME`, `CURRENT_NODE_IP`) stay the only differences between nodes.

```ini
TLS_DIR=/etc/flowfirst/tls
TLS_CRL_FILE=/etc/flowfirst/tls/ca.crl     # blank = no revocation check in HAProxy / API Gateway (Process 1) / Python clients
TLS_MIN_VERSION=1.2                        # or 1.3
TLS_API_ENABLED=true                       # API Gateway (Process 1) serves HTTPS; HAProxy fronts it with mutual TLS
TLS_API_CLIENT_AUTH=required               # required | optional | none
TLS_RABBITMQ_ENABLED=true                  # connect to 5671
TLS_MARIADB_ENABLED=true
TLS_ZK_ENABLED=true                        # connect to ZK_TLS_CLIENT_PORT (2281)
TLS_SWIFT_ENABLED=true                     # SWIFT_AUTH_URL must then start with https://
```

Do **not** flip all of them at once. Section F gives the safe order, and each switch is only valid after the matching server-side script has been run.

#### F. Safe rollout order (test cluster first)

Every step keeps the plain-text listeners on until the encrypted path is proven, so you can stop at any step and the cluster still works. After each step run `./security/verify_tls.sh` (section J) and `./scripts/test_flows_and_inspect_db.sh`.

| Step | Where | Command | How to undo |
|---|---|---|---|
| 1 | admin host | `init_ca.sh`, `issue_all.sh`, `install_certs.sh` (sections C–D) | delete `/etc/flowfirst/tls` |
| 2 | node1, then node2, then node3 | `sudo ./security/enable_rabbitmq_tls.sh` | remove the `BEGIN/END FLOWFIRST TLS` block in `rabbitmq.conf`, restart |
| 3 | node1–3 | `sudo ./security/enable_zookeeper_tls.sh`, one node at a time (two of three must stay up) | restore `zoo.cfg.pre-tls` |
| 4 | node1–3 | `sudo ./security/enable_mariadb_tls.sh`, one node at a time | delete `zz-flowfirst-tls.cnf`, restart |
| 5 | node1–3 | `sudo ./security/enable_swift_tls.sh` | remove the `cert_file`/`key_file` lines from `[DEFAULT]` |
| 6 | node1–4 | set `TLS_RABBITMQ_ENABLED`, `TLS_ZK_ENABLED`, `TLS_MARIADB_ENABLED`, `TLS_SWIFT_ENABLED` to `true` in `.env` (one switch at a time) and restart the processes | set back to `false` |
| 7 | node1–3 | `TLS_API_ENABLED=true`, restart API Gateway (Process 1) (Pacemaker), then `sudo ./haproxy/setup_haproxy.sh` | set back to `false` and re-run the setup script |
| 8 | one node | `sudo ./security/enable_mariadb_tls.sh --require-client-cert` (replicates to the others) | `ALTER USER … REQUIRE NONE` |
| 9 | node1–3 | optional hardening: `--disable-plain` on the RabbitMQ and ZooKeeper scripts | re-run without the flag |

Notes that matter in practice:

- **HAProxy is managed by Pacemaker.** After `setup_haproxy.sh` regenerates `/etc/haproxy/haproxy.cfg`, validate with `sudo haproxy -c -f /etc/haproxy/haproxy.cfg` and let Pacemaker pick it up (`sudo pcs resource restart haproxy-res`). **Never** start `haproxy.service` by hand. The setup script refuses to render when a required certificate file is missing, so a half-installed PKI cannot produce a broken config silently.
- **Step 7 changes the API for every client at once.** Once HAProxy requires client certificates, plain `http://VIP:8080` and any client without a certificate get refused. Distribute client certificates first (section G).
- **`--inter-node` (RabbitMQ), `--galera` (MariaDB) and `--quorum` (ZooKeeper)** encrypt node-to-node traffic. Each needs one maintenance window with the whole component stopped, because a node using TLS cannot talk to one that does not. The comments at the top of each script give the order. They were **not tested** (limit L-6).
- Do step 8 only after step 6 for MariaDB is confirmed, otherwise the pipeline processes lose database access.

#### G. Calling the REST API from outside (mutual TLS)

Give each external user their own certificate (section C). Then:

```bash
# Health (note: /health asks ZooKeeper, so it fails if ZooKeeper is down; /gt/status does not)
curl --cacert ca.crt --cert admin-client.crt --key admin-client.key https://flowfirst.local:8080/health | jq .

curl -s --cacert ca.crt --cert admin-client.crt --key admin-client.key \
     -X POST https://${FLOWFIRST_VIP}:8080/api/flow1 -H "Content-Type: application/json" \
     -d '{"item_id": 301, "counter": 500}' | jq .
```

Rules:

- The certificate must be signed by the FlowFirst CA and still valid, and `--cacert` must be the FlowFirst `ca.crt`. There is deliberately no "insecure" option anywhere in the project.
- The server name in the URL must be one the VIP certificate lists: the VIP IP, `flowfirst.local` (or your `PKI_VIP_DNS`), or `localhost`. For a DNS name add it to `/etc/hosts` or your DNS.
- HAProxy sets the `X-SSL-Client-CN` header from the verified certificate and overwrites any value the client sent. API Gateway (Process 1) can therefore use it as "who called". This was tested and a spoofed header was replaced.
- A refused connection shows up as `curl: (35)` or `(56)` (no or bad certificate), `(60)` (server not trusted or wrong name) or `(7)` (port closed). Exact codes vary with the curl and OpenSSL versions; read the message text, and confirm with `openssl s_client`.
- Test scripts: `scripts/test_flows_and_inspect_db.sh` and `scripts/load_test_api.py` use the node's `pipeline` certificate by default; from an admin host set `TLS_CA_FILE`, `TLS_CLIENT_CERT` and `TLS_CLIENT_KEY` (or `LOAD_CACERT`, `LOAD_CERT`, `LOAD_KEY`) to the admin files.

#### H. Remote node4 (Remote Worker (Process 5))

Node4 is outside the Pacemaker cluster and only **connects out**, so it gets one identity: `node4-pipeline`, installed as `pipeline/pipeline.{crt,key}` plus `ca.crt` and `ca.crl`. `install_certs.sh --node 4` does exactly that; no server key ever reaches node4.

- Its `.env` is identical to the other nodes except `NODE_NAME` and `CURRENT_NODE_IP`, so the same `TLS_*` switches apply, and it reaches RabbitMQ (5671), ZooKeeper (2281), MariaDB and Swift with its own certificate.
- The firewall between node4 and node1–3 must allow the TLS ports (5671, 2281, plus the existing MariaDB, Swift and HAProxy ports).
- If node4 is compromised, revoke `node4-pipeline` (section I) and also rotate the RabbitMQ and MariaDB application passwords, because revocation is not enforced by every component (section L).

#### I. Rotation, expiry and revocation

```bash
./security/list_certs.sh --warn-days 30      # exit 1 if anything is expiring or revoked: run it from cron
./security/issue_cert.sh --name node2-api --profile server --san "…" --force     # re-issue one certificate
./security/install_certs.sh --node 2         # copy the new files
sudo systemctl restart flowfirst-process1    # or: restart the service that uses the certificate
```

Revocation:

```bash
./security/revoke_cert.sh alice keyCompromise
./security/install_certs.sh --crl-only       # publish the new ca.crl to every node
```

Who actually honours the CRL (`TLS_CRL_FILE`):

| Component | Honours `ca.crl`? | Evidence |
|---|---|---|
| HAProxy (API, Swift frontends) | Yes | tested: a revoked client certificate was refused |
| API Gateway (Process 1) HTTPS listener | Yes | tested: a revoked client certificate was refused |
| RabbitMQ | **No** | tested: a revoked certificate still connected |
| MariaDB | **Yes**, when `TLS_CRL_FILE` is set (`ssl_crl`) | tested on a MariaDB 11.8 container: a revoked client certificate was refused (`certificate revoked`); without `ssl_crl` it connected. The server reads the file at start or on `FLUSH SSL` |
| ZooKeeper | **No** (not read by its TLS settings) | documented, not tested |

For the components that do not enforce it (RabbitMQ, ZooKeeper), revoking is not enough. Also change or delete the RabbitMQ user or password and re-issue the certificate. For ZooKeeper, remove the client's access at the network level and re-issue. ZooKeeper's `ssl.crl` option is documented to work from CRL distribution points inside the certificate, which these certificates do not carry (no `crlDistributionPoints` extension); I did not test it and did not enable it.

**After `revoke_cert.sh` and `install_certs.sh --crl-only`:** run `mariadb -e "FLUSH SSL"` on each Galera node (otherwise MariaDB keeps the old list: tested, a newly revoked certificate still connected until `FLUSH SSL`), reload HAProxy, and restart the Python processes (they read the file at start-up).

**The CRL expires, and an expired CRL is an outage.** The CRL has a `nextUpdate` date (`PKI_CRL_DAYS`, default 30). Tested on both a MariaDB server and Python's `ssl` module: once it passes, a certificate that is *not* revoked is refused as well (`certificate expired` / `certificate verify failed`), because the server can no longer prove the certificate is unrevoked. Every component that loads the CRL does this, so with `TLS_CRL_FILE` set you must re-issue it before the date:

```bash
./security/refresh_crl.sh [--days 30]        # on the CA host: new CRL, revocations kept, nothing revoked
./security/install_certs.sh --crl-only       # push it to every node, then FLUSH SSL / reload / restart as above
./security/list_certs.sh                     # prints the CRL's nextUpdate; exit 1 if expired or within 7 days
```

`refresh_crl.sh` can run from cron (`PKI_CA_PASSPHRASE_FILE`, see its header), but that stores the CA passphrase on the CA host: weigh that against remembering. `scripts/preflight_check.py` on each node also checks the installed CRL (PASS with days left and revoked count, WARN under 7 days or when `TLS_CRL_FILE` is blank, FAIL when expired or unreadable). Leaving `TLS_CRL_FILE` blank avoids the expiry risk and also disables revocation checking.

CA expiry: the CA lasts 3650 days by default (`PKI_CA_DAYS`). Replacing the CA means re-issuing and re-installing everything; plan it well before expiry.

#### J. Verify

```bash
./security/verify_tls.sh                      # from a node: uses that node's pipeline certificate
./security/verify_tls.sh --env /path/.env --ca ca.crt --cert admin-client.crt --key admin-client.key   # from an admin host
```

Only components whose `TLS_*_ENABLED` is `true` are probed; the others print `SKIP`. For each endpoint it checks that a valid client is accepted, a client without a certificate is refused and plain text is refused. Exit code 0 means all passed, 1 at least one failed, 2 a usage or file error.

What a PASS does **not** prove: revocation, cipher strength beyond what was negotiated, or that TLS 1.0/1.1 clients are refused (modern OpenSSL cannot even offer them). The RabbitMQ, MariaDB, ZooKeeper and Swift branches of this script have not been exercised against the real servers.

Manual spot checks:

```bash
openssl s_client -connect ${FLOWFIRST_VIP}:8080 -CAfile /etc/flowfirst/tls/ca.crt -brief </dev/null      # server certificate + TLS version
openssl s_client -connect ${NODE1_IP}:5671 -CAfile /etc/flowfirst/tls/ca.crt \
    -cert /etc/flowfirst/tls/pipeline/pipeline.crt -key /etc/flowfirst/tls/pipeline/pipeline.key -brief </dev/null
openssl x509 -in /etc/flowfirst/tls/pipeline/pipeline.crt -noout -subject -enddate -ext subjectAltName
```

With TLS on, `tcpdump` captures show encrypted payloads (see the `tcpdump` section above): handshakes and byte counts only. That is the expected result.

#### K000. Publish confirms and API Gateway (Process 1) publish connections

API Gateway (Process 1) answers `200` only after RabbitMQ has confirmed that it holds the message (publisher confirms, `mandatory=True`). A confirm costs one broker round trip per publish, so a single connection limits throughput. API Gateway (Process 1) therefore keeps `API_PUBLISH_CONNECTIONS` (default 4, range 1 to 32) connections, each with its own channel and lock, and spreads requests over them round-robin. Each connection reconnects and re-enables confirms by itself after a broker restart.

Measured on **one RabbitMQ container, a 2-vCPU VM, 100 concurrent callers, a single run per row**. These are illustrative, not a cluster result and not a sizing guarantee; repeat with `scripts/load_test_api.py` on the real cluster (Scenario 61):

| Design | Rate | p95 latency |
|---|---|---|
| No confirms (rejected: a message could be lost silently) | ~13,300 msg/s | 9.9 ms |
| 1 connection with confirms | ~1,340 msg/s | 83 ms |
| 2 connections | ~1,770 msg/s | 80 ms |
| 4 connections (default) | ~2,630 msg/s | 60 ms |
| 8 connections | ~4,270 msg/s | 32 ms |

Tested against a real RabbitMQ 3 container: 2000 concurrent publishes all arrived exactly once (2000 distinct message ids, queue depth 2000), the heartbeat ran between publishes without deadlock, and after `rabbitmqctl stop_app` / `start_app` publishes failed with `StreamLostError` while the broker was down and then succeeded again without restarting API Gateway (Process 1).

Limits: while the broker is down, requests fail (the client gets an error and should retry); nothing is queued in API Gateway (Process 1). More connections mean more RabbitMQ connections per API Gateway (Process 1) instance (3 instances x 4 = 12 by default). Confirm batching across requests would be faster still but was not built.

#### K00. Request limits and error responses (API Gateway (Process 1))

| Input | Limit (`.env`) | Refused with |
|---|---|---|
| POST body size | `API_MAX_BODY_BYTES` (1 MiB) | `413`; the body is not read |
| `count` of `POST /api/batch` | `API_MAX_BATCH_COUNT` (100) | `400`; nothing is published |
| `limit` of `GET /swift/objects` | `API_MAX_LIST_LIMIT` (1000) | `400` |
| Time a connection may stall | `API_REQUEST_TIMEOUT_S` (30) | connection closed by the server |
| Bad `Content-Length`, JSON that is not an object, non-numeric `item_id`, `counter`, `value`, `remote_metric`, `count` or `limit` | — | `400` (before this change a non-numeric field returned `500`, and a bad `limit` dropped the connection) |

Error bodies are generic: `{"error": "...", "request_id": "<12 hex>"}`. The exception, with its stack trace for 5xx, goes to the process log (`/var/log/flowfirst/api_gateway.log`) under the same `request_id`, so a support ticket can be matched to the log line: `grep <request_id> /var/log/flowfirst/api_gateway.log`. 404 responses no longer echo the requested path or an unknown config key, and `/swift/status` no longer returns the Swift URL or the raw exception. `API_EXPOSE_ERROR_DETAILS=true` adds a `details` field for development.

Tested with the real handler (26 checks, plain HTTP, no real RabbitMQ, ZooKeeper or Swift): every limit above, the boundary values (a body just under the limit is accepted, `count` 0/-3/`x`/100000000 are refused), a stalled client being cut off after the configured 3 s test timeout, the server staying healthy afterwards, and no host, path or library text in any error body.

Limits of this work: (1) the log still contains the exception text, which can include host names or other content from the failing library, so protect the log files and do not log secrets in exception messages; (2) the stall timeout was tested over plain HTTP only, not over the TLS listener; (3) HAProxy enforces `HAPROXY_MAX_BODY_BYTES` (default 1 MiB, same as `API_MAX_BODY_BYTES`) on the API frontend with `http-request deny deny_status 413` + `option http-buffer-request` — this was not tested against a live HAProxy; (4) these are per-request limits, not rate limiting: a client with a valid certificate can still send many valid requests, which Scenario 61 sizes; (5) `/health`, `/gt/status` and `/swift/objects` still return operational detail (node names, greenthread lists, object names) to any API client.

#### K0. Who may change runtime configuration (`POST /zk/config`)

Writing a runtime config value affects every process in the cluster, so it is not open to every certificate holder. API Gateway (Process 1) resolves the caller as follows:

1. The caller's **client certificate** on the connection to API Gateway (Process 1) gives its CN.
2. **Only if** that connection was made by one of `ADMIN_TRUSTED_PROXY_CNS` (HAProxy's backend identity, `<node>-pipeline`), the `X-SSL-Client-CN` header is believed. HAProxy deletes any client-sent copy and sets it from the verified certificate. A direct caller with some other certificate that sends the header is treated as its own certificate CN.
3. The CN must be in `ADMIN_ALLOWED_CNS`. A blank list allows nobody. Without TLS there is no caller identity, so the request is refused unless `ALLOW_UNAUTHENTICATED_ADMIN=true` (dev only).

Tested with the real handler over real mutual TLS (certificates from `security/`): the `admin-client` certificate got 200; another valid certificate got 403, also when it forged the header; the HAProxy backend certificate forwarding `admin-client` got 200, and forwarding another name got 403; another node's certificate sending the admin header got 403; a client with no certificate was refused by TLS. Without TLS the default was 403 and the opt-out 200. ZooKeeper itself was replaced by a stub in that test, so the write to ZooKeeper was not exercised. The behind-HAProxy path was reproduced by sending the header directly, not through a running HAProxy.

Limits: this protects the one config-write endpoint. It is not general role-based access control, every other endpoint (including the Swift and flow endpoints) still accepts any valid client certificate, and `GET /zk/config` is readable by any API client.

#### K. Troubleshooting

| Symptom | Likely cause | Check / fix |
|---|---|---|
| A process exits at start with a TLS message | Fail-closed: file missing, unreadable key, or `http://` Swift URL with TLS on | `journalctl -u flowfirst-processN`, `ls -l /etc/flowfirst/tls/…`; key must be readable by `flowuser`; re-run `install_certs.sh` |
| `certificate verify failed` / `(60)` | Wrong CA, or the name used is not in the SAN | `openssl x509 -noout -ext subjectAltName`; use the VIP name listed there |
| `certificate has expired` or `not yet valid` | Expiry, or clock skew between nodes | `chronyc tracking`; `./security/list_certs.sh` |
| HAProxy refuses every client | Out-of-date CRL (suspected, untested), or `ca-file` missing | `openssl crl -in ca.crl -noout -nextupdate`; re-publish the CRL |
| HAProxy backend `DOWN`, log says handshake failure | Backend certificate does not match or API Gateway (Process 1) not on HTTPS | backend server lines use `sni` + `verifyhost <node>`; API Gateway (Process 1) must run with `TLS_API_ENABLED=true` |
| RabbitMQ: `handshake_failure` | Client certificate not signed by the CA, or none presented | `/var/log/rabbitmq/*.log`; check `TLS_CLIENT_CERT` |
| MariaDB: `Access denied` after `--require-client-cert` | Client sends no certificate | `TLS_MARIADB_ENABLED=true` and certificate readable; undo with `ALTER USER … REQUIRE NONE` |
| ZooKeeper: `ruok` / `mntr` return nothing on the TLS port | Four-letter words do not work over TLS | use the plain port while it exists, or the admin server on `ZK_ADMIN_PORT` |
| Swift client certificate is not requested | Swift 2.38 proxy hardcodes `CERT_NONE` | by design; enforcement is at HAProxy (L-3) |
| `.env` TLS values have no effect | `${VAR}` in a value is not expanded by python-dotenv | use literal paths |

#### L. What was tested, and the limits

**Tested** (workstation, with containers where noted; never on RHEL nodes):

- The PKI scripts: CA, 21 certificates, expiry report, revocation and CRL, per-node `--stage` layout.
- The Python TLS module (fails closed) and API Gateway (Process 1)'s HTTPS listener with the real eventlet code: valid certificates got `200`; no certificate, a revoked certificate, plain HTTP, an untrusted CA and a TLS 1.1 client were refused.
- HAProxy 3.4 in a container in front of mutual-TLS backends: same refusals, plus a wrong hostname; `X-SSL-Client-CN` set from the certificate and not spoofable. `haproxy -c` passes for TLS off and on.
- RabbitMQ 3 (container): valid certificate connected; none or wrong hostname refused.
- MariaDB 11 (container): valid certificate worked; untrusted server CA rejected by the client; `REQUIRE X509` worked with a certificate and denied without.
- ZooKeeper 3.9 (container, kazoo): valid certificate worked; none, rogue-CA and plain clients on the TLS port refused.
- Swift 2.38.2 proxy (container): served HTTPS; untrusted CA and plain HTTP refused.
- `verify_tls.sh` against local TLS servers (13 of 13 checks, and deliberate failures caught); `load_test_api.py` against a local mutual-TLS server; the RabbitMQ queue-depth monitor against a local TLS server.

**Not tested, or limited:**

1. **No run on a real four-node cluster**, and none through Pacemaker/pcsd (both are unchanged by this work).
2. The `sudo`, restart and firewall paths of the `enable_*_tls.sh` scripts, the ssh push of `install_certs.sh`, and the ZooKeeper keystore step inside the script (the same commands were run by hand). The HAProxy-to-MariaDB/RabbitMQ TLS pass-through was designed as TCP pass-through and not run end to end with Galera.
3. **Swift is not mutual TLS at the proxy**: Swift 2.38 hardcodes `ssl.CERT_NONE`, and prints that SSL there is for testing only. This is in the upstream Swift code and cannot be changed from this repository. Client certificates are enforced by HAProxy; the HAProxy-to-proxy hop is encrypted. For stronger isolation put stunnel or nginx in front of the proxy. Anyone who can reach port 8888 on a node directly can talk to the proxy without a client certificate: firewall it to the HAProxy hosts.
4. **Revocation** is not enforced by RabbitMQ or ZooKeeper (see I). It is enforced by HAProxy, API Gateway (Process 1) and (with `ssl_crl`) MariaDB client connections; whether Galera replication traffic honours the CRL was not tested. Every component that does enforce it fails closed when the CRL expires (see I).
5. **Galera SST with `rsync` is not encrypted** by Galera TLS, and blocks the donor. Switch to `MARIADB_SST_METHOD=mariabackup` (non-blocking, no extra encryption by default) and add `MARIADB_SST_ENCRYPT=true` to encrypt the stream with the cluster's own TLS certificates. All nodes must use the same method; change it with a rolling restart (node3 → node2 → node1). This has not been tested on a real cluster.
6. `--inter-node`, `--galera` and `--quorum` were not tested.
7. Pacemaker/Corosync (and pcsd) traffic uses their own mechanisms and is not covered here.
8. Leaf keys are not passphrase-protected, so protect `/etc/flowfirst/tls` with filesystem permissions and disk encryption. The CA key is protected only by its passphrase and the admin host.
9. Encryption protects the network. It does not replace the application passwords (RabbitMQ, MariaDB, Swift, HAProxy stats): change the sample values in `.env`.

---

## Scenario 68: Notification Listener — Receiving and Printing Live Notifications

> **Goal:** enable the notification facility, run the standalone listener script (`scripts/notification_listener.py`) on any node or machine, and watch notification envelopes printed to the terminal in real time as flows and CRUD operations complete.

### Overview

The notification listener is a self-contained Python script that:

1. Connects directly to RabbitMQ using the same credentials as the pipeline.
2. Declares a durable subscriber queue and binds it to the `flowfirst.notifications` fanout exchange.
3. Prints every notification envelope as it arrives — with colour, structured fields, and the payload subset.

It has **no dependency on the FlowFirst source tree** — only `pika` is required. Multiple listeners can run simultaneously; each gets its own copy of every notification.

---

### Prerequisites

**A. Enable notifications on the cluster** (one-time change — do this before running the listener).

On every cluster node, add to `/opt/flowfirst/.env`:

```ini
NOTIFICATIONS_ENABLED=true
```

Then restart DB Updater (Process 4) so it picks up the change:

```bash
# Under Pacemaker (Nodes 1–3):
sudo pcs resource restart flowfirst-process4

# Verify it came back up on the leader node:
sudo pcs status | grep process4
```

**B. Confirm the exchange exists** (optional sanity check — after Process 4 restarts):

```bash
sudo rabbitmqctl list_exchanges | grep notifications
# Expected:  flowfirst.notifications   fanout
```

**C. Install pika on the machine where you will run the listener** (skip if the FlowFirst venv is active):

```bash
pip install pika
# or, if the FlowFirst venv is active:
source /opt/flowfirst/.venv/bin/activate
```

---

### Which node to run on

| Where | `RMQ_HOST` setting | Notes |
|---|---|---|
| **Node 1, 2, or 3** (recommended for quick tests) | `127.0.0.1` (default — no need to set it) | Connects directly to the local broker; no firewall rules needed. |
| **Node 4** (remote worker node) | Set `RMQ_HOST=<VIP or any cluster node IP>` | Node 4 has no RabbitMQ broker, so it must connect over the network. |
| **Your laptop or CI machine** | Set `RMQ_HOST=<VIP or any cluster node IP>` | Port 5672 (or 5673 for the HAProxy LB) must be reachable. Use the VIP for automatic failover. |

> Two processes sharing the same `RMQ_QUEUE` name **compete** for messages (round-robin — each gets half). To receive every notification independently on multiple machines, give each a different `RMQ_QUEUE` value.

---

### Step 1 — Run on a cluster node (simplest)

```bash
# SSH to any of Node 1, 2, or 3:
ssh node1

# Set the password (must match RABBITMQ_PASSWORD in .env):
export RMQ_PASSWORD=<your RABBITMQ_PASSWORD>

# Run the listener (repo is at /opt/flowfirst):
python3 /opt/flowfirst/scripts/notification_listener.py
```

Expected startup output:

```
FlowFirst Notification Listener
  Exchange : flowfirst.notifications
  Queue    : flowfirst.notifications.listener
  Broker   : 127.0.0.1:5672 (user=flowuser)
  Flows    : all
  Events   : all
Press Ctrl-C to stop.

Connected. Waiting for notifications …
```

---

### Step 2 — Trigger notifications

Open a second terminal and send requests through any of the existing flows:

```bash
VIP=192.168.1.100   # your FLOWFIRST_VIP

# Flow 1 — telemetry message (emits event_type: telemetry.processed)
curl -s -X POST http://${VIP}:8080/api/flow1 \
  -H "Content-Type: application/json" \
  -d '{"item_id": 101, "counter": 500}' | jq .

# Flow 4 — create IoT device (emits event_type: device.created)
curl -s -X POST http://${VIP}:8080/api/devices \
  -H "Content-Type: application/json" \
  -d '{"device_name":"sensor-01","device_type":"temperature_sensor","location":"zone-A"}' | jq .

# Flow 7 — soft-delete a device (emits event_type: device.decommissioned)
# Replace <device_id> with the device_id returned by the Flow 4 request above
curl -s -X DELETE http://${VIP}:8080/api/devices/<device_id> | jq .
```

Each request travels P1 → P2 → P3 → P4. DB Updater (Process 4) emits the notification **after** persisting and acking the pipeline message. Expect a short delay (< 1 s under normal load) between the API response and the notification appearing in the listener.

---

### Step 3 — Read the output

A Flow 1 notification looks like this:

```
────────────────────────────────────────────────────────────────────────
▶ telemetry.processed  flow=1  entity=message:101
  occurred=2025-07-14 11:30:06  node=node2
  notification_id=3f7a9c12-4b2e-41a8-9c3d-7f8e1b2a6d5c
  message_id=c1d2e3f4-5a6b-7c8d-9e0f-1a2b3c4d5e6f
  payload:
    {
      "flow": 1,
      "item_id": 101,
      "counter": 510,
      "initial_data": "Flow-1 original payload for item #101"
    }
  [#1 received, #0 filtered]
```

A Flow 4 (device create) notification:

```
────────────────────────────────────────────────────────────────────────
▶ device.created  flow=4  entity=device:a8b2c3d4-1234-5678-abcd-ef0123456789
  occurred=2025-07-14 11:30:08  node=node1
  notification_id=9a1b2c3d-...
  message_id=d4e5f6a7-...
  payload:
    {
      "device_name": "sensor-01",
      "device_type": "temperature_sensor",
      "location": "zone-A",
      "firmware_version": "1.0.0",
      "status": "active",
      "owner_team": "ops"
    }
  [#2 received, #0 filtered]
```

---

### Step 4 — Filter by flow or event type

```bash
# Watch device CRUD events only (flows 4, 6, 7):
FILTER_FLOWS=4,6,7 \
RMQ_PASSWORD=<password> \
python3 /opt/flowfirst/scripts/notification_listener.py

# Watch only device creation events:
FILTER_EVENTS=device.created \
RMQ_PASSWORD=<password> \
python3 /opt/flowfirst/scripts/notification_listener.py

# Watch device updates and decommissions only:
FILTER_EVENTS=device.updated,device.edge_synced,device.decommissioned \
RMQ_PASSWORD=<password> \
python3 /opt/flowfirst/scripts/notification_listener.py
```

Filtered messages are still consumed and acked from the queue (so the queue does not back up), but are not printed.

---

### Step 5 — Run from a remote machine or laptop

```bash
export RMQ_HOST=192.168.1.100     # VIP — fails over automatically if a node dies
export RMQ_PORT=5672              # direct; or use 5673 for the HAProxy LB port
export RMQ_USER=flowuser
export RMQ_PASSWORD=<password>
export RMQ_QUEUE=flowfirst.notifications.my-laptop   # unique queue name

python3 scripts/notification_listener.py
```

Use the VIP as `RMQ_HOST` so the listener automatically reconnects to the surviving broker if a node fails. The built-in reconnect loop retries every `RECONNECT_DELAY` seconds (default 5).

---

### Step 6 — Run two listeners simultaneously

Each listener with a different `RMQ_QUEUE` value receives every notification independently:

```bash
# Terminal 1 — team-a subscriber
RMQ_QUEUE=flowfirst.notifications.team-a \
RMQ_PASSWORD=<password> \
python3 /opt/flowfirst/scripts/notification_listener.py

# Terminal 2 — team-b subscriber (different queue = full copy)
RMQ_QUEUE=flowfirst.notifications.team-b \
RMQ_PASSWORD=<password> \
python3 /opt/flowfirst/scripts/notification_listener.py
```

Verify both queues exist:

```bash
sudo rabbitmqctl list_queues name messages consumers | grep notifications
# flowfirst.notifications.team-a    0    1
# flowfirst.notifications.team-b    0    1
```

---

### Environment variable reference

| Variable | Default | Required | Description |
|---|---|---|---|
| `RMQ_PASSWORD` | *(none)* | **Yes** | Must match `RABBITMQ_PASSWORD` in the cluster `.env`. |
| `RMQ_HOST` | `127.0.0.1` | No | Broker IP or VIP. Use VIP for automatic failover. |
| `RMQ_PORT` | `5672` | No | AMQP port. Use `5673` for the HAProxy LB port. |
| `RMQ_USER` | `flowuser` | No | Must match `RABBITMQ_USER` in `.env`. |
| `RMQ_EXCHANGE` | `flowfirst.notifications` | No | Must match `NOTIFICATIONS_EXCHANGE` in `.env`. |
| `RMQ_QUEUE` | `flowfirst.notifications.listener` | No | Each unique name is an independent subscriber. |
| `RMQ_PREFETCH` | `1` | No | In-flight messages limit. Raise for higher throughput. |
| `FILTER_FLOWS` | *(blank = all)* | No | Comma-separated flow numbers: `4,6,7`. |
| `FILTER_EVENTS` | *(blank = all)* | No | Comma-separated event types: `device.created,device.updated`. |
| `PRETTY` | `true` | No | `false` = compact JSON, useful for piping to `jq`. |
| `RECONNECT_DELAY` | `5` | No | Seconds to wait before reconnecting after a broker error. |

---

### Stopping the listener

Press **Ctrl-C**. The listener finishes the current message, closes the connection cleanly, and prints a summary:

```
Signal received — stopping.

Stopped. 14 notification(s) printed, 2 filtered, 16 total received.
```

The subscriber queue persists on the broker (it is durable) and accumulates notifications while the listener is stopped. When the listener restarts it will drain any queued notifications first.

To delete the queue when you are done (so it does not grow indefinitely while the listener is not running):

```bash
sudo rabbitmqctl delete_queue flowfirst.notifications.listener
```

---

### What this scenario does NOT cover

- TLS between the listener and RabbitMQ. If `TLS_RABBITMQ_ENABLED=true` on the cluster, the listener needs a client certificate. Add `pika.SSLOptions` with the FlowFirst pipeline certificate (`TLS_CLIENT_CERT` / `TLS_CLIENT_KEY`) — see `src/config/config.py` `get_connection()` for the exact pattern.
- Authentication / authorisation between the listener and the REST API (the listener bypasses the API entirely).
- Guaranteeing delivery when the listener is offline. Envelopes published while no queue is bound to the exchange are silently dropped by RabbitMQ. Start the listener (so the queue exists and is bound) before traffic begins if you need every notification.

---

## Authentication Architecture

> **Status:** API-key and HMAC token authentication are implemented in `src/application/authz.py` and wired into API Gateway (Process 1). The `none` (default) and `cert` modes re-use the existing cert infrastructure. All modes are unit-tested (25 new tests in `tests/test_authz.py`). The HTTP handler integration (`_require_auth`, `POST /auth/token`) was tested by running the existing process1 harness tests with `AUTH_MODE=none`; the harness does not yet exercise auth refusals on flow endpoints directly — add them if you extend this. MFA, biometric, SSO and FIDO2 are documented below but **not implemented**.

### Why authentication matters here

TLS (Scenario 63) proves the *connection* is encrypted and the *certificate* came from the FlowFirst CA — but any client with a valid FlowFirst certificate, or any client that can reach port 8080 without TLS, can publish messages to the pipeline. The flow endpoints (`/api/flow1-3`, `/api/batch`) have no caller gate by default. The options below add one.

### Option 1 — Password / API-key authentication

**How it works.** The caller puts a random key in the `X-Api-Key` header. API Gateway (Process 1) hashes the incoming value with SHA-256 and compares it (constant-time, no timing oracle) against the stored hash. The raw key never touches disk; only its digest is stored in `.env`.

**To enable:**
```bash
# Generate a key (run once per calling service):
python3 -c "import secrets,hashlib; k=secrets.token_urlsafe(32); \
  print(f'myapp:{hashlib.sha256(k.encode()).hexdigest()}  # raw key: {k}')"
# → myapp:7f3a...  # raw key: abc123...
```
Add the `label:digest` pair to `AUTH_API_KEYS` in `.env` on every node (comma-separated for multiple callers), then:
```
AUTH_MODE=api_key
AUTH_API_KEYS=myapp:7f3a...,ci_bot:9e2c...
```

**Call the API:**
```bash
curl -X POST http://<VIP>:8080/api/flow1 \
  -H "X-Api-Key: abc123..." \
  -H "Content-Type: application/json" \
  -d '{"item_id": 1}'
```
A missing or wrong key returns HTTP 401. The key label appears in the API Gateway (Process 1) log so you can see which caller sent each request.

**Key rotation:** add the new key alongside the old one (both work), update all callers, then remove the old digest from `AUTH_API_KEYS` and reload (`sudo systemctl reload flowfirst-process1`). There is no downtime if you overlap.

**Limits:** API keys are long-lived secrets. Anyone who intercepts the key can use it until it is rotated. Use TLS to protect the key in transit; without TLS the key is visible to anyone on the network.

---

### Option 2 — Token / Bearer authentication (HMAC-signed)

**How it works.** API Gateway (Process 1) issues short-lived tokens at `POST /auth/token`. Each token is a base64-encoded JSON payload (`sub`, `exp`, version) plus an HMAC-SHA256 signature over it, separated by `.`. The signature is computed with `AUTH_TOKEN_SECRET` (a shared secret, never sent to callers). Tokens expire after `AUTH_TOKEN_TTL_S` (default 1 hour). On every flow request the caller sends `Authorization: Bearer <token>`; API Gateway (Process 1) verifies the signature and the expiry before proceeding.

**To enable:**
```bash
# Generate the signing secret (run ONCE, put the SAME value on all nodes):
python3 -c "import secrets; print(secrets.token_hex(32))"
```
```
AUTH_MODE=token
AUTH_API_KEYS=myapp:7f3a...     # api_key used to authenticate the token-issuance request
AUTH_TOKEN_SECRET=<64 hex chars from above>
AUTH_TOKEN_TTL_S=3600           # token lifetime; shorten for higher-risk environments
```

**Token flow:**
```bash
# Step 1 — get a token (once per session / per expiry):
TOKEN=$(curl -s -X POST http://<VIP>:8080/auth/token \
  -H "Content-Type: application/json" \
  -d '{"api_key": "abc123..."}' | python3 -c "import sys,json; print(json.load(sys.stdin)['token'])")

# Step 2 — use it for flow requests:
curl -X POST http://<VIP>:8080/api/flow1 \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"item_id": 1}'
```
In `token` mode, `X-Api-Key` is also accepted directly on flow endpoints, so short-lived clients don't need to implement the token exchange.

**Token rotation / secret rotation:** change `AUTH_TOKEN_SECRET` and restart all API Gateway (Process 1) instances. Every token signed with the old secret is immediately invalid; callers must obtain a new token. Because `AUTH_API_KEYS` is separate, key rotation and secret rotation are independent.

**Limits:** tokens are not revokable before expiry — a stolen token is valid until `exp`. Keep `AUTH_TOKEN_TTL_S` short for sensitive environments. The token payload is base64-encoded (not encrypted): the subject and expiry are readable to anyone who has the token, though the content is not a secret. This is not a JWT and does not use an external identity provider.

---

### Option 3 — Certificate authentication (cert mode)

**How it works.** This re-uses the mutual-TLS infrastructure from Scenario 63. Any client that presents a certificate signed by the FlowFirst CA is allowed to call the flow endpoints. The certificate's CN is logged as the caller identity.

**To enable:**
```
TLS_API_ENABLED=true            # must be on; without TLS there are no client certificates
AUTH_MODE=cert
```
Issue a certificate for each calling service with `security/issue_cert.sh`. The admin gate (`POST /zk/config`) stays on its own `ADMIN_ALLOWED_CNS` list and is not affected by `AUTH_MODE`.

**Limits:** every valid FlowFirst certificate grants access, including internal service certificates (`<node>-pipeline`). To restrict to specific callers, use `AUTH_MODE=none` and rely on the admin gate's `ADMIN_ALLOWED_CNS` pattern — but that only gates the admin endpoint. For flow-level per-caller restrictions, switch to `api_key` or `token` mode.

---

### Combining modes with the existing admin gate

| Concern | Mechanism | Configured by |
|---|---|---|
| Flow endpoint access (publish messages) | `AUTH_MODE` | `AUTH_MODE`, `AUTH_API_KEYS`, `AUTH_TOKEN_SECRET` |
| Admin config changes (`POST /zk/config`) | Certificate CN allow-list | `ADMIN_ALLOWED_CNS`, `ADMIN_TRUSTED_PROXY_CNS` |
| Transport confidentiality and peer identity | Mutual TLS | `TLS_API_ENABLED`, Scenario 63 |

The three layers are independent. You can run `AUTH_MODE=none` with mutual TLS (only cert-holders can connect, but any cert-holder can publish), or `AUTH_MODE=api_key` without TLS (keys work but are sent in clear text — not recommended), or both together.

---

### Options documented here but not implemented: MFA, biometric, SSO, FIDO2

These require an external identity system (an identity provider, authenticator app, hardware key or browser). They are not feasible without one. The descriptions below explain what each is and how it would integrate.

#### Multi-Factor Authentication (MFA)

MFA requires a caller to prove two independent factors, typically something they know (a password or API key) and something they possess (a one-time code from an authenticator app, an SMS, a hardware token). In a REST API context the most common pattern is TOTP (Time-based One-Time Password, RFC 6238): the caller presents `{"api_key": "...", "totp": "123456"}` to the token endpoint; the server verifies the TOTP against the shared seed and issues a short-lived token only if both checks pass.

To add MFA to this solution: store a TOTP seed alongside each API key in a secrets store (not `.env`), verify the 6-digit code with a library such as `pyotp`, and issue the token only on success. The flow endpoint does not change — it still validates a Bearer token. MFA adds a second gate at the token-issuance step. Not implemented here because it requires a secrets store and out-of-band seed distribution.

#### Biometric authentication

Biometrics (fingerprint, face, iris) authenticate a person at a device. The biometric match runs on the device, not the server; the server receives a signed assertion proving the match occurred. In a web context this is almost always delivered via WebAuthn (the FIDO2 protocol). In a machine-to-machine API like FlowFirst (where there is no browser or human at the keyboard) biometrics are not applicable: calling services are not people, and pipeline processes run unattended. Biometrics could be used to protect the operator's console login (Linux PAM, SSH keys managed by a hardware authenticator), but that is outside the scope of the API itself.

#### Single Sign-On (SSO) — SAML and OIDC

SSO lets users authenticate once with a central Identity Provider (IdP — e.g. Entra ID, Okta, Keycloak) and receive a credential they can use with multiple services. For REST APIs the common protocol is OpenID Connect (OIDC), which sits on top of OAuth 2.0: the IdP issues a signed JWT access token; the service verifies it against the IdP's public key (fetched from the JWKS endpoint).

To add OIDC to this solution: replace `POST /auth/token` with an OIDC authorisation code or client-credentials flow at the IdP; verify the `Authorization: Bearer <id_token>` on each request by checking the JWT signature against the IdP's JWKS URL and validating `iss`, `aud`, `exp` and `iat` claims. The `verify_token` function in `authz.py` would be replaced by a standard JWT library (e.g. `python-jose` or `PyJWT`). The `AUTH_TOKEN_SECRET` shared secret is replaced by the IdP's asymmetric key pair, so the server never holds a signing key. This is the correct approach when the cluster is part of a larger platform with an existing user directory. Not implemented here because it requires an external IdP.

#### Passwordless / FIDO2 passkeys

FIDO2 passkeys use public-key cryptography on a device (hardware security key, phone, or platform authenticator) to authenticate without a password. The server stores only a public key; the private key never leaves the device. The protocol (WebAuthn) requires a browser or platform runtime, a user presence check, and a challenge-response round trip: the server sends a nonce, the authenticator signs it with the private key, the server verifies with the stored public key.

In the context of FlowFirst: passkeys are relevant for the *operator* accessing the management endpoints (e.g. `/zk/config`) from a browser-based admin tool, not for machine-to-machine API calls. To add passkey support: implement the WebAuthn registration and assertion flows on a management UI, issue a session token (similar to `POST /auth/token`) after a successful assertion, and use that token for subsequent management API calls. Libraries: `py_webauthn` (Python). Hardware key support adds phishing resistance that software TOTP does not provide. Not implemented here because it requires a management UI and a WebAuthn-capable client.

---

### Limits of the implemented authentication

1. API keys and token secrets are stored in `.env` — a flat file on each node. Anyone who can read `/opt/flowfirst/.env` (typically `flowuser` and `root`) can extract them. Protect the file with filesystem permissions (`chmod 600`) and disk encryption. A proper secrets manager (Vault, AWS Secrets Manager, RHEL IdM) would seal them at rest.
2. Tokens are not revokable before expiry. If a token is stolen, the only remediation is to rotate `AUTH_TOKEN_SECRET` (which invalidates all tokens immediately, logging out all current callers).
3. There is no per-caller rate limiting. An authenticated caller can submit as many requests as the pipeline can handle. Scenario 61 sizes the cluster capacity; a malicious or buggy authenticated caller can fill the queues.
4. `AUTH_API_KEYS` and `AUTH_TOKEN_SECRET` must be identical on all three nodes. They are not auto-synced — change them in `.env` on every node and restart API Gateway (Process 1) on each (rolling restart is safe: during the restart one node will briefly use the old secret, tokens signed by the other nodes still work because the secret is the same).
5. The `cert` mode does not distinguish between calling services: every certificate issued by the CA grants the same access. For per-service access control use `api_key` or `token` mode with a separate key per service.

---

## Authorization Architecture

> **Status:** RBAC, ACL, and ABAC authorization are fully implemented in [`src/application/authz.py`](src/application/authz.py) and wired into API Gateway (Process 1). Default `AUTHZ_MODE=none` preserves existing behaviour. 54 new tests in [`tests/test_authz.py`](tests/test_authz.py). OAuth2, ReBAC, DAC, MAC, and Policy-as-Code are documented below but **not implemented** (each requires external infrastructure).

Authorization runs **after** authentication. Authentication answers *"who are you?"* (identity); authorization answers *"what are you allowed to do?"* (access control). They are independent layers — you can run `AUTH_MODE=api_key` with full RBAC, or `AUTH_MODE=none` (no authentication) with `AUTHZ_MODE=none` (no authorization, the default), or any combination.

### Resources and actions

Every protected endpoint is represented as `resource:action`:

| Endpoint | Resource | Action |
|---|---|---|
| `POST /api/flow1` | `flow:1` | `publish` |
| `POST /api/flow2` | `flow:2` | `publish` |
| `POST /api/flow3` | `flow:3` | `publish` |
| `POST /api/batch` | `flow:*` | `publish` |
| `POST /zk/config` | `zk:config` | `write` |
| `GET /zk/config` | `zk:config` | `read` |
| `GET /zk/health` | `zk:health` | `read` |
| `GET /swift/objects` | `swift:objects` | `list` |
| `GET /swift/object/<name>` | `swift:object` | `read` |
| `GET /gt/status` | `gt:status` | `read` |
| `GET /health` | `health` | `read` |
| `GET /ready` | `ready` | `read` |

Resource patterns support `fnmatch` globs: `flow:*` matches `flow:1`, `flow:2`, and `flow:3`.

---

### Option 1 — RBAC (Role-Based Access Control)

**How it works.** Every identity is assigned one or more roles. Each role carries a set of `resource:action` permission tokens. If any of the identity's roles grants the requested `resource:action`, the request is allowed.

**Built-in roles** (always available, no configuration needed):

| Role | Permissions |
|---|---|
| `publisher` | `flow:1:publish`, `flow:2:publish`, `flow:3:publish` |
| `reader` | `health:read`, `ready:read`, `gt:status:read`, `zk:health:read`, `zk:config:read`, `swift:objects:list`, `swift:object:read` |
| `admin` | Everything in `reader` + `zk:config:write` |
| `operator` | All of the above (`publisher` + `admin`) |

**To enable:**
```
AUTHZ_MODE=rbac
AUTHZ_IDENTITY_ROLES=ci_bot=publisher;  ops_alice=operator;  monitor_svc=reader
```

`ci_bot` may publish to any flow; `ops_alice` has all access; `monitor_svc` may only read.

**Custom roles** — define additional roles in `AUTHZ_ROLES`:
```
# Format: rolename=resource:action,resource:action;  rolename2=...
AUTHZ_ROLES=ingest=flow:1:publish,flow:2:publish;  readonly_monitor=health:read,ready:read
AUTHZ_IDENTITY_ROLES=legacy_app=ingest;  nagios=readonly_monitor
```

**Identity source.** The identity used for role lookup is:
- In `AUTH_MODE=api_key` or `token` — the API key label (e.g. `ci_bot`) or the token subject.
- In `AUTH_MODE=cert` — the client certificate CN.
- In `AUTH_MODE=none` — the client certificate CN if present, otherwise `anonymous`.

---

### Option 2 — ACL (Access Control List)

**How it works.** An explicit per-resource allow-list maps each `resource:action` to a set of permitted identities. There are no roles — each identity must appear in the ACL for each resource it needs.

**To enable:**
```
AUTHZ_MODE=acl
# Format: resource:action=identity1,identity2;  resource2:action=identity3
AUTHZ_ACL=flow:1:publish=ci_bot,data_ingest;  zk:config:write=admin-cert;  flow:*:publish=batch_svc
```

`ci_bot` and `data_ingest` may publish to Flow 1; `admin-cert` may write ZK config; `batch_svc` may publish to any flow. Any identity **not in the list for a given resource:action** is denied even if it appears elsewhere in the ACL.

**Wildcard resources** work in ACL entries exactly as in RBAC permissions: `flow:*:publish=batch_svc` grants publish on `flow:1`, `flow:2`, and `flow:3`.

---

### Option 3 — ABAC (Attribute-Based Access Control)

**How it works.** A JSON list of rules is evaluated in order. Each rule has a `when` map of attribute conditions and an `effect` of `"allow"` or `"deny"`. The first rule whose conditions all match wins.

**Supported `when` keys:**

| Key | Matches when |
|---|---|
| `identity` | Caller's identity equals this value exactly |
| `role` | Caller has this role (requires `AUTHZ_IDENTITY_ROLES`) |
| `action` | Action equals this value (`publish`, `read`, etc.) |
| `resource` | Resource equals or glob-matches this value |
| `method` | HTTP method (`GET`, `POST`, …) |
| `hour_utc_lt` | Current UTC hour is **less than** this number (0–23) |
| `hour_utc_gte` | Current UTC hour is **≥** this number (0–23) |

**To enable:**
```
AUTHZ_MODE=abac
# JSON must be on one line in .env (python-dotenv limitation)
AUTHZ_POLICY=[{"when":{"identity":"bad_bot"},"effect":"deny"},{"when":{"action":"publish","hour_utc_lt":8},"effect":"deny"},{"when":{"action":"publish","hour_utc_gte":18},"effect":"deny"},{"when":{"role":"publisher"},"effect":"allow"}]
```

This policy: denies `bad_bot` at any time, denies all publishes outside 08:00–18:00 UTC, and allows anyone with the `publisher` role during business hours.

**Unknown `when` keys are silently ignored** (forward compatibility) — add new keys in a future version without breaking existing policies.

---

### Combining layers: `rbac+acl`, `rbac+abac`, `rbac+acl+abac`

For production the recommended mode is `rbac+acl+abac`. Each layer adds a different kind of control:

```
AUTHZ_MODE=rbac+acl+abac
AUTHZ_IDENTITY_ROLES=ci_bot=publisher;  ops=operator
AUTHZ_ACL=flow:1:publish=ci_bot,emergency_svc
AUTHZ_POLICY=[{"when":{"identity":"banned_key"},"effect":"deny"}]
```

**Evaluation order** (hardwired, cannot be changed):

```
1.  ABAC deny rules     — explicit deny beats everything
2.  RBAC                — role-based grant
3.  ACL                 — per-resource explicit grant (can override RBAC denial)
4.  ABAC allow rules    — last-chance grant (can override RBAC + ACL denial)
5.  Default: DENY       — fail closed
```

> 📐 **[View diagram: Authorization Decision Flowchart](docs/diagrams.md#7-authorization-decision-flowchart-rbac--acl--abac)**

---

### End-to-end example: ci_bot publishes to Flow 1

```bash
# .env configuration
AUTHZ_MODE=rbac+acl+abac
AUTHZ_IDENTITY_ROLES=ci_bot=publisher
AUTHZ_ACL=flow:1:publish=ci_bot
AUTHZ_POLICY=[{"when":{"identity":"banned_key"},"effect":"deny"}]
AUTH_MODE=api_key
AUTH_API_KEYS=ci_bot:<sha256_of_ci_bot_key>

# Request:
curl -X POST http://<VIP>:8080/api/flow1 \
  -H "X-Api-Key: <ci_bot_raw_key>" \
  -H "Content-Type: application/json" \
  -d '{"item_id": 42}'

# API Gateway (Process 1) checks, in order:
# 1. Authentication: sha256(key) matches ci_bot digest → identity = "ci_bot"
# 2. ABAC deny: "banned_key" rule does not match "ci_bot" → continue
# 3. RBAC: "ci_bot" has role "publisher" → "publisher" grants "flow:1:publish" → ALLOW
```

---

### Options documented here but not implemented

#### OAuth 2.0 / OpenID Connect (OIDC)

OAuth 2.0 is an authorization *delegation* framework, not a self-contained implementation. It requires an **Authorization Server (AS)** — a separate service such as Keycloak, Okta, Entra ID, or Auth0 — that issues signed JWT access tokens. API Gateway (Process 1) would verify each token against the AS's public key (fetched from its JWKS endpoint) without needing the signing key itself.

**How it would integrate:**
- Replace `POST /auth/token` with an OAuth2 client-credentials flow at the AS.
- On each request, verify `Authorization: Bearer <jwt>` by checking the signature (RS256/ES256), `iss`, `aud`, `exp`, and `iat` claims.
- Extract the caller identity from the `sub` claim; feed it into the existing `check_authz` pipeline.
- Replace `AUTH_TOKEN_SECRET` with the AS's JWKS URL.

**Why not implemented here:** requires an external AS. The token mechanism in `authz.py` is a deliberate simplified stand-in for environments without an AS, not a production OAuth2 implementation.

---

#### ReBAC / Zanzibar (Relationship-Based Access Control)

ReBAC models permissions as a **graph of relationships** between principals and objects (e.g. "alice is member of group_a, group_a is editor of project_1, project_1 owns flow:1"). Google Zanzibar is the canonical design; open-source implementations include SpiceDB and OpenFGA.

**Why not implemented here:** ReBAC requires a persistent graph store (an external service) and a query API. For FlowFirst's flat resource model (9 resources, 4 actions, a handful of identities) the added complexity is not justified. RBAC covers the same ground with less infrastructure.

---

#### DAC (Discretionary Access Control)

In DAC, the *owner* of a resource decides who may access it and can delegate that right. Classic example: UNIX file permissions where the file owner can `chmod` and `chown`.

**Why not implemented here:** FlowFirst has no concept of resource ownership. Messages are produced by API Gateway (Process 1) and consumed by Processes 2–4; there is no per-message owner whose permissions should vary. Implementing DAC would require attaching an owner identity and a permission mask to every resource object and checking them at every access point — a significant data-model change with no clear benefit for a pipeline.

---

#### MAC (Mandatory Access Control)

In MAC, a central authority assigns **sensitivity labels** (e.g. Confidential, Secret, Top Secret) to subjects and objects. Access is controlled by comparing labels: a subject may only read objects at its clearance level or below (no-read-up) and write objects at its clearance level or above (no-write-down). The owner cannot override the label. SELinux on RHEL 9 is a MAC system for the OS layer.

**Why not implemented here:** MAC requires a classification system. FlowFirst does not classify messages — all pipeline data is treated uniformly. MAC would be appropriate if the pipeline carried data of different sensitivity levels (e.g. PII vs. non-PII flows) and callers needed to be restricted by clearance. To add it: assign a numeric level to each flow (`flow:1 = level 1`, `flow:2 = level 2`, …) and store a maximum-clearance level per identity; reject requests where the identity's clearance is below the resource's level.

---

#### Policy-as-Code (OPA / Cedar)

Policy-as-Code separates authorization logic from application code. The application makes an authorization query (`{"principal": ..., "action": ..., "resource": ...}`) to a policy engine, which evaluates it against a policy written in a dedicated language and returns allow/deny.

- **Open Policy Agent (OPA)**: uses Rego, a declarative query language. The application calls the OPA REST API or embeds the OPA Go library. Policies are stored in and hot-reloaded from version-controlled `.rego` files.
- **Cedar**: AWS's policy language, designed for fine-grained authorization. The Cedar SDK is available in Python.

**Why not implemented here:** the ABAC engine in `authz.py` covers the same use cases (attribute matching, time-based rules) with a far smaller footprint — no external process, no Rego learning curve, no SDK dependency. For an organization that already runs OPA as a policy sidecar, replacing `check_authz` with an OPA query is straightforward: call `http://localhost:8181/v1/data/flowfirst/allow` with the same attribute dict and interpret the response.

---

### Limits of the implemented authorization

1. **No session persistence.** `_AUTHZ_ROLES`, `_AUTHZ_IDENTITY_ROLES`, `_AUTHZ_ACL`, and `_AUTHZ_POLICY` are parsed once at process start from `.env`. Changing them requires restarting API Gateway (Process 1) on every node (rolling restart is safe: each node picks up the new policy independently).
2. **Flat identity model.** There are no groups within RBAC — assign each identity individually in `AUTHZ_IDENTITY_ROLES`. For large deployments, consider OPA or a directory-backed identity store.
3. **`flow:*` in the ACL is a glob, not a literal.** An ACL entry `flow:*:publish=svc` grants `svc` publish access on all three flows. If you need per-flow ACLs alongside a wildcard entry, list the flows explicitly.
4. **ABAC time rules use UTC server time.** If callers are in different time zones, make sure the time window in `AUTHZ_POLICY` accounts for them.
5. **No deny logging to a SIEM.** Denied requests are logged to the API Gateway (Process 1) journal at `WARNING` level (`[authz] DENIED`). For audit trails, forward the journal to a SIEM (e.g. with `systemd-journal-remote` or Filebeat).

---

## Backup and Restore

> **Status:** the scripts below were tested with real MariaDB 10.11, ZooKeeper 3.9 and RabbitMQ 3 containers on one workstation (database round trip with 500 awkward rows, a simulated table loss, ZooKeeper config, RabbitMQ definitions, damage and wrong-passphrase detection). **They have not been run on the four-node cluster**, not against Galera, not with TLS on, not on RHEL, and not from Pacemaker or cron. Rehearse a full restore on a test cluster before relying on them.

### What needs a backup, and what does not

| State | Where it lives | In the backup? | Why |
|---|---|---|---|
| Processed messages (the business data) | MariaDB `processed_messages`, incl. `raw_payload` | **Yes** (`db`) | the only copy of record |
| Swift objects | `/srv/node` on nodes 1-3 | **Yes** (`swift`) | `backup_flowfirst.sh --only swift` downloads every object from `SWIFT_CONTAINER` via the `swift` CLI and encrypts them. If Swift is disabled or the CLI is not present the part is skipped with a warning (not failed). Restore: `restore_flowfirst.sh ... --swift`. |
| Swift hash prefix/suffix and rings | `/etc/swift` | **Yes** (`config`) | without them no stored object can ever be located again |
| `.env`, `galera.cnf`, `haproxy.cfg`, `zoo.cfg` + `myid`, `rabbitmq.conf`, systemd units | each node | **Yes** (`config`) | they hold the passwords and node identity |
| TLS directory including **private keys** and, if you keep it there, the CA | `/etc/flowfirst/tls` | **Yes** (`config`) | re-issuing is possible, but every certificate would change at once |
| ZooKeeper runtime tuning | `/flowfirst/config/*` | **Yes** (`zk`) | operator-chosen values |
| ZooKeeper election, registry, health, dedup | `/flowfirst/...` | No | rebuilt by the running processes |
| RabbitMQ users, permissions, policies (`flowfirst-dlx`), queue definitions | RabbitMQ | **Yes** (`rabbit`) | rebuilding by hand is error-prone |
| **Messages waiting in queues** (including `flowfirst_dead_letters`) | RabbitMQ | **No** | transient pipeline state. After a total loss they are lost; clients whose request returned `200` but whose message had not reached the database yet must resend. Run `replay_dead_letters.py` and let queues drain before a planned restore. |
| The CA private key kept offline | admin host | not by these scripts | back it up yourself, offline and separately |

Because Galera has three members, most single-node database failures need **no restore**: a replacement node re-syncs (IST/SST). RabbitMQ is different: the pipeline queues are classic durable queues on one node each and are **not** replicated (see [Message Safety](#message-safety-publisher-confirms-and-the-dead-letter-queue), limit 1), so messages queued on a lost node can be lost. Backups are for the cases replication does not help with: a bad `DELETE`, a bug that wrote wrong data to all three copies, ransomware, loss of the whole site, or a wrong password change.

### Making a backup

```bash
# once: a passphrase of at least 16 characters, mode 600, kept OFF the cluster and away from the backups
sudo sh -c 'umask 077; openssl rand -base64 32 > /root/.flowfirst-backup.pass'

# on ONE of nodes 1-3 (needs mariadb-dump or mysqldump, rabbitmqctl, the Python venv for ZooKeeper,
# and the 'swift' CLI for the swift part; activate the venv or set PATH accordingly):
sudo ./scripts/backup_flowfirst.sh --out /backup/flowfirst --passphrase-file /root/.flowfirst-backup.pass --keep 14

# on EVERY node (each has its own .env, myid and certificates):
sudo ./scripts/backup_flowfirst.sh --out /backup/flowfirst --passphrase-file /root/.flowfirst-backup.pass --only config
```

* Everything is encrypted with AES-256-CBC and PBKDF2 (600,000 iterations) **before** it touches disk; the database dump is streamed through `openssl`, so no plaintext dump ever exists. The files are mode 600 in a mode 700 directory.
* The database dump uses `--single-transaction`, so it is consistent without locking writers, and reads from this node. On Galera a dump does not desync the node (unlike an `rsync` SST).
* After writing, each file is decrypted again and checked (`gzip -t`), and its SHA-256 goes into `MANIFEST.sha256`. The exit code is 0 only if every selected part succeeded. A failed part leaves `BACKUP.info` saying `parts_failed=...`, and `--keep` never deletes older backups when the new one is incomplete.
* **Losing the passphrase loses every backup.** Store it in a password manager or vault, not on the backup host.
* To run it daily, call the first command from a systemd timer or root's cron (an example is not provided and was not tested). Copy `/backup/flowfirst` off the site; this script does not.

### Checking a backup (do this on a schedule)

```bash
./scripts/restore_flowfirst.sh --from /backup/flowfirst/flowfirst-<host>-<time> --passphrase-file /root/.flowfirst-backup.pass --verify
```

`--verify` changes nothing. It checks the manifest, the checksums, that every file decrypts with this passphrase and that archives and dumps can be read to the end. A backup that has never been verified is a hope, not a backup.

### Restoring

| Goal | Command |
|---|---|
| Prove a dump is usable, and compare it | `restore_flowfirst.sh --from DIR --passphrase-file F --db --into-database flowfirst_restore_check` then compare `SELECT COUNT(*)` / `CHECKSUM TABLE` with the live table |
| Recover some rows after a bad delete | the same, then `INSERT ... SELECT` from `flowfirst_restore_check` into the live table; drop the check database afterwards |
| Replace the live database | stop DB Updater (Process 4) (`sudo pcs resource disable flowfirst-p4-res-clone`), then `restore_flowfirst.sh ... --db --in-place --yes-overwrite`, then `pcs resource enable flowfirst-p4-res-clone`. The dump drops and recreates `MARIADB_DB`; Galera replicates the result to the other nodes. |
| Get configuration files back | `restore_flowfirst.sh ... --extract-config /root/restore-tmp` (an existing, **empty** directory). Nothing is copied over `/`; you copy the files you need by hand, then delete the directory because it holds keys and passwords. |
| ZooKeeper runtime tuning | `restore_flowfirst.sh ... --zk` (only the four known keys are written; anything else in a doctored file is skipped) |
| RabbitMQ users and policies | `restore_flowfirst.sh ... --rabbit` decrypts to a temporary file and prints the `rabbitmqctl import_definitions` command to run from a second terminal; it merges, it does not wipe |
| Swift objects | `restore_flowfirst.sh ... --swift` decrypts the archive and re-uploads every object to `SWIFT_CONTAINER` (use `--swift-container NAME` to override). Objects already in the container are overwritten. The container is created if missing. |

Full-site recovery order: time sync (Phase 0a), restore `config` on each node, bring up MariaDB (bootstrap one node, `--db` restore, join the others), ZooKeeper, RabbitMQ (import definitions), Swift (restore `/etc/swift` first so the rings match, then `--swift` to restore objects), HAProxy, Pacemaker, then the processes. Only the database part of this sequence was exercised.

**Recovery point and time:** the recovery point is the time of the last backup (messages after it are gone unless clients resend); the recovery time is whatever the dump size and the network dictate. Neither was measured on the cluster; time a restore of a production-sized dump and record it.

### Limits of this work

1. Logical dump: `mariadb-dump` is simple and portable, but slow to restore for very large tables. For hundreds of gigabytes use `mariabackup` (physical, supports incremental) instead; no script for that is provided.
2. No point-in-time recovery: binary logging is not configured, so you cannot restore "to 14:03".
3. Swift objects are backed up by the `swift` part, but the backup is a point-in-time snapshot: objects written after the backup and before a failure are lost. Objects in Swift are also present as `raw_payload` in the database dump, so a total loss of Swift can be recovered from the database alone by re-running the pipeline with the existing rows; only objects that differ from the database (manually uploaded, or modified outside the pipeline) are permanently at risk.
4. The encryption protects confidentiality and uses a checksum for damage; it does not stop someone who can write the backup store from replacing files **and** the manifest. Keep an out-of-band copy of the manifest digests if that matters.
5. `--only config` copies keys into the archive; protect and rotate the passphrase like the keys themselves. The CA key is only included if it lives under `TLS_DIR`.
6. Tested with MariaDB 10.11 in a container, not the Galera build in production, and without `TLS_MARIADB_ENABLED`; the TLS client options in the generated option file were not exercised.
7. `mapfile`, used by `--keep`, needs bash 4 (RHEL 9 has it; macOS' default bash 3.2 does not). The retention step was not exercised.
8. Restoring a database does not restore the ZooKeeper dedup znodes (they expire anyway), so replaying old messages after a restore can insert rows again where the database no longer has them. This is intended, because the database is the record.

## Message Safety: Publisher Confirms and the Dead-Letter Queue

> **Status:** tested on one workstation against a real RabbitMQ 3 container (single node, not a cluster) using the repo's own `config.setup_queues()`, the policy command from `scripts/setup_rabbitmq_dlx.sh --print`, and `scripts/replay_dead_letters.py`. Every behaviour listed under "Tested" was observed, not assumed. **Not tested:** a three-node RabbitMQ cluster, node failure during a publish, quorum or mirrored queues (the queues here are classic durable queues), TLS on these paths, or Transformer (Process 2) to 5 rejecting a real message end to end (the rejection was simulated by a client doing `basic_nack(requeue=False)`, which is exactly what the handlers do).

This chapter explains two protections and how they fit together. The run-book step that applies them is [R3](#r3-dead-letter-policy-and-publisher-confirms).

**1. Publisher confirms and `mandatory=True` (in the code, no switch).** Every publish to RabbitMQ, from API Gateway (Process 1) and from Processes 2 to 5, waits for the broker's answer (`channel.confirm_delivery()` in `setup_queues`) and sets `mandatory=True`. So a publish either succeeds or **raises**:

| Situation | Result | Tested |
|---|---|---|
| Queue exists | accepted | yes |
| Routing key matches no queue, `mandatory=True` | `pika.exceptions.UnroutableError`; API Gateway (Process 1) answers with an error instead of `200` | yes |
| Same, with `mandatory=False` | accepted and **silently dropped** | yes (this is why the flag is set) |
| Broker refuses the message (`basic.nack`) | `pika.exceptions.NackError` | not provoked |
| Connection lost mid-publish | exception from pika; API Gateway (Process 1) reconnects on the next request | covered by the broker-restart test in the throughput chapter |

A consumer (Transformer (Process 2) to 5) acknowledges its input message only after its own publish was confirmed, so a message is never acknowledged upstream and lost downstream. Confirms cost one broker round trip per publish; see the throughput chapter and `API_PUBLISH_CONNECTIONS`. A confirm means the broker took the message, not that it reached the database.

**2. The dead-letter queue (a RabbitMQ policy you must apply).** When a handler raises, the process logs the exception, increments `flow<N>_failed`, and does `basic_nack(requeue=False)`. What happens next depends on one policy:

| Policy `flowfirst-dlx` | A rejected message | Tested |
|---|---|---|
| **Not applied** | **discarded**: both the pipeline queue and `flowfirst_dead_letters` are empty afterwards | yes |
| Applied | moved to the durable queue `flowfirst_dead_letters` (via the fanout exchange `flowfirst.dlx`), with an `x-death` header recording the queue it came from and the reason `rejected` | yes |

`sudo ./scripts/setup_rabbitmq_dlx.sh` applies it once on one RabbitMQ node (`--print` shows the command, `--remove` clears it). It is a policy rather than a queue argument so it can be added to a running cluster; changing queue arguments would make RabbitMQ refuse every process's queue declaration. The pattern `^flow[123]_` matches the nine pipeline queues and not `flowfirst_dead_letters`, so a dead-lettered message cannot be dead-lettered again in a loop. The exchange and queue are created by the processes (`setup_queues`) and are idempotent. `scripts/preflight_check.py` reports a missing policy as **FAIL** and a non-empty dead-letter queue as WARN.

**What fills the dead-letter queue.** Anything that makes a handler raise: a database that is down or has a lock timeout (DB Updater (Process 4)), Swift errors that are not caught, a malformed message, a bug. While MariaDB is down, every message DB Updater (Process 4) receives goes to the dead-letter queue (or is discarded without the policy), so an outage can create many entries. The `FlowFirstDeadLetters` alert fires on the first one ([Monitoring and Alerting](#monitoring-and-alerting)).

**Working the queue** (`scripts/replay_dead_letters.py`; uses the same `.env`, TLS and client certificate as the pipeline):

```bash
python3 scripts/replay_dead_letters.py                    # list: message_id, item_id, flow, origin queue, reason. Changes nothing.
python3 scripts/replay_dead_letters.py --replay --limit 1 # try one first
python3 scripts/replay_dead_letters.py --replay           # send every message back to the queue it was rejected from
python3 scripts/replay_dead_letters.py --discard          # delete them, after you decided they are not worth keeping
```

1. **Find the cause first** (the process log has the traceback and the `[Flow N] Error` line). A message that fails for a permanent reason goes straight back to the dead-letter queue after replay.
2. Replay republishes with a confirm and `mandatory=True`, then acknowledges the dead-letter copy. If the broker refuses the republish the message **stays** in the dead-letter queue. Tested: bodies came back identical, persistent (`delivery_mode=2`), and without the `x-death` header, so a replayed message that fails again is a fresh failure.
3. A message **without** an `x-death` header (for example one put into the dead-letter queue by hand) has an unknown origin: replay lists it as `UNKNOWN`, says it cannot replay it and leaves it. `--discard` removes it. Tested.
4. Replay and discard handle the messages waiting when the script starts (or `--limit N`). `--limit` does not choose which messages: it takes them in queue order. The listing is read-only: messages are released back unchanged.
5. Duplicates: a replayed message reuses its original `message_id`. DB Updater (Process 4) checks the ZooKeeper dedup barrier first, so a message that was already persisted is skipped, **but only for `ZK_DEDUP_TTL_MS` (default 5 minutes)** after it was first marked. After that the same id passes the barrier again. The table has a `UNIQUE` key on `message_id` and the insert is `ON DUPLICATE KEY UPDATE` (`src/db/db.py`), so a second pass **overwrites the existing row with the replayed message's values instead of adding a second row**: no duplicate rows, but the stored values become those of the replayed copy. I read this in the code; I did not run a replay through DB Updater (Process 4) against MariaDB.
6. If the origin queue was deleted, the script re-declares it first (`setup_queues`) and the replay succeeds. Tested.

**Limits (read these).**
1. **Pipeline queues hold messages on one RabbitMQ node by default (`RABBITMQ_QUEUE_TYPE=classic`).** A node that dies before the message is consumed can lose the messages stored only on it; confirms do not change that. Set `RABBITMQ_QUEUE_TYPE=quorum` to replicate every queue across all nodes (a publisher confirm is sent only after a majority has the message). The type cannot be changed while the queues exist: stop the pipeline, run `scripts/migrate_queue_type.py --to quorum` (it moves messages one by one with confirms, is safe to re-run if interrupted, and prints a dry-run option), then set `RABBITMQ_QUEUE_TYPE=quorum` in `.env` on every node before starting the processes.
2. The dead-letter queue is just another queue: it has no size or age limit, and nothing replays it automatically. Messages wait until a person acts. It is not part of the backup (see [Backup and Restore](#backup-and-restore)).
3. The policy and the script assume RabbitMQ's default virtual host `/`; the code connects without choosing another one.
4. Replay does not preserve ordering relative to newer messages.
5. API Gateway (Process 1) can answer `200` only for what the broker confirmed, but a message that the broker took and a later stage then rejects is no longer visible to the client: the `200` response does not mean the row exists. Use the dead-letter alert and the persisted/published counters to notice.

---

## Health, Readiness and Dependencies

> **Status:** tested two ways on one workstation. (1) Unit tests with the real API Gateway (Process 1) handler and a stubbed ZooKeeper whose calls hang 20 s: `/health` and `/ready` answer in well under 3 s, and deliberately re-adding a ZooKeeper call to `/health` made the test fail. (2) The real API Gateway (Process 1) (`python -m src.application.process1`) in a container with a real single-node ZooKeeper 3.9 and RabbitMQ 3, with a real HAProxy (`haproxy:lts`) probing it every 3 s. With ZooKeeper **stopped**: `/health` 200 in about 1 ms, `/ready` 200 in about 1 ms and reporting ZooKeeper `disconnected`, `POST /api/flow1` still 200, a probe on `/health` stayed up (HAProxy returned 200), while a probe on `/zk/health` (the kind of call the old `/health` made) made HAProxy mark the backend down (503). The old `list_registered` call took 10.1 s to fail (`ZK_TIMEOUT`), and `/zk/health` did not answer within 40 s. After ZooKeeper was started again `/ready` returned to `connected` and every probe passed. **Not tested:** a three-node ensemble, Pacemaker, or this on RHEL. The old `/health` itself was not re-run end to end: its behaviour is inferred from the `/zk/health` probe, which makes the same kind of call.

API Gateway (Process 1) has three questions it can answer, and they use different endpoints so that one dependency problem cannot take the whole API out of rotation:

| Endpoint | Question | Calls out to | Used by |
|---|---|---|---|
| `GET /health` | Is this process alive and answering? | nothing (local state only) | **HAProxy** (`option httpchk GET /health`, every 3 s, `fall 2`) |
| `GET /ready` | Can this node publish right now? | nothing: it reads state the process already holds | operators, scripts, a load balancer you add yourself |
| `GET /zk/health` | What do ZooKeeper's registry and health tree say, and which workers are registered? | ZooKeeper | operators (`jq .registered_workers`) |

**Why `/health` changed.** It used to call ZooKeeper (`list_registered` for five processes). When ZooKeeper is slow or unreachable that call waits up to `ZK_TIMEOUT` (default 10 s), which is longer than HAProxy's patience for a probe (`fall 2` at 3 s intervals), so a ZooKeeper problem would make HAProxy mark **every** API node down at once (shown above with a probe on `/zk/health`), even though publishing does not need ZooKeeper. `/health` now returns the greenthread list, counters and the endpoint list, from memory. The fields `zk_config` and `zk_registered_workers` were **removed** from it: `zk_registered_workers` moved to `/zk/health` as `registered_workers`; `zk_config` stays on `GET /zk/config`. Anything you wrote that reads those two fields from `/health` must change.

**`/ready`** returns `200 {"status": "ready"}` when at least one RabbitMQ publisher connection is open and the process is not shutting down, otherwise `503 {"status": "not_ready"}`. It lists the open and configured connections and the ZooKeeper client state (`connected`, `disconnected`, `not_started`) as *information*: ZooKeeper being down does not make a node not ready, because publishing works without it (runtime config falls back to defaults, and leader election only affects Processes 2 to 4). The check reads local state only; it does not test the broker, so a connection that died a moment ago can still count as open until the heartbeat greenthread notices (up to 30 s).

**Do not point HAProxy at `/ready`.** Publisher connections are re-opened by the next publish, not in the background. A node that reports `not_ready` after a broker restart would be taken out of rotation, receive no requests, and therefore never reconnect. `/health` is the right probe; use `/ready` to look, not to route.

**Limits.**
1. `/health` being green means the process is running. It says nothing about RabbitMQ, MariaDB, ZooKeeper or Swift; for those use `/ready`, `/zk/health`, `/swift/status` and the alerts in [Monitoring and Alerting](#monitoring-and-alerting).
2. No endpoint checks MariaDB: API Gateway (Process 1) never talks to it.
3. `/ready` after an idle broker restart can say `not_ready` until the first request reconnects; see above.
4. `/health`, `/ready` and `/zk/health` follow the same TLS and client-certificate rules as the rest of the API.

---

## Monitoring and Alerting

> **Status:** the `/metrics` listener was tested on one workstation: unit tests, a child process serving it over plain HTTP and over mutual TLS, and a real Prometheus (container) scraping it with a client certificate (target `up`) and without one (target `down`). The alert rules pass `promtool check rules` and 7 `promtool test rules` scenarios on synthetic series. The RabbitMQ metric names were checked against a real RabbitMQ 3 container. `monitoring/alertmanager.yml` passes `amtool check-config`. **Not tested:** a scrape of a running 4-node cluster, the Galera/MariaDB exporter names (written from the exporter's documentation, syntax-checked only), `node_exporter` names, live Alertmanager routing delivering a notification, and no dashboard is shipped.

**What you get.** With `METRICS_ENABLED=true` each process serves `GET /metrics` in Prometheus text format on `METRICS_PORT_BASE` + its number: API Gateway (Process 1) on 9101, Transformer (Process 2) on 9102, … Remote Worker (Process 5) on 9105. It is a separate listener, not part of the REST API and **not behind HAProxy**, so Prometheus scrapes every node directly, including Node 4. The processes already counted what they handled; the listener only exposes it. It is off by default and a listener that cannot start (port in use, bad certificate) is logged and skipped: the pipeline keeps running, and it never falls back to plaintext when TLS is on.

| Series | Meaning |
|---|---|
| `flowfirst_messages_total{process,flow,event}` | Counters since process start. Events: `published` (P1), `processed` (P2, P5), `forwarded`/`reflected` (P3), `persisted`, `swift_stored`, `dedup_skipped` (P4), and **`failed`** (P2–P5: the handler raised and the message was nacked without requeue). |
| `flowfirst_http_responses_total{class}` | REST API responses by status class (`2xx`…`5xx`), API Gateway (Process 1) only. |
| `flowfirst_events_total{event}` | Other counters (`dedup_reaped`). |
| `flowfirst_up`, `flowfirst_start_time_seconds`, `flowfirst_stopping` | The process answers; when it started (a changing value means a restart); asked to stop. |
| `flowfirst_greenthreads` | Greenthreads running. **0 on a standby instance**: a process that lost (or has not won) the ZooKeeper election serves `/metrics` but does no work, so `up` alone cannot tell active from standby. |
| `flowfirst_publish_connections_open` / `_configured` | API Gateway (Process 1) publisher connections (see API_PUBLISH_CONNECTIONS). |
| `flowfirst_tls_cert_expiry_seconds{cert}` | Seconds until the `api`, `pipeline` and `ca` certificates this process loads expire (negative = expired). Present only when `TLS_API_ENABLED=true`. |

Every series carries `node` and `process` labels. The output holds counters and expiry times, never message content, hostnames of other systems, or credentials. Counters a process can emit are created at 0 when it starts, so the first failure after a restart is visible to `increase()`.

**Turn it on** (every node, in `.env`; then restart the processes):

```bash
METRICS_ENABLED=true
METRICS_HOST=<this node's own IP>      # not 0.0.0.0 unless the firewall limits who can connect
METRICS_PORT_BASE=9100
sudo firewall-cmd --permanent --add-rich-rule='rule family="ipv4" source address="<prometheus-ip>" port port="9101-9105" protocol="tcp" accept'
sudo firewall-cmd --reload
sudo python3 scripts/preflight_check.py        # the "metrics" line: PASS with mutual TLS, WARN if open to the network
```

With `TLS_API_ENABLED=true` the listener reuses the API server certificate and `TLS_API_CLIENT_AUTH`, so Prometheus needs a client certificate signed by the FlowFirst CA (`security/issue_cert.sh`, see Scenario 63) and `server_name` must match a name in that node's `-api` certificate. Without TLS, anyone who can reach the port can read the counters: firewall it. The systemd sandbox allows this listener (it only needs TCP).

**Prometheus.** `monitoring/prometheus.yml` is a sample scrape configuration (replace the `<…>` placeholders): the five FlowFirst targets per node, plus the infrastructure exporters the alerts rely on. Those are **not part of FlowFirst**: RabbitMQ's built-in plugin (`rabbitmq-plugins enable rabbitmq_prometheus`, port 15692; per-queue series come from `/metrics/detailed?family=queue_coarse_metrics&family=queue_consumer_count`), `mysqld_exporter` on the Galera nodes (run it on 9114, because its default 9104 is DB Updater (Process 4)'s metrics port), and `node_exporter`. ZooKeeper is not scraped: the repo's `zoo.cfg` does not enable a Prometheus metrics provider.

**Alerts.** `monitoring/flowfirst.rules.yml` (FlowFirst's own series and the RabbitMQ queue series) and `monitoring/infrastructure.rules.yml` (Galera, RabbitMQ alarms, disk, clock). Thresholds are starting points; tune them to your traffic.

**Alertmanager.** `monitoring/alertmanager.yml` routes all alerts to the file receiver: a small Python webhook (`monitoring/alertmanager-file-receiver.py`) that appends each call as a JSON line to `/var/log/flowfirst/alerts.log`. Run it as a systemd service alongside Alertmanager. Install both with `sudo ./monitoring/setup_alertmanager.sh` (downloads the latest release from GitHub, creates the `alertmanager` user, installs two systemd units). Wire it into Prometheus by uncommenting the `alerting:` block in `monitoring/prometheus.yml` and replacing `<alertmanager-ip>`.

```bash
# Install on one cluster node (or a dedicated monitoring host):
sudo ./monitoring/setup_alertmanager.sh

# Open port 9093 if Prometheus is on a different host:
sudo firewall-cmd --permanent --add-port=9093/tcp && sudo firewall-cmd --reload

# Watch alerts as they arrive:
sudo journalctl -u flowfirst-alert-receiver -f
sudo tail -f /var/log/flowfirst/alerts.log | python3 -c 'import sys,json; [print(json.dumps(json.loads(l),indent=2)) for l in sys.stdin]'
```

To replace the file receiver with a real notification channel (email, Slack, PagerDuty) at any point: add a receiver block and a route to `alertmanager.yml`, reload Alertmanager with `systemctl reload alertmanager`, and remove or keep the `file-log` route. The rules and the Prometheus scrape do not change.

| Alert | Fires when | First action |
|---|---|---|
| `FlowFirstDeadLetters` (critical) | any message in `flowfirst_dead_letters` | Read the logs, fix the cause, `scripts/replay_dead_letters.py` (Run-Book R3) |
| `FlowFirstMessagesFailing` (warning) | a handler failed any message in 5 min | Process log; without the DLX policy these messages are **discarded** |
| `FlowFirstProcessDown` (critical) | a scrape target is down 2 min | `systemctl status`, the journal, firewall, client certificate |
| `FlowFirstNoActiveInstance` (critical) | every instance of a process is on standby | ZooKeeper quorum, Pacemaker |
| `FlowFirstPipelineStalled` (critical) | P1 published and P4 persisted nothing for 10 min on a flow | Queue alerts, MariaDB, the DB Updater (Process 4) leader |
| `FlowFirstApi5xxRate` (warning) | over 5% of API responses are 5xx | API Gateway (Process 1) log by the `request_id` in the response |
| `FlowFirstQueueBacklog`, `FlowFirstQueueNoConsumers` | over 10 000 ready / no consumer on a `flow*` queue | Scenario 61 tuning table |
| `FlowFirstCertificateExpiresSoon` / `…VerySoon` | under 30 / 7 days | Scenario 63 renewal |
| `GaleraClusterSizeLow`, `GaleraNodeNotSynced`, `RabbitMq…`, `NodeDiskFillingUp`, `NodeClockSkew` | see the file | the description in the alert |

Check the rules without a cluster: `promtool check rules monitoring/*.rules.yml` and, from `monitoring/tests/`, `promtool test rules flowfirst.test.yml`.

**Limits (read these).**
1. A counter restarts at 0 when its process restarts (Prometheus' `increase()` and `rate()` handle that). The counters are per process instance, not per cluster: `sum by (flow, event)` across nodes gives the cluster view.
2. `FlowFirstPipelineStalled` assumes API Gateway (Process 1) **and** DB Updater (Process 4) are both scraped; with only one of them it will fire falsely. It cannot tell a stuck pipeline from messages that are legitimately still in flight for under 10 minutes.
3. Failures are counted when the handler raises. A message lost *before* a handler runs (for example, an unroutable publish) shows as a 5xx, not as `failed`.
4. `FlowFirstProcessDown` also fires for a process that is up but whose metrics port is blocked or whose client certificate was rejected: the cause is in the Prometheus target page (`lastError`).
5. Dedup-skipped messages are counted, but there is no alert on them: failover re-deliveries make a non-zero value normal.
6. There is no ZooKeeper, HAProxy or Swift alert, and no dashboard. The file receiver is a starting point: it records every alert but sends no page. Replace or supplement it with a real notification channel in `alertmanager.yml`.
7. The listener has no request limits beyond a small connection pool and a 10 s socket timeout; do not expose it beyond the monitoring network.

---

## Notifications

> **Status:** the module and all its wiring were implemented in `src/notifications/notifications.py`. Off by default (`NOTIFICATIONS_ENABLED=false`); enabling it requires a `.env` change and a DB Updater (Process 4) restart — no pipeline queue changes, no schema changes.

### Overview

When `NOTIFICATIONS_ENABLED=true`, DB Updater (Process 4) publishes a lightweight JSON **notification envelope** to a RabbitMQ fanout exchange (`flowfirst.notifications` by default) immediately after each pipeline message is successfully persisted **and** acked. External consumers bind their own durable queues to that exchange; FlowFirst only owns the exchange itself.

**Key design properties:**
- Notifications are emitted **after** `ch.basic_ack()` — the pipeline message is never put at risk for a notification.
- `mandatory=False` on every notification publish — if no subscriber is bound, the envelope is silently discarded; it never bounces back or raises an exception in the pipeline.
- All exceptions inside `notify()` are caught and logged as `WARNING`; the handler is never affected.
- `history[]` is always stripped from the payload field — it is already in MariaDB and Swift.
- `NOTIFICATIONS_FLOWS` lets you restrict emission to a subset of flows with no code change.

### Configuration

Add to `.env` (all nodes, identical values):

| Variable | Default | Effect |
|---|---|---|
| `NOTIFICATIONS_ENABLED` | `false` | Master switch. Set `true` to activate. |
| `NOTIFICATIONS_EXCHANGE` | `flowfirst.notifications` | RabbitMQ fanout exchange name declared by FlowFirst. |
| `NOTIFICATIONS_FLOWS` | *(blank)* | Comma-separated flow numbers to emit for (e.g. `4,6,7`). Blank or `*` = all flows 1–7. |
| `NOTIFICATIONS_MAX_PAYLOAD_BYTES` | `8192` | The `payload` field is omitted (replaced by `payload_bytes`) when the serialised payload exceeds this. `0` = always include. |

```bash
# Enable in .env (every node), then restart DB Updater (Process 4):
NOTIFICATIONS_ENABLED=true
# Restart under Pacemaker:
sudo pcs resource restart flowfirst-process4
```

### Notification Envelope Schema

Every envelope is a compact JSON object published to the exchange with `content_type: application/json` and `delivery_mode: Persistent`:

```json
{
  "notification_id": "3f7a9c12-...",
  "event_type":      "device.created",
  "flow":            4,
  "entity_id":       "a8b2c3d4-...",
  "entity_type":     "device",
  "occurred_at":     "2025-07-14 11:30:00",
  "node":            "node1",
  "message_id":      "c1d2e3f4-...",
  "payload": {
    "device_name": "sensor-42",
    "device_type": "temperature_sensor",
    "location":    "facility/zone-1"
  }
}
```

When the serialised payload exceeds `NOTIFICATIONS_MAX_PAYLOAD_BYTES`, the `payload` key is replaced with:
```json
{ "payload_bytes": 14392 }
```

### Event Type Reference

| Flow | Operation | `event_type` | `entity_type` |
|------|-----------|-------------|---------------|
| 1 | Telemetry processed | `telemetry.processed` | `message` |
| 2 | Metric processed | `metric.processed` | `message` |
| 3 | Remote telemetry processed | `telemetry.remote_processed` | `message` |
| 4 | IoT device created | `device.created` | `device` |
| 5 | IoT devices queried | `device.queried` | `device` |
| 6 (direct path) | IoT device updated | `device.updated` | `device` |
| 6 (remote edge-sync) | IoT device edge-synced | `device.edge_synced` | `device` |
| 7 | IoT device decommissioned | `device.decommissioned` | `device` |

### Subscriber Setup

Subscribers are fully independent of FlowFirst. Each subscriber creates its own durable queue and binds it to the exchange **once** before consuming. FlowFirst never declares, consumes, or purges subscriber queues.

**Python example (pika):**
```python
import pika

conn = pika.BlockingConnection(pika.ConnectionParameters("localhost"))
ch   = conn.channel()

# Declare your own queue (durable so it survives a broker restart)
ch.queue_declare(queue="my-svc.notifications", durable=True)

# Bind it to the FlowFirst notifications exchange (fanout — no routing key needed)
ch.queue_bind(queue="my-svc.notifications", exchange="flowfirst.notifications")

def on_notification(ch, method, properties, body):
    import json
    env = json.loads(body)
    print(env["event_type"], env["entity_id"])
    ch.basic_ack(delivery_tag=method.delivery_tag)

ch.basic_qos(prefetch_count=1)
ch.basic_consume(queue="my-svc.notifications", on_message_callback=on_notification)
ch.start_consuming()
```

**rabbitmqctl one-liner (create queue and bind, run once on any broker node):**
```bash
rabbitmqctl eval '
  {ok, C}  = amqp_connection:start(#amqp_params_network{}),
  {ok, Ch} = amqp_channel:open(C),
  amqp_channel:call(Ch, #'"'"'queue.declare'"'"'{queue = <<"my-svc.notifications">>, durable = true}),
  amqp_channel:call(Ch, #'"'"'queue.bind'"'"'{queue    = <<"my-svc.notifications">>,
                                              exchange = <<"flowfirst.notifications">>}).'
```

### Many Notifications per Flow

A single pipeline message can produce more than one notification. Call `notify()` multiple times in the same handler (each call is one `basic_publish` on the already-open channel — no new connections):

```python
# Example: emit a "start" and "complete" notification within one flow (custom usage)
_notif.notify(ch, "device.update.started",   data, flow=6)
# ... additional work ...
_notif.notify(ch, "device.update.completed", data, flow=6)
```

Because `NOTIFICATIONS_FLOWS` filters by flow number, both calls are subject to the same filter.

### Ordering Guarantee and Delivery Semantics

Notifications use **at-most-once** delivery:

- They are emitted after `basic_ack` — a DB Updater (Process 4) crash between the ack and the notify means the pipeline message is safe (already committed) but the notification for it is not sent.
- `mandatory=False` — a notification with no bound subscriber queue is silently dropped by RabbitMQ; it does not cause a publisher error.
- There is no built-in retry, acknowledgement tracking, or subscriber authentication. For guaranteed delivery, reliable ordering, or per-subscriber access control, run a relay service that consumes from the fanout queue and re-publishes with the required semantics.

### Metrics

With `METRICS_ENABLED=true`, DB Updater (Process 4) exposes one counter per flow:

| Series | Meaning |
|---|---|
| `flowfirst_messages_total{flow="N",event="notified"}` | Notification envelopes successfully published to the exchange (flows 1–7). |
| `flowfirst_messages_total{flow="6",event="remote_notified"}` | Envelopes published for the Flow 6 remote edge-sync path specifically. |

A counter at zero with `NOTIFICATIONS_ENABLED=true` and traffic flowing means `notify()` is raising internally and logging warnings — check the DB Updater (Process 4) log.

### What This Facility Does Not Do

- No delivery guarantee beyond RabbitMQ's own durable queue behaviour for bound subscribers.
- No subscriber authentication — anyone with AMQP access to the broker can bind a queue.
- No filtering beyond `NOTIFICATIONS_FLOWS` (by flow number). Event-type filtering is the subscriber's responsibility.
- No dashboard, no dedicated alert rule. Use `flowfirst_messages_total{event="notified"}` in your own Prometheus query.
- No ZooKeeper dedup — unlike pipeline messages, a failover re-delivery of a pipeline message will not produce a duplicate notification because the dedup barrier fires before the handler reaches `basic_ack`.

---

## Production Rollout Run-Book

> **Status: nothing in this run-book has been run on a real four-node cluster.** Every step below was written from the scripts and the tests that exist in the repository (containers on one workstation, unit tests, and the real API Gateway (Process 1) handler with the broker, ZooKeeper and Swift stubbed). Treat the first run as a rehearsal on a **test cluster**, record what differs, and correct this document. Commands that are not in a repository script (for example `ALTER USER`, `rabbitmqctl change_password`) are the standard tools' syntax and were not run here.

Use it in two situations: **(1) a new cluster**, where you follow Phases 0 to 6 above and use this chapter as the order of the additions; **(2) an existing cluster** that was installed before these changes (sample passwords, no dead-letter policy, no request limits), where you apply them one at a time with the checks below. Do one node at a time for anything that restarts a process, and never on all three nodes at once.

### R0. Before you touch anything

| Step | Command | You expect |
|---|---|---|
| Record the starting state | `sudo pcs status`, `./scripts/test_flows_and_inspect_db.sh` | all resources `Started`; flows 1 and 2 reach the database |
| Backup the `.env` | `sudo cp -p /opt/flowfirst/.env /root/flowfirst.env.$(date +%F)` (mode 600, keep it off the cluster too) | file exists |
| Run the node check | `sudo python3 scripts/preflight_check.py` on **every** node | list of PASS / WARN / FAIL: the FAILs are your work list |

`scripts/preflight_check.py` is read-only. It checks `.env` permissions and literals, blank host lists, secret strength (never printing a value), `ALLOW_DEFAULT_SECRETS`, the request limits, `API_EXPOSE_ERROR_DETAILS`, the admin settings, which TLS switches are off, the certificate files, and (on nodes 1-3 as root) whether the dead-letter policy exists and the dead-letter queue is empty. It does **not** contact MariaDB, ZooKeeper or Swift and does not prove the cluster works.

### R1. Deploy the new code (one node at a time)

```bash
cd /opt/flowfirst && git pull
source .venv/bin/activate
pip install --require-hashes -r requirements.txt      # the locked versions; nothing newer is picked up
./scripts/ci_check.sh --quick                         # optional, needs requirements-dev.txt
sudo pcs node standby node1                           # drain node1 (VIP and clones move away)
sudo systemctl restart flowfirst-process1 flowfirst-process2 flowfirst-process3 flowfirst-process4
sudo pcs node unstandby node1
curl -s http://<VIP>:8080/health | jq .status         # "healthy"
```

Then node2, then node3. **Expected failure on an unchanged `.env`:** the processes refuse to start with `Refusing to start with weak secrets` if the old sample passwords are still in `.env`. That is intended; do R2 first, or set `ALLOW_DEFAULT_SECRETS=true` for the length of the rollout only, and remove it afterwards.

### R2. Replace the sample passwords (existing clusters)

`generate_secrets.sh` writes new values into `.env`, but **the services still have the old passwords**. Changing the file alone locks the pipeline out, so rotate in each service and then restart. Do it in a maintenance window; messages published during the switch can fail.

| Secret | Where the old value lives | Change it there (run once, on one node) | Then |
|---|---|---|---|
| `RABBITMQ_PASSWORD` | RabbitMQ user `RABBITMQ_USER` | `sudo rabbitmqctl change_password <user> '<new>'` (the install script does not change an existing user's password) | restart P1 to P5 |
| `MARIADB_PASSWORD` | MariaDB user `MARIADB_USER` | `ALTER USER '<user>'@'%' IDENTIFIED BY '<new>'; ALTER USER '<user>'@'localhost' IDENTIFIED BY '<new>';` (replicates through Galera) | restart P1 to P5 |
| `SWIFT_KEY` | `/etc/swift/proxy-server.conf` (`user_<account>_<user> = <key> .admin`) on nodes 1-3 | re-run `sudo ./swift/setup_swift.sh` after updating `.env` (it rewrites that file; it keeps the existing rings) | restart `swift-proxy`, then P4 |
| `HAPROXY_STATS_PASS` | `/etc/haproxy/haproxy.cfg` | `sudo ./haproxy/setup_haproxy.sh`, then `sudo haproxy -c -f /etc/haproxy/haproxy.cfg`, then `sudo pcs resource restart haproxy-res` (never start `haproxy.service` yourself) | open `:9000` with the new login |
| `SWIFT_HASH_PATH_PREFIX` / `_SUFFIX` | `/etc/swift/swift.conf` and the object placement | **Do not change these on a cluster that already stores objects.** Keep the existing values in `.env`. | none |

Procedure: `./scripts/generate_secrets.sh` (prints, writes nothing), copy the four password values you intend to rotate, edit `.env` on **one** node, change the service side as above, copy `.env` to the other nodes (only `NODE_NAME` and `CURRENT_NODE_IP` differ), restart one node at a time, `sudo python3 scripts/preflight_check.py`, and finally delete `.env.bak` and the shell history lines that contained a password. The old passwords were public sample values, so treat any cluster that ever ran with them as exposed and review who could reach RabbitMQ, MariaDB and Swift.

### R3. Dead-letter policy and publisher confirms

Background, tested behaviour and limits: [Message Safety](#message-safety-publisher-confirms-and-the-dead-letter-queue).

```bash
sudo ./scripts/setup_rabbitmq_dlx.sh --print      # show the command first
sudo ./scripts/setup_rabbitmq_dlx.sh              # once, on one RabbitMQ node
sudo rabbitmqctl list_policies                    # flowfirst-dlx   ^flow[123]_
python3 scripts/replay_dead_letters.py            # list what is waiting (read-only)
```

Confirms and `mandatory=True` need no switch: they are in the code from R1. To prove the policy works, publish a message that a consumer will reject (for example stop the database so DB Updater (Process 4) nacks), then check that `flowfirst_dead_letters` grew by one and that `python3 scripts/replay_dead_letters.py --replay --limit 1` puts it back once the database is up again. Without the policy a rejected message is **discarded**: `preflight_check.py` reports that as FAIL. Operations: look at the queue depth during every check (`rabbitmqctl list_queues name messages | grep dead`); find out **why** before replaying (the process log has the exception), because replaying a message that fails for a permanent reason only puts it back.

### R4. Admin endpoint (`POST /zk/config`)

The endpoint is refused for everyone until TLS is on and the caller's certificate CN is allowed:

```bash
# .env on nodes 1-3 (after R6 has turned TLS_API_ENABLED on)
ADMIN_ALLOWED_CNS=admin-client
ADMIN_TRUSTED_PROXY_CNS=node1-pipeline,node2-pipeline,node3-pipeline
ALLOW_UNAUTHENTICATED_ADMIN=false
```

Check both directions: with the `admin-client` certificate `POST /zk/config` returns `200`; with any other valid certificate it returns `403 {"error":"Forbidden"}` and the process log shows `[authz] DENIED`. Before TLS is on, `GET /zk/config` still works; only the write is closed.

### R5. Request limits and publish connections

| Setting | Default | Check |
|---|---|---|
| `API_MAX_BODY_BYTES`, `API_MAX_BATCH_COUNT`, `API_MAX_LIST_LIMIT`, `API_REQUEST_TIMEOUT_S` | 1 MiB, 100, 1000, 30 s | `curl -s -o /dev/null -w '%{http_code}' -X POST http://<VIP>:8080/api/batch -d '{"count":100000}'` returns `400`; a 2 MiB body returns `413` |
| `API_EXPOSE_ERROR_DETAILS` | `false` | an error body has `error` and `request_id` only; `grep <request_id> /var/log/flowfirst/api_gateway.log` finds the cause |
| `API_PUBLISH_CONNECTIONS` | 4 | `sudo rabbitmqctl list_connections | grep -c flowuser` rises by 4 per running API Gateway (Process 1) |

Raise `API_PUBLISH_CONNECTIONS` only after a load run shows publish latency is the bottleneck (Scenario 61 section C), and watch RabbitMQ memory and connection count. HAProxy has its own `timeout http-request 10s` and no body-size limit; if a limit at the edge matters to you, that is not done.

### R6. TLS rollout

Follow Scenario 63 sections C to F exactly (CA and certificates on an admin host, `install_certs.sh`, one component at a time, `security/verify_tls.sh` after each). The order that keeps the cluster up: MariaDB, ZooKeeper, RabbitMQ, Swift, then the API. Turn each `TLS_*_ENABLED` on in `.env` on all nodes **before** restarting any node on that component, because a process with TLS on cannot talk to a server still on plaintext and the reverse. If a step fails, the rollback is the previous `.env` and the previous server setting; `enable_*_tls.sh` keeps backups.

If you set `TLS_CRL_FILE`, also schedule `security/refresh_crl.sh` + `install_certs.sh --crl-only` (an expired CRL makes every component that loads it refuse **all** certificates: Scenario 63 section I). `preflight_check.py` reports the CRL's remaining life on each node.

### R7. Sandbox, health probe and monitoring

1. **Sandbox.** `sudo ./systemd/install_services.sh /opt/flowfirst` installs the hardening drop-in with the units (Phase 4, Step 4c); restart the processes and check `systemd-analyze security flowfirst-process1`. `preflight_check.py` warns when a unit lacks the drop-in.
2. **Health probe.** HAProxy keeps probing `GET /health`, which no longer touches ZooKeeper (chapter *Health, Readiness and Dependencies*). Anything of yours that read `zk_config` or `zk_registered_workers` from `/health` must move to `/zk/config` and `/zk/health`. Do not point HAProxy at `/ready`.
3. **Monitoring.** Set `METRICS_ENABLED=true`, `METRICS_HOST=<node ip>` and open ports 9101 to 9105 to Prometheus only; use `monitoring/prometheus.yml` and `monitoring/*.rules.yml`, then enable `rabbitmq_prometheus`, `mysqld_exporter` (on 9114) and `node_exporter` (chapter *Monitoring and Alerting*). Send a test message to `flowfirst_dead_letters` and confirm `FlowFirstDeadLetters` fires.
4. **Backups.** Schedule `backup_flowfirst.sh` and rehearse `restore_flowfirst.sh --verify` (chapter *Backup and Restore*).

### R8. Measure it (Scenarios 61 and 62)

Run these from a separate client host, **once before** and **once after** R2 to R6, so each change has a number:

| What | Scenario | Compare |
|---|---|---|
| Throughput and error rate at 100, 500, 1000 concurrent callers | 61 B | requests per second, error %, p95 |
| Does `API_PUBLISH_CONNECTIONS` help on this hardware | 61 C | rate and p95 at 1, 4, 8 |
| Freshness (API call to stored record) | 62 D | seconds |
| Behaviour with a node down | 62 G | errors during, time to recover |
| Cost of TLS | 62 B/E | CPU and p95 with and without |

The only benchmark in this repository (one RabbitMQ container, 2 vCPU, ReadMe K000) is not a cluster result; do not quote it as one.

### R9. Sign-off checklist

| Done | Check | Evidence |
|---|---|---|
| | `preflight_check.py` has no FAIL on all 4 nodes | saved output |
| | `scripts/test_flows_and_inspect_db.sh` passes through the VIP | saved output |
| | `security/verify_tls.sh` all PASS (if TLS is on) | saved output |
| | dead-letter test (R3) done; queue empty | queue depth |
| | admin allowed for `admin-client`, refused for another CN (R4) | two curl outputs |
| | limits answer 400/413 (R5) | curl codes |
| | node failure test (Scenario 62 G) with the new code | recovery time |
| | load result recorded with conditions (Scenario 62 H) | report |
| | `.env` backups exist off-cluster; no `.env.bak` left | `ls -la` |
| | `backup_flowfirst.sh` ran on every node and `restore_flowfirst.sh --verify` passed; a restore into a second database matched `CHECKSUM TABLE` | saved output |
| | `systemd-analyze security flowfirst-process1` shows the sandbox; all five units active | saved output |
| | `curl http://<node>:8082/health` answers while ZooKeeper is briefly stopped on a test node; `/ready` shows `zookeeper: disconnected` | two curl outputs |
| | Prometheus targets `up` on every node; the test dead-letter alert fired and was routed to a person | target page, alert |
| | CRL refresh scheduled; `list_certs.sh` shows the CRL `nextUpdate` more than 7 days away (if `TLS_CRL_FILE` is set) | saved output |

### R10. What this run-book does not cover

Still open: no point-in-time database recovery; no dashboard; no ZooKeeper, HAProxy or Swift alerts; the file-receiver Alertmanager setup records alerts to a log file but sends no page (add a real receiver in `alertmanager.yml`); live Alertmanager routing has not been tested; Swift objects are now backed up but the swift backup part has not been tested against a real Swift cluster; Swift cannot require client certificates at its proxy; RabbitMQ and ZooKeeper do not honour the CRL; `MARIADB_SST_METHOD=mariabackup` with `MARIADB_SST_ENCRYPT=true` is implemented but not tested on a real cluster; `HAPROXY_MAX_BODY_BYTES` is in the template but not tested against a live HAProxy; no per-client rate limiting; the GitHub Actions workflow has never been run; and **nothing here has been run on a real four-node cluster, on RHEL 9, with SELinux enforcing, or under Pacemaker**. The chapters above state what was tested and how.
