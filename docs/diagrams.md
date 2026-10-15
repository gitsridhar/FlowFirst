# FlowFirst — Architecture Diagrams

All Mermaid diagrams for the FlowFirst solution in one place.
Links back to the relevant section of [ReadMe.md](../ReadMe.md) are provided under each diagram title.

---

## 1. Multi-Node Cluster Architecture & Network Topology

> Source: [ReadMe.md § Multi-Node Cluster Architecture](../ReadMe.md#multi-node-cluster-architecture--network-topology)

Shows the 4-node cluster layout: Nodes 1–3 running all pipeline processes, Galera, RabbitMQ, ZooKeeper and HAProxy; Remote Node 4 running the Remote Worker only; the Pacemaker Virtual IP as the single entry point; and HAProxy round-robin to API Gateway instances.

```mermaid
graph TD
    Client["Client / curl"] -->|"VIP:8080"| VIP["Pacemaker VIP"]
    VIP --> HAProxy["HAProxy\nREST :8080 round-robin\nSQL :3307 load-balanced\nRMQ :5673 load-balanced\nSwift :8090 load-balanced"]

    subgraph Node1[Node 1]
        P1a["API Gateway"]
        P2a["Transformer"]
        P3a["Forwarder"]
        P4a["DB Updater"]
        RMQ1[("RabbitMQ")]
        GAL1[("Galera Node 1")]
    end

    subgraph Node2[Node 2]
        P1b["API Gateway"]
        P2b["Transformer"]
        P3b["Forwarder"]
        P4b["DB Updater"]
        RMQ2[("RabbitMQ")]
        GAL2[("Galera Node 2")]
    end

    subgraph Node3[Node 3]
        P1c["API Gateway"]
        P2c["Transformer"]
        P3c["Forwarder"]
        P4c["DB Updater"]
        RMQ3[("RabbitMQ")]
        GAL3[("Galera Node 3")]
    end

    subgraph Node4[Remote Node 4]
        P5["Remote Worker"]
    end

    HAProxy -->|"Round-Robin"| P1a
    HAProxy -->|"Round-Robin"| P1b
    HAProxy -->|"Round-Robin"| P1c

    P1a -.->|"RMQ: flow3_p1_to_p5"| P5
    P1b -.->|"RMQ: flow3_p1_to_p5"| P5
    P1c -.->|"RMQ: flow3_p1_to_p5"| P5

    P5 -.->|"RMQ: flow3_p5_to_p2"| P2a
    P5 -.->|"RMQ: flow3_p5_to_p2"| P2b
    P5 -.->|"RMQ: flow3_p5_to_p2"| P2c

    P2a -.->|"RMQ: flow3_p2_to_p4"| P4a
    P2b -.->|"RMQ: flow3_p2_to_p4"| P4b
    P2c -.->|"RMQ: flow3_p2_to_p4"| P4c

    P4a -.->|"SQL"| HAProxy
    P4b -.->|"SQL"| HAProxy
    P4c -.->|"SQL"| HAProxy

    HAProxy -->|"SQL"| GAL1
    HAProxy -->|"SQL"| GAL2
    HAProxy -->|"SQL"| GAL3

    GAL1 <-->|"wsrep"| GAL2
    GAL2 <-->|"wsrep"| GAL3
    GAL1 <-->|"wsrep"| GAL3
```

---

## 2. End-to-End Sequence & Galera Replication Flow

> Source: [ReadMe.md § End-to-End Sequence](../ReadMe.md#end-to-end-sequence--galera-replication-flow)

Shows a single Flow 1 request travelling from the client through HAProxy → API Gateway → RabbitMQ → Transformer → Forwarder → DB Updater → Galera wsrep replication.

```mermaid
sequenceDiagram
    autonumber
    actor Client as curl Client
    participant VIP as Pacemaker VIP
    participant HAP as HAProxy
    participant P1 as API Gateway (Process 1)
    participant RMQ as RabbitMQ
    participant P2 as Transformer (Process 2)
    participant P3 as Forwarder (Process 3)
    participant P4 as DB Updater (Process 4)
    participant DB as Galera Cluster

    rect rgb(240, 245, 255)
        Note over Client,P1: Phase 1 - REST API via Virtual IP
        Client->>VIP: POST /api/flow1
        VIP->>HAP: forward to port 8080
        HAP->>P1: round-robin to one node
        P1->>RMQ: publish to flow1_p1_to_p2
        P1-->>Client: HTTP 200 OK
    end

    rect rgb(240, 255, 245)
        Note over RMQ,P4: Phase 2 - Async Pipeline Transformation
        RMQ->>P2: consume message
        Note over P2: counter += step, append audit entry
        P2->>RMQ: reflect to flow1_p2_to_p3
        RMQ->>P3: consume reflected message
        Note over P3: attach forward audit stage
        P3->>RMQ: forward to flow1_p3_to_p4
        RMQ->>P4: consume final message
    end

    rect rgb(255, 245, 240)
        Note over P4,DB: Phase 3 - Persist and Galera Replication
        P4->>HAP: SQL INSERT via port 3306
        HAP->>DB: write to active Galera node
        DB-->>DB: wsrep certification across all 3 nodes
        DB-->>P4: commit success
    end
```

---

## 3. Database Schema & ER Diagram

> Source: [ReadMe.md § Database Schema](../ReadMe.md#database-schema--er-diagram)

Entity-relationship diagram for the two MariaDB tables: `processed_messages` (Flows 1–3) and `devices` (Flows 4–7 IoT CRUD).

```mermaid
erDiagram
    PROCESSED_MESSAGES {
        bigint id PK "Auto Increment"
        varchar(64) message_id UK "Unique UUID"
        int flow_id "Flow identifier (1-3)"
        int item_id "Sequence number"
        varchar(255) initial_data "Origin payload"
        int counter_value "Flow 1 reflected counter"
        decimal(10_2) metric_value "Flow 2 examined & scaled metric"
        varchar(50) examined_status "HIGH / NORMAL"
        varchar(50) verified_by "process3 verification mark"
        json history_trail "Full audit log with timestamps"
        json raw_payload "Complete final payload object"
        timestamp created_at "Record creation time"
    }

    DEVICES {
        bigint id PK "Auto Increment"
        varchar(36) device_id UK "UUID assigned at create"
        varchar(100) device_name "Human-readable label"
        varchar(50) device_type "temperature_sensor | pressure_gauge | flow_meter | humidity_sensor | vibration_sensor | power_monitor"
        varchar(200) location "e.g. factory/zone-A"
        varchar(20) firmware_version "Default 1.0.0"
        json tags "Arbitrary key-value metadata"
        varchar(20) status "active | decommissioned"
        varchar(100) owner_team "Owning team"
        timestamp created_at "Row creation time"
        timestamp updated_at "Last upsert time (auto ON UPDATE)"
    }
```

---

## 4. SSL Certificate Creation, Distribution and Runtime Usage

> Source: [ReadMe.md § Certificate lifecycle and runtime usage](../ReadMe.md#certificate-lifecycle-and-runtime-usage)

Four-stage view of the full TLS certificate lifecycle:
- **① Create** — `security/init_ca.sh` + `issue_all.sh` produce 21 certificates (server, peer and client profiles) on the admin/CA host
- **② Distribute** — `security/install_certs.sh` pushes only the needed files to each node via SSH+sudo
- **③ Runtime** — which certificate each component presents and verifies in a live cluster
- **④ Maintain** — CRL refresh, revocation, expiry checks and per-node preflight

```mermaid
flowchart TD
    %% ── Stage 1: Creation ───────────────────────────────────────────
    subgraph CREATE ["① Create — Admin / CA host (pki/ stays here, never on nodes)"]
        direction TB
        CA_INIT["<b>security/init_ca.sh</b>
RSA-4096 root CA
pki/ca/ca.key  (AES-256 encrypted)
pki/ca/ca.crt  (public)
pki/ca/ca.crl  (empty CRL)"]

        ISSUE_VIP["<b>flowfirst-vip</b>  [server]
Subject Alt Names: VIP IP, flowfirst.local
Used by: HAProxy HTTPS/Swift/MariaDB-LB/RabbitMQ-LB frontend"]
        ISSUE_API["<b>nodeN-api</b>  [server]  ×3
Subject Alt Names: node IP, hostname
Used by: API Gateway (Process 1) HTTPS listener on each node"]
        ISSUE_RMQ["<b>nodeN-rabbitmq</b>  [peer]  ×3
SANs: node IP + VIP IP/DNS
Used by: RabbitMQ broker TLS + inter-node clustering"]
        ISSUE_MDB["<b>nodeN-mariadb</b>  [peer]  ×3
SANs: node IP + VIP IP/DNS
Used by: MariaDB/Galera client TLS + replication"]
        ISSUE_ZK["<b>nodeN-zookeeper</b>  [peer]  ×3
SANs: node IP, hostname
Used by: ZooKeeper TLS clients + quorum"]
        ISSUE_SW["<b>nodeN-swift</b>  [server]  ×3
SANs: node IP, hostname
Used by: Swift proxy server"]
        ISSUE_PIPE["<b>nodeN-pipeline</b>  [client]  ×3
Used by: Processes 1–4 client identity on each node
Also used by: HAProxy towards API Gateway (Process 1) backends"]
        ISSUE_N4["<b>node4-pipeline</b>  [client]
Used by: Remote Worker (Process 5) client identity (remote node4)"]
        ISSUE_ADM["<b>admin-client</b>  [client]
Used by: operators / curl for mTLS REST API access"]

        CA_INIT -->|"security/issue_all.sh"| ISSUE_VIP
        CA_INIT -->|"security/issue_all.sh"| ISSUE_API
        CA_INIT -->|"security/issue_all.sh"| ISSUE_RMQ
        CA_INIT -->|"security/issue_all.sh"| ISSUE_MDB
        CA_INIT -->|"security/issue_all.sh"| ISSUE_ZK
        CA_INIT -->|"security/issue_all.sh"| ISSUE_SW
        CA_INIT -->|"security/issue_all.sh"| ISSUE_PIPE
        CA_INIT -->|"security/issue_all.sh"| ISSUE_N4
        CA_INIT -->|"security/issue_all.sh"| ISSUE_ADM
    end

    %% ── Stage 2: Distribution ────────────────────────────────────────
    subgraph DIST ["② Distribute — security/install_certs.sh (SSH+sudo → /etc/flowfirst/tls/)"]
        direction LR
        N1["<b>Node 1</b>
ca.crt  ca.crl
api/api.{crt,key}
pipeline/pipeline.{crt,key}
haproxy/vip.pem  backend.pem
rabbitmq/rabbitmq.{crt,key}
mariadb/mariadb.{crt,key}
zookeeper/zookeeper.{crt,key}
swift/swift.{crt,key}"]
        N2["<b>Node 2</b>
(same set, node2 leaf certs)"]
        N3["<b>Node 3</b>
(same set, node3 leaf certs)"]
        N4["<b>Node 4</b>
ca.crt  ca.crl
pipeline/pipeline.{crt,key}"]
    end

    CREATE -->|"SSH + sudo copy"| DIST

    %% ── Stage 3: Runtime usage ───────────────────────────────────────
    subgraph RUNTIME ["③ Runtime — TLS connections in a live cluster"]
        direction TB
        subgraph EXT ["External access"]
            CURL(["curl / API client
admin-client.crt  →  HAProxy :8080
← verifies flowfirst-vip"])
        end
        subgraph API_TIER ["API tier  (nodes 1–3)"]
            HA(["HAProxy :8080
presents vip.pem  (server)
presents backend.pem  (client → P1)
verifies ca.crt + ca.crl"])
            P1(["API Gateway (Process 1) :8082
presents nodeN-api.crt  (server)
verifies pipeline client cert
src/tls/tls.py → server_context()"])
        end
        subgraph PIPELINE ["Pipeline processes  (nodes 1–3)"]
            P24(["Processes 2–4
presents nodeN-pipeline.crt (client)
tls.py → client_context()"])
            P5(["Remote Worker (Process 5)  (node4)
presents node4-pipeline.crt (client)
tls.py → client_context()"])
        end
        subgraph INFRA ["Infrastructure  (nodes 1–3)"]
            RMQ(["RabbitMQ :5671
nodeN-rabbitmq.crt (server)
peer certs for inter-node"])
            MDB(["MariaDB :3306
nodeN-mariadb.crt (server/peer)
Galera wsrep replication"])
            ZK(["ZooKeeper :2281
nodeN-zookeeper.crt (server/peer)
leader election + dedup"])
            SW(["Swift :8888/8090
nodeN-swift.crt (server)
via HAProxy pass-through"])
        end

        CURL -- "mTLS :8080" --> HA
        HA -- "mTLS :8082\nbackend.pem" --> P1
        P1 -- "pipeline.crt" --> RMQ
        P1 -- "pipeline.crt" --> MDB
        P1 -- "pipeline.crt" --> ZK
        P24 -- "pipeline.crt" --> RMQ
        P24 -- "pipeline.crt" --> MDB
        P24 -- "pipeline.crt" --> ZK
        P24 -- "pipeline.crt" --> SW
        P5 -- "pipeline.crt" --> RMQ
        P5 -- "pipeline.crt" --> ZK
    end

    DIST --> RUNTIME

    %% ── Stage 4: Maintenance ─────────────────────────────────────────
    subgraph MAINT ["④ Maintain — ongoing certificate operations"]
        direction TB
        CRL_R["<b>security/refresh_crl.sh</b>  (weekly cron)
Re-signs CRL before nextUpdate expires
Run install_certs.sh --crl-only after"]
        REVOKE["<b>security/revoke_cert.sh NAME</b>
Marks cert revoked → regenerates CRL
Run install_certs.sh --crl-only after"]
        LIST["<b>security/list_certs.sh</b>
Prints days-to-expiry for every issued cert
Run from cron to alert before expiry"]
        VERIFY["<b>security/verify_tls.sh</b>
Probes every TLS endpoint
Prints PASS / FAIL per component"]
        PREFLIGHT["<b>scripts/preflight_check.py</b>
Per-node check: CRL days left, cert paths,
TLS switches, secret strength"]
    end

    RUNTIME -.->|"cert expired / revoked"| MAINT
    MAINT -.->|"install_certs.sh re-pushes CRL"| DIST
```

---

## 5. PKI Lifecycle — Creation, Distribution and Renewal

> Source: [ReadMe.md § PKI lifecycle](../ReadMe.md#certificate-lifecycle-and-runtime-usage)

Left-to-right view focused on the admin host: CA initialisation, 21 leaf certificate issuance, per-node distribution, weekly CRL refresh and certificate revocation.

```mermaid
flowchart TD
    subgraph ADMIN ["Admin / CA host  (pki/ kept here, never on nodes)"]
        direction TB
        INIT["<b>security/init_ca.sh</b>\nGenerate RSA-4096 root CA\n(ca.key encrypted AES-256, ca.crt public)\nca.crl created at the same time"]
        ISSUE["<b>security/issue_all.sh</b>\nIssues 21 leaf certs under pki/issued/\n• flowfirst-vip  (server)\n• node1-3: -api -rabbitmq -mariadb\n  -zookeeper -swift (server/peer)\n  -pipeline (client)\n• node4-pipeline (client)\n• admin-client (client)"]
        INSTALL["<b>security/install_certs.sh</b>\nSSH+sudo: copies only the files\neach node needs into /etc/flowfirst/tls/"]
        REFRESH["<b>security/refresh_crl.sh</b>\n(weekly cron) re-sign the CRL\nbefore nextUpdate expires"]
        REVOKE["<b>security/revoke_cert.sh NAME</b>\nMark cert revoked → regen CRL\nThen re-run install_certs.sh --crl-only"]
        INIT --> ISSUE
        ISSUE --> INSTALL
        REFRESH -->|"install_certs.sh --crl-only"| INSTALL
        REVOKE -->|"install_certs.sh --crl-only"| INSTALL
    end

    subgraph NODES ["Cluster nodes (node1 – node3)  /etc/flowfirst/tls/"]
        direction TB
        SHARED["ca.crt  ca.crl  (all nodes, root:root 644)"]
        API_F["api/api.{crt,key}  → API Gateway (Process 1) HTTPS listener  (flowuser)"]
        PIPE_F["pipeline/pipeline.{crt,key}  → client identity for all processes  (flowuser)"]
        HA_F["haproxy/vip.pem (VIP server cert)\nhaproxy/backend.pem (= pipeline cert)\n→ HAProxy identity  (haproxy)"]
        RMQ_F["rabbitmq/rabbitmq.{crt,key}  (rabbitmq)"]
        MDB_F["mariadb/mariadb.{crt,key}  (mysql)"]
        ZK_F["zookeeper/zookeeper.{crt,key}  (zookeeper)"]
        SW_F["swift/swift.{crt,key}  (swift)"]
    end

    subgraph NODE4 ["Node 4  /etc/flowfirst/tls/"]
        N4_SHARED["ca.crt  ca.crl"]
        N4_PIPE["pipeline/pipeline.{crt,key}  → Remote Worker (Process 5) client identity"]
    end

    INSTALL -->|"node1-3"| NODES
    INSTALL -->|"node4"| NODE4
```

---

## 6. Runtime TLS Connections — Who Presents Which Certificate

> Source: [ReadMe.md § Runtime TLS connections](../ReadMe.md#certificate-lifecycle-and-runtime-usage)

Left-to-right map of every live TLS connection: external client mTLS through HAProxy, re-encrypted backend to API Gateway (Process 1), and all pipeline processes presenting their `pipeline.crt` client identity to RabbitMQ, MariaDB, ZooKeeper and Swift.

```mermaid
flowchart LR
    CLIENT(["External client\nadmin-client.crt"])
    HA(["HAProxy  :8080\nvip.pem server\nbackend.pem client"])
    P1(["API Gateway (Process 1)  :8082\napi.crt server"])
    P25(["Processes 2–4\npipeline.crt client"])
    P5(["Remote Worker (Process 5) (node4)\npipeline.crt client"])
    RMQ(["RabbitMQ  :5671\nrabbitmq.crt server"])
    MDB(["MariaDB  :3306/3307\nmariadb.crt server"])
    ZK(["ZooKeeper  :2281\nzookeeper.crt server"])
    SW(["Swift proxy  :8888/8090\nswift.crt server"])
    CA(["FlowFirst Root CA\n(ca.crt on every node)"])

    CA -.->|"trust anchor"| CLIENT
    CA -.->|"trust anchor"| HA
    CA -.->|"trust anchor"| P1
    CA -.->|"trust anchor"| P25
    CA -.->|"trust anchor"| P5

    CLIENT -- "mTLS  8080\nX-SSL-Client-CN→header" --> HA
    HA -- "mTLS  8082\nbackend.pem client" --> P1
    P1 -- "mTLS pipeline.crt" --> RMQ
    P1 -- "mTLS pipeline.crt" --> MDB
    P1 -- "mTLS pipeline.crt" --> ZK
    P25 -- "mTLS pipeline.crt" --> RMQ
    P25 -- "mTLS pipeline.crt" --> MDB
    P25 -- "mTLS pipeline.crt" --> ZK
    P25 -- "mTLS pipeline.crt" --> SW
    P5 -- "mTLS pipeline.crt" --> RMQ
    P5 -- "mTLS pipeline.crt" --> ZK

    RMQ -. "peer certs (optional\n--inter-node)" .- RMQ
    MDB -. "peer certs (optional\n--galera)" .- MDB
    ZK -. "peer certs (optional\n--quorum)" .- ZK
```

---

## 7. Authorization Decision Flowchart (RBAC / ACL / ABAC)

> Source: [ReadMe.md § Authorization](../ReadMe.md#scenario-65-authorization-rbac-acl-and-abac)

Hardwired evaluation order for `AUTHZ_MODE`: ABAC deny → RBAC → ACL → ABAC allow → default DENY. Shows how `rbac+acl+abac` combines all three engines.

```mermaid
flowchart TD
    A["Request: identity, action, resource"] --> B{"AUTHZ_MODE=none?"}
    B -- yes --> ALLOW["Allow"]
    B -- no --> C{"ABAC deny rule matches?"}
    C -- yes --> DENY["Deny 403"]
    C -- no --> D{"rbac in mode AND identity has roles?"}
    D -- "grants" --> ALLOW
    D -- "denies" --> E{"acl in mode?"}
    D -- "no roles" --> F{"acl in mode?"}
    E -- "acl grants" --> ALLOW
    E -- "acl denies" --> G{"abac in mode AND allow rule matches?"}
    F -- "acl grants" --> ALLOW
    F -- "acl denies / no entry" --> G
    G -- yes --> ALLOW
    G -- no --> DENY
```

---

*Generated from [`ReadMe.md`](../ReadMe.md). Re-extract with `grep -n '^\`\`\`mermaid' ReadMe.md` to find updated line numbers.*
