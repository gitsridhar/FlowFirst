#!/usr/bin/env bash
set -euo pipefail

# ==============================================================================
# FlowFirst - ZooKeeper Ensemble Setup for RHEL 9.6
#
# Installs and configures a 3-node ZooKeeper ensemble used by the FlowFirst
# pipeline for:
#   - Leader election  (one active worker per process per cluster)
#   - Distributed config store (live threshold / routing changes, no restart)
#   - Service registry (ephemeral znodes — dead processes auto-unregister)
#   - Message dedup barrier (Process 4 prevents double-inserts into Galera)
#   - Pipeline health dashboard (znode tree reflects live pipeline state)
#
# Run this script on ALL THREE nodes.  Each node sets its own myid from
# CURRENT_NODE_IP matched against NODE1_IP / NODE2_IP / NODE3_IP in .env.
#
# Usage:
#   sudo ./zookeeper/setup_zookeeper.sh
# ==============================================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

# Source .env
if [ -f "${ROOT_DIR}/.env" ]; then
    set -a
    # shellcheck disable=SC1091
    source "${ROOT_DIR}/.env"
    set +a
fi

ZK_VERSION="${ZK_VERSION:-3.9.2}"
ZK_INSTALL_DIR="${ZK_INSTALL_DIR:-/opt/zookeeper}"
ZK_DATA_DIR="${ZK_DATA_DIR:-/var/lib/zookeeper/data}"
ZK_LOG_DIR="${ZK_LOG_DIR:-/var/log/zookeeper}"
ZK_CLIENT_PORT="${ZK_CLIENT_PORT:-2181}"
ZK_PEER_PORT="${ZK_PEER_PORT:-2888}"
ZK_ELECTION_PORT="${ZK_ELECTION_PORT:-3888}"
ZK_USER="zookeeper"
ZK_HEAP_MB="${ZK_HEAP_MB:-256}"

NODE1_IP="${NODE1_IP:-192.168.1.101}"
NODE2_IP="${NODE2_IP:-192.168.1.102}"
NODE3_IP="${NODE3_IP:-192.168.1.103}"
CURRENT_NODE_IP="${CURRENT_NODE_IP:-}"

# Derive this node's myid (1, 2, or 3) from its IP
if [ "${CURRENT_NODE_IP}" = "${NODE1_IP}" ]; then
    MY_ID=1
elif [ "${CURRENT_NODE_IP}" = "${NODE2_IP}" ]; then
    MY_ID=2
elif [ "${CURRENT_NODE_IP}" = "${NODE3_IP}" ]; then
    MY_ID=3
else
    echo "ERROR: CURRENT_NODE_IP='${CURRENT_NODE_IP}' does not match any of"
    echo "  NODE1_IP=${NODE1_IP}, NODE2_IP=${NODE2_IP}, NODE3_IP=${NODE3_IP}"
    echo "Set CURRENT_NODE_IP correctly in /opt/flowfirst/.env"
    exit 1
fi

echo "=================================================================="
echo " FlowFirst - ZooKeeper ${ZK_VERSION} Ensemble Setup"
echo " This node: ${CURRENT_NODE_IP}  (myid=${MY_ID})"
echo " Ensemble:  ${NODE1_IP}, ${NODE2_IP}, ${NODE3_IP}"
echo " Client port:   ${ZK_CLIENT_PORT}"
echo " Install dir:   ${ZK_INSTALL_DIR}"
echo "=================================================================="

# ---------------------------------------------------------------------------
# Step 1: Java (ZooKeeper requires JRE 11+)
# ---------------------------------------------------------------------------
echo "[1/8] Checking Java..."
if ! command -v java &>/dev/null || ! java -version 2>&1 | grep -qE "version \"(11|17|21)"; then
    echo "  Installing Java 17 (OpenJDK)..."
    sudo dnf install -y java-17-openjdk-headless
else
    echo "  Java already present: $(java -version 2>&1 | head -1)"
fi

# ---------------------------------------------------------------------------
# Step 2: Download and extract ZooKeeper
# ---------------------------------------------------------------------------
echo "[2/8] Installing ZooKeeper ${ZK_VERSION}..."
ZK_TARBALL="apache-zookeeper-${ZK_VERSION}-bin.tar.gz"
ZK_URL="https://downloads.apache.org/zookeeper/zookeeper-${ZK_VERSION}/${ZK_TARBALL}"
ZK_MIRROR_URL="https://archive.apache.org/dist/zookeeper/zookeeper-${ZK_VERSION}/${ZK_TARBALL}"

if [ ! -d "${ZK_INSTALL_DIR}" ]; then
    TMP_DIR=$(mktemp -d)
    echo "  Downloading ${ZK_TARBALL}..."
    if ! curl -fsSL "${ZK_URL}" -o "${TMP_DIR}/${ZK_TARBALL}" 2>/dev/null; then
        echo "  Primary mirror failed — trying archive.apache.org..."
        curl -fsSL "${ZK_MIRROR_URL}" -o "${TMP_DIR}/${ZK_TARBALL}"
    fi
    sudo mkdir -p "${ZK_INSTALL_DIR}"
    sudo tar -xzf "${TMP_DIR}/${ZK_TARBALL}" --strip-components=1 -C "${ZK_INSTALL_DIR}"
    rm -rf "${TMP_DIR}"
    echo "  Extracted to ${ZK_INSTALL_DIR}"
else
    echo "  ${ZK_INSTALL_DIR} already exists — skipping download"
fi

# ---------------------------------------------------------------------------
# Step 3: Create dedicated user and directories
# ---------------------------------------------------------------------------
echo "[3/8] Creating '${ZK_USER}' user and data/log directories..."
if ! id "${ZK_USER}" &>/dev/null; then
    sudo useradd --system --no-create-home --shell /sbin/nologin "${ZK_USER}"
fi
sudo mkdir -p "${ZK_DATA_DIR}" "${ZK_LOG_DIR}"
sudo chown -R "${ZK_USER}:${ZK_USER}" "${ZK_INSTALL_DIR}" "${ZK_DATA_DIR}" "${ZK_LOG_DIR}"

# ---------------------------------------------------------------------------
# Step 4: Write myid
# ---------------------------------------------------------------------------
echo "[4/8] Writing myid=${MY_ID} to ${ZK_DATA_DIR}/myid..."
echo "${MY_ID}" | sudo tee "${ZK_DATA_DIR}/myid" > /dev/null
sudo chown "${ZK_USER}:${ZK_USER}" "${ZK_DATA_DIR}/myid"

# ---------------------------------------------------------------------------
# Step 5: Write zoo.cfg
# ---------------------------------------------------------------------------
echo "[5/8] Writing ${ZK_INSTALL_DIR}/conf/zoo.cfg..."
sudo tee "${ZK_INSTALL_DIR}/conf/zoo.cfg" > /dev/null << EOF
# FlowFirst ZooKeeper ensemble configuration
# Generated by zookeeper/setup_zookeeper.sh

tickTime=2000
initLimit=10
syncLimit=5

dataDir=${ZK_DATA_DIR}
dataLogDir=${ZK_LOG_DIR}

clientPort=${ZK_CLIENT_PORT}

# Ensemble members (server.myid=host:peer_port:election_port)
server.1=${NODE1_IP}:${ZK_PEER_PORT}:${ZK_ELECTION_PORT}
server.2=${NODE2_IP}:${ZK_PEER_PORT}:${ZK_ELECTION_PORT}
server.3=${NODE3_IP}:${ZK_PEER_PORT}:${ZK_ELECTION_PORT}

# Session and connection tuning
maxClientCnxns=120
minSessionTimeout=4000
maxSessionTimeout=40000

# Auto-purge snapshots (keep last 5, run hourly)
autopurge.snapRetainCount=5
autopurge.purgeInterval=1

# 4-letter word commands (for health checks)
4lw.commands.whitelist=mntr,srvr,stat,ruok,dump,conf,isro

# Admin server (ZK 3.5+)
admin.enableServer=true
admin.serverPort=8080
EOF

# ---------------------------------------------------------------------------
# Step 6: Write java.env (heap size)
# ---------------------------------------------------------------------------
echo "[6/8] Writing ${ZK_INSTALL_DIR}/conf/java.env..."
sudo tee "${ZK_INSTALL_DIR}/conf/java.env" > /dev/null << EOF
export JVMFLAGS="-Xmx${ZK_HEAP_MB}m -Xms${ZK_HEAP_MB}m"
export ZOO_LOG_DIR="${ZK_LOG_DIR}"
EOF

# ---------------------------------------------------------------------------
# Step 7: Install systemd unit
# ---------------------------------------------------------------------------
echo "[7/8] Installing zookeeper.service systemd unit..."
sudo tee /etc/systemd/system/zookeeper.service > /dev/null << EOF
[Unit]
Description=Apache ZooKeeper (FlowFirst ensemble)
After=network.target
Wants=network.target

[Service]
Type=forking
User=${ZK_USER}
Group=${ZK_USER}
Environment="JAVA_HOME=/usr/lib/jvm/jre-17"
Environment="ZOO_LOG_DIR=${ZK_LOG_DIR}"
ExecStart=${ZK_INSTALL_DIR}/bin/zkServer.sh start
ExecStop=${ZK_INSTALL_DIR}/bin/zkServer.sh stop
ExecReload=${ZK_INSTALL_DIR}/bin/zkServer.sh restart
PIDFile=${ZK_DATA_DIR}/zookeeper_server.pid
Restart=on-failure
RestartSec=5s
StandardOutput=journal
StandardError=journal
SyslogIdentifier=zookeeper

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl daemon-reload
sudo systemctl enable zookeeper

# ---------------------------------------------------------------------------
# Step 8: Open firewall ports and start service
# ---------------------------------------------------------------------------
echo "[8/8] Opening firewall ports and starting ZooKeeper..."
if command -v firewall-cmd &>/dev/null && sudo systemctl is-active --quiet firewalld; then
    sudo firewall-cmd --permanent --add-port="${ZK_CLIENT_PORT}/tcp"
    sudo firewall-cmd --permanent --add-port="${ZK_PEER_PORT}/tcp"
    sudo firewall-cmd --permanent --add-port="${ZK_ELECTION_PORT}/tcp"
    sudo firewall-cmd --reload
    echo "  Firewall ports opened: ${ZK_CLIENT_PORT}/tcp, ${ZK_PEER_PORT}/tcp, ${ZK_ELECTION_PORT}/tcp"
fi

sudo systemctl restart zookeeper
sleep 3

# Verify with 4-letter word
STATUS=$(echo ruok | nc -w2 127.0.0.1 "${ZK_CLIENT_PORT}" 2>/dev/null || true)
if [ "${STATUS}" = "imok" ]; then
    echo "  ZooKeeper is responding: imok"
else
    echo "  WARNING: ZooKeeper did not respond 'imok' yet."
    echo "  The ensemble requires a quorum (2 of 3 nodes) before it becomes fully operational."
    echo "  Start ZooKeeper on the other nodes, then re-check with:"
    echo "    echo ruok | nc 127.0.0.1 ${ZK_CLIENT_PORT}"
fi

echo ""
echo "=================================================================="
echo " ZooKeeper setup complete on this node (myid=${MY_ID})"
echo " Run this script on all 3 nodes, then verify:"
echo "   echo mntr | nc 127.0.0.1 ${ZK_CLIENT_PORT} | grep zk_server_state"
echo "   # One node: zk_server_state\tleader"
echo "   # Two nodes: zk_server_state\tfollower"
echo "=================================================================="
