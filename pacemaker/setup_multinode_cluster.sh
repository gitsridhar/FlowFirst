#!/usr/bin/env bash
set -euo pipefail

# ==============================================================================
# FlowFirst - Multi-Node Pacemaker Cluster Setup Script (3 Nodes)
# ==============================================================================

CLUSTER_NAME="${1:-flowfirst_cluster}"
NODE1_NAME="${2:-node1}"
NODE1_IP="${3:-192.168.1.101}"
NODE2_NAME="${4:-node2}"
NODE2_IP="${5:-192.168.1.102}"
NODE3_NAME="${6:-node3}"
NODE3_IP="${7:-192.168.1.103}"

WORKING_DIR="$(pwd)"

echo "=================================================================="
echo " Setting up 3-Node Pacemaker High Availability Cluster"
echo " Cluster: ${CLUSTER_NAME}"
echo " Node 1:  ${NODE1_NAME} (${NODE1_IP})"
echo " Node 2:  ${NODE2_NAME} (${NODE2_IP})"
echo " Node 3:  ${NODE3_NAME} (${NODE3_IP})"
echo "=================================================================="

# 1. Install Pacemaker, Corosync, and PCS on current host
echo "[1/6] Installing Pacemaker, Corosync, PCS, and HAProxy..."
if command -v dnf >/dev/null 2>&1; then
    sudo dnf install -y pcs pacemaker corosync fence-agents-all haproxy
fi

# 2. Enable and start pcsd
echo "[2/6] Starting pcsd service..."
sudo systemctl enable --now pcsd

# 3. Set cluster administrative password
echo "[3/6] Setting hacluster password..."
echo "hacluster:hacluster123" | sudo chpasswd

# 4. Authenticate all 3 cluster nodes
echo "[4/6] Authenticating all 3 nodes..."
sudo pcs host auth \
    "${NODE1_NAME}" addr="${NODE1_IP}" \
    "${NODE2_NAME}" addr="${NODE2_IP}" \
    "${NODE3_NAME}" addr="${NODE3_IP}" \
    -u hacluster -p hacluster123

# 5. Create and initialize 3-node Corosync cluster
echo "[5/6] Initializing Corosync/Pacemaker cluster with 3 nodes..."
sudo pcs cluster setup "${CLUSTER_NAME}" \
    "${NODE1_NAME}" addr="${NODE1_IP}" \
    "${NODE2_NAME}" addr="${NODE2_IP}" \
    "${NODE3_NAME}" addr="${NODE3_IP}" \
    --force

sudo pcs cluster start --all
sudo pcs cluster enable --all

# 6. Cluster Quorum and STONITH settings
echo "[6/6] Configuring cluster properties for multi-node operation..."
# In production with fencing hardware, configure STONITH fence agents.
# For demo/software HA:
sudo pcs property set stonith-enabled=false

echo ""
echo "=================================================================="
echo " 3-Node Cluster setup successfully initialized!"
echo " Run ./pacemaker/configure_multinode_resources.sh to deploy VIP,"
echo " HAProxy, and cloned FlowFirst process resources."
echo "=================================================================="
