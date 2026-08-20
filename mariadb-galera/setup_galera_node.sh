#!/usr/bin/env bash
set -euo pipefail

# ==============================================================================
# FlowFirst - MariaDB Galera Cluster Setup Script for RHEL 9.6
# ==============================================================================

CURRENT_NODE_NAME="${1:-node1}"
CURRENT_NODE_IP="${2:-192.168.1.101}"
NODE1_IP="${3:-192.168.1.101}"
NODE2_IP="${4:-192.168.1.102}"
NODE3_IP="${5:-192.168.1.103}"

echo "=================================================================="
echo " Setting up MariaDB Galera Cluster Node on RHEL 9.6"
echo " Node Name:      ${CURRENT_NODE_NAME}"
echo " Node IP:        ${CURRENT_NODE_IP}"
echo " Cluster Nodes:  ${NODE1_IP}, ${NODE2_IP}, ${NODE3_IP}"
echo "=================================================================="

# 1. Install MariaDB server and Galera packages
echo "[1/5] Installing mariadb-server and galera..."
if command -v dnf >/dev/null 2>&1; then
    sudo dnf install -y mariadb-server mariadb mariadb-server-galera galera rsync
fi

# 2. Configure Firewall for Galera replication ports
echo "[2/5] Configuring firewall ports for MariaDB and Galera (3306, 4567, 4568, 4444)..."
if command -v firewall-cmd >/dev/null 2>&1 && sudo systemctl is-active --quiet firewalld; then
    sudo firewall-cmd --permanent --add-port=3306/tcp   # Standard MySQL/MariaDB client port
    sudo firewall-cmd --permanent --add-port=4567/tcp   # Galera Cluster replication traffic
    sudo firewall-cmd --permanent --add-port=4567/udp   # Galera Cluster multicast replication
    sudo firewall-cmd --permanent --add-port=4568/tcp   # Incremental State Transfer (IST)
    sudo firewall-cmd --permanent --add-port=4444/tcp   # State Snapshot Transfer (SST)
    sudo firewall-cmd --reload
fi

# 3. Deploy Galera configuration
echo "[3/5] Generating /etc/my.cnf.d/galera.cnf..."
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
sudo sed \
    -e "s/wsrep_cluster_address=.*/wsrep_cluster_address=\"gcomm:\/\/${NODE1_IP},${NODE2_IP},${NODE3_IP}\"/g" \
    -e "s/wsrep_node_address=.*/wsrep_node_address=\"${CURRENT_NODE_IP}\"/g" \
    -e "s/wsrep_node_name=.*/wsrep_node_name=\"${CURRENT_NODE_NAME}\"/g" \
    "${SCRIPT_DIR}/galera.cnf.template" | sudo tee /etc/my.cnf.d/galera.cnf > /dev/null

# 4. Stop standalone mariadb if running
sudo systemctl stop mariadb || true

echo ""
echo "=================================================================="
echo " Galera configuration written successfully to /etc/my.cnf.d/galera.cnf"
echo ""
echo " Next Steps to start the cluster:"
echo " - On Node 1 (Bootstrap node ONLY):"
echo "     sudo ./mariadb-galera/bootstrap_galera.sh"
echo " - On Node 2 & Node 3 (Joiner nodes):"
echo "     sudo systemctl enable --now mariadb"
echo "=================================================================="
