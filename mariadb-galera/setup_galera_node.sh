#!/usr/bin/env bash
set -euo pipefail

# ==============================================================================
# FlowFirst - MariaDB Galera Cluster Setup Script for RHEL 9.6
# Automatically loads configuration from .env if present
# ==============================================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

# Source environment variables if .env exists
if [ -f "${ROOT_DIR}/.env" ]; then
    # shellcheck disable=SC1091
    set -a
    source "${ROOT_DIR}/.env"
    set +a
fi

CURRENT_NODE_NAME="${1:-${NODE_NAME:-node1}}"
CURRENT_NODE_IP="${2:-${CURRENT_NODE_IP:-192.168.1.101}}"
NODE1_IP="${3:-${NODE1_IP:-192.168.1.101}}"
NODE2_IP="${4:-${NODE2_IP:-192.168.1.102}}"
NODE3_IP="${5:-${NODE3_IP:-192.168.1.103}}"
MARIADB_PORT="${MARIADB_PORT:-3306}"
MARIADB_GALERA_CLUSTER_NAME="${MARIADB_GALERA_CLUSTER_NAME:-flowfirst_galera_cluster}"
MARIADB_GALERA_WSREP_PROVIDER="${MARIADB_GALERA_WSREP_PROVIDER:-/usr/lib64/galera-4/libgalera_smm.so}"

# Construct cluster address list from environment
if [ -n "${MARIADB_HOSTS:-}" ]; then
    CLUSTER_ADDRESSES="${MARIADB_HOSTS}"
else
    CLUSTER_ADDRESSES="${NODE1_IP},${NODE2_IP},${NODE3_IP}"
fi

echo "=================================================================="
echo " Setting up MariaDB Galera Cluster Node on RHEL 9.6"
echo " Node Name:         ${CURRENT_NODE_NAME}"
echo " Node IP:           ${CURRENT_NODE_IP}"
echo " Cluster Name:      ${MARIADB_GALERA_CLUSTER_NAME}"
echo " Cluster Members:   ${CLUSTER_ADDRESSES}"
echo " WSREP Provider:    ${MARIADB_GALERA_WSREP_PROVIDER}"
echo " MariaDB Port:      ${MARIADB_PORT}"
echo "=================================================================="

# 1. Install MariaDB server and Galera packages
echo "[1/5] Installing mariadb-server and galera..."
if command -v dnf >/dev/null 2>&1; then
    sudo dnf install -y mariadb-server mariadb mariadb-server-galera galera rsync
fi

# 2. Configure Firewall for Galera replication ports
echo "[2/5] Configuring firewall ports for MariaDB and Galera (3306, 4567, 4568, 4444)..."
if command -v firewall-cmd >/dev/null 2>&1 && sudo systemctl is-active --quiet firewalld; then
    sudo firewall-cmd --permanent --add-port="${MARIADB_PORT}/tcp"   # Standard MySQL/MariaDB client port
    sudo firewall-cmd --permanent --add-port=4567/tcp               # Galera Cluster replication traffic
    sudo firewall-cmd --permanent --add-port=4567/udp               # Galera Cluster multicast replication
    sudo firewall-cmd --permanent --add-port=4568/tcp               # Incremental State Transfer (IST)
    sudo firewall-cmd --permanent --add-port=4444/tcp               # State Snapshot Transfer (SST)
    sudo firewall-cmd --reload
fi

# 3. Deploy Galera configuration
echo "[3/5] Generating /etc/my.cnf.d/galera.cnf..."
sudo sed \
    -e "s|__MARIADB_GALERA_WSREP_PROVIDER__|${MARIADB_GALERA_WSREP_PROVIDER}|g" \
    -e "s|__MARIADB_GALERA_CLUSTER_NAME__|${MARIADB_GALERA_CLUSTER_NAME}|g" \
    -e "s|__GALERA_CLUSTER_ADDRESSES__|${CLUSTER_ADDRESSES}|g" \
    -e "s|__CURRENT_NODE_IP__|${CURRENT_NODE_IP}|g" \
    -e "s|__CURRENT_NODE_NAME__|${CURRENT_NODE_NAME}|g" \
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
