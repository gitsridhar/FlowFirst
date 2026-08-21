#!/usr/bin/env bash
set -euo pipefail

# ==============================================================================
# FlowFirst - HAProxy Setup Script for 3-Node Cluster on RHEL 9.6
# Automatically loads network configuration from .env if present
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

NODE1_NAME="${NODE1_NAME:-node1}"
NODE1_IP="${1:-${NODE1_IP:-192.168.1.101}}"
NODE2_NAME="${NODE2_NAME:-node2}"
NODE2_IP="${2:-${NODE2_IP:-192.168.1.102}}"
NODE3_NAME="${NODE3_NAME:-node3}"
NODE3_IP="${3:-${NODE3_IP:-192.168.1.103}}"
VIP="${4:-${FLOWFIRST_VIP:-192.168.1.100}}"
API_PORT="${API_PORT:-8080}"
MARIADB_PORT="${MARIADB_PORT:-3306}"
RABBITMQ_PORT="${RABBITMQ_PORT:-5672}"
HAPROXY_STATS_PORT="${HAPROXY_STATS_PORT:-9000}"
HAPROXY_STATS_USER="${HAPROXY_STATS_USER:-admin}"
HAPROXY_STATS_PASS="${HAPROXY_STATS_PASS:-admin123}"

echo "=================================================================="
echo " Setting up HAProxy Load Balancer for FlowFirst Multi-Node Cluster"
echo " Node 1:          ${NODE1_NAME} (${NODE1_IP})"
echo " Node 2:          ${NODE2_NAME} (${NODE2_IP})"
echo " Node 3:          ${NODE3_NAME} (${NODE3_IP})"
echo " Virtual IP (VIP):${VIP}"
echo " API Port:        ${API_PORT}"
echo " MariaDB Port:    ${MARIADB_PORT}"
echo " RabbitMQ Port:   ${RABBITMQ_PORT}"
echo " Stats Dashboard: :${HAPROXY_STATS_PORT}"
echo "=================================================================="

# 1. Install HAProxy
echo "[1/5] Installing HAProxy..."
if command -v dnf >/dev/null 2>&1; then
    sudo dnf install -y haproxy
fi

# 2. Allow binding to non-local IP addresses (crucial for Virtual IP / Pacemaker VIP)
echo "[2/5] Configuring sysctl net.ipv4.ip_nonlocal_bind..."
sudo sysctl -w net.ipv4.ip_nonlocal_bind=1
echo "net.ipv4.ip_nonlocal_bind=1" | sudo tee /etc/sysctl.d/99-haproxy-nonlocalbind.conf > /dev/null

# 3. Generate HAProxy configuration from template using environment variables
echo "[3/5] Generating /etc/haproxy/haproxy.cfg..."
sed \
    -e "s/__NODE1_NAME__/${NODE1_NAME}/g" \
    -e "s/__NODE1_IP__/${NODE1_IP}/g" \
    -e "s/__NODE2_NAME__/${NODE2_NAME}/g" \
    -e "s/__NODE2_IP__/${NODE2_IP}/g" \
    -e "s/__NODE3_NAME__/${NODE3_NAME}/g" \
    -e "s/__NODE3_IP__/${NODE3_IP}/g" \
    -e "s/__API_PORT__/${API_PORT}/g" \
    -e "s/__MARIADB_PORT__/${MARIADB_PORT}/g" \
    -e "s/__RABBITMQ_PORT__/${RABBITMQ_PORT}/g" \
    -e "s/__HAPROXY_STATS_PORT__/${HAPROXY_STATS_PORT}/g" \
    -e "s/__HAPROXY_STATS_USER__/${HAPROXY_STATS_USER}/g" \
    -e "s/__HAPROXY_STATS_PASS__/${HAPROXY_STATS_PASS}/g" \
    "${SCRIPT_DIR}/haproxy.cfg.template" | sudo tee /etc/haproxy/haproxy.cfg > /dev/null

# 4. Open firewall ports
echo "[4/5] Opening firewall ports (: ${API_PORT}, : ${MARIADB_PORT}, : ${RABBITMQ_PORT}, : ${HAPROXY_STATS_PORT})..."
if command -v firewall-cmd >/dev/null 2>&1 && sudo systemctl is-active --quiet firewalld; then
    sudo firewall-cmd --permanent --add-port="${API_PORT}/tcp"
    sudo firewall-cmd --permanent --add-port="${MARIADB_PORT}/tcp"
    sudo firewall-cmd --permanent --add-port="${RABBITMQ_PORT}/tcp"
    sudo firewall-cmd --permanent --add-port="${HAPROXY_STATS_PORT}/tcp"
    sudo firewall-cmd --reload
fi

# 5. Validate configuration syntax
echo "[5/5] Validating configuration syntax..."
sudo haproxy -c -f /etc/haproxy/haproxy.cfg

echo ""
echo "=================================================================="
echo " HAProxy configuration successfully installed and verified!"
echo " Note: In a Pacemaker-managed cluster, HAProxy will be controlled"
echo " as a Pacemaker cluster resource (or clone)."
echo "=================================================================="
