#!/usr/bin/env bash
set -euo pipefail

# ==============================================================================
# FlowFirst - HAProxy Setup Script for 3-Node Cluster on RHEL 9.6
# Automatically loads network configuration from .env if present
# ==============================================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

# Source environment variables if .env exists
if [[ -f "${ROOT_DIR}/.env" ]]; then
    set -a
    # shellcheck disable=SC1091
    source "${ROOT_DIR}/.env"
    set +a
fi

NODE1_NAME="${NODE1_NAME:-node1}"
NODE1_IP="${1:-${NODE1_IP:-}}"
NODE2_NAME="${NODE2_NAME:-node2}"
NODE2_IP="${2:-${NODE2_IP:-}}"
NODE3_NAME="${NODE3_NAME:-node3}"
NODE3_IP="${3:-${NODE3_IP:-}}"
VIP="${4:-${FLOWFIRST_VIP:-}}"

# Validate required IP variables are set (must come from .env)
for _var in NODE1_IP NODE2_IP NODE3_IP FLOWFIRST_VIP; do
    _val="${!_var:-}"
    if [[ -z "${_val}" ]] || [[ "${_val}" =~ ^\$\{ ]]; then
        echo "ERROR: ${_var} is not set. Set it in /opt/flowfirst/.env and re-run."
        exit 1
    fi
done
VIP="${VIP:-${FLOWFIRST_VIP}}"
VIP_NIC="${VIP_NIC:-}"           # set in .env — see pre-flight check below
API_PORT="${API_PORT:-8080}"
MARIADB_PORT="${MARIADB_PORT:-3306}"
RABBITMQ_PORT="${RABBITMQ_PORT:-5672}"
HAPROXY_STATS_PORT="${HAPROXY_STATS_PORT:-9000}"
HAPROXY_STATS_USER="${HAPROXY_STATS_USER:-admin}"
HAPROXY_STATS_PASS="${HAPROXY_STATS_PASS:-admin123}"

# ---------------------------------------------------------------------------
# Pre-flight: discover and validate the cluster NIC
# ---------------------------------------------------------------------------
# VIP_NIC must be the physical NIC that the cluster traffic uses on THIS node.
# RHEL 9 uses predictable network interface names (e.g. ens3, ens192, enp0s3,
# eth0) that differ between hardware/virtualisation platforms.
# If VIP_NIC is not set in .env, auto-detect the interface that carries
# the node's own cluster IP (CURRENT_NODE_IP or NODE1_IP as fallback).
detect_cluster_nic() {
    local probe_ip="${CURRENT_NODE_IP:-${NODE1_IP}}"
    local detected
    detected=$(ip -o route get "${probe_ip}" 2>/dev/null \
               | awk '{for(i=1;i<=NF;i++) if($i=="dev") print $(i+1)}' \
               | head -1)
    if [[ -z "${detected}" ]]; then
        # Fallback: interface that owns the probe_ip
        detected=$(ip -o addr show \
                   | awk -v ip="${probe_ip}" '$0 ~ ip {print $2}' \
                   | head -1)
    fi
    echo "${detected}"
}

if [[ -z "${VIP_NIC}" ]]; then
    VIP_NIC=$(detect_cluster_nic)
    if [[ -z "${VIP_NIC}" ]]; then
        echo "ERROR: Cannot determine cluster NIC automatically."
        echo "  Set VIP_NIC=<interface> in /opt/flowfirst/.env"
        echo "  Run: ip -o link show | awk '{print \$2}' | tr -d ':'"
        echo "  to list available interfaces on this node."
        exit 1
    fi
    echo "  [INFO] VIP_NIC not set in .env — auto-detected: ${VIP_NIC}"
fi

# Confirm the interface actually exists on this node
if ! ip link show "${VIP_NIC}" &>/dev/null; then
    echo "ERROR: Interface '${VIP_NIC}' does not exist on this node."
    echo "  Available interfaces:"
    ip -o link show | awk '{print "   ", $2}' | tr -d ':'
    echo ""
    echo "  Fix: set VIP_NIC=<correct-interface> in /opt/flowfirst/.env"
    echo "  Common RHEL 9 names: ens3, ens192, enp0s3, enp1s0, eth0"
    exit 1
fi

echo "=================================================================="
echo " Setting up HAProxy Load Balancer for FlowFirst Multi-Node Cluster"
echo " Node 1:          ${NODE1_NAME} (${NODE1_IP})"
echo " Node 2:          ${NODE2_NAME} (${NODE2_IP})"
echo " Node 3:          ${NODE3_NAME} (${NODE3_IP})"
echo " Virtual IP (VIP):${VIP}"
echo " VIP NIC:         ${VIP_NIC}"
echo " API Port:        ${API_PORT}"
echo " MariaDB Port:    ${MARIADB_PORT}"
echo " RabbitMQ Port:   ${RABBITMQ_PORT}"
echo " Stats Dashboard: :${HAPROXY_STATS_PORT}"
echo "=================================================================="

# 1. Install HAProxy
echo "[1/6] Installing HAProxy..."
if command -v dnf >/dev/null 2>&1; then
    sudo dnf install -y haproxy
fi

# 2. Allow HAProxy to bind to the VIP address even when Pacemaker hasn't
#    assigned it to this node yet.  Without this, HAProxy fails to start
#    on nodes that don't currently hold the VIP.
echo "[2/6] Configuring net.ipv4.ip_nonlocal_bind=1 ..."
sudo sysctl -w net.ipv4.ip_nonlocal_bind=1
echo "net.ipv4.ip_nonlocal_bind=1" | \
    sudo tee /etc/sysctl.d/99-haproxy-nonlocalbind.conf > /dev/null
# Apply immediately and persist across reboots
sudo sysctl --system | grep nonlocal_bind || true

# 3. SELinux: allow HAProxy to bind to cluster ports
echo "[3/6] Configuring SELinux for HAProxy..."
if command -v getsebool &>/dev/null && sudo getsebool haproxy_connect_any &>/dev/null; then
    sudo setsebool -P haproxy_connect_any 1
    echo "  SELinux: haproxy_connect_any set to ON"
else
    echo "  SELinux: haproxy_connect_any not available — skipping"
fi

# 4. Generate HAProxy configuration from template
echo "[4/6] Generating /etc/haproxy/haproxy.cfg..."
sed \
    -e "s/__NODE1_NAME__/${NODE1_NAME}/g" \
    -e "s/__NODE1_IP__/${NODE1_IP}/g" \
    -e "s/__NODE2_NAME__/${NODE2_NAME}/g" \
    -e "s/__NODE2_IP__/${NODE2_IP}/g" \
    -e "s/__NODE3_NAME__/${NODE3_NAME}/g" \
    -e "s/__NODE3_IP__/${NODE3_IP}/g" \
    -e "s/__VIP__/${VIP}/g" \
    -e "s/__API_PORT__/${API_PORT}/g" \
    -e "s/__MARIADB_PORT__/${MARIADB_PORT}/g" \
    -e "s/__RABBITMQ_PORT__/${RABBITMQ_PORT}/g" \
    -e "s/__HAPROXY_STATS_PORT__/${HAPROXY_STATS_PORT}/g" \
    -e "s/__HAPROXY_STATS_USER__/${HAPROXY_STATS_USER}/g" \
    -e "s/__HAPROXY_STATS_PASS__/${HAPROXY_STATS_PASS}/g" \
    "${SCRIPT_DIR}/haproxy.cfg.template" | sudo tee /etc/haproxy/haproxy.cfg > /dev/null

# 5. Open firewall ports
echo "[5/6] Opening firewall ports..."
if command -v firewall-cmd >/dev/null 2>&1 && sudo systemctl is-active --quiet firewalld; then
    sudo firewall-cmd --permanent --add-port="${API_PORT}/tcp"
    sudo firewall-cmd --permanent --add-port="${MARIADB_PORT}/tcp"
    sudo firewall-cmd --permanent --add-port="${RABBITMQ_PORT}/tcp"
    sudo firewall-cmd --permanent --add-port="${HAPROXY_STATS_PORT}/tcp"
    sudo firewall-cmd --reload
fi

# 6. Validate configuration syntax
echo "[6/6] Validating HAProxy configuration syntax..."
sudo haproxy -c -f /etc/haproxy/haproxy.cfg

echo ""
echo "=================================================================="
echo " HAProxy configuration successfully installed and verified!"
echo " VIP_NIC used: ${VIP_NIC}"
echo " Note: HAProxy is managed by Pacemaker as part of vip-haproxy-group."
echo " Do NOT start haproxy.service directly — let Pacemaker control it."
echo "=================================================================="
