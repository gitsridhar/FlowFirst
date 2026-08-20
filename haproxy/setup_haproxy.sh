#!/usr/bin/env bash
set -euo pipefail

# ==============================================================================
# FlowFirst - HAProxy Setup Script for 3-Node Cluster on RHEL 9.6
# ==============================================================================

NODE1_IP="${1:-192.168.1.101}"
NODE2_IP="${2:-192.168.1.102}"
NODE3_IP="${3:-192.168.1.103}"
VIP="${4:-192.168.1.100}"

echo "=================================================================="
echo " Setting up HAProxy Load Balancer for FlowFirst Multi-Node Cluster"
echo " Node 1: ${NODE1_IP}"
echo " Node 2: ${NODE2_IP}"
echo " Node 3: ${NODE3_IP}"
echo " Virtual IP (VIP): ${VIP}"
echo "=================================================================="

# 1. Install HAProxy
echo "[1/4] Installing HAProxy..."
if command -v dnf >/dev/null 2>&1; then
    sudo dnf install -y haproxy
fi

# 2. Allow binding to non-local IP addresses (crucial for Virtual IP / Pacemaker VIP)
echo "[2/4] Configuring sysctl net.ipv4.ip_nonlocal_bind..."
sudo sysctl -w net.ipv4.ip_nonlocal_bind=1
echo "net.ipv4.ip_nonlocal_bind=1" | sudo tee /etc/sysctl.d/99-haproxy-nonlocalbind.conf

# 3. Generate HAProxy configuration from template
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
echo "[3/4] Generating /etc/haproxy/haproxy.cfg..."
sed \
    -e "s/192.168.1.101/${NODE1_IP}/g" \
    -e "s/192.168.1.102/${NODE2_IP}/g" \
    -e "s/192.168.1.103/${NODE3_IP}/g" \
    "${SCRIPT_DIR}/haproxy.cfg.template" | sudo tee /etc/haproxy/haproxy.cfg > /dev/null

# 4. Open firewall ports
echo "[4/4] Opening firewall ports for REST API (:8080), Stats (:9000), and AMQP (:5672)..."
if command -v firewall-cmd >/dev/null 2>&1 && sudo systemctl is-active --quiet firewalld; then
    sudo firewall-cmd --permanent --add-port=8080/tcp
    sudo firewall-cmd --permanent --add-port=9000/tcp
    sudo firewall-cmd --permanent --add-port=5672/tcp
    sudo firewall-cmd --reload
fi

# 5. Validate configuration syntax
sudo haproxy -c -f /etc/haproxy/haproxy.cfg

echo ""
echo "=================================================================="
echo " HAProxy configuration successfully installed and verified!"
echo " Note: In a Pacemaker-managed cluster, HAProxy will be controlled"
echo " as a Pacemaker cluster resource (or clone)."
echo "=================================================================="
