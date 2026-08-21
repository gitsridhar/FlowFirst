#!/usr/bin/env bash
set -euo pipefail

# ==============================================================================
# FlowFirst - Configure Multi-Node Pacemaker Resources (VIP, HAProxy, & Pipelines)
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

VIP="${1:-${FLOWFIRST_VIP:-192.168.1.100}}"
VIP_NIC="${2:-${VIP_NIC:-eth0}}"
VIP_CIDR_NETMASK="${3:-${VIP_CIDR_NETMASK:-24}}"

echo "=================================================================="
echo " Configuring Multi-Node Pacemaker Resources"
echo " Virtual IP (VIP):    ${VIP}"
echo " Network Interface:   ${VIP_NIC}"
echo " CIDR Netmask:        ${VIP_CIDR_NETMASK}"
echo "=================================================================="

# 1. Clean up prior resources if present
echo "[1/5] Removing existing cluster resources..."
for res in vip-haproxy-group flowfirst-vip haproxy-clone haproxy-res flowfirst-p1-res-clone flowfirst-p2-res-clone flowfirst-p3-res-clone flowfirst-p4-res-clone flowfirst-p1-res flowfirst-p2-res flowfirst-p3-res flowfirst-p4-res; do
    if sudo pcs resource status "${res}" >/dev/null 2>&1; then
        echo "  Deleting resource ${res}..."
        sudo pcs resource delete "${res}" --force || true
    fi
done

# 2. Configure Virtual IP (IPaddr2 resource agent)
echo "[2/5] Creating Virtual IP resource (flowfirst-vip)..."
sudo pcs resource create flowfirst-vip ocf:heartbeat:IPaddr2 \
    ip="${VIP}" \
    cidr_netmask="${VIP_CIDR_NETMASK}" \
    nic="${VIP_NIC}" \
    op monitor interval=10s timeout=20s

# 3. Configure HAProxy as a Pacemaker Cluster Resource
echo "[3/5] Creating HAProxy cluster resource..."
sudo pcs resource create haproxy-res systemd:haproxy \
    op monitor interval=15s timeout=20s \
    op start timeout=30s \
    op stop timeout=30s

# Group Virtual IP with HAProxy so HAProxy runs wherever the VIP lands
sudo pcs resource group add vip-haproxy-group flowfirst-vip haproxy-res

# 4. Configure FlowFirst Processes (Cloned Active-Active across all 3 nodes)
echo "[4/5] Creating cloned active-active resources for Process 1, 2, 3, and 4..."

# Process 4: MariaDB persister
sudo pcs resource create flowfirst-p4-res systemd:flowfirst-process4 \
    op monitor interval=15s timeout=20s
sudo pcs resource clone flowfirst-p4-res clone-max=3 clone-node-max=1

# Process 3: Reflector & Forwarder
sudo pcs resource create flowfirst-p3-res systemd:flowfirst-process3 \
    op monitor interval=15s timeout=20s
sudo pcs resource clone flowfirst-p3-res clone-max=3 clone-node-max=1

# Process 2: Examiner & Reflector
sudo pcs resource create flowfirst-p2-res systemd:flowfirst-process2 \
    op monitor interval=15s timeout=20s
sudo pcs resource clone flowfirst-p2-res clone-max=3 clone-node-max=1

# Process 1: REST API Producer (listening on :8080 on all 3 nodes)
sudo pcs resource create flowfirst-p1-res systemd:flowfirst-process1 \
    op monitor interval=15s timeout=20s
sudo pcs resource clone flowfirst-p1-res clone-max=3 clone-node-max=1

# 5. Order constraints: Processes start before HAProxy routes traffic
echo "[5/5] Setting startup ordering constraints..."
sudo pcs constraint order start flowfirst-p1-res-clone then start vip-haproxy-group

echo ""
echo "=================================================================="
echo " Multi-node resources successfully configured!"
echo " Current cluster status:"
echo "=================================================================="
sudo pcs status
