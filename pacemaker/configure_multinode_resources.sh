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

VIP="${1:-${FLOWFIRST_VIP:-}}"
VIP_NIC="${2:-${VIP_NIC:-}}"
VIP_CIDR_NETMASK="${3:-${VIP_CIDR_NETMASK:-24}}"

# Validate VIP is set (must come from .env)
if [[ -z "${VIP}" ]] || [[ "${VIP}" =~ ^\$\{ ]]; then
    echo "ERROR: FLOWFIRST_VIP is not set. Set it in /opt/flowfirst/.env and re-run."
    exit 1
fi

echo "=================================================================="
echo " Configuring Multi-Node Pacemaker Resources"
echo " Virtual IP (VIP):    ${VIP}"
echo " Network Interface:   ${VIP_NIC}"
echo " CIDR Netmask:        ${VIP_CIDR_NETMASK}"
echo "=================================================================="

# Pre-flight: verify the NIC exists on this node before touching the cluster
echo "[0/5] Pre-flight: verifying network interface '${VIP_NIC}'..."
if ! ip link show "${VIP_NIC}" >/dev/null 2>&1; then
    echo ""
    echo "ERROR: Network interface '${VIP_NIC}' does not exist on this node."
    echo ""
    echo "  Available interfaces:"
    ip -o link show | awk -F': ' '{print "    " $2}' | grep -v '^    lo$'
    echo ""
    echo "  Fix: set VIP_NIC to the correct interface name in .env and re-run."
    echo "  Auto-detect cluster NIC:  ip route get ${VIP} | awk '{print \$5; exit}'"
    echo "  List all NICs:            ip -o link show | awk -F': ' '{print \$2}'"
    exit 1
fi
echo "  Interface '${VIP_NIC}' found — proceeding."
echo ""

# 1. Clean up prior resources if present
echo "[1/5] Removing existing cluster resources..."
for res in vip-haproxy-group flowfirst-vip haproxy-clone haproxy-res flowfirst-p1-res-clone flowfirst-p2-res-clone flowfirst-p3-res-clone flowfirst-p4-res-clone flowfirst-p1-res flowfirst-p2-res flowfirst-p3-res flowfirst-p4-res; do
    if sudo pcs resource status "${res}" >/dev/null 2>&1; then
        echo "  Deleting resource ${res}..."
        sudo pcs resource delete "${res}" --force || true
    fi
done

# Stop any orphan standalone systemd units or manually launched background processes
echo "  Stopping any standalone/unmanaged process instances on cluster nodes..."
for node in "${NODE1_IP}" "${NODE2_IP}" "${NODE3_IP}"; do
    ssh -o BatchMode=yes -o ConnectTimeout=3 "${node}" \
        "sudo systemctl stop flowfirst-process1 flowfirst-process2 flowfirst-process3 flowfirst-process4 2>/dev/null || true; \
         sudo pkill -f 'python.*process[1-4]\.py' 2>/dev/null || true" 2>/dev/null || true
done

# 2. Configure Virtual IP (IPaddr2 resource agent)
echo "[2/5] Creating Virtual IP resource (flowfirst-vip)..."
sudo pcs resource create flowfirst-vip ocf:heartbeat:IPaddr2 \
    ip="${VIP}" \
    cidr_netmask="${VIP_CIDR_NETMASK}" \
    nic="${VIP_NIC}" \
    op monitor interval=10s timeout=20s

# 3. Ensure HAProxy config is generated and valid across all 3 nodes before registering with Pacemaker
echo "[3/5] Verifying HAProxy configuration across all cluster nodes..."
for node in "${NODE1_IP}" "${NODE2_IP}" "${NODE3_IP}"; do
    echo "  Configuring and verifying HAProxy on ${node}..."
    ssh -o BatchMode=yes -o ConnectTimeout=5 "${node}" \
        "cd /opt/flowfirst && sudo ./haproxy/setup_haproxy.sh" 2>/dev/null || true
done

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
