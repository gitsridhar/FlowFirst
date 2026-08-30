#!/usr/bin/env bash
set -euo pipefail

# ==============================================================================
# FlowFirst - Multi-Node Pacemaker Cluster Setup Script (3 Nodes)
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

CLUSTER_NAME="${1:-${CLUSTER_NAME:-flowfirst_cluster}}"
NODE1_NAME="${2:-${NODE1_NAME:-node1}}"
NODE1_IP="${3:-${NODE1_IP:-192.168.1.101}}"
NODE2_NAME="${4:-${NODE2_NAME:-node2}}"
NODE2_IP="${5:-${NODE2_IP:-192.168.1.102}}"
NODE3_NAME="${6:-${NODE3_NAME:-node3}}"
NODE3_IP="${7:-${NODE3_IP:-192.168.1.103}}"

echo "=================================================================="
echo " Setting up 3-Node Pacemaker High Availability Cluster"
echo " Cluster: ${CLUSTER_NAME}"
echo " Node 1:  ${NODE1_NAME} (${NODE1_IP})"
echo " Node 2:  ${NODE2_NAME} (${NODE2_IP})"
echo " Node 3:  ${NODE3_NAME} (${NODE3_IP})"
echo "=================================================================="

# 1. Enable the High Availability repo and install Pacemaker, Corosync, PCS
#
# pcs / pacemaker / corosync are in the HA add-on repo, which is DISABLED by
# default on all RHEL 9 variants.  The repo ID differs by distro:
#   RHEL 9 (subscription) : rhel-9-for-x86_64-highavailability-rpms
#                           (or rhel-9-for-aarch64-highavailability-rpms)
#   CentOS Stream 9        : highavailability   (mirrorlist-based)
#   AlmaLinux 9            : almalinux-highavailability
#   Rocky Linux 9          : rocky-ha
#
# Strategy: try dnf config-manager for each known ID; fall back to adding a
# direct CentOS Stream 9 HA repo entry if none of the above are present.
echo "[1/6] Enabling High Availability repo and installing cluster packages..."

ARCH_DNF=$(uname -m)

enable_ha_repo() {
    # Check which known HA repo ID exists on this system
    for repo_id in \
        "rhel-9-for-${ARCH_DNF}-highavailability-rpms" \
        "highavailability" \
        "almalinux-highavailability" \
        "rocky-ha"
    do
        if sudo dnf repolist --all 2>/dev/null | grep -q "^${repo_id}"; then
            echo "  Enabling repo: ${repo_id}"
            sudo dnf config-manager --set-enabled "${repo_id}" 2>/dev/null || \
            sudo dnf config-manager --enable "${repo_id}" 2>/dev/null || true
            return 0
        fi
    done

    # No known repo found — add the CentOS Stream 9 HA mirrorlist as fallback
    echo "  No HA repo found via config-manager; adding CentOS Stream 9 HA repo..."
    sudo tee /etc/yum.repos.d/centos-ha.repo > /dev/null << 'HAREPO'
[centos-stream9-ha]
name=CentOS Stream 9 - HighAvailability
baseurl=https://mirror.stream.centos.org/9-stream/HighAvailability/$basearch/os/
gpgcheck=1
enabled=1
gpgkey=https://www.centos.org/keys/RPM-GPG-KEY-CentOS-Official
sslverify=1
metadata_expire=300
HAREPO
}

enable_ha_repo

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
# For demo/software HA (disable STONITH):
sudo pcs property set stonith-enabled=false

echo ""
echo "=================================================================="
echo " 3-Node Cluster setup successfully initialized!"
echo " Run ./pacemaker/configure_multinode_resources.sh to deploy VIP,"
echo " HAProxy, and cloned FlowFirst process resources."
echo "=================================================================="
