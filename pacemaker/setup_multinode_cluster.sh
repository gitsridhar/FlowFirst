#!/usr/bin/env bash
set -euo pipefail

# ==============================================================================
# FlowFirst - Multi-Node Pacemaker Cluster Setup Script (3 Nodes)
# Run on NODE 1 ONLY after running the prerequisites on all three nodes.
#
# PRE-REQUISITE — run these commands on EVERY node (node1, node2, node3)
# before executing this script:
#
#   sudo ./pacemaker/setup_multinode_cluster.sh --prepare-node
#
# That flag installs packages, starts pcsd, opens firewall ports, sets the
# hacluster password, and adds /etc/hosts entries.  It can be run safely
# multiple times.  Once all three nodes are prepared, run this script without
# flags on node1 to complete the cluster initialisation.
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

CLUSTER_NAME="${CLUSTER_NAME:-flowfirst_cluster}"
NODE1_NAME="${NODE1_NAME:-node1}"
NODE1_IP="${NODE1_IP:-}"
NODE2_NAME="${NODE2_NAME:-node2}"
NODE2_IP="${NODE2_IP:-}"
NODE3_NAME="${NODE3_NAME:-node3}"
NODE3_IP="${NODE3_IP:-}"
HACLUSTER_PASS="${HACLUSTER_PASS:-hacluster123}"

# Validate required IP variables (must come from .env — no hardcoded fallbacks)
for _var in NODE1_IP NODE2_IP NODE3_IP; do
    _val="${!_var:-}"
    if [[ -z "${_val}" ]] || [[ "${_val}" =~ ^\$\{ ]]; then
        echo "ERROR: ${_var} is not set. Set it in /opt/flowfirst/.env and re-run."
        exit 1
    fi
done

# Cluster ports that must be open on every node
# 2224/tcp  pcsd REST API  (pcs host auth)
# 3121/tcp  pacemaker remote
# 5403/tcp  corosync qdevice
# 5404/udp  corosync multicast
# 5405/udp  corosync token ring
CLUSTER_TCP_PORTS=(2224 3121 5403)
CLUSTER_UDP_PORTS=(5404 5405)

ARCH_DNF=$(uname -m)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
info()  { echo "  [INFO]  $*"; }
ok()    { echo "  [OK]    $*"; }
warn()  { echo "  [WARN]  $*"; }
fail()  { echo "  [FAIL]  $*"; exit 1; }

# ---------------------------------------------------------------------------
# enable_ha_repo — enable / add HA + AppStream + BaseOS repos
# ---------------------------------------------------------------------------
enable_ha_repo() {
    for repo_id in \
        "rhel-9-for-${ARCH_DNF}-highavailability-rpms" \
        "highavailability" \
        "almalinux-highavailability" \
        "rocky-ha"
    do
        if sudo dnf repolist --all 2>/dev/null | grep -q "^${repo_id}"; then
            info "Enabling repo: ${repo_id}"
            sudo dnf config-manager --set-enabled "${repo_id}" 2>/dev/null || \
            sudo dnf config-manager --enable      "${repo_id}" 2>/dev/null || true
            return 0
        fi
    done

    # Fallback: add CentOS Stream 9 HA + AppStream + BaseOS
    info "No HA repo found via config-manager; adding CentOS Stream 9 repos..."
    sudo tee /etc/yum.repos.d/centos-ha.repo > /dev/null << 'HAREPO'
[centos-stream9-ha]
name=CentOS Stream 9 - HighAvailability
baseurl=https://mirror.stream.centos.org/9-stream/HighAvailability/$basearch/os/
gpgcheck=1
enabled=1
gpgkey=https://www.centos.org/keys/RPM-GPG-KEY-CentOS-Official
sslverify=1
metadata_expire=300

[centos-stream9-appstream]
name=CentOS Stream 9 - AppStream
baseurl=https://mirror.stream.centos.org/9-stream/AppStream/$basearch/os/
gpgcheck=1
enabled=1
gpgkey=https://www.centos.org/keys/RPM-GPG-KEY-CentOS-Official
sslverify=1
metadata_expire=300

[centos-stream9-baseos]
name=CentOS Stream 9 - BaseOS
baseurl=https://mirror.stream.centos.org/9-stream/BaseOS/$basearch/os/
gpgcheck=1
enabled=1
gpgkey=https://www.centos.org/keys/RPM-GPG-KEY-CentOS-Official
sslverify=1
metadata_expire=300
HAREPO
}

# ---------------------------------------------------------------------------
# prepare_node — install packages, open firewall, set password, /etc/hosts
# Run this on EVERY node before running the cluster init on node1.
# ---------------------------------------------------------------------------
prepare_node() {
    echo "============================================================"
    echo " Preparing this node for Pacemaker cluster membership"
    echo "============================================================"

    # 1. Enable HA repo and install packages
    echo "[P1/5] Enabling HA repo and installing cluster packages..."
    enable_ha_repo
    sudo dnf install -y pcs pacemaker corosync fence-agents-all haproxy

    # 1b. Automatically generate FlowFirst HAProxy configuration if setup script is available
    if [[ -x "${ROOT_DIR}/haproxy/setup_haproxy.sh" ]]; then
        echo "  [INFO] Generating FlowFirst HAProxy configuration..."
        sudo "${ROOT_DIR}/haproxy/setup_haproxy.sh" || true
    fi

    # 2. Enable and start pcsd — must be running before pcs host auth
    echo "[P2/5] Enabling and starting pcsd (TCP 2224)..."
    sudo systemctl enable --now pcsd
    # Give pcsd a moment to bind its port
    sleep 2
    if sudo systemctl is-active --quiet pcsd; then
        ok "pcsd is running"
    else
        fail "pcsd failed to start — check: journalctl -u pcsd"
    fi

    # 3. Set hacluster password — must be identical on all nodes
    echo "[P3/5] Setting hacluster password..."
    echo "hacluster:${HACLUSTER_PASS}" | sudo chpasswd
    ok "hacluster password set"

    # 4. Open required firewall ports
    echo "[P4/5] Opening cluster ports in firewalld..."
    if sudo systemctl is-active --quiet firewalld; then
        for port in "${CLUSTER_TCP_PORTS[@]}"; do
            sudo firewall-cmd --permanent --add-port="${port}/tcp" 2>/dev/null || true
        done
        for port in "${CLUSTER_UDP_PORTS[@]}"; do
            sudo firewall-cmd --permanent --add-port="${port}/udp" 2>/dev/null || true
        done
        # pcs ships a predefined firewalld service — use it if available
        sudo firewall-cmd --permanent --add-service=high-availability 2>/dev/null || true
        sudo firewall-cmd --reload
        ok "Firewall ports opened: TCP ${CLUSTER_TCP_PORTS[*]}, UDP ${CLUSTER_UDP_PORTS[*]}"
    else
        warn "firewalld is not running — skipping firewall configuration"
        warn "Ensure ports 2224/tcp 3121/tcp 5403/tcp 5404/udp 5405/udp are reachable"
    fi

    # 5. Add all cluster nodes to /etc/hosts (idempotent)
    echo "[P5/5] Adding cluster nodes to /etc/hosts..."
    for entry in \
        "${NODE1_IP} ${NODE1_NAME}" \
        "${NODE2_IP} ${NODE2_NAME}" \
        "${NODE3_IP} ${NODE3_NAME}"
    do
        ip_part="${entry%% *}"
        name_part="${entry##* }"
        if grep -q "^${ip_part}[[:space:]]" /etc/hosts; then
            info "/etc/hosts already has ${ip_part} — skipping"
        else
            echo "${entry}" | sudo tee -a /etc/hosts > /dev/null
            ok "Added: ${entry}"
        fi
    done

    echo ""
    echo "============================================================"
    echo " Node preparation complete."
    echo " Run this on node2 and node3 as well, then run:"
    echo "   sudo ./pacemaker/setup_multinode_cluster.sh"
    echo " on node1 only to complete cluster initialisation."
    echo "============================================================"
}

# ---------------------------------------------------------------------------
# preflight_checks — verify node2 and node3 are reachable on port 2224
# before attempting pcs host auth
# ---------------------------------------------------------------------------
preflight_checks() {
    echo "[0/6] Pre-flight connectivity checks..."
    local all_ok=true

    for pair in "${NODE1_NAME}:${NODE1_IP}" "${NODE2_NAME}:${NODE2_IP}" "${NODE3_NAME}:${NODE3_IP}"; do
        local name="${pair%%:*}"
        local ip="${pair##*:}"

        # TCP port 2224 (pcsd)
        if timeout 5 bash -c ">/dev/tcp/${ip}/2224" 2>/dev/null; then
            ok "  ${name} (${ip}):2224 reachable"
        else
            warn "  ${name} (${ip}):2224 NOT reachable"
            warn "  → Run: sudo ./pacemaker/setup_multinode_cluster.sh --prepare-node"
            warn "    on ${name} and ensure firewalld allows port 2224/tcp"
            all_ok=false
        fi
    done

    if [[ "${all_ok}" == "false" ]]; then
        echo ""
        echo "  ── Pre-flight failed ──────────────────────────────────────────"
        echo "  One or more nodes are not reachable on port 2224 (pcsd)."
        echo ""
        echo "  Run the following on EVERY node that failed the check above:"
        echo ""
        echo "    cd /opt/flowfirst"
        echo "    sudo ./pacemaker/setup_multinode_cluster.sh --prepare-node"
        echo ""
        echo "  That command installs packages, starts pcsd, opens firewall"
        echo "  ports, sets the hacluster password, and updates /etc/hosts."
        echo "  Once all three nodes pass the check, re-run this script."
        echo "  ───────────────────────────────────────────────────────────────"
        exit 1
    fi

    ok "All 3 nodes reachable on port 2224 — proceeding with cluster init"
}

# ---------------------------------------------------------------------------
# Main — dispatch on --prepare-node flag or run full cluster init
# ---------------------------------------------------------------------------
if [[ "${1:-}" == "--prepare-node" ]]; then
    prepare_node
    exit 0
fi

# Full cluster init — runs on node1 only
echo "=================================================================="
echo " Setting up 3-Node Pacemaker High Availability Cluster"
echo " Cluster: ${CLUSTER_NAME}"
echo " Node 1:  ${NODE1_NAME} (${NODE1_IP})"
echo " Node 2:  ${NODE2_NAME} (${NODE2_IP})"
echo " Node 3:  ${NODE3_NAME} (${NODE3_IP})"
echo "=================================================================="

# 0. Pre-flight: verify all nodes reachable on port 2224
preflight_checks

# 1. Ensure this node's own packages are installed (idempotent)
echo "[1/6] Ensuring cluster packages installed on this node..."
enable_ha_repo
sudo dnf install -y pcs pacemaker corosync fence-agents-all haproxy
if [[ -x "${ROOT_DIR}/haproxy/setup_haproxy.sh" ]]; then
    sudo "${ROOT_DIR}/haproxy/setup_haproxy.sh" || true
fi
sudo systemctl enable --now pcsd

# 2. Set hacluster password on this node
echo "[2/6] Setting hacluster password on this node..."
echo "hacluster:${HACLUSTER_PASS}" | sudo chpasswd

# 3. Authenticate all 3 cluster nodes
echo "[3/6] Authenticating all 3 nodes with pcs host auth..."
sudo pcs host auth \
    "${NODE1_NAME}" addr="${NODE1_IP}" \
    "${NODE2_NAME}" addr="${NODE2_IP}" \
    "${NODE3_NAME}" addr="${NODE3_IP}" \
    -u hacluster -p "${HACLUSTER_PASS}"

# 4. Create and initialize 3-node Corosync cluster
echo "[4/6] Initializing Corosync/Pacemaker cluster with 3 nodes..."
sudo pcs cluster setup "${CLUSTER_NAME}" \
    "${NODE1_NAME}" addr="${NODE1_IP}" \
    "${NODE2_NAME}" addr="${NODE2_IP}" \
    "${NODE3_NAME}" addr="${NODE3_IP}" \
    --force

sudo pcs cluster start --all
sudo pcs cluster enable --all

# 5. Wait for cluster to reach quorum
echo "[5/6] Waiting for cluster quorum..."
for i in $(seq 1 12); do
    if sudo pcs status 2>/dev/null | grep -q "partition with quorum"; then
        ok "Cluster has quorum"
        break
    fi
    info "Waiting for quorum... (${i}/12)"
    sleep 5
done
if ! sudo pcs status 2>/dev/null | grep -q "partition with quorum"; then
    warn "Cluster did not reach quorum within 60 s — check: sudo pcs status"
fi

# 6. Cluster properties
echo "[6/6] Configuring cluster properties..."
sudo pcs property set stonith-enabled=false

echo ""
echo "=================================================================="
echo " 3-Node Cluster setup successfully initialized!"
echo ""
sudo pcs status 2>/dev/null || true
echo ""
echo " Run ./pacemaker/configure_multinode_resources.sh to deploy VIP,"
echo " HAProxy, and cloned FlowFirst process resources."
echo "=================================================================="
