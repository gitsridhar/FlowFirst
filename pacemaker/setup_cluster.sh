#!/usr/bin/env bash
set -euo pipefail

# ==============================================================================
# FlowFirst - Pacemaker High Availability Cluster Setup Script for RHEL 9.6
# ==============================================================================

CLUSTER_NAME="${1:-flowfirst_cluster}"
NODE_NAME="$(hostname -s)"
WORKING_DIR="$(pwd)"

echo "=================================================================="
echo " Setting up Pacemaker Cluster for FlowFirst Pipeline"
echo " Cluster Name: ${CLUSTER_NAME}"
echo " Node Name:    ${NODE_NAME}"
echo " Working Dir:  ${WORKING_DIR}"
echo "=================================================================="

# 1. Install Pacemaker and Corosync (High Availability Add-On on RHEL 9)
echo "[1/6] Installing Pacemaker, Corosync, and PCS..."
if command -v dnf >/dev/null 2>&1; then
    sudo dnf install -y pcs pacemaker corosync fence-agents-all
fi

# 2. Enable and Start pcsd daemon
echo "[2/6] Enabling and starting pcsd..."
sudo systemctl enable --now pcsd

# 3. Set hacluster user password if needed
echo "[3/6] Setting hacluster password..."
echo "hacluster:hacluster123" | sudo chpasswd

# 4. Authenticate node and setup cluster
echo "[4/6] Creating and starting cluster..."
sudo pcs host auth "${NODE_NAME}" -u hacluster -p hacluster123
sudo pcs cluster setup "${CLUSTER_NAME}" "${NODE_NAME}" --force
sudo pcs cluster start --all
sudo pcs cluster enable --all

# 5. Disable STONITH and Quorum policy for single-node / development deployments
echo "[5/6] Configuring cluster properties (stonith-disabled, no-quorum-policy)..."
sudo pcs property set stonith-enabled=false
sudo pcs property set no-quorum-policy=ignore

# 6. Ensure systemd unit files are installed and disabled from regular systemd boot
echo "[6/6] Ensuring systemd unit files are present for Pacemaker systemd provider..."
sudo "${WORKING_DIR}/systemd/install_services.sh" "${WORKING_DIR}"

# Pacemaker must manage the services, so disable them from systemd autostart
sudo systemctl disable flowfirst-process1.service flowfirst-process2.service flowfirst-process3.service flowfirst-process4.service flowfirst.target || true
sudo systemctl stop flowfirst-process1.service flowfirst-process2.service flowfirst-process3.service flowfirst-process4.service flowfirst.target || true

echo ""
echo "=================================================================="
echo " Cluster setup complete! Now running configure_pacemaker_resources.sh..."
echo "=================================================================="
"${WORKING_DIR}/pacemaker/configure_resources.sh"
