#!/usr/bin/env bash
set -euo pipefail

# ==============================================================================
# FlowFirst - Pacemaker Resource Configuration Script
# Configures Process 1, 2, 3, 4 as Pacemaker cluster resources with ordering,
# colocation, and resource grouping.
# ==============================================================================

echo "=================================================================="
echo " Configuring Pacemaker Resources for FlowFirst Pipeline"
echo "=================================================================="

# 1. Clean up any existing FlowFirst pacemaker resources
echo "[1/4] Cleaning existing FlowFirst resources if present..."
for res in flowfirst-group flowfirst-p1-res flowfirst-p2-res flowfirst-p3-res flowfirst-p4-res; do
    if sudo pcs resource status "${res}" >/dev/null 2>&1; then
        echo "  Deleting existing resource ${res}..."
        sudo pcs resource delete "${res}" --force || true
    fi
done

# 2. Create systemd-based Pacemaker resources with monitoring intervals
echo "[2/4] Creating Pacemaker resources for all 4 processes..."

# Process 4: MariaDB persister (downstream sink)
sudo pcs resource create flowfirst-p4-res systemd:flowfirst-process4 \
    op monitor interval=15s timeout=20s \
    op start timeout=30s \
    op stop timeout=30s

# Process 3: Reflector & Forwarder
sudo pcs resource create flowfirst-p3-res systemd:flowfirst-process3 \
    op monitor interval=15s timeout=20s \
    op start timeout=30s \
    op stop timeout=30s

# Process 2: Examiner & Reflector
sudo pcs resource create flowfirst-p2-res systemd:flowfirst-process2 \
    op monitor interval=15s timeout=20s \
    op start timeout=30s \
    op stop timeout=30s

# Process 1: REST API Producer
sudo pcs resource create flowfirst-p1-res systemd:flowfirst-process1 \
    op monitor interval=15s timeout=20s \
    op start timeout=30s \
    op stop timeout=30s

# 3. Create a unified Resource Group
# Group enforces:
#  - Sequential startup: flowfirst-p4-res -> flowfirst-p3-res -> flowfirst-p2-res -> flowfirst-p1-res
#  - Sequential shutdown: flowfirst-p1-res -> flowfirst-p2-res -> flowfirst-p3-res -> flowfirst-p4-res
#  - Colocation of all 4 resources on the active cluster node
echo "[3/4] Creating resource group 'flowfirst-group'..."
sudo pcs resource group add flowfirst-group \
    flowfirst-p4-res \
    flowfirst-p3-res \
    flowfirst-p2-res \
    flowfirst-p1-res

# 4. Show cluster status
echo "[4/4] Verifying Pacemaker status..."
sudo pcs status
echo ""
echo "=================================================================="
echo " Pacemaker resources successfully configured!"
echo " Use 'sudo pcs status' to check cluster & resource health."
echo "=================================================================="
