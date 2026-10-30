#!/usr/bin/env bash
set -euo pipefail

# ==============================================================================
# FlowFirst - generate secrets on node1 and push .env to all other nodes.
#
# Run ONCE on node1 after copying src/config/.env.node1 to /opt/flowfirst/.env:
#
#   sudo ./scripts/deploy_secrets.sh
#
# What it does:
#   1. Calls generate_secrets.sh --write to fill in the four password placeholders
#      (RABBITMQ_PASSWORD, MARIADB_PASSWORD, SWIFT_KEY, HAPROXY_STATS_PASS).
#      SWIFT_HASH_PATH_PREFIX / SUFFIX are already set in the .env file and are
#      left untouched.
#   2. Pushes the resulting .env to nodes 2, 3 and 4 over SSH (as flowadmin).
#   3. Patches NODE_NAME and CURRENT_NODE_IP on each remote node so the two
#      per-node values stay correct after the copy.
#
# Prerequisites (all done in Phase 0):
#   - /opt/flowfirst/.env exists on THIS node (cp src/config/.env.node1 .env)
#   - SSH key-based access to the other nodes works (Phase 0 pre-step)
#   - flowadmin has passwordless sudo on all nodes (Phase 0 pre-step)
#
# The script never prints any secret value and never stores a secret in a
# temporary file that outlives the run.
# ==============================================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
ENV_FILE="${ROOT_DIR}/.env"

# ---------------------------------------------------------------------------
# Load the node IPs from .env so we know where to push
# ---------------------------------------------------------------------------
if [[ ! -f "${ENV_FILE}" ]]; then
    echo "ERROR: ${ENV_FILE} not found."
    echo "  Copy the per-node template first:  cp src/config/.env.node1 .env"
    exit 1
fi

# Read only the IP variables we need — deliberately NOT sourcing the whole file
# (which would expose secrets into the environment).
_get() { grep "^${1}=" "${ENV_FILE}" 2>/dev/null | cut -d= -f2-; }
NODE2_IP="$(_get NODE2_IP)"
NODE3_IP="$(_get NODE3_IP)"
NODE4_IP="$(_get NODE4_IP)"
SSH_USER="${DEPLOY_SSH_USER:-flowadmin}"

for _var in NODE2_IP NODE3_IP NODE4_IP; do
    [[ -n "${!_var}" ]] || { echo "ERROR: ${_var} is not set in ${ENV_FILE}."; exit 1; }
done

echo "=================================================================="
echo " FlowFirst — generate secrets and deploy .env to all nodes"
echo " This node (node1): secrets will be generated here"
echo " Targets: ${NODE2_IP} (node2)  ${NODE3_IP} (node3)  ${NODE4_IP} (node4)"
echo " SSH user: ${SSH_USER}  (override: DEPLOY_SSH_USER=<user> ./deploy_secrets.sh)"
echo "=================================================================="

# ---------------------------------------------------------------------------
# Step 1: Generate passwords on this node
# ---------------------------------------------------------------------------
echo ""
echo "[1/2] Generating secrets on node1..."
"${SCRIPT_DIR}/generate_secrets.sh" --write --file "${ENV_FILE}"
chmod 600 "${ENV_FILE}"

# ---------------------------------------------------------------------------
# Step 2: Push to nodes 2, 3, 4 and restore per-node identity on each
# ---------------------------------------------------------------------------
echo ""
echo "[2/2] Pushing .env to remote nodes..."

declare -A NODE_MAP
NODE_MAP["${NODE2_IP}"]="2"
NODE_MAP["${NODE3_IP}"]="3"
NODE_MAP["${NODE4_IP}"]="4"

for node_ip in "${NODE2_IP}" "${NODE3_IP}" "${NODE4_IP}"; do
    nodenum="${NODE_MAP[${node_ip}]}"
    echo "  → node${nodenum} (${node_ip})"

    # Copy the file
    scp -q "${ENV_FILE}" "${SSH_USER}@${node_ip}:/opt/flowfirst/.env"

    # Restore the two per-node lines — the scp above overwrote them with node1's values
    # shellcheck disable=SC2087
    ssh -T "${SSH_USER}@${node_ip}" <<REMOTE
set -e
sed -i 's/^NODE_NAME=.*/NODE_NAME=node${nodenum}/' /opt/flowfirst/.env
sed -i 's/^CURRENT_NODE_IP=.*/CURRENT_NODE_IP=${node_ip}/' /opt/flowfirst/.env
chmod 600 /opt/flowfirst/.env
REMOTE

    echo "    node${nodenum}: deployed and per-node identity set"
done

echo ""
echo "=================================================================="
echo " Done. All four nodes have matching secrets."
echo " SWIFT_HASH_PATH_* were pre-set in the .env files and were not changed."
echo ""
echo " Next:"
echo "   • Delete the backup:  rm ${ENV_FILE}.bak"
echo "   • Continue with Phase 1 (MariaDB Galera)"
echo "=================================================================="
