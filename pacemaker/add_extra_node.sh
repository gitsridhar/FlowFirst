#!/usr/bin/env bash
set -euo pipefail

# ==============================================================================
# FlowFirst - add an extra core node (extramgmtnode<N>) to the running Pacemaker cluster.
#
#   sudo ./pacemaker/add_extra_node.sh extramgmtnode1 [--dry-run]
#
# Run on mgmtnode1 (any existing cluster member) AFTER:
#   - sudo ./pacemaker/setup_multinode_cluster.sh --prepare-node   was run on the NEW node
#   - sudo ./systemd/install_services.sh /opt/flowfirst            was run on the NEW node, and its
#     flowfirst-process1..4 units were disabled (Pacemaker owns them)
#   - sudo ./haproxy/setup_haproxy.sh                              was run on the NEW node (config only)
#
# What it does (each step is skipped when already done, so a re-run is safe):
#   1. pcs host auth <name> addr=<ip>            (hacluster password from HACLUSTER_PASS)
#   2. pcs cluster node add <name> addr=<ip> --start --enable
#   3. raises clone-max of the four process clones from the current value to 3 + EXTRA_COUNT
#
# It never touches the VIP/HAProxy group: that group is not cloned, Pacemaker simply may place it on the
# new node too once HAProxy is configured there. It never starts haproxy.service directly.
# ==============================================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

if [[ -f "${ROOT_DIR}/.env" ]]; then
    set -a
    # shellcheck disable=SC1091
    source "${ROOT_DIR}/.env"
    set +a
fi
# shellcheck disable=SC1091
source "${ROOT_DIR}/scripts/extra_nodes_lib.sh"
extra_nodes_load || exit 1

NEW_NAME="${1:-}"
DRY_RUN=0
[ "${2:-}" = "--dry-run" ] && DRY_RUN=1
[[ "${NEW_NAME}" =~ ^extramgmtnode[0-9]+$ ]] || { echo "usage: $0 extramgmtnode<N> [--dry-run]" >&2; exit 2; }

NEW_IP=""
for i in "${!EXTRA_NAMES[@]}"; do
    [ "${EXTRA_NAMES[$i]}" = "${NEW_NAME}" ] && NEW_IP="${EXTRA_IPS[$i]}"
done
[ -n "${NEW_IP}" ] || { echo "ERROR: ${NEW_NAME} is not listed in EXTRA_NODES in .env." >&2; exit 1; }

HACLUSTER_PASS="${HACLUSTER_PASS:-hacluster123}"
CLONE_MAX=$((3 + EXTRA_COUNT))
PROCESS_CLONES=(flowfirst-p1-res-clone flowfirst-p2-res-clone flowfirst-p3-res-clone flowfirst-p4-res-clone)

run() { if [ "${DRY_RUN}" = 1 ]; then echo "  [dry-run] $*"; else "$@"; fi; }

[ "${DRY_RUN}" = 1 ] || [ "$(id -u)" = 0 ] || { echo "ERROR: run as root (sudo)." >&2; exit 1; }
command -v pcs >/dev/null 2>&1 || { echo "ERROR: pcs not found on this node." >&2; exit 1; }

echo "[1/4] Pre-flight: ${NEW_NAME} (${NEW_IP}) must answer on pcsd port 2224..."
if ! timeout 5 bash -c ">/dev/tcp/${NEW_IP}/2224" 2>/dev/null; then
    echo "ERROR: ${NEW_IP}:2224 is not reachable. On the new node run:"
    echo "    sudo ./pacemaker/setup_multinode_cluster.sh --prepare-node"
    exit 1
fi

if pcs status nodes 2>/dev/null | grep -qw "${NEW_NAME}"; then
    echo "[2/4] ${NEW_NAME} is already a cluster member — skipping auth/add."
else
    echo "[2/4] Authenticating and adding ${NEW_NAME}..."
    run pcs host auth "${NEW_NAME}" addr="${NEW_IP}" -u hacluster -p "${HACLUSTER_PASS}"
    run pcs cluster node add "${NEW_NAME}" addr="${NEW_IP}" --start --enable
fi

echo "[3/4] Raising clone-max to ${CLONE_MAX} on the process clones..."
for c in "${PROCESS_CLONES[@]}"; do
    run pcs resource update "${c}" clone-max="${CLONE_MAX}"
done

echo "[4/4] Result:"
if [ "${DRY_RUN}" = 1 ]; then
    echo "  [dry-run] pcs status"
else
    pcs status
fi
echo ""
echo "Expect: ${NEW_NAME} Online, and Started on it: flowfirst-p1-res .. flowfirst-p4-res."
echo "Quorum now needs $(( (CLONE_MAX) / 2 + 1 )) of ${CLONE_MAX} nodes (remotenode1, the remote worker, is not a member)."
