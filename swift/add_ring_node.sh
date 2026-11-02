#!/usr/bin/env bash
set -euo pipefail

# ==============================================================================
# FlowFirst - add extra core nodes (EXTRA_NODES) to the Swift account/container/object rings.
#
#   sudo ./swift/add_ring_node.sh            on node1: add the devices, rebalance, show what changed
#   sudo ./swift/add_ring_node.sh --push     ... and copy the rings to every other node over SSH
#   sudo ./swift/add_ring_node.sh --check    read-only: list which extra nodes are / are not in the rings
#
# Why this exists: swift/setup_swift.sh builds the rings only once (a fixed seed makes them identical on
# the first three nodes) and then KEEPS them. A new node therefore has to be added to the existing rings
# on ONE node, and the resulting files copied everywhere. Never run setup_swift.sh on the new node before
# it has received these rings: it would refuse, because building fresh rings there would split the cluster.
#
# Zone for extramgmtnode<N> is 3+N (extramgmtnode1 = zone 4). Zones are never reused, so a removed node's
# zone number stays retired. SWIFT_REPLICAS stays at its value (default 3): with 4 zones every object
# still has 3 copies, spread over 3 of the 4 nodes.
#
# Data moves gradually: Swift's min_part_hours (1 in these rings) limits how many partitions may move per
# rebalance, and the replicators then copy the data in the background. Run the script again after an hour
# if you want more partitions to move; every run is safe to repeat.
# ==============================================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

if [ -f "${ROOT_DIR}/.env" ]; then
    set -a
    # shellcheck disable=SC1091
    source "${ROOT_DIR}/.env"
    set +a
fi
# shellcheck disable=SC1091
source "${ROOT_DIR}/scripts/extra_nodes_lib.sh"
extra_nodes_load || exit 1

MODE=add
case "${1:-}" in
    "")      ;;
    --push)  MODE=push ;;
    --check) MODE=check ;;
    *)       echo "usage: $0 [--push|--check]" >&2; exit 2 ;;
esac

SWIFT_DEVICE="${SWIFT_DEVICE:-flowfirst}"
SWIFT_ACCOUNT_PORT="${SWIFT_ACCOUNT_PORT:-6202}"
SWIFT_CONTAINER_PORT="${SWIFT_CONTAINER_PORT:-6201}"
SWIFT_OBJECT_PORT="${SWIFT_OBJECT_PORT:-6200}"
SWIFT_RING_SEED="${SWIFT_RING_SEED:-12345}"
SSH_USER="${DEPLOY_SSH_USER:-flowadmin}"
RINGS=(account container object)

[ "${EXTRA_COUNT}" -gt 0 ] || { echo "ERROR: EXTRA_NODES is blank in .env — nothing to add."; exit 1; }
command -v swift-ring-builder >/dev/null 2>&1 || { echo "ERROR: swift-ring-builder not found (run on a node where Swift is installed)."; exit 1; }
for r in "${RINGS[@]}"; do
    [ -f "/etc/swift/${r}.builder" ] || {
        echo "ERROR: /etc/swift/${r}.builder is missing. Run this on the node where setup_swift.sh built the rings (node1)."
        exit 1; }
done

port_for() { case "$1" in account) echo "${SWIFT_ACCOUNT_PORT}" ;; container) echo "${SWIFT_CONTAINER_PORT}" ;; object) echo "${SWIFT_OBJECT_PORT}" ;; esac; }
in_ring() { swift-ring-builder "/etc/swift/$1.builder" 2>/dev/null | grep -qE "[[:space:]]$2[[:space:]]+${3}[[:space:]]"; }

if [ "${MODE}" = check ]; then
    for i in "${!EXTRA_NAMES[@]}"; do
        for r in "${RINGS[@]}"; do
            if in_ring "${r}" "${EXTRA_IPS[$i]}" "$(port_for "${r}")"; then
                echo "  ${EXTRA_NAMES[$i]}: in ${r} ring"
            else
                echo "  ${EXTRA_NAMES[$i]}: NOT in ${r} ring"
            fi
        done
    done
    exit 0
fi

[ "$(id -u)" = 0 ] || { echo "ERROR: run as root (sudo)." >&2; exit 1; }

changed=0
for i in "${!EXTRA_NAMES[@]}"; do
    name="${EXTRA_NAMES[$i]}" ip="${EXTRA_IPS[$i]}" zone="$(extra_node_id "${EXTRA_NAMES[$i]}")"
    for r in "${RINGS[@]}"; do
        port="$(port_for "${r}")"
        if in_ring "${r}" "${ip}" "${port}"; then
            echo "  ${name}: already in ${r} ring (zone ${zone}) — skipped"
        else
            swift-ring-builder "/etc/swift/${r}.builder" add --region 1 --zone "${zone}" \
                --ip "${ip}" --port "${port}" --device "${SWIFT_DEVICE}" --weight 100 >/dev/null
            echo "  ${name}: added to ${r} ring (zone ${zone}, ${ip}:${port})"
            changed=1
        fi
    done
done

if [ "${changed}" = 1 ]; then
    for r in "${RINGS[@]}"; do
        # exit status 1 = "no change possible yet" (min_part_hours); that is not an error for us
        swift-ring-builder "/etc/swift/${r}.builder" rebalance -s "${SWIFT_RING_SEED}" || {
            rc=$?; [ "${rc}" -eq 1 ] || { echo "ERROR: rebalance of the ${r} ring failed (rc=${rc})." >&2; exit "${rc}"; }; }
    done
fi
chown swift:swift /etc/swift/*.builder /etc/swift/*.ring.gz 2>/dev/null || true

echo ""
echo "Ring checksums on this node (must be identical on every node afterwards):"
(cd /etc/swift && md5sum account.ring.gz container.ring.gz object.ring.gz | sed 's/^/   /')

if [ "${MODE}" = push ]; then
    echo ""
    targets=("${NODE2_IP}" "${NODE3_IP}" "${EXTRA_IPS[@]}")
    for ip in "${targets[@]}"; do
        [ "${ip}" = "${CURRENT_NODE_IP:-}" ] && continue
        echo "  → ${ip}"
        for r in "${RINGS[@]}"; do
            for f in "${r}.builder" "${r}.ring.gz"; do
                ssh -o BatchMode=yes -o ConnectTimeout=5 "${SSH_USER}@${ip}" \
                    "sudo mkdir -p /etc/swift && sudo tee /etc/swift/${f} >/dev/null && sudo chown swift:swift /etc/swift/${f} 2>/dev/null || true" \
                    < "/etc/swift/${f}"
            done
        done
    done
    echo "Rings copied. Swift services pick up a changed ring file by themselves (no restart needed)."
else
    echo ""
    echo "Not pushed. Copy account/container/object .builder and .ring.gz to /etc/swift on every other node,"
    echo "or re-run with --push."
fi
