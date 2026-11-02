#!/usr/bin/env bash
set -euo pipefail

# ==============================================================================
# FlowFirst - join an extra core node (extramgmtnode<N>) to the RabbitMQ cluster.
#
#   sudo ./scripts/join_rabbitmq_cluster.sh [--seed NAME] [--dry-run]
#
# Run ON THE NEW NODE, after scripts/install_rabbitmq_rhel9.sh has installed and started RabbitMQ
# there (ReadMe Scenario 76). The install script does NOT cluster; this script does:
#   1. checks that this node is listed in EXTRA_NODES and that its short hostname equals NODE_NAME
#      (RabbitMQ names the node rabbit@<short hostname>)
#   2. makes every cluster member resolvable by name in /etc/hosts (idempotent)
#   3. opens the RabbitMQ ports in firewalld if it is running
#   4. copies the Erlang cookie from the seed node over SSH, straight into place (never printed,
#      never written to a temporary file)
#   5. stop_app -> reset -> join_cluster rabbit@<seed> -> start_app
#   6. with RABBITMQ_QUEUE_TYPE=quorum, adds this node as a member of the pipeline queues
#
# Already joined = nothing is changed (safe to re-run). The reset in step 5 only wipes THIS node's
# own (empty, freshly installed) RabbitMQ state; it never touches the seed.
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
source "${SCRIPT_DIR}/extra_nodes_lib.sh"
extra_nodes_load || exit 1

SEED_NAME="${NODE1_NAME:-node1}"
DRY_RUN=0
while [ $# -gt 0 ]; do
    case "$1" in
        --seed)    SEED_NAME="${2:?--seed needs a node name}"; shift 2 ;;
        --dry-run) DRY_RUN=1; shift ;;
        -h|--help) sed -n 4,22p "$0"; exit 0 ;;
        *)         echo "usage: $0 [--seed NAME] [--dry-run]" >&2; exit 2 ;;
    esac
done
[[ "${SEED_NAME}" =~ ^[A-Za-z0-9][A-Za-z0-9-]*$ ]] || { echo "ERROR: --seed must be a plain host name." >&2; exit 1; }

SSH_USER="${DEPLOY_SSH_USER:-flowadmin}"
COOKIE=/var/lib/rabbitmq/.erlang.cookie
RABBITMQ_QUEUE_TYPE="${RABBITMQ_QUEUE_TYPE:-classic}"
ME="${NODE_NAME:-}"
MY_IP="${CURRENT_NODE_IP:-}"

run() { if [ "${DRY_RUN}" = 1 ]; then echo "  [dry-run] $*"; else "$@"; fi; }

# ---------------------------------------------------------------------------
echo "[1/6] Checking this node..."
[ "${EXTRA_COUNT}" -gt 0 ] || { echo "ERROR: EXTRA_NODES is blank in .env."; exit 1; }
[ "$(extra_name_for_ip "${MY_IP}" || true)" = "${ME}" ] || {
    echo "ERROR: NODE_NAME / CURRENT_NODE_IP in .env must be one of the entries in EXTRA_NODES."; exit 1; }
[ "$(hostname -s)" = "${ME}" ] || {
    echo "ERROR: short hostname is '$(hostname -s)' but NODE_NAME is '${ME}'."
    echo "  RabbitMQ would be named rabbit@$(hostname -s), which the other nodes cannot resolve."
    echo "  Fix:  sudo hostnamectl set-hostname ${ME}   then restart rabbitmq-server and re-run."
    exit 1; }
[ "${DRY_RUN}" = 1 ] || [ "$(id -u)" = 0 ] || { echo "ERROR: run as root (sudo)." >&2; exit 1; }
command -v rabbitmqctl >/dev/null 2>&1 || { echo "ERROR: rabbitmqctl not found — run scripts/install_rabbitmq_rhel9.sh first." >&2; exit 1; }

if [ "${DRY_RUN}" = 0 ] && rabbitmqctl -q cluster_status 2>/dev/null | grep -q "rabbit@${SEED_NAME}"; then
    echo "  Already a member of the cluster with rabbit@${SEED_NAME} — nothing to join."
    rabbitmqctl -q cluster_status | sed -n '/Running Nodes/,/^$/p'
    exit 0
fi

# ---------------------------------------------------------------------------
echo "[2/6] Making every cluster member resolvable by name (/etc/hosts)..."
entries=("${NODE1_IP} ${NODE1_NAME:-node1}" "${NODE2_IP} ${NODE2_NAME:-node2}" "${NODE3_IP} ${NODE3_NAME:-node3}")
for i in "${!EXTRA_NAMES[@]}"; do entries+=("${EXTRA_IPS[$i]} ${EXTRA_NAMES[$i]}"); done
for entry in "${entries[@]}"; do
    if grep -q "^${entry%% *}[[:space:]]" /etc/hosts 2>/dev/null; then
        echo "  ${entry%% *} already in /etc/hosts"
    elif [ "${DRY_RUN}" = 1 ]; then
        echo "  [dry-run] would add: ${entry}"
    else
        echo "${entry}" >> /etc/hosts
        echo "  added: ${entry}"
    fi
done
getent hosts "${SEED_NAME}" >/dev/null || [ "${DRY_RUN}" = 1 ] || { echo "ERROR: ${SEED_NAME} does not resolve."; exit 1; }

# ---------------------------------------------------------------------------
echo "[3/6] Opening RabbitMQ ports (5672, 5671, 25672, 4369)..."
if command -v firewall-cmd >/dev/null 2>&1 && systemctl is-active --quiet firewalld; then
    for p in 5672 5671 25672 4369; do run firewall-cmd --permanent --add-port="${p}/tcp" >/dev/null; done
    run firewall-cmd --reload >/dev/null
else
    echo "  firewalld not running — skipped"
fi

# ---------------------------------------------------------------------------
echo "[4/6] Copying the Erlang cookie from ${SEED_NAME} (not printed)..."
if [ "${DRY_RUN}" = 1 ]; then
    echo "  [dry-run] ssh ${SSH_USER}@${SEED_NAME} sudo cat ${COOKIE} | install -m 400 -o rabbitmq -g rabbitmq /dev/stdin ${COOKIE}"
else
    systemctl stop rabbitmq-server
    umask 077
    if ! ssh -o BatchMode=yes -o ConnectTimeout=5 "${SSH_USER}@${SEED_NAME}" "sudo cat ${COOKIE}" \
            | install -m 400 -o rabbitmq -g rabbitmq /dev/stdin "${COOKIE}.new"; then
        rm -f "${COOKIE}.new"
        echo "ERROR: could not read the cookie from ${SEED_NAME} (needs SSH key access + passwordless sudo for ${SSH_USER})." >&2
        systemctl start rabbitmq-server || true
        exit 1
    fi
    [ -s "${COOKIE}.new" ] || { rm -f "${COOKIE}.new"; echo "ERROR: the cookie from ${SEED_NAME} is empty." >&2; exit 1; }
    mv -f "${COOKIE}.new" "${COOKIE}"
    systemctl start rabbitmq-server
fi

# ---------------------------------------------------------------------------
echo "[5/6] Joining rabbit@${SEED_NAME}..."
run rabbitmqctl stop_app
run rabbitmqctl reset
run rabbitmqctl join_cluster "rabbit@${SEED_NAME}"
run rabbitmqctl start_app

# ---------------------------------------------------------------------------
echo "[6/6] Queue replication (RABBITMQ_QUEUE_TYPE=${RABBITMQ_QUEUE_TYPE})..."
if [ "${RABBITMQ_QUEUE_TYPE}" = "quorum" ]; then
    run rabbitmq-queues grow "rabbit@${ME}" all --queue-pattern '^flow[123]_'
else
    echo "  classic queues are not replicated — nothing to do (the policy flowfirst-dlx is cluster-wide)"
fi

echo ""
echo "Done. Verify:  sudo rabbitmqctl cluster_status   (this node must appear under 'Running Nodes')"
echo "               sudo rabbitmqctl list_policies     (flowfirst-dlx must be listed)"
echo "Existing users and policies come from the cluster; the pipeline user is NOT re-created here."
