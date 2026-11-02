#!/usr/bin/env bash
set -euo pipefail

# ==============================================================================
# FlowFirst - orchestrator for adding an extra core node (ReadMe Scenario 76).
#
#   ./scripts/add_extra_node.sh extramgmtnode1 --plan            print the steps, change nothing
#   ./scripts/add_extra_node.sh extramgmtnode1                   run all steps in order
#   ./scripts/add_extra_node.sh extramgmtnode1 --only 5          run one step
#   ./scripts/add_extra_node.sh extramgmtnode1 --from 5          resume from a step after a failure
#   ./scripts/add_extra_node.sh extramgmtnode1 --dry-run         show every remote command, run none
#
# Run on node1 as the user that has SSH key access to every node (flowadmin by default,
# DEPLOY_SSH_USER overrides) with passwordless sudo on them.
#
# Prerequisites, done by hand BEFORE this script (ReadMe Scenario 76, steps A-D):
#   - the new host is installed (RHEL 9.6), has the static IP from EXTRA_NODES and the host name <name>
#   - chrony is synchronised, /opt/flowfirst is cloned, the venv + requirements are installed
#   - the line EXTRA_NODES=<name>:<ip> is already in node1's /opt/flowfirst/.env
#
# Every step is safe to repeat. Secrets are never printed; .env is copied with scp and its per-node
# lines are rewritten on the target. Existing nodes are changed one at a time, with a health wait.
# ==============================================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
ENV_FILE="${ROOT_DIR}/.env"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/extra_nodes_lib.sh"

NEW_NAME="${1:-}"
shift || true
MODE=all STEP=0 DRY_RUN=0
while [ $# -gt 0 ]; do
    case "$1" in
        --plan)    MODE=plan; shift ;;
        --only)    MODE=only; STEP="${2:?--only needs a step number}"; shift 2 ;;
        --from)    MODE=from; STEP="${2:?--from needs a step number}"; shift 2 ;;
        --dry-run) DRY_RUN=1; shift ;;
        *)         echo "usage: $0 extramgmtnode<N> [--plan|--only N|--from N] [--dry-run]" >&2; exit 2 ;;
    esac
done
[[ "${NEW_NAME}" =~ ^extramgmtnode[0-9]+$ ]] || { echo "usage: $0 extramgmtnode<N> [--plan|--only N|--from N] [--dry-run]" >&2; exit 2; }
[[ "${STEP}" =~ ^[0-9]+$ ]] || { echo "ERROR: step must be a number." >&2; exit 2; }
[ -f "${ENV_FILE}" ] || { echo "ERROR: ${ENV_FILE} not found." >&2; exit 1; }

# Read only the plain values we need (never source the file: it holds secrets)
_get() { grep "^${1}=" "${ENV_FILE}" 2>/dev/null | head -n1 | cut -d= -f2-; }
NODE1_IP="$(_get NODE1_IP)"; NODE2_IP="$(_get NODE2_IP)"; NODE3_IP="$(_get NODE3_IP)"; NODE4_IP="$(_get NODE4_IP)"
extra_nodes_load "$(_get EXTRA_NODES)" || exit 1
for v in NODE1_IP NODE2_IP NODE3_IP NODE4_IP; do [ -n "${!v}" ] || { echo "ERROR: ${v} is not set in .env." >&2; exit 1; }; done

NEW_IP=""
for i in "${!EXTRA_NAMES[@]}"; do [ "${EXTRA_NAMES[$i]}" = "${NEW_NAME}" ] && NEW_IP="${EXTRA_IPS[$i]}"; done
[ -n "${NEW_IP}" ] || { echo "ERROR: ${NEW_NAME} is not in EXTRA_NODES in ${ENV_FILE}." >&2; exit 1; }

SSH_USER="${DEPLOY_SSH_USER:-flowadmin}"
APP=/opt/flowfirst
EXISTING_CORE=("${NODE1_IP}" "${NODE2_IP}" "${NODE3_IP}")
for i in "${!EXTRA_NAMES[@]}"; do [ "${EXTRA_NAMES[$i]}" != "${NEW_NAME}" ] && EXISTING_CORE+=("${EXTRA_IPS[$i]}"); done
ALL_CORE=("${EXISTING_CORE[@]}" "${NEW_IP}")
ALL_HOSTS=("${ALL_CORE[@]}" "${NODE4_IP}")

say() { printf '\n== %s\n' "$*"; }
rsh() {   # rsh IP 'command'  -> run as SSH_USER on IP
    if [ "${DRY_RUN}" = 1 ]; then echo "  [dry-run] ssh ${SSH_USER}@$1 $2"; return 0; fi
    ssh -o BatchMode=yes -o ConnectTimeout=8 "${SSH_USER}@$1" "$2"
}
here() { if [ "${DRY_RUN}" = 1 ]; then echo "  [dry-run] $*"; else "$@"; fi; }
zk_ok() { rsh "$1" "for i in \$(seq 1 20); do [ \"\$(echo ruok | nc -w2 127.0.0.1 ${ZK_CLIENT_PORT:-2181})\" = imok ] && exit 0; sleep 3; done; exit 1"; }

STEPS=(
  "Check the new host, SSH and (re)sync the code on every node"
  "Distribute .env: EXTRA_NODES and ADMIN_TRUSTED_PROXY_CNS everywhere, full .env to ${NEW_NAME}"
  "ZooKeeper: add server.N on each existing member (rolling), then install on ${NEW_NAME}"
  "MariaDB Galera: install and join ${NEW_NAME}"
  "RabbitMQ: install and join ${NEW_NAME}"
  "Swift: add ${NEW_NAME} to the rings, push them, install Swift on ${NEW_NAME}"
  "HAProxy: regenerate the config on every core node"
  "Systemd units on ${NEW_NAME} (installed, then disabled: Pacemaker owns them)"
  "Pacemaker: prepare ${NEW_NAME}, add it to the cluster, raise clone-max"
  "Verify"
)

step_1() {
    for ip in "${ALL_HOSTS[@]}"; do
        rsh "${ip}" "true" || { echo "ERROR: cannot SSH to ${SSH_USER}@${ip} with a key." >&2; return 1; }
        echo "  ssh ok: ${ip}"
    done
    rsh "${NEW_IP}" "test -d ${APP}/.git && test -x ${APP}/.venv/bin/python" || { echo "ERROR: ${APP} (clone + .venv) is missing on ${NEW_IP} — do steps A-D of the Scenario first." >&2; return 1; }
    for ip in "${ALL_HOSTS[@]}"; do
        rsh "${ip}" "cd ${APP} && git pull --ff-only -q && test -f scripts/extra_nodes_lib.sh" || { echo "ERROR: code sync failed on ${ip}." >&2; return 1; }
        echo "  code up to date: ${ip}"
    done
}

_set_env_line() {   # _set_env_line IP KEY VALUE   (VALUE already validated: no shell/sed specials)
    rsh "$1" "grep -q '^$2=' ${APP}/.env && sudo sed -i 's|^$2=.*|$2=$3|' ${APP}/.env || echo '$2=$3' | sudo tee -a ${APP}/.env >/dev/null; sudo chmod 600 ${APP}/.env"
}

step_2() {
    local extras cns want="${NEW_NAME}-pipeline"
    extras="$(_get EXTRA_NODES)"
    cns="$(_get ADMIN_TRUSTED_PROXY_CNS)"
    [[ "${cns}" =~ ^[A-Za-z0-9._,-]*$ ]] || { echo "ERROR: ADMIN_TRUSTED_PROXY_CNS has unexpected characters." >&2; return 1; }
    if [[ ",${cns}," != *",${want},"* ]]; then
        cns="${cns:+${cns},}${want}"
        here sed -i.bak "s|^ADMIN_TRUSTED_PROXY_CNS=.*|ADMIN_TRUSTED_PROXY_CNS=${cns}|" "${ENV_FILE}"
        echo "  local .env: added ${want} to ADMIN_TRUSTED_PROXY_CNS (backup: .env.bak — delete it when done)"
    fi
    for ip in "${NODE2_IP}" "${NODE3_IP}" "${NODE4_IP}"; do
        _set_env_line "${ip}" EXTRA_NODES "${extras}"
        _set_env_line "${ip}" ADMIN_TRUSTED_PROXY_CNS "${cns}"
        echo "  updated .env on ${ip}"
    done
    for ip in "${EXISTING_CORE[@]:3}"; do
        _set_env_line "${ip}" EXTRA_NODES "${extras}"; _set_env_line "${ip}" ADMIN_TRUSTED_PROXY_CNS "${cns}"
        echo "  updated .env on ${ip}"
    done
    here scp -q "${ENV_FILE}" "${SSH_USER}@${NEW_IP}:${APP}/.env"
    rsh "${NEW_IP}" "sudo sed -i 's|^NODE_NAME=.*|NODE_NAME=${NEW_NAME}|; s|^CURRENT_NODE_IP=.*|CURRENT_NODE_IP=${NEW_IP}|' ${APP}/.env && sudo chmod 600 ${APP}/.env"
    echo "  .env copied to ${NEW_NAME} with its own NODE_NAME / CURRENT_NODE_IP"
}

step_3() {
    # ZooKeeper (no dynamic reconfiguration here) needs a rolling restart with the new server.N lines. The running
    # leader still uses its old 3-member view, so it keeps serving while the FOLLOWERS are restarted one at a time
    # with the new 4-member zoo.cfg. The new node cannot be synchronised by an old-config leader, so the LEADER is
    # restarted LAST: the election then runs among followers + new node (3 of 4 votes). Each step waits for imok.
    local ip mode followers=() leader=""
    for ip in "${EXISTING_CORE[@]}"; do
        [ "${ip}" = "${NODE4_IP}" ] && continue
        mode="$(rsh "${ip}" "echo srvr | nc -w2 127.0.0.1 ${ZK_CLIENT_PORT:-2181} | awk '/^Mode:/{print \$2}'" 2>/dev/null || true)"
        if [ "${mode}" = leader ]; then leader="${ip}"; else followers+=("${ip}"); fi
    done
    for ip in "${followers[@]}"; do
        echo "  follower ${ip}: new zoo.cfg + restart"
        rsh "${ip}" "cd ${APP} && sudo ./zookeeper/setup_zookeeper.sh >/dev/null"
        zk_ok "${ip}" || { echo "ERROR: ZooKeeper on ${ip} did not answer imok — stop and read journalctl -u zookeeper." >&2; return 1; }
    done
    echo "  installing on ${NEW_NAME} (it syncs after the leader restart below)"
    rsh "${NEW_IP}" "cd ${APP} && sudo ./zookeeper/setup_zookeeper.sh"
    zk_ok "${NEW_IP}" || { echo "ERROR: ZooKeeper on ${NEW_NAME} did not answer imok." >&2; return 1; }
    if [ -n "${leader}" ]; then
        echo "  leader ${leader}: new zoo.cfg + restart (last)"
        rsh "${leader}" "cd ${APP} && sudo ./zookeeper/setup_zookeeper.sh >/dev/null"
        zk_ok "${leader}" || { echo "ERROR: ZooKeeper on ${leader} did not answer imok." >&2; return 1; }
    fi
}

step_4() {
    rsh "${NEW_IP}" "cd ${APP} && sudo ./mariadb-galera/setup_galera_node.sh && sudo systemctl enable --now mariadb"
    rsh "${NEW_IP}" "for i in \$(seq 1 60); do s=\$(sudo mariadb -N -e \"SHOW STATUS LIKE 'wsrep_local_state_comment'\" 2>/dev/null | awk '{print \$2}'); [ \"\$s\" = Synced ] && exit 0; sleep 5; done; echo 'not Synced after 5 min (a large SST can take longer: watch journalctl -u mariadb)'; exit 1"
    echo "  ${NEW_NAME} is Synced"
}

step_5() {
    rsh "${NEW_IP}" "cd ${APP} && sudo ./scripts/install_rabbitmq_rhel9.sh && sudo ./scripts/join_rabbitmq_cluster.sh"
}

step_6() {
    rsh "${NODE1_IP}" "cd ${APP} && sudo ./swift/add_ring_node.sh --push"
    rsh "${NEW_IP}" "cd ${APP} && sudo ./swift/setup_swift.sh"
    for ip in "${EXISTING_CORE[@]}"; do
        [ "${ip}" = "${NODE4_IP}" ] && continue
        rsh "${ip}" "cd ${APP} && sudo ./swift/setup_swift.sh >/dev/null"   # refreshes memcached / rsync / firewall; rings are kept
        echo "  swift refreshed on ${ip}"
    done
}

step_7() {
    for ip in "${ALL_CORE[@]}"; do
        rsh "${ip}" "cd ${APP} && sudo ./haproxy/setup_haproxy.sh && sudo haproxy -c -f /etc/haproxy/haproxy.cfg && { ! sudo systemctl is-active --quiet haproxy || sudo systemctl reload haproxy; }"
        echo "  haproxy regenerated on ${ip} (reloaded only where Pacemaker already runs it)"
    done
}

step_8() {
    rsh "${NEW_IP}" "cd ${APP} && sudo ./systemd/install_services.sh ${APP} && sudo systemctl disable flowfirst-process1 flowfirst-process2 flowfirst-process3 flowfirst-process4"
}

step_9() {
    rsh "${NEW_IP}" "cd ${APP} && sudo ./pacemaker/setup_multinode_cluster.sh --prepare-node"
    rsh "${NODE1_IP}" "cd ${APP} && sudo ./pacemaker/add_extra_node.sh ${NEW_NAME}"
}

step_10() {
    rsh "${NEW_IP}" "cd ${APP} && .venv/bin/python scripts/preflight_check.py" || echo "  preflight reported problems — read its output above"
    rsh "${NEW_IP}" "curl -s http://127.0.0.1:\${API_BACKEND_PORT:-8082}/health || true"; echo
    echo "  Now run the checks listed in ReadMe Scenario 76, step 'Verify'."
}

echo "Adding ${NEW_NAME} (${NEW_IP}); existing core nodes: ${EXISTING_CORE[*]}; remote worker: ${NODE4_IP}"
[ "${DRY_RUN}" = 1 ] && echo "(dry run: remote commands are printed, not executed)"
n=0
for title in "${STEPS[@]}"; do
    n=$((n + 1))
    if [ "${MODE}" = plan ]; then printf '  %2d. %s\n' "${n}" "${title}"; continue; fi
    [ "${MODE}" = only ] && [ "${n}" != "${STEP}" ] && continue
    [ "${MODE}" = from ] && [ "${n}" -lt "${STEP}" ] && continue
    say "Step ${n}/${#STEPS[@]}: ${title}"
    # set -e is ignored inside a function called from '||' / 'if', so run the step in a subshell that sets it itself
    set +e
    ( set -e; "step_${n}" )
    rc=$?
    set -e
    [ "${rc}" -eq 0 ] || { echo "STOPPED at step ${n}. Fix the cause, then resume:  $0 ${NEW_NAME} --from ${n}" >&2; exit 1; }
done
echo ""
[ "${MODE}" = plan ] || echo "Finished."
