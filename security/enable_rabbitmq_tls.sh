#!/usr/bin/env bash
# ==============================================================================
# FlowFirst - enable TLS on a RabbitMQ node (run on each of node1-3, as a user with sudo).
#
#   sudo ./security/enable_rabbitmq_tls.sh                     AMQPS :5671 + HTTPS management :15671 (plain AMQP stays on)
#   sudo ./security/enable_rabbitmq_tls.sh --disable-plain     also close the plain AMQP :5672 and HTTP :15672 listeners
#   sudo ./security/enable_rabbitmq_tls.sh --inter-node        also encrypt the Erlang cluster bus (:25672) - see warning
#   sudo ./security/enable_rabbitmq_tls.sh --no-restart        write the configuration only
#   ./security/enable_rabbitmq_tls.sh --print                  print what would be written and exit (no root needed)
#
# Needs the certificates from security/install_certs.sh in TLS_DIR (default /etc/flowfirst/tls):
#   ca.crt  rabbitmq/rabbitmq.crt  rabbitmq/rabbitmq.key
#
# Effect: clients must present a certificate signed by the FlowFirst CA (verify_peer + fail_if_no_peer_cert).
#
# KNOWN LIMIT - revocation is NOT enforced by this script. Live test: a client certificate revoked in ca.crl was
# still accepted, because rabbitmq.conf has no CRL setting and nothing here configures one. (Erlang ssl can check
# CRLs through advanced.config with a crl_cache; that was not tried.) A client also needs the flowuser password,
# so to cut off a compromised identity change that password (or delete the user) and restart the processes.
# Keep certificate lifetimes short for the same reason.
# The configuration lives in a marked block (BEGIN/END FLOWFIRST TLS); re-running replaces only that block.
#
# Safe order for a running cluster (one node at a time, so pause_minority keeps a majority up):
#   1. run this script on node1, then node2, then node3                (plain AMQP still works)
#   2. set TLS_RABBITMQ_ENABLED=true in .env on every node and restart the processes
#   3. optional: re-run with --disable-plain to close the plaintext ports
#   --inter-node is different: a node using TLS distribution cannot talk to one that does not, so every node
#   must be changed in the same maintenance window (stop all RabbitMQ, run it on every node, start all).
# ==============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
if [[ -f "${ROOT_DIR}/.env" ]]; then set -a; source "${ROOT_DIR}/.env"; set +a; fi   # shellcheck disable=SC1091

TLS_DIR="${TLS_DIR:-/etc/flowfirst/tls}"
RMQ_ETC="${RMQ_ETC_DIR:-/etc/rabbitmq}"
RABBITMQ_TLS_PORT="${RABBITMQ_TLS_PORT:-5671}"
RMQ_MGMT_TLS_PORT="${RMQ_MGMT_TLS_PORT:-15671}"
TLS_MIN_VERSION="${TLS_MIN_VERSION:-1.2}"
BEGIN="# BEGIN FLOWFIRST TLS (managed by security/enable_rabbitmq_tls.sh - do not edit inside)"
END="# END FLOWFIRST TLS"

DISABLE_PLAIN=0; INTER_NODE=0; RESTART=1; PRINT=0
while [[ $# -gt 0 ]]; do
    case "$1" in
        --disable-plain) DISABLE_PLAIN=1 ;;
        --inter-node)    INTER_NODE=1 ;;
        --no-restart)    RESTART=0 ;;
        --print)         PRINT=1 ;;
        -h|--help)       sed -n '2,24p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'; exit 0 ;;
        *) echo "ERROR: unknown option '$1'" >&2; exit 2 ;;
    esac
    shift
done

# Values end up in config files: accept only plain absolute paths, port numbers and 1.2/1.3.
[[ "${TLS_DIR}" =~ ^/[A-Za-z0-9._/-]+$ && "${RMQ_ETC}" =~ ^/[A-Za-z0-9._/-]+$ ]] || { echo "ERROR: TLS_DIR / RMQ_ETC_DIR must be absolute paths of safe characters." >&2; exit 1; }
[[ "${RABBITMQ_TLS_PORT}" =~ ^[1-9][0-9]*$ && "${RMQ_MGMT_TLS_PORT}" =~ ^[1-9][0-9]*$ ]] || { echo "ERROR: ports must be positive integers." >&2; exit 1; }
[[ "${TLS_MIN_VERSION}" =~ ^1\.[23]$ ]] || { echo "ERROR: TLS_MIN_VERSION must be 1.2 or 1.3." >&2; exit 1; }

CA="${TLS_DIR}/ca.crt"; CRT="${TLS_DIR}/rabbitmq/rabbitmq.crt"; KEY="${TLS_DIR}/rabbitmq/rabbitmq.key"

conf_block() {
    echo "${BEGIN}"
    echo "listeners.ssl.default = ${RABBITMQ_TLS_PORT}"
    [[ ${DISABLE_PLAIN} -eq 1 ]] && echo "listeners.tcp = none"
    cat <<EOF
ssl_options.cacertfile           = ${CA}
ssl_options.certfile             = ${CRT}
ssl_options.keyfile              = ${KEY}
ssl_options.verify               = verify_peer
ssl_options.fail_if_no_peer_cert = true
ssl_options.honor_cipher_order   = true
EOF
    if [[ "${TLS_MIN_VERSION}" == "1.3" ]]; then
        echo "ssl_options.versions.1           = tlsv1.3"
    else
        echo "ssl_options.versions.1           = tlsv1.3"; echo "ssl_options.versions.2           = tlsv1.2"
    fi
    cat <<EOF
management.ssl.port       = ${RMQ_MGMT_TLS_PORT}
management.ssl.cacertfile = ${CA}
management.ssl.certfile   = ${CRT}
management.ssl.keyfile    = ${KEY}
EOF
    echo "${END}"
}

dist_config() {   # Erlang distribution (inter-node) - the same peer certificate acts as server and client
    cat <<EOF
%% managed by security/enable_rabbitmq_tls.sh
[
  {server, [
    {cacertfile, "${CA}"}, {certfile, "${CRT}"}, {keyfile, "${KEY}"},
    {secure_renegotiate, true}, {verify, verify_peer}, {fail_if_no_peer_cert, true}
  ]},
  {client, [
    {cacertfile, "${CA}"}, {certfile, "${CRT}"}, {keyfile, "${KEY}"},
    {secure_renegotiate, true}, {verify, verify_peer}
  ]}
].
EOF
}

env_block() {
    echo "${BEGIN}"
    echo "SERVER_ADDITIONAL_ERL_ARGS=\"-proto_dist inet_tls -ssl_dist_optfile ${RMQ_ETC}/inter_node_tls.config\""
    echo "CTL_ERL_ARGS=\"-proto_dist inet_tls -ssl_dist_optfile ${RMQ_ETC}/inter_node_tls.config\""
    echo "${END}"
}

if [[ ${PRINT} -eq 1 ]]; then
    echo "### ${RMQ_ETC}/rabbitmq.conf"; conf_block
    if [[ ${INTER_NODE} -eq 1 ]]; then
        echo; echo "### ${RMQ_ETC}/rabbitmq-env.conf"; env_block
        echo; echo "### ${RMQ_ETC}/inter_node_tls.config"; dist_config
    fi
    exit 0
fi

if [[ ${EUID} -ne 0 ]]; then echo "ERROR: run with sudo (it writes ${RMQ_ETC})." >&2; exit 1; fi

# Fail closed: refuse to enable TLS when the service account cannot read its certificate and key.
for f in "${CA}" "${CRT}" "${KEY}"; do
    [[ -f "${f}" ]] || { echo "ERROR: ${f} not found. Run security/install_certs.sh first." >&2; exit 1; }
done
if id rabbitmq >/dev/null 2>&1; then
    for f in "${CA}" "${CRT}" "${KEY}"; do
        runuser -u rabbitmq -- test -r "${f}" || { echo "ERROR: user 'rabbitmq' cannot read ${f} (check owner/mode and directory permissions)." >&2; exit 1; }
    done
fi

replace_block() {   # replace_block FILE FUNCTION : drop any old managed block, append a fresh one
    local file="$1" fn="$2"
    mkdir -p "$(dirname "${file}")"; touch "${file}"
    [[ -f "${file}.pre-tls" ]] || cp -p "${file}" "${file}.pre-tls"
    local tmp; tmp="$(mktemp)"
    sed "/^# BEGIN FLOWFIRST TLS/,/^# END FLOWFIRST TLS/d" "${file}" > "${tmp}"
    { cat "${tmp}"; "${fn}"; } > "${file}"; rm -f "${tmp}"
}

echo "[1/4] ${RMQ_ETC}/rabbitmq.conf  (AMQPS :${RABBITMQ_TLS_PORT}, management HTTPS :${RMQ_MGMT_TLS_PORT}, plain closed: $([[ ${DISABLE_PLAIN} -eq 1 ]] && echo yes || echo no))"
replace_block "${RMQ_ETC}/rabbitmq.conf" conf_block
chown root:rabbitmq "${RMQ_ETC}/rabbitmq.conf" 2>/dev/null || true; chmod 640 "${RMQ_ETC}/rabbitmq.conf"

if [[ ${INTER_NODE} -eq 1 ]]; then
    echo "[2/4] inter-node TLS: ${RMQ_ETC}/inter_node_tls.config and rabbitmq-env.conf"
    dist_config > "${RMQ_ETC}/inter_node_tls.config"
    chown root:rabbitmq "${RMQ_ETC}/inter_node_tls.config" 2>/dev/null || true; chmod 640 "${RMQ_ETC}/inter_node_tls.config"
    replace_block "${RMQ_ETC}/rabbitmq-env.conf" env_block
else
    echo "[2/4] inter-node TLS: not requested (cluster bus :25672 stays plain; use --inter-node)"
fi

echo "[3/4] firewall"
if command -v firewall-cmd >/dev/null 2>&1 && systemctl is-active --quiet firewalld; then
    firewall-cmd --permanent --add-port="${RABBITMQ_TLS_PORT}/tcp" >/dev/null
    firewall-cmd --permanent --add-port="${RMQ_MGMT_TLS_PORT}/tcp" >/dev/null
    firewall-cmd --reload >/dev/null
    echo "  opened ${RABBITMQ_TLS_PORT}/tcp and ${RMQ_MGMT_TLS_PORT}/tcp"
else
    echo "  firewalld not active - nothing to do"
fi

if [[ ${RESTART} -eq 1 ]]; then
    echo "[4/4] restarting rabbitmq-server on this node (roll through the nodes one at a time)"
    systemctl restart rabbitmq-server
    sleep 5
    rabbitmq-diagnostics -q listeners 2>/dev/null | grep -E "amqp|https" || echo "  (could not list listeners yet - check: rabbitmq-diagnostics listeners)"
else
    echo "[4/4] --no-restart: run 'systemctl restart rabbitmq-server' to apply"
fi
echo "Undo: restore ${RMQ_ETC}/rabbitmq.conf.pre-tls (and rabbitmq-env.conf.pre-tls), then restart."
