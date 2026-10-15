#!/usr/bin/env bash
# ==============================================================================
# FlowFirst PKI - distribute certificates to the nodes (least privilege: each node
# receives only its OWN keys, plus the public CA certificate and CRL).
#
#   ./security/install_certs.sh                 all nodes over ssh (needs passwordless ssh + sudo)
#   ./security/install_certs.sh --node 2        one node (1-4)
#   ./security/install_certs.sh --crl-only      only refresh ca.crl on the nodes
#   ./security/install_certs.sh --stage DIR     do not use ssh; write the per-node trees to DIR/<nodeN>/ for review
#
# Layout on each node (TLS_DIR, default /etc/flowfirst/tls):
#   ca.crt  ca.crl                       root:root 644        public
#   pipeline/pipeline.{crt,key}          flowuser             client identity of Processes 1-5
#   api/api.{crt,key}                    flowuser             API Gateway (Process 1) HTTPS listener        (node1-3)
#   haproxy/{vip,backend}.pem            haproxy              VIP server cert, backend client cert (node1-3)
#   rabbitmq/rabbitmq.{crt,key}          rabbitmq             (node1-3)
#   mariadb/mariadb.{crt,key}            mysql                (node1-3)
#   zookeeper/zookeeper.{crt,key}        zookeeper            (node1-3)
#   swift/swift.{crt,key}                swift                (node1-3)
# Private keys are mode 600, owned by the service account. If the account does not exist yet
# (service not installed), the file stays root:root 600 and a notice is printed: re-run after install.
# ==============================================================================
set -euo pipefail
# shellcheck disable=SC1091
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

TLS_DIR="${TLS_DIR:-/etc/flowfirst/tls}"
SSH_USER="${PKI_SSH_USER:-}"
ONLY_NODE=""; CRL_ONLY=0; STAGE_ROOT=""
while [[ $# -gt 0 ]]; do
    case "$1" in
        --node)     ONLY_NODE="${2:-}"; shift 2 ;;
        --crl-only) CRL_ONLY=1; shift ;;
        --stage)    STAGE_ROOT="${2:-}"; shift 2 ;;
        -h|--help)  sed -n '2,24p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'; exit 0 ;;
        *) die "Unknown option '$1'." ;;
    esac
done
[[ -z "${ONLY_NODE}" || "${ONLY_NODE}" =~ ^[1-4]$ ]] || die "--node must be 1, 2, 3 or 4."
[[ "${TLS_DIR}" =~ ^/[A-Za-z0-9._/-]+$ ]] || die "TLS_DIR must be an absolute path of safe characters."
[[ -z "${SSH_USER}" || "${SSH_USER}" =~ ^[a-z_][a-z0-9_-]*$ ]] || die "Invalid PKI_SSH_USER."
require_ca
[[ -f "${CA_DIR}/ca.crl" ]] || die "No CRL at ${CA_DIR}/ca.crl."

# --- staging -----------------------------------------------------------------
MANIFEST=""      # lines: relative-path owner mode
STAGE=""
add() {   # add SRC REL OWNER MODE
    [[ -f "$1" ]] || die "Missing file $1 (run security/issue_all.sh first)."
    mkdir -p "$(dirname "${STAGE}/$2")"
    cp "$1" "${STAGE}/$2"
    MANIFEST+="$2 $3 $4"$'\n'
}

stage_node() {   # stage_node N
    local n="$1" nm; nm="$(node_name "$n")"
    local I="${ISSUED_DIR}"
    STAGE="$2"; MANIFEST=""
    rm -rf "${STAGE}"; mkdir -p "${STAGE}"
    add "${CA_DIR}/ca.crt" ca.crt root 644
    add "${CA_DIR}/ca.crl" ca.crl root 644
    if [[ ${CRL_ONLY} -eq 1 ]]; then return; fi
    add "${I}/${nm}-pipeline/${nm}-pipeline.crt" pipeline/pipeline.crt flowuser 644
    add "${I}/${nm}-pipeline/${nm}-pipeline.key" pipeline/pipeline.key flowuser 600
    if [[ "${n}" != "4" ]]; then
        add "${I}/${nm}-api/${nm}-api.crt"             api/api.crt              flowuser  644
        add "${I}/${nm}-api/${nm}-api.key"             api/api.key              flowuser  600
        add "${I}/flowfirst-vip/flowfirst-vip.pem"     haproxy/vip.pem          haproxy   600
        add "${I}/${nm}-pipeline/${nm}-pipeline.pem"   haproxy/backend.pem      haproxy   600
        add "${I}/${nm}-rabbitmq/${nm}-rabbitmq.crt"   rabbitmq/rabbitmq.crt    rabbitmq  644
        add "${I}/${nm}-rabbitmq/${nm}-rabbitmq.key"   rabbitmq/rabbitmq.key    rabbitmq  600
        add "${I}/${nm}-mariadb/${nm}-mariadb.crt"     mariadb/mariadb.crt      mysql     644
        add "${I}/${nm}-mariadb/${nm}-mariadb.key"     mariadb/mariadb.key      mysql     600
        add "${I}/${nm}-zookeeper/${nm}-zookeeper.crt" zookeeper/zookeeper.crt  zookeeper 644
        add "${I}/${nm}-zookeeper/${nm}-zookeeper.key" zookeeper/zookeeper.key  zookeeper 600
        add "${I}/${nm}-swift/${nm}-swift.crt"         swift/swift.crt          swift     644
        add "${I}/${nm}-swift/${nm}-swift.key"         swift/swift.key          swift     600
    fi
}

# Remote script: owner/mode values come only from the fixed lists above, never from user input.
remote_script() {   # reads MANIFEST
    echo 'set -u'
    echo "T='${TLS_DIR}'"
    echo 'install -d -m 755 -o root -g root "$T"'
    local rel owner mode
    while read -r rel owner mode; do
        [[ -n "${rel}" ]] || continue
        echo "d=\$(dirname \"\$T/${rel}\"); [ \"\$d\" = \"\$T\" ] || install -d -m 755 -o root -g root \"\$d\""
        echo "o='${owner}'; id \"\$o\" >/dev/null 2>&1 || { echo \"  NOTICE: user '\$o' does not exist yet; ${rel} left root:root (re-run after installing the service)\"; o=root; }"
        echo "chown \"\$o\":\"\$o\" \"\$T/${rel}\" 2>/dev/null || chown \"\$o\" \"\$T/${rel}\"; chmod ${mode} \"\$T/${rel}\""
        # service accounts must be able to traverse their own directory
        case "${rel}" in */*) echo "[ \"\$o\" = root ] || chown root:\"\$o\" \"\$(dirname \"\$T/${rel}\")\" 2>/dev/null; chmod 750 \"\$(dirname \"\$T/${rel}\")\" 2>/dev/null || true" ;; esac
    done <<< "${MANIFEST}"
    echo 'command -v restorecon >/dev/null 2>&1 && restorecon -R "$T" >/dev/null 2>&1 || true'
}

WORK="$(mktemp -d)"; trap 'rm -rf "${WORK}"' EXIT
nodes="1 2 3 4"; [[ -n "${ONLY_NODE}" ]] && nodes="${ONLY_NODE}"

for n in ${nodes}; do
    nm="$(node_name "$n")"; ip="$(node_ip "$n")"
    [[ -n "${ip}" && ! "${ip}" =~ ^\$\{ ]] || die "NODE${n}_IP is not set in ${_env_file}."
    if [[ -n "${STAGE_ROOT}" ]]; then
        mkdir -p "${STAGE_ROOT}"; stage_node "$n" "${STAGE_ROOT}/${nm}"
        remote_script > "${STAGE_ROOT}/${nm}.install.sh"
        echo "  staged ${nm}: $(find "${STAGE_ROOT}/${nm}" -type f | wc -l | tr -d ' ') files in ${STAGE_ROOT}/${nm}"
        continue
    fi
    echo "Installing on ${nm} (${ip}) -> ${TLS_DIR} ..."
    stage_node "$n" "${WORK}/${nm}"
    target="${SSH_USER:+${SSH_USER}@}${ip}"
    ssh -o BatchMode=yes -o ConnectTimeout=10 "${target}" "sudo install -d -m 755 '${TLS_DIR}'" || die "ssh/sudo to ${target} failed."
    tar -C "${WORK}/${nm}" -cf - . | ssh -o BatchMode=yes "${target}" "sudo tar -xf - -C '${TLS_DIR}' --no-same-owner --no-same-permissions"
    remote_script | ssh -o BatchMode=yes "${target}" "sudo bash -s"
    echo "  ${nm}: $(find "${WORK}/${nm}" -type f | wc -l | tr -d ' ') files installed"
done

[[ -n "${STAGE_ROOT}" ]] && echo "Review the trees, then run without --stage to install. Each <node>.install.sh shows the ownership commands that would run." || true
echo "Done."
