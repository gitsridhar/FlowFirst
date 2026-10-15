#!/usr/bin/env bash
# ==============================================================================
# FlowFirst PKI - issue the full certificate inventory for the cluster in one go.
#
#   ./security/issue_all.sh [--force]     (already-issued certificates are skipped unless --force)
#
# Reads NODE1..4_IP, FLOWFIRST_VIP (and optional NODEn_NAME) from .env.
#
# Inventory (name -> profile -> used for)
#   flowfirst-vip      server  HAProxy on the VIP: HTTPS API, Swift, MariaDB-LB and RabbitMQ-LB TLS, stats page
#   <node>-api         server  API Gateway (Process 1) HTTPS listener on node1..3 (HAProxy re-encrypts to it)
#   <node>-rabbitmq    peer    RabbitMQ broker on node1..3 (clients + inter-node clustering)
#   <node>-mariadb     peer    MariaDB / Galera on node1..3 (clients + replication, SST/IST)
#   <node>-zookeeper   peer    ZooKeeper on node1..3 (clients + quorum + leader election)
#   <node>-swift       server  Swift proxy on node1..3
#   <node>-pipeline    client  identity of Processes 1-4 on node1..3, and of HAProxy towards its backends
#   node4-pipeline     client  identity of Remote Worker (Process 5) on the remote node4 (it only ever connects out)
#   admin-client       client  operator / test identity for the external API (mutual TLS)
# More external API users: ./security/issue_cert.sh --name <user> --profile client --cn "<user>"
# ==============================================================================
set -euo pipefail
# shellcheck disable=SC1091
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

FORCE_ARG=()
for a in "$@"; do
    case "$a" in
        --force) FORCE_ARG=(--force) ;;
        -h|--help) sed -n '2,19p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'; exit 0 ;;
        *) die "Unknown option '$a'." ;;
    esac
done

require_openssl; require_ca
for n in 1 2 3 4; do
    [[ -n "$(node_ip "$n")" && ! "$(node_ip "$n")" =~ ^\$\{ ]] || die "NODE${n}_IP is not set in ${_env_file} (literal values only)."
done
[[ -n "${FLOWFIRST_VIP:-}" && ! "${FLOWFIRST_VIP}" =~ ^\$\{ ]] || die "FLOWFIRST_VIP is not set in ${_env_file}."
VIP_DNS="${PKI_VIP_DNS:-flowfirst.local}"
export PKI_CA_PASS=""; unset PKI_CA_PASS
ca_pass_init          # ask for the passphrase once; exported to the issue_cert.sh children

issue() {   # issue NAME PROFILE CN [SAN]
    local name="$1" profile="$2" cn="$3" san="${4:-}" args=(--name "$1" --profile "$2" --cn "$3")
    [[ -n "${san}" ]] && args+=(--san "${san}")
    if [[ -e "${ISSUED_DIR}/${name}/${name}.crt" && ${#FORCE_ARG[@]} -eq 0 ]]; then
        printf '  exists %-22s (skipped; use --force to re-issue)\n' "${name}"; return 0
    fi
    "${SEC_DIR}/issue_cert.sh" "${args[@]}" ${FORCE_ARG[@]+"${FORCE_ARG[@]}"}
}

echo "Issuing certificates into ${ISSUED_DIR} ..."
issue flowfirst-vip server "flowfirst-vip" "DNS:${VIP_DNS},DNS:localhost,IP:${FLOWFIRST_VIP},IP:127.0.0.1"

for i in 1 2 3; do
    nm="$(node_name "$i")"; ip="$(node_ip "$i")"
    san="DNS:${nm},DNS:localhost,IP:${ip},IP:127.0.0.1"
    # MARIADB_HOST / RABBITMQ_HOST point at the VIP, so those two brokers must also be valid for the VIP name.
    vipsan="${san},DNS:${VIP_DNS},IP:${FLOWFIRST_VIP}"
    issue "${nm}-api"       server "${nm}-api"       "${san}"
    issue "${nm}-rabbitmq"  peer   "${nm}-rabbitmq"  "${vipsan}"
    issue "${nm}-mariadb"   peer   "${nm}-mariadb"   "${vipsan}"
    issue "${nm}-zookeeper" peer   "${nm}-zookeeper" "${san}"
    issue "${nm}-swift"     server "${nm}-swift"     "${san}"
    issue "${nm}-pipeline"  client "${nm}-pipeline"
done

n4="$(node_name 4)"
issue "${n4}-pipeline" client "${n4}-pipeline"
issue admin-client client "admin-client"

echo
echo "Done. $(find "${ISSUED_DIR}" -name '*.crt' ! -name ca.crt | wc -l | tr -d ' ') certificates in ${ISSUED_DIR}."
echo "Next: ./security/install_certs.sh   (copies each node its own files plus the CA certificate)"
