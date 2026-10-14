#!/usr/bin/env bash
# ==============================================================================
# FlowFirst PKI - issue ONE leaf certificate signed by the FlowFirst root CA.
#
#   ./security/issue_cert.sh --name NAME --profile server|client|peer \
#        [--cn "Common Name"] [--san "DNS:host,IP:10.0.0.1,..."] [--days N] [--force]
#
# Profiles (X.509 extendedKeyUsage):
#   server  serverAuth             a service that clients connect to (HAProxy, MariaDB, ...)
#   client  clientAuth             an identity that connects (API user, a pipeline process, node4)
#   peer    serverAuth+clientAuth  both: inter-node traffic (Galera, RabbitMQ clustering, ZooKeeper quorum)
#
# Output (pki/issued/NAME/):
#   NAME.key   private key, mode 600, NOT passphrase-protected (services must start unattended)
#   NAME.crt   certificate
#   NAME.pem   certificate + private key in one file (HAProxy 'crt' format), mode 600
#   ca.crt     the root CA certificate (trust anchor)
# ==============================================================================
set -euo pipefail
# shellcheck disable=SC1091
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

NAME=""; PROFILE=""; CN=""; SAN=""; DAYS="${PKI_LEAF_DAYS}"; FORCE=0
while [[ $# -gt 0 ]]; do
    case "$1" in
        --name)    NAME="${2:-}"; shift 2 ;;
        --profile) PROFILE="${2:-}"; shift 2 ;;
        --cn)      CN="${2:-}"; shift 2 ;;
        --san)     SAN="${2:-}"; shift 2 ;;
        --days)    DAYS="${2:-}"; shift 2 ;;
        --force)   FORCE=1; shift ;;
        -h|--help) sed -n '2,19p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'; exit 0 ;;
        *) die "Unknown option '$1' (see --help)." ;;
    esac
done

[[ -n "${NAME}" ]]    || die "--name is required."
[[ -n "${PROFILE}" ]] || die "--profile is required (server, client or peer)."
validate_name "${NAME}"
CN="${CN:-${NAME}}"; validate_cn "${CN}"
require_int days "${DAYS}"
case "${PROFILE}" in
    server) EKU="serverAuth" ;;
    client) EKU="clientAuth" ;;
    peer)   EKU="serverAuth, clientAuth" ;;
    *) die "--profile must be server, client or peer (got '${PROFILE}')." ;;
esac
if [[ "${PROFILE}" != "client" && -z "${SAN}" ]]; then
    die "--san is required for profile '${PROFILE}': modern TLS clients ignore the CN and check the subjectAltName. Example: --san \"DNS:${NAME},IP:10.0.0.11\""
fi
# SAN entries are written into an OpenSSL config file: allow only DNS:/IP: items with safe characters.
if [[ -n "${SAN}" ]]; then
    IFS=',' read -r -a _san_items <<< "${SAN}"
    for _s in "${_san_items[@]}"; do
        [[ "${_s}" =~ ^(DNS:[A-Za-z0-9*][A-Za-z0-9._-]{0,252}|IP:[0-9a-fA-F:.]{2,45})$ ]] || die "Invalid SAN entry '${_s}' (use DNS:name or IP:address)."
    done
fi

require_openssl; require_ca
OUT="${ISSUED_DIR}/${NAME}"
if [[ -e "${OUT}/${NAME}.crt" && ${FORCE} -ne 1 ]]; then
    die "${OUT}/${NAME}.crt already exists. Use --force to re-issue (a new key and serial; the old certificate is NOT revoked automatically - see security/revoke_cert.sh)."
fi
ca_key_unlock

umask 077
mkdir -p "${OUT}"
WORK="$(mktemp -d "${OUT}/.work.XXXXXX")"
trap 'rm -rf "${WORK}"; ca_key_lock' EXIT

{
    echo "[ req ]"
    echo "distinguished_name = dn"
    echo "prompt = no"
    echo "string_mask = utf8only"
    echo "[ dn ]"
    echo "O = ${PKI_ORG}"
    echo "CN = ${CN}"
} > "${WORK}/req.cnf"

{
    echo "[ leaf ]"
    echo "basicConstraints = critical, CA:false"
    echo "subjectKeyIdentifier = hash"
    echo "authorityKeyIdentifier = keyid,issuer"
    echo "keyUsage = critical, digitalSignature, keyEncipherment"
    echo "extendedKeyUsage = ${EKU}"
    if [[ -n "${SAN}" ]]; then
        echo "subjectAltName = @san"
        echo "[ san ]"
        _d=0; _i=0
        for _s in "${_san_items[@]}"; do
            case "${_s}" in
                DNS:*) _d=$((_d+1)); echo "DNS.${_d} = ${_s#DNS:}" ;;
                IP:*)  _i=$((_i+1)); echo "IP.${_i} = ${_s#IP:}" ;;
            esac
        done
    fi
} > "${WORK}/ext.cnf"

openssl genrsa -out "${WORK}/leaf.key" "${PKI_LEAF_BITS}" 2>/dev/null
openssl req -new -sha256 -config "${WORK}/req.cnf" -key "${WORK}/leaf.key" -out "${WORK}/leaf.csr"
openssl ca -batch -notext -config "${CA_DIR}/ca.cnf" -keyfile "${CA_KEY_PLAIN}" -extfile "${WORK}/ext.cnf" -extensions leaf \
    -startdate "$(utc_stamp "-${PKI_BACKDATE_MIN}")" -days "${DAYS}" \
    -in "${WORK}/leaf.csr" -out "${WORK}/leaf.crt" >/dev/null 2>&1 \
    || die "Signing failed."

# Verify against the CA before publishing the files, and check that key and certificate belong together.
openssl verify -CAfile "${CA_DIR}/ca.crt" -purpose "$([[ "${PROFILE}" == "client" ]] && echo sslclient || echo sslserver)" "${WORK}/leaf.crt" >/dev/null \
    || die "The issued certificate does not verify against the CA."
_kpub="$(openssl pkey -in "${WORK}/leaf.key" -pubout 2>/dev/null | openssl sha256)"
_cpub="$(openssl x509 -in "${WORK}/leaf.crt" -pubkey -noout | openssl sha256)"
[[ "${_kpub}" == "${_cpub}" ]] || die "Key and certificate do not match."

install -m 600 "${WORK}/leaf.key" "${OUT}/${NAME}.key"
install -m 644 "${WORK}/leaf.crt" "${OUT}/${NAME}.crt"
install -m 644 "${CA_DIR}/ca.crt" "${OUT}/ca.crt"
cat "${OUT}/${NAME}.crt" "${OUT}/${NAME}.key" > "${OUT}/${NAME}.pem"; chmod 600 "${OUT}/${NAME}.pem"

_serial="$(openssl x509 -in "${OUT}/${NAME}.crt" -noout -serial | cut -d= -f2)"
_end="$(openssl x509 -in "${OUT}/${NAME}.crt" -noout -enddate | cut -d= -f2)"
printf '  issued %-22s %-7s serial=%s expires=%s\n' "${NAME}" "${PROFILE}" "${_serial}" "${_end}"
