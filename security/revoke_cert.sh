#!/usr/bin/env bash
# ==============================================================================
# FlowFirst PKI - revoke a leaf certificate and refresh the CRL.
#
#   ./security/revoke_cert.sh NAME [reason]
#   reason: keyCompromise (default) | superseded | cessationOfOperation | unspecified
#
# After revoking, publish the new CRL: ./security/install_certs.sh --crl-only
# A revoked certificate is only refused by components that load the CRL
# (HAProxy 'crl-file', API Gateway (Process 1) and the Python clients when TLS_CRL_FILE is set).
# Components without CRL support must instead get a new certificate and a restart.
# ==============================================================================
set -euo pipefail
# shellcheck disable=SC1091
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

NAME="${1:-}"; REASON="${2:-keyCompromise}"
[[ -n "${NAME}" && "${NAME}" != -* ]] || { sed -n '2,12p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'; exit 1; }
validate_name "${NAME}"
case "${REASON}" in keyCompromise|superseded|cessationOfOperation|unspecified) ;; *) die "Invalid reason '${REASON}'." ;; esac

require_openssl; require_ca
CRT="${ISSUED_DIR}/${NAME}/${NAME}.crt"
[[ -f "${CRT}" ]] || die "No certificate ${CRT}."
SERIAL="$(openssl x509 -in "${CRT}" -noout -serial | cut -d= -f2)"
if grep -q "^R.*[[:space:]]${SERIAL}[[:space:]]" "${CA_DIR}/index.txt"; then
    echo "  ${NAME} (serial ${SERIAL}) is already revoked."; exit 0
fi

ca_key_unlock
openssl ca -config "${CA_DIR}/ca.cnf" -keyfile "${CA_KEY_PLAIN}" -revoke "${CRT}" -crl_reason "${REASON}" >/dev/null 2>&1 \
    || die "Revocation failed."
gen_crl
echo "  revoked ${NAME} (serial ${SERIAL}, reason ${REASON}); CRL updated: ${CA_DIR}/ca.crl"
echo "  Next: ./security/install_certs.sh --crl-only   then reload the services that use the CRL."
