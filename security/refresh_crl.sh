#!/usr/bin/env bash
# ==============================================================================
# FlowFirst PKI - re-issue the CRL WITHOUT revoking anything (run on the CA/admin host).
#
#   ./security/refresh_crl.sh [--days N]       new ca.crl valid for N days (default PKI_CRL_DAYS, 30)
#
# Why: a CRL has a nextUpdate date. Once it passes, every component that loads the CRL (API Gateway (Process 1) and the
# Python clients with TLS_CRL_FILE set, HAProxy crl-file, MariaDB ssl_crl) treats ALL certificates as
# unverifiable and refuses them - an outage, not a warning. Refresh it well before that, from cron:
#   0 3 * * 1  PKI_CA_PASSPHRASE_FILE=/root/.ca-pass /opt/flowfirst/security/refresh_crl.sh && \
#              /opt/flowfirst/security/install_certs.sh --crl-only
# After installing the file, MariaDB needs 'FLUSH SSL' (or a restart) and HAProxy a reload; Python
# processes read the file when they start, so restart them (or they keep the old CRL in memory).
# ==============================================================================
set -euo pipefail
# shellcheck disable=SC1091
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

DAYS="${PKI_CRL_DAYS}"
while [[ $# -gt 0 ]]; do
    case "$1" in
        --days) DAYS="${2:-}"; shift 2 ;;
        -h|--help) sed -n '2,15p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'; exit 0 ;;
        *) die "Unknown option '$1'." ;;
    esac
done
require_int --days "${DAYS}"
require_openssl; require_ca

ca_key_unlock
openssl ca -config "${CA_DIR}/ca.cnf" -keyfile "${CA_KEY_PLAIN}" -gencrl -crldays "${DAYS}" \
    -out "${CA_DIR}/ca.crl.tmp" >/dev/null 2>&1 || die "CRL generation failed."
mv -f "${CA_DIR}/ca.crl.tmp" "${CA_DIR}/ca.crl"
chmod 644 "${CA_DIR}/ca.crl"
echo "  CRL refreshed: ${CA_DIR}/ca.crl  $(openssl crl -in "${CA_DIR}/ca.crl" -noout -nextupdate)"
echo "  Next: ./security/install_certs.sh --crl-only   then FLUSH SSL on MariaDB, reload HAProxy, restart the Python processes."
