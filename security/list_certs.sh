#!/usr/bin/env bash
# ==============================================================================
# FlowFirst PKI - list every issued certificate with profile, expiry and status.
#
#   ./security/list_certs.sh [--warn-days N]     (default 30)  exit 1 if any is expired/expiring or revoked
# ==============================================================================
set -euo pipefail
# shellcheck disable=SC1091
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

WARN=30
[[ "${1:-}" == "--warn-days" ]] && { WARN="${2:-}"; require_int "--warn-days" "${WARN}"; }
require_openssl; require_ca

printf '%-24s %-8s %-10s %-26s %s\n' NAME PROFILE SERIAL EXPIRES STATUS
bad=0
for d in "${ISSUED_DIR}"/*/; do
    [[ -d "${d}" ]] || continue
    name="$(basename "${d}")"; crt="${d}${name}.crt"
    [[ -f "${crt}" ]] || continue
    serial="$(openssl x509 -in "${crt}" -noout -serial | cut -d= -f2)"
    end="$(openssl x509 -in "${crt}" -noout -enddate | cut -d= -f2)"
    eku="$(openssl x509 -in "${crt}" -noout -ext extendedKeyUsage 2>/dev/null | tr -d '\n')"
    case "${eku}" in *"Server"*"Client"*) prof=peer ;; *"Server"*) prof=server ;; *"Client"*) prof=client ;; *) prof=? ;; esac
    if grep -q "^R.*[[:space:]]${serial}[[:space:]]" "${CA_DIR}/index.txt"; then
        status="REVOKED"; bad=1
    elif ! openssl x509 -in "${crt}" -noout -checkend 0 >/dev/null; then
        status="EXPIRED"; bad=1
    elif ! openssl x509 -in "${crt}" -noout -checkend $((WARN * 86400)) >/dev/null; then
        status="expires within ${WARN} days"; bad=1
    else
        status="ok"
    fi
    printf '%-24s %-8s %-10s %-26s %s\n' "${name}" "${prof}" "${serial}" "${end}" "${status}"
done
echo
echo "CA: $(openssl x509 -in "${CA_DIR}/ca.crt" -noout -enddate)"

# The CRL: once its nextUpdate passes, components that load it refuse EVERY certificate (see refresh_crl.sh).
if [[ -f "${CA_DIR}/ca.crl" ]]; then
    crl_next="$(openssl crl -in "${CA_DIR}/ca.crl" -noout -nextupdate | cut -d= -f2)"
    next_epoch="$(date -u -d "${crl_next}" +%s 2>/dev/null || date -u -j -f '%b %e %T %Y %Z' "${crl_next}" +%s 2>/dev/null || echo 0)"
    now_epoch="$(date -u +%s)"
    if [[ "${next_epoch}" -le "${now_epoch}" ]]; then
        echo "CRL: nextUpdate ${crl_next}  EXPIRED - run security/refresh_crl.sh now"; bad=1
    elif [[ "${next_epoch}" -le $((now_epoch + 7 * 86400)) ]]; then
        echo "CRL: nextUpdate ${crl_next}  expires within 7 days - run security/refresh_crl.sh"; bad=1
    else
        echo "CRL: nextUpdate ${crl_next}"
    fi
fi
exit "${bad}"
