#!/usr/bin/env bash
# ==============================================================================
# FlowFirst - enable TLS on the Swift proxy (run on each of node1-3, as a user with sudo).
#
#   sudo ./security/enable_swift_tls.sh                     add cert_file/key_file to [DEFAULT] in proxy-server.conf
#   sudo ./security/enable_swift_tls.sh --no-restart        write the configuration only
#   ./security/enable_swift_tls.sh --print                  print the config block and exit (no root needed)
#
# Needs the certificates from security/install_certs.sh in TLS_DIR (default /etc/flowfirst/tls):
#   swift/swift.crt  swift/swift.key  (CA is NOT sent to the proxy backend; see limits below)
#
# Architecture notes:
#   HAProxy terminates client TLS for Swift (VIP port 8090 -> swift-proxy 8888).  This script adds
#   *backend* TLS so the HAProxy -> swift-proxy hop is also encrypted.
#
#   Swift's eventlet WSGI TLS (cert_file/key_file in proxy-server.conf) has two limits found in
#   swift/common/wsgi.py 2.38.2:
#     1. ssl.CERT_NONE is hardcoded: the proxy does NOT request or verify an inbound client cert.
#        Client-cert enforcement happens at HAProxy, not at Swift.
#     2. The Swift source itself prints: "WARNING: SSL should only be enabled for testing purposes.
#        Use external SSL termination for a production deployment."  For production, consider
#        stunnel or nginx as a local TLS wrapper on each node instead.
#
#   With TLS_SWIFT_ENABLED=true in .env, src/swift/swift_storage.py presents the pipeline client cert to
#   HAProxy (which enforces it). After HAProxy, the connection reaches the proxy over TLS but
#   without client-cert verification.
#
# The settings go into the [DEFAULT] section inside a marked block (BEGIN/END FLOWFIRST TLS).
# Re-running replaces only that block. The first run saves proxy-server.conf.pre-tls for undo.
# Undo: cp /etc/swift/proxy-server.conf.pre-tls /etc/swift/proxy-server.conf && restart.
# ==============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
if [[ -f "${ROOT_DIR}/.env" ]]; then set -a; source "${ROOT_DIR}/.env"; set +a; fi   # shellcheck disable=SC1091

TLS_DIR="${TLS_DIR:-/etc/flowfirst/tls}"
SWIFT_CONF="${SWIFT_CONF:-/etc/swift/proxy-server.conf}"
SWIFT_USER="${SWIFT_USER:-swift}"
SWIFT_PROXY_PORT="${SWIFT_PROXY_PORT:-8888}"
BEGIN="# BEGIN FLOWFIRST TLS (managed by security/enable_swift_tls.sh - do not edit inside)"
END="# END FLOWFIRST TLS"

RESTART=1; PRINT=0
while [[ $# -gt 0 ]]; do
    case "$1" in
        --no-restart) RESTART=0 ;;
        --print)      PRINT=1 ;;
        -h|--help)    sed -n '2,33p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'; exit 0 ;;
        *) echo "ERROR: unknown option '$1'" >&2; exit 2 ;;
    esac
    shift
done

for v in TLS_DIR SWIFT_CONF; do
    [[ "${!v}" =~ ^/[A-Za-z0-9._/-]+$ ]] || { echo "ERROR: ${v} must be an absolute path of letters, digits and . _ / - ." >&2; exit 1; }
done
[[ "${SWIFT_USER}" =~ ^[a-z_][a-z0-9_-]*$ ]] || { echo "ERROR: invalid SWIFT_USER." >&2; exit 1; }

CRT="${TLS_DIR}/swift/swift.crt"; KEY="${TLS_DIR}/swift/swift.key"

# The block goes inside [DEFAULT]. Swift reads cert_file/key_file from [DEFAULT], not from any filter section.
block() {
    printf '%s\n' "${BEGIN}" \
        "cert_file = ${CRT}" \
        "key_file = ${KEY}" \
        "# NOTE: Swift does not request or verify a client certificate (ssl.CERT_NONE). See script header." \
        "${END}"
}

# Rewrite FILE: drop any existing managed block, then insert a fresh one right after the [DEFAULT] header.
insert_block() {
    local file="$1" injected=0 in_block=0
    while IFS= read -r line; do
        if [[ "${line}" == "${BEGIN}" ]]; then in_block=1; continue; fi
        if [[ "${line}" == "${END}" ]]; then in_block=0; continue; fi
        [[ ${in_block} -eq 1 ]] && continue
        printf '%s\n' "${line}"
        if [[ ${injected} -eq 0 && "${line}" == "[DEFAULT]" ]]; then
            block; injected=1
        fi
    done < "${file}"
    if [[ ${injected} -eq 0 ]]; then
        echo "ERROR: no [DEFAULT] section in ${file}; file not changed." >&2; return 3
    fi
}

if [[ ${PRINT} -eq 1 ]]; then block; exit 0; fi
if [[ ${EUID} -ne 0 ]]; then echo "ERROR: run with sudo (it writes ${SWIFT_CONF})." >&2; exit 1; fi

for f in "${CRT}" "${KEY}" "${SWIFT_CONF}"; do
    [[ -f "${f}" ]] || { echo "ERROR: ${f} not found. Run security/install_certs.sh (and swift/setup_swift.sh) first." >&2; exit 1; }
done
if id "${SWIFT_USER}" >/dev/null 2>&1; then
    for f in "${CRT}" "${KEY}"; do
        runuser -u "${SWIFT_USER}" -- test -r "${f}" \
            || { echo "ERROR: user '${SWIFT_USER}' cannot read ${f} (check owner/mode and directory permissions)." >&2; exit 1; }
    done
fi

echo "[1/2] ${SWIFT_CONF}"
[[ -f "${SWIFT_CONF}.pre-tls" ]] || cp -p "${SWIFT_CONF}" "${SWIFT_CONF}.pre-tls"
tmp="$(mktemp)"; insert_block "${SWIFT_CONF}" > "${tmp}" && cat "${tmp}" > "${SWIFT_CONF}"; rm -f "${tmp}"
chown "${SWIFT_USER}:${SWIFT_USER}" "${SWIFT_CONF}" 2>/dev/null || true; chmod 640 "${SWIFT_CONF}"
echo "  cert_file = ${CRT}"; echo "  key_file = ${KEY}"
echo "  NOTE: Swift does not verify an inbound client certificate (ssl.CERT_NONE). See script header."

if [[ ${RESTART} -eq 1 ]]; then
    echo "[2/2] restarting openstack-swift-proxy (roll through nodes one at a time)"
    systemctl restart openstack-swift-proxy
    sleep 3
    python3 -c "
import urllib.request, ssl
ctx = ssl.create_default_context(); ctx.check_hostname = False; ctx.verify_mode = ssl.CERT_NONE
try:
    r = urllib.request.urlopen('https://127.0.0.1:${SWIFT_PROXY_PORT}/healthcheck', context=ctx, timeout=5)
    print('  HTTPS healthcheck:', r.status)
except Exception as e:
    print('  HTTPS healthcheck not reachable yet:', e)
    print('  Check: systemctl status openstack-swift-proxy')
"
else
    echo "[2/2] --no-restart: run 'systemctl restart openstack-swift-proxy' to apply"
fi
echo "Undo: cp ${SWIFT_CONF}.pre-tls ${SWIFT_CONF} && systemctl restart openstack-swift-proxy"
