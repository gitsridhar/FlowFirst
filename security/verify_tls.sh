#!/usr/bin/env bash
# ==============================================================================
# FlowFirst - probe every TLS endpoint of the cluster and report PASS / FAIL.
#
#   ./security/verify_tls.sh                      run from a cluster node (uses its pipeline certificate)
#   ./security/verify_tls.sh --env /path/.env     read addresses, ports and TLS switches from another .env
#   ./security/verify_tls.sh --ca ca.crt --cert admin-client.crt --key admin-client.key
#                                                 run from an admin host with the admin-client identity
#
# Only components whose switch is on in the .env are probed (TLS_API_ENABLED, TLS_RABBITMQ_ENABLED,
# TLS_MARIADB_ENABLED, TLS_ZK_ENABLED, TLS_SWIFT_ENABLED); the others are reported as SKIP.
# Exit code 0 = every probed check passed, 1 = at least one FAIL.
#
# What a PASS proves, per endpoint:
#   * "accepts valid client cert"  a TLS >= 1.2 handshake completed, the server certificate chains to the FlowFirst CA
#                                  and matches the address that was dialled
#   * "refuses no client cert"     a client without a certificate is rejected (also after the TLS 1.3 handshake)
#   * "refuses plain text"         a plain-text HTTP request gets no 2xx answer
# What it does NOT prove: revocation (see ReadMe Scenario 63), cipher strength beyond what the client negotiated,
# or that a legacy TLS 1.0/1.1 client is refused (modern OpenSSL cannot even offer those versions).
# MariaDB uses openssl's -starttls mysql, so it needs the openssl command; everything else needs python3.
# ==============================================================================
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
ENV_FILE="${ROOT_DIR}/.env"; CA=""; CERT=""; KEY=""
while [[ $# -gt 0 ]]; do
    case "$1" in
        --env)  ENV_FILE="${2:-}"; shift 2 ;;
        --ca)   CA="${2:-}"; shift 2 ;;
        --cert) CERT="${2:-}"; shift 2 ;;
        --key)  KEY="${2:-}"; shift 2 ;;
        -h|--help) sed -n '2,23p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'; exit 0 ;;
        *) echo "ERROR: unknown option '$1'" >&2; exit 2 ;;
    esac
done
[[ -f "${ENV_FILE}" ]] || { echo "ERROR: ${ENV_FILE} not found (use --env)." >&2; exit 2; }
set -a; source "${ENV_FILE}"; set +a   # shellcheck disable=SC1090

TLS_DIR="${TLS_DIR:-/etc/flowfirst/tls}"
CA="${CA:-${TLS_CA_FILE:-${TLS_DIR}/ca.crt}}"
CERT="${CERT:-${TLS_CLIENT_CERT:-${TLS_DIR}/pipeline/pipeline.crt}}"
KEY="${KEY:-${TLS_CLIENT_KEY:-${TLS_DIR}/pipeline/pipeline.key}}"
for f in "${CA}" "${CERT}" "${KEY}"; do [[ -r "${f}" ]] || { echo "ERROR: cannot read ${f}" >&2; exit 2; }; done
command -v python3 >/dev/null 2>&1 || { echo "ERROR: python3 not found." >&2; exit 2; }

is_true() { case "$(printf %s "${1:-}" | tr A-Z a-z)" in 1|true|yes|on) return 0 ;; *) return 1 ;; esac; }
NODES=(); for n in 1 2 3; do v="NODE${n}_IP"; [[ -n "${!v:-}" ]] && NODES+=("${!v}"); done
VIP="${FLOWFIRST_VIP:-}"
[[ ${#NODES[@]} -gt 0 && -n "${VIP}" ]] || { echo "ERROR: NODE1_IP..NODE3_IP and FLOWFIRST_VIP must be set in ${ENV_FILE}." >&2; exit 2; }

# --- the one place that talks TLS (python3) -----------------------------------
PROBE="$(mktemp)"; trap 'rm -f "${PROBE}"' EXIT
cat > "${PROBE}" <<'PY'
import socket, ssl, sys
host, port, mode, ca, cert, key = sys.argv[1:7]; port = int(port)
def out(msg): print(msg); sys.exit(0)
try:
    sock = socket.create_connection((host, port), timeout=6)
except Exception as e:
    out("UNREACHABLE %s: %s" % (type(e).__name__, e))
REQ = b"GET /health HTTP/1.0\r\n\r\n"
if mode == "plain":
    data = b""
    try:
        sock.settimeout(4); sock.sendall(REQ); data = sock.recv(64)
    except Exception:
        pass
    if data.startswith(b"HTTP/") and data[8:10] == b" 2":
        out("PLAINTEXT-ANSWERED " + data[:20].decode("latin1").strip())
    out("PLAINTEXT-REFUSED")
ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
ctx.load_verify_locations(ca)
ctx.minimum_version = ssl.TLSVersion.TLSv1_2
if mode == "cert":
    ctx.load_cert_chain(cert, key)
try:
    t = ctx.wrap_socket(sock, server_hostname=host)
except ssl.SSLCertVerificationError as e:
    out("SERVER-CERT-REJECTED %s" % e.verify_message)
except ssl.SSLError as e:
    out("HANDSHAKE-REFUSED %s" % (e.reason or e))
except OSError as e:
    out("HANDSHAKE-REFUSED %s" % type(e).__name__)
# TLS 1.3 completes the handshake before the server judges the client certificate: provoke the alert.
t.settimeout(3)
try:
    t.sendall(REQ); t.recv(1)
except ssl.SSLError as e:
    out("REFUSED-AFTER-HANDSHAKE %s" % (e.reason or e))
except socket.timeout:
    pass
except OSError as e:
    out("AMBIGUOUS closed after handshake without an alert (%s)" % type(e).__name__)
cn = "-"
for rdn in (t.getpeercert() or {}).get("subject", ()):
    for k, v in rdn:
        if k == "commonName": cn = v
out("ACCEPTED %s %s server-cn=%s" % (t.version(), t.cipher()[0], cn))
PY

with_timeout() { local s=$1; shift; if command -v timeout >/dev/null 2>&1; then timeout "$s" "$@"; else perl -e 'alarm shift; exec @ARGV' "$s" "$@"; fi; }

PASS=0; FAILS=0
pass() { printf '  PASS  %-52s %s\n' "$1" "${2:-}"; PASS=$((PASS+1)); }
fail() { printf '  FAIL  %-52s %s\n' "$1" "${2:-}"; FAILS=$((FAILS+1)); }
info() { printf '  INFO  %-52s %s\n' "$1" "${2:-}"; }
skip() { printf '  SKIP  %s\n' "$1"; }
probe() { with_timeout 25 python3 "${PROBE}" "$1" "$2" "$3" "${CA}" "${CERT}" "${KEY}" 2>&1 | tail -n 1; }

check_accepts() {   # check_accepts LABEL HOST PORT : valid client certificate must be accepted
    local r; r="$(probe "$2" "$3" cert)"
    if [[ "${r}" == ACCEPTED* ]]; then pass "$1 accepts valid client cert" "${r#ACCEPTED }"; else fail "$1 accepts valid client cert" "${r}"; fi
}
check_refuses_nocert() {   # a client without a certificate must be refused
    local r; r="$(probe "$2" "$3" nocert)"
    case "${r}" in
        HANDSHAKE-REFUSED*|REFUSED-AFTER-HANDSHAKE*) pass "$1 refuses no client cert" "${r}" ;;
        ACCEPTED*) fail "$1 refuses no client cert" "ACCEPTED without a certificate" ;;
        *) fail "$1 refuses no client cert" "could not show a refusal: ${r}" ;;
    esac
}
check_accepts_nocert() {   # server is known not to ask for a client certificate (documented limit)
    local r; r="$(probe "$2" "$3" nocert)"
    if [[ "${r}" == ACCEPTED* ]]; then info "$1 without client cert" "accepted (documented: this server does not ask for one)"; else info "$1 without client cert" "${r}"; fi
}
check_plain_refused() {
    local r; r="$(probe "$2" "$3" plain)"
    case "${r}" in
        PLAINTEXT-REFUSED) pass "$1 refuses plain text" ;;
        UNREACHABLE*) fail "$1 refuses plain text" "${r}" ;;
        *) fail "$1 refuses plain text" "${r}" ;;
    esac
}
check_tls_only() {   # TLS without client certificate must work (server cert check only), e.g. the stats page
    local r; r="$(probe "$2" "$3" nocert)"
    if [[ "${r}" == ACCEPTED* ]]; then pass "$1 serves TLS (server cert verified)" "${r#ACCEPTED }"; else fail "$1 serves TLS (server cert verified)" "${r}"; fi
}
plain_port_open() {   # informational: a plain port that is still listening
    local r; r="$(with_timeout 8 python3 -c "import socket,sys
try:
    socket.create_connection((sys.argv[1], int(sys.argv[2])), timeout=4).close(); print('open')
except Exception:
    print('closed')" "$2" "$3" 2>&1 | tail -n 1)"
    info "$1 plain port $3" "${r} (close it once every client uses TLS)"
}

echo "FlowFirst TLS verification   CA=${CA}"
echo "  client identity: ${CERT}"

echo "[REST API]"
if is_true "${TLS_API_ENABLED:-false}"; then
    AUTH="${TLS_API_CLIENT_AUTH:-required}"
    check_accepts "VIP ${VIP}:${API_PORT:-8080}" "${VIP}" "${API_PORT:-8080}"
    [[ "${AUTH}" == "required" ]] && check_refuses_nocert "VIP ${VIP}:${API_PORT:-8080}" "${VIP}" "${API_PORT:-8080}"
    check_plain_refused "VIP ${VIP}:${API_PORT:-8080}" "${VIP}" "${API_PORT:-8080}"
    check_tls_only "stats ${VIP}:${HAPROXY_STATS_PORT:-9000}" "${VIP}" "${HAPROXY_STATS_PORT:-9000}"
    for ip in "${NODES[@]}"; do
        check_accepts "node ${ip}:${API_BACKEND_PORT:-8082}" "${ip}" "${API_BACKEND_PORT:-8082}"
        [[ "${AUTH}" == "required" ]] && check_refuses_nocert "node ${ip}:${API_BACKEND_PORT:-8082}" "${ip}" "${API_BACKEND_PORT:-8082}"
        check_plain_refused "node ${ip}:${API_BACKEND_PORT:-8082}" "${ip}" "${API_BACKEND_PORT:-8082}"
    done
else skip "TLS_API_ENABLED is off"; fi

echo "[Swift]"
if is_true "${TLS_SWIFT_ENABLED:-false}"; then
    check_accepts "VIP ${VIP}:${SWIFT_LB_PORT:-8090}" "${VIP}" "${SWIFT_LB_PORT:-8090}"
    check_refuses_nocert "VIP ${VIP}:${SWIFT_LB_PORT:-8090}" "${VIP}" "${SWIFT_LB_PORT:-8090}"
    for ip in "${NODES[@]}"; do
        check_accepts "node ${ip}:${SWIFT_PROXY_PORT:-8888}" "${ip}" "${SWIFT_PROXY_PORT:-8888}"
        check_accepts_nocert "node ${ip}:${SWIFT_PROXY_PORT:-8888}" "${ip}" "${SWIFT_PROXY_PORT:-8888}"
    done
else skip "TLS_SWIFT_ENABLED is off"; fi

echo "[RabbitMQ]"
if is_true "${TLS_RABBITMQ_ENABLED:-false}"; then
    for ip in "${NODES[@]}"; do
        check_accepts "node ${ip}:${RABBITMQ_TLS_PORT:-5671}" "${ip}" "${RABBITMQ_TLS_PORT:-5671}"
        check_refuses_nocert "node ${ip}:${RABBITMQ_TLS_PORT:-5671}" "${ip}" "${RABBITMQ_TLS_PORT:-5671}"
        plain_port_open "node ${ip}" "${ip}" "${RABBITMQ_PORT:-5672}"
    done
    check_accepts "VIP ${VIP}:${RABBITMQ_LB_PORT:-5673} (TCP pass-through)" "${VIP}" "${RABBITMQ_LB_PORT:-5673}"
else skip "TLS_RABBITMQ_ENABLED is off"; fi

echo "[MariaDB]"
if is_true "${TLS_MARIADB_ENABLED:-false}"; then
    if command -v openssl >/dev/null 2>&1; then
        mdb() {   # mdb LABEL HOST PORT : STARTTLS handshake with the client certificate, server cert verified
            local r; r="$(with_timeout 25 openssl s_client -starttls mysql -connect "$2:$3" -CAfile "${CA}" -cert "${CERT}" -key "${KEY}" \
                          -verify_hostname "$2" -verify_return_error -brief </dev/null 2>&1)"
            if grep -q "Verification: OK" <<<"${r}"; then pass "$1 accepts valid client cert" "$(grep -m1 -E '^Protocol version' <<<"${r}" | sed 's/Protocol version: //')"
            else fail "$1 accepts valid client cert" "$(tr '\n' ' ' <<<"${r}" | cut -c1-110)"; fi
        }
        for ip in "${NODES[@]}"; do mdb "node ${ip}:${MARIADB_PORT:-3306}" "${ip}" "${MARIADB_PORT:-3306}"; done
        mdb "VIP ${VIP}:${MARIADB_LB_PORT:-3307} (TCP pass-through)" "${VIP}" "${MARIADB_LB_PORT:-3307}"
        info "MariaDB client cert" "required only if the app user has REQUIRE X509 (enable_mariadb_tls.sh --require-client-cert)"
    else fail "MariaDB" "openssl command not found"; fi
else skip "TLS_MARIADB_ENABLED is off"; fi

echo "[ZooKeeper]"
if is_true "${TLS_ZK_ENABLED:-false}"; then
    for ip in "${NODES[@]}"; do
        check_accepts "node ${ip}:${ZK_TLS_CLIENT_PORT:-2281}" "${ip}" "${ZK_TLS_CLIENT_PORT:-2281}"
        check_refuses_nocert "node ${ip}:${ZK_TLS_CLIENT_PORT:-2281}" "${ip}" "${ZK_TLS_CLIENT_PORT:-2281}"
        plain_port_open "node ${ip}" "${ip}" "${ZK_CLIENT_PORT:-2181}"
    done
else skip "TLS_ZK_ENABLED is off"; fi

echo
echo "Result: ${PASS} passed, ${FAILS} failed"
[[ ${FAILS} -eq 0 ]]
