#!/usr/bin/env bash
# ==============================================================================
# FlowFirst - enable TLS on a MariaDB (Galera) node (run on each of node1-3, as a user with sudo).
#
#   sudo ./security/enable_mariadb_tls.sh                    client connections: server TLS 1.2/1.3 (plain clients still allowed)
#   sudo ./security/enable_mariadb_tls.sh --galera           ALSO encrypt Galera replication (:4567) - see warning
#   sudo ./security/enable_mariadb_tls.sh --require-client-cert   ALTER USER <app user> REQUIRE X509 (run on ONE node; it replicates)
#   sudo ./security/enable_mariadb_tls.sh --no-restart       write the configuration only
#   ./security/enable_mariadb_tls.sh --print [--galera]      print the configuration and exit (no root needed)
#
# Needs the certificates from security/install_certs.sh in TLS_DIR (default /etc/flowfirst/tls):
#   ca.crt  mariadb/mariadb.crt  mariadb/mariadb.key  (and ca.crl when TLS_CRL_FILE is set)
#
# The settings go into their own drop-in file (zz-flowfirst-tls.cnf, read AFTER galera.cnf so it wins),
# so galera.cnf is never edited. Delete that file and restart to undo.
#
# Safe order for a running cluster:
#   1. run this script on node1, node2, node3 one at a time (each restart rejoins the cluster by IST)
#   2. set TLS_MARIADB_ENABLED=true in .env on every node, restart the processes, confirm they connect
#   3. run --require-client-cert on one node: from then on the app user cannot connect without a certificate
#   --galera is different: a node using TLS replication cannot talk to one that does not, so stop MariaDB on
#   ALL nodes, run it on every node, then bootstrap the cluster again (mariadb-galera/bootstrap_galera.sh).
#   Not covered: with wsrep_sst_method=rsync a full state transfer (SST) is NOT encrypted by Galera TLS.
#   Keep the cluster on the private network, or switch the method to mariabackup with an [sst] TLS section.
#
# REVOCATION - VERIFIED for client certificates. With TLS_CRL_FILE set this script adds 'ssl_crl=<file>' and a
# certificate revoked in that CRL is refused ("sslv3 alert certificate revoked"); without it the same certificate
# connects (MariaDB 11.8 container, both cases tested). Two consequences to plan for:
#   - the server reads the file at start-up or on 'FLUSH SSL' (no restart needed). After install_certs.sh --crl-only
#     run:  mariadb -e "FLUSH SSL"  on every node, otherwise a new revocation is not enforced yet.
#   - once the CRL's nextUpdate passes, the server refuses EVERY client certificate (tested: "certificate expired").
#     Re-issue the CRL regularly with security/refresh_crl.sh (list_certs.sh warns 7 days ahead).
# Not tested: whether Galera replication (--galera) honours the CRL; assume it does not and use the node firewall.
# ==============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
if [[ -f "${ROOT_DIR}/.env" ]]; then set -a; source "${ROOT_DIR}/.env"; set +a; fi   # shellcheck disable=SC1091

TLS_DIR="${TLS_DIR:-/etc/flowfirst/tls}"
TLS_CRL_FILE="${TLS_CRL_FILE:-}"
TLS_MIN_VERSION="${TLS_MIN_VERSION:-1.2}"
TLS_CIPHERS="${TLS_CIPHERS:-ECDHE+AESGCM:ECDHE+CHACHA20}"
MY_CNF_DIR="${MY_CNF_DIR:-/etc/my.cnf.d}"
GALERA_CNF="${GALERA_CNF:-${MY_CNF_DIR}/galera.cnf}"
DROPIN="${MY_CNF_DIR}/zz-flowfirst-tls.cnf"
DB_USER="${MARIADB_USER:-flowuser}"

GALERA=0; REQUIRE=0; RESTART=1; PRINT=0
while [[ $# -gt 0 ]]; do
    case "$1" in
        --galera)              GALERA=1 ;;
        --require-client-cert) REQUIRE=1 ;;
        --no-restart)          RESTART=0 ;;
        --print)               PRINT=1 ;;
        -h|--help)             sed -n '2,33p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'; exit 0 ;;
        *) echo "ERROR: unknown option '$1'" >&2; exit 2 ;;
    esac
    shift
done

for v in TLS_DIR MY_CNF_DIR GALERA_CNF; do
    [[ "${!v}" =~ ^/[A-Za-z0-9._/-]+$ ]] || { echo "ERROR: ${v} must be an absolute path of letters, digits and . _ / - ." >&2; exit 1; }
done
[[ -z "${TLS_CRL_FILE}" || "${TLS_CRL_FILE}" =~ ^/[A-Za-z0-9._/-]+$ ]] || { echo "ERROR: TLS_CRL_FILE must be an absolute path of safe characters." >&2; exit 1; }
[[ "${TLS_MIN_VERSION}" =~ ^1\.[23]$ ]] || { echo "ERROR: TLS_MIN_VERSION must be 1.2 or 1.3." >&2; exit 1; }
[[ "${TLS_CIPHERS}" =~ ^[A-Za-z0-9:+!@_=-]+$ ]] || { echo "ERROR: TLS_CIPHERS contains unsafe characters." >&2; exit 1; }
[[ "${DB_USER}" =~ ^[A-Za-z_][A-Za-z0-9_]{0,31}$ ]] || { echo "ERROR: MARIADB_USER must be letters, digits and _ only." >&2; exit 1; }

CA="${TLS_DIR}/ca.crt"; CRT="${TLS_DIR}/mariadb/mariadb.crt"; KEY="${TLS_DIR}/mariadb/mariadb.key"

tls_versions() { [[ "${TLS_MIN_VERSION}" == "1.3" ]] && echo "TLSv1.3" || echo "TLSv1.2,TLSv1.3"; }

# Galera takes ONE wsrep_provider_options string; a later file replaces the whole value, so keep the
# options galera.cnf already sets (e.g. gcache.size) and append the TLS ones.
galera_options() {
    local base=""
    if [[ -f "${GALERA_CNF}" ]]; then
        base="$(sed -n 's/^[[:space:]]*wsrep_provider_options[[:space:]]*=[[:space:]]*"\{0,1\}\([^"]*\)"\{0,1\}[[:space:]]*$/\1/p' "${GALERA_CNF}" | tail -n 1)"
        base="${base%;}"
    fi
    local tls="socket.ssl=yes;socket.ssl_ca=${CA};socket.ssl_cert=${CRT};socket.ssl_key=${KEY}"
    [[ -n "${base}" ]] && echo "${base};${tls}" || echo "${tls}"
}

render() {
    cat <<EOF
# Managed by security/enable_mariadb_tls.sh - read after galera.cnf. Delete this file and restart to undo.
[mysqld]
ssl_ca=${CA}
ssl_cert=${CRT}
ssl_key=${KEY}
tls_version=$(tls_versions)
ssl_cipher=${TLS_CIPHERS}
EOF
    [[ -n "${TLS_CRL_FILE}" ]] && echo "ssl_crl=${TLS_CRL_FILE}"
    if [[ ${GALERA} -eq 1 ]]; then
        echo "wsrep_provider_options=\"$(galera_options)\""
    fi
}

if [[ ${PRINT} -eq 1 ]]; then render; exit 0; fi
if [[ ${EUID} -ne 0 ]]; then echo "ERROR: run with sudo (it writes ${MY_CNF_DIR})." >&2; exit 1; fi

# Fail closed: refuse to enable TLS when the service account cannot read its certificate and key,
# otherwise the server would not start (and under Galera the node would not rejoin).
files=("${CA}" "${CRT}" "${KEY}"); [[ -n "${TLS_CRL_FILE}" ]] && files+=("${TLS_CRL_FILE}")
for f in "${files[@]}"; do
    [[ -f "${f}" ]] || { echo "ERROR: ${f} not found. Run security/install_certs.sh first." >&2; exit 1; }
done
if id mysql >/dev/null 2>&1; then
    for f in "${files[@]}"; do
        runuser -u mysql -- test -r "${f}" || { echo "ERROR: user 'mysql' cannot read ${f} (check owner/mode and directory permissions)." >&2; exit 1; }
    done
fi

if [[ ${REQUIRE} -eq 1 ]]; then
    # Done on its own: it replicates through Galera, so ONE node is enough.
    echo "Requiring a client certificate for '${DB_USER}' (any certificate signed by the FlowFirst CA)..."
    mariadb -e "ALTER USER '${DB_USER}'@'%' REQUIRE X509; ALTER USER '${DB_USER}'@'localhost' REQUIRE X509;"
    mariadb -e "SELECT user, host, ssl_type FROM mysql.user WHERE user='${DB_USER}';"
    exit 0
fi

echo "[1/3] ${DROPIN}"
mkdir -p "${MY_CNF_DIR}"
[[ -f "${DROPIN}" ]] && cp -p "${DROPIN}" "${DROPIN}.bak"
render > "${DROPIN}"
chown root:mysql "${DROPIN}" 2>/dev/null || true; chmod 640 "${DROPIN}"
echo "      client TLS: on   Galera replication TLS: $([[ ${GALERA} -eq 1 ]] && echo on || echo 'off (use --galera)')"

echo "[2/3] firewall: no new port (TLS uses the existing 3306 and 4567)"

if [[ ${RESTART} -eq 1 ]]; then
    echo "[3/3] restarting mariadb on this node (roll through the nodes one at a time)"
    systemctl restart mariadb
    sleep 3
    mariadb -N -e "SHOW GLOBAL VARIABLES WHERE Variable_name IN ('have_ssl','tls_version','ssl_cipher');" 2>/dev/null \
        || echo "  (could not query the server yet - check: systemctl status mariadb)"
else
    echo "[3/3] --no-restart: run 'systemctl restart mariadb' to apply"
fi
echo "Undo: rm ${DROPIN} && systemctl restart mariadb"
