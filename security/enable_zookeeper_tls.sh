#!/usr/bin/env bash
# ==============================================================================
# FlowFirst - enable TLS on a ZooKeeper node (run on each of node1-3, as a user with sudo).
#
#   sudo ./security/enable_zookeeper_tls.sh                 add the TLS client port (secureClientPort, default 2281)
#                                                           with a REQUIRED client certificate; the plain port stays on
#   sudo ./security/enable_zookeeper_tls.sh --disable-plain also stop listening on the plain clientPort (2181)
#   sudo ./security/enable_zookeeper_tls.sh --quorum        also encrypt the traffic between the ZooKeeper servers
#                                                           (2888/3888) - see warning
#   sudo ./security/enable_zookeeper_tls.sh --no-restart    write the configuration and keystores only
#   ./security/enable_zookeeper_tls.sh --print              print the zoo.cfg block and exit (no root needed)
#
# Needs the certificates from security/install_certs.sh in TLS_DIR (default /etc/flowfirst/tls):
#   ca.crt  zookeeper/zookeeper.crt  zookeeper/zookeeper.key
# ZooKeeper is Java, so the script builds two PKCS12 files from those PEMs (needs openssl and keytool):
#   zookeeper/keystore.p12   server identity      zookeeper/truststore.p12   the FlowFirst CA
# plus zookeeper/keystore.pass (random password, owner zookeeper, mode 600). It is read through
# ssl.*.passwordPath, so the password is not written into zoo.cfg.
#
# The settings live in a marked block at the end of zoo.cfg (BEGIN/END FLOWFIRST TLS); re-running replaces it.
# The first run saves zoo.cfg.pre-tls for undo.
#
# Safe order: run on node1, node2, node3 one at a time (a restart makes that node rejoin the quorum; two of three
# must stay up). Then set TLS_ZK_ENABLED=true in .env on every node (ZK_TLS_CLIENT_PORT must match) and restart the
# processes. Only then think about --disable-plain.
#   --quorum needs one maintenance window: a server using TLS quorum cannot talk to one that does not.
#
# Consequences to know about:
#   * 4-letter words (ruok, mntr, ...) do not work on the TLS port. Use the plain port while it exists, or the
#     admin server: curl http://127.0.0.1:8081/commands/ruok  (ZK_ADMIN_PORT).
#   * Revocation: ZooKeeper's TLS does not read ca.crl. Same remedy as for RabbitMQ: re-issue + short lifetimes.
# ==============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
if [[ -f "${ROOT_DIR}/.env" ]]; then set -a; source "${ROOT_DIR}/.env"; set +a; fi   # shellcheck disable=SC1091

TLS_DIR="${TLS_DIR:-/etc/flowfirst/tls}"
ZK_INSTALL_DIR="${ZK_INSTALL_DIR:-/opt/zookeeper}"
ZK_CONF="${ZK_CONF:-${ZK_INSTALL_DIR}/conf/zoo.cfg}"
ZK_TLS_CLIENT_PORT="${ZK_TLS_CLIENT_PORT:-2281}"
TLS_MIN_VERSION="${TLS_MIN_VERSION:-1.2}"
ZK_USER="${ZK_USER:-zookeeper}"

DISABLE_PLAIN=0; QUORUM=0; RESTART=1; PRINT=0
while [[ $# -gt 0 ]]; do
    case "$1" in
        --disable-plain) DISABLE_PLAIN=1 ;;
        --quorum)        QUORUM=1 ;;
        --no-restart)    RESTART=0 ;;
        --print)         PRINT=1 ;;
        -h|--help)       sed -n '2,33p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'; exit 0 ;;
        *) echo "ERROR: unknown option '$1'" >&2; exit 2 ;;
    esac
    shift
done

for v in TLS_DIR ZK_INSTALL_DIR ZK_CONF; do
    [[ "${!v}" =~ ^/[A-Za-z0-9._/-]+$ ]] || { echo "ERROR: ${v} must be an absolute path of letters, digits and . _ / - ." >&2; exit 1; }
done
[[ "${ZK_TLS_CLIENT_PORT}" =~ ^[1-9][0-9]*$ ]] || { echo "ERROR: ZK_TLS_CLIENT_PORT must be a positive integer." >&2; exit 1; }
[[ "${TLS_MIN_VERSION}" =~ ^1\.[23]$ ]] || { echo "ERROR: TLS_MIN_VERSION must be 1.2 or 1.3." >&2; exit 1; }
[[ "${ZK_USER}" =~ ^[a-z_][a-z0-9_-]*$ ]] || { echo "ERROR: invalid ZK_USER." >&2; exit 1; }

D="${TLS_DIR}/zookeeper"
CA="${TLS_DIR}/ca.crt"; CRT="${D}/zookeeper.crt"; KEY="${D}/zookeeper.key"
KS="${D}/keystore.p12"; TS="${D}/truststore.p12"; PW="${D}/keystore.pass"

protocols() { [[ "${TLS_MIN_VERSION}" == "1.3" ]] && echo "TLSv1.3" || echo "TLSv1.3,TLSv1.2"; }

block() {
    cat <<EOF
# BEGIN FLOWFIRST TLS (managed by security/enable_zookeeper_tls.sh - do not edit inside)
serverCnxnFactory=org.apache.zookeeper.server.NettyServerCnxnFactory
secureClientPort=${ZK_TLS_CLIENT_PORT}
ssl.keyStore.location=${KS}
ssl.keyStore.passwordPath=${PW}
ssl.keyStore.type=PKCS12
ssl.trustStore.location=${TS}
ssl.trustStore.passwordPath=${PW}
ssl.trustStore.type=PKCS12
ssl.clientAuth=need
ssl.enabledProtocols=$(protocols)
ssl.hostnameVerification=true
EOF
    if [[ ${QUORUM} -eq 1 ]]; then
        cat <<EOF
sslQuorum=true
ssl.quorum.keyStore.location=${KS}
ssl.quorum.keyStore.passwordPath=${PW}
ssl.quorum.keyStore.type=PKCS12
ssl.quorum.trustStore.location=${TS}
ssl.quorum.trustStore.passwordPath=${PW}
ssl.quorum.trustStore.type=PKCS12
ssl.quorum.clientAuth=need
ssl.quorum.enabledProtocols=$(protocols)
ssl.quorum.hostnameVerification=true
EOF
    fi
    echo "# END FLOWFIRST TLS"
}

if [[ ${PRINT} -eq 1 ]]; then block; exit 0; fi
if [[ ${EUID} -ne 0 ]]; then echo "ERROR: run with sudo (it writes ${ZK_CONF} and ${D})." >&2; exit 1; fi

for tool in openssl keytool; do
    command -v "${tool}" >/dev/null 2>&1 || { echo "ERROR: '${tool}' not found (keytool comes with java-17-openjdk-headless)." >&2; exit 1; }
done
for f in "${CA}" "${CRT}" "${KEY}" "${ZK_CONF}"; do
    [[ -f "${f}" ]] || { echo "ERROR: ${f} not found. Run security/install_certs.sh (and zookeeper/setup_zookeeper.sh) first." >&2; exit 1; }
done

echo "[1/4] building ${KS} and ${TS}"
umask 077
pass="$(openssl rand -hex 24)"
tmp="$(mktemp -d)"; trap 'rm -rf "${tmp}"' EXIT
openssl pkcs12 -export -inkey "${KEY}" -in "${CRT}" -certfile "${CA}" -name zookeeper -passout "pass:${pass}" -out "${tmp}/keystore.p12"
keytool -importcert -noprompt -alias flowfirst-ca -file "${CA}" -storetype PKCS12 -keystore "${tmp}/truststore.p12" -storepass "${pass}" >/dev/null
printf '%s' "${pass}" > "${tmp}/keystore.pass"
install -m 600 "${tmp}/keystore.p12" "${KS}"; install -m 600 "${tmp}/truststore.p12" "${TS}"; install -m 600 "${tmp}/keystore.pass" "${PW}"
if id "${ZK_USER}" >/dev/null 2>&1; then
    chown "${ZK_USER}:${ZK_USER}" "${KS}" "${TS}" "${PW}"
    runuser -u "${ZK_USER}" -- test -r "${KS}" -a -r "${TS}" -a -r "${PW}" \
        || { echo "ERROR: user '${ZK_USER}' cannot read the keystores in ${D} (check directory permissions)." >&2; exit 1; }
fi

echo "[2/4] ${ZK_CONF}  (secureClientPort ${ZK_TLS_CLIENT_PORT}, client certificate required, quorum TLS: $([[ ${QUORUM} -eq 1 ]] && echo on || echo off), plain port: $([[ ${DISABLE_PLAIN} -eq 1 ]] && echo closed || echo kept))"
[[ -f "${ZK_CONF}.pre-tls" ]] || cp -p "${ZK_CONF}" "${ZK_CONF}.pre-tls"
sed -e '/^# BEGIN FLOWFIRST TLS/,/^# END FLOWFIRST TLS/d' "${ZK_CONF}" > "${tmp}/zoo.cfg"
if [[ ${DISABLE_PLAIN} -eq 1 ]]; then sed -i 's/^clientPort=/#clientPort=/' "${tmp}/zoo.cfg"; fi
{ cat "${tmp}/zoo.cfg"; echo; block; } > "${ZK_CONF}"
chown "${ZK_USER}:${ZK_USER}" "${ZK_CONF}" 2>/dev/null || true

echo "[3/4] firewall"
if command -v firewall-cmd >/dev/null 2>&1 && systemctl is-active --quiet firewalld; then
    firewall-cmd --permanent --add-port="${ZK_TLS_CLIENT_PORT}/tcp" >/dev/null && firewall-cmd --reload >/dev/null
    echo "  opened ${ZK_TLS_CLIENT_PORT}/tcp"
else
    echo "  firewalld not active - nothing to do"
fi

if [[ ${RESTART} -eq 1 ]]; then
    echo "[4/4] restarting zookeeper on this node (roll through the nodes one at a time; keep 2 of 3 up)"
    systemctl restart zookeeper
    sleep 5
    (echo srvr | nc -w2 127.0.0.1 "${ZK_CLIENT_PORT:-2181}" 2>/dev/null | grep -E "Mode|Zookeeper version") || echo "  (plain port not answering - check: journalctl -u zookeeper)"
else
    echo "[4/4] --no-restart: run 'systemctl restart zookeeper' to apply"
fi
echo "Undo: cp ${ZK_CONF}.pre-tls ${ZK_CONF} && systemctl restart zookeeper"
