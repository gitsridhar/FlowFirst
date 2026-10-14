#!/usr/bin/env bash
set -euo pipefail

# ==============================================================================
# FlowFirst - HAProxy Setup Script for 3-Node Cluster on RHEL 9.6
# Automatically loads network configuration from .env if present
# ==============================================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

# Source environment variables if .env exists
if [[ -f "${ROOT_DIR}/.env" ]]; then
    set -a
    # shellcheck disable=SC1091
    source "${ROOT_DIR}/.env"
    set +a
fi

NODE1_NAME="${NODE1_NAME:-node1}"
NODE1_IP="${1:-${NODE1_IP:-}}"
NODE2_NAME="${NODE2_NAME:-node2}"
NODE2_IP="${2:-${NODE2_IP:-}}"
NODE3_NAME="${NODE3_NAME:-node3}"
NODE3_IP="${3:-${NODE3_IP:-}}"
VIP="${4:-${FLOWFIRST_VIP:-}}"

# Validate required IP variables are set (must come from .env)
for _var in NODE1_IP NODE2_IP NODE3_IP FLOWFIRST_VIP; do
    _val="${!_var:-}"
    if [[ -z "${_val}" ]] || [[ "${_val}" =~ ^\$\{ ]]; then
        echo "ERROR: ${_var} is not set. Set it in /opt/flowfirst/.env and re-run."
        exit 1
    fi
done
VIP="${VIP:-${FLOWFIRST_VIP}}"
VIP_NIC="${VIP_NIC:-}"           # set in .env — see pre-flight check below
API_PORT="${API_PORT:-8080}"
API_BACKEND_PORT="${API_BACKEND_PORT:-8082}"
MARIADB_PORT="${MARIADB_PORT:-3306}"
MARIADB_LB_PORT="${MARIADB_LB_PORT:-3307}"
RABBITMQ_PORT="${RABBITMQ_PORT:-5672}"
RABBITMQ_LB_PORT="${RABBITMQ_LB_PORT:-5673}"
SWIFT_PROXY_PORT="${SWIFT_PROXY_PORT:-8888}"
SWIFT_LB_PORT="${SWIFT_LB_PORT:-8090}"
HAPROXY_STATS_PORT="${HAPROXY_STATS_PORT:-9000}"
HAPROXY_STATS_USER="${HAPROXY_STATS_USER:-admin}"
HAPROXY_STATS_PASS="${HAPROXY_STATS_PASS:-}"
# shellcheck disable=SC1091
source "${ROOT_DIR}/scripts/secrets_lib.sh"
require_strong_secret HAPROXY_STATS_PASS "${HAPROXY_STATS_PASS}"
# Scale limits (defaults = the values that were previously hard-coded in the template)
HAPROXY_MAXCONN="${HAPROXY_MAXCONN:-4000}"                      # global: max concurrent connections for the whole process
HAPROXY_FRONTEND_MAXCONN="${HAPROXY_FRONTEND_MAXCONN:-3000}"    # defaults: max concurrent connections per frontend
for _var in HAPROXY_MAXCONN HAPROXY_FRONTEND_MAXCONN; do
    if ! [[ "${!_var}" =~ ^[1-9][0-9]*$ ]]; then
        echo "ERROR: ${_var} must be a positive integer (got '${!_var}')."
        exit 1
    fi
done

# ---------------------------------------------------------------------------
# TLS (see security/ and ReadMe Scenario 63).  Every switch is OFF by default, in which case the
# generated config is the same plain-text config as before.
#   TLS_API_ENABLED     HTTPS on the VIP API frontend (+ stats page) and HTTPS to the Process 1 backends.
#                       TLS_API_CLIENT_AUTH: required = mutual TLS (default) | optional | none
#   TLS_SWIFT_ENABLED   HTTPS + required client certificate on the Swift frontend, HTTPS to the swift-proxy nodes
#   TLS_RABBITMQ_ENABLED  RabbitMQ is balanced in TCP mode (TLS end to end); only the backend port changes
#   MariaDB             TCP pass-through too; nothing to change here
# ---------------------------------------------------------------------------
TLS_DIR="${TLS_DIR:-/etc/flowfirst/tls}"
TLS_CA_FILE="${TLS_CA_FILE:-${TLS_DIR}/ca.crt}"
TLS_CRL_FILE="${TLS_CRL_FILE:-}"
TLS_MIN_VERSION="${TLS_MIN_VERSION:-1.2}"
TLS_CIPHERS="${TLS_CIPHERS:-ECDHE+AESGCM:ECDHE+CHACHA20}"
TLS_API_ENABLED="${TLS_API_ENABLED:-false}"
TLS_API_CLIENT_AUTH="${TLS_API_CLIENT_AUTH:-required}"
TLS_SWIFT_ENABLED="${TLS_SWIFT_ENABLED:-false}"
TLS_RABBITMQ_ENABLED="${TLS_RABBITMQ_ENABLED:-false}"
RABBITMQ_TLS_PORT="${RABBITMQ_TLS_PORT:-5671}"
HAPROXY_VIP_PEM="${HAPROXY_VIP_PEM:-${TLS_DIR}/haproxy/vip.pem}"          # VIP server cert + key (one file)
HAPROXY_BACKEND_PEM="${HAPROXY_BACKEND_PEM:-${TLS_DIR}/haproxy/backend.pem}"  # client cert + key HAProxy presents to backends

_is_true() { case "$(printf %s "$1" | tr A-Z a-z)" in 1|true|yes|on) return 0 ;; *) return 1 ;; esac; }
for _var in TLS_DIR TLS_CA_FILE TLS_CRL_FILE HAPROXY_VIP_PEM HAPROXY_BACKEND_PEM; do
    _val="${!_var}"
    if [[ -n "${_val}" ]] && ! [[ "${_val}" =~ ^/[A-Za-z0-9._/-]+$ ]]; then
        echo "ERROR: ${_var} must be an absolute path of letters, digits and . _ / - (got '${_val}')."
        exit 1
    fi
done
[[ "${TLS_MIN_VERSION}" =~ ^1\.[23]$ ]] || { echo "ERROR: TLS_MIN_VERSION must be 1.2 or 1.3 (got '${TLS_MIN_VERSION}')."; exit 1; }
[[ "${TLS_CIPHERS}" =~ ^[A-Za-z0-9:+!@_=-]+$ ]] || { echo "ERROR: TLS_CIPHERS contains characters outside A-Za-z0-9:+!@_=- ."; exit 1; }
[[ "${TLS_API_CLIENT_AUTH}" =~ ^(required|optional|none)$ ]] || { echo "ERROR: TLS_API_CLIENT_AUTH must be required, optional or none."; exit 1; }
[[ "${RABBITMQ_TLS_PORT}" =~ ^[1-9][0-9]*$ ]] || { echo "ERROR: RABBITMQ_TLS_PORT must be a positive integer."; exit 1; }

# Values substituted into the template placeholders (empty = plain text, as before TLS existed)
HAP_TLS_MIN="TLSv${TLS_MIN_VERSION}"
HAP_STATS_TLS="" HAP_API_BIND_TLS="" HAP_API_ACL="" HAP_SWIFT_BIND_TLS=""
HAP_API_TLS_N1="" HAP_API_TLS_N2="" HAP_API_TLS_N3="" HAP_SWIFT_TLS_N1="" HAP_SWIFT_TLS_N2="" HAP_SWIFT_TLS_N3=""
HAP_RABBITMQ_BACKEND_PORT="${RABBITMQ_PORT}"
_crl=""; [[ -n "${TLS_CRL_FILE}" ]] && _crl=" crl-file ${TLS_CRL_FILE}"
# Backends are addressed by IP, so the node name must be given explicitly as SNI and verifyhost (live-tested:
# without 'sni' HAProxy 3.x failed the backend handshake with "certificate verify failed").
_server_tls() {   # _server_tls NODE_NAME : TLS to a backend node, verified against the CA and the node name
    echo "ssl ca-file ${TLS_CA_FILE}${_crl} crt ${HAPROXY_BACKEND_PEM} verify required sni str($1) verifyhost $1"
}
if _is_true "${TLS_API_ENABLED}"; then
    HAP_STATS_TLS="ssl crt ${HAPROXY_VIP_PEM}"
    case "${TLS_API_CLIENT_AUTH}" in
        required) HAP_API_BIND_TLS="ssl crt ${HAPROXY_VIP_PEM} ca-file ${TLS_CA_FILE}${_crl} verify required"
                  HAP_API_ACL="http-request deny deny_status 403 unless { ssl_c_used }" ;;
        optional) HAP_API_BIND_TLS="ssl crt ${HAPROXY_VIP_PEM} ca-file ${TLS_CA_FILE}${_crl} verify optional" ;;
        none)     HAP_API_BIND_TLS="ssl crt ${HAPROXY_VIP_PEM}" ;;
    esac
    HAP_API_TLS_N1="$(_server_tls "${NODE1_NAME}")"
    HAP_API_TLS_N2="$(_server_tls "${NODE2_NAME}")"
    HAP_API_TLS_N3="$(_server_tls "${NODE3_NAME}")"
fi
if _is_true "${TLS_SWIFT_ENABLED}"; then
    HAP_SWIFT_BIND_TLS="ssl crt ${HAPROXY_VIP_PEM} ca-file ${TLS_CA_FILE}${_crl} verify required"
    HAP_SWIFT_TLS_N1="$(_server_tls "${NODE1_NAME}")"
    HAP_SWIFT_TLS_N2="$(_server_tls "${NODE2_NAME}")"
    HAP_SWIFT_TLS_N3="$(_server_tls "${NODE3_NAME}")"
fi
_is_true "${TLS_RABBITMQ_ENABLED}" && HAP_RABBITMQ_BACKEND_PORT="${RABBITMQ_TLS_PORT}"

render_config() {
    # '|' is the sed delimiter for the TLS values because they contain '/'. Their characters were validated above.
    sed \
        -e "s/__NODE1_NAME__/${NODE1_NAME}/g" \
        -e "s/__NODE1_IP__/${NODE1_IP}/g" \
        -e "s/__NODE2_NAME__/${NODE2_NAME}/g" \
        -e "s/__NODE2_IP__/${NODE2_IP}/g" \
        -e "s/__NODE3_NAME__/${NODE3_NAME}/g" \
        -e "s/__NODE3_IP__/${NODE3_IP}/g" \
        -e "s/__VIP__/${VIP}/g" \
        -e "s/__API_PORT__/${API_PORT}/g" \
        -e "s/__API_BACKEND_PORT__/${API_BACKEND_PORT}/g" \
        -e "s/__MARIADB_PORT__/${MARIADB_PORT}/g" \
        -e "s/__MARIADB_LB_PORT__/${MARIADB_LB_PORT}/g" \
        -e "s/__RABBITMQ_BACKEND_PORT__/${HAP_RABBITMQ_BACKEND_PORT}/g" \
        -e "s/__RABBITMQ_LB_PORT__/${RABBITMQ_LB_PORT}/g" \
        -e "s/__SWIFT_PROXY_PORT__/${SWIFT_PROXY_PORT}/g" \
        -e "s/__SWIFT_LB_PORT__/${SWIFT_LB_PORT}/g" \
        -e "s/__HAPROXY_STATS_PORT__/${HAPROXY_STATS_PORT}/g" \
        -e "s/__HAPROXY_STATS_USER__/${HAPROXY_STATS_USER}/g" \
        -e "s/__HAPROXY_STATS_PASS__/${HAPROXY_STATS_PASS}/g" \
        -e "s/__HAPROXY_MAXCONN__/${HAPROXY_MAXCONN}/g" \
        -e "s/__HAPROXY_FRONTEND_MAXCONN__/${HAPROXY_FRONTEND_MAXCONN}/g" \
        -e "s|__TLS_MIN_VERSION__|${HAP_TLS_MIN}|g" \
        -e "s|__TLS_CIPHERS__|${TLS_CIPHERS}|g" \
        -e "s|__STATS_BIND_TLS__|${HAP_STATS_TLS}|g" \
        -e "s|__API_BIND_TLS__|${HAP_API_BIND_TLS}|g" \
        -e "s|__API_ACL__|${HAP_API_ACL}|g" \
        -e "s|__API_TLS_N1__|${HAP_API_TLS_N1}|g" \
        -e "s|__API_TLS_N2__|${HAP_API_TLS_N2}|g" \
        -e "s|__API_TLS_N3__|${HAP_API_TLS_N3}|g" \
        -e "s|__SWIFT_BIND_TLS__|${HAP_SWIFT_BIND_TLS}|g" \
        -e "s|__SWIFT_TLS_N1__|${HAP_SWIFT_TLS_N1}|g" \
        -e "s|__SWIFT_TLS_N2__|${HAP_SWIFT_TLS_N2}|g" \
        -e "s|__SWIFT_TLS_N3__|${HAP_SWIFT_TLS_N3}|g" \
        "${SCRIPT_DIR}/haproxy.cfg.template"
}

# Test/review helper: print the generated config and stop (no sudo, no NIC check, nothing installed).
if [[ "${HAPROXY_RENDER_ONLY:-0}" == "1" ]]; then
    render_config
    exit 0
fi

# Fail closed: with TLS on, the files HAProxy needs must exist (it would otherwise refuse to start under Pacemaker).
_need_tls_files=()
if _is_true "${TLS_API_ENABLED}" || _is_true "${TLS_SWIFT_ENABLED}"; then
    _need_tls_files+=("${HAPROXY_VIP_PEM}" "${HAPROXY_BACKEND_PEM}" "${TLS_CA_FILE}")
    [[ -n "${TLS_CRL_FILE}" ]] && _need_tls_files+=("${TLS_CRL_FILE}")
fi
for _f in ${_need_tls_files[@]+"${_need_tls_files[@]}"}; do
    if ! sudo test -f "${_f}"; then
        echo "ERROR: TLS is enabled but ${_f} does not exist on this node."
        echo "  Create and distribute the certificates first: ./security/issue_all.sh && ./security/install_certs.sh"
        exit 1
    fi
done

# ---------------------------------------------------------------------------
# Pre-flight: discover and validate the cluster NIC
# ---------------------------------------------------------------------------
# VIP_NIC must be the physical NIC that the cluster traffic uses on THIS node.
# RHEL 9 uses predictable network interface names (e.g. ens3, ens192, enp0s3,
# eth0) that differ between hardware/virtualisation platforms.
# If VIP_NIC is not set in .env, auto-detect the interface that carries
# the node's own cluster IP (CURRENT_NODE_IP or NODE1_IP as fallback).
detect_cluster_nic() {
    local probe_ip="${CURRENT_NODE_IP:-${NODE1_IP}}"
    local detected
    detected=$(ip -o route get "${probe_ip}" 2>/dev/null \
               | awk '{for(i=1;i<=NF;i++) if($i=="dev") print $(i+1)}' \
               | head -1)
    if [[ -z "${detected}" ]]; then
        # Fallback: interface that owns the probe_ip
        detected=$(ip -o addr show \
                   | awk -v ip="${probe_ip}" '$0 ~ ip {print $2}' \
                   | head -1)
    fi
    echo "${detected}"
}

if [[ -z "${VIP_NIC}" ]]; then
    VIP_NIC=$(detect_cluster_nic)
    if [[ -z "${VIP_NIC}" ]]; then
        echo "ERROR: Cannot determine cluster NIC automatically."
        echo "  Set VIP_NIC=<interface> in /opt/flowfirst/.env"
        echo "  Run: ip -o link show | awk '{print \$2}' | tr -d ':'"
        echo "  to list available interfaces on this node."
        exit 1
    fi
    echo "  [INFO] VIP_NIC not set in .env — auto-detected: ${VIP_NIC}"
fi

# Confirm the interface actually exists on this node
if ! ip link show "${VIP_NIC}" &>/dev/null; then
    echo "ERROR: Interface '${VIP_NIC}' does not exist on this node."
    echo "  Available interfaces:"
    ip -o link show | awk '{print "   ", $2}' | tr -d ':'
    echo ""
    echo "  Fix: set VIP_NIC=<correct-interface> in /opt/flowfirst/.env"
    echo "  Common RHEL 9 names: ens3, ens192, enp0s3, enp1s0, eth0"
    exit 1
fi

echo "=================================================================="
echo " Setting up HAProxy Load Balancer for FlowFirst Multi-Node Cluster"
echo " Node 1:          ${NODE1_NAME} (${NODE1_IP})"
echo " Node 2:          ${NODE2_NAME} (${NODE2_IP})"
echo " Node 3:          ${NODE3_NAME} (${NODE3_IP})"
echo " Virtual IP (VIP):${VIP}"
echo " VIP NIC:         ${VIP_NIC}"
echo " API Port (VIP):  ${API_PORT}"
echo " Backend Port:    ${API_BACKEND_PORT}"
echo " MariaDB Port:    ${MARIADB_PORT} (LB Port: ${MARIADB_LB_PORT})"
echo " RabbitMQ Port:   ${RABBITMQ_PORT} (LB Port: ${RABBITMQ_LB_PORT})"
echo " Swift Port:      ${SWIFT_PROXY_PORT} (LB Port: ${SWIFT_LB_PORT})"
echo " Stats Dashboard: :${HAPROXY_STATS_PORT}"
echo "=================================================================="

# 1. Install HAProxy
echo "[1/6] Installing HAProxy..."
if command -v dnf >/dev/null 2>&1; then
    sudo dnf install -y haproxy
fi

# 2. Allow HAProxy to bind to the VIP address even when Pacemaker hasn't
#    assigned it to this node yet.  Without this, HAProxy fails to start
#    on nodes that don't currently hold the VIP.
echo "[2/6] Configuring net.ipv4.ip_nonlocal_bind=1 and runtime directories..."
sudo sysctl -w net.ipv4.ip_nonlocal_bind=1
echo "net.ipv4.ip_nonlocal_bind=1" | \
    sudo tee /etc/sysctl.d/99-haproxy-nonlocalbind.conf > /dev/null
# Apply immediately and persist across reboots
sudo sysctl --system | grep nonlocal_bind || true

# Ensure chroot, log, and runtime state directories exist with correct permissions and SELinux contexts
sudo mkdir -p /var/lib/haproxy /run/haproxy /var/log/haproxy
sudo chown -R haproxy:haproxy /var/lib/haproxy /run/haproxy /var/log/haproxy 2>/dev/null || true
sudo chmod 755 /var/lib/haproxy /run/haproxy /var/log/haproxy 2>/dev/null || true
if command -v restorecon &>/dev/null; then
    sudo restorecon -Rv /var/lib/haproxy /run/haproxy /var/log/haproxy /etc/haproxy >/dev/null 2>&1 || true
fi

# 3. SELinux: allow HAProxy to bind to cluster ports and access shared memory / cluster resources
echo "[3/6] Configuring SELinux for HAProxy..."
if command -v getsebool &>/dev/null && sudo getsebool haproxy_connect_any &>/dev/null; then
    sudo setsebool -P haproxy_connect_any 1
    echo "  SELinux: haproxy_connect_any set to ON"
else
    echo "  SELinux: haproxy_connect_any not available — skipping"
fi
if command -v getsebool &>/dev/null && sudo getsebool daemons_enable_cluster_mode &>/dev/null; then
    sudo setsebool -P daemons_enable_cluster_mode 1
    echo "  SELinux: daemons_enable_cluster_mode set to ON"
fi

# 4. Generate HAProxy configuration from template
echo "[4/6] Generating /etc/haproxy/haproxy.cfg..."
render_config | sudo tee /etc/haproxy/haproxy.cfg > /dev/null

# 5. Open firewall ports
echo "[5/6] Opening firewall ports..."
if command -v firewall-cmd >/dev/null 2>&1 && sudo systemctl is-active --quiet firewalld; then
    sudo firewall-cmd --permanent --add-port="${API_PORT}/tcp"
    sudo firewall-cmd --permanent --add-port="${API_BACKEND_PORT}/tcp"
    sudo firewall-cmd --permanent --add-port="${MARIADB_PORT}/tcp"
    sudo firewall-cmd --permanent --add-port="${MARIADB_LB_PORT}/tcp"
    sudo firewall-cmd --permanent --add-port="${RABBITMQ_PORT}/tcp"
    _is_true "${TLS_RABBITMQ_ENABLED}" && sudo firewall-cmd --permanent --add-port="${RABBITMQ_TLS_PORT}/tcp"
    sudo firewall-cmd --permanent --add-port="${RABBITMQ_LB_PORT}/tcp"
    sudo firewall-cmd --permanent --add-port="${SWIFT_LB_PORT}/tcp"
    sudo firewall-cmd --permanent --add-port="${HAPROXY_STATS_PORT}/tcp"
    sudo firewall-cmd --reload
fi

# 6. Validate configuration syntax
echo "[6/6] Validating HAProxy configuration syntax..."
sudo haproxy -c -f /etc/haproxy/haproxy.cfg

echo ""
echo "=================================================================="
echo " HAProxy configuration successfully installed and verified!"
echo " VIP_NIC used: ${VIP_NIC}"
echo " Note: HAProxy is managed by Pacemaker as part of vip-haproxy-group."
echo " Do NOT start haproxy.service directly — let Pacemaker control it."
echo "=================================================================="
