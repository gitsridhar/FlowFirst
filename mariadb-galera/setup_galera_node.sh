#!/usr/bin/env bash
set -euo pipefail

# ==============================================================================
# FlowFirst - MariaDB Galera Cluster Setup Script for RHEL 9.6
# Automatically loads configuration from .env if present
# ==============================================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

# Source environment variables if .env exists
if [ -f "${ROOT_DIR}/.env" ]; then
    # shellcheck disable=SC1091
    set -a
    source "${ROOT_DIR}/.env"
    set +a
fi

CURRENT_NODE_NAME="${1:-${NODE_NAME:-node1}}"
CURRENT_NODE_IP="${2:-${CURRENT_NODE_IP:-}}"
NODE1_IP="${3:-${NODE1_IP:-}}"
NODE2_IP="${4:-${NODE2_IP:-}}"
NODE3_IP="${5:-${NODE3_IP:-}}"

# Validate required IP variables (must come from .env — no hardcoded fallbacks)
for _var in CURRENT_NODE_IP NODE1_IP NODE2_IP NODE3_IP; do
    _val="${!_var:-}"
    if [ -z "${_val}" ]; then
        echo "ERROR: ${_var} is not set. Set it in /opt/flowfirst/.env and re-run."
        exit 1
    fi
done
MARIADB_PORT="${MARIADB_PORT:-3306}"
MARIADB_GALERA_CLUSTER_NAME="${MARIADB_GALERA_CLUSTER_NAME:-flowfirst_galera_cluster}"
MARIADB_GALERA_WSREP_PROVIDER="${MARIADB_GALERA_WSREP_PROVIDER:-}"
MARIADB_SST_METHOD="${MARIADB_SST_METHOD:-rsync}"
MARIADB_SST_ENCRYPT="${MARIADB_SST_ENCRYPT:-false}"

if [[ "$MARIADB_SST_METHOD" != "rsync" && "$MARIADB_SST_METHOD" != "mariabackup" ]]; then
    echo "ERROR: MARIADB_SST_METHOD must be 'rsync' or 'mariabackup', not '${MARIADB_SST_METHOD}'" >&2
    exit 1
fi

# Construct cluster address list from environment
if [ -n "${MARIADB_HOSTS:-}" ]; then
    CLUSTER_ADDRESSES="${MARIADB_HOSTS}"
else
    CLUSTER_ADDRESSES="${NODE1_IP},${NODE2_IP},${NODE3_IP}"
fi

echo "=================================================================="
echo " Setting up MariaDB Galera Cluster Node on RHEL 9.6"
echo " Node Name:         ${CURRENT_NODE_NAME}"
echo " Node IP:           ${CURRENT_NODE_IP}"
echo " Cluster Name:      ${MARIADB_GALERA_CLUSTER_NAME}"
echo " Cluster Members:   ${CLUSTER_ADDRESSES}"
echo " MariaDB Port:      ${MARIADB_PORT}"
echo "=================================================================="

# 1. Configure official MariaDB repository for latest GA release
echo "[1/5] Configuring official MariaDB repository for latest release..."
if command -v dnf >/dev/null 2>&1; then
    # Disable OS AppStream mariadb module to prevent conflicts with the latest official repository
    sudo dnf module reset -y mariadb 2>/dev/null || true
    sudo dnf module disable -y mariadb 2>/dev/null || true

    TMP_REPO_SETUP="$(mktemp /tmp/mariadb_repo_setup.XXXXXX)"
    if curl -fsSL -o "${TMP_REPO_SETUP}" "https://r2.mariadb.com/downloads/mariadb_repo_setup" 2>/dev/null || \
       curl -fsSL -o "${TMP_REPO_SETUP}" "https://downloads.mariadb.com/MariaDB/mariadb_repo_setup" 2>/dev/null; then
        echo "  Running official mariadb_repo_setup script..."
        sudo bash "${TMP_REPO_SETUP}" --skip-maxscale
        rm -f "${TMP_REPO_SETUP}"
    else
        echo "  Writing official /etc/yum.repos.d/mariadb.repo..."
        sudo tee /etc/yum.repos.d/mariadb.repo > /dev/null << 'EOF'
[mariadb-main]
name = MariaDB Server (Latest)
baseurl = https://dlm.mariadb.com/repo/mariadb-server/latest/yum/rhel/9/$basearch
gpgkey = https://supplychain.mariadb.com/MariaDB-Server-GPG-KEY
gpgcheck = 1
enabled = 1
EOF
    fi
    sudo dnf clean metadata
    sudo dnf makecache

    # 2. Install MariaDB server and Galera packages
    echo "[2/5] Installing latest MariaDB server and Galera packages..."
    PKGS=(MariaDB-server MariaDB-client galera-4 rsync)
    [[ "$MARIADB_SST_METHOD" == "mariabackup" ]] && PKGS+=(MariaDB-backup)
    if ! sudo dnf install -y "${PKGS[@]}"; then
        echo "  Retrying with standard package names..."
        FALLBACK_PKGS=(mariadb-server mariadb mariadb-server-galera galera rsync)
        [[ "$MARIADB_SST_METHOD" == "mariabackup" ]] && FALLBACK_PKGS+=(mariadb-backup)
        sudo dnf install -y "${FALLBACK_PKGS[@]}"
    fi
fi

# Detect WSREP provider library path if not explicitly configured
if [ -z "${MARIADB_GALERA_WSREP_PROVIDER}" ]; then
    if [ -f "/usr/lib64/galera-4/libgalera_smm.so" ]; then
        MARIADB_GALERA_WSREP_PROVIDER="/usr/lib64/galera-4/libgalera_smm.so"
    elif [ -f "/usr/lib64/galera/libgalera_smm.so" ]; then
        MARIADB_GALERA_WSREP_PROVIDER="/usr/lib64/galera/libgalera_smm.so"
    else
        MARIADB_GALERA_WSREP_PROVIDER="/usr/lib64/galera-4/libgalera_smm.so"
    fi
fi
echo "  WSREP Provider:   ${MARIADB_GALERA_WSREP_PROVIDER}"

# 3. Configure Firewall for Galera replication ports
echo "[3/5] Configuring firewall ports for MariaDB and Galera (3306, 4567, 4568, 4444)..."
if command -v firewall-cmd >/dev/null 2>&1 && sudo systemctl is-active --quiet firewalld; then
    sudo firewall-cmd --permanent --add-port="${MARIADB_PORT}/tcp"   # Standard MySQL/MariaDB client port
    sudo firewall-cmd --permanent --add-port=4567/tcp               # Galera Cluster replication traffic
    sudo firewall-cmd --permanent --add-port=4567/udp               # Galera Cluster multicast replication
    sudo firewall-cmd --permanent --add-port=4568/tcp               # Incremental State Transfer (IST)
    sudo firewall-cmd --permanent --add-port=4444/tcp               # State Snapshot Transfer (SST)
    sudo firewall-cmd --reload
fi

# 4. Deploy Galera configuration
echo "[4/5] Generating /etc/my.cnf.d/galera.cnf..."
# Build the optional SST encryption block (mariabackup + encrypt only).
# Write it to a temp file so it can be read by Python without any shell quoting problems.
_SST_BLOCK_FILE="$(mktemp /tmp/flowfirst-sst-block.XXXXXX)"
trap 'rm -f "$_SST_BLOCK_FILE"' EXIT
if [[ "$MARIADB_SST_METHOD" == "mariabackup" && "$MARIADB_SST_ENCRYPT" == "true" ]]; then
    _TLS_D="${TLS_DIR:-/etc/flowfirst/tls}"
    _CA="${TLS_CA_FILE:-${_TLS_D}/ca.crt}"
    _CERT="${TLS_CLIENT_CERT:-${_TLS_D}/pipeline/pipeline.crt}"
    _KEY="${TLS_CLIENT_KEY:-${_TLS_D}/pipeline/pipeline.key}"
    cat > "$_SST_BLOCK_FILE" <<SSTEOF

[sst]
# mariabackup SST encryption - uses the cluster's own TLS certificates (Scenario 63).
# encrypt=4 = OpenSSL TLS over socat; tca/tcert/tkey are the CA, cert and key.
encrypt=4
tca=${_CA}
tcert=${_CERT}
tkey=${_KEY}
tls-version=TLSv1.2,TLSv1.3
SSTEOF
    echo "  SST encryption: mariabackup + TLS (ca=${_CA})"
else
    [[ "$MARIADB_SST_METHOD" == "mariabackup" ]] && echo "  SST: mariabackup (plaintext - set MARIADB_SST_ENCRYPT=true to encrypt)"
    [[ "$MARIADB_SST_METHOD" == "rsync" ]] && echo "  SST: rsync (plaintext, donor blocks - set MARIADB_SST_METHOD=mariabackup to change)"
    # empty file: the template placeholder is replaced with an empty string
fi

# Use Python to substitute all placeholders (including the multi-line SST block)
# without any shell quoting issues. __SST_ENCRYPT_BLOCK__ is read from a file.
_GALERA_CNF="$(python3 - "${SCRIPT_DIR}/galera.cnf.template" "$_SST_BLOCK_FILE" \
    "${MARIADB_GALERA_WSREP_PROVIDER}" "${MARIADB_GALERA_CLUSTER_NAME}" \
    "${CLUSTER_ADDRESSES}" "${CURRENT_NODE_IP}" "${CURRENT_NODE_NAME}" \
    "${MARIADB_SST_METHOD}" <<'PYEOF'
import sys
template_file, sst_block_file = sys.argv[1], sys.argv[2]
wsrep_provider, cluster_name, cluster_addrs, node_ip, node_name, sst_method = sys.argv[3:]
with open(template_file) as f:
    template = f.read()
with open(sst_block_file) as f:
    sst_block = f.read()
for placeholder, value in (
    ("__MARIADB_GALERA_WSREP_PROVIDER__", wsrep_provider),
    ("__MARIADB_GALERA_CLUSTER_NAME__", cluster_name),
    ("__GALERA_CLUSTER_ADDRESSES__", cluster_addrs),
    ("__CURRENT_NODE_IP__", node_ip),
    ("__CURRENT_NODE_NAME__", node_name),
    ("__MARIADB_SST_METHOD__", sst_method),
    ("__SST_ENCRYPT_BLOCK__", sst_block),
):
    template = template.replace(placeholder, value)
sys.stdout.write(template)
PYEOF
)"
printf '%s\n' "${_GALERA_CNF}" | sudo tee /etc/my.cnf.d/galera.cnf > /dev/null

# 5. Stop standalone mariadb if running
echo "[5/5] Stopping standalone MariaDB service if running..."
sudo systemctl stop mariadb || true

echo ""
echo "=================================================================="
echo " Galera configuration written successfully to /etc/my.cnf.d/galera.cnf"
echo ""
echo " Next Steps to start the cluster:"
echo " - On Node 1 (Bootstrap node ONLY):"
echo "     sudo ./mariadb-galera/bootstrap_galera.sh"
echo " - On Node 2 & Node 3 (Joiner nodes):"
echo "     sudo systemctl enable --now mariadb"
echo "=================================================================="
