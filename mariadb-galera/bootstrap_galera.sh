#!/usr/bin/env bash
set -euo pipefail

# ==============================================================================
# FlowFirst - Bootstrap MariaDB Galera Cluster (Run on Node 1 only)
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

DB_NAME="${MARIADB_DB:-flowfirst_db}"
DB_USER="${MARIADB_USER:-flowuser}"
DB_PASS="${MARIADB_PASSWORD:-flowpassword}"

echo "=================================================================="
echo " Bootstrapping MariaDB Galera Primary Node"
echo " Database: ${DB_NAME}"
echo " User:     ${DB_USER}"
echo "=================================================================="

# 1. Stop any running instance
sudo systemctl stop mariadb || true

# 2. Bootstrap new Galera cluster
echo "Bootstrapping new Galera cluster with galera_new_cluster..."
sudo galera_new_cluster

# 3. Initialize FlowFirst Database and Users dynamically from .env
echo "Initializing FlowFirst database schema and user permissions..."
sudo mariadb -u root << EOF
CREATE DATABASE IF NOT EXISTS \`${DB_NAME}\` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
CREATE USER IF NOT EXISTS '${DB_USER}'@'%' IDENTIFIED BY '${DB_PASS}';
CREATE USER IF NOT EXISTS '${DB_USER}'@'localhost' IDENTIFIED BY '${DB_PASS}';
GRANT ALL PRIVILEGES ON \`${DB_NAME}\`.* TO '${DB_USER}'@'%';
GRANT ALL PRIVILEGES ON \`${DB_NAME}\`.* TO '${DB_USER}'@'localhost';
FLUSH PRIVILEGES;
EOF

# 4. Import table schema if init_db.sql exists
if [ -f "${ROOT_DIR}/init_db.sql" ]; then
    echo "Importing table schema from init_db.sql..."
    sudo mariadb -u "${DB_USER}" -p"${DB_PASS}" "${DB_NAME}" < "${ROOT_DIR}/init_db.sql"
fi

echo ""
echo "=================================================================="
echo " Galera Cluster successfully bootstrapped on primary node!"
echo " Checking cluster status..."
sudo mariadb -u root -e "SHOW STATUS LIKE 'wsrep_cluster_size'; SHOW STATUS LIKE 'wsrep_incoming_addresses';"
echo ""
echo " Now start MariaDB on secondary nodes using:"
echo "     sudo systemctl enable --now mariadb"
echo "=================================================================="
