#!/usr/bin/env bash
set -euo pipefail

# ==============================================================================
# FlowFirst - Bootstrap MariaDB Galera Cluster (Run on Node 1 only)
# ==============================================================================

echo "=================================================================="
echo " Bootstrapping MariaDB Galera Primary Node (Node 1)"
echo "=================================================================="

# 1. Stop any running instance
sudo systemctl stop mariadb || true

# 2. Bootstrap new Galera cluster
echo "Bootstrapping new Galera cluster with galera_new_cluster..."
sudo galera_new_cluster

# 3. Initialize FlowFirst Database and Users
echo "Initializing FlowFirst database schema and user permissions..."
sudo mariadb -u root << 'EOF'
CREATE DATABASE IF NOT EXISTS flowfirst_db CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
CREATE USER IF NOT EXISTS 'flowuser'@'%' IDENTIFIED BY 'flowpassword';
CREATE USER IF NOT EXISTS 'flowuser'@'localhost' IDENTIFIED BY 'flowpassword';
GRANT ALL PRIVILEGES ON flowfirst_db.* TO 'flowuser'@'%';
GRANT ALL PRIVILEGES ON flowfirst_db.* TO 'flowuser'@'localhost';
FLUSH PRIVILEGES;
EOF

# 4. Import table schema if init_db.sql exists
WORKING_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
if [ -f "${WORKING_DIR}/init_db.sql" ]; then
    echo "Importing table schema from init_db.sql..."
    sudo mariadb -u flowuser -pflowpassword flowfirst_db < "${WORKING_DIR}/init_db.sql"
fi

echo ""
echo "=================================================================="
echo " Galera Cluster successfully bootstrapped on Node 1!"
echo " Checking cluster status..."
sudo mariadb -u root -e "SHOW STATUS LIKE 'wsrep_cluster_size'; SHOW STATUS LIKE 'wsrep_incoming_addresses';"
echo ""
echo " Now start MariaDB on Node 2 and Node 3 using:"
echo "     sudo systemctl start mariadb"
echo "=================================================================="
