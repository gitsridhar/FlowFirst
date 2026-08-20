#!/usr/bin/env bash
# ==============================================================================
# FlowFirst - Check MariaDB Galera Cluster Status
# ==============================================================================

echo "=== Galera Cluster Status ==="
sudo mariadb -u root -e "
SHOW STATUS LIKE 'wsrep_cluster_size';
SHOW STATUS LIKE 'wsrep_cluster_status';
SHOW STATUS LIKE 'wsrep_connected';
SHOW STATUS LIKE 'wsrep_ready';
SHOW STATUS LIKE 'wsrep_local_state_comment';
SHOW STATUS LIKE 'wsrep_incoming_addresses';
"
