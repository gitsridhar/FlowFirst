#!/usr/bin/env bash
set -euo pipefail

# ==============================================================================
# FlowFirst - OpenStack Swift Cluster Setup for RHEL 9.6
#
# Installs a native (RPM / systemd) Swift object store across the 3-node
# cluster.  Every node runs ALL Swift roles:
#   - swift-proxy      :SWIFT_PROXY_PORT (8888)  tempauth, load-balanced by HAProxy
#   - swift-account    :6202
#   - swift-container  :6201
#   - swift-object     :6200
#   - replicators / auditors / updaters / reaper
#   - rsyncd           :873   (used by the replicators)
#   - memcached        :11211 (shared token cache — all proxies use all 3 servers)
#
# Cluster awareness:
#   - The account / container / object rings are built from NODE1..3_IP with one
#     zone per node and 3 replicas, so each object lives on all three nodes.
#   - Rings are built with a fixed seed and fixed device order, so every node
#     produces byte-identical *.ring.gz files; nothing has to be copied between
#     nodes.  The hash prefix/suffix come from .env and MUST be identical on all
#     nodes (the script refuses to run with the placeholder values).
#   - Proxies share tempauth tokens via memcached on all three nodes, so a client
#     may authenticate on one node and use its token on another via HAProxy.
#
# Run this script on ALL THREE nodes (node4 does not run a Swift server).
# Swift is managed by plain systemd (like MariaDB / RabbitMQ / ZooKeeper), not by
# Pacemaker.  Clients reach it through HAProxy on the VIP at SWIFT_LB_PORT.
#
# Usage:
#   sudo ./swift/setup_swift.sh
# ==============================================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

if [ -f "${ROOT_DIR}/.env" ]; then
    set -a
    # shellcheck disable=SC1091
    source "${ROOT_DIR}/.env"
    set +a
fi

SWIFT_OPENSTACK_RELEASE="${SWIFT_OPENSTACK_RELEASE:-epoxy}"
SWIFT_PROXY_PORT="${SWIFT_PROXY_PORT:-8888}"
SWIFT_ACCOUNT_PORT="${SWIFT_ACCOUNT_PORT:-6202}"
SWIFT_CONTAINER_PORT="${SWIFT_CONTAINER_PORT:-6201}"
SWIFT_OBJECT_PORT="${SWIFT_OBJECT_PORT:-6200}"
SWIFT_LB_PORT="${SWIFT_LB_PORT:-8090}"
SWIFT_DEVICES_DIR="${SWIFT_DEVICES_DIR:-/srv/node}"
SWIFT_DEVICE="${SWIFT_DEVICE:-flowfirst}"
SWIFT_MOUNT_CHECK="${SWIFT_MOUNT_CHECK:-false}"
SWIFT_PART_POWER="${SWIFT_PART_POWER:-10}"
SWIFT_REPLICAS="${SWIFT_REPLICAS:-3}"
SWIFT_RING_SEED="${SWIFT_RING_SEED:-12345}"
SWIFT_HASH_PATH_PREFIX="${SWIFT_HASH_PATH_PREFIX:-}"
SWIFT_HASH_PATH_SUFFIX="${SWIFT_HASH_PATH_SUFFIX:-}"
SWIFT_USER="${SWIFT_USER:-test:tester}"
SWIFT_KEY="${SWIFT_KEY:-testing}"
MEMCACHED_PORT="${MEMCACHED_PORT:-11211}"

NODE1_IP="${NODE1_IP:-}"
NODE2_IP="${NODE2_IP:-}"
NODE3_IP="${NODE3_IP:-}"
CURRENT_NODE_IP="${CURRENT_NODE_IP:-}"

# Validate required variables (must come from .env — no hardcoded fallbacks)
for _var in NODE1_IP NODE2_IP NODE3_IP CURRENT_NODE_IP SWIFT_HASH_PATH_PREFIX SWIFT_HASH_PATH_SUFFIX; do
    _val="${!_var:-}"
    if [[ -z "${_val}" ]] || [[ "${_val}" =~ ^\$\{ ]]; then
        echo "ERROR: ${_var} is not set. Set it in /opt/flowfirst/.env and re-run."
        exit 1
    fi
done
for _var in SWIFT_HASH_PATH_PREFIX SWIFT_HASH_PATH_SUFFIX; do
    if [[ "${!_var}" == changeme* ]]; then
        echo "ERROR: ${_var} still has the placeholder value from src/config/.env.example."
        echo "  Generate one value and put the SAME value in .env on all 3 nodes:"
        echo "    openssl rand -hex 16"
        exit 1
    fi
done
if [[ "${SWIFT_USER}" != *:* ]]; then
    echo "ERROR: SWIFT_USER must be '<account>:<user>' (e.g. test:tester), got '${SWIFT_USER}'"
    exit 1
fi
SWIFT_AUTH_ACCOUNT="${SWIFT_USER%%:*}"
SWIFT_AUTH_USER="${SWIFT_USER#*:}"

if [ "${CURRENT_NODE_IP}" = "${NODE1_IP}" ]; then
    MY_ID=1
elif [ "${CURRENT_NODE_IP}" = "${NODE2_IP}" ]; then
    MY_ID=2
elif [ "${CURRENT_NODE_IP}" = "${NODE3_IP}" ]; then
    MY_ID=3
else
    echo "ERROR: CURRENT_NODE_IP='${CURRENT_NODE_IP}' does not match any of"
    echo "  NODE1_IP=${NODE1_IP}, NODE2_IP=${NODE2_IP}, NODE3_IP=${NODE3_IP}"
    echo "Set CURRENT_NODE_IP correctly in /opt/flowfirst/.env"
    exit 1
fi

MEMCACHE_SERVERS="${NODE1_IP}:${MEMCACHED_PORT},${NODE2_IP}:${MEMCACHED_PORT},${NODE3_IP}:${MEMCACHED_PORT}"
SWIFT_DEVICE_PATH="${SWIFT_DEVICES_DIR}/${SWIFT_DEVICE}"

echo "=================================================================="
echo " FlowFirst - Swift (OpenStack ${SWIFT_OPENSTACK_RELEASE}) Cluster Setup"
echo " This node:     ${CURRENT_NODE_IP}  (zone ${MY_ID})"
echo " Swift nodes:   ${NODE1_IP}, ${NODE2_IP}, ${NODE3_IP}"
echo " Proxy port:    ${SWIFT_PROXY_PORT}  (HAProxy VIP port: ${SWIFT_LB_PORT})"
echo " Ring ports:    account ${SWIFT_ACCOUNT_PORT} / container ${SWIFT_CONTAINER_PORT} / object ${SWIFT_OBJECT_PORT}"
echo " Device path:   ${SWIFT_DEVICE_PATH}"
echo " Replicas:      ${SWIFT_REPLICAS}"
echo "=================================================================="

# ---------------------------------------------------------------------------
# Step 1: RDO / CentOS Cloud SIG repository (RHEL 9 has no Swift server RPMs)
# ---------------------------------------------------------------------------
echo "[1/11] Configuring the CentOS Cloud SIG OpenStack ${SWIFT_OPENSTACK_RELEASE} repository..."
REPO_ID="centos-openstack-${SWIFT_OPENSTACK_RELEASE}"
GPG_KEY="/etc/pki/rpm-gpg/RPM-GPG-KEY-CentOS-SIG-Cloud"
if [ ! -f "${GPG_KEY}" ]; then
    sudo curl -fsSL "https://www.centos.org/keys/RPM-GPG-KEY-CentOS-SIG-Cloud" -o "${GPG_KEY}"
fi
sudo rpm --import "${GPG_KEY}"
# Repo is disabled by default: it also carries newer builds of many base
# libraries, and must not take part in routine 'dnf update' runs on the cluster.
# It is enabled explicitly (--enablerepo) only when installing / updating Swift.
sudo tee "/etc/yum.repos.d/${REPO_ID}.repo" > /dev/null << EOF
[${REPO_ID}]
name=CentOS Cloud SIG - OpenStack ${SWIFT_OPENSTACK_RELEASE} (FlowFirst Swift only)
baseurl=https://mirror.stream.centos.org/SIGs/9-stream/cloud/\$basearch/openstack-${SWIFT_OPENSTACK_RELEASE}/
gpgcheck=1
enabled=0
gpgkey=file://${GPG_KEY}
module_hotfixes=1
EOF

# ---------------------------------------------------------------------------
# Step 2: Install Swift packages
# ---------------------------------------------------------------------------
echo "[2/11] Installing Swift server packages..."
if ! sudo dnf install -y --enablerepo="${REPO_ID}" \
        openstack-swift-proxy openstack-swift-account \
        openstack-swift-container openstack-swift-object \
        memcached rsync rsync-daemon attr xfsprogs policycoreutils-python-utils; then
    echo "ERROR: dnf could not resolve all Swift dependencies."
    echo "  If a python3-* package is missing, enable the CodeReady Builder repo and retry:"
    echo "    sudo subscription-manager repos --enable codeready-builder-for-rhel-9-\$(arch)-rpms"
    exit 1
fi

# ---------------------------------------------------------------------------
# Step 3: swift user, directories, storage device
# ---------------------------------------------------------------------------
echo "[3/11] Preparing swift user and storage directory ${SWIFT_DEVICE_PATH}..."
if ! id swift &>/dev/null; then
    sudo useradd --system --no-create-home --shell /sbin/nologin swift
fi
sudo mkdir -p /etc/swift /var/cache/swift /var/run/swift "${SWIFT_DEVICE_PATH}"
sudo chown -R swift:swift /etc/swift /var/cache/swift /var/run/swift
sudo chown swift:swift "${SWIFT_DEVICES_DIR}" "${SWIFT_DEVICE_PATH}"

# Swift stores object metadata in extended attributes — fail early if unsupported
_xtest="${SWIFT_DEVICE_PATH}/.xattr_test"
sudo touch "${_xtest}"
if ! sudo setfattr -n user.swift_test -v 1 "${_xtest}" 2>/dev/null; then
    sudo rm -f "${_xtest}"
    echo "ERROR: ${SWIFT_DEVICE_PATH} does not support extended attributes."
    echo "  Use XFS (recommended) or ext4 for the Swift data directory."
    exit 1
fi
sudo rm -f "${_xtest}"

# swift-recon (-u / -d) always runs a real mount-point check, even with mount_check = false.
# A plain directory on the root filesystem would be reported "Not mounted", so mark it with
# the .ismount stub that Swift accepts. Skipped when mount checking is on, where a missing
# mount must stay visible.
if [ "${SWIFT_MOUNT_CHECK}" != "true" ] && ! mountpoint -q "${SWIFT_DEVICE_PATH}"; then
    sudo -u swift touch "${SWIFT_DEVICE_PATH}/.ismount"
    echo "  Created ${SWIFT_DEVICE_PATH}/.ismount (lets swift-recon treat the plain directory as a device)"
fi

# rsyncd runs confined (rsync_t) and must be able to write replicated data
if command -v semanage &>/dev/null && command -v getenforce &>/dev/null \
        && [ "$(getenforce)" != "Disabled" ]; then
    sudo semanage fcontext -a -t rsync_data_t "${SWIFT_DEVICES_DIR}(/.*)?" 2>/dev/null \
        || sudo semanage fcontext -m -t rsync_data_t "${SWIFT_DEVICES_DIR}(/.*)?"
    sudo restorecon -R "${SWIFT_DEVICES_DIR}"
    echo "  SELinux: ${SWIFT_DEVICES_DIR} labelled rsync_data_t"
fi

# ---------------------------------------------------------------------------
# Step 4: swift.conf (hash prefix/suffix — identical on every node)
# ---------------------------------------------------------------------------
echo "[4/11] Writing /etc/swift/swift.conf..."
sudo tee /etc/swift/swift.conf > /dev/null << EOF
# FlowFirst Swift cluster configuration — generated by swift/setup_swift.sh
# The hash path prefix/suffix MUST be identical on all nodes and never changed
# after data has been stored.
[swift-hash]
swift_hash_path_prefix = ${SWIFT_HASH_PATH_PREFIX}
swift_hash_path_suffix = ${SWIFT_HASH_PATH_SUFFIX}

[storage-policy:0]
name = Policy-0
default = yes
EOF
sudo chown swift:swift /etc/swift/swift.conf
sudo chmod 640 /etc/swift/swift.conf

# ---------------------------------------------------------------------------
# Step 5: memcached (shared tempauth token cache, listens on the node IP)
# ---------------------------------------------------------------------------
echo "[5/11] Configuring memcached on ${CURRENT_NODE_IP}:${MEMCACHED_PORT}..."
sudo tee /etc/sysconfig/memcached > /dev/null << EOF
PORT="${MEMCACHED_PORT}"
USER="memcached"
MAXCONN="1024"
CACHESIZE="64"
OPTIONS="-l 127.0.0.1,${CURRENT_NODE_IP}"
EOF

# ---------------------------------------------------------------------------
# Step 6: rsyncd (used by the account / container / object replicators)
# ---------------------------------------------------------------------------
echo "[6/11] Writing /etc/rsyncd.conf..."
sudo tee /etc/rsyncd.conf > /dev/null << EOF
# FlowFirst Swift replication — generated by swift/setup_swift.sh
uid = swift
gid = swift
log file = /var/log/rsyncd.log
pid file = /var/run/rsyncd.pid
address = ${CURRENT_NODE_IP}
hosts allow = ${NODE1_IP} ${NODE2_IP} ${NODE3_IP}
hosts deny = *

[account]
max connections = 25
path = ${SWIFT_DEVICES_DIR}
read only = false
lock file = /var/lock/account.lock

[container]
max connections = 25
path = ${SWIFT_DEVICES_DIR}
read only = false
lock file = /var/lock/container.lock

[object]
max connections = 25
path = ${SWIFT_DEVICES_DIR}
read only = false
lock file = /var/lock/object.lock
EOF

# ---------------------------------------------------------------------------
# Step 7: Account / container / object server configuration
# ---------------------------------------------------------------------------
echo "[7/11] Writing account / container / object server configs..."
for _f in account container object proxy; do
    if [ -f "/etc/swift/${_f}-server.conf" ] && [ ! -f "/etc/swift/${_f}-server.conf.rpm-orig" ]; then
        sudo cp "/etc/swift/${_f}-server.conf" "/etc/swift/${_f}-server.conf.rpm-orig"
    fi
done

sudo tee /etc/swift/account-server.conf > /dev/null << EOF
# Generated by swift/setup_swift.sh
[DEFAULT]
bind_ip = ${CURRENT_NODE_IP}
bind_port = ${SWIFT_ACCOUNT_PORT}
user = swift
swift_dir = /etc/swift
devices = ${SWIFT_DEVICES_DIR}
mount_check = ${SWIFT_MOUNT_CHECK}
workers = 2
recon_cache_path = /var/cache/swift

[pipeline:main]
pipeline = healthcheck recon account-server

[app:account-server]
use = egg:swift#account

[filter:healthcheck]
use = egg:swift#healthcheck

[filter:recon]
use = egg:swift#recon

[account-replicator]

[account-auditor]

[account-reaper]
EOF

sudo tee /etc/swift/container-server.conf > /dev/null << EOF
# Generated by swift/setup_swift.sh
[DEFAULT]
bind_ip = ${CURRENT_NODE_IP}
bind_port = ${SWIFT_CONTAINER_PORT}
user = swift
swift_dir = /etc/swift
devices = ${SWIFT_DEVICES_DIR}
mount_check = ${SWIFT_MOUNT_CHECK}
workers = 2
recon_cache_path = /var/cache/swift

[pipeline:main]
pipeline = healthcheck recon container-server

[app:container-server]
use = egg:swift#container

[filter:healthcheck]
use = egg:swift#healthcheck

[filter:recon]
use = egg:swift#recon

[container-replicator]

[container-updater]

[container-auditor]
EOF

sudo tee /etc/swift/object-server.conf > /dev/null << EOF
# Generated by swift/setup_swift.sh
[DEFAULT]
bind_ip = ${CURRENT_NODE_IP}
bind_port = ${SWIFT_OBJECT_PORT}
user = swift
swift_dir = /etc/swift
devices = ${SWIFT_DEVICES_DIR}
mount_check = ${SWIFT_MOUNT_CHECK}
workers = 2
recon_cache_path = /var/cache/swift

[pipeline:main]
pipeline = healthcheck recon object-server

[app:object-server]
use = egg:swift#object

[filter:healthcheck]
use = egg:swift#healthcheck

[filter:recon]
use = egg:swift#recon

[object-replicator]

[object-updater]

[object-auditor]
EOF

# ---------------------------------------------------------------------------
# Step 8: Proxy server (tempauth + shared memcached)
# ---------------------------------------------------------------------------
echo "[8/11] Writing /etc/swift/proxy-server.conf..."
sudo tee /etc/swift/proxy-server.conf > /dev/null << EOF
# Generated by swift/setup_swift.sh
[DEFAULT]
bind_ip = 0.0.0.0
bind_port = ${SWIFT_PROXY_PORT}
user = swift
swift_dir = /etc/swift
workers = 2

[pipeline:main]
pipeline = catch_errors gatekeeper healthcheck proxy-logging cache listing_formats tempauth copy proxy-logging proxy-server

[app:proxy-server]
use = egg:swift#proxy
account_autocreate = true

[filter:catch_errors]
use = egg:swift#catch_errors

[filter:gatekeeper]
use = egg:swift#gatekeeper

[filter:healthcheck]
use = egg:swift#healthcheck

[filter:proxy-logging]
use = egg:swift#proxy_logging

# All three proxies use the same three memcached servers so that a tempauth
# token issued by one node is valid on the others (HAProxy round-robins).
[filter:cache]
use = egg:swift#memcache
memcache_servers = ${MEMCACHE_SERVERS}

[filter:listing_formats]
use = egg:swift#listing_formats

[filter:tempauth]
use = egg:swift#tempauth
user_${SWIFT_AUTH_ACCOUNT}_${SWIFT_AUTH_USER} = ${SWIFT_KEY} .admin

[filter:copy]
use = egg:swift#copy
EOF
sudo chown swift:swift /etc/swift/*.conf
sudo chmod 640 /etc/swift/proxy-server.conf

# ---------------------------------------------------------------------------
# Step 9: Build rings (deterministic — identical on every node)
# ---------------------------------------------------------------------------
echo "[9/11] Building account / container / object rings..."
build_ring() {
    local name="$1" port="$2"
    if [ -f "/etc/swift/${name}.ring.gz" ]; then
        echo "  ${name}.ring.gz already exists — keeping it"
        return
    fi
    local b=(sudo swift-ring-builder "/etc/swift/${name}.builder")
    "${b[@]}" create "${SWIFT_PART_POWER}" "${SWIFT_REPLICAS}" 1 > /dev/null
    local zone=1 ip
    for ip in "${NODE1_IP}" "${NODE2_IP}" "${NODE3_IP}"; do
        "${b[@]}" add --region 1 --zone "${zone}" --ip "${ip}" --port "${port}" \
            --device "${SWIFT_DEVICE}" --weight 100 > /dev/null
        zone=$((zone + 1))
    done
    "${b[@]}" rebalance -s "${SWIFT_RING_SEED}" > /dev/null
    echo "  ${name}.ring.gz built"
}
build_ring account "${SWIFT_ACCOUNT_PORT}"
build_ring container "${SWIFT_CONTAINER_PORT}"
build_ring object "${SWIFT_OBJECT_PORT}"
sudo chown -R swift:swift /etc/swift

# ---------------------------------------------------------------------------
# Step 10: Firewall
# ---------------------------------------------------------------------------
echo "[10/11] Opening firewall ports..."
if command -v firewall-cmd &>/dev/null && sudo systemctl is-active --quiet firewalld; then
    for _p in "${SWIFT_PROXY_PORT}" "${SWIFT_ACCOUNT_PORT}" "${SWIFT_CONTAINER_PORT}" "${SWIFT_OBJECT_PORT}"; do
        sudo firewall-cmd --permanent --add-port="${_p}/tcp" > /dev/null
    done
    # rsync and memcached are cluster-internal: allow only the three Swift nodes
    for _ip in "${NODE1_IP}" "${NODE2_IP}" "${NODE3_IP}"; do
        for _p in 873 "${MEMCACHED_PORT}"; do
            sudo firewall-cmd --permanent \
                --add-rich-rule="rule family=ipv4 source address=${_ip} port port=${_p} protocol=tcp accept" > /dev/null
        done
    done
    sudo firewall-cmd --reload > /dev/null
    echo "  Opened: proxy ${SWIFT_PROXY_PORT}, ring ports ${SWIFT_ACCOUNT_PORT}/${SWIFT_CONTAINER_PORT}/${SWIFT_OBJECT_PORT}; rsync 873 + memcached ${MEMCACHED_PORT} for cluster nodes only"
fi

# ---------------------------------------------------------------------------
# Step 11: Start services and verify
# ---------------------------------------------------------------------------
echo "[11/11] Enabling and starting Swift services..."
SWIFT_UNITS=(
    openstack-swift-account openstack-swift-account-replicator
    openstack-swift-account-auditor openstack-swift-account-reaper
    openstack-swift-container openstack-swift-container-replicator
    openstack-swift-container-updater openstack-swift-container-auditor
    openstack-swift-object openstack-swift-object-replicator
    openstack-swift-object-updater openstack-swift-object-auditor
    openstack-swift-proxy
)
sudo systemctl daemon-reload
sudo systemctl enable memcached rsyncd
sudo systemctl restart memcached rsyncd
sudo systemctl enable "${SWIFT_UNITS[@]}"
sudo systemctl restart "${SWIFT_UNITS[@]}"
sleep 3

_failed=0
for _u in "${SWIFT_UNITS[@]}" memcached rsyncd; do
    if ! sudo systemctl is-active --quiet "${_u}"; then
        echo "  WARNING: ${_u} is not active — check: journalctl -u ${_u} -n 50"
        _failed=1
    fi
done

if [ "$(curl -s -o /dev/null -w '%{http_code}' "http://${CURRENT_NODE_IP}:${SWIFT_PROXY_PORT}/healthcheck")" = "200" ]; then
    echo "  Proxy healthcheck OK on ${CURRENT_NODE_IP}:${SWIFT_PROXY_PORT}"
else
    echo "  WARNING: proxy /healthcheck did not return 200"
    _failed=1
fi

_auth_hdrs=$(curl -s -D - -o /dev/null \
    -H "X-Storage-User: ${SWIFT_USER}" -H "X-Storage-Pass: ${SWIFT_KEY}" \
    "http://${CURRENT_NODE_IP}:${SWIFT_PROXY_PORT}/auth/v1.0" || true)
if grep -qi '^X-Auth-Token:' <<< "${_auth_hdrs}"; then
    echo "  tempauth OK for ${SWIFT_USER}"
else
    echo "  WARNING: tempauth did not issue a token for ${SWIFT_USER}"
    _failed=1
fi

echo ""
echo "=================================================================="
echo " Swift setup complete on this node (zone ${MY_ID})"
echo " Ring checksums (must be IDENTICAL on all 3 nodes):"
(cd /etc/swift && sudo md5sum account.ring.gz container.ring.gz object.ring.gz | sed 's/^/   /')
echo ""
echo " Run this script on all 3 nodes, then re-run haproxy/setup_haproxy.sh so the"
echo " VIP serves Swift on :${SWIFT_LB_PORT}, and verify:"
echo "   swift -A http://<VIP>:${SWIFT_LB_PORT}/auth/v1.0 -U ${SWIFT_USER} -K <key> stat"
if [ "${_failed}" -ne 0 ]; then
    echo ""
    echo " NOTE: warnings above are expected until the other nodes are also"
    echo " configured (replication and memcached peers must be reachable)."
fi
echo "=================================================================="
