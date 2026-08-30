#!/usr/bin/env bash
set -euo pipefail

# ==============================================================================
# FlowFirst - RabbitMQ Server & Erlang Installation Script for RHEL 9.6
#
# Architecture-aware installation strategy (verified live as of 2025):
#
#   x86_64  — install from yum1/yum2.rabbitmq.com repos via dnf
#   aarch64 — download release RPMs directly from GitHub (packagecloud el9/aarch64
#              repo exists as a URL but its primary.xml is empty — no packages)
#
# GPG keys: github.com/rabbitmq/signing-keys/releases/tag/3.0
#   Only v2 (SHA-256) keys are imported; packagecloud's v1 (SHA-1) keys are
#   rejected by RHEL 9 rpm and are not needed.
# ==============================================================================

echo "=================================================================="
echo " Installing RabbitMQ Server & Erlang on RHEL 9.6"
echo "=================================================================="

# ---------------------------------------------------------------------------
# Pre-flight: clock synchronisation check
# RabbitMQ cluster nodes MUST have synchronised system clocks.
# Clock drift causes Erlang distribution handshake failures and node rejections.
# ---------------------------------------------------------------------------
echo "[PRE-FLIGHT] Checking clock synchronisation..."
CHRONY_OK=false
if systemctl is-active --quiet chronyd 2>/dev/null; then
    LEAP=$(chronyc tracking 2>/dev/null | grep "^Leap status" | awk '{print $NF}' || true)
    if [ "${LEAP}" = "Normal" ]; then
        OFFSET=$(chronyc tracking 2>/dev/null | grep "^System time" | awk '{print $4, $5}' || true)
        echo "  chronyd is running and synchronised (offset: ${OFFSET})."
        CHRONY_OK=true
    else
        echo "  WARNING: chronyd is running but clock status is '${LEAP}'."
    fi
else
    echo "  WARNING: chronyd is not running."
fi

if [ "${CHRONY_OK}" = "false" ]; then
    echo ""
    echo "  *** CLOCK NOT SYNCHRONISED ***"
    echo "  RabbitMQ may fail to start or join the cluster with an unsynchronised clock."
    echo "  Run the following before proceeding:"
    echo "    sudo ./scripts/setup_chronyd.sh"
    echo ""
    echo "  Continuing in 10 seconds — press Ctrl-C to abort and fix the clock first."
    sleep 10
fi
echo ""

# ---------------------------------------------------------------------------
# Detect system architecture
# ---------------------------------------------------------------------------
ARCH=$(uname -m)   # x86_64 | aarch64
echo "  Detected architecture: ${ARCH}"

# ---------------------------------------------------------------------------
# Key URLs — github.com/rabbitmq/signing-keys, release 3.0 (GnuPG v2 / SHA-256)
# ---------------------------------------------------------------------------
ERLANG_KEY_URL="https://github.com/rabbitmq/signing-keys/releases/download/3.0/cloudsmith.rabbitmq-erlang.E495BB49CC4BBE5B.key"
SERVER_KEY_URL="https://github.com/rabbitmq/signing-keys/releases/download/3.0/cloudsmith.rabbitmq-server.9F4587F226208342.key"
RELEASE_KEY_URL="https://github.com/rabbitmq/signing-keys/releases/download/3.0/rabbitmq-release-signing-key.asc"

# ---------------------------------------------------------------------------
# 1. Import GPG Signing Keys
#    rpm --import cannot read from a pipe on RHEL 9 — download to temp files.
# ---------------------------------------------------------------------------
echo "[1/5] Importing GPG signing keys ..."

TMPDIR_KEYS=$(mktemp -d)
trap 'rm -rf "${TMPDIR_KEYS}"' EXIT

echo "  → Erlang repo key          (E495BB49CC4BBE5B)"
curl -fsSL -o "${TMPDIR_KEYS}/erlang.key"  "${ERLANG_KEY_URL}"
echo "  → RabbitMQ server repo key (9F4587F226208342)"
curl -fsSL -o "${TMPDIR_KEYS}/server.key"  "${SERVER_KEY_URL}"
echo "  → RabbitMQ release signing key (primary trust anchor)"
curl -fsSL -o "${TMPDIR_KEYS}/release.key" "${RELEASE_KEY_URL}"

sudo rpm --import "${TMPDIR_KEYS}/erlang.key"
sudo rpm --import "${TMPDIR_KEYS}/server.key"
sudo rpm --import "${TMPDIR_KEYS}/release.key"
echo "  GPG keys imported OK."

# ===========================================================================
# x86_64 path — use official yum repos
# ===========================================================================
if [[ "${ARCH}" == "x86_64" ]]; then

# ---------------------------------------------------------------------------
# 2. Write repo file (x86_64)
# ---------------------------------------------------------------------------
echo "[2/5] Writing /etc/yum.repos.d/rabbitmq.repo (x86_64) ..."

sudo tee /etc/yum.repos.d/rabbitmq.repo > /dev/null << 'EOF'
[rabbitmq-erlang]
name=rabbitmq-erlang
baseurl=https://yum1.rabbitmq.com/erlang/el/9/x86_64
        https://yum2.rabbitmq.com/erlang/el/9/x86_64
repo_gpgcheck=1
enabled=1
gpgkey=https://github.com/rabbitmq/signing-keys/releases/download/3.0/cloudsmith.rabbitmq-erlang.E495BB49CC4BBE5B.key
gpgcheck=1
sslverify=1
sslcacert=/etc/pki/tls/certs/ca-bundle.crt
metadata_expire=300

[rabbitmq-erlang-noarch]
name=rabbitmq-erlang-noarch
baseurl=https://yum1.rabbitmq.com/erlang/el/9/noarch
        https://yum2.rabbitmq.com/erlang/el/9/noarch
repo_gpgcheck=1
enabled=1
gpgkey=https://github.com/rabbitmq/signing-keys/releases/download/3.0/cloudsmith.rabbitmq-erlang.E495BB49CC4BBE5B.key
       https://github.com/rabbitmq/signing-keys/releases/download/3.0/rabbitmq-release-signing-key.asc
gpgcheck=1
sslverify=1
sslcacert=/etc/pki/tls/certs/ca-bundle.crt
metadata_expire=300

[rabbitmq-server]
name=rabbitmq-server
baseurl=https://yum2.rabbitmq.com/rabbitmq/el/9/x86_64
        https://yum1.rabbitmq.com/rabbitmq/el/9/x86_64
repo_gpgcheck=1
enabled=1
gpgkey=https://github.com/rabbitmq/signing-keys/releases/download/3.0/cloudsmith.rabbitmq-server.9F4587F226208342.key
       https://github.com/rabbitmq/signing-keys/releases/download/3.0/rabbitmq-release-signing-key.asc
gpgcheck=1
sslverify=1
sslcacert=/etc/pki/tls/certs/ca-bundle.crt
metadata_expire=300

[rabbitmq-server-noarch]
name=rabbitmq-server-noarch
baseurl=https://yum2.rabbitmq.com/rabbitmq/el/9/noarch
        https://yum1.rabbitmq.com/rabbitmq/el/9/noarch
repo_gpgcheck=1
enabled=1
gpgkey=https://github.com/rabbitmq/signing-keys/releases/download/3.0/cloudsmith.rabbitmq-server.9F4587F226208342.key
       https://github.com/rabbitmq/signing-keys/releases/download/3.0/rabbitmq-release-signing-key.asc
gpgcheck=1
sslverify=1
sslcacert=/etc/pki/tls/certs/ca-bundle.crt
metadata_expire=300
EOF
echo "  Repo file written OK."

# ---------------------------------------------------------------------------
# 3. Install via dnf (x86_64)
# ---------------------------------------------------------------------------
echo "[3/5] Refreshing DNF metadata and installing erlang + rabbitmq-server ..."
sudo dnf clean metadata
sudo dnf makecache
sudo dnf install -y erlang rabbitmq-server

# ===========================================================================
# aarch64 path — download release RPMs directly from GitHub
#
# Why not repos?
#   yum1/yum2.rabbitmq.com  — no aarch64 tree (404)
#   packagecloud el/9/aarch64 — repo URL exists but primary.xml is empty
#
# GitHub releases carry:
#   erlang-rpm:      erlang-X.Y.Z-1.el9.aarch64.rpm  (exact el9 build)
#   rabbitmq-server: rabbitmq-server-X.Y.Z-1.el8.noarch.rpm (noarch, works on el9)
# ===========================================================================
elif [[ "${ARCH}" == "aarch64" ]]; then

echo "[2/5] Resolving latest release versions from GitHub API ..."

# Resolve latest erlang-rpm release tag and RPM URL
ERLANG_TAG=$(curl -fsSL 'https://api.github.com/repos/rabbitmq/erlang-rpm/releases/latest' \
  | python3 -c "import sys,json; print(json.load(sys.stdin)['tag_name'])")
ERLANG_VER="${ERLANG_TAG#v}"
ERLANG_RPM_URL="https://github.com/rabbitmq/erlang-rpm/releases/download/${ERLANG_TAG}/erlang-${ERLANG_VER}-1.el9.aarch64.rpm"
echo "  Erlang   : ${ERLANG_TAG}  →  erlang-${ERLANG_VER}-1.el9.aarch64.rpm"

# Resolve latest rabbitmq-server release tag and RPM URL
SERVER_TAG=$(curl -fsSL 'https://api.github.com/repos/rabbitmq/rabbitmq-server/releases/latest' \
  | python3 -c "import sys,json; print(json.load(sys.stdin)['tag_name'])")
SERVER_VER="${SERVER_TAG#v}"
SERVER_RPM_URL="https://github.com/rabbitmq/rabbitmq-server/releases/download/${SERVER_TAG}/rabbitmq-server-${SERVER_VER}-1.el8.noarch.rpm"
echo "  RabbitMQ : ${SERVER_TAG}  →  rabbitmq-server-${SERVER_VER}-1.el8.noarch.rpm"

echo "[3/5] Downloading and installing RPMs (aarch64) ..."
TMPDIR_RPMS=$(mktemp -d)
# keep TMPDIR_RPMS in the same trap
trap 'rm -rf "${TMPDIR_KEYS}" "${TMPDIR_RPMS}"' EXIT

echo "  → Downloading erlang RPM ..."
curl -fsSL -o "${TMPDIR_RPMS}/erlang.rpm" "${ERLANG_RPM_URL}"

echo "  → Downloading rabbitmq-server RPM ..."
curl -fsSL -o "${TMPDIR_RPMS}/rabbitmq-server.rpm" "${SERVER_RPM_URL}"

echo "  → Installing RPMs with dnf ..."
# Install erlang first (rabbitmq-server depends on it)
sudo dnf install -y "${TMPDIR_RPMS}/erlang.rpm"
sudo dnf install -y "${TMPDIR_RPMS}/rabbitmq-server.rpm"

else
    echo "ERROR: Unsupported architecture '${ARCH}'. Supported: x86_64, aarch64."
    exit 1
fi

# ---------------------------------------------------------------------------
# 4. Enable management plugin and start service
# ---------------------------------------------------------------------------
echo "[4/5] Enabling rabbitmq_management plugin and starting service ..."
sudo rabbitmq-plugins enable rabbitmq_management
sudo systemctl enable --now rabbitmq-server

# ---------------------------------------------------------------------------
# 5. Configure FlowFirst pipeline user
# ---------------------------------------------------------------------------
echo "[5/5] Configuring FlowFirst pipeline user ..."
if ! sudo rabbitmqctl list_users 2>/dev/null | grep -q "^flowuser"; then
    sudo rabbitmqctl add_user flowuser flowpassword || true
    sudo rabbitmqctl set_user_tags flowuser administrator || true
    sudo rabbitmqctl set_permissions -p / flowuser ".*" ".*" ".*" || true
    echo "  Created user: flowuser"
else
    echo "  User flowuser already exists — skipping."
fi

echo ""
echo "=================================================================="
echo " RabbitMQ installation completed successfully!"
echo ""
echo " Architecture        :  ${ARCH}"
echo " AMQP Broker Port    :  5672"
echo " Cluster Bus Port    :  25672"
echo " Management Web UI   :  http://<node-ip>:15672"
echo "   Default login     :  guest / guest"
echo "   Pipeline login    :  flowuser / flowpassword"
echo ""
echo " Service status:"
sudo systemctl status rabbitmq-server --no-pager -l
echo "=================================================================="
