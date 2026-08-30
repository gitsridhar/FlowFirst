#!/usr/bin/env bash
set -euo pipefail

# ==============================================================================
# FlowFirst - RabbitMQ Server & Erlang Installation Script for RHEL 9.6
#
# Architecture-aware repo selection (verified live as of 2025):
#
#   x86_64 ── Erlang + RabbitMQ server:  yum1/yum2.rabbitmq.com
#   aarch64 ─ Erlang + RabbitMQ server:  packagecloud.io/rabbitmq
#
# Why two sources?
#   yum1/yum2.rabbitmq.com only carries x86_64 (+ noarch); aarch64 returns 404.
#   packagecloud.io carries both x86_64 and aarch64, and is the fallback for ARM.
#
# GPG keys: github.com/rabbitmq/signing-keys/releases/tag/3.0
# ==============================================================================

echo "=================================================================="
echo " Installing RabbitMQ Server & Erlang on RHEL 9.6"
echo "=================================================================="

# ---------------------------------------------------------------------------
# Detect system architecture
# ---------------------------------------------------------------------------
ARCH=$(uname -m)          # x86_64 | aarch64
echo "  Detected architecture: ${ARCH}"

# ---------------------------------------------------------------------------
# Key URLs — github.com/rabbitmq/signing-keys, release 3.0
# ---------------------------------------------------------------------------
ERLANG_KEY_URL="https://github.com/rabbitmq/signing-keys/releases/download/3.0/cloudsmith.rabbitmq-erlang.E495BB49CC4BBE5B.key"
SERVER_KEY_URL="https://github.com/rabbitmq/signing-keys/releases/download/3.0/cloudsmith.rabbitmq-server.9F4587F226208342.key"
RELEASE_KEY_URL="https://github.com/rabbitmq/signing-keys/releases/download/3.0/rabbitmq-release-signing-key.asc"
# packagecloud keys (needed for aarch64 repos)
PC_ERLANG_KEY_URL="https://packagecloud.io/rabbitmq/erlang/gpgkey"
PC_SERVER_KEY_URL="https://packagecloud.io/rabbitmq/rabbitmq-server/gpgkey"

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

if [[ "${ARCH}" == "aarch64" ]]; then
    echo "  → Packagecloud Erlang key (aarch64 repos)"
    curl -fsSL -o "${TMPDIR_KEYS}/pc_erlang.key" "${PC_ERLANG_KEY_URL}"
    echo "  → Packagecloud RabbitMQ server key (aarch64 repos)"
    curl -fsSL -o "${TMPDIR_KEYS}/pc_server.key" "${PC_SERVER_KEY_URL}"
    sudo rpm --import "${TMPDIR_KEYS}/pc_erlang.key"
    sudo rpm --import "${TMPDIR_KEYS}/pc_server.key"
fi

echo "  GPG keys imported OK."

# ---------------------------------------------------------------------------
# 2. Write DNF/Yum Repository Definitions
#
#   x86_64:  yum1.rabbitmq.com / yum2.rabbitmq.com
#            (/erlang/el/9/x86_64, /erlang/el/9/noarch,
#             /rabbitmq/el/9/x86_64, /rabbitmq/el/9/noarch)
#
#   aarch64: packagecloud.io/rabbitmq
#            (/erlang/el/9/aarch64, /rabbitmq-server/el/9/aarch64)
# ---------------------------------------------------------------------------
echo "[2/5] Writing /etc/yum.repos.d/rabbitmq.repo (arch=${ARCH}) ..."

if [[ "${ARCH}" == "x86_64" ]]; then

sudo tee /etc/yum.repos.d/rabbitmq.repo > /dev/null << 'EOF'
## ── Erlang x86_64 ────────────────────────────────────────────────────────────
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

## ── Erlang noarch ────────────────────────────────────────────────────────────
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

## ── RabbitMQ Server x86_64 ───────────────────────────────────────────────────
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

## ── RabbitMQ Server noarch ───────────────────────────────────────────────────
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

elif [[ "${ARCH}" == "aarch64" ]]; then

sudo tee /etc/yum.repos.d/rabbitmq.repo > /dev/null << 'EOF'
## ── Erlang aarch64 (packagecloud) ────────────────────────────────────────────
[rabbitmq-erlang]
name=rabbitmq-erlang
baseurl=https://packagecloud.io/rabbitmq/erlang/el/9/aarch64
repo_gpgcheck=1
enabled=1
gpgkey=https://packagecloud.io/rabbitmq/erlang/gpgkey
       https://github.com/rabbitmq/signing-keys/releases/download/3.0/cloudsmith.rabbitmq-erlang.E495BB49CC4BBE5B.key
gpgcheck=1
sslverify=1
sslcacert=/etc/pki/tls/certs/ca-bundle.crt
metadata_expire=300

## ── RabbitMQ Server aarch64 (packagecloud) ───────────────────────────────────
[rabbitmq-server]
name=rabbitmq-server
baseurl=https://packagecloud.io/rabbitmq/rabbitmq-server/el/9/aarch64
repo_gpgcheck=1
enabled=1
gpgkey=https://packagecloud.io/rabbitmq/rabbitmq-server/gpgkey
       https://github.com/rabbitmq/signing-keys/releases/download/3.0/cloudsmith.rabbitmq-server.9F4587F226208342.key
       https://github.com/rabbitmq/signing-keys/releases/download/3.0/rabbitmq-release-signing-key.asc
gpgcheck=1
sslverify=1
sslcacert=/etc/pki/tls/certs/ca-bundle.crt
metadata_expire=300
EOF

else
    echo "ERROR: Unsupported architecture '${ARCH}'. Only x86_64 and aarch64 are supported."
    exit 1
fi

echo "  Repo file written OK."

# ---------------------------------------------------------------------------
# 3. Clean metadata cache and install packages
# ---------------------------------------------------------------------------
echo "[3/5] Refreshing DNF metadata and installing erlang + rabbitmq-server ..."
sudo dnf clean metadata
sudo dnf makecache
sudo dnf install -y erlang rabbitmq-server

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
