#!/usr/bin/env bash
set -euo pipefail

# ==============================================================================
# FlowFirst - RabbitMQ Server & Erlang Installation Script for RHEL 9.6
#
# Repo mirrors (as of 2025):
#   The old Cloudsmith baseurls (dl.cloudsmith.io/public/rabbitmq/…) are dead.
#   The old Cloudsmith key URL for the server key had a wrong fingerprint suffix.
#   Current canonical mirrors: yum1.rabbitmq.com / yum2.rabbitmq.com
#   GPG keys: github.com/rabbitmq/signing-keys/releases/tag/3.0
# ==============================================================================

echo "=================================================================="
echo " Installing RabbitMQ Server & Erlang on RHEL 9.6"
echo "=================================================================="

# ---------------------------------------------------------------------------
# Key URLs — github.com/rabbitmq/signing-keys, release 3.0
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

# ---------------------------------------------------------------------------
# 2. Write DNF/Yum Repository Definitions
#    Mirrors: yum1.rabbitmq.com / yum2.rabbitmq.com  (Cloudsmith is dead)
#    Both mirrors are listed so dnf falls back automatically if one is down.
# ---------------------------------------------------------------------------
echo "[2/5] Writing /etc/yum.repos.d/rabbitmq.repo ..."

sudo tee /etc/yum.repos.d/rabbitmq.repo > /dev/null << EOF
## ── Erlang ($basearch packages) ─────────────────────────────────────────────
[rabbitmq-erlang]
name=rabbitmq-erlang
baseurl=https://yum1.rabbitmq.com/erlang/el/9/\$basearch
        https://yum2.rabbitmq.com/erlang/el/9/\$basearch
repo_gpgcheck=1
enabled=1
gpgkey=https://github.com/rabbitmq/signing-keys/releases/download/3.0/cloudsmith.rabbitmq-erlang.E495BB49CC4BBE5B.key
gpgcheck=1
sslverify=1
sslcacert=/etc/pki/tls/certs/ca-bundle.crt
metadata_expire=300

## ── Erlang (noarch packages) ─────────────────────────────────────────────────
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

## ── RabbitMQ Server ($basearch packages) ─────────────────────────────────────
[rabbitmq-server]
name=rabbitmq-server
baseurl=https://yum2.rabbitmq.com/rabbitmq/el/9/\$basearch
        https://yum1.rabbitmq.com/rabbitmq/el/9/\$basearch
repo_gpgcheck=1
enabled=1
gpgkey=https://github.com/rabbitmq/signing-keys/releases/download/3.0/cloudsmith.rabbitmq-server.9F4587F226208342.key
       https://github.com/rabbitmq/signing-keys/releases/download/3.0/rabbitmq-release-signing-key.asc
gpgcheck=1
sslverify=1
sslcacert=/etc/pki/tls/certs/ca-bundle.crt
metadata_expire=300

## ── RabbitMQ Server (noarch packages) ───────────────────────────────────────
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
echo " AMQP Broker Port  :  5672"
echo " Cluster Bus Port  :  25672"
echo " Management Web UI :  http://<node-ip>:15672"
echo "   Default login   :  guest / guest"
echo "   Pipeline login  :  flowuser / flowpassword"
echo ""
echo " Service status:"
sudo systemctl status rabbitmq-server --no-pager -l
echo "=================================================================="
