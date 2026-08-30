#!/usr/bin/env bash
set -euo pipefail

# ==============================================================================
# FlowFirst - RabbitMQ Server & Erlang Installation Script for RHEL 9.6
#
# Key sources (as of 2025):
#   GPG keys are published at https://github.com/rabbitmq/signing-keys/releases/tag/3.0
#   The old Cloudsmith key URLs (dl.cloudsmith.io/…/gpg.*.key) are NO LONGER VALID.
#   Use the GitHub release assets below instead.
# ==============================================================================

echo "=================================================================="
echo " Installing RabbitMQ Server & Erlang on RHEL 9.6"
echo "=================================================================="

# ---------------------------------------------------------------------------
# Key URLs — sourced from github.com/rabbitmq/signing-keys, release 3.0
# ---------------------------------------------------------------------------
ERLANG_KEY_URL="https://github.com/rabbitmq/signing-keys/releases/download/3.0/cloudsmith.rabbitmq-erlang.E495BB49CC4BBE5B.key"
SERVER_KEY_URL="https://github.com/rabbitmq/signing-keys/releases/download/3.0/cloudsmith.rabbitmq-server.9F4587F226208342.key"
RELEASE_KEY_URL="https://github.com/rabbitmq/signing-keys/releases/download/3.0/rabbitmq-release-signing-key.asc"

# ---------------------------------------------------------------------------
# 1. Import GPG Signing Keys
# ---------------------------------------------------------------------------
echo "[1/5] Importing GPG signing keys from github.com/rabbitmq/signing-keys ..."

# rpm --import cannot read from a pipe on RHEL 9 — download to temp files first
TMPDIR_KEYS=$(mktemp -d)
trap 'rm -rf "${TMPDIR_KEYS}"' EXIT

echo "  → Downloading Erlang repo key (E495BB49CC4BBE5B)"
curl -fsSL -o "${TMPDIR_KEYS}/erlang.key"   "${ERLANG_KEY_URL}"
echo "  → Downloading RabbitMQ server repo key (9F4587F226208342)"
curl -fsSL -o "${TMPDIR_KEYS}/server.key"   "${SERVER_KEY_URL}"
echo "  → Downloading RabbitMQ release signing key (primary trust anchor)"
curl -fsSL -o "${TMPDIR_KEYS}/release.key"  "${RELEASE_KEY_URL}"

echo "  → Importing keys into RPM database"
sudo rpm --import "${TMPDIR_KEYS}/erlang.key"
sudo rpm --import "${TMPDIR_KEYS}/server.key"
sudo rpm --import "${TMPDIR_KEYS}/release.key"

echo "  GPG keys imported OK."

# ---------------------------------------------------------------------------
# 2. Write DNF/Yum Repository Definitions
# ---------------------------------------------------------------------------
echo "[2/5] Writing /etc/yum.repos.d/rabbitmq.repo ..."

sudo tee /etc/yum.repos.d/rabbitmq.repo > /dev/null << EOF
## ── Erlang (from Cloudsmith, signed with E495BB49CC4BBE5B) ─────────────────
[rabbitmq-erlang]
name=rabbitmq-erlang
baseurl=https://dl.cloudsmith.io/public/rabbitmq/rabbitmq-erlang/rpm/el/9/\$basearch
repo_gpgcheck=1
enabled=1
# GPG key hosted on GitHub (Cloudsmith URL is no longer valid)
gpgkey=https://github.com/rabbitmq/signing-keys/releases/download/3.0/cloudsmith.rabbitmq-erlang.E495BB49CC4BBE5B.key
gpgcheck=0
sslverify=1
sslcacert=/etc/pki/tls/certs/ca-bundle.crt
metadata_expire=300

## ── RabbitMQ Server (from Cloudsmith, signed with 9F4587F226208342) ────────
[rabbitmq-server]
name=rabbitmq-server
baseurl=https://dl.cloudsmith.io/public/rabbitmq/rabbitmq-server/rpm/el/9/noarch
repo_gpgcheck=1
enabled=1
# GPG key hosted on GitHub (Cloudsmith URL is no longer valid; fingerprint suffix
# changed from 2620D4E7 → 26208342 in signing-keys release 3.0)
gpgkey=https://github.com/rabbitmq/signing-keys/releases/download/3.0/cloudsmith.rabbitmq-server.9F4587F226208342.key
        https://github.com/rabbitmq/signing-keys/releases/download/3.0/rabbitmq-release-signing-key.asc
gpgcheck=0
sslverify=1
sslcacert=/etc/pki/tls/certs/ca-bundle.crt
metadata_expire=300
EOF

echo "  Repo file written OK."

# ---------------------------------------------------------------------------
# 3. Clean metadata cache and install packages
# ---------------------------------------------------------------------------
echo "[3/5] Cleaning DNF cache and installing erlang + rabbitmq-server ..."
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
# 5. Configure admin user for the pipeline
# ---------------------------------------------------------------------------
echo "[5/5] Configuring FlowFirst pipeline user ..."
if ! sudo rabbitmqctl list_users 2>/dev/null | grep -q "flowuser"; then
    sudo rabbitmqctl add_user flowuser flowpassword || true
    sudo rabbitmqctl set_user_tags flowuser administrator || true
    sudo rabbitmqctl set_permissions -p / flowuser ".*" ".*" ".*" || true
    echo "  Created user: flowuser"
else
    echo "  User flowuser already exists — skipping creation."
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
