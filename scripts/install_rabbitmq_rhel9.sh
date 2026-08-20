#!/usr/bin/env bash
set -euo pipefail

# ==============================================================================
# FlowFirst - RabbitMQ & Erlang Installation Script for RHEL 9.6
# ==============================================================================

echo "=================================================================="
echo " Installing RabbitMQ Server & Erlang on RHEL 9.6"
echo "=================================================================="

# 1. Import GPG Signing Keys for Erlang and RabbitMQ
echo "[1/5] Importing GPG keys..."
sudo rpm --import 'https://dl.cloudsmith.io/public/rabbitmq/rabbitmq-erlang/gpg.E495BB49CC4BBE5B.key'
sudo rpm --import 'https://dl.cloudsmith.io/public/rabbitmq/rabbitmq-server/gpg.9F4587F22620D4E7.key'

# 2. Add Yum/DNF Repository Definitions for RHEL 9 (el/9)
echo "[2/5] Creating /etc/yum.repos.d/rabbitmq.repo..."
sudo tee /etc/yum.repos.d/rabbitmq.repo << 'EOF'
[rabbitmq-erlang]
name=rabbitmq-erlang
baseurl=https://dl.cloudsmith.io/public/rabbitmq/rabbitmq-erlang/rpm/el/9/$basearch
repo_gpgcheck=1
enabled=1
gpgkey=https://dl.cloudsmith.io/public/rabbitmq/rabbitmq-erlang/gpg.E495BB49CC4BBE5B.key
gpgcheck=0
sslverify=1
sslcacert=/etc/pki/tls/certs/ca-bundle.crt
metadata_expire=300

[rabbitmq-server]
name=rabbitmq-server
baseurl=https://dl.cloudsmith.io/public/rabbitmq/rabbitmq-server/rpm/el/9/noarch
repo_gpgcheck=1
enabled=1
gpgkey=https://dl.cloudsmith.io/public/rabbitmq/rabbitmq-server/gpg.9F4587F22620D4E7.key
gpgcheck=0
sslverify=1
sslcacert=/etc/pki/tls/certs/ca-bundle.crt
metadata_expire=300
EOF

# 3. Update Package Cache and Install Erlang & RabbitMQ Server
echo "[3/5] Installing erlang and rabbitmq-server via dnf..."
sudo dnf update -y
sudo dnf install -y erlang rabbitmq-server

# 4. Enable Management Plugin & Start RabbitMQ Service
echo "[4/5] Enabling rabbitmq_management plugin and starting service..."
sudo rabbitmq-plugins enable rabbitmq_management
sudo systemctl enable --now rabbitmq-server

# 5. Configure Default Admin User / Permissions (optional for remote management)
echo "[5/5] Configuring RabbitMQ default admin permissions..."
if ! sudo rabbitmqctl list_users | grep -q "flowuser"; then
    sudo rabbitmqctl add_user flowuser flowpassword || true
    sudo rabbitmqctl set_user_tags flowuser administrator || true
    sudo rabbitmqctl set_permissions -p / flowuser ".*" ".*" ".*" || true
fi

echo ""
echo "=================================================================="
echo " RabbitMQ installation completed successfully!"
echo " AMQP Broker Port:   5672"
echo " Management Web UI:  http://<IP>:15672 (guest/guest or flowuser/flowpassword)"
echo " Service Status:"
sudo systemctl status rabbitmq-server --no-pager
echo "=================================================================="
