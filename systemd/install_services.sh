#!/usr/bin/env bash
set -euo pipefail

# FlowFirst systemd service installer
INSTALL_DIR="${1:-/opt/flowfirst}"
SYSTEMD_DIR="/etc/systemd/system"
RUN_USER="flowuser"

echo "=== FlowFirst Systemd Services Installation ==="
echo "Target directory: ${INSTALL_DIR}"
echo "Running user:     ${RUN_USER}"

# 1. Create dedicated system user if not exists
if ! id -u "${RUN_USER}" >/dev/null 2>&1; then
    echo "Creating system user '${RUN_USER}'..."
    useradd --system --no-create-home --shell /sbin/nologin "${RUN_USER}"
fi

# 2. Ensure application directory exists and has proper permissions
mkdir -p "${INSTALL_DIR}"
chown -R "${RUN_USER}:${RUN_USER}" "${INSTALL_DIR}"

# 3. Copy service unit files into /etc/systemd/system/
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
echo "Copying unit files to ${SYSTEMD_DIR}..."

for unit in flowfirst-process1.service flowfirst-process2.service flowfirst-process3.service flowfirst-process4.service flowfirst.target; do
    sed "s|/opt/flowfirst|${INSTALL_DIR}|g; s|User=flowuser|User=${RUN_USER}|g; s|Group=flowuser|Group=${RUN_USER}|g" \
        "${SCRIPT_DIR}/${unit}" > "${SYSTEMD_DIR}/${unit}"
    chmod 644 "${SYSTEMD_DIR}/${unit}"
    echo "  Installed ${unit}"
done

# 4. Reload systemd daemon
systemctl daemon-reload

echo ""
echo "=== Installation Completed ==="
echo "You can now control the pipeline with:"
echo "  sudo systemctl start flowfirst.target     # Starts all 4 processes"
echo "  sudo systemctl status 'flowfirst-*'       # Check status of all processes"
echo "  sudo systemctl stop flowfirst.target      # Stops all 4 processes"
echo "  sudo systemctl enable flowfirst.target    # Enable auto-start on boot"
