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

# Mode selection:
# --remote / node4: install only flowfirst-process5.service
# default (node1-node3): install flowfirst-process1..4 and flowfirst.target
TARGET_MODE="${2:-cluster}"

if [[ "${TARGET_MODE}" == "--remote" ]] || [[ "${TARGET_MODE}" == "remote" ]] || [[ "${TARGET_MODE}" == "node4" ]]; then
    UNITS=("flowfirst-process5.service")
    echo "Installing Remote Node units (Process 5 only)..."
else
    UNITS=("flowfirst-process1.service" "flowfirst-process2.service" "flowfirst-process3.service" "flowfirst-process4.service" "flowfirst.target")
    echo "Installing Cluster Node units (Processes 1-4 & target)..."
fi

for unit in "${UNITS[@]}"; do
    sed "s|/opt/flowfirst|${INSTALL_DIR}|g; s|User=flowuser|User=${RUN_USER}|g; s|Group=flowuser|Group=${RUN_USER}|g" \
        "${SCRIPT_DIR}/${unit}" > "${SYSTEMD_DIR}/${unit}"
    chmod 644 "${SYSTEMD_DIR}/${unit}"
    echo "  Installed ${unit}"
done

# 3b. Sandbox drop-in: one shared file, installed next to every unit as <unit>.d/10-hardening.conf.
#     Skip with FLOWFIRST_NO_HARDENING=1 (only to bisect a problem; the units run unsandboxed then).
HARDEN_SRC="${SCRIPT_DIR}/flowfirst-hardening.conf"
for unit in "${UNITS[@]}"; do
    [[ "${unit}" == *.service ]] || continue
    dropdir="${SYSTEMD_DIR}/${unit}.d"
    if [[ "${FLOWFIRST_NO_HARDENING:-0}" == "1" ]]; then
        rm -f "${dropdir}/10-hardening.conf"
        echo "  Sandbox NOT installed for ${unit} (FLOWFIRST_NO_HARDENING=1)"
        continue
    fi
    [[ -f "${HARDEN_SRC}" ]] || { echo "ERROR: ${HARDEN_SRC} not found" >&2; exit 1; }
    mkdir -p "${dropdir}"
    cp "${HARDEN_SRC}" "${dropdir}/10-hardening.conf"
    # An application directory under /home or /root must stay readable: ProtectHome=true would hide it.
    if [[ "${INSTALL_DIR}" == /home/* || "${INSTALL_DIR}" == /root/* ]]; then
        sed -i 's/^ProtectHome=true/ProtectHome=read-only/' "${dropdir}/10-hardening.conf"
        echo "  ${INSTALL_DIR} is under /home or /root: ProtectHome=read-only for ${unit}"
    fi
    chmod 644 "${dropdir}/10-hardening.conf"
    echo "  Sandbox installed for ${unit}"
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
echo "  systemd-analyze security flowfirst-process1.service   # sandbox score; lower is better"
