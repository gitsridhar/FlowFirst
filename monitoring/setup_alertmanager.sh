#!/usr/bin/env bash
set -euo pipefail
# ==============================================================================
# FlowFirst - install and configure Alertmanager on RHEL 9 (single node).
#
#   sudo ./monitoring/setup_alertmanager.sh
#
# What this does:
#   1. Downloads the latest Alertmanager release from GitHub (x86_64 or aarch64).
#   2. Installs binaries to /usr/local/bin.
#   3. Creates the 'alertmanager' system user and /etc/alertmanager, /var/lib/alertmanager.
#   4. Copies monitoring/alertmanager.yml to /etc/alertmanager/alertmanager.yml.
#   5. Installs a systemd unit for Alertmanager.
#   6. Installs a systemd unit for the file receiver (monitoring/alertmanager-file-receiver.py).
#   7. Enables and starts both units.
#
# Run this on ONE of the cluster nodes (or a dedicated monitoring host). Prometheus must
# point its alerting: block at this node's IP + port 9093. See monitoring/prometheus.yml.
#
# Requires: curl, tar, gzip, python3 (for the receiver), sudo.
# ==============================================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(dirname "$SCRIPT_DIR")"

[[ $EUID -eq 0 ]] || { echo "ERROR: run as root (sudo)" >&2; exit 1; }

# ---- resolve latest release from GitHub API ---------------------------------
echo "[1/7] Resolving latest Alertmanager release ..."
ARCH="$(uname -m)"
case "$ARCH" in
    x86_64)  GOARCH="amd64" ;;
    aarch64) GOARCH="arm64" ;;
    *)       echo "ERROR: unsupported architecture $ARCH" >&2; exit 1 ;;
esac
LATEST_TAG="$(curl -fsSL 'https://api.github.com/repos/prometheus/alertmanager/releases/latest' \
    | python3 -c 'import json,sys; print(json.load(sys.stdin)["tag_name"])')"
VERSION="${LATEST_TAG#v}"
URL="https://github.com/prometheus/alertmanager/releases/download/${LATEST_TAG}/alertmanager-${VERSION}.linux-${GOARCH}.tar.gz"
echo "  Alertmanager ${LATEST_TAG} (${GOARCH})"

# ---- download and install ---------------------------------------------------
echo "[2/7] Downloading ..."
TMPDIR="$(mktemp -d)"; trap "rm -rf $TMPDIR" EXIT
curl -fsSL -o "$TMPDIR/alertmanager.tar.gz" "$URL"
tar -xzf "$TMPDIR/alertmanager.tar.gz" -C "$TMPDIR"
AM_DIR="$TMPDIR/alertmanager-${VERSION}.linux-${GOARCH}"

echo "[3/7] Installing binaries to /usr/local/bin ..."
install -m 755 "$AM_DIR/alertmanager" /usr/local/bin/alertmanager
install -m 755 "$AM_DIR/amtool"       /usr/local/bin/amtool

# ---- user and directories ---------------------------------------------------
echo "[4/7] Creating user and directories ..."
id alertmanager >/dev/null 2>&1 || useradd --system --no-create-home --shell /sbin/nologin alertmanager
install -d -o alertmanager -g alertmanager -m 750 /etc/alertmanager
install -d -o alertmanager -g alertmanager -m 750 /var/lib/alertmanager
install -d -o root -g root -m 755 /var/log/flowfirst 2>/dev/null || true
# The file receiver writes to /var/log/flowfirst/alerts.log; alertmanager user needs write access
chown alertmanager:alertmanager /var/log/flowfirst 2>/dev/null || true

# ---- config -----------------------------------------------------------------
echo "[5/7] Installing alertmanager.yml ..."
install -m 640 -o alertmanager -g alertmanager \
    "$SCRIPT_DIR/alertmanager.yml" /etc/alertmanager/alertmanager.yml

# ---- validate config (amtool is now installed) -------------------------------
amtool check-config /etc/alertmanager/alertmanager.yml \
    && echo "  Config syntax OK" \
    || { echo "ERROR: alertmanager.yml failed validation; aborting." >&2; exit 1; }

# ---- receiver script --------------------------------------------------------
echo "[6/7] Installing alert file receiver ..."
RECEIVER_SRC="$SCRIPT_DIR/alertmanager-file-receiver.py"
[[ -f "$RECEIVER_SRC" ]] || { echo "ERROR: $RECEIVER_SRC not found" >&2; exit 1; }
install -m 755 "$RECEIVER_SRC" /usr/local/bin/flowfirst-alert-receiver

# ---- Python venv path -------------------------------------------------------
PYTHON3="$REPO_DIR/.venv/bin/python3"
[[ -x "$PYTHON3" ]] || PYTHON3="$(command -v python3)"
[[ -n "$PYTHON3" ]] || { echo "ERROR: python3 not found" >&2; exit 1; }

# ---- systemd units ----------------------------------------------------------
echo "[7/7] Installing and enabling systemd units ..."
cat > /etc/systemd/system/alertmanager.service <<UNIT
[Unit]
Description=Alertmanager for FlowFirst
After=network.target prometheus.service
Documentation=https://prometheus.io/docs/alerting/latest/alertmanager/

[Service]
Type=simple
User=alertmanager
Group=alertmanager
ExecStart=/usr/local/bin/alertmanager \\
    --config.file=/etc/alertmanager/alertmanager.yml \\
    --storage.path=/var/lib/alertmanager \\
    --web.listen-address=0.0.0.0:9093 \\
    --log.level=info
Restart=on-failure
RestartSec=5s
LimitNOFILE=65536

[Install]
WantedBy=multi-user.target
UNIT

cat > /etc/systemd/system/flowfirst-alert-receiver.service <<UNIT
[Unit]
Description=FlowFirst alert file receiver (webhook -> /var/log/flowfirst/alerts.log)
After=network.target alertmanager.service

[Service]
Type=simple
User=alertmanager
Group=alertmanager
Environment="ALERT_LOG_FILE=/var/log/flowfirst/alerts.log"
Environment="ALERT_RECEIVER_PORT=9099"
Environment="ALERT_RECEIVER_HOST=127.0.0.1"
ExecStart=${PYTHON3} /usr/local/bin/flowfirst-alert-receiver
Restart=on-failure
RestartSec=5s
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
UNIT

systemctl daemon-reload
systemctl enable --now alertmanager flowfirst-alert-receiver

echo ""
echo "Alertmanager   : http://$(hostname -I | awk '{print $1}'):9093"
echo "Alert log      : /var/log/flowfirst/alerts.log"
echo ""
echo "Next: add the alerting: block to prometheus.yml (see monitoring/prometheus.yml) and restart Prometheus."
echo "Open port 9093 if Prometheus is on a different host:"
echo "  sudo firewall-cmd --permanent --add-port=9093/tcp && sudo firewall-cmd --reload"
