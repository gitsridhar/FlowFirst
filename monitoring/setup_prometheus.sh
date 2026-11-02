#!/usr/bin/env bash
set -euo pipefail
# ==============================================================================
# FlowFirst - install and configure Prometheus on RHEL 9 (single node).
#
#   sudo ./monitoring/setup_prometheus.sh
#
# What this does:
#   1. Resolves and downloads the latest Prometheus release from GitHub (x86_64 or aarch64).
#   2. Installs binaries (prometheus, promtool) to /usr/local/bin.
#   3. Creates the 'prometheus' system user and /etc/prometheus, /var/lib/prometheus.
#   4. Copies monitoring/prometheus.yml and rule files to /etc/prometheus/.
#   5. Validates config and rule files using promtool.
#   6. Installs a hardened systemd unit for Prometheus.
#   7. Enables and starts prometheus.service.
#
# Run this on ONE dedicated monitoring host (or one of the cluster nodes).
#
# Requires: curl, tar, gzip, python3, sudo.
# ==============================================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(dirname "$SCRIPT_DIR")"

[[ $EUID -eq 0 ]] || { echo "ERROR: run as root (sudo)" >&2; exit 1; }

# ---- resolve latest release from GitHub API ---------------------------------
echo "[1/7] Resolving latest Prometheus release ..."
ARCH="$(uname -m)"
case "$ARCH" in
    x86_64)  GOARCH="amd64" ;;
    aarch64) GOARCH="arm64" ;;
    *)       echo "ERROR: unsupported architecture $ARCH" >&2; exit 1 ;;
esac
LATEST_TAG="$(curl -fsSL 'https://api.github.com/repos/prometheus/prometheus/releases/latest' \
    | python3 -c 'import json,sys; print(json.load(sys.stdin)["tag_name"])')"
VERSION="${LATEST_TAG#v}"
URL="https://github.com/prometheus/prometheus/releases/download/${LATEST_TAG}/prometheus-${VERSION}.linux-${GOARCH}.tar.gz"
echo "  Prometheus ${LATEST_TAG} (${GOARCH})"

# ---- download and install ---------------------------------------------------
echo "[2/7] Downloading ..."
TMPDIR="$(mktemp -d)"; trap "rm -rf $TMPDIR" EXIT
curl -fsSL -o "$TMPDIR/prometheus.tar.gz" "$URL"
tar -xzf "$TMPDIR/prometheus.tar.gz" -C "$TMPDIR"
PROM_DIR="$TMPDIR/prometheus-${VERSION}.linux-${GOARCH}"

echo "[3/7] Installing binaries to /usr/local/bin ..."
install -m 755 "$PROM_DIR/prometheus" /usr/local/bin/prometheus
install -m 755 "$PROM_DIR/promtool"   /usr/local/bin/promtool

# ---- user and directories ---------------------------------------------------
echo "[4/7] Creating user and directories ..."
id prometheus >/dev/null 2>&1 || useradd --system --no-create-home --shell /sbin/nologin prometheus
install -d -o prometheus -g prometheus -m 750 /etc/prometheus
install -d -o prometheus -g prometheus -m 750 /var/lib/prometheus

# ---- config and rule files --------------------------------------------------
echo "[5/7] Installing prometheus.yml and rule files ..."
install -m 640 -o prometheus -g prometheus \
    "$SCRIPT_DIR/prometheus.yml" /etc/prometheus/prometheus.yml

if [[ -f "$SCRIPT_DIR/flowfirst.rules.yml" ]]; then
    install -m 640 -o prometheus -g prometheus \
        "$SCRIPT_DIR/flowfirst.rules.yml" /etc/prometheus/flowfirst.rules.yml
fi

if [[ -f "$SCRIPT_DIR/infrastructure.rules.yml" ]]; then
    install -m 640 -o prometheus -g prometheus \
        "$SCRIPT_DIR/infrastructure.rules.yml" /etc/prometheus/infrastructure.rules.yml
fi

# ---- validate config (promtool is now installed) ----------------------------
echo "  Validating Prometheus configuration with promtool ..."
promtool check config /etc/prometheus/prometheus.yml \
    && echo "  Config and rules syntax OK" \
    || { echo "ERROR: prometheus.yml failed validation; aborting." >&2; exit 1; }

# ---- systemd unit -----------------------------------------------------------
echo "[6/7] Installing systemd unit ..."
cat > /etc/systemd/system/prometheus.service <<UNIT
[Unit]
Description=Prometheus Server for FlowFirst
After=network.target
Documentation=https://prometheus.io/docs/introduction/overview/

[Service]
Type=simple
User=prometheus
Group=prometheus
ExecStart=/usr/local/bin/prometheus \\
    --config.file=/etc/prometheus/prometheus.yml \\
    --storage.tsdb.path=/var/lib/prometheus \\
    --web.listen-address=0.0.0.0:9090 \\
    --web.enable-lifecycle \\
    --log.level=info
ExecReload=/usr/bin/curl -s -X POST http://localhost:9090/-/reload
Restart=on-failure
RestartSec=5s
LimitNOFILE=65536

[Install]
WantedBy=multi-user.target
UNIT

# ---- enable and start -------------------------------------------------------
echo "[7/7] Enabling and starting prometheus.service ..."
systemctl daemon-reload
systemctl enable --now prometheus

echo ""
echo "Prometheus Server : http://$(hostname -I | awk '{print $1}'):9090"
echo "Configuration     : /etc/prometheus/prometheus.yml"
echo "Rules             : /etc/prometheus/*.rules.yml"
echo "TSDB Storage      : /var/lib/prometheus"
echo ""
echo "Next steps:"
echo "  1. Update target IPs in /etc/prometheus/prometheus.yml to match your cluster nodes."
echo "  2. Validate changes: promtool check config /etc/prometheus/prometheus.yml"
echo "  3. Reload without restarting: curl -s -X POST http://localhost:9090/-/reload"
echo "  4. If Prometheus is on a dedicated host, open port 9090:"
echo "     sudo firewall-cmd --permanent --add-port=9090/tcp && sudo firewall-cmd --reload"
