#!/usr/bin/env bash
set -euo pipefail
# ==============================================================================
# FlowFirst - install and configure Grafana (OSS) on RHEL 9 (single node).
#
#   sudo ./monitoring/setup_grafana.sh
#
# What this does:
#   1. Reads GRAFANA_* from .env (repo .env, or /opt/flowfirst/.env) and validates them (fail closed).
#   2. Adds the official Grafana RPM repository (GPG-checked) and installs the 'grafana' package with dnf.
#   3. Writes /etc/grafana/grafana.ini (the RPM default is kept once as grafana.ini.rpm-default).
#   4. Stores the admin password and the data-encryption key as mode-640 files in /etc/grafana/secrets
#      (grafana.ini reads them with $__file{}; no secret is ever on a command line or in grafana.ini).
#   5. Provisions the Prometheus datasource and the two FlowFirst dashboards from this repo.
#   6. Installs a small systemd hardening drop-in, enables and starts grafana-server.service.
#
# Run it on the host that runs Prometheus (monitoring/setup_prometheus.sh), or any host that can reach it.
# Safe to re-run: it re-applies configuration and dashboards and restarts Grafana. It never changes an existing
# data-encryption key, and it does not change the admin password of an existing Grafana database (see the ReadMe).
#
# Requires: dnf, curl, openssl, python3, sudo. Settings (all optional except the password), in .env:
#   GRAFANA_ADMIN_PASSWORD      REQUIRED, strong (./scripts/generate_secrets.sh --write creates it)
#   GRAFANA_ADMIN_USER          default admin
#   GRAFANA_HOST                address to bind, default 127.0.0.1. Never 0.0.0.0 / :: / blank (refused).
#   GRAFANA_PORT                default 3000 (1024-65535)
#   GRAFANA_PROMETHEUS_URL      default http://127.0.0.1:9090
#   GRAFANA_TLS_ENABLED         default false. true = HTTPS (TLS 1.2+); needs the two files below
#   GRAFANA_TLS_CERT_FILE / GRAFANA_TLS_KEY_FILE   PEM server certificate (with the chain) and its key
#   GRAFANA_TLS_CA_FILE         optional; used only so this script can health-check the HTTPS listener
# ==============================================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(dirname "$SCRIPT_DIR")"

[[ $EUID -eq 0 ]] || { echo "ERROR: run as root (sudo)" >&2; exit 1; }
die() { echo "ERROR: $*" >&2; exit 1; }

# ---- settings from .env -------------------------------------------------------
ENV_FILE=""
for cand in "${REPO_DIR}/.env" /opt/flowfirst/.env; do
    [[ -f "$cand" ]] && { ENV_FILE="$cand"; break; }
done
if [[ -n "$ENV_FILE" ]]; then
    set -a
    # shellcheck disable=SC1090
    source "$ENV_FILE"
    set +a
else
    echo "WARNING: no .env found; using defaults, and GRAFANA_ADMIN_PASSWORD must be in the environment." >&2
fi

GRAFANA_ADMIN_USER="${GRAFANA_ADMIN_USER:-admin}"
GRAFANA_ADMIN_PASSWORD="${GRAFANA_ADMIN_PASSWORD:-}"
GRAFANA_HOST="${GRAFANA_HOST:-127.0.0.1}"
GRAFANA_PORT="${GRAFANA_PORT:-3000}"
GRAFANA_PROMETHEUS_URL="${GRAFANA_PROMETHEUS_URL:-http://127.0.0.1:9090}"
GRAFANA_TLS_ENABLED="$(printf '%s' "${GRAFANA_TLS_ENABLED:-false}" | tr '[:upper:]' '[:lower:]')"
GRAFANA_TLS_CERT_FILE="${GRAFANA_TLS_CERT_FILE:-}"
GRAFANA_TLS_KEY_FILE="${GRAFANA_TLS_KEY_FILE:-}"
GRAFANA_TLS_CA_FILE="${GRAFANA_TLS_CA_FILE:-}"

# shellcheck disable=SC1091
source "${REPO_DIR}/scripts/secrets_lib.sh"
require_strong_secret GRAFANA_ADMIN_PASSWORD "${GRAFANA_ADMIN_PASSWORD}"

[[ "$GRAFANA_ADMIN_USER" =~ ^[A-Za-z0-9._-]{1,32}$ ]] || die "GRAFANA_ADMIN_USER must be 1-32 characters of A-Z a-z 0-9 . _ -"
case "$GRAFANA_HOST" in
    ""|0.0.0.0|::|"[::]") die "GRAFANA_HOST must be a specific address (127.0.0.1 or this host's IP), not '${GRAFANA_HOST}'. Binding every interface is refused." ;;
esac
[[ "$GRAFANA_HOST" =~ ^[A-Za-z0-9._:-]{1,253}$ ]] || die "GRAFANA_HOST contains characters that are not allowed."
[[ "$GRAFANA_PORT" =~ ^[0-9]{4,5}$ && "$GRAFANA_PORT" -ge 1024 && "$GRAFANA_PORT" -le 65535 ]] || die "GRAFANA_PORT must be 1024-65535."
_url_re='^https?://[]A-Za-z0-9._:/[-]+$'    # "]" first in the bracket expression = literal; portable across bash versions
[[ "$GRAFANA_PROMETHEUS_URL" =~ $_url_re ]] || die "GRAFANA_PROMETHEUS_URL must be http(s)://host[:port] with no spaces or special characters."
case "$GRAFANA_TLS_ENABLED" in
    true|false) ;;
    *) die "GRAFANA_TLS_ENABLED must be true or false." ;;
esac
if [[ "$GRAFANA_TLS_ENABLED" == "true" ]]; then
    [[ -r "$GRAFANA_TLS_CERT_FILE" ]] || die "GRAFANA_TLS_ENABLED=true but GRAFANA_TLS_CERT_FILE is missing or unreadable. There is no plain-text fallback."
    [[ -r "$GRAFANA_TLS_KEY_FILE"  ]] || die "GRAFANA_TLS_ENABLED=true but GRAFANA_TLS_KEY_FILE is missing or unreadable. There is no plain-text fallback."
    [[ -z "$GRAFANA_TLS_CA_FILE" || -r "$GRAFANA_TLS_CA_FILE" ]] || die "GRAFANA_TLS_CA_FILE is set but unreadable."
    openssl x509 -in "$GRAFANA_TLS_CERT_FILE" -noout >/dev/null 2>&1 || die "GRAFANA_TLS_CERT_FILE is not a PEM certificate."
    openssl x509 -in "$GRAFANA_TLS_CERT_FILE" -noout -checkend 0 >/dev/null 2>&1 || die "GRAFANA_TLS_CERT_FILE has expired."
    cert_pub="$(openssl x509 -in "$GRAFANA_TLS_CERT_FILE" -noout -pubkey | openssl sha256)"
    key_pub="$(openssl pkey -in "$GRAFANA_TLS_KEY_FILE" -pubout 2>/dev/null | openssl sha256)" || die "GRAFANA_TLS_KEY_FILE is not a readable PEM private key (passphrase-protected keys are not supported)."
    [[ "$cert_pub" == "$key_pub" ]] || die "GRAFANA_TLS_CERT_FILE and GRAFANA_TLS_KEY_FILE do not belong together."
fi
SCHEME="http"; [[ "$GRAFANA_TLS_ENABLED" == "true" ]] && SCHEME="https"

# ---- check the files we ship before touching the system ---------------------------
PROV_SRC="$SCRIPT_DIR/grafana/provisioning"
DASH_SRC="$SCRIPT_DIR/grafana/dashboards"
[[ -f "$PROV_SRC/datasources/flowfirst.yml" && -f "$PROV_SRC/dashboards/flowfirst.yml" ]] || die "provisioning files missing under $PROV_SRC"
ls "$DASH_SRC"/*.json >/dev/null 2>&1 || die "no dashboard JSON in $DASH_SRC"
for f in "$DASH_SRC"/*.json; do
    python3 -c 'import json,sys; json.load(open(sys.argv[1]))' "$f" || die "$f is not valid JSON"
done

# ---- install Grafana from the official RPM repository ----------------------------
echo "[1/6] Adding the Grafana RPM repository (GPG-checked) ..."
cat > /etc/yum.repos.d/grafana.repo <<'REPO'
[grafana]
name=grafana
baseurl=https://rpm.grafana.com
repo_gpgcheck=1
enabled=1
gpgcheck=1
gpgkey=https://rpm.grafana.com/gpg.key
sslverify=1
sslcacert=/etc/pki/tls/certs/ca-bundle.crt
REPO

echo "[2/6] Installing the grafana package ..."
dnf install -y grafana
id grafana >/dev/null 2>&1 || die "package 'grafana' did not create the grafana user"

# ---- secrets ------------------------------------------------------------------------
echo "[3/6] Writing secrets (mode 640, root:grafana) ..."
install -d -o root -g grafana -m 750 /etc/grafana/secrets
( umask 027; printf '%s' "$GRAFANA_ADMIN_PASSWORD" > /etc/grafana/secrets/admin_password )
chown root:grafana /etc/grafana/secrets/admin_password; chmod 640 /etc/grafana/secrets/admin_password
if [[ ! -s /etc/grafana/secrets/secret_key ]]; then
    # Encrypts datasource credentials stored in the Grafana database. NEVER change it afterwards.
    ( umask 027; openssl rand -hex 16 | tr -d '\n' > /etc/grafana/secrets/secret_key )
    chown root:grafana /etc/grafana/secrets/secret_key; chmod 640 /etc/grafana/secrets/secret_key
    echo "  created a new data-encryption key (/etc/grafana/secrets/secret_key): back it up with the Grafana database"
fi

TLS_INI=""
if [[ "$GRAFANA_TLS_ENABLED" == "true" ]]; then
    install -d -o root -g grafana -m 750 /etc/grafana/tls
    install -m 640 -o root -g grafana "$GRAFANA_TLS_CERT_FILE" /etc/grafana/tls/grafana.crt
    install -m 640 -o root -g grafana "$GRAFANA_TLS_KEY_FILE"  /etc/grafana/tls/grafana.key
    TLS_INI=$'cert_file = /etc/grafana/tls/grafana.crt\ncert_key = /etc/grafana/tls/grafana.key\nmin_tls_version = TLS1.2'
fi

# ---- grafana.ini ----------------------------------------------------------------------
echo "[4/6] Writing /etc/grafana/grafana.ini ..."
if [[ -f /etc/grafana/grafana.ini && ! -f /etc/grafana/grafana.ini.rpm-default ]]; then
    cp -p /etc/grafana/grafana.ini /etc/grafana/grafana.ini.rpm-default
fi
COOKIE_SECURE="false"; HSTS="false"
[[ "$GRAFANA_TLS_ENABLED" == "true" ]] && { COOKIE_SECURE="true"; HSTS="true"; }
cat > /etc/grafana/grafana.ini <<INI
# Managed by FlowFirst monitoring/setup_grafana.sh - re-running the script overwrites this file.
# Everything not listed here keeps the Grafana default. The RPM's original is grafana.ini.rpm-default.
[server]
protocol = ${SCHEME}
http_addr = ${GRAFANA_HOST}
http_port = ${GRAFANA_PORT}
${TLS_INI}

[security]
admin_user = ${GRAFANA_ADMIN_USER}
# admin_password applies only when the Grafana database is first created; see the ReadMe to change it later.
admin_password = \$__file{/etc/grafana/secrets/admin_password}
secret_key = \$__file{/etc/grafana/secrets/secret_key}
cookie_secure = ${COOKIE_SECURE}
cookie_samesite = lax
strict_transport_security = ${HSTS}
allow_embedding = false
disable_gravatar = true

[users]
allow_sign_up = false
allow_org_create = false

[auth.anonymous]
enabled = false

[analytics]
reporting_enabled = false
check_for_updates = false
check_for_plugin_updates = false
feedback_links_enabled = false

[snapshots]
external_enabled = false

[metrics]
enabled = false
INI
chown root:grafana /etc/grafana/grafana.ini; chmod 640 /etc/grafana/grafana.ini

# ---- provisioning and dashboards ---------------------------------------------------------
echo "[5/6] Provisioning the Prometheus datasource and the FlowFirst dashboards ..."
install -d -o root -g grafana -m 750 /etc/grafana/provisioning/datasources /etc/grafana/provisioning/dashboards
sed "s|__PROMETHEUS_URL__|${GRAFANA_PROMETHEUS_URL}|" "$PROV_SRC/datasources/flowfirst.yml" > /etc/grafana/provisioning/datasources/flowfirst.yml
install -m 640 -o root -g grafana "$PROV_SRC/dashboards/flowfirst.yml" /etc/grafana/provisioning/dashboards/flowfirst.yml
chown root:grafana /etc/grafana/provisioning/datasources/flowfirst.yml; chmod 640 /etc/grafana/provisioning/datasources/flowfirst.yml
install -d -o root -g grafana -m 750 /var/lib/grafana/dashboards/flowfirst
rm -f /var/lib/grafana/dashboards/flowfirst/*.json     # the repo is the source of truth; stale copies must not linger
for f in "$DASH_SRC"/*.json; do
    install -m 640 -o root -g grafana "$f" "/var/lib/grafana/dashboards/flowfirst/$(basename "$f")"
done

# ---- systemd -------------------------------------------------------------------------------
echo "[6/6] Installing the systemd hardening drop-in, enabling and starting grafana-server ..."
install -d -m 755 /etc/systemd/system/grafana-server.service.d
cat > /etc/systemd/system/grafana-server.service.d/10-hardening.conf <<'UNIT'
# FlowFirst: modest sandbox for Grafana. It needs only TCP (Prometheus, browsers), /var/lib/grafana and /var/log/grafana.
# Per-host exceptions go in a 99-local.conf drop-in. Re-test with: systemd-analyze security grafana-server
[Service]
NoNewPrivileges=true
PrivateTmp=true
ProtectHome=true
ProtectKernelTunables=true
ProtectKernelModules=true
ProtectControlGroups=true
RestrictAddressFamilies=AF_INET AF_INET6 AF_UNIX
UNIT
systemctl daemon-reload
systemctl enable grafana-server
systemctl restart grafana-server

# ---- health check (no credentials needed) ---------------------------------------------------
HEALTH_HOST="$GRAFANA_HOST"; [[ "$HEALTH_HOST" == *:* ]] && HEALTH_HOST="[${HEALTH_HOST}]"
CURL_TLS=()
HEALTH_OK=skip
if [[ "$GRAFANA_TLS_ENABLED" == "true" ]]; then
    [[ -n "$GRAFANA_TLS_CA_FILE" ]] && CURL_TLS=(--cacert "$GRAFANA_TLS_CA_FILE") || HEALTH_OK=notls
fi
if [[ "$HEALTH_OK" == "skip" ]]; then
    HEALTH_OK=no
    for _ in $(seq 1 30); do
        if curl -fsS "${CURL_TLS[@]}" --max-time 3 "${SCHEME}://${HEALTH_HOST}:${GRAFANA_PORT}/api/health" 2>/dev/null | grep -q '"database": *"ok"'; then HEALTH_OK=yes; break; fi
        sleep 1
    done
fi

echo ""
echo "Grafana        : ${SCHEME}://${GRAFANA_HOST}:${GRAFANA_PORT}   (user: ${GRAFANA_ADMIN_USER}; password: GRAFANA_ADMIN_PASSWORD in .env)"
echo "Configuration  : /etc/grafana/grafana.ini, /etc/grafana/provisioning/"
echo "Dashboards     : /var/lib/grafana/dashboards/flowfirst/  (folder 'FlowFirst' in the UI)"
echo "Datasource     : FlowFirst Prometheus -> ${GRAFANA_PROMETHEUS_URL}"
case "$HEALTH_OK" in
    yes)   echo "Health check   : OK (/api/health reports database ok)" ;;
    notls) echo "Health check   : skipped (set GRAFANA_TLS_CA_FILE to let this script check the HTTPS listener)" ;;
    *)     echo "Health check   : FAILED - see: journalctl -u grafana-server -n 50   and   /var/log/grafana/grafana.log" >&2; exit 1 ;;
esac
echo ""
echo "Next steps:"
echo "  1. Open the port to your browsers only (not needed for 127.0.0.1; use an SSH tunnel instead):"
echo "     sudo firewall-cmd --permanent --add-rich-rule='rule family=\"ipv4\" source address=\"<admin-network>/<prefix>\" port port=\"${GRAFANA_PORT}\" protocol=\"tcp\" accept' && sudo firewall-cmd --reload"
echo "  2. Log in and open Dashboards -> FlowFirst. Panels stay empty until Prometheus scrapes the cluster (ReadMe, Phase 7)."
