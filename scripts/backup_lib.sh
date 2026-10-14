#!/usr/bin/env bash
# ==============================================================================
# FlowFirst - shared helpers for backup_flowfirst.sh and restore_flowfirst.sh. Sourced, not run.
#
# Loads .env (FLOWFIRST_ENV_FILE, else <repo>/.env, else /opt/flowfirst/.env), builds a private MariaDB client option
# file (so the password never appears in `ps`), and offers checksum / encryption helpers.
# ==============================================================================

BK_ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

bk_log()  { printf '%s %s\n' "$(date -u +%H:%M:%S)" "$*"; }
bk_warn() { printf '%s WARN  %s\n' "$(date -u +%H:%M:%S)" "$*" >&2; }
bk_die()  { printf '%s ERROR %s\n' "$(date -u +%H:%M:%S)" "$*" >&2; exit 1; }

bk_load_env() {
    local f="${FLOWFIRST_ENV_FILE:-}"
    if [[ -z "$f" ]]; then
        if   [[ -f "${BK_ROOT_DIR}/.env" ]]; then f="${BK_ROOT_DIR}/.env"
        elif [[ -f /opt/flowfirst/.env ]];  then f=/opt/flowfirst/.env
        else bk_die ".env not found (set FLOWFIRST_ENV_FILE)"; fi
    fi
    [[ -r "$f" ]] || bk_die "cannot read $f"
    set -a; source "$f"; set +a
    BK_ENV_FILE="$f"
}

bk_sha256() {   # bk_sha256 FILE -> hex digest
    if command -v sha256sum >/dev/null 2>&1; then sha256sum "$1" | awk '{print $1}'
    else shasum -a 256 "$1" | awk '{print $1}'; fi
}

# MariaDB client options. Creates a mode-600 file with host, port, user, password and (when TLS_MARIADB_ENABLED) the
# client certificate; removed by bk_cleanup. Sets BK_DB_CNF.
bk_make_db_cnf() {
    local host="${BACKUP_DB_HOST:-${CURRENT_NODE_IP:-${MARIADB_HOST:-127.0.0.1}}}"
    local port="${MARIADB_PORT:-3306}" user="${MARIADB_USER:-}" pass="${MARIADB_PASSWORD:-}"
    [[ -n "$user" && -n "$pass" ]] || bk_die "MARIADB_USER / MARIADB_PASSWORD are not set in $BK_ENV_FILE"
    local old_umask; old_umask="$(umask)"; umask 077
    BK_DB_CNF="$(mktemp "${TMPDIR:-/tmp}/flowfirst-db.XXXXXX")" || bk_die "cannot create a temporary file"
    umask "$old_umask"
    local esc="${pass//\\/\\\\}"; esc="${esc//\"/\\\"}"
    {
        echo "[client]"
        echo "host=${host}"
        echo "port=${port}"
        echo "user=${user}"
        echo "password=\"${esc}\""
        if [[ "${TLS_MARIADB_ENABLED:-false}" == "true" ]]; then
            local d="${TLS_DIR:-/etc/flowfirst/tls}"
            echo "ssl-ca=${TLS_CA_FILE:-$d/ca.crt}"
            echo "ssl-cert=${TLS_CLIENT_CERT:-$d/pipeline/pipeline.crt}"
            echo "ssl-key=${TLS_CLIENT_KEY:-$d/pipeline/pipeline.key}"
            echo "ssl-verify-server-cert"
        fi
    } > "$BK_DB_CNF"
}

bk_db_tool() {   # bk_db_tool dump|client  -> prints the executable name found, or nothing
    local c
    if [[ "$1" == dump ]]; then for c in mariadb-dump mysqldump; do command -v "$c" >/dev/null 2>&1 && { echo "$c"; return 0; }; done
    else for c in mariadb mysql; do command -v "$c" >/dev/null 2>&1 && { echo "$c"; return 0; }; done; fi
    return 1
}

# Encryption: AES-256-CBC with PBKDF2 via openssl. This provides confidentiality but NOT tamper detection by itself;
# the SHA-256 in MANIFEST.sha256 detects accidental damage, and an attacker who can change both could change both.
# Keep the manifest (or just its digest) somewhere the backup store cannot write to if tampering matters.
bk_encrypt() {   # bk_encrypt PASSFILE IN OUT
    openssl enc -aes-256-cbc -pbkdf2 -iter 600000 -salt -pass "file:$1" -in "$2" -out "$3"
}
bk_decrypt() {   # bk_decrypt PASSFILE IN OUT
    openssl enc -d -aes-256-cbc -pbkdf2 -iter 600000 -pass "file:$1" -in "$2" -out "$3"
}

bk_check_passfile() {
    [[ -f "$1" && -r "$1" ]] || bk_die "passphrase file not readable: $1"
    [[ -s "$1" ]] || bk_die "passphrase file is empty: $1"
    local mode; mode="$(stat -c %a "$1" 2>/dev/null || stat -f %Lp "$1")"
    [[ "$mode" =~ ^[0-7]00$ ]] || bk_die "passphrase file must not be accessible by group/others (chmod 600 $1), mode is $mode"
    local n; n="$(wc -c < "$1" | tr -d ' ')"
    [[ "$n" -ge 16 ]] || bk_die "passphrase is shorter than 16 characters"
}

BK_TMP_FILES=()
bk_cleanup() { local f; for f in ${BK_TMP_FILES[@]+"${BK_TMP_FILES[@]}"} "${BK_DB_CNF:-}"; do [[ -n "$f" && -e "$f" ]] && rm -rf "$f"; done; return 0; }
