#!/usr/bin/env bash
# ==============================================================================
# FlowFirst - encrypted backup of the state that cannot be rebuilt.
#
#   ./scripts/backup_flowfirst.sh --out /backup/flowfirst --passphrase-file /root/.flowfirst-backup.pass
#   ./scripts/backup_flowfirst.sh --out DIR --passphrase-file F --only db,config --keep 14
#
# WHAT IS BACKED UP (each part is optional with --only; default = all that this node can do)
#   db       MariaDB database MARIADB_DB, consistent snapshot (--single-transaction, InnoDB), incl. raw_payload
#   config   .env, galera.cnf, swift.conf + rings, proxy/account/container/object configs, haproxy.cfg, zoo.cfg + myid,
#            rabbitmq.conf, the TLS directory INCLUDING PRIVATE KEYS  (this is why every backup is encrypted)
#   zk       ZooKeeper /flowfirst/config values (runtime tuning). Election, registry, health and dedup are rebuilt.
#   rabbit   RabbitMQ definitions: users (password HASHES), vhosts, permissions, policies (incl. flowfirst-dlx), queues
#   swift    All objects in SWIFT_CONTAINER downloaded to a local directory, tarballed and encrypted. Requires the
#            'swift' CLI (python-swiftclient, already in requirements.txt) and SWIFT_ENABLED=true in .env.
#            Restore: --swift in restore_flowfirst.sh.
# WHAT IS NOT BACKED UP, ON PURPOSE
#   * messages waiting in RabbitMQ queues (transient pipeline state; see ReadMe, Backup chapter, for the consequence)
#
# OUTPUT: <out>/flowfirst-<host>-<UTC>/{db.sql.gz.enc, config.tar.gz.enc, zk-config.json.enc, rabbit-definitions.json.enc,
#         MANIFEST.sha256, BACKUP.info}. AES-256-CBC + PBKDF2 (600k) with the passphrase file; nothing sensitive is
#         written unencrypted (dumps are streamed through openssl). MANIFEST.sha256 detects damage; it does not stop
#         someone who can write the backup store from replacing files AND the manifest.
# The passphrase is the only way back in. Store it away from the backups; losing it loses the backups.
#
# Run on ONE node per cluster for db/zk/rabbit, and on EVERY node for config (each node has its own myid, IP and .env
# differences). Run as root to read /etc/swift, /etc/flowfirst/tls and to call rabbitmqctl.
# Exit code 0 only if every selected part was written and re-read successfully.
# ==============================================================================
set -euo pipefail
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/backup_lib.sh"

OUT=""; PASSFILE=""; ONLY="db,config,zk,rabbit,swift"; KEEP=0; NO_ENCRYPT=0
usage() { sed -n 2,6p "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'; echo "options: --out DIR --passphrase-file FILE [--only db,config,zk,rabbit] [--keep N]"; exit 2; }
while [[ $# -gt 0 ]]; do
    case "$1" in
        --out)             OUT="${2:-}"; shift 2 ;;
        --passphrase-file) PASSFILE="${2:-}"; shift 2 ;;
        --only)            ONLY="${2:-}"; shift 2 ;;
        --keep)            KEEP="${2:-}"; shift 2 ;;
        -h|--help)         usage ;;
        *) echo "unknown option: $1" >&2; usage ;;
    esac
done
[[ -n "$OUT" ]]      || { echo "--out is required" >&2; usage; }
[[ -n "$PASSFILE" ]] || { echo "--passphrase-file is required (backups contain secrets and are always encrypted)" >&2; usage; }
[[ "$KEEP" =~ ^[0-9]+$ ]] || bk_die "--keep must be a number"
for p in ${ONLY//,/ }; do [[ "$p" =~ ^(db|config|zk|rabbit|swift)$ ]] || bk_die "unknown part '$p' in --only"; done
want() { [[ ",$ONLY," == *",$1,"* ]]; }

trap bk_cleanup EXIT
bk_check_passfile "$PASSFILE"
bk_load_env
command -v openssl >/dev/null || bk_die "openssl not found"

HOST="$(hostname -s 2>/dev/null || hostname)"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
DEST="${OUT%/}/flowfirst-${HOST}-${STAMP}"
old_umask="$(umask)"; umask 077
mkdir -p "$DEST" || bk_die "cannot create $DEST"
umask "$old_umask"
chmod 700 "$DEST"
bk_log "writing $DEST"
FAILED=()
DONE=()

# ---- db ---------------------------------------------------------------------------------------------------------
if want db; then
    DUMP="$(bk_db_tool dump)" || { bk_warn "mariadb-dump / mysqldump not installed on this node: db skipped"; FAILED+=(db); DUMP=""; }
    if [[ -n "$DUMP" ]]; then
        bk_make_db_cnf
        bk_log "db: dumping ${MARIADB_DB:-?} with $DUMP (single transaction)"
        # pipefail makes a failed dump fail the whole pipeline, so a truncated dump is never reported as good.
        if "$DUMP" --defaults-extra-file="$BK_DB_CNF" --single-transaction --routines --triggers --events \
                   --hex-blob --default-character-set=utf8mb4 --databases "${MARIADB_DB}" \
                | gzip -9 | bk_encrypt "$PASSFILE" /dev/stdin "$DEST/db.sql.gz.enc"; then
            DONE+=(db.sql.gz.enc)
        else
            bk_warn "db: dump failed"; rm -f "$DEST/db.sql.gz.enc"; FAILED+=(db)
        fi
    fi
fi

# ---- config -----------------------------------------------------------------------------------------------------
if want config; then
    CANDIDATES=("$BK_ENV_FILE" /etc/my.cnf.d/galera.cnf /etc/swift /etc/haproxy/haproxy.cfg /etc/rabbitmq/rabbitmq.conf
                "${ZK_INSTALL_DIR:-/opt/zookeeper}/conf/zoo.cfg" "${ZK_DATA_DIR:-/var/lib/zookeeper/data}/myid"
                "${TLS_DIR:-/etc/flowfirst/tls}" /etc/systemd/system/flowfirst-process*.service /etc/systemd/system/flowfirst.target)
    PRESENT=()
    for f in "${CANDIDATES[@]}"; do [[ -e "$f" ]] && PRESENT+=("$f"); done
    if [[ ${#PRESENT[@]} -eq 0 ]]; then
        bk_warn "config: nothing found to back up"; FAILED+=(config)
    else
        bk_log "config: ${#PRESENT[@]} path(s)"
        for f in "${PRESENT[@]}"; do [[ -r "$f" ]] || bk_warn "config: $f is not readable by this user (run as root); the archive may be incomplete"; done
        if tar -czf - "${PRESENT[@]}" 2>"$DEST/.tar.err" | bk_encrypt "$PASSFILE" /dev/stdin "$DEST/config.tar.gz.enc"; then
            if [[ -s "$DEST/.tar.err" ]] && grep -q 'Permission denied\|Cannot open' "$DEST/.tar.err"; then
                bk_warn "config: some files were NOT archived:"; head -5 "$DEST/.tar.err" >&2; FAILED+=(config)
            else DONE+=(config.tar.gz.enc); fi
        else bk_warn "config: archive failed"; rm -f "$DEST/config.tar.gz.enc"; FAILED+=(config); fi
        rm -f "$DEST/.tar.err"
    fi
fi

# ---- zk ---------------------------------------------------------------------------------------------------------
if want zk; then
    PY="${PYTHON:-}"; [[ -z "$PY" && -x "$BK_ROOT_DIR/.venv/bin/python" ]] && PY="$BK_ROOT_DIR/.venv/bin/python"; PY="${PY:-python3}"
    bk_log "zk: reading /flowfirst/config"
    if (cd "$BK_ROOT_DIR" && "$PY" -W ignore scripts/zk_config_backup.py dump) 2>"$DEST/.zk.err" \
          | bk_encrypt "$PASSFILE" /dev/stdin "$DEST/zk-config.json.enc"; then
        DONE+=(zk-config.json.enc)
    else bk_warn "zk: failed: $(tail -1 "$DEST/.zk.err" 2>/dev/null | cut -c1-150)"; rm -f "$DEST/zk-config.json.enc"; FAILED+=(zk); fi
    rm -f "$DEST/.zk.err"
fi

# ---- swift ------------------------------------------------------------------------------------------------------
if want swift; then
    # Check whether Swift is enabled in .env
    if [[ "${SWIFT_ENABLED:-true}" != "true" && "${SWIFT_ENABLED:-true}" != "1" ]]; then
        bk_log "swift: SWIFT_ENABLED is not true, skipping"
    else
        SWIFT_CMD=""
        for _c in "$BK_ROOT_DIR/.venv/bin/swift" "$(command -v swift 2>/dev/null)"; do
            [[ -x "$_c" ]] && { SWIFT_CMD="$_c"; break; }
        done
        if [[ -z "$SWIFT_CMD" ]]; then
            bk_warn "swift: 'swift' CLI not found (install python-swiftclient or activate the virtualenv); swift skipped"
            FAILED+=(swift)
        else
            # Build the auth env so we do not pass the key on the command line
            _ST_AUTH="${SWIFT_AUTH_URL:-}"
            _ST_USER="${SWIFT_USER:-}"
            _ST_KEY="${SWIFT_KEY:-}"
            _ST_CONTAINER="${SWIFT_CONTAINER:-flowfirst_messages}"
            [[ -n "$_ST_AUTH" && -n "$_ST_USER" && -n "$_ST_KEY" ]] || { bk_warn "swift: SWIFT_AUTH_URL / SWIFT_USER / SWIFT_KEY not set; skipped"; FAILED+=(swift); }
            if want swift && [[ ${FAILED[-1]:-} != swift ]]; then
                # TLS options if enabled
                _SWIFT_OPTS=()
                if [[ "${TLS_SWIFT_ENABLED:-false}" == "true" ]]; then
                    _TLS_D="${TLS_DIR:-/etc/flowfirst/tls}"
                    _SWIFT_OPTS+=(--os-cacert "${TLS_CA_FILE:-$_TLS_D/ca.crt}"
                                  --os-cert   "${TLS_CLIENT_CERT:-$_TLS_D/pipeline/pipeline.crt}"
                                  --os-key    "${TLS_CLIENT_KEY:-$_TLS_D/pipeline/pipeline.key}")
                fi
                bk_log "swift: downloading all objects from container '$_ST_CONTAINER' ..."
                SWIFT_DIR="$(mktemp -d "${TMPDIR:-/tmp}/flowfirst-swift.XXXXXX")"; BK_TMP_FILES+=("$SWIFT_DIR")
                # swift download exits 0 even when the container is empty; non-zero on auth / network failure
                if ST_AUTH="$_ST_AUTH" ST_USER="$_ST_USER" ST_KEY="$_ST_KEY" \
                   "$SWIFT_CMD" "${_SWIFT_OPTS[@]}" download "$_ST_CONTAINER" --output-dir "$SWIFT_DIR" 2>"$DEST/.swift.err"; then
                    OBJ_COUNT="$(find "$SWIFT_DIR" -type f | wc -l | tr -d ' ')"
                    bk_log "swift: $OBJ_COUNT object(s) downloaded; archiving ..."
                    if tar -czf - -C "$SWIFT_DIR" . 2>>"$DEST/.swift.err" | bk_encrypt "$PASSFILE" /dev/stdin "$DEST/swift-objects.tar.gz.enc"; then
                        DONE+=(swift-objects.tar.gz.enc)
                    else
                        bk_warn "swift: archive/encrypt failed"; rm -f "$DEST/swift-objects.tar.gz.enc"; FAILED+=(swift)
                        [[ -s "$DEST/.swift.err" ]] && head -5 "$DEST/.swift.err" >&2
                    fi
                else
                    bk_warn "swift: download failed:"; head -5 "$DEST/.swift.err" >&2; FAILED+=(swift)
                fi
                rm -f "$DEST/.swift.err"
            fi
        fi
    fi
fi

# ---- rabbit -----------------------------------------------------------------------------------------------------
if want rabbit; then
    if ! command -v rabbitmqctl >/dev/null 2>&1; then bk_warn "rabbit: rabbitmqctl not on this node (use a RabbitMQ node): skipped"; FAILED+=(rabbit)
    else
        RJ="$(mktemp "${TMPDIR:-/tmp}/flowfirst-rmq.XXXXXX")"; chmod 600 "$RJ"; BK_TMP_FILES+=("$RJ")
        # rabbitmqctl runs as the rabbitmq user, which must be able to write the file: world-writable is not acceptable
        # for password hashes, so write to a rabbitmq-owned directory when we are root.
        if [[ $EUID -eq 0 ]] && id rabbitmq >/dev/null 2>&1; then
            RD="$(mktemp -d /var/lib/rabbitmq/flowfirst-export.XXXXXX)"; chown rabbitmq "$RD"; chmod 700 "$RD"; BK_TMP_FILES+=("$RD")
            RJ="$RD/defs.json"
        fi
        bk_log "rabbit: exporting definitions"
        if rabbitmqctl export_definitions "$RJ" >/dev/null 2>&1 && [[ -s "$RJ" ]] && bk_encrypt "$PASSFILE" "$RJ" "$DEST/rabbit-definitions.json.enc"; then
            DONE+=(rabbit-definitions.json.enc)
        else bk_warn "rabbit: export failed (needs root on a RabbitMQ node, RabbitMQ 3.8+)"; rm -f "$DEST/rabbit-definitions.json.enc"; FAILED+=(rabbit); fi
        rm -rf "$RJ" 2>/dev/null || true
    fi
fi

# ---- verify what was written ------------------------------------------------------------------------------------
: > "$DEST/MANIFEST.sha256"
for f in ${DONE[@]+"${DONE[@]}"}; do
    [[ -s "$DEST/$f" ]] || { bk_warn "$f is empty"; FAILED+=("$f"); continue; }
    # re-read: decrypt to /dev/null, so a bad passphrase or damaged file is found now, not during a disaster
    if ! bk_decrypt "$PASSFILE" "$DEST/$f" /dev/stdout 2>/dev/null | { case "$f" in *.gz.enc) gzip -t ;; *) cat >/dev/null ;; esac; }; then
        bk_warn "$f cannot be decrypted/read back"; FAILED+=("$f"); continue
    fi
    printf '%s  %s\n' "$(bk_sha256 "$DEST/$f")" "$f" >> "$DEST/MANIFEST.sha256"
done
{
    echo "created_utc=$STAMP"; echo "host=$HOST"; echo "parts_requested=$ONLY"
    echo "parts_written=${DONE[*]:-none}"; echo "parts_failed=${FAILED[*]:-none}"
    echo "database=${MARIADB_DB:-}"; echo "format=1"
} > "$DEST/BACKUP.info"
chmod 600 "$DEST"/* 2>/dev/null || true

# ---- retention (only this host's complete backups, newest N kept) -----------------------------------------------
if [[ "$KEEP" -gt 0 && ${#FAILED[@]} -eq 0 ]]; then
    # only directories that are real backups (have BACKUP.info) are counted and ever deleted
    mapfile -t OLD < <(for d in "${OUT%/}"/flowfirst-"${HOST}"-*; do [[ -f "$d/BACKUP.info" ]] && echo "$d"; done | sort -r | tail -n +"$((KEEP + 1))")
    for d in ${OLD[@]+"${OLD[@]}"}; do bk_log "retention: removing $d"; rm -rf "$d"; done
fi

if [[ ${#FAILED[@]} -gt 0 ]]; then
    bk_warn "INCOMPLETE backup in $DEST. Failed: ${FAILED[*]}"; exit 1
fi
bk_log "OK: ${DONE[*]}  ->  $DEST"
