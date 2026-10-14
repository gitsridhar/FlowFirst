#!/usr/bin/env bash
# ==============================================================================
# FlowFirst - verify and restore a backup made by backup_flowfirst.sh.
#
#   ./scripts/restore_flowfirst.sh --from DIR --passphrase-file F --verify
#       Checks checksums and that every file decrypts and is readable. Changes nothing. Do this regularly.
#   ./scripts/restore_flowfirst.sh --from DIR --passphrase-file F --extract-config /some/empty/dir
#       Decrypts config.tar.gz.enc into that directory (never over / ), for you to copy from by hand.
#   ./scripts/restore_flowfirst.sh --from DIR --passphrase-file F --db --into-database flowfirst_db_restore
#       Loads the dump into a DIFFERENT database name, to check it or copy rows from it.
#   ./scripts/restore_flowfirst.sh --from DIR --passphrase-file F --db --in-place --yes-overwrite
#       Replaces the live database with the dump. The dump contains DROP/CREATE statements for MARIADB_DB.
#   ./scripts/restore_flowfirst.sh --from DIR --passphrase-file F --zk | --rabbit
#       Writes ZooKeeper runtime config back / prints how to import the RabbitMQ definitions.
#
# Nothing here is applied unless you name it, and the one destructive action needs --yes-overwrite as well.
# ==============================================================================
set -euo pipefail
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/backup_lib.sh"

FROM=""; PASSFILE=""; VERIFY=0; EXTRACT=""; DB=0; INTO=""; INPLACE=0; YES=0; ZK=0; RABBIT=0
while [[ $# -gt 0 ]]; do
    case "$1" in
        --from) FROM="${2:-}"; shift 2 ;;
        --passphrase-file) PASSFILE="${2:-}"; shift 2 ;;
        --verify) VERIFY=1; shift ;;
        --extract-config) EXTRACT="${2:-}"; shift 2 ;;
        --db) DB=1; shift ;;
        --into-database) INTO="${2:-}"; shift 2 ;;
        --in-place) INPLACE=1; shift ;;
        --yes-overwrite) YES=1; shift ;;
        --zk) ZK=1; shift ;;
        --rabbit) RABBIT=1; shift ;;
        -h|--help) sed -n 2,19p "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'; exit 0 ;;
        *) echo "unknown option: $1" >&2; exit 2 ;;
    esac
done
[[ -d "$FROM" ]] || bk_die "--from must be a backup directory"
[[ -n "$PASSFILE" ]] || bk_die "--passphrase-file is required"
trap bk_cleanup EXIT
bk_check_passfile "$PASSFILE"

verify() {
    local bad=0 f want got
    [[ -s "$FROM/MANIFEST.sha256" ]] || bk_die "no MANIFEST.sha256 in $FROM: not a complete backup"
    grep -q '^parts_failed=none' "$FROM/BACKUP.info" 2>/dev/null || { bk_warn "BACKUP.info says some parts failed:"; grep '^parts_' "$FROM/BACKUP.info" >&2; bad=1; }
    while read -r want f; do
        [[ -f "$FROM/$f" ]] || { bk_warn "missing $f"; bad=1; continue; }
        got="$(bk_sha256 "$FROM/$f")"
        [[ "$got" == "$want" ]] || { bk_warn "CHECKSUM MISMATCH $f"; bad=1; continue; }
        if ! bk_decrypt "$PASSFILE" "$FROM/$f" /dev/stdout 2>/dev/null | { case "$f" in *.gz.enc) gzip -t ;; *) cat >/dev/null ;; esac; }; then
            bk_warn "cannot decrypt/read $f (wrong passphrase or damaged)"; bad=1; continue
        fi
        bk_log "ok  $f"
    done < "$FROM/MANIFEST.sha256"
    return $bad
}

if [[ $VERIFY -eq 1 ]]; then verify && bk_log "VERIFIED $FROM" || bk_die "verification FAILED for $FROM"; fi

if [[ -n "$EXTRACT" ]]; then
    [[ "$EXTRACT" != "/" && "$EXTRACT" != "" ]] || bk_die "refusing to extract into /"
    [[ -d "$EXTRACT" && -z "$(ls -A "$EXTRACT")" ]] || bk_die "$EXTRACT must be an existing EMPTY directory"
    bk_decrypt "$PASSFILE" "$FROM/config.tar.gz.enc" /dev/stdout | tar -xzf - -C "$EXTRACT"
    bk_log "config extracted to $EXTRACT (paths are kept relative; contains secrets and private keys: protect it)"
fi

if [[ $DB -eq 1 ]]; then
    [[ -n "$INTO" || $INPLACE -eq 1 ]] || bk_die "--db needs --into-database NAME or --in-place"
    [[ -z "$INTO" || $INPLACE -eq 0 ]] || bk_die "use either --into-database or --in-place"
    [[ $INPLACE -eq 0 || $YES -eq 1 ]] || bk_die "--in-place replaces the live database; add --yes-overwrite if you mean it"
    bk_load_env
    CLIENT="$(bk_db_tool client)" || bk_die "mariadb / mysql client not installed"
    bk_make_db_cnf
    if [[ -n "$INTO" ]]; then
        [[ "$INTO" =~ ^[A-Za-z0-9_]+$ ]] || bk_die "database name may contain letters, digits and _ only"
        [[ "$INTO" != "${MARIADB_DB}" ]] || bk_die "$INTO is the live database; use --in-place"
        bk_log "loading dump into new database $INTO"
        "$CLIENT" --defaults-extra-file="$BK_DB_CNF" -e "CREATE DATABASE \`$INTO\` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
        # the dump names MARIADB_DB in CREATE DATABASE / USE: rewrite those two statements to the new name
        bk_decrypt "$PASSFILE" "$FROM/db.sql.gz.enc" /dev/stdout | gzip -dc \
            | sed -e "s/^CREATE DATABASE \/\*!32312 IF NOT EXISTS\*\/ \`${MARIADB_DB}\`/CREATE DATABASE IF NOT EXISTS \`$INTO\`/" \
                  -e "s/^USE \`${MARIADB_DB}\`;/USE \`$INTO\`;/" \
            | "$CLIENT" --defaults-extra-file="$BK_DB_CNF"
        bk_log "loaded. Row count: $("$CLIENT" --defaults-extra-file="$BK_DB_CNF" -N -e "SELECT COUNT(*) FROM \`$INTO\`.processed_messages")"
    else
        bk_warn "REPLACING database ${MARIADB_DB} from $FROM. Stop the pipeline writers (Process 4) first."
        bk_decrypt "$PASSFILE" "$FROM/db.sql.gz.enc" /dev/stdout | gzip -dc | "$CLIENT" --defaults-extra-file="$BK_DB_CNF"
        bk_log "restored. Row count: $("$CLIENT" --defaults-extra-file="$BK_DB_CNF" -N -e "SELECT COUNT(*) FROM \`${MARIADB_DB}\`.processed_messages")"
    fi
fi

if [[ $ZK -eq 1 ]]; then
    bk_load_env
    PY="${PYTHON:-}"; [[ -z "$PY" && -x "$BK_ROOT_DIR/.venv/bin/python" ]] && PY="$BK_ROOT_DIR/.venv/bin/python"; PY="${PY:-python3}"
    bk_decrypt "$PASSFILE" "$FROM/zk-config.json.enc" /dev/stdout | (cd "$BK_ROOT_DIR" && "$PY" -W ignore scripts/zk_config_backup.py restore)
fi

if [[ $RABBIT -eq 1 ]]; then
    T="$(mktemp -d "${TMPDIR:-/tmp}/flowfirst-rmq.XXXXXX")"; chmod 700 "$T"; BK_TMP_FILES+=("$T")
    bk_decrypt "$PASSFILE" "$FROM/rabbit-definitions.json.enc" "$T/defs.json"
    chmod 644 "$T/defs.json"; chmod 755 "$T"
    cat <<MSG
Decrypted RabbitMQ definitions to $T/defs.json (contains password hashes; it is deleted when this script exits, so
run the import from ANOTHER terminal while this one waits, or copy the file first):
    sudo rabbitmqctl import_definitions $T/defs.json
Definitions are merged: existing users/queues with the same name are overwritten, others are kept.
Press Enter when done.
MSG
    read -r _ || true
fi
[[ $VERIFY -eq 0 && -z "$EXTRACT" && $DB -eq 0 && $ZK -eq 0 && $RABBIT -eq 0 ]] && bk_die "nothing to do: give --verify, --extract-config, --db, --zk or --rabbit"
exit 0
