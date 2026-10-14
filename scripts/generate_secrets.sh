#!/usr/bin/env bash
set -euo pipefail

# ==============================================================================
# FlowFirst - generate strong random secrets for .env.
#
#   ./scripts/generate_secrets.sh              print KEY=value lines (nothing is written; copy them into .env)
#   ./scripts/generate_secrets.sh --write      replace the values IN .env (backup saved as .env.bak, mode 600)
#   ./scripts/generate_secrets.sh --write --file /path/.env
#
# Generates: RABBITMQ_PASSWORD, MARIADB_PASSWORD, SWIFT_KEY, HAPROXY_STATS_PASS, and the two SWIFT_HASH_PATH_* values.
# Values are 32 hex characters from `openssl rand` (letters and digits only, so they are safe in .env, in SQL and in
# haproxy.cfg without any quoting).
#
# DO THIS ONCE, BEFORE the first install, and put the SAME .env values on every node (only NODE_NAME and
# CURRENT_NODE_IP differ). Changing a password later means changing it in RabbitMQ / MariaDB / Swift too:
#   - SWIFT_HASH_PATH_PREFIX / SUFFIX must NEVER change after data is stored (the install script refuses placeholders).
#   - Keep the file out of git (.env is in .gitignore) and mode 600.
# ==============================================================================

WRITE=0; ENV_FILE=""
while [[ $# -gt 0 ]]; do
    case "$1" in
        --write) WRITE=1; shift ;;
        --file)  ENV_FILE="${2:-}"; shift 2 ;;
        *) echo "usage: $0 [--write] [--file PATH]" >&2; exit 2 ;;
    esac
done
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="${ENV_FILE:-${SCRIPT_DIR}/../.env}"

command -v openssl >/dev/null 2>&1 || { echo "ERROR: openssl not found." >&2; exit 1; }
gen() { openssl rand -hex 16; }

KEYS=(RABBITMQ_PASSWORD MARIADB_PASSWORD SWIFT_KEY HAPROXY_STATS_PASS SWIFT_HASH_PATH_PREFIX SWIFT_HASH_PATH_SUFFIX)
VALUES=()
for k in "${KEYS[@]}"; do VALUES+=("$(gen)"); done

if [[ ${WRITE} -eq 0 ]]; then
    for i in "${!KEYS[@]}"; do echo "${KEYS[$i]}=${VALUES[$i]}"; done
    echo "# (not written - run with --write to update ${ENV_FILE})" >&2
    exit 0
fi

[[ -f "${ENV_FILE}" ]] || { echo "ERROR: ${ENV_FILE} not found (copy src/config/.env.example first)." >&2; exit 1; }
cp -p "${ENV_FILE}" "${ENV_FILE}.bak"
chmod 600 "${ENV_FILE}" "${ENV_FILE}.bak"
for i in "${!KEYS[@]}"; do
    k="${KEYS[$i]}"; v="${VALUES[$i]}"
    if grep -q "^${k}=" "${ENV_FILE}"; then
        # hex values contain no sed metacharacters, so a plain substitution is safe
        sed "s|^${k}=.*|${k}=${v}|" "${ENV_FILE}" > "${ENV_FILE}.new" && mv "${ENV_FILE}.new" "${ENV_FILE}"
    else
        echo "${k}=${v}" >> "${ENV_FILE}"
    fi
done
chmod 600 "${ENV_FILE}"
echo "Updated ${#KEYS[@]} secrets in ${ENV_FILE} (previous file: ${ENV_FILE}.bak - delete it once you have copied the .env to the other nodes)."
