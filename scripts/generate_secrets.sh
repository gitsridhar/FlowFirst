#!/usr/bin/env bash
set -euo pipefail

# ==============================================================================
# FlowFirst - generate strong random secrets for .env.
#
#   ./scripts/generate_secrets.sh              print KEY=value lines (nothing is written; copy them into .env)
#   ./scripts/generate_secrets.sh --write      replace the values IN .env (backup saved as .env.bak, mode 600)
#   ./scripts/generate_secrets.sh --write --file /path/.env
#
# Generates fresh values for: RABBITMQ_PASSWORD, MARIADB_PASSWORD, SWIFT_KEY, HAPROXY_STATS_PASS.
# Values are 32 hex characters from `openssl rand` (letters and digits only, safe in .env, SQL and haproxy.cfg).
#
# SWIFT_HASH_PATH_PREFIX and SWIFT_HASH_PATH_SUFFIX are pre-set in the per-node .env files
# (src/config/.env.node*) and are SKIPPED by this script when they already hold a non-placeholder
# value.  They must NEVER change after Swift data has been stored: changing them makes every
# existing object unreachable.  Only overwrite them if the current value still starts with "changeme".
#
# DO THIS ONCE, BEFORE the first install.  Copy the resulting .env to every node — only NODE_NAME
# and CURRENT_NODE_IP differ between nodes.  Changing a password later means changing it in
# RabbitMQ / MariaDB / Swift too.  Keep the file out of git and mode 600.
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

# Passwords — always regenerated.
KEYS=(RABBITMQ_PASSWORD MARIADB_PASSWORD SWIFT_KEY HAPROXY_STATS_PASS)
VALUES=()
for k in "${KEYS[@]}"; do VALUES+=("$(gen)"); done

if [[ ${WRITE} -eq 0 ]]; then
    for i in "${!KEYS[@]}"; do echo "${KEYS[$i]}=${VALUES[$i]}"; done
    # Show current hash path values (informational only — not regenerated)
    if [[ -f "${ENV_FILE}" ]]; then
        for hk in SWIFT_HASH_PATH_PREFIX SWIFT_HASH_PATH_SUFFIX; do
            cur="$(grep "^${hk}=" "${ENV_FILE}" 2>/dev/null | cut -d= -f2- || true)"
            echo "# ${hk}=${cur}  (already set — not regenerated)"
        done
    fi
    echo "# (not written - run with --write to update ${ENV_FILE})" >&2
    exit 0
fi

[[ -f "${ENV_FILE}" ]] || { echo "ERROR: ${ENV_FILE} not found (copy src/config/.env.node1 or .env.example first)." >&2; exit 1; }
cp -p "${ENV_FILE}" "${ENV_FILE}.bak"
chmod 600 "${ENV_FILE}" "${ENV_FILE}.bak"

skipped=0
for i in "${!KEYS[@]}"; do
    k="${KEYS[$i]}"; v="${VALUES[$i]}"
    if grep -q "^${k}=" "${ENV_FILE}"; then
        # hex values contain no sed metacharacters, so a plain substitution is safe
        sed "s|^${k}=.*|${k}=${v}|" "${ENV_FILE}" > "${ENV_FILE}.new" && mv "${ENV_FILE}.new" "${ENV_FILE}"
    else
        echo "${k}=${v}" >> "${ENV_FILE}"
    fi
done

# SWIFT_HASH_PATH_* — only overwrite if still a placeholder (starts with "changeme").
# Pre-built .env.node* files already have real values; leave them alone.
for hk in SWIFT_HASH_PATH_PREFIX SWIFT_HASH_PATH_SUFFIX; do
    cur="$(grep "^${hk}=" "${ENV_FILE}" 2>/dev/null | cut -d= -f2- || true)"
    if [[ "${cur}" == changeme* ]]; then
        sed "s|^${hk}=.*|${hk}=$(gen)|" "${ENV_FILE}" > "${ENV_FILE}.new" && mv "${ENV_FILE}.new" "${ENV_FILE}"
        echo "  ${hk}: placeholder replaced with a new random value"
    else
        echo "  ${hk}: already set — not changed"
        skipped=$((skipped + 1))
    fi
done

chmod 600 "${ENV_FILE}"
echo "Updated ${#KEYS[@]} password(s) in ${ENV_FILE} (${skipped} hash path value(s) skipped — already set)."
echo "Previous file: ${ENV_FILE}.bak — delete it once you have copied .env to the other nodes."
