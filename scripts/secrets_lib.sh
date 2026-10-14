#!/usr/bin/env bash
# ==============================================================================
# FlowFirst - shared secret checks for the install scripts. Sourced, not run.
#
#   source "${ROOT_DIR}/scripts/secrets_lib.sh"
#   require_strong_secret RABBITMQ_PASSWORD "${RABBITMQ_PASSWORD:-}"
#
# Mirrors config.weak_secret_reason() in src/config/config.py (same list, same minimum length), so the install scripts
# and the Python processes agree on what counts as weak. Keep the two lists in step.
# Prints the variable NAME and the reason, never the value. ALLOW_DEFAULT_SECRETS=true downgrades the error to a
# warning (throw-away dev setups only).
# ==============================================================================

SECRET_MIN_LENGTH="${SECRET_MIN_LENGTH:-12}"
[[ "${SECRET_MIN_LENGTH}" =~ ^[0-9]+$ && "${SECRET_MIN_LENGTH}" -ge 8 ]] || SECRET_MIN_LENGTH=12

weak_secret_reason() {   # prints a reason, or nothing when the value is acceptable
    local v="$1" lower
    lower="$(printf '%s' "${v}" | tr '[:upper:]' '[:lower:]')"
    if [[ -z "${v//[[:space:]]/}" ]]; then echo "is empty"; return 0; fi
    case "${lower}" in
        flowpassword|testing|admin123|admin|guest|password|passw0rd|secret|root|test|changeme*|change-me|changeit|12345678|123456789012)
            echo "is a well-known sample/default value"; return 0 ;;
    esac
    if [[ ${#v} -lt ${SECRET_MIN_LENGTH} ]]; then echo "is shorter than ${SECRET_MIN_LENGTH} characters"; return 0; fi
    return 0
}

# require_strong_secret NAME VALUE  -> exits the calling script (status 1) unless the value is acceptable
require_strong_secret() {
    local name="$1" why
    why="$(weak_secret_reason "${2:-}")"
    [[ -z "${why}" ]] && return 0
    case "$(printf '%s' "${ALLOW_DEFAULT_SECRETS:-false}" | tr '[:upper:]' '[:lower:]')" in
        1|true|yes|on) echo "WARNING: ${name} ${why} (ALLOW_DEFAULT_SECRETS=true - never use this in production)." >&2; return 0 ;;
    esac
    echo "ERROR: ${name} ${why}. Set a strong value in .env (generate one with ./scripts/generate_secrets.sh)," >&2
    echo "       or set ALLOW_DEFAULT_SECRETS=true for a local dev setup only." >&2
    exit 1
}
