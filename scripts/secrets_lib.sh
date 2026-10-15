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

# Short HMACs (12 hex chars) of well-known weak/sample values — matches config.py logic.
# Internal salt: not a secret, just part of the algorithm.
_WS="flowfirst-weak-check-v1"
_WEAK_HMACS=(
    "1e06b40fd2c5" "277a8508e759" "286e77bfb1b4" "2c4a5054eef6"
    "305b118aae95" "32e88888418a" "36a5e33f7ed5" "3b7602385358"
    "45adcbf86d4f" "73ccf6684d50" "7aae4a5208f4" "a026c82f92ce"
    "b2e495c1610b" "c6460fb7f6b5" "da4cbc6162f1"
)
# HMAC-SHA256 of "changeme" with the same salt, 12 chars — catches changeme-* prefix variants
_CHANGEME_HMAC12="b2e495c1610b"

_hmac12() {   # _hmac12 <salt> <value> → first 12 hex chars of HMAC-SHA256
    printf '%s' "$2" | openssl dgst -sha256 -hmac "$1" 2>/dev/null | awk '{print substr($NF,1,12)}'
}

weak_secret_reason() {   # prints a reason, or nothing when the value is acceptable
    local v="$1" lower tag tag8 entry
    lower="$(printf '%s' "${v}" | tr '[:upper:]' '[:lower:]')"
    if [[ -z "${v//[[:space:]]/}" ]]; then echo "is empty"; return 0; fi
    tag="$(_hmac12 "${_WS}" "${lower}")"
    tag8="$(_hmac12 "${_WS}" "${lower:0:8}")"
    for entry in "${_WEAK_HMACS[@]}"; do
        [[ "${tag}" == "${entry}" ]] && { echo "is a well-known sample/default value"; return 0; }
    done
    [[ "${tag8}" == "${_CHANGEME_HMAC12}" ]] && { echo "is a well-known sample/default value"; return 0; }
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
