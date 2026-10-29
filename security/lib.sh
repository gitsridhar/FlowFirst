#!/usr/bin/env bash
# ==============================================================================
# FlowFirst PKI - shared helpers. Sourced by the other scripts; not run directly.
# Works with bash 3.2+ and OpenSSL 1.1.1+ / 3.x (RHEL 9 ships OpenSSL 3.x).
# ==============================================================================

SEC_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SEC_DIR}/.." && pwd)"

# Load .env (plain KEY=VALUE literals only - see AGENTS.md). Variables already in the environment win.
_env_file="${FLOWFIRST_ENV_FILE:-${ROOT_DIR}/.env}"
if [[ -f "${_env_file}" ]]; then
    set -a
    # shellcheck disable=SC1090
    source "${_env_file}"
    set +a
fi

# Where the PKI lives. Keep it on the CA/admin host; only per-node files are copied to nodes.
PKI_DIR="${PKI_DIR:-${ROOT_DIR}/pki}"
CA_DIR="${PKI_DIR}/ca"
ISSUED_DIR="${PKI_DIR}/issued"
PKI_ORG="${PKI_ORG:-FlowFirst}"
PKI_CA_CN="${PKI_CA_CN:-FlowFirst Internal Root CA}"
PKI_CA_BITS="${PKI_CA_BITS:-4096}"
PKI_LEAF_BITS="${PKI_LEAF_BITS:-2048}"
PKI_CA_DAYS="${PKI_CA_DAYS:-3650}"
PKI_LEAF_DAYS="${PKI_LEAF_DAYS:-365}"
PKI_CRL_DAYS="${PKI_CRL_DAYS:-30}"
PKI_BACKDATE_MIN="${PKI_BACKDATE_MIN:-5}"      # notBefore = now - N minutes, tolerates small clock skew

die()  { echo "ERROR: $*" >&2; exit 1; }
info() { echo "  $*"; }

require_openssl() {
    command -v openssl >/dev/null 2>&1 || die "openssl not found (sudo dnf install -y openssl)"
    local v; v="$(openssl version | awk '{print $2}')"
    case "${v}" in
        3.*|1.1.1*) ;;
        *) die "OpenSSL ${v} is too old; 1.1.1 or newer is required." ;;
    esac
}

require_int() {   # require_int NAME VALUE
    [[ "$2" =~ ^[1-9][0-9]*$ ]] || die "$1 must be a positive integer (got '$2')."
}

# Names become directory names and part of file paths; CNs become part of the certificate subject.
validate_name() { [[ "$1" =~ ^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$ ]] || die "Invalid name '$1' (allowed: letters, digits, '.', '_', '-'; max 64)."; }
validate_cn()   { [[ "$1" =~ ^[A-Za-z0-9][A-Za-z0-9._@\ -]{0,63}$ ]] || die "Invalid common name '$1' (allowed: letters, digits, space, '.', '_', '-', '@'; max 64)."; }

ca_present() { [[ -f "${CA_DIR}/ca.crt" && -f "${CA_DIR}/ca.key" && -f "${CA_DIR}/ca.cnf" ]]; }
require_ca() { ca_present || die "No CA found in ${CA_DIR}. Run security/init_ca.sh first."; }

# Read the CA key passphrase once per script run into PKI_CA_PASS (never put on a command line).
# Sources, in order: PKI_CA_PASS already exported, PKI_CA_PASSPHRASE_FILE (first line), interactive prompt.
ca_pass_init() {
    if [[ -z "${PKI_CA_PASS:-}" ]]; then
        if [[ -n "${PKI_CA_PASSPHRASE_FILE:-}" ]]; then
            [[ -r "${PKI_CA_PASSPHRASE_FILE}" ]] || die "Cannot read PKI_CA_PASSPHRASE_FILE=${PKI_CA_PASSPHRASE_FILE}"
            PKI_CA_PASS="$(head -n 1 "${PKI_CA_PASSPHRASE_FILE}")"
        else
            [[ -t 0 ]] || die "The CA passphrase is needed. Run from a terminal, or set PKI_CA_PASSPHRASE_FILE to a file (mode 600) holding it."
            read -r -s -p "CA key passphrase: " PKI_CA_PASS
            echo >&2
        fi
    fi
    [[ -n "${PKI_CA_PASS}" ]] || die "Empty CA passphrase."
    export PKI_CA_PASS
}

# Decrypt the CA key into a private temporary directory (RAM-backed /dev/shm when available) for the
# duration of one script run, and remove it on exit. Some OpenSSL 3.x builds cannot read an encrypted key
# directly in 'req'/'ca' with -passin, but 'pkcs8 -passin' always can, so every script uses this path.
CA_KEY_PLAIN=""
_CA_KEY_TMP=""
ca_key_unlock() {
    [[ -n "${CA_KEY_PLAIN}" ]] && return 0
    ca_pass_init
    local base="${TMPDIR:-/tmp}"; [[ -d /dev/shm && -w /dev/shm ]] && base=/dev/shm
    _CA_KEY_TMP="$(umask 077; mktemp -d "${base%/}/flowfirst-ca.XXXXXX")" || die "Cannot create a temporary directory."
    # openssl genrsa -aes256 produces different PEM headers depending on the version:
    #   OpenSSL < 3.x and LibreSSL  →  PKCS#1  "BEGIN RSA PRIVATE KEY"  (openssl rsa can decrypt it)
    #   OpenSSL 3.x (ubuntu-latest) →  PKCS#8  "BEGIN ENCRYPTED PRIVATE KEY"  (openssl pkcs8 can decrypt it)
    # Try the PKCS#8 path first (newer), fall back to the PKCS#1 path (older / LibreSSL).
    if ! openssl pkcs8 -in "${CA_DIR}/ca.key" -passin env:PKI_CA_PASS \
            -out "${_CA_KEY_TMP}/ca.plain.key" 2>/dev/null; then
        if ! openssl rsa -in "${CA_DIR}/ca.key" -passin env:PKI_CA_PASS \
                -out "${_CA_KEY_TMP}/ca.plain.key" 2>/dev/null; then
            ca_key_lock; die "Cannot decrypt ${CA_DIR}/ca.key - wrong CA passphrase?"
        fi
    fi
    CA_KEY_PLAIN="${_CA_KEY_TMP}/ca.plain.key"
}
ca_key_lock() {
    [[ -n "${_CA_KEY_TMP}" && -d "${_CA_KEY_TMP}" ]] && rm -rf "${_CA_KEY_TMP}"
    _CA_KEY_TMP=""; CA_KEY_PLAIN=""
}
trap ca_key_lock EXIT

# UTC timestamp N minutes from now, in the YYMMDDHHMMSSZ form that 'openssl ca -startdate' takes.
utc_stamp() {
    date -u -d "$1 minutes" +%y%m%d%H%M%SZ 2>/dev/null || date -u "-v$1M" +%y%m%d%H%M%SZ
}

# Names/IPs of the cluster from .env (defaults match the rest of the repo).
node_name() { local v="NODE$1_NAME"; echo "${!v:-node$1}"; }
node_ip()   { local v="NODE$1_IP";   echo "${!v:-}"; }

# Bump the CRL (called by init and revoke).
gen_crl() {
    ca_key_unlock
    openssl ca -config "${CA_DIR}/ca.cnf" -keyfile "${CA_KEY_PLAIN}" -gencrl \
        -out "${CA_DIR}/ca.crl.tmp" >/dev/null 2>&1 || die "CRL generation failed."
    mv -f "${CA_DIR}/ca.crl.tmp" "${CA_DIR}/ca.crl"
    chmod 644 "${CA_DIR}/ca.crl"
}
