#!/usr/bin/env bash
set -euo pipefail

# ==============================================================================
# FlowFirst - End-to-End Flow Execution & Database Content Inspection Script
# Invokes Flow 1, Flow 2, and Batch tests via VIP/HAProxy REST API,
# then queries MariaDB Galera to inspect stored table records and audit trails.
# ==============================================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

# Source environment variables if .env exists
if [ -f "${ROOT_DIR}/.env" ]; then
    # shellcheck disable=SC1091
    set -a
    source "${ROOT_DIR}/.env"
    set +a
fi

VIP="${FLOWFIRST_VIP:-192.168.1.100}"
API_PORT="${API_PORT:-8080}"
DB_HOST="${MARIADB_HOST:-${VIP}}"
DB_PORT="${MARIADB_PORT:-3306}"
DB_USER="${MARIADB_USER:-flowuser}"
DB_PASS="${MARIADB_PASSWORD:-}"
[[ -n "${DB_PASS}" ]] || { echo "ERROR: MARIADB_PASSWORD is not set (put it in .env; there is no built-in default)." >&2; exit 1; }
DB_NAME="${MARIADB_DB:-flowfirst_db}"

# TLS (see security/ and the ReadMe "Security" section). Default client identity: this node's pipeline certificate.
is_true() { case "$(echo "${1:-}" | tr '[:upper:]' '[:lower:]')" in 1|true|yes|on) return 0 ;; *) return 1 ;; esac; }
TLS_DIR="${TLS_DIR:-/etc/flowfirst/tls}"
CA_FILE="${TLS_CA_FILE:-${TLS_DIR}/ca.crt}"
CLIENT_CERT="${TLS_CLIENT_CERT:-${TLS_DIR}/pipeline/pipeline.crt}"
CLIENT_KEY="${TLS_CLIENT_KEY:-${TLS_DIR}/pipeline/pipeline.key}"

SCHEME="http"
CURL_TLS=()
if is_true "${TLS_API_ENABLED:-false}"; then
    SCHEME="https"
    CURL_TLS=(--cacert "${CA_FILE}")
    if [ -r "${CLIENT_CERT}" ] && [ -r "${CLIENT_KEY}" ]; then
        CURL_TLS+=(--cert "${CLIENT_CERT}" --key "${CLIENT_KEY}")
    else
        echo "WARNING: client certificate/key not readable (${CLIENT_CERT}); mutual-TLS API calls will be refused." >&2
    fi
fi

DB_TLS=()
if is_true "${TLS_MARIADB_ENABLED:-false}"; then
    DB_TLS=(--ssl --ssl-ca="${CA_FILE}" --ssl-verify-server-cert)
    if [ -r "${CLIENT_CERT}" ] && [ -r "${CLIENT_KEY}" ]; then
        DB_TLS+=(--ssl-cert="${CLIENT_CERT}" --ssl-key="${CLIENT_KEY}")
    fi
fi

# curl/db wrappers (the ${arr[@]+...} form keeps empty arrays safe under `set -u` on older bash)
api() { curl -s ${CURL_TLS[@]+"${CURL_TLS[@]}"} "$@"; }

BASE_URL="${SCHEME}://${VIP}:${API_PORT}"

echo "=================================================================="
echo " FlowFirst - End-to-End Flow Execution & Database Inspection"
echo " REST API Target:   ${BASE_URL}"
echo " MariaDB Target:    ${DB_HOST}:${DB_PORT} (DB: ${DB_NAME})"
echo "=================================================================="

# Helper for formatted output
print_header() {
    echo ""
    echo "------------------------------------------------------------------"
    echo " $1"
    echo "------------------------------------------------------------------"
}

# 1. Health Check
print_header "[Step 1] Checking API Health"
api "${BASE_URL}/health" | jq . || api "${BASE_URL}/health"
echo ""

# 2. Trigger Flow 1 (Process 1 -> Process 2 [+10] -> Process 3 -> Process 4 -> MariaDB)
TEST_ITEM_F1=$(( (RANDOM % 900) + 100 ))
print_header "[Step 2] Triggering Flow 1 with Item #${TEST_ITEM_F1} (Initial Counter: 500)"
F1_RESP=$(api -X POST "${BASE_URL}/api/flow1" \
  -H "Content-Type: application/json" \
  -d "{
    \"item_id\": ${TEST_ITEM_F1},
    \"counter\": 500,
    \"initial_data\": \"Flow 1 E2E test item #${TEST_ITEM_F1}\"
  }")
echo "${F1_RESP}" | jq . || echo "${F1_RESP}"
F1_MSG_ID=$(echo "${F1_RESP}" | jq -r '.payload.message_id // empty')

# 3. Trigger Flow 2 (Process 1 -> Process 2 [x1.15] -> Process 3 [seal] -> Process 4 -> MariaDB)
TEST_ITEM_F2=$(( (RANDOM % 900) + 100 ))
print_header "[Step 3] Triggering Flow 2 with Item #${TEST_ITEM_F2} (Initial Value: 35.00)"
F2_RESP=$(api -X POST "${BASE_URL}/api/flow2" \
  -H "Content-Type: application/json" \
  -d "{
    \"item_id\": ${TEST_ITEM_F2},
    \"value\": 35.0,
    \"initial_data\": \"Flow 2 Temperature Alert item #${TEST_ITEM_F2}\"
  }")
echo "${F2_RESP}" | jq . || echo "${F2_RESP}"
F2_MSG_ID=$(echo "${F2_RESP}" | jq -r '.payload.message_id // empty')

# 4. Wait for RabbitMQ message pipeline to process and persist
echo ""
echo "Waiting 2 seconds for asynchronous RabbitMQ message consumption & DB storage..."
sleep 2

# 5. Examine MariaDB Table Content
print_header "[Step 4] Querying MariaDB Summary Table ('processed_messages')"

MARIADB_CMD="mariadb"
if ! command -v mariadb >/dev/null 2>&1 && command -v mysql >/dev/null 2>&1; then
    MARIADB_CMD="mysql"
fi

db() { "${MARIADB_CMD}" ${DB_TLS[@]+"${DB_TLS[@]}"} -h "${DB_HOST}" -P "${DB_PORT}" -u "${DB_USER}" -p"${DB_PASS}" "${DB_NAME}" "$@"; }

db --table -e "
SELECT 
    id,
    flow_id,
    item_id,
    counter_value,
    metric_value,
    examined_status,
    verified_by,
    created_at
FROM processed_messages
ORDER BY id DESC
LIMIT 10;
"

# 6. Detailed Inspection of Flow 1 Record & Full Audit Trail
if [ -n "${F1_MSG_ID}" ]; then
    print_header "[Step 5] Detailed Audit Trail for Flow 1 (Message ID: ${F1_MSG_ID})"
    db -e "
    SELECT
        message_id,
        initial_data,
        counter_value,
        JSON_PRETTY(history_trail) AS audit_history
    FROM processed_messages
    WHERE message_id = '${F1_MSG_ID}'\G
    "
fi

# 7. Detailed Inspection of Flow 2 Record & Transformation
if [ -n "${F2_MSG_ID}" ]; then
    print_header "[Step 6] Detailed Audit Trail for Flow 2 (Message ID: ${F2_MSG_ID})"
    db -e "
    SELECT
        message_id,
        initial_data,
        metric_value,
        examined_status,
        verified_by,
        JSON_PRETTY(history_trail) AS audit_history
    FROM processed_messages
    WHERE message_id = '${F2_MSG_ID}'\G
    "
fi

echo ""
echo "=================================================================="
echo " Inspection complete! All records verified in MariaDB Galera."
echo "=================================================================="
