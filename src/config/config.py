import os
import re
import socket
import pika
from dotenv import load_dotenv

load_dotenv()


def _int_env(name: str, default: int) -> int:
    """Read an env var as int, guarding against unexpanded shell references.

    python-dotenv does not expand ${VAR} references — if a .env file was
    copied from .env.example without substitution, variables like
    RABBITMQ_PORT may arrive as the literal string '${RABBITMQ_PORT}'.
    In that case we log a warning and fall back to the supplied default.
    """
    raw = os.getenv(name, "").strip()
    if not raw or re.search(r"\$\{?\w+\}?", raw):
        if raw:
            import sys
            print(
                f"[config] WARNING: {name}='{raw}' contains an unexpanded shell "
                f"reference — python-dotenv does not expand ${{VAR}}. "
                f"Using default: {default}. Fix your .env file.",
                file=sys.stderr,
            )
        return default
    return int(raw)


# Host identifier for multi-node cluster awareness
NODE_NAME = os.getenv("NODE_NAME", socket.gethostname())

# Primary Cluster Node IPs & VIP from environment
NODE1_IP = os.getenv("NODE1_IP", "127.0.0.1")
NODE2_IP = os.getenv("NODE2_IP", "127.0.0.1")
NODE3_IP = os.getenv("NODE3_IP", "127.0.0.1")
NODE4_IP = os.getenv("NODE4_IP", "127.0.0.1")
NODE4_NAME = os.getenv("NODE4_NAME", "node4")
FLOWFIRST_VIP = os.getenv("FLOWFIRST_VIP", NODE1_IP)

# Strip unexpanded shell references from IP/host values too
def _str_env(name: str, default: str) -> str:
    raw = os.getenv(name, "").strip()
    if not raw or re.search(r"\$\{?\w+\}?", raw):
        if raw:
            import sys
            print(
                f"[config] WARNING: {name}='{raw}' contains an unexpanded shell "
                f"reference — using default: '{default}'.",
                file=sys.stderr,
            )
        return default
    return raw


# Re-read VIP and hosts with the guarded reader
FLOWFIRST_VIP = _str_env("FLOWFIRST_VIP", NODE1_IP)


def _bool_env(name: str, default: bool = False) -> bool:
    raw = _str_env(name, "true" if default else "false").lower()
    return raw in ("1", "true", "yes", "on")


# TLS settings (security/ creates the certificates; src/tls/tls.py builds the contexts).
# Every switch defaults to off so that nothing changes until it is turned on. When a switch is on and a
# file is missing or unreadable the process refuses to start: it never falls back to plaintext.
TLS_DIR            = _str_env("TLS_DIR", "/etc/flowfirst/tls")
TLS_CA_FILE        = _str_env("TLS_CA_FILE", f"{TLS_DIR}/ca.crt")
TLS_CRL_FILE       = _str_env("TLS_CRL_FILE", "")      # blank = no revocation check
TLS_CLIENT_CERT    = _str_env("TLS_CLIENT_CERT", f"{TLS_DIR}/pipeline/pipeline.crt")   # identity of Processes 1-5
TLS_CLIENT_KEY     = _str_env("TLS_CLIENT_KEY", f"{TLS_DIR}/pipeline/pipeline.key")
TLS_MIN_VERSION    = _str_env("TLS_MIN_VERSION", "1.2")                                # 1.2 or 1.3
TLS_CIPHERS        = os.getenv("TLS_CIPHERS", "ECDHE+AESGCM:ECDHE+CHACHA20").strip()   # TLS 1.2 suites; blank = library default
TLS_API_ENABLED       = _bool_env("TLS_API_ENABLED")          # Process 1 serves HTTPS
TLS_API_CERT          = _str_env("TLS_API_CERT", f"{TLS_DIR}/api/api.crt")
TLS_API_KEY           = _str_env("TLS_API_KEY", f"{TLS_DIR}/api/api.key")
TLS_API_CLIENT_AUTH   = _str_env("TLS_API_CLIENT_AUTH", "required").lower()   # required | optional | none
TLS_RABBITMQ_ENABLED  = _bool_env("TLS_RABBITMQ_ENABLED")
TLS_MARIADB_ENABLED   = _bool_env("TLS_MARIADB_ENABLED")
TLS_ZK_ENABLED        = _bool_env("TLS_ZK_ENABLED")
TLS_SWIFT_ENABLED     = _bool_env("TLS_SWIFT_ENABLED")

# Authorisation for admin actions (POST /zk/config changes runtime behaviour of the whole cluster).
# ADMIN_ALLOWED_CNS: comma-separated certificate Common Names allowed to do that (e.g. "admin-client"). Blank = nobody.
# ADMIN_TRUSTED_PROXY_CNS: CNs of the HAProxy backend identity; only these may forward a caller identity in
#   X-SSL-Client-CN (HAProxy sets it from the verified client certificate and deletes any client-supplied copy).
# ALLOW_UNAUTHENTICATED_ADMIN: dev-only escape hatch for setups WITHOUT TLS (there is no caller identity to check).
ADMIN_ALLOWED_CNS             = _str_env("ADMIN_ALLOWED_CNS", "")
ADMIN_TRUSTED_PROXY_CNS       = _str_env("ADMIN_TRUSTED_PROXY_CNS", "")
ALLOW_UNAUTHENTICATED_ADMIN   = _bool_env("ALLOW_UNAUTHENTICATED_ADMIN")

# Flow-endpoint authentication (see src/application/authz.py for full description).
# AUTH_MODE: none (default, original behaviour) | api_key | token | cert
#   none     — no authentication on flow endpoints; admin endpoint is unaffected (still cert-gated).
#   api_key  — X-Api-Key header required on every flow request. Keys in AUTH_API_KEYS.
#   token    — Authorization: Bearer <token> OR X-Api-Key accepted. POST /auth/token issues tokens.
#   cert     — any valid client certificate (signed by the FlowFirst CA) required. TLS_API_ENABLED must be true.
# AUTH_API_KEYS: comma-separated 'label:sha256hex' entries. Generate:
#   python3 -c "import secrets,hashlib; k=secrets.token_urlsafe(32); \
#     print(f'myapp:{hashlib.sha256(k.encode()).hexdigest()}  # raw key: {k}')"
# AUTH_TOKEN_SECRET: random secret for HMAC-SHA256 token signing. Minimum 32 characters.
#   Generate: python3 -c "import secrets; print(secrets.token_hex(32))"
# AUTH_TOKEN_TTL_S: token lifetime in seconds (default 3600 = 1 hour).
_AUTH_MODE_VALID = frozenset({"none", "api_key", "token", "cert"})
AUTH_MODE         = _str_env("AUTH_MODE", "none").lower()
if AUTH_MODE not in _AUTH_MODE_VALID:
    raise ValueError(f"AUTH_MODE must be one of {sorted(_AUTH_MODE_VALID)}, not '{AUTH_MODE}'")
AUTH_API_KEYS     = os.getenv("AUTH_API_KEYS", "")        # not run through _str_env: may be blank
AUTH_TOKEN_SECRET = os.getenv("AUTH_TOKEN_SECRET", "")    # blank means tokens cannot be issued/verified
AUTH_TOKEN_TTL_S  = max(60, _int_env("AUTH_TOKEN_TTL_S", 3600))

# ---------------------------------------------------------------------------
# Per-endpoint authorization engine (RBAC / ACL / ABAC).
# Runs AFTER authentication — authentication answers "who are you?",
# authorization answers "what are you allowed to do?".
# See src/application/authz.py for the full design and evaluation order.
#
# AUTHZ_MODE:  none (default, existing behaviour) | rbac | acl | abac |
#              rbac+acl | rbac+abac | rbac+acl+abac
#
# AUTHZ_ROLES: custom role definitions.
#   Format: rolename=resource:action,resource:action;  rolename2=...
#   Built-in roles (always present): publisher, reader, admin, operator
#
# AUTHZ_IDENTITY_ROLES: map identities (API key labels, cert CNs, token subjects) to roles.
#   Format: identity1=role1,role2;  identity2=role3
#
# AUTHZ_ACL: per-resource explicit grants (fine-grained additions/exceptions).
#   Format: resource:action=identity1,identity2;  resource2:action=identity3
#   Example: flow:1:publish=ci_bot,data_ingest;  zk:config:write=admin-cert
#
# AUTHZ_POLICY: JSON list of ABAC rules.
#   Format: [{"when": {"identity":"bad_bot"}, "effect":"deny"}, ...]
#   Supported 'when' keys: identity, role, action, resource, method,
#     hour_utc_lt, hour_utc_gte
# ---------------------------------------------------------------------------
_AUTHZ_MODE_VALID = frozenset({
    "none", "rbac", "acl", "abac",
    "rbac+acl", "rbac+abac", "rbac+acl+abac",
})
AUTHZ_MODE           = _str_env("AUTHZ_MODE", "none").lower()
if AUTHZ_MODE not in _AUTHZ_MODE_VALID:
    raise ValueError(f"AUTHZ_MODE must be one of {sorted(_AUTHZ_MODE_VALID)}, not '{AUTHZ_MODE}'")
AUTHZ_ROLES          = os.getenv("AUTHZ_ROLES", "")
AUTHZ_IDENTITY_ROLES = os.getenv("AUTHZ_IDENTITY_ROLES", "")
AUTHZ_ACL            = os.getenv("AUTHZ_ACL", "")
AUTHZ_POLICY         = os.getenv("AUTHZ_POLICY", "")

RABBITMQ_TLS_PORT     = _int_env("RABBITMQ_TLS_PORT", 5671)
ZK_TLS_CLIENT_PORT    = _int_env("ZK_TLS_CLIENT_PORT", 2281)

# Process 1 REST API Settings
# Process 1 listens on API_BACKEND_PORT (8082). HAProxy frontend listens on API_PORT (8080).
API_HOST = os.getenv("API_HOST", "0.0.0.0")
API_PORT = _int_env("API_PORT", 8080)
API_BACKEND_PORT = _int_env("API_BACKEND_PORT", 8082)

# Scale parameters (defaults reproduce the original fixed behaviour).
# API_REQUEST_POOL_SIZE: concurrent HTTP requests one Process 1 serves; 0 = GT_WORKER_CONCURRENCY * 10 (40).
# API_LISTEN_BACKLOG: pending-connection queue of the Process 1 socket (the kernel caps it at net.core.somaxconn).
API_REQUEST_POOL_SIZE = max(0, _int_env("API_REQUEST_POOL_SIZE", 0))
API_LISTEN_BACKLOG = max(1, _int_env("API_LISTEN_BACKLOG", 50))

# Request limits (Process 1). Anything above a limit is refused, never silently truncated.
#   API_MAX_BODY_BYTES: largest accepted POST body (HTTP 413 above it).  API_MAX_BATCH_COUNT: largest "count" for POST /api/batch.
#   API_MAX_LIST_LIMIT: largest "limit" for GET /swift/objects.  API_REQUEST_TIMEOUT_S: socket timeout per connection, so a
#   client that stalls while sending headers or the body cannot hold a request slot forever.
#   API_EXPOSE_ERROR_DETAILS=true puts exception text in error responses - leave it false anywhere real users connect.
API_MAX_BODY_BYTES      = max(1024, _int_env("API_MAX_BODY_BYTES", 1048576))
API_MAX_BATCH_COUNT     = max(1, _int_env("API_MAX_BATCH_COUNT", 100))
API_MAX_LIST_LIMIT      = max(1, _int_env("API_MAX_LIST_LIMIT", 1000))
API_REQUEST_TIMEOUT_S   = max(1, _int_env("API_REQUEST_TIMEOUT_S", 30))
API_EXPOSE_ERROR_DETAILS = _bool_env("API_EXPOSE_ERROR_DETAILS")

# Process 1 publisher connections. Publisher confirms (chunk "message safety") make every publish wait for the broker, so
# ONE shared connection caps Process 1 at roughly 1/RTT messages per second. Several connections publish in parallel.
# Each connection is used by one greenthread at a time (its own semaphore). 1 reproduces the previous single-connection design.
# Measured on one RabbitMQ container (illustrative, see ReadMe): 1 -> ~1.3k msg/s, 4 -> ~2.6k, 8 -> ~4.3k at 100 concurrent callers.
API_PUBLISH_CONNECTIONS = max(1, min(32, _int_env("API_PUBLISH_CONNECTIONS", 4)))
# Monitoring (src/metrics/metrics.py). Off by default. When on, each process serves GET /metrics (Prometheus text
# format) on METRICS_PORT_BASE + its number: process1 -> 9101 ... process5 -> 9105. It is NOT behind HAProxy, so
# Prometheus scrapes every node directly. With TLS_API_ENABLED=true it uses the API certificate and TLS_API_CLIENT_AUTH.
# METRICS_HOST: bind address. Use the node's own IP (or 127.0.0.1 with a local agent) instead of 0.0.0.0 where you can.
METRICS_ENABLED   = _bool_env("METRICS_ENABLED")
METRICS_HOST      = _str_env("METRICS_HOST", "0.0.0.0")
METRICS_PORT_BASE = max(1024, min(65530, _int_env("METRICS_PORT_BASE", 9100)))

# RabbitMQ Connection Settings
RABBITMQ_PORT = _int_env("RABBITMQ_PORT", 5672)
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", FLOWFIRST_VIP)
RABBITMQ_USER = os.getenv("RABBITMQ_USER", "flowuser")
RABBITMQ_PASSWORD = os.getenv("RABBITMQ_PASSWORD", "flowpassword")

# Build RabbitMQ host list from NODE1_IP..NODE3_IP or RABBITMQ_HOSTS env
# (with TLS on, the brokers are reached on RABBITMQ_TLS_PORT)
_raw_rmq_hosts = os.getenv("RABBITMQ_HOSTS", "").strip()
_rmq_port = RABBITMQ_TLS_PORT if TLS_RABBITMQ_ENABLED else RABBITMQ_PORT
if _raw_rmq_hosts:
    RABBITMQ_HOSTS = _raw_rmq_hosts
else:
    RABBITMQ_HOSTS = f"{NODE1_IP}:{_rmq_port},{NODE2_IP}:{_rmq_port},{NODE3_IP}:{_rmq_port}"

# MariaDB Connection Settings
MARIADB_PORT = _int_env("MARIADB_PORT", 3306)
MARIADB_HOST = _str_env("MARIADB_HOST", FLOWFIRST_VIP)
MARIADB_USER = os.getenv("MARIADB_USER", "flowuser")
MARIADB_PASSWORD = os.getenv("MARIADB_PASSWORD", "flowpassword")
MARIADB_DB = os.getenv("MARIADB_DB", "flowfirst_db")

# Build MariaDB host list from NODE1_IP..NODE3_IP or MARIADB_HOSTS env
_raw_mariadb_hosts = os.getenv("MARIADB_HOSTS", "").strip()
if _raw_mariadb_hosts:
    MARIADB_HOSTS = _raw_mariadb_hosts
else:
    MARIADB_HOSTS = f"{NODE1_IP},{NODE2_IP},{NODE3_IP}"

# Swift Object Storage Connection Settings
# Swift is a native 3-node cluster (swift/setup_swift.sh) behind HAProxy on the VIP
SWIFT_LB_PORT = _int_env("SWIFT_LB_PORT", 8090)
SWIFT_AUTH_URL = _str_env("SWIFT_AUTH_URL", f"{'https' if TLS_SWIFT_ENABLED else 'http'}://{FLOWFIRST_VIP}:{SWIFT_LB_PORT}/auth/v1.0")
SWIFT_AUTH_VERSION = os.getenv("SWIFT_AUTH_VERSION", "1.0")
SWIFT_USER = os.getenv("SWIFT_USER", "test:tester")
SWIFT_KEY = os.getenv("SWIFT_KEY", "testing")
SWIFT_CONTAINER = os.getenv("SWIFT_CONTAINER", "flowfirst_messages")
SWIFT_ENABLED = os.getenv("SWIFT_ENABLED", "true").strip().lower() in ("true", "1", "yes", "on")


# ---------------------------------------------------------------------------
# Secret hygiene - fail closed.
# The sample values that ship in documentation and in .env.example are public. A process that would start with one of
# them (or with an empty / short secret) refuses to start instead, naming the variable but never printing its value.
# ALLOW_DEFAULT_SECRETS=true is the single, explicit opt-out for a throw-away local/dev setup (it logs a warning).
# ---------------------------------------------------------------------------
SECRET_MIN_LENGTH      = max(8, _int_env("SECRET_MIN_LENGTH", 12))
ALLOW_DEFAULT_SECRETS  = _bool_env("ALLOW_DEFAULT_SECRETS")
_KNOWN_WEAK_SECRETS = frozenset({
    "flowpassword", "testing", "admin123", "admin", "guest", "password", "passw0rd", "secret", "root", "test",
    "changeme", "change-me", "changeit", "12345678", "123456789012",
})


def weak_secret_reason(value) -> str:
    """Return why a secret is unacceptable, or '' when it is fine. Never includes the secret itself."""
    text = (value or "").strip()
    if not text:
        return "is empty"
    if text.lower() in _KNOWN_WEAK_SECRETS or text.lower().startswith("changeme"):
        return "is a well-known sample/default value"
    if len(text) < SECRET_MIN_LENGTH:
        return "is shorter than %d characters" % SECRET_MIN_LENGTH
    return ""


def check_secrets() -> None:
    """Raise RuntimeError if a required secret is a default, empty or short. Called once when this module loads."""
    secrets = {"RABBITMQ_PASSWORD": RABBITMQ_PASSWORD, "MARIADB_PASSWORD": MARIADB_PASSWORD}
    if SWIFT_ENABLED:
        secrets["SWIFT_KEY"] = SWIFT_KEY
    problems = ["%s %s" % (name, why) for name, value in secrets.items() if (why := weak_secret_reason(value))]
    if not problems:
        return
    if ALLOW_DEFAULT_SECRETS:
        import logging
        logging.getLogger("flowfirst.config").warning(
            "ALLOW_DEFAULT_SECRETS=true: running with weak secrets (%s). NEVER use this in production.", "; ".join(problems))
        return
    raise RuntimeError(
        "Refusing to start with weak secrets: " + "; ".join(problems) + ". Set strong values in .env "
        "(generate with: ./scripts/generate_secrets.sh), or set ALLOW_DEFAULT_SECRETS=true for a local dev setup only.")


check_secrets()

# Flow 1 Queues
# Flow 1: P1 -> [flow1_p1_to_p2] -> P2 -> (reflects modified) -> [flow1_p2_to_p3] -> P3 -> [flow1_p3_to_p4] -> P4 (persists to MariaDB)
QUEUE_FLOW1_P1_TO_P2 = "flow1_p1_to_p2"
QUEUE_FLOW1_P2_TO_P3 = "flow1_p2_to_p3"
QUEUE_FLOW1_P3_TO_P4 = "flow1_p3_to_p4"

# Flow 2 Queues
# Flow 2: P1 -> [flow2_p1_to_p2] -> P2 -> (examines/modifies) -> [flow2_p2_to_p3] -> P3 -> (reflects modified) -> [flow2_p3_reflected] -> P4 (persists to MariaDB)
QUEUE_FLOW2_P1_TO_P2 = "flow2_p1_to_p2"
QUEUE_FLOW2_P2_TO_P3 = "flow2_p2_to_p3"
QUEUE_FLOW2_P3_REFLECTED = "flow2_p3_reflected"

# Flow 3 Queues (Remote Node 4 / Process 5 Scenario)
# Flow 3: P1 (Node1-3) -> [flow3_p1_to_p5] -> P5 (Remote Node 4) -> (modifies/reflects) -> [flow3_p5_to_p2] -> P2 (Node1-3) -> [flow3_p2_to_p4] -> P4 (persists to MariaDB & Swift)
QUEUE_FLOW3_P1_TO_P5 = "flow3_p1_to_p5"
QUEUE_FLOW3_P5_TO_P2 = "flow3_p5_to_p2"
QUEUE_FLOW3_P2_TO_P4 = "flow3_p2_to_p4"

# Flow 4 Queues (IoT Device Create)
# Flow 4: P1 -> [flow4_p1_to_p2] -> P2 (enrich) -> [flow4_p2_to_p3] -> P3 (validate) -> [flow4_p3_to_p4] -> P4 (persist)
QUEUE_FLOW4_P1_TO_P2 = "flow4_p1_to_p2"
QUEUE_FLOW4_P2_TO_P3 = "flow4_p2_to_p3"
QUEUE_FLOW4_P3_TO_P4 = "flow4_p3_to_p4"

# Flow 5 Queues (IoT Device Query)
# Flow 5: P1 -> [flow5_p1_to_p2] -> P2 (normalize) -> [flow5_p2_to_p3] -> P3 (validate) -> [flow5_p3_to_p4] -> P4 (query+archive)
QUEUE_FLOW5_P1_TO_P2 = "flow5_p1_to_p2"
QUEUE_FLOW5_P2_TO_P3 = "flow5_p2_to_p3"
QUEUE_FLOW5_P3_TO_P4 = "flow5_p3_to_p4"

# Flow 6 Queues (IoT Device Update — also fans out to remote P5)
# Flow 6: P1 -> [flow6_p1_to_p2] -> P2 (sequence) -> [flow6_p2_to_p3] -> P3 (validate)
#         -> [flow6_p3_to_p4] (direct persist) AND [flow6_p3_to_p5] (remote fan-out)
#         -> P5 (edge sync) -> [flow6_p5_to_p2] -> P2 -> [flow6_p2_to_p4] -> P4 (remote result persist)
QUEUE_FLOW6_P1_TO_P2 = "flow6_p1_to_p2"
QUEUE_FLOW6_P2_TO_P3 = "flow6_p2_to_p3"
QUEUE_FLOW6_P3_TO_P4 = "flow6_p3_to_p4"
QUEUE_FLOW6_P3_TO_P5 = "flow6_p3_to_p5"
QUEUE_FLOW6_P5_TO_P2 = "flow6_p5_to_p2"
QUEUE_FLOW6_P2_TO_P4 = "flow6_p2_to_p4"

# Flow 7 Queues (IoT Device Soft-Delete)
# Flow 7: P1 -> [flow7_p1_to_p2] -> P2 (mark) -> [flow7_p2_to_p3] -> P3 (validate) -> [flow7_p3_to_p4] -> P4 (decommission)
QUEUE_FLOW7_P1_TO_P2 = "flow7_p1_to_p2"
QUEUE_FLOW7_P2_TO_P3 = "flow7_p2_to_p3"
QUEUE_FLOW7_P3_TO_P4 = "flow7_p3_to_p4"

# Greenthread (eventlet) settings
GT_POOL_SIZE          = int(os.getenv("GT_POOL_SIZE",          "1000"))
GT_WORKER_CONCURRENCY = int(os.getenv("GT_WORKER_CONCURRENCY", "4"))
GT_METRICS_INTERVAL_S = int(os.getenv("GT_METRICS_INTERVAL_S", "60"))
# GT_CONSUMERS_PER_QUEUE: consumer greenthreads (each with its own RabbitMQ connection) per queue in P2-P5.
# GT_PREFETCH_COUNT: unacknowledged messages RabbitMQ may hand to one consumer at a time.
GT_CONSUMERS_PER_QUEUE = max(1, _int_env("GT_CONSUMERS_PER_QUEUE", 1))
GT_PREFETCH_COUNT      = max(1, _int_env("GT_PREFETCH_COUNT", 1))

# ZooKeeper connection settings
ZK_CLIENT_PORT    = _int_env("ZK_CLIENT_PORT", 2181)
_raw_zk_hosts     = _str_env("ZK_HOSTS", "")
_zk_port          = ZK_TLS_CLIENT_PORT if TLS_ZK_ENABLED else ZK_CLIENT_PORT
if _raw_zk_hosts:
    ZK_HOSTS = _raw_zk_hosts
else:
    ZK_HOSTS = f"{NODE1_IP}:{_zk_port},{NODE2_IP}:{_zk_port},{NODE3_IP}:{_zk_port}"
ZK_TIMEOUT        = float(os.getenv("ZK_TIMEOUT", "10"))
ZK_DEDUP_TTL_MS   = int(os.getenv("ZK_DEDUP_TTL_MS", "300000"))

# Dead-letter handling: a message a consumer rejects (nack, requeue=False) is routed by the policy
# "flowfirst-dlx" to this fanout exchange and kept in QUEUE_DEAD_LETTERS instead of being discarded.
# Not part of ALL_QUEUES: no pipeline process consumes it. See scripts/replay_dead_letters.py.
DLX_EXCHANGE        = "flowfirst.dlx"
QUEUE_DEAD_LETTERS  = "flowfirst_dead_letters"

# Queue replication. "classic" = one node holds each queue (the original behaviour: a node that dies can lose the messages
# queued on it). "quorum" = every queue is replicated by Raft across the RabbitMQ cluster, a publisher confirm is sent only
# after a majority has the message, and one node can fail without losing messages. The type of an EXISTING queue cannot
# be changed: declaring it with a different type is refused (PRECONDITION_FAILED). Use scripts/migrate_queue_type.py.
RABBITMQ_QUEUE_TYPE = _str_env("RABBITMQ_QUEUE_TYPE", "classic").lower()
if RABBITMQ_QUEUE_TYPE not in ("classic", "quorum"):
    raise ValueError(f"RABBITMQ_QUEUE_TYPE must be 'classic' or 'quorum', not '{RABBITMQ_QUEUE_TYPE}'")


def queue_arguments(queue_type: str = None):
    """Declaration arguments for a pipeline queue. None for classic, so classic declarations stay exactly as before."""
    return {"x-queue-type": "quorum"} if (queue_type or RABBITMQ_QUEUE_TYPE) == "quorum" else None

ALL_QUEUES = [
    QUEUE_FLOW1_P1_TO_P2,
    QUEUE_FLOW1_P2_TO_P3,
    QUEUE_FLOW1_P3_TO_P4,
    QUEUE_FLOW2_P1_TO_P2,
    QUEUE_FLOW2_P2_TO_P3,
    QUEUE_FLOW2_P3_REFLECTED,
    QUEUE_FLOW3_P1_TO_P5,
    QUEUE_FLOW3_P5_TO_P2,
    QUEUE_FLOW3_P2_TO_P4,
    QUEUE_FLOW4_P1_TO_P2,
    QUEUE_FLOW4_P2_TO_P3,
    QUEUE_FLOW4_P3_TO_P4,
    QUEUE_FLOW5_P1_TO_P2,
    QUEUE_FLOW5_P2_TO_P3,
    QUEUE_FLOW5_P3_TO_P4,
    QUEUE_FLOW6_P1_TO_P2,
    QUEUE_FLOW6_P2_TO_P3,
    QUEUE_FLOW6_P3_TO_P4,
    QUEUE_FLOW6_P3_TO_P5,
    QUEUE_FLOW6_P5_TO_P2,
    QUEUE_FLOW6_P2_TO_P4,
    QUEUE_FLOW7_P1_TO_P2,
    QUEUE_FLOW7_P2_TO_P3,
    QUEUE_FLOW7_P3_TO_P4,
]


def get_connection():
    """Create and return a blocking connection to RabbitMQ with multi-node failover support.

    When a list of ConnectionParameters is passed to BlockingConnection (multi-host
    path), pika picks the first reachable broker but does NOT expose a public .params
    attribute on the returned connection object — accessing .params raises AttributeError.
    To allow callers to log the connected address without depending on pika internals,
    this function attaches a plain `_connected_to` string attribute to the connection
    before returning it.
    """
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)

    ssl_options = None
    if TLS_RABBITMQ_ENABLED:
        from src.tls.tls import client_context        # imported here: tls.py imports this module
        ssl_options = pika.SSLOptions(client_context("rabbitmq"))   # server_hostname defaults to the host being dialled

    # If hosts are supplied, try them in order (pika picks the first reachable one)
    if RABBITMQ_HOSTS:
        hosts = [h.strip() for h in RABBITMQ_HOSTS.split(",") if h.strip()]
        params_list = [
            pika.ConnectionParameters(
                host=h.split(":")[0],
                port=int(h.split(":")[1]) if ":" in h else _rmq_port,
                credentials=credentials,
                ssl_options=ssl_options,
                heartbeat=60,
                blocked_connection_timeout=300,
            )
            for h in hosts
        ]
        conn = pika.BlockingConnection(params_list)
        # pika internally stores the winning params on the impl layer; surface it safely
        resolved = getattr(getattr(conn, "_impl", None), "params", None)
        if resolved is not None:
            conn._connected_to = f"{resolved.host}:{resolved.port}"
        else:
            conn._connected_to = RABBITMQ_HOSTS  # fallback: show the whole list
        return conn

    parameters = pika.ConnectionParameters(
        host=RABBITMQ_HOST,
        port=_rmq_port,
        credentials=credentials,
        ssl_options=ssl_options,
        heartbeat=60,
        blocked_connection_timeout=300,
    )
    conn = pika.BlockingConnection(parameters)
    conn._connected_to = f"{RABBITMQ_HOST}:{_rmq_port}"
    return conn


def setup_queues(channel):
    """Declare all queues as durable, plus the dead-letter exchange and queue.

    The pipeline queues are declared WITHOUT dead-letter arguments on purpose: changing the arguments of an existing
    queue makes RabbitMQ refuse the declaration (PRECONDITION_FAILED). The dead-letter exchange is attached to them by
    a policy instead (scripts/setup_rabbitmq_dlx.sh), which is safe on a running cluster. Only NEW objects are declared
    here, so this stays idempotent whether or not the policy exists yet."""
    # Publisher confirms: basic_publish() now blocks until the broker has accepted (and, for persistent messages on
    # quorum/mirrored queues, replicated) the message, and RAISES pika.exceptions.NackError / UnroutableError otherwise.
    # Callers therefore never acknowledge an input or answer 200 for a message the broker did not take.
    channel.confirm_delivery()
    args = queue_arguments()
    try:
        for queue_name in ALL_QUEUES:
            channel.queue_declare(queue=queue_name, durable=True, arguments=args)
        channel.exchange_declare(exchange=DLX_EXCHANGE, exchange_type="fanout", durable=True)
        channel.queue_declare(queue=QUEUE_DEAD_LETTERS, durable=True, arguments=args)
    except pika.exceptions.ChannelClosedByBroker as exc:
        if exc.reply_code != 406:
            raise
        raise RuntimeError(
            f"RabbitMQ refused to declare a queue ({exc.reply_text}). A queue that already exists cannot change type: "
            f"RABBITMQ_QUEUE_TYPE={RABBITMQ_QUEUE_TYPE} does not match it. Drain the pipeline and run "
            f"scripts/migrate_queue_type.py --to {RABBITMQ_QUEUE_TYPE}, or set RABBITMQ_QUEUE_TYPE back."
        ) from exc
    channel.queue_bind(queue=QUEUE_DEAD_LETTERS, exchange=DLX_EXCHANGE)
