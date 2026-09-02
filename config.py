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

# Process 1 REST API Settings
# Process 1 listens on API_BACKEND_PORT (8082). HAProxy frontend listens on API_PORT (8080).
API_HOST = os.getenv("API_HOST", "0.0.0.0")
API_PORT = _int_env("API_PORT", 8080)
API_BACKEND_PORT = _int_env("API_BACKEND_PORT", 8082)

# RabbitMQ Connection Settings
RABBITMQ_PORT = _int_env("RABBITMQ_PORT", 5672)
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", FLOWFIRST_VIP)
RABBITMQ_USER = os.getenv("RABBITMQ_USER", "flowuser")
RABBITMQ_PASSWORD = os.getenv("RABBITMQ_PASSWORD", "flowpassword")

# Build RabbitMQ host list from NODE1_IP..NODE3_IP or RABBITMQ_HOSTS env
_raw_rmq_hosts = os.getenv("RABBITMQ_HOSTS", "").strip()
if _raw_rmq_hosts:
    RABBITMQ_HOSTS = _raw_rmq_hosts
else:
    RABBITMQ_HOSTS = f"{NODE1_IP}:{RABBITMQ_PORT},{NODE2_IP}:{RABBITMQ_PORT},{NODE3_IP}:{RABBITMQ_PORT}"

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
SWIFT_AUTH_URL = os.getenv("SWIFT_AUTH_URL", f"http://{FLOWFIRST_VIP}:8080/auth/v1.0")
SWIFT_AUTH_VERSION = os.getenv("SWIFT_AUTH_VERSION", "1.0")
SWIFT_USER = os.getenv("SWIFT_USER", "test:tester")
SWIFT_KEY = os.getenv("SWIFT_KEY", "testing")
SWIFT_CONTAINER = os.getenv("SWIFT_CONTAINER", "flowfirst_messages")
SWIFT_ENABLED = os.getenv("SWIFT_ENABLED", "true").strip().lower() in ("true", "1", "yes", "on")

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

# Greenthread (eventlet) settings
GT_POOL_SIZE          = int(os.getenv("GT_POOL_SIZE",          "1000"))
GT_WORKER_CONCURRENCY = int(os.getenv("GT_WORKER_CONCURRENCY", "4"))
GT_METRICS_INTERVAL_S = int(os.getenv("GT_METRICS_INTERVAL_S", "60"))

# ZooKeeper connection settings
ZK_HOSTS = os.getenv(
    "ZK_HOSTS",
    f"{os.getenv('NODE1_IP','127.0.0.1')}:2181,"
    f"{os.getenv('NODE2_IP','127.0.0.1')}:2181,"
    f"{os.getenv('NODE3_IP','127.0.0.1')}:2181",
)
ZK_TIMEOUT        = float(os.getenv("ZK_TIMEOUT", "10"))
ZK_DEDUP_TTL_MS   = int(os.getenv("ZK_DEDUP_TTL_MS", "300000"))
ZK_CLIENT_PORT    = _int_env("ZK_CLIENT_PORT", 2181)

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

    # If hosts are supplied, try them in order (pika picks the first reachable one)
    if RABBITMQ_HOSTS:
        hosts = [h.strip() for h in RABBITMQ_HOSTS.split(",") if h.strip()]
        params_list = [
            pika.ConnectionParameters(
                host=h.split(":")[0],
                port=int(h.split(":")[1]) if ":" in h else RABBITMQ_PORT,
                credentials=credentials,
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
        port=RABBITMQ_PORT,
        credentials=credentials,
        heartbeat=60,
        blocked_connection_timeout=300,
    )
    conn = pika.BlockingConnection(parameters)
    conn._connected_to = f"{RABBITMQ_HOST}:{RABBITMQ_PORT}"
    return conn


def setup_queues(channel):
    """Declare all queues as durable."""
    for queue_name in ALL_QUEUES:
        channel.queue_declare(queue=queue_name, durable=True)
