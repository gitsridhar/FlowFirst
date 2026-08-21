import os
import socket
import pika
from dotenv import load_dotenv

load_dotenv()

# Host identifier for multi-node cluster awareness
NODE_NAME = os.getenv("NODE_NAME", socket.gethostname())

# Primary Cluster Node IPs & VIP from environment
NODE1_IP = os.getenv("NODE1_IP", "127.0.0.1")
NODE2_IP = os.getenv("NODE2_IP", "127.0.0.1")
NODE3_IP = os.getenv("NODE3_IP", "127.0.0.1")
FLOWFIRST_VIP = os.getenv("FLOWFIRST_VIP", NODE1_IP)

# Process 1 REST API Settings
API_HOST = os.getenv("API_HOST", "0.0.0.0")
API_PORT = int(os.getenv("API_PORT", 8080))

# RabbitMQ Connection Settings
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", 5672))
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
MARIADB_PORT = int(os.getenv("MARIADB_PORT", 3306))
MARIADB_HOST = os.getenv("MARIADB_HOST", FLOWFIRST_VIP)
MARIADB_USER = os.getenv("MARIADB_USER", "flowuser")
MARIADB_PASSWORD = os.getenv("MARIADB_PASSWORD", "flowpassword")
MARIADB_DB = os.getenv("MARIADB_DB", "flowfirst_db")

# Build MariaDB host list from NODE1_IP..NODE3_IP or MARIADB_HOSTS env
_raw_mariadb_hosts = os.getenv("MARIADB_HOSTS", "").strip()
if _raw_mariadb_hosts:
    MARIADB_HOSTS = _raw_mariadb_hosts
else:
    MARIADB_HOSTS = f"{NODE1_IP},{NODE2_IP},{NODE3_IP}"

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

ALL_QUEUES = [
    QUEUE_FLOW1_P1_TO_P2,
    QUEUE_FLOW1_P2_TO_P3,
    QUEUE_FLOW1_P3_TO_P4,
    QUEUE_FLOW2_P1_TO_P2,
    QUEUE_FLOW2_P2_TO_P3,
    QUEUE_FLOW2_P3_REFLECTED,
]


def get_connection():
    """Create and return a blocking connection to RabbitMQ with multi-node failover support."""
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)

    # If hosts are supplied, try them in order or pick active VIP
    if RABBITMQ_HOSTS:
        hosts = [h.strip() for h in RABBITMQ_HOSTS.split(",") if h.strip()]
        params = [
            pika.ConnectionParameters(
                host=h.split(":")[0],
                port=int(h.split(":")[1]) if ":" in h else RABBITMQ_PORT,
                credentials=credentials,
                heartbeat=60,
                blocked_connection_timeout=300,
            )
            for h in hosts
        ]
        return pika.BlockingConnection(params)

    parameters = pika.ConnectionParameters(
        host=RABBITMQ_HOST,
        port=RABBITMQ_PORT,
        credentials=credentials,
        heartbeat=60,
        blocked_connection_timeout=300,
    )
    return pika.BlockingConnection(parameters)


def setup_queues(channel):
    """Declare all queues as durable."""
    for queue_name in ALL_QUEUES:
        channel.queue_declare(queue=queue_name, durable=True)
