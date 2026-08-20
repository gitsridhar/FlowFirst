import os
import pika
from dotenv import load_dotenv

load_dotenv()

# Process 1 REST API Settings
API_HOST = os.getenv("API_HOST", "0.0.0.0")
API_PORT = int(os.getenv("API_PORT", 8080))

# RabbitMQ Connection Settings
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", 5672))
RABBITMQ_USER = os.getenv("RABBITMQ_USER", "guest")
RABBITMQ_PASSWORD = os.getenv("RABBITMQ_PASSWORD", "guest")

# MariaDB Connection Settings
MARIADB_HOST = os.getenv("MARIADB_HOST", "localhost")
MARIADB_PORT = int(os.getenv("MARIADB_PORT", 3306))
MARIADB_USER = os.getenv("MARIADB_USER", "flowuser")
MARIADB_PASSWORD = os.getenv("MARIADB_PASSWORD", "flowpassword")
MARIADB_DB = os.getenv("MARIADB_DB", "flowfirst_db")

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
    """Create and return a blocking connection to RabbitMQ."""
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
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
