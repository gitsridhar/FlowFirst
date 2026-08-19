import os
import pika
from dotenv import load_dotenv

load_dotenv()

# RabbitMQ Connection Settings
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", 5672))
RABBITMQ_USER = os.getenv("RABBITMQ_USER", "guest")
RABBITMQ_PASSWORD = os.getenv("RABBITMQ_PASSWORD", "guest")

# Flow 1 Queues
# Flow 1: Process 1 -> [flow1_p1_to_p2] -> Process 2 -> (reflects modified) -> [flow1_p2_to_p3] -> Process 3
QUEUE_FLOW1_P1_TO_P2 = "flow1_p1_to_p2"
QUEUE_FLOW1_P2_TO_P3 = "flow1_p2_to_p3"

# Flow 2 Queues
# Flow 2: Process 1 -> [flow2_p1_to_p2] -> Process 2 -> (examines/modifies) -> [flow2_p2_to_p3] -> Process 3 -> (reflects modified) -> [flow2_p3_reflected]
QUEUE_FLOW2_P1_TO_P2 = "flow2_p1_to_p2"
QUEUE_FLOW2_P2_TO_P3 = "flow2_p2_to_p3"
QUEUE_FLOW2_P3_REFLECTED = "flow2_p3_reflected"

ALL_QUEUES = [
    QUEUE_FLOW1_P1_TO_P2,
    QUEUE_FLOW1_P2_TO_P3,
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
