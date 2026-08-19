import json
import time
import uuid
import pika
from config import (
    get_connection,
    setup_queues,
    QUEUE_FLOW1_P1_TO_P2,
    QUEUE_FLOW2_P1_TO_P2,
)


def send_flow1_message(channel, item_id: int):
    payload = {
        "flow": 1,
        "message_id": str(uuid.uuid4()),
        "item_id": item_id,
        "initial_data": f"Flow-1 original payload for item #{item_id}",
        "counter": 100 + item_id,
        "history": [
            {
                "stage": "process1_created",
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
                "status": "prepared",
            }
        ],
    }
    body = json.dumps(payload, indent=2)
    channel.basic_publish(
        exchange="",
        routing_key=QUEUE_FLOW1_P1_TO_P2,
        body=body,
        properties=pika.BasicProperties(
            delivery_mode=pika.DeliveryMode.Persistent,
            content_type="application/json",
        ),
    )
    print(f"[Process 1] [Flow 1] Sent message #{item_id} to '{QUEUE_FLOW1_P1_TO_P2}': (counter={payload['counter']})")


def send_flow2_message(channel, item_id: int):
    payload = {
        "flow": 2,
        "message_id": str(uuid.uuid4()),
        "item_id": item_id,
        "initial_data": f"Flow-2 raw metric for item #{item_id}",
        "value": round(25.0 + item_id * 1.5, 2),
        "history": [
            {
                "stage": "process1_created",
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
                "status": "prepared",
            }
        ],
    }
    body = json.dumps(payload, indent=2)
    channel.basic_publish(
        exchange="",
        routing_key=QUEUE_FLOW2_P1_TO_P2,
        body=body,
        properties=pika.BasicProperties(
            delivery_mode=pika.DeliveryMode.Persistent,
            content_type="application/json",
        ),
    )
    print(f"[Process 1] [Flow 2] Sent message #{item_id} to '{QUEUE_FLOW2_P1_TO_P2}': (value={payload['value']})")


def main():
    print("[Process 1] Starting data generator producer...")
    connection = get_connection()
    channel = connection.channel()
    setup_queues(channel)

    item_count = 5
    try:
        for i in range(1, item_count + 1):
            send_flow1_message(channel, i)
            time.sleep(0.5)
            send_flow2_message(channel, i)
            time.sleep(1.0)
        print(f"[Process 1] Finished publishing {item_count} items for both flows.")
    except KeyboardInterrupt:
        print("\n[Process 1] Interrupted by user.")
    finally:
        connection.close()
        print("[Process 1] Connection closed.")


if __name__ == "__main__":
    main()
