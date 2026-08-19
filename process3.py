import json
import time
import pika
from config import (
    get_connection,
    setup_queues,
    QUEUE_FLOW1_P2_TO_P3,
    QUEUE_FLOW1_P3_TO_P4,
    QUEUE_FLOW2_P2_TO_P3,
    QUEUE_FLOW2_P3_REFLECTED,
)


def on_flow1_message(ch, method, properties, body):
    """
    Flow 1 handler:
    Process 3 receives the reflected message from Process 2, adds audit notes,
    and forwards it to Process 4 (QUEUE_FLOW1_P3_TO_P4) to be stored in MariaDB.
    """
    try:
        data = json.loads(body.decode("utf-8"))
        print(f"\n[Process 3] [Flow 1] Picked up item #{data.get('item_id')}")
        print(f"             Counter: {data.get('counter')}")

        data["process3_acknowledged"] = True
        data["history"].append(
            {
                "stage": "process3_received_and_forwarded",
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
                "status": "forwarded_to_process4",
            }
        )

        forward_body = json.dumps(data, indent=2)
        ch.basic_publish(
            exchange="",
            routing_key=QUEUE_FLOW1_P3_TO_P4,
            body=forward_body,
            properties=pika.BasicProperties(
                delivery_mode=pika.DeliveryMode.Persistent,
                content_type="application/json",
            ),
        )
        print(f"[Process 3] [Flow 1] Forwarded item #{data.get('item_id')} to '{QUEUE_FLOW1_P3_TO_P4}' for DB storage")
        ch.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        print(f"[Process 3] [Flow 1] Error: {e}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)


def on_flow2_message(ch, method, properties, body):
    """
    Flow 2 handler:
    Process 3 picks up the examined data from Process 2, modifies it slightly,
    and reflects it back to the queue (QUEUE_FLOW2_P3_REFLECTED) which Process 4 consumes.
    """
    try:
        data = json.loads(body.decode("utf-8"))
        print(f"\n[Process 3] [Flow 2 PICKUP & REFLECT] Received item #{data.get('item_id')} (status={data.get('examined_status')}, value={data.get('value')})")

        # Modify slightly before reflecting back to queue
        data["verified_by"] = "process3"
        data["completed"] = True
        data["history"].append(
            {
                "stage": "process3_reflected",
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
                "modification": "Added verification seal and marked completed",
            }
        )

        reflected_body = json.dumps(data, indent=2)
        ch.basic_publish(
            exchange="",
            routing_key=QUEUE_FLOW2_P3_REFLECTED,
            body=reflected_body,
            properties=pika.BasicProperties(
                delivery_mode=pika.DeliveryMode.Persistent,
                content_type="application/json",
            ),
        )
        print(f"[Process 3] [Flow 2] Reflected modified data back to queue '{QUEUE_FLOW2_P3_REFLECTED}'")
        ch.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        print(f"[Process 3] [Flow 2] Error: {e}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)


def main():
    print("[Process 3] Starting consumer & reflector...")
    connection = get_connection()
    channel = connection.channel()
    setup_queues(channel)

    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(
        queue=QUEUE_FLOW1_P2_TO_P3,
        on_message_callback=on_flow1_message,
        auto_ack=False,
    )
    channel.basic_consume(
        queue=QUEUE_FLOW2_P2_TO_P3,
        on_message_callback=on_flow2_message,
        auto_ack=False,
    )

    print(f"[Process 3] Listening on queues: '{QUEUE_FLOW1_P2_TO_P3}' and '{QUEUE_FLOW2_P2_TO_P3}'")
    print(f"[Process 3] Forwarding Flow 1 to: '{QUEUE_FLOW1_P3_TO_P4}'")
    print(f"[Process 3] Reflecting Flow 2 to: '{QUEUE_FLOW2_P3_REFLECTED}'")
    print("[Process 3] Press Ctrl+C to exit.")
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        print("\n[Process 3] Stopping consumer...")
        channel.stop_consuming()
    finally:
        connection.close()
        print("[Process 3] Connection closed.")


if __name__ == "__main__":
    main()
