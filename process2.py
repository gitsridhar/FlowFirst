import json
import time
import pika
from config import (
    get_connection,
    setup_queues,
    QUEUE_FLOW1_P1_TO_P2,
    QUEUE_FLOW1_P2_TO_P3,
    QUEUE_FLOW2_P1_TO_P2,
    QUEUE_FLOW2_P2_TO_P3,
)


def on_flow1_message(ch, method, properties, body):
    """
    Flow 1 handler:
    Process 2 receives data from Process 1, modifies it, and reflects it back
    to the downstream queue (QUEUE_FLOW1_P2_TO_P3) for Process 3.
    """
    try:
        data = json.loads(body.decode("utf-8"))
        print(f"\n[Process 2] [Flow 1] Received from '{QUEUE_FLOW1_P1_TO_P2}': item #{data.get('item_id')}")

        # Modify the data slightly
        data["counter"] = data.get("counter", 0) + 10  # Increment counter
        data["process2_flow1_reflected"] = True
        data["history"].append(
            {
                "stage": "process2_reflected",
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
                "modification": "Added +10 to counter and flagged reflected",
            }
        )

        reflected_body = json.dumps(data, indent=2)
        ch.basic_publish(
            exchange="",
            routing_key=QUEUE_FLOW1_P2_TO_P3,
            body=reflected_body,
            properties=pika.BasicProperties(
                delivery_mode=pika.DeliveryMode.Persistent,
                content_type="application/json",
            ),
        )
        print(f"[Process 2] [Flow 1] Reflected modified data to '{QUEUE_FLOW1_P2_TO_P3}': (new counter={data['counter']})")
        ch.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        print(f"[Process 2] [Flow 1] Error processing message: {e}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)


def on_flow2_message(ch, method, properties, body):
    """
    Flow 2 handler:
    Process 2 receives data from Process 1, examines it, modifies/enriches it,
    and pushes it to QUEUE_FLOW2_P2_TO_P3 where Process 3 picks it up.
    """
    try:
        data = json.loads(body.decode("utf-8"))
        val = data.get("value", 0.0)
        print(f"\n[Process 2] [Flow 2] Received from '{QUEUE_FLOW2_P1_TO_P2}': item #{data.get('item_id')} (value={val})")

        # Examine and enrich/modify the data
        status = "HIGH" if val > 30.0 else "NORMAL"
        data["examined_status"] = status
        data["value"] = round(val * 1.15, 2)  # 15% adjustment after examination
        data["history"].append(
            {
                "stage": "process2_examined_and_forwarded",
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
                "status_assigned": status,
                "modification": "Applied 15% scaling factor and assigned status",
            }
        )

        forwarded_body = json.dumps(data, indent=2)
        ch.basic_publish(
            exchange="",
            routing_key=QUEUE_FLOW2_P2_TO_P3,
            body=forwarded_body,
            properties=pika.BasicProperties(
                delivery_mode=pika.DeliveryMode.Persistent,
                content_type="application/json",
            ),
        )
        print(f"[Process 2] [Flow 2] Forwarded examined data to '{QUEUE_FLOW2_P2_TO_P3}': (status={status}, new_val={data['value']})")
        ch.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        print(f"[Process 2] [Flow 2] Error processing message: {e}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)


def main():
    print("[Process 2] Starting consumer & processor...")

    # Retry loop — Pacemaker may start this process before RabbitMQ is ready.
    connection = None
    for attempt in range(1, 11):
        try:
            connection = get_connection()
            break
        except Exception as e:
            print(f"[Process 2] RabbitMQ not ready (attempt {attempt}/10): {e} — retrying in 5s...")
            import time as _time; _time.sleep(5)
    if connection is None:
        print("[Process 2] Could not connect to RabbitMQ after 10 attempts. Exiting.")
        raise SystemExit(1)

    print(f"[Process 2] Connected to RabbitMQ at {connection._connected_to}")
    channel = connection.channel()
    setup_queues(channel)

    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(
        queue=QUEUE_FLOW1_P1_TO_P2,
        on_message_callback=on_flow1_message,
        auto_ack=False,
    )
    channel.basic_consume(
        queue=QUEUE_FLOW2_P1_TO_P2,
        on_message_callback=on_flow2_message,
        auto_ack=False,
    )

    print(f"[Process 2] Listening on queues: '{QUEUE_FLOW1_P1_TO_P2}' and '{QUEUE_FLOW2_P1_TO_P2}'")
    print("[Process 2] Press Ctrl+C to exit.")
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        print("\n[Process 2] Stopping consumer...")
        channel.stop_consuming()
    finally:
        connection.close()
        print("[Process 2] Connection closed.")


if __name__ == "__main__":
    main()
