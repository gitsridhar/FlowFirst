import json
import time
import pika
from config import (
    get_connection,
    setup_queues,
    QUEUE_FLOW1_P3_TO_P4,
    QUEUE_FLOW2_P3_REFLECTED,
)
from db import init_database, insert_processed_message


def on_flow1_message(ch, method, properties, body):
    """
    Process 4 handler for Flow 1:
    Receives forwarded message from Process 3, logs the receipt, and saves it to MariaDB.
    """
    try:
        data = json.loads(body.decode("utf-8"))
        print(f"\n[Process 4] [Flow 1 FINAL RECEIVE] Consumed item #{data.get('item_id')} (message_id: {data.get('message_id')})")
        
        # Append process4 persistence step to history
        data["history"].append(
            {
                "stage": "process4_saved_to_mariadb",
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
                "database_action": "INSERT/UPDATE processed_messages",
            }
        )
        
        row_id = insert_processed_message(data)
        print(f"[Process 4] [Flow 1] Successfully persisted to MariaDB (row_id: {row_id})")
        print(f"             Initial Data: {data.get('initial_data')}")
        print(f"             Final Counter: {data.get('counter')}")
        print(f"             Audit Steps: {len(data.get('history', []))} recorded")

        ch.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        print(f"[Process 4] [Flow 1] Database / Processing Error: {e}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)


def on_flow2_message(ch, method, properties, body):
    """
    Process 4 handler for Flow 2:
    Consumes reflected message from Process 3 (QUEUE_FLOW2_P3_REFLECTED) and persists it into MariaDB.
    """
    try:
        data = json.loads(body.decode("utf-8"))
        print(f"\n[Process 4] [Flow 2 FINAL RECEIVE] Consumed reflected item #{data.get('item_id')} (message_id: {data.get('message_id')})")
        
        # Append process4 persistence step to history
        data["history"].append(
            {
                "stage": "process4_saved_to_mariadb",
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
                "database_action": "INSERT/UPDATE processed_messages",
            }
        )

        row_id = insert_processed_message(data)
        print(f"[Process 4] [Flow 2] Successfully persisted to MariaDB (row_id: {row_id})")
        print(f"             Status: {data.get('examined_status')}, Final Metric Value: {data.get('value')}")
        print(f"             Audit Trail:")
        for step in data.get("history", []):
            desc = step.get("modification") or step.get("status") or step.get("database_action")
            print(f"               - [{step.get('stage')}] at {step.get('timestamp')}: {desc}")

        ch.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        print(f"[Process 4] [Flow 2] Database / Processing Error: {e}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)


def main():
    print("[Process 4] Initializing MariaDB schema and starting consumer...")
    try:
        init_database()
        print("[Process 4] MariaDB schema verified/ready.")
    except Exception as e:
        print(f"[Process 4] Warning: Could not auto-initialize MariaDB (is MariaDB running?): {e}")

    # Retry loop — Pacemaker may start this process before RabbitMQ is ready.
    connection = None
    for attempt in range(1, 11):
        try:
            connection = get_connection()
            break
        except Exception as e:
            print(f"[Process 4] RabbitMQ not ready (attempt {attempt}/10): {e} — retrying in 5s...")
            import time as _time; _time.sleep(5)
    if connection is None:
        print("[Process 4] Could not connect to RabbitMQ after 10 attempts. Exiting.")
        raise SystemExit(1)

    print(f"[Process 4] Connected to RabbitMQ at {connection._connected_to}")
    channel = connection.channel()
    setup_queues(channel)

    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(
        queue=QUEUE_FLOW1_P3_TO_P4,
        on_message_callback=on_flow1_message,
        auto_ack=False,
    )
    channel.basic_consume(
        queue=QUEUE_FLOW2_P3_REFLECTED,
        on_message_callback=on_flow2_message,
        auto_ack=False,
    )

    print(f"[Process 4] Listening on queues: '{QUEUE_FLOW1_P3_TO_P4}' and '{QUEUE_FLOW2_P3_REFLECTED}'")
    print("[Process 4] Ready to persist incoming messages into MariaDB 'processed_messages' table.")
    print("[Process 4] Press Ctrl+C to exit.")
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        print("\n[Process 4] Stopping consumer...")
        channel.stop_consuming()
    finally:
        connection.close()
        print("[Process 4] RabbitMQ connection closed.")


if __name__ == "__main__":
    main()
