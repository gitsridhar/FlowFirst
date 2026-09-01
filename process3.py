import json
import time
import pika
from config import (
    NODE_NAME,
    get_connection,
    setup_queues,
    QUEUE_FLOW1_P2_TO_P3,
    QUEUE_FLOW1_P3_TO_P4,
    QUEUE_FLOW2_P2_TO_P3,
    QUEUE_FLOW2_P3_REFLECTED,
)
import zk as zklib


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
                "processed_by": NODE_NAME,
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
    and reflects it back to the queue (QUEUE_FLOW2_P3_REFLECTED) for Process 4.
    """
    try:
        data = json.loads(body.decode("utf-8"))
        print(f"\n[Process 3] [Flow 2 PICKUP & REFLECT] Received item #{data.get('item_id')} "
              f"(status={data.get('examined_status')}, value={data.get('value')})")

        data["verified_by"] = "process3"
        data["completed"] = True
        data["history"].append(
            {
                "stage": "process3_reflected",
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
                "processed_by": NODE_NAME,
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


def _consume(connection):
    """Active consumer loop — runs only on the elected leader node."""
    channel = connection.channel()
    setup_queues(channel)
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue=QUEUE_FLOW1_P2_TO_P3, on_message_callback=on_flow1_message, auto_ack=False)
    channel.basic_consume(queue=QUEUE_FLOW2_P2_TO_P3, on_message_callback=on_flow2_message, auto_ack=False)

    print(f"[Process 3] LEADER — consuming '{QUEUE_FLOW1_P2_TO_P3}' and '{QUEUE_FLOW2_P2_TO_P3}'")
    print(f"[Process 3] Forwarding Flow 1 to: '{QUEUE_FLOW1_P3_TO_P4}'")
    print(f"[Process 3] Reflecting Flow 2 to: '{QUEUE_FLOW2_P3_REFLECTED}'")
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        print("\n[Process 3] Stopping consumer...")
        channel.stop_consuming()
    finally:
        connection.close()
        print("[Process 3] Connection closed.")


def main():
    print("[Process 3] Starting consumer & reflector...")
    print(f"[Process 3] Node: {NODE_NAME} — connecting to ZooKeeper...")

    # --- ZooKeeper: connect and register ---
    try:
        zklib.register_service("process3", NODE_NAME)
        print(f"[Process 3] ZooKeeper service registered for node '{NODE_NAME}'")
    except Exception as e:
        print(f"[Process 3] WARNING: ZooKeeper unavailable ({e}) — continuing without ZK features.")

    # --- RabbitMQ connection with retry ---
    connection = None
    for attempt in range(1, 11):
        try:
            connection = get_connection()
            break
        except Exception as e:
            print(f"[Process 3] RabbitMQ not ready (attempt {attempt}/10): {e} — retrying in 5s...")
            import time as _time; _time.sleep(5)
    if connection is None:
        print("[Process 3] Could not connect to RabbitMQ after 10 attempts. Exiting.")
        raise SystemExit(1)

    print(f"[Process 3] Connected to RabbitMQ at {connection._connected_to}")

    # --- Leader election: only the elected leader consumes ---
    print(f"[Process 3] Entering ZooKeeper leader election...")
    try:
        election = zklib.LeaderElection("process3", NODE_NAME)
        election.run(lambda: _consume(connection))
    except Exception as e:
        print(f"[Process 3] ZooKeeper election failed ({e}) — running without election (single-node mode).")
        _consume(connection)
    finally:
        zklib.close_client()


if __name__ == "__main__":
    main()
