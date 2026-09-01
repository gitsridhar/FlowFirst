import json
import time
import pika
from config import (
    NODE_NAME,
    get_connection,
    setup_queues,
    QUEUE_FLOW1_P1_TO_P2,
    QUEUE_FLOW1_P2_TO_P3,
    QUEUE_FLOW2_P1_TO_P2,
    QUEUE_FLOW2_P2_TO_P3,
)
import zk as zklib

# ZooKeeper config watcher — hot-reloads thresholds and scale factors
_config_watcher = None


def on_flow1_message(ch, method, properties, body):
    """
    Flow 1 handler:
    Process 2 receives data from Process 1, modifies it (counter += step from ZK),
    and reflects it to QUEUE_FLOW1_P2_TO_P3 for Process 3.
    """
    try:
        data = json.loads(body.decode("utf-8"))
        print(f"\n[Process 2] [Flow 1] Received from '{QUEUE_FLOW1_P1_TO_P2}': item #{data.get('item_id')}")

        # Read live counter step from ZooKeeper (hot-reloadable, default 10)
        counter_step = _config_watcher.get_int("flow1_counter_step") if _config_watcher else 10

        data["counter"] = data.get("counter", 0) + counter_step
        data["process2_flow1_reflected"] = True
        data["history"].append(
            {
                "stage": "process2_reflected",
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
                "processed_by": NODE_NAME,
                "modification": f"Added +{counter_step} to counter (step from ZK config)",
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
        print(f"[Process 2] [Flow 1] Reflected to '{QUEUE_FLOW1_P2_TO_P3}': (new counter={data['counter']}, step={counter_step})")
        ch.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        print(f"[Process 2] [Flow 1] Error processing message: {e}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)


def on_flow2_message(ch, method, properties, body):
    """
    Flow 2 handler:
    Process 2 examines the value against the ZK-configurable HIGH threshold,
    applies the ZK-configurable scale factor, and forwards to QUEUE_FLOW2_P2_TO_P3.
    """
    try:
        data = json.loads(body.decode("utf-8"))
        val = data.get("value", 0.0)
        print(f"\n[Process 2] [Flow 2] Received from '{QUEUE_FLOW2_P1_TO_P2}': item #{data.get('item_id')} (value={val})")

        # Read live thresholds from ZooKeeper
        high_threshold = _config_watcher.get_float("flow2_high_threshold") if _config_watcher else 30.0
        scale_factor   = _config_watcher.get_float("flow2_scale_factor")   if _config_watcher else 1.15

        status = "HIGH" if val > high_threshold else "NORMAL"
        data["examined_status"] = status
        data["value"] = round(val * scale_factor, 2)
        data["history"].append(
            {
                "stage": "process2_examined_and_forwarded",
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
                "processed_by": NODE_NAME,
                "status_assigned": status,
                "modification": f"Applied {scale_factor}x scale (threshold={high_threshold}, from ZK config)",
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
        print(f"[Process 2] [Flow 2] Forwarded to '{QUEUE_FLOW2_P2_TO_P3}': (status={status}, new_val={data['value']}, scale={scale_factor})")
        ch.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        print(f"[Process 2] [Flow 2] Error processing message: {e}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)


def _consume(connection):
    """Active consumer loop — runs only on the elected leader node."""
    channel = connection.channel()
    setup_queues(channel)
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue=QUEUE_FLOW1_P1_TO_P2, on_message_callback=on_flow1_message, auto_ack=False)
    channel.basic_consume(queue=QUEUE_FLOW2_P1_TO_P2, on_message_callback=on_flow2_message, auto_ack=False)

    print(f"[Process 2] LEADER — consuming '{QUEUE_FLOW1_P1_TO_P2}' and '{QUEUE_FLOW2_P1_TO_P2}'")
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        print("\n[Process 2] Stopping consumer...")
        channel.stop_consuming()
    finally:
        connection.close()
        print("[Process 2] Connection closed.")


def main():
    global _config_watcher

    print("[Process 2] Starting consumer & processor...")
    print(f"[Process 2] Node: {NODE_NAME} — connecting to ZooKeeper...")

    # --- ZooKeeper: connect, register, watch config ---
    try:
        zklib.register_service("process2", NODE_NAME)
        _config_watcher = zklib.ConfigWatcher()
        print(f"[Process 2] ZooKeeper ready. Live config: {_config_watcher.get_all()}")
    except Exception as e:
        print(f"[Process 2] WARNING: ZooKeeper unavailable ({e}) — continuing without ZK features.")
        _config_watcher = None

    # --- RabbitMQ connection with retry ---
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

    # --- Leader election: only the elected leader consumes ---
    print(f"[Process 2] Entering ZooKeeper leader election...")
    try:
        election = zklib.LeaderElection("process2", NODE_NAME)
        election.run(lambda: _consume(connection))
    except Exception as e:
        print(f"[Process 2] ZooKeeper election failed ({e}) — running without election (single-node mode).")
        _consume(connection)
    finally:
        zklib.close_client()


if __name__ == "__main__":
    main()
