# IMPORTANT: monkey_patch() must be the very first call, before any other import.
import gt
gt.monkey_patch()

import json
import time
import pika
import eventlet
from config import (
    NODE_NAME,
    setup_queues,
    QUEUE_FLOW1_P1_TO_P2,
    QUEUE_FLOW1_P2_TO_P3,
    QUEUE_FLOW2_P1_TO_P2,
    QUEUE_FLOW2_P2_TO_P3,
)
import zk as zklib

# ZooKeeper config watcher — hot-reloads thresholds and scale factors
_config_watcher = None


# ---------------------------------------------------------------------------
# Per-queue message handlers (called inside each queue's own greenthread)
# ---------------------------------------------------------------------------

def handle_flow1_message(ch, method, properties, body):
    """
    Flow 1 handler — runs inside the flow1 consumer greenthread.
    Reads counter_step live from ZooKeeper config on every message.
    """
    try:
        data = json.loads(body.decode("utf-8"))
        print(f"\n[P2][gt] [Flow 1] Received item #{data.get('item_id')} from '{QUEUE_FLOW1_P1_TO_P2}'")

        counter_step = _config_watcher.get_int("flow1_counter_step") if _config_watcher else 10

        data["counter"] = data.get("counter", 0) + counter_step
        data["process2_flow1_reflected"] = True
        data["history"].append({
            "stage": "process2_reflected",
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "processed_by": NODE_NAME,
            "modification": f"Added +{counter_step} to counter (step from ZK config)",
        })

        ch.basic_publish(
            exchange="",
            routing_key=QUEUE_FLOW1_P2_TO_P3,
            body=json.dumps(data, indent=2),
            properties=pika.BasicProperties(
                delivery_mode=pika.DeliveryMode.Persistent,
                content_type="application/json",
            ),
        )
        gt.metrics_counter["flow1_processed"] += 1
        print(f"[P2][gt] [Flow 1] Reflected to '{QUEUE_FLOW1_P2_TO_P3}' (counter={data['counter']}, step={counter_step})")
        ch.basic_ack(delivery_tag=method.delivery_tag)
        gt.sleep(0)   # yield — let other greenthreads run
    except Exception as e:
        print(f"[P2][gt] [Flow 1] Error: {e}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)


def handle_flow2_message(ch, method, properties, body):
    """
    Flow 2 handler — runs inside the flow2 consumer greenthread.
    Reads high_threshold and scale_factor live from ZooKeeper config.
    """
    try:
        data = json.loads(body.decode("utf-8"))
        val  = data.get("value", 0.0)
        print(f"\n[P2][gt] [Flow 2] Received item #{data.get('item_id')} (value={val})")

        high_threshold = _config_watcher.get_float("flow2_high_threshold") if _config_watcher else 30.0
        scale_factor   = _config_watcher.get_float("flow2_scale_factor")   if _config_watcher else 1.15

        status          = "HIGH" if val > high_threshold else "NORMAL"
        data["examined_status"] = status
        data["value"]           = round(val * scale_factor, 2)
        data["history"].append({
            "stage": "process2_examined_and_forwarded",
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "processed_by": NODE_NAME,
            "status_assigned": status,
            "modification": f"Applied {scale_factor}x scale (threshold={high_threshold}, from ZK config)",
        })

        ch.basic_publish(
            exchange="",
            routing_key=QUEUE_FLOW2_P2_TO_P3,
            body=json.dumps(data, indent=2),
            properties=pika.BasicProperties(
                delivery_mode=pika.DeliveryMode.Persistent,
                content_type="application/json",
            ),
        )
        gt.metrics_counter["flow2_processed"] += 1
        print(f"[P2][gt] [Flow 2] Forwarded to '{QUEUE_FLOW2_P2_TO_P3}' (status={status}, val={data['value']}, scale={scale_factor})")
        ch.basic_ack(delivery_tag=method.delivery_tag)
        gt.sleep(0)   # yield
    except Exception as e:
        print(f"[P2][gt] [Flow 2] Error: {e}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)


# ---------------------------------------------------------------------------
# Per-queue consumer greenthreads — each owns its own pika connection+channel
# ---------------------------------------------------------------------------

def _gt_consume_flow1():
    """
    Greenthread: dedicated consumer for QUEUE_FLOW1_P1_TO_P2.
    Owns its own RabbitMQ connection — independent of the flow2 consumer.
    Yields after each message so flow2 messages are also processed promptly.
    """
    conn = gt.get_rmq_connection_with_retry("p2-flow1")
    ch   = conn.channel()
    setup_queues(ch)
    ch.basic_qos(prefetch_count=1)
    ch.basic_consume(queue=QUEUE_FLOW1_P1_TO_P2, on_message_callback=handle_flow1_message, auto_ack=False)
    print(f"[P2][gt] flow1-consumer greenthread started — consuming '{QUEUE_FLOW1_P1_TO_P2}'")
    try:
        while not gt.is_stopping():
            conn.process_data_events(time_limit=1)
            gt.sleep(0)   # yield to sibling greenthreads
    finally:
        conn.close()
        print("[P2][gt] flow1-consumer greenthread stopped.")


def _gt_consume_flow2():
    """
    Greenthread: dedicated consumer for QUEUE_FLOW2_P1_TO_P2.
    Owns its own RabbitMQ connection — independent of the flow1 consumer.
    """
    conn = gt.get_rmq_connection_with_retry("p2-flow2")
    ch   = conn.channel()
    setup_queues(ch)
    ch.basic_qos(prefetch_count=1)
    ch.basic_consume(queue=QUEUE_FLOW2_P1_TO_P2, on_message_callback=handle_flow2_message, auto_ack=False)
    print(f"[P2][gt] flow2-consumer greenthread started — consuming '{QUEUE_FLOW2_P1_TO_P2}'")
    try:
        while not gt.is_stopping():
            conn.process_data_events(time_limit=1)
            gt.sleep(0)
    finally:
        conn.close()
        print("[P2][gt] flow2-consumer greenthread stopped.")


def _gt_zk_health_reporter():
    while not gt.is_stopping():
        try:
            zklib._update_health("process2", NODE_NAME, "leader")
        except Exception:
            pass
        gt.sleep(30)


def _consume():
    """Leader callback: launch all greenthreads for Process 2."""
    gt.register_signal_handlers()

    # Greenthread 1: Flow 1 consumer (own connection)
    gt.spawn("p2_flow1_consumer", _gt_consume_flow1, restart_on_error=True)

    # Greenthread 2: Flow 2 consumer (own connection)
    gt.spawn("p2_flow2_consumer", _gt_consume_flow2, restart_on_error=True)

    # Greenthread 3: ZooKeeper health reporter
    gt.periodic("p2_zk_health", _gt_zk_health_reporter, 30)

    # Greenthread 4: Metrics reporter
    gt.start_metrics_reporter("process2")

    print(f"[P2][gt] All greenthreads started: {gt.list_greenthreads()}")

    gt.stop_event.wait()
    print("[P2][gt] Shutdown signal — waiting for greenthreads to finish...")
    gt.wait_all()


def main():
    global _config_watcher

    print(f"[P2][gt] Starting (node={NODE_NAME}, pool={gt.GT_POOL_SIZE}) ...")

    # ZooKeeper: connect, register, config watch
    try:
        zklib.register_service("process2", NODE_NAME)
        _config_watcher = zklib.ConfigWatcher()
        print(f"[P2][gt] ZK ready. Config: {_config_watcher.get_all()}")
    except Exception as e:
        print(f"[P2][gt] WARNING: ZK unavailable ({e}) — degraded mode.")
        _config_watcher = None

    # Leader election
    try:
        election = zklib.LeaderElection("process2", NODE_NAME)
        election.run(_consume)
    except Exception as e:
        print(f"[P2][gt] ZK election failed ({e}) — single-node mode.")
        _consume()
    finally:
        zklib.close_client()


if __name__ == "__main__":
    main()
