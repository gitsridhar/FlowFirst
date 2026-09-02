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
    QUEUE_FLOW3_P1_TO_P5,
    QUEUE_FLOW3_P5_TO_P2,
)
import zk as zklib

_config_watcher = None


def handle_flow3_message(ch, method, properties, body):
    """
    Flow 3 handler on Remote Node 4 / Process 5.
    Consumes from flow3_p1_to_p5, modifies/transforms the data,
    appends execution history, and reflects back to RabbitMQ pool on flow3_p5_to_p2.
    """
    try:
        data = json.loads(body.decode("utf-8"))
        item_id = data.get("item_id")
        msg_id = data.get("message_id", "")
        print(f"\n[P5][gt][Remote Node] Consumed Flow 3 item #{item_id} (msg_id={msg_id})")

        remote_step = _config_watcher.get_int("flow3_remote_multiplier") if _config_watcher else 2
        initial_val = data.get("remote_metric", 50.0)
        computed_val = round(initial_val * remote_step + 7.5, 2)

        data["remote_processed"] = True
        data["remote_node"] = NODE_NAME
        data["remote_computed_value"] = computed_val
        data["history"].append({
            "stage": "process5_remote_transformed_and_reflected",
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "processed_by": NODE_NAME,
            "node_type": "remote_node4",
            "modification": f"Applied remote transform (multiplier={remote_step}, computed_value={computed_val})",
        })

        ch.basic_publish(
            exchange="",
            routing_key=QUEUE_FLOW3_P5_TO_P2,
            body=json.dumps(data, indent=2),
            properties=pika.BasicProperties(
                delivery_mode=pika.DeliveryMode.Persistent,
                content_type="application/json",
            ),
        )
        gt.metrics_counter["flow3_processed"] += 1
        print(f"[P5][gt][Remote Node] Reflected to '{QUEUE_FLOW3_P5_TO_P2}' (computed={computed_val})")
        ch.basic_ack(delivery_tag=method.delivery_tag)
        gt.sleep(0)
    except Exception as e:
        print(f"[P5][gt][Remote Node] Error handling flow3 message: {e}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)


def _gt_consume_flow3():
    """Greenthread: dedicated consumer for QUEUE_FLOW3_P1_TO_P5."""
    conn = gt.get_rmq_connection_with_retry("p5-flow3")
    ch = conn.channel()
    setup_queues(ch)
    ch.basic_qos(prefetch_count=1)
    ch.basic_consume(queue=QUEUE_FLOW3_P1_TO_P5, on_message_callback=handle_flow3_message, auto_ack=False)
    print(f"[P5][gt][Remote Node] flow3-consumer greenthread started — consuming '{QUEUE_FLOW3_P1_TO_P5}'")
    try:
        while not gt.is_stopping():
            conn.process_data_events(time_limit=1)
            gt.sleep(0)
    finally:
        conn.close()
        print("[P5][gt][Remote Node] flow3-consumer greenthread stopped.")


def _gt_zk_health_reporter():
    while not gt.is_stopping():
        try:
            zklib._update_health("process5", NODE_NAME, "active")
        except Exception:
            pass
        gt.sleep(30)


def _consume():
    """Start all greenthreads for Process 5."""
    gt.register_signal_handlers()

    # Greenthread 1: Flow 3 Consumer
    gt.spawn("p5_flow3_consumer", _gt_consume_flow3, restart_on_error=True)

    # Greenthread 2: ZooKeeper Health Reporter
    gt.periodic("p5_zk_health", _gt_zk_health_reporter, 30)

    # Greenthread 3: Metrics Reporter
    gt.start_metrics_reporter("process5")

    print(f"[P5][gt][Remote Node] All greenthreads started: {gt.list_greenthreads()}")
    gt.stop_event.wait()
    print("[P5][gt][Remote Node] Shutdown signal received — waiting for greenthreads...")
    gt.wait_all()


def main():
    global _config_watcher
    print(f"[P5][gt][Remote Node] Starting Process 5 on {NODE_NAME} (pool={gt.GT_POOL_SIZE}) ...")

    try:
        zklib.register_service("process5", NODE_NAME)
        _config_watcher = zklib.ConfigWatcher()
        print(f"[P5][gt] ZK service registered.")
    except Exception as e:
        print(f"[P5][gt] WARNING: ZK unavailable ({e}) — standalone mode.")
        _config_watcher = None

    _consume()
    zklib.close_client()


if __name__ == "__main__":
    main()
