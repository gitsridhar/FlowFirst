# IMPORTANT: monkey_patch() must be the very first call, before any other import.
from src.gt import gt
gt.monkey_patch()

import json
import logging
import time
import pika
import eventlet
import hashlib
from src.config.config import (
    NODE_NAME,
    setup_queues,
    QUEUE_FLOW3_P1_TO_P5,
    QUEUE_FLOW3_P5_TO_P2,
    QUEUE_FLOW6_P3_TO_P5,
    QUEUE_FLOW6_P5_TO_P2,
    GT_PREFETCH_COUNT,
    GT_CONSUMERS_PER_QUEUE,
)
from src.metrics import metrics
from src.zookeeper import zk as zklib

_config_watcher = None


log = logging.getLogger("flowfirst.p5")

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
        log.info(f"[Remote Node] Consumed Flow 3 item #{item_id} (msg_id={msg_id})")

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
            mandatory=True,
            routing_key=QUEUE_FLOW3_P5_TO_P2,
            body=json.dumps(data, indent=2),
            properties=pika.BasicProperties(
                delivery_mode=pika.DeliveryMode.Persistent,
                content_type="application/json",
            ),
        )
        gt.metrics_counter["flow3_processed"] += 1
        log.info(f"[Remote Node] Reflected to '{QUEUE_FLOW3_P5_TO_P2}' (computed={computed_val})")
        ch.basic_ack(delivery_tag=method.delivery_tag)
        gt.sleep(0)
    except Exception as e:
        log.error(f"[Remote Node] Error handling flow3 message: {e}", exc_info=True)
        gt.metrics_counter["flow3_failed"] += 1
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)


def handle_flow6_message(ch, method, properties, body):
    """Flow 6 remote handler on Process 5 (Remote Node 4). Applies edge-sync step."""
    try:
        data = json.loads(body.decode("utf-8"))
        device_id = data.get("device_id", "")
        update_seq = data.get("update_sequence", 0)
        msg_id = data.get("message_id", "")
        log.info(f"[Remote Node] Consumed Flow 6 update for device_id={device_id} (msg_id={msg_id})")

        token_src = f"{device_id}:{update_seq}"
        edge_sync_token = hashlib.sha256(token_src.encode()).hexdigest()[:16]

        data["remote_sync_applied"] = True
        data["edge_sync_token"] = edge_sync_token
        data["remote_node"] = NODE_NAME
        data["history"].append({
            "stage": "process5_edge_sync_applied",
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "processed_by": NODE_NAME,
            "node_type": "remote_node4",
            "modification": f"Edge sync applied (token={edge_sync_token})",
        })

        ch.basic_publish(
            exchange="",
            mandatory=True,
            routing_key=QUEUE_FLOW6_P5_TO_P2,
            body=json.dumps(data, indent=2),
            properties=pika.BasicProperties(
                delivery_mode=pika.DeliveryMode.Persistent,
                content_type="application/json",
            ),
        )
        gt.metrics_counter["flow6_processed"] += 1
        log.info(f"[Remote Node] Flow 6 reflected to '{QUEUE_FLOW6_P5_TO_P2}' (token={edge_sync_token})")
        ch.basic_ack(delivery_tag=method.delivery_tag)
        gt.sleep(0)
    except Exception as e:
        log.error(f"[Remote Node] Error handling flow6 message: {e}", exc_info=True)
        gt.metrics_counter["flow6_failed"] += 1
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)


def _gt_consume_flow6():
    """Greenthread: dedicated consumer for QUEUE_FLOW6_P3_TO_P5."""
    conn = gt.get_rmq_connection_with_retry("p5-flow6")
    ch = conn.channel()
    setup_queues(ch)
    ch.basic_qos(prefetch_count=GT_PREFETCH_COUNT)
    ch.basic_consume(queue=QUEUE_FLOW6_P3_TO_P5, on_message_callback=handle_flow6_message, auto_ack=False)
    log.info(f"[Remote Node] flow6-consumer greenthread started — consuming '{QUEUE_FLOW6_P3_TO_P5}'")
    try:
        while not gt.is_stopping():
            conn.process_data_events(time_limit=1)
            gt.sleep(0)
    finally:
        conn.close()
        log.info("[Remote Node] flow6-consumer greenthread stopped.")


def _gt_consume_flow3():
    """Greenthread: dedicated consumer for QUEUE_FLOW3_P1_TO_P5."""
    conn = gt.get_rmq_connection_with_retry("p5-flow3")
    ch = conn.channel()
    setup_queues(ch)
    ch.basic_qos(prefetch_count=GT_PREFETCH_COUNT)
    ch.basic_consume(queue=QUEUE_FLOW3_P1_TO_P5, on_message_callback=handle_flow3_message, auto_ack=False)
    log.info(f"[Remote Node] flow3-consumer greenthread started — consuming '{QUEUE_FLOW3_P1_TO_P5}'")
    try:
        while not gt.is_stopping():
            conn.process_data_events(time_limit=1)
            gt.sleep(0)
    finally:
        conn.close()
        log.info("[Remote Node] flow3-consumer greenthread stopped.")


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

    # Flow 3 Consumer
    gt.spawn_consumers("p5_flow3_consumer", _gt_consume_flow3, GT_CONSUMERS_PER_QUEUE)

    # Flow 6 Consumer (remote edge-sync for device update)
    gt.spawn_consumers("p5_flow6_consumer", _gt_consume_flow6, GT_CONSUMERS_PER_QUEUE)

    # ZooKeeper Health Reporter
    gt.periodic("p5_zk_health", _gt_zk_health_reporter, 30)

    # Metrics Reporter
    gt.start_metrics_reporter("process5")

    log.info(f"[Remote Node] All greenthreads started: {gt.list_greenthreads()}")
    gt.stop_event.wait()
    log.info("[Remote Node] Shutdown signal received — waiting for greenthreads...")
    gt.wait_all()


def main():
    global _config_watcher
    log.info(f"[Remote Node] Starting Process 5 on {NODE_NAME} (pool={gt.GT_POOL_SIZE}) ...")
    metrics.start("process5")

    try:
        zklib.register_service("process5", NODE_NAME)
        _config_watcher = zklib.ConfigWatcher()
        log.info(f"ZK service registered.")
    except Exception as e:
        log.warning(f"ZK unavailable ({e}) — standalone mode.")
        _config_watcher = None

    _consume()
    zklib.close_client()


if __name__ == "__main__":
    main()
