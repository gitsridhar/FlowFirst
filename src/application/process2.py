# IMPORTANT: monkey_patch() must be the very first call, before any other import.
from src.gt import gt
gt.monkey_patch()

import json
import logging
import time
import pika
import eventlet
from src.config.config import (
    NODE_NAME,
    setup_queues,
    QUEUE_FLOW1_P1_TO_P2,
    QUEUE_FLOW1_P2_TO_P3,
    QUEUE_FLOW2_P1_TO_P2,
    QUEUE_FLOW2_P2_TO_P3,
    QUEUE_FLOW3_P5_TO_P2,
    QUEUE_FLOW3_P2_TO_P4,
    QUEUE_FLOW4_P1_TO_P2,
    QUEUE_FLOW4_P2_TO_P3,
    QUEUE_FLOW5_P1_TO_P2,
    QUEUE_FLOW5_P2_TO_P3,
    QUEUE_FLOW6_P1_TO_P2,
    QUEUE_FLOW6_P2_TO_P3,
    QUEUE_FLOW6_P5_TO_P2,
    QUEUE_FLOW6_P2_TO_P4,
    QUEUE_FLOW7_P1_TO_P2,
    QUEUE_FLOW7_P2_TO_P3,
    GT_PREFETCH_COUNT,
    GT_CONSUMERS_PER_QUEUE,
)
from src.metrics import metrics
from src.zookeeper import zk as zklib

# ZooKeeper config watcher — hot-reloads thresholds and scale factors
_config_watcher = None


log = logging.getLogger("flowfirst.p2")

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
        log.info(f"[Flow 1] Received item #{data.get('item_id')} from '{QUEUE_FLOW1_P1_TO_P2}'")

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
            mandatory=True,
            routing_key=QUEUE_FLOW1_P2_TO_P3,
            body=json.dumps(data, indent=2),
            properties=pika.BasicProperties(
                delivery_mode=pika.DeliveryMode.Persistent,
                content_type="application/json",
            ),
        )
        gt.metrics_counter["flow1_processed"] += 1
        log.info(f"[Flow 1] Reflected to '{QUEUE_FLOW1_P2_TO_P3}' (counter={data['counter']}, step={counter_step})")
        ch.basic_ack(delivery_tag=method.delivery_tag)
        gt.sleep(0)   # yield — let other greenthreads run
    except Exception as e:
        log.error(f"[Flow 1] Error: {e}", exc_info=True)
        gt.metrics_counter["flow1_failed"] += 1
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)


def handle_flow2_message(ch, method, properties, body):
    """
    Flow 2 handler — runs inside the flow2 consumer greenthread.
    Reads high_threshold and scale_factor live from ZooKeeper config.
    """
    try:
        data = json.loads(body.decode("utf-8"))
        val  = data.get("value", 0.0)
        log.info(f"[Flow 2] Received item #{data.get('item_id')} (value={val})")

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
            mandatory=True,
            routing_key=QUEUE_FLOW2_P2_TO_P3,
            body=json.dumps(data, indent=2),
            properties=pika.BasicProperties(
                delivery_mode=pika.DeliveryMode.Persistent,
                content_type="application/json",
            ),
        )
        gt.metrics_counter["flow2_processed"] += 1
        log.info(f"[Flow 2] Forwarded to '{QUEUE_FLOW2_P2_TO_P3}' (status={status}, val={data['value']}, scale={scale_factor})")
        ch.basic_ack(delivery_tag=method.delivery_tag)
        gt.sleep(0)   # yield
    except Exception as e:
        log.error(f"[Flow 2] Error: {e}", exc_info=True)
        gt.metrics_counter["flow2_failed"] += 1
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)


def handle_flow3_message(ch, method, properties, body):
    """
    Flow 3 handler on Process 2 (Cluster Nodes 1-3).
    Receives modified data back from Remote Node 4 / Process 5 via RabbitMQ pool,
    evaluates and logs confirmation, and forwards directly to Process 4 on QUEUE_FLOW3_P2_TO_P4.
    """
    try:
        data = json.loads(body.decode("utf-8"))
        item_id = data.get("item_id")
        msg_id = data.get("message_id", "")
        log.info(f"[Flow 3] Received reflected item #{item_id} from Process 5 on Remote Node (msg_id={msg_id})")

        data["process2_verified_remote"] = True
        data["history"].append({
            "stage": "process2_received_from_remote_and_forwarded_to_p4",
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "processed_by": NODE_NAME,
            "status": "remote_flow3_verified",
            "action": f"Received from {data.get('remote_node')} (computed_val={data.get('remote_computed_value')}) and forwarded to Process 4",
        })

        ch.basic_publish(
            exchange="",
            mandatory=True,
            routing_key=QUEUE_FLOW3_P2_TO_P4,
            body=json.dumps(data, indent=2),
            properties=pika.BasicProperties(
                delivery_mode=pika.DeliveryMode.Persistent,
                content_type="application/json",
            ),
        )
        gt.metrics_counter["flow3_processed"] += 1
        log.info(f"[Flow 3] Forwarded to Process 4 via '{QUEUE_FLOW3_P2_TO_P4}'")
        ch.basic_ack(delivery_tag=method.delivery_tag)
        gt.sleep(0)
    except Exception as e:
        log.error(f"[Flow 3] Error: {e}", exc_info=True)
        gt.metrics_counter["flow3_failed"] += 1
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)


def handle_flow4_message(ch, method, properties, body):
    """Flow 4 (create device) handler on Process 2. Enriches device payload with fingerprint."""
    import hashlib
    try:
        data = json.loads(body.decode("utf-8"))
        log.info(f"[Flow 4] Received create-device '{data.get('device_name')}' (msg_id={data.get('message_id')})")

        fingerprint_src = f"{data.get('device_name','')!s}:{data.get('device_type','')!s}:{data.get('location','')!s}"
        data["device_fingerprint"] = hashlib.sha256(fingerprint_src.encode()).hexdigest()
        data["enriched_at"] = time.strftime("%Y-%m-%d %H:%M:%S")
        data["history"].append({
            "stage": "process2_enriched",
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "processed_by": NODE_NAME,
            "enrichment": "device_fingerprint_computed",
        })

        ch.basic_publish(
            exchange="",
            mandatory=True,
            routing_key=QUEUE_FLOW4_P2_TO_P3,
            body=json.dumps(data, indent=2),
            properties=pika.BasicProperties(
                delivery_mode=pika.DeliveryMode.Persistent,
                content_type="application/json",
            ),
        )
        gt.metrics_counter["flow4_processed"] += 1
        log.info(f"[Flow 4] Enriched and forwarded to '{QUEUE_FLOW4_P2_TO_P3}'")
        ch.basic_ack(delivery_tag=method.delivery_tag)
        gt.sleep(0)
    except Exception as e:
        log.error(f"[Flow 4] Error: {e}", exc_info=True)
        gt.metrics_counter["flow4_failed"] += 1
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)


def handle_flow5_message(ch, method, properties, body):
    """Flow 5 (query devices) handler on Process 2. Normalizes filter values."""
    try:
        data = json.loads(body.decode("utf-8"))
        log.info(f"[Flow 5] Received device query (msg_id={data.get('message_id')})")

        filters = data.get("filters", {})
        if isinstance(filters.get("device_type"), str):
            filters["device_type"] = filters["device_type"].lower()
        if isinstance(filters.get("status"), str):
            filters["status"] = filters["status"].lower()
        data["filters"] = filters
        data["query_enriched_at"] = time.strftime("%Y-%m-%d %H:%M:%S")
        data["history"].append({
            "stage": "process2_query_normalized",
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "processed_by": NODE_NAME,
            "enrichment": "filter_values_normalized",
        })

        ch.basic_publish(
            exchange="",
            mandatory=True,
            routing_key=QUEUE_FLOW5_P2_TO_P3,
            body=json.dumps(data, indent=2),
            properties=pika.BasicProperties(
                delivery_mode=pika.DeliveryMode.Persistent,
                content_type="application/json",
            ),
        )
        gt.metrics_counter["flow5_processed"] += 1
        log.info(f"[Flow 5] Normalized and forwarded to '{QUEUE_FLOW5_P2_TO_P3}'")
        ch.basic_ack(delivery_tag=method.delivery_tag)
        gt.sleep(0)
    except Exception as e:
        log.error(f"[Flow 5] Error: {e}", exc_info=True)
        gt.metrics_counter["flow5_failed"] += 1
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)


def handle_flow6_message(ch, method, properties, body):
    """Flow 6 (update device) handler on Process 2. Adds update_sequence counter."""
    try:
        data = json.loads(body.decode("utf-8"))
        log.info(f"[Flow 6] Received update for device_id={data.get('device_id')} (msg_id={data.get('message_id')})")

        update_seq = _config_watcher.get_int("flow6_update_seq") if _config_watcher else int(time.time() * 1000)
        data["update_sequence"] = update_seq
        data["previous_state_tracked"] = True
        data["history"].append({
            "stage": "process2_sequenced",
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "processed_by": NODE_NAME,
            "update_sequence": update_seq,
        })

        ch.basic_publish(
            exchange="",
            mandatory=True,
            routing_key=QUEUE_FLOW6_P2_TO_P3,
            body=json.dumps(data, indent=2),
            properties=pika.BasicProperties(
                delivery_mode=pika.DeliveryMode.Persistent,
                content_type="application/json",
            ),
        )
        gt.metrics_counter["flow6_processed"] += 1
        log.info(f"[Flow 6] Sequenced (seq={update_seq}) and forwarded to '{QUEUE_FLOW6_P2_TO_P3}'")
        ch.basic_ack(delivery_tag=method.delivery_tag)
        gt.sleep(0)
    except Exception as e:
        log.error(f"[Flow 6] Error: {e}", exc_info=True)
        gt.metrics_counter["flow6_failed"] += 1
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)


def handle_flow6_remote_return_message(ch, method, properties, body):
    """Flow 6 remote return handler on Process 2. Forwards P5 result to P4."""
    try:
        data = json.loads(body.decode("utf-8"))
        log.info(f"[Flow 6] Received remote-sync result for device_id={data.get('device_id')} "
                 f"(msg_id={data.get('message_id')})")

        data["process2_remote_return_verified"] = True
        data["history"].append({
            "stage": "process2_received_remote_sync_result",
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "processed_by": NODE_NAME,
            "action": f"Received edge-sync from {data.get('remote_node')} and forwarding to P4",
        })

        ch.basic_publish(
            exchange="",
            mandatory=True,
            routing_key=QUEUE_FLOW6_P2_TO_P4,
            body=json.dumps(data, indent=2),
            properties=pika.BasicProperties(
                delivery_mode=pika.DeliveryMode.Persistent,
                content_type="application/json",
            ),
        )
        gt.metrics_counter["flow6_remote_processed"] += 1
        log.info(f"[Flow 6] Remote-return forwarded to '{QUEUE_FLOW6_P2_TO_P4}'")
        ch.basic_ack(delivery_tag=method.delivery_tag)
        gt.sleep(0)
    except Exception as e:
        log.error(f"[Flow 6] Remote return error: {e}", exc_info=True)
        gt.metrics_counter["flow6_failed"] += 1
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)


def handle_flow7_message(ch, method, properties, body):
    """Flow 7 (soft-delete device) handler on Process 2. Marks deletion metadata."""
    try:
        data = json.loads(body.decode("utf-8"))
        log.info(f"[Flow 7] Received soft-delete for device_id={data.get('device_id')} "
                 f"(msg_id={data.get('message_id')})")

        grace_period_s = _config_watcher.get_int("flow7_grace_period_s") if _config_watcher else 0
        data["deletion_requested_by"] = NODE_NAME
        data["grace_period_s"] = grace_period_s
        data["history"].append({
            "stage": "process2_deletion_marked",
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "processed_by": NODE_NAME,
            "grace_period_s": grace_period_s,
        })

        ch.basic_publish(
            exchange="",
            mandatory=True,
            routing_key=QUEUE_FLOW7_P2_TO_P3,
            body=json.dumps(data, indent=2),
            properties=pika.BasicProperties(
                delivery_mode=pika.DeliveryMode.Persistent,
                content_type="application/json",
            ),
        )
        gt.metrics_counter["flow7_processed"] += 1
        log.info(f"[Flow 7] Marked (grace={grace_period_s}s) and forwarded to '{QUEUE_FLOW7_P2_TO_P3}'")
        ch.basic_ack(delivery_tag=method.delivery_tag)
        gt.sleep(0)
    except Exception as e:
        log.error(f"[Flow 7] Error: {e}", exc_info=True)
        gt.metrics_counter["flow7_failed"] += 1
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
    ch.basic_qos(prefetch_count=GT_PREFETCH_COUNT)
    ch.basic_consume(queue=QUEUE_FLOW1_P1_TO_P2, on_message_callback=handle_flow1_message, auto_ack=False)
    log.info(f"flow1-consumer greenthread started — consuming '{QUEUE_FLOW1_P1_TO_P2}'")
    try:
        while not gt.is_stopping():
            conn.process_data_events(time_limit=1)
            gt.sleep(0)   # yield to sibling greenthreads
    finally:
        conn.close()
        log.info("flow1-consumer greenthread stopped.")


def _gt_consume_flow2():
    """
    Greenthread: dedicated consumer for QUEUE_FLOW2_P1_TO_P2.
    Owns its own RabbitMQ connection — independent of the flow1 consumer.
    """
    conn = gt.get_rmq_connection_with_retry("p2-flow2")
    ch   = conn.channel()
    setup_queues(ch)
    ch.basic_qos(prefetch_count=GT_PREFETCH_COUNT)
    ch.basic_consume(queue=QUEUE_FLOW2_P1_TO_P2, on_message_callback=handle_flow2_message, auto_ack=False)
    log.info(f"flow2-consumer greenthread started — consuming '{QUEUE_FLOW2_P1_TO_P2}'")
    try:
        while not gt.is_stopping():
            conn.process_data_events(time_limit=1)
            gt.sleep(0)
    finally:
        conn.close()
        log.info("flow2-consumer greenthread stopped.")


def _gt_consume_flow3():
    """
    Greenthread: dedicated consumer for QUEUE_FLOW3_P5_TO_P2 (from Remote Node 4).
    Owns its own RabbitMQ connection.
    """
    conn = gt.get_rmq_connection_with_retry("p2-flow3")
    ch   = conn.channel()
    setup_queues(ch)
    ch.basic_qos(prefetch_count=GT_PREFETCH_COUNT)
    ch.basic_consume(queue=QUEUE_FLOW3_P5_TO_P2, on_message_callback=handle_flow3_message, auto_ack=False)
    log.info(f"flow3-consumer greenthread started — consuming '{QUEUE_FLOW3_P5_TO_P2}'")
    try:
        while not gt.is_stopping():
            conn.process_data_events(time_limit=1)
            gt.sleep(0)
    finally:
        conn.close()
        log.info("flow3-consumer greenthread stopped.")


def _gt_consume_flow4():
    """Greenthread: dedicated consumer for QUEUE_FLOW4_P1_TO_P2 (own connection)."""
    conn = gt.get_rmq_connection_with_retry("p2-flow4")
    ch   = conn.channel()
    setup_queues(ch)
    ch.basic_qos(prefetch_count=GT_PREFETCH_COUNT)
    ch.basic_consume(queue=QUEUE_FLOW4_P1_TO_P2, on_message_callback=handle_flow4_message, auto_ack=False)
    log.info(f"flow4-consumer greenthread started — consuming '{QUEUE_FLOW4_P1_TO_P2}'")
    try:
        while not gt.is_stopping():
            conn.process_data_events(time_limit=1)
            gt.sleep(0)
    finally:
        conn.close()
        log.info("flow4-consumer greenthread stopped.")


def _gt_consume_flow5():
    """Greenthread: dedicated consumer for QUEUE_FLOW5_P1_TO_P2 (own connection)."""
    conn = gt.get_rmq_connection_with_retry("p2-flow5")
    ch   = conn.channel()
    setup_queues(ch)
    ch.basic_qos(prefetch_count=GT_PREFETCH_COUNT)
    ch.basic_consume(queue=QUEUE_FLOW5_P1_TO_P2, on_message_callback=handle_flow5_message, auto_ack=False)
    log.info(f"flow5-consumer greenthread started — consuming '{QUEUE_FLOW5_P1_TO_P2}'")
    try:
        while not gt.is_stopping():
            conn.process_data_events(time_limit=1)
            gt.sleep(0)
    finally:
        conn.close()
        log.info("flow5-consumer greenthread stopped.")


def _gt_consume_flow6():
    """Greenthread: dedicated consumer for QUEUE_FLOW6_P1_TO_P2 (own connection)."""
    conn = gt.get_rmq_connection_with_retry("p2-flow6")
    ch   = conn.channel()
    setup_queues(ch)
    ch.basic_qos(prefetch_count=GT_PREFETCH_COUNT)
    ch.basic_consume(queue=QUEUE_FLOW6_P1_TO_P2, on_message_callback=handle_flow6_message, auto_ack=False)
    log.info(f"flow6-consumer greenthread started — consuming '{QUEUE_FLOW6_P1_TO_P2}'")
    try:
        while not gt.is_stopping():
            conn.process_data_events(time_limit=1)
            gt.sleep(0)
    finally:
        conn.close()
        log.info("flow6-consumer greenthread stopped.")


def _gt_consume_flow6_remote_return():
    """Greenthread: dedicated consumer for QUEUE_FLOW6_P5_TO_P2 (own connection)."""
    conn = gt.get_rmq_connection_with_retry("p2-flow6-remote")
    ch   = conn.channel()
    setup_queues(ch)
    ch.basic_qos(prefetch_count=GT_PREFETCH_COUNT)
    ch.basic_consume(queue=QUEUE_FLOW6_P5_TO_P2, on_message_callback=handle_flow6_remote_return_message, auto_ack=False)
    log.info(f"flow6-remote-return-consumer greenthread started — consuming '{QUEUE_FLOW6_P5_TO_P2}'")
    try:
        while not gt.is_stopping():
            conn.process_data_events(time_limit=1)
            gt.sleep(0)
    finally:
        conn.close()
        log.info("flow6-remote-return-consumer greenthread stopped.")


def _gt_consume_flow7():
    """Greenthread: dedicated consumer for QUEUE_FLOW7_P1_TO_P2 (own connection)."""
    conn = gt.get_rmq_connection_with_retry("p2-flow7")
    ch   = conn.channel()
    setup_queues(ch)
    ch.basic_qos(prefetch_count=GT_PREFETCH_COUNT)
    ch.basic_consume(queue=QUEUE_FLOW7_P1_TO_P2, on_message_callback=handle_flow7_message, auto_ack=False)
    log.info(f"flow7-consumer greenthread started — consuming '{QUEUE_FLOW7_P1_TO_P2}'")
    try:
        while not gt.is_stopping():
            conn.process_data_events(time_limit=1)
            gt.sleep(0)
    finally:
        conn.close()
        log.info("flow7-consumer greenthread stopped.")


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
    gt.spawn_consumers("p2_flow1_consumer", _gt_consume_flow1, GT_CONSUMERS_PER_QUEUE)

    # Greenthread 2: Flow 2 consumer (own connection)
    gt.spawn_consumers("p2_flow2_consumer", _gt_consume_flow2, GT_CONSUMERS_PER_QUEUE)

    # Greenthread 3: Flow 3 consumer (from remote Process 5)
    gt.spawn_consumers("p2_flow3_consumer", _gt_consume_flow3, GT_CONSUMERS_PER_QUEUE)

    # Flow 4 consumer (create device)
    gt.spawn_consumers("p2_flow4_consumer", _gt_consume_flow4, GT_CONSUMERS_PER_QUEUE)

    # Flow 5 consumer (query devices)
    gt.spawn_consumers("p2_flow5_consumer", _gt_consume_flow5, GT_CONSUMERS_PER_QUEUE)

    # Flow 6 consumer (update device — from P1)
    gt.spawn_consumers("p2_flow6_consumer", _gt_consume_flow6, GT_CONSUMERS_PER_QUEUE)

    # Flow 6 remote return consumer (from P5 edge-sync)
    gt.spawn_consumers("p2_flow6_remote_consumer", _gt_consume_flow6_remote_return, GT_CONSUMERS_PER_QUEUE)

    # Flow 7 consumer (soft-delete device)
    gt.spawn_consumers("p2_flow7_consumer", _gt_consume_flow7, GT_CONSUMERS_PER_QUEUE)

    # ZooKeeper health reporter
    gt.periodic("p2_zk_health", _gt_zk_health_reporter, 30)

    # Metrics reporter
    gt.start_metrics_reporter("process2")

    log.info(f"All greenthreads started: {gt.list_greenthreads()}")

    gt.stop_event.wait()
    log.info("Shutdown signal — waiting for greenthreads to finish...")
    gt.wait_all()


def main():
    global _config_watcher

    log.info(f"Starting (node={NODE_NAME}, pool={gt.GT_POOL_SIZE}) ...")
    metrics.start("process2")

    # ZooKeeper: connect, register, config watch
    try:
        zklib.register_service("process2", NODE_NAME)
        _config_watcher = zklib.ConfigWatcher()
        log.info(f"ZK ready. Config: {_config_watcher.get_all()}")
    except Exception as e:
        log.warning(f"ZK unavailable ({e}) — degraded mode.")
        _config_watcher = None

    # Leader election
    try:
        election = zklib.LeaderElection("process2", NODE_NAME)
        election.run(_consume)
    except Exception as e:
        log.warning(f"ZK election failed ({e}) — single-node mode.")
        _consume()
    finally:
        zklib.close_client()


if __name__ == "__main__":
    main()
