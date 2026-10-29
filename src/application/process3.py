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
    QUEUE_FLOW1_P2_TO_P3,
    QUEUE_FLOW1_P3_TO_P4,
    QUEUE_FLOW2_P2_TO_P3,
    QUEUE_FLOW2_P3_REFLECTED,
    QUEUE_FLOW4_P2_TO_P3,
    QUEUE_FLOW4_P3_TO_P4,
    QUEUE_FLOW5_P2_TO_P3,
    QUEUE_FLOW5_P3_TO_P4,
    QUEUE_FLOW6_P2_TO_P3,
    QUEUE_FLOW6_P3_TO_P4,
    QUEUE_FLOW6_P3_TO_P5,
    QUEUE_FLOW7_P2_TO_P3,
    QUEUE_FLOW7_P3_TO_P4,
    GT_PREFETCH_COUNT,
    GT_CONSUMERS_PER_QUEUE,
)
from src.metrics import metrics
from src.zookeeper import zk as zklib


log = logging.getLogger("flowfirst.forwarder")

def handle_flow1_message(ch, method, properties, body):
    """
    Flow 1 handler — runs inside the flow1 consumer greenthread.
    Forwarder (Process 3) adds audit note and forwards to DB Updater (Process 4).
    """
    try:
        data = json.loads(body.decode("utf-8"))
        log.info(f"[Flow 1] Picked up item #{data.get('item_id')} (counter={data.get('counter')})")

        data["process3_acknowledged"] = True
        data["history"].append({
            "stage": "process3_received_and_forwarded",
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "processed_by": NODE_NAME,
            "status": "forwarded_to_process4",
        })

        ch.basic_publish(
            exchange="",
            mandatory=True,
            routing_key=QUEUE_FLOW1_P3_TO_P4,
            body=json.dumps(data, indent=2),
            properties=pika.BasicProperties(
                delivery_mode=pika.DeliveryMode.Persistent,
                content_type="application/json",
            ),
        )
        gt.metrics_counter["flow1_forwarded"] += 1
        log.info(f"[Flow 1] Forwarded item #{data.get('item_id')} to '{QUEUE_FLOW1_P3_TO_P4}'")
        ch.basic_ack(delivery_tag=method.delivery_tag)
        gt.sleep(0)
    except Exception as e:
        log.error(f"[Flow 1] Error: {e}", exc_info=True)
        gt.metrics_counter["flow1_failed"] += 1
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)


def handle_flow2_message(ch, method, properties, body):
    """
    Flow 2 handler — runs inside the flow2 consumer greenthread.
    Forwarder (Process 3) verifies and reflects back to QUEUE_FLOW2_P3_REFLECTED for DB Updater (Process 4).
    """
    try:
        data = json.loads(body.decode("utf-8"))
        log.info(f"[Flow 2] Received item #{data.get('item_id')} "
                 f"(status={data.get('examined_status')}, value={data.get('value')})")

        data["verified_by"] = "forwarder"
        data["completed"]   = True
        data["history"].append({
            "stage": "process3_reflected",
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "processed_by": NODE_NAME,
            "modification": "Added verification seal and marked completed",
        })

        ch.basic_publish(
            exchange="",
            mandatory=True,
            routing_key=QUEUE_FLOW2_P3_REFLECTED,
            body=json.dumps(data, indent=2),
            properties=pika.BasicProperties(
                delivery_mode=pika.DeliveryMode.Persistent,
                content_type="application/json",
            ),
        )
        gt.metrics_counter["flow2_reflected"] += 1
        log.info(f"[Flow 2] Reflected to '{QUEUE_FLOW2_P3_REFLECTED}'")
        ch.basic_ack(delivery_tag=method.delivery_tag)
        gt.sleep(0)
    except Exception as e:
        log.error(f"[Flow 2] Error: {e}", exc_info=True)
        gt.metrics_counter["flow2_failed"] += 1
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)


_ALLOWED_DEVICE_TYPES = frozenset({
    "temperature_sensor", "pressure_gauge", "flow_meter",
    "humidity_sensor", "vibration_sensor", "power_monitor",
})


def handle_flow4_message(ch, method, properties, body):
    """Flow 4 (create device) validation handler on Forwarder (Process 3)."""
    try:
        data = json.loads(body.decode("utf-8"))
        device_name = (data.get("device_name") or "").strip()
        device_type = (data.get("device_type") or "").strip()
        log.info(f"[Flow 4] Validating create-device '{device_name}' (type={device_type})")

        if not device_name:
            log.error(f"[Flow 4] Validation failed: device_name is empty (msg_id={data.get('message_id')})")
            gt.metrics_counter["flow4_failed"] += 1
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            return
        if device_type not in _ALLOWED_DEVICE_TYPES:
            log.error(f"[Flow 4] Validation failed: device_type '{device_type}' not in allowed set "
                      f"(msg_id={data.get('message_id')})")
            gt.metrics_counter["flow4_failed"] += 1
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            return

        data["validation_passed"] = True
        data["history"].append({
            "stage": "process3_validated",
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "processed_by": NODE_NAME,
            "validation": "device_name_and_type_valid",
        })

        ch.basic_publish(
            exchange="",
            mandatory=True,
            routing_key=QUEUE_FLOW4_P3_TO_P4,
            body=json.dumps(data, indent=2),
            properties=pika.BasicProperties(
                delivery_mode=pika.DeliveryMode.Persistent,
                content_type="application/json",
            ),
        )
        gt.metrics_counter["flow4_forwarded"] += 1
        log.info(f"[Flow 4] Validated and forwarded to '{QUEUE_FLOW4_P3_TO_P4}'")
        ch.basic_ack(delivery_tag=method.delivery_tag)
        gt.sleep(0)
    except Exception as e:
        log.error(f"[Flow 4] Error: {e}", exc_info=True)
        gt.metrics_counter["flow4_failed"] += 1
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)


def handle_flow5_message(ch, method, properties, body):
    """Flow 5 (query devices) validation handler on Forwarder (Process 3)."""
    _recognized_filter_keys = {"device_type", "status", "owner_team", "location_contains"}
    try:
        data = json.loads(body.decode("utf-8"))
        filters = data.get("filters", {})
        log.info(f"[Flow 5] Validating device query (msg_id={data.get('message_id')})")

        if not filters or not (set(filters.keys()) & _recognized_filter_keys):
            # Accept empty filters (return all devices) — just ensure filters is a dict
            if not isinstance(filters, dict):
                log.error(f"[Flow 5] Validation failed: filters must be a dict (msg_id={data.get('message_id')})")
                gt.metrics_counter["flow5_failed"] += 1
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
                return

        data["query_validated"] = True
        data["history"].append({
            "stage": "process3_query_validated",
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "processed_by": NODE_NAME,
            "validation": "query_filters_valid",
        })

        ch.basic_publish(
            exchange="",
            mandatory=True,
            routing_key=QUEUE_FLOW5_P3_TO_P4,
            body=json.dumps(data, indent=2),
            properties=pika.BasicProperties(
                delivery_mode=pika.DeliveryMode.Persistent,
                content_type="application/json",
            ),
        )
        gt.metrics_counter["flow5_forwarded"] += 1
        log.info(f"[Flow 5] Validated and forwarded to '{QUEUE_FLOW5_P3_TO_P4}'")
        ch.basic_ack(delivery_tag=method.delivery_tag)
        gt.sleep(0)
    except Exception as e:
        log.error(f"[Flow 5] Error: {e}", exc_info=True)
        gt.metrics_counter["flow5_failed"] += 1
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)


def handle_flow6_message(ch, method, properties, body):
    """Flow 6 (update device) validation handler on Forwarder (Process 3). Dual-publishes to P4 and P5."""
    try:
        data = json.loads(body.decode("utf-8"))
        device_id = (data.get("device_id") or "").strip()
        log.info(f"[Flow 6] Validating update for device_id={device_id} (msg_id={data.get('message_id')})")

        if not device_id or len(device_id) != 36:
            log.error(f"[Flow 6] Validation failed: device_id is empty or not UUID format "
                      f"(msg_id={data.get('message_id')})")
            gt.metrics_counter["flow6_failed"] += 1
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            return

        data["validation_passed"] = True
        data["history"].append({
            "stage": "process3_validated",
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "processed_by": NODE_NAME,
            "validation": "device_id_valid_uuid",
        })

        msg_body = json.dumps(data, indent=2)
        props = pika.BasicProperties(
            delivery_mode=pika.DeliveryMode.Persistent,
            content_type="application/json",
        )
        # Dual-publish: direct persistence path AND remote edge-sync fan-out.
        # Both publishes use mandatory=True with publisher confirms.  If the first
        # succeeds but the second raises, the message has already landed on P4's
        # queue — the only safe option is to ack the inbound message so it is not
        # delivered again (which would cause a duplicate insert on P4) and log the
        # P5 publish failure.  The ZK dedup barrier in P4 guards against any
        # duplicate even if the message is somehow re-delivered later.
        p4_published = False
        try:
            ch.basic_publish(exchange="", mandatory=True, routing_key=QUEUE_FLOW6_P3_TO_P4,
                             body=msg_body, properties=props)
            p4_published = True
            ch.basic_publish(exchange="", mandatory=True, routing_key=QUEUE_FLOW6_P3_TO_P5,
                             body=msg_body, properties=props)
        except Exception as pub_exc:
            if p4_published:
                # P4 path delivered; P5 path failed.  Ack so we don't re-deliver to P4.
                log.error(f"[Flow 6] P5 publish failed after P4 succeeded — acking to avoid duplicate P4 insert: {pub_exc}", exc_info=True)
                gt.metrics_counter["flow6_failed"] += 1
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return
            raise   # P4 itself failed — let the outer except handle it
        gt.metrics_counter["flow6_forwarded"] += 1
        log.info(f"[Flow 6] Validated and dual-published to '{QUEUE_FLOW6_P3_TO_P4}' and '{QUEUE_FLOW6_P3_TO_P5}'")
        ch.basic_ack(delivery_tag=method.delivery_tag)
        gt.sleep(0)
    except Exception as e:
        log.error(f"[Flow 6] Error: {e}", exc_info=True)
        gt.metrics_counter["flow6_failed"] += 1
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)


def handle_flow7_message(ch, method, properties, body):
    """Flow 7 (soft-delete device) validation handler on Forwarder (Process 3)."""
    try:
        data = json.loads(body.decode("utf-8"))
        device_id = (data.get("device_id") or "").strip()
        log.info(f"[Flow 7] Validating soft-delete for device_id={device_id} (msg_id={data.get('message_id')})")

        if not device_id:
            log.error(f"[Flow 7] Validation failed: device_id is empty (msg_id={data.get('message_id')})")
            gt.metrics_counter["flow7_failed"] += 1
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            return

        data["deletion_validated"] = True
        data["history"].append({
            "stage": "process3_deletion_validated",
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "processed_by": NODE_NAME,
            "validation": "device_id_present",
        })

        ch.basic_publish(
            exchange="",
            mandatory=True,
            routing_key=QUEUE_FLOW7_P3_TO_P4,
            body=json.dumps(data, indent=2),
            properties=pika.BasicProperties(
                delivery_mode=pika.DeliveryMode.Persistent,
                content_type="application/json",
            ),
        )
        gt.metrics_counter["flow7_forwarded"] += 1
        log.info(f"[Flow 7] Validated and forwarded to '{QUEUE_FLOW7_P3_TO_P4}'")
        ch.basic_ack(delivery_tag=method.delivery_tag)
        gt.sleep(0)
    except Exception as e:
        log.error(f"[Flow 7] Error: {e}", exc_info=True)
        gt.metrics_counter["flow7_failed"] += 1
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)


def _gt_consume_flow1():
    """Greenthread: dedicated consumer for QUEUE_FLOW1_P2_TO_P3 (own connection)."""
    conn = gt.get_rmq_connection_with_retry("p3-flow1")
    ch   = conn.channel()
    setup_queues(ch)
    ch.basic_qos(prefetch_count=GT_PREFETCH_COUNT)
    ch.basic_consume(queue=QUEUE_FLOW1_P2_TO_P3, on_message_callback=handle_flow1_message, auto_ack=False)
    log.info(f"flow1-consumer greenthread started — consuming '{QUEUE_FLOW1_P2_TO_P3}'")
    try:
        while not gt.is_stopping():
            conn.process_data_events(time_limit=1)
            gt.sleep(0)
    finally:
        conn.close()
        log.info("flow1-consumer greenthread stopped.")


def _gt_consume_flow2():
    """Greenthread: dedicated consumer for QUEUE_FLOW2_P2_TO_P3 (own connection)."""
    conn = gt.get_rmq_connection_with_retry("p3-flow2")
    ch   = conn.channel()
    setup_queues(ch)
    ch.basic_qos(prefetch_count=GT_PREFETCH_COUNT)
    ch.basic_consume(queue=QUEUE_FLOW2_P2_TO_P3, on_message_callback=handle_flow2_message, auto_ack=False)
    log.info(f"flow2-consumer greenthread started — consuming '{QUEUE_FLOW2_P2_TO_P3}'")
    try:
        while not gt.is_stopping():
            conn.process_data_events(time_limit=1)
            gt.sleep(0)
    finally:
        conn.close()
        log.info("flow2-consumer greenthread stopped.")


def _gt_consume_flow4():
    """Greenthread: dedicated consumer for QUEUE_FLOW4_P2_TO_P3 (own connection)."""
    conn = gt.get_rmq_connection_with_retry("p3-flow4")
    ch   = conn.channel()
    setup_queues(ch)
    ch.basic_qos(prefetch_count=GT_PREFETCH_COUNT)
    ch.basic_consume(queue=QUEUE_FLOW4_P2_TO_P3, on_message_callback=handle_flow4_message, auto_ack=False)
    log.info(f"flow4-consumer greenthread started — consuming '{QUEUE_FLOW4_P2_TO_P3}'")
    try:
        while not gt.is_stopping():
            conn.process_data_events(time_limit=1)
            gt.sleep(0)
    finally:
        conn.close()
        log.info("flow4-consumer greenthread stopped.")


def _gt_consume_flow5():
    """Greenthread: dedicated consumer for QUEUE_FLOW5_P2_TO_P3 (own connection)."""
    conn = gt.get_rmq_connection_with_retry("p3-flow5")
    ch   = conn.channel()
    setup_queues(ch)
    ch.basic_qos(prefetch_count=GT_PREFETCH_COUNT)
    ch.basic_consume(queue=QUEUE_FLOW5_P2_TO_P3, on_message_callback=handle_flow5_message, auto_ack=False)
    log.info(f"flow5-consumer greenthread started — consuming '{QUEUE_FLOW5_P2_TO_P3}'")
    try:
        while not gt.is_stopping():
            conn.process_data_events(time_limit=1)
            gt.sleep(0)
    finally:
        conn.close()
        log.info("flow5-consumer greenthread stopped.")


def _gt_consume_flow6():
    """Greenthread: dedicated consumer for QUEUE_FLOW6_P2_TO_P3 (own connection)."""
    conn = gt.get_rmq_connection_with_retry("p3-flow6")
    ch   = conn.channel()
    setup_queues(ch)
    ch.basic_qos(prefetch_count=GT_PREFETCH_COUNT)
    ch.basic_consume(queue=QUEUE_FLOW6_P2_TO_P3, on_message_callback=handle_flow6_message, auto_ack=False)
    log.info(f"flow6-consumer greenthread started — consuming '{QUEUE_FLOW6_P2_TO_P3}'")
    try:
        while not gt.is_stopping():
            conn.process_data_events(time_limit=1)
            gt.sleep(0)
    finally:
        conn.close()
        log.info("flow6-consumer greenthread stopped.")


def _gt_consume_flow7():
    """Greenthread: dedicated consumer for QUEUE_FLOW7_P2_TO_P3 (own connection)."""
    conn = gt.get_rmq_connection_with_retry("p3-flow7")
    ch   = conn.channel()
    setup_queues(ch)
    ch.basic_qos(prefetch_count=GT_PREFETCH_COUNT)
    ch.basic_consume(queue=QUEUE_FLOW7_P2_TO_P3, on_message_callback=handle_flow7_message, auto_ack=False)
    log.info(f"flow7-consumer greenthread started — consuming '{QUEUE_FLOW7_P2_TO_P3}'")
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
            zklib._update_health("forwarder", NODE_NAME, "leader")
        except Exception:
            pass
        gt.sleep(30)


def _consume():
    """Leader callback: launch all greenthreads for Forwarder (Process 3)."""
    gt.register_signal_handlers()

    gt.spawn_consumers("p3_flow1_consumer", _gt_consume_flow1, GT_CONSUMERS_PER_QUEUE)
    gt.spawn_consumers("p3_flow2_consumer", _gt_consume_flow2, GT_CONSUMERS_PER_QUEUE)
    gt.spawn_consumers("p3_flow4_consumer", _gt_consume_flow4, GT_CONSUMERS_PER_QUEUE)
    gt.spawn_consumers("p3_flow5_consumer", _gt_consume_flow5, GT_CONSUMERS_PER_QUEUE)
    gt.spawn_consumers("p3_flow6_consumer", _gt_consume_flow6, GT_CONSUMERS_PER_QUEUE)
    gt.spawn_consumers("p3_flow7_consumer", _gt_consume_flow7, GT_CONSUMERS_PER_QUEUE)
    gt.periodic("p3_zk_health",  _gt_zk_health_reporter, 30)
    gt.start_metrics_reporter("forwarder")

    log.info(f"All greenthreads started: {gt.list_greenthreads()}")
    log.info(f"Forwarding Flow 1 → '{QUEUE_FLOW1_P3_TO_P4}'")
    log.info(f"Reflecting Flow 2 → '{QUEUE_FLOW2_P3_REFLECTED}'")

    gt.stop_event.wait()
    log.info("Shutdown signal — waiting for greenthreads to finish...")
    gt.wait_all()


def main():
    log.info(f"Starting (node={NODE_NAME}, pool={gt.GT_POOL_SIZE}) ...")
    metrics.start("forwarder")

    try:
        zklib.register_service("forwarder", NODE_NAME)
        log.info(f"ZK service registered.")
    except Exception as e:
        log.warning(f"ZK unavailable ({e}) — degraded mode.")

    try:
        election = zklib.LeaderElection("forwarder", NODE_NAME)
        election.run(_consume)
    except Exception as e:
        log.warning(f"ZK election failed ({e}) — single-node mode.")
        _consume()
    finally:
        zklib.close_client()


if __name__ == "__main__":
    main()
