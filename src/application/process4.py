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
    QUEUE_FLOW1_P3_TO_P4,
    QUEUE_FLOW2_P3_REFLECTED,
    QUEUE_FLOW3_P2_TO_P4,
    QUEUE_FLOW4_P3_TO_P4,
    QUEUE_FLOW5_P3_TO_P4,
    QUEUE_FLOW6_P3_TO_P4,
    QUEUE_FLOW6_P2_TO_P4,
    QUEUE_FLOW7_P3_TO_P4,
    GT_PREFETCH_COUNT,
    GT_CONSUMERS_PER_QUEUE,
)
from src.db.db import init_database, insert_processed_message, upsert_device, soft_delete_device, query_devices
from src.swift import swift_storage
from src.metrics import metrics
from src.zookeeper import zk as zklib


log = logging.getLogger("flowfirst.p4")

def handle_flow1_message(ch, method, properties, body):
    """
    Flow 1 final handler — runs inside the flow1 consumer greenthread.
    ZK dedup barrier prevents double-inserts on Pacemaker failover re-delivery.
    """
    try:
        data   = json.loads(body.decode("utf-8"))
        msg_id = data.get("message_id", "")
        log.info(f"[Flow 1] Consumed item #{data.get('item_id')} (msg_id={msg_id})")

        # ZooKeeper dedup barrier (atomic create — first caller wins)
        if not zklib.check_and_mark_processed(msg_id):
            log.warning(f"[Flow 1] DUPLICATE via ZK dedup — skipping insert for {msg_id}")
            gt.metrics_counter["flow1_dedup_skipped"] += 1
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        data["history"].append({
            "stage": "process4_saved_to_mariadb",
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "persisted_by": NODE_NAME,
            "database_action": "INSERT/UPDATE processed_messages",
        })

        row_id = insert_processed_message(data)
        gt.metrics_counter["flow1_persisted"] += 1

        # Also store payload in Swift Object Storage
        swift_ok, swift_res = swift_storage.put_message_object(data)
        if swift_ok:
            gt.metrics_counter["flow1_swift_stored"] += 1
            log.info(f"[Flow 1] Stored in Swift object storage: {swift_res}")
        else:
            log.warning(f"[Flow 1] Swift store note: {swift_res}")

        log.info(f"[Flow 1] Persisted to MariaDB (row_id={row_id}, "
                 f"counter={data.get('counter')}, steps={len(data.get('history', []))})")
        ch.basic_ack(delivery_tag=method.delivery_tag)
        gt.sleep(0)
    except Exception as e:
        log.error(f"[Flow 1] Error: {e}", exc_info=True)
        gt.metrics_counter["flow1_failed"] += 1
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)


def handle_flow2_message(ch, method, properties, body):
    """
    Flow 2 final handler — runs inside the flow2 consumer greenthread.
    ZK dedup barrier prevents double-inserts.
    """
    try:
        data   = json.loads(body.decode("utf-8"))
        msg_id = data.get("message_id", "")
        log.info(f"[Flow 2] Consumed item #{data.get('item_id')} (msg_id={msg_id})")

        if not zklib.check_and_mark_processed(msg_id):
            log.warning(f"[Flow 2] DUPLICATE via ZK dedup — skipping insert for {msg_id}")
            gt.metrics_counter["flow2_dedup_skipped"] += 1
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        data["history"].append({
            "stage": "process4_saved_to_mariadb",
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "persisted_by": NODE_NAME,
            "database_action": "INSERT/UPDATE processed_messages",
        })

        row_id = insert_processed_message(data)
        gt.metrics_counter["flow2_persisted"] += 1

        # Also store payload in Swift Object Storage
        swift_ok, swift_res = swift_storage.put_message_object(data)
        if swift_ok:
            gt.metrics_counter["flow2_swift_stored"] += 1
            log.info(f"[Flow 2] Stored in Swift object storage: {swift_res}")
        else:
            log.warning(f"[Flow 2] Swift store note: {swift_res}")

        log.info(f"[Flow 2] Persisted (row_id={row_id}, "
                 f"status={data.get('examined_status')}, value={data.get('value')})")
        for step in data.get("history", []):
            desc = step.get("modification") or step.get("status") or step.get("database_action")
            log.debug(f"  [{step.get('stage')}] @ {step.get('timestamp')}: {desc}")
        ch.basic_ack(delivery_tag=method.delivery_tag)
        gt.sleep(0)
    except Exception as e:
        log.error(f"[Flow 2] Error: {e}", exc_info=True)
        gt.metrics_counter["flow2_failed"] += 1
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)


def handle_flow3_message(ch, method, properties, body):
    """
    Flow 3 final handler — runs inside the flow3 consumer greenthread.
    Persists data completed from P1 -> P5 (Node 4) -> P2 -> P4 into MariaDB & Swift.
    """
    try:
        data   = json.loads(body.decode("utf-8"))
        msg_id = data.get("message_id", "")
        log.info(f"[Flow 3 - Remote Pipeline] Consumed item #{data.get('item_id')} (msg_id={msg_id})")

        if not zklib.check_and_mark_processed(msg_id):
            log.warning(f"[Flow 3] DUPLICATE via ZK dedup — skipping insert for {msg_id}")
            gt.metrics_counter["flow3_dedup_skipped"] += 1
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        data["history"].append({
            "stage": "process4_saved_to_mariadb",
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "persisted_by": NODE_NAME,
            "database_action": "INSERT/UPDATE processed_messages (flow3 remote complete)",
        })

        row_id = insert_processed_message(data)
        gt.metrics_counter["flow3_persisted"] += 1

        swift_ok, swift_res = swift_storage.put_message_object(data)
        if swift_ok:
            gt.metrics_counter["flow3_swift_stored"] += 1
            log.info(f"[Flow 3] Stored in Swift object storage: {swift_res}")
        else:
            log.warning(f"[Flow 3] Swift store note: {swift_res}")

        log.info(f"[Flow 3] Persisted to MariaDB (row_id={row_id}, "
                 f"remote_val={data.get('remote_computed_value')}, steps={len(data.get('history', []))})")
        ch.basic_ack(delivery_tag=method.delivery_tag)
        gt.sleep(0)
    except Exception as e:
        log.error(f"[Flow 3] Error: {e}", exc_info=True)
        gt.metrics_counter["flow3_failed"] += 1
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)


def handle_flow4_message(ch, method, properties, body):
    """Flow 4 (create device) final handler — persists new device to MariaDB and Swift."""
    try:
        data   = json.loads(body.decode("utf-8"))
        msg_id = data.get("message_id", "")
        log.info(f"[Flow 4] Persisting new device '{data.get('device_name')}' (msg_id={msg_id})")

        if not zklib.check_and_mark_processed(msg_id):
            log.warning(f"[Flow 4] DUPLICATE via ZK dedup — skipping for {msg_id}")
            gt.metrics_counter["flow4_dedup_skipped"] += 1
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        data["history"].append({
            "stage": "process4_device_created",
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "persisted_by": NODE_NAME,
            "database_action": "upsert_device",
        })

        upsert_device(data)
        insert_processed_message(data)
        gt.metrics_counter["flow4_persisted"] += 1

        swift_ok, swift_res = swift_storage.put_message_object(data)
        if swift_ok:
            gt.metrics_counter["flow4_swift_stored"] += 1
            log.info(f"[Flow 4] Stored in Swift: {swift_res}")
        else:
            log.warning(f"[Flow 4] Swift store note: {swift_res}")

        log.info(f"[Flow 4] Device '{data.get('device_name')}' persisted to MariaDB")
        ch.basic_ack(delivery_tag=method.delivery_tag)
        gt.sleep(0)
    except Exception as e:
        log.error(f"[Flow 4] Error: {e}", exc_info=True)
        gt.metrics_counter["flow4_failed"] += 1
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)


def handle_flow5_message(ch, method, properties, body):
    """Flow 5 (query devices) final handler — queries MariaDB and archives results to Swift."""
    try:
        data   = json.loads(body.decode("utf-8"))
        msg_id = data.get("message_id", "")
        log.info(f"[Flow 5] Executing device query (msg_id={msg_id})")

        if not zklib.check_and_mark_processed(msg_id):
            log.warning(f"[Flow 5] DUPLICATE via ZK dedup — skipping for {msg_id}")
            gt.metrics_counter["flow5_dedup_skipped"] += 1
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        filters = data.get("filters", {})
        results = query_devices(filters)
        data["query_results"] = results
        data["query_result_count"] = len(results)

        data["history"].append({
            "stage": "process4_query_executed",
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "persisted_by": NODE_NAME,
            "result_count": len(results),
        })

        insert_processed_message(data)
        gt.metrics_counter["flow5_persisted"] += 1

        # Archive query result in Swift under a query-specific name
        query_object_name = f"query_{msg_id}.json"
        swift_ok, swift_res = swift_storage.put_message_object({**data, "swift_object_name": query_object_name})
        if swift_ok:
            gt.metrics_counter["flow5_swift_stored"] += 1
            log.info(f"[Flow 5] Query result archived in Swift: {swift_res}")
        else:
            log.warning(f"[Flow 5] Swift archive note: {swift_res}")

        log.info(f"[Flow 5] Query returned {len(results)} devices (msg_id={msg_id})")
        ch.basic_ack(delivery_tag=method.delivery_tag)
        gt.sleep(0)
    except Exception as e:
        log.error(f"[Flow 5] Error: {e}", exc_info=True)
        gt.metrics_counter["flow5_failed"] += 1
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)


def handle_flow6_direct_message(ch, method, properties, body):
    """Flow 6 direct persistence handler (from flow6_p3_to_p4) — upserts device."""
    try:
        data   = json.loads(body.decode("utf-8"))
        msg_id = data.get("message_id", "")
        device_id = data.get("device_id", "")
        log.info(f"[Flow 6 direct] Persisting update for device_id={device_id} (msg_id={msg_id})")

        if not zklib.check_and_mark_processed(msg_id):
            log.warning(f"[Flow 6 direct] DUPLICATE via ZK dedup — skipping for {msg_id}")
            gt.metrics_counter["flow6_dedup_skipped"] += 1
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        # Merge updates into the top-level device fields for upsert
        updates = data.get("updates", {})
        device_data = {**data, **updates}

        data["history"].append({
            "stage": "process4_device_updated_direct",
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "persisted_by": NODE_NAME,
            "database_action": "upsert_device (direct path)",
        })

        upsert_device(device_data)
        insert_processed_message(data)
        gt.metrics_counter["flow6_persisted"] += 1

        swift_ok, swift_res = swift_storage.put_message_object(data)
        if swift_ok:
            gt.metrics_counter["flow6_swift_stored"] += 1
            log.info(f"[Flow 6 direct] Stored in Swift: {swift_res}")
        else:
            log.warning(f"[Flow 6 direct] Swift store note: {swift_res}")

        log.info(f"[Flow 6 direct] Device {device_id} updated in MariaDB")
        ch.basic_ack(delivery_tag=method.delivery_tag)
        gt.sleep(0)
    except Exception as e:
        log.error(f"[Flow 6 direct] Error: {e}", exc_info=True)
        gt.metrics_counter["flow6_failed"] += 1
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)


def handle_flow6_remote_message(ch, method, properties, body):
    """Flow 6 remote return handler (from flow6_p2_to_p4) — idempotent upsert after edge sync."""
    try:
        data   = json.loads(body.decode("utf-8"))
        msg_id = data.get("message_id", "")
        device_id = data.get("device_id", "")
        remote_dedup_key = f"{msg_id}_remote"
        log.info(f"[Flow 6 remote] Persisting remote-sync result for device_id={device_id} (msg_id={msg_id})")

        if not zklib.check_and_mark_processed(remote_dedup_key):
            log.warning(f"[Flow 6 remote] DUPLICATE via ZK dedup — skipping for {remote_dedup_key}")
            gt.metrics_counter["flow6_remote_dedup_skipped"] += 1
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        updates = data.get("updates", {})
        device_data = {**data, **updates}

        data["history"].append({
            "stage": "process4_device_updated_remote",
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "persisted_by": NODE_NAME,
            "database_action": "upsert_device (remote edge-sync path)",
            "edge_sync_token": data.get("edge_sync_token"),
        })

        upsert_device(device_data)
        insert_processed_message({**data, "message_id": remote_dedup_key})
        gt.metrics_counter["flow6_remote_persisted"] += 1

        # Distinguish remote path in Swift by prefixing object name
        swift_data = {**data, "swift_path_prefix": "remote_sync_"}
        swift_ok, swift_res = swift_storage.put_message_object(swift_data)
        if swift_ok:
            gt.metrics_counter["flow6_remote_swift_stored"] += 1
            log.info(f"[Flow 6 remote] Stored remote-sync result in Swift: {swift_res}")
        else:
            log.warning(f"[Flow 6 remote] Swift store note: {swift_res}")

        log.info(f"[Flow 6 remote] Device {device_id} remote-sync persisted (edge_token={data.get('edge_sync_token')})")
        ch.basic_ack(delivery_tag=method.delivery_tag)
        gt.sleep(0)
    except Exception as e:
        log.error(f"[Flow 6 remote] Error: {e}", exc_info=True)
        gt.metrics_counter["flow6_failed"] += 1
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)


def handle_flow7_message(ch, method, properties, body):
    """Flow 7 (soft-delete device) final handler — sets status=decommissioned in MariaDB."""
    try:
        data   = json.loads(body.decode("utf-8"))
        msg_id = data.get("message_id", "")
        device_id = data.get("device_id", "")
        log.info(f"[Flow 7] Soft-deleting device_id={device_id} (msg_id={msg_id})")

        if not zklib.check_and_mark_processed(msg_id):
            log.warning(f"[Flow 7] DUPLICATE via ZK dedup — skipping for {msg_id}")
            gt.metrics_counter["flow7_dedup_skipped"] += 1
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        rows_affected = soft_delete_device(device_id)

        data["history"].append({
            "stage": "process4_device_decommissioned",
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "persisted_by": NODE_NAME,
            "database_action": "soft_delete_device (status=decommissioned)",
            "rows_affected": rows_affected,
        })

        insert_processed_message(data)
        gt.metrics_counter["flow7_persisted"] += 1

        # Store tombstone in Swift
        swift_ok, swift_res = swift_storage.put_message_object(data)
        if swift_ok:
            gt.metrics_counter["flow7_swift_stored"] += 1
            log.info(f"[Flow 7] Tombstone stored in Swift: {swift_res}")
        else:
            log.warning(f"[Flow 7] Swift tombstone note: {swift_res}")

        log.info(f"[Flow 7] Device {device_id} decommissioned (rows_affected={rows_affected})")
        ch.basic_ack(delivery_tag=method.delivery_tag)
        gt.sleep(0)
    except Exception as e:
        log.error(f"[Flow 7] Error: {e}", exc_info=True)
        gt.metrics_counter["flow7_failed"] += 1
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)


def _gt_consume_flow1():
    """Greenthread: dedicated consumer for QUEUE_FLOW1_P3_TO_P4 (own connection)."""
    conn = gt.get_rmq_connection_with_retry("p4-flow1")
    ch   = conn.channel()
    setup_queues(ch)
    ch.basic_qos(prefetch_count=GT_PREFETCH_COUNT)
    ch.basic_consume(queue=QUEUE_FLOW1_P3_TO_P4, on_message_callback=handle_flow1_message, auto_ack=False)
    log.info(f"flow1-consumer greenthread started — consuming '{QUEUE_FLOW1_P3_TO_P4}'")
    try:
        while not gt.is_stopping():
            conn.process_data_events(time_limit=1)
            gt.sleep(0)
    finally:
        conn.close()
        log.info("flow1-consumer greenthread stopped.")


def _gt_consume_flow2():
    """Greenthread: dedicated consumer for QUEUE_FLOW2_P3_REFLECTED (own connection)."""
    conn = gt.get_rmq_connection_with_retry("p4-flow2")
    ch   = conn.channel()
    setup_queues(ch)
    ch.basic_qos(prefetch_count=GT_PREFETCH_COUNT)
    ch.basic_consume(queue=QUEUE_FLOW2_P3_REFLECTED, on_message_callback=handle_flow2_message, auto_ack=False)
    log.info(f"flow2-consumer greenthread started — consuming '{QUEUE_FLOW2_P3_REFLECTED}'")
    try:
        while not gt.is_stopping():
            conn.process_data_events(time_limit=1)
            gt.sleep(0)
    finally:
        conn.close()
        log.info("flow2-consumer greenthread stopped.")


def _gt_consume_flow3():
    """Greenthread: dedicated consumer for QUEUE_FLOW3_P2_TO_P4 (own connection)."""
    conn = gt.get_rmq_connection_with_retry("p4-flow3")
    ch   = conn.channel()
    setup_queues(ch)
    ch.basic_qos(prefetch_count=GT_PREFETCH_COUNT)
    ch.basic_consume(queue=QUEUE_FLOW3_P2_TO_P4, on_message_callback=handle_flow3_message, auto_ack=False)
    log.info(f"flow3-consumer greenthread started — consuming '{QUEUE_FLOW3_P2_TO_P4}'")
    try:
        while not gt.is_stopping():
            conn.process_data_events(time_limit=1)
            gt.sleep(0)
    finally:
        conn.close()
        log.info("flow3-consumer greenthread stopped.")


def _gt_consume_flow4():
    """Greenthread: dedicated consumer for QUEUE_FLOW4_P3_TO_P4 (own connection)."""
    conn = gt.get_rmq_connection_with_retry("p4-flow4")
    ch   = conn.channel()
    setup_queues(ch)
    ch.basic_qos(prefetch_count=GT_PREFETCH_COUNT)
    ch.basic_consume(queue=QUEUE_FLOW4_P3_TO_P4, on_message_callback=handle_flow4_message, auto_ack=False)
    log.info(f"flow4-consumer greenthread started — consuming '{QUEUE_FLOW4_P3_TO_P4}'")
    try:
        while not gt.is_stopping():
            conn.process_data_events(time_limit=1)
            gt.sleep(0)
    finally:
        conn.close()
        log.info("flow4-consumer greenthread stopped.")


def _gt_consume_flow5():
    """Greenthread: dedicated consumer for QUEUE_FLOW5_P3_TO_P4 (own connection)."""
    conn = gt.get_rmq_connection_with_retry("p4-flow5")
    ch   = conn.channel()
    setup_queues(ch)
    ch.basic_qos(prefetch_count=GT_PREFETCH_COUNT)
    ch.basic_consume(queue=QUEUE_FLOW5_P3_TO_P4, on_message_callback=handle_flow5_message, auto_ack=False)
    log.info(f"flow5-consumer greenthread started — consuming '{QUEUE_FLOW5_P3_TO_P4}'")
    try:
        while not gt.is_stopping():
            conn.process_data_events(time_limit=1)
            gt.sleep(0)
    finally:
        conn.close()
        log.info("flow5-consumer greenthread stopped.")


def _gt_consume_flow6_direct():
    """Greenthread: dedicated consumer for QUEUE_FLOW6_P3_TO_P4 (own connection)."""
    conn = gt.get_rmq_connection_with_retry("p4-flow6-direct")
    ch   = conn.channel()
    setup_queues(ch)
    ch.basic_qos(prefetch_count=GT_PREFETCH_COUNT)
    ch.basic_consume(queue=QUEUE_FLOW6_P3_TO_P4, on_message_callback=handle_flow6_direct_message, auto_ack=False)
    log.info(f"flow6-direct-consumer greenthread started — consuming '{QUEUE_FLOW6_P3_TO_P4}'")
    try:
        while not gt.is_stopping():
            conn.process_data_events(time_limit=1)
            gt.sleep(0)
    finally:
        conn.close()
        log.info("flow6-direct-consumer greenthread stopped.")


def _gt_consume_flow6_remote():
    """Greenthread: dedicated consumer for QUEUE_FLOW6_P2_TO_P4 (own connection)."""
    conn = gt.get_rmq_connection_with_retry("p4-flow6-remote")
    ch   = conn.channel()
    setup_queues(ch)
    ch.basic_qos(prefetch_count=GT_PREFETCH_COUNT)
    ch.basic_consume(queue=QUEUE_FLOW6_P2_TO_P4, on_message_callback=handle_flow6_remote_message, auto_ack=False)
    log.info(f"flow6-remote-consumer greenthread started — consuming '{QUEUE_FLOW6_P2_TO_P4}'")
    try:
        while not gt.is_stopping():
            conn.process_data_events(time_limit=1)
            gt.sleep(0)
    finally:
        conn.close()
        log.info("flow6-remote-consumer greenthread stopped.")


def _gt_consume_flow7():
    """Greenthread: dedicated consumer for QUEUE_FLOW7_P3_TO_P4 (own connection)."""
    conn = gt.get_rmq_connection_with_retry("p4-flow7")
    ch   = conn.channel()
    setup_queues(ch)
    ch.basic_qos(prefetch_count=GT_PREFETCH_COUNT)
    ch.basic_consume(queue=QUEUE_FLOW7_P3_TO_P4, on_message_callback=handle_flow7_message, auto_ack=False)
    log.info(f"flow7-consumer greenthread started — consuming '{QUEUE_FLOW7_P3_TO_P4}'")
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
            zklib._update_health("process4", NODE_NAME, "leader")
        except Exception:
            pass
        gt.sleep(30)


def _gt_dedup_reaper():
    """
    Greenthread: periodically cleans up old dedup znodes to keep ZooKeeper tidy.
    Runs every GT_DEDUP_REAP_INTERVAL_S seconds (default 10 minutes).
    Removes /flowfirst/dedup/* znodes older than ZK_DEDUP_TTL_MS milliseconds.
    """
    import os as _os
    reap_interval = int(_os.getenv("GT_DEDUP_REAP_INTERVAL_S", "600"))
    ttl_ms        = int(_os.getenv("ZK_DEDUP_TTL_MS", "300000"))

    while not gt.is_stopping():
        gt.sleep(reap_interval)
        try:
            zk_client = zklib.get_client()
            children  = zk_client.get_children("/flowfirst/dedup")
            now_ms    = int(time.time() * 1000)
            reaped    = 0
            for child in children:
                path = f"/flowfirst/dedup/{child}"
                try:
                    _, stat = zk_client.get(path)
                    age_ms  = now_ms - stat.ctime   # ctime is ms since epoch
                    if age_ms > ttl_ms:
                        zk_client.delete(path)
                        reaped += 1
                except Exception:
                    pass
            if reaped:
                gt.metrics_counter["dedup_reaped"] += reaped
                log.info(f"dedup-reaper: removed {reaped} expired znodes "
                         f"(ttl={ttl_ms}ms, interval={reap_interval}s)")
        except Exception as e:
            log.error(f"dedup-reaper error: {e}", exc_info=True)


def _consume():
    """Leader callback: launch all greenthreads for Process 4."""
    gt.register_signal_handlers()

    # Greenthread 1: Flow 1 consumer (own connection)
    gt.spawn_consumers("p4_flow1_consumer", _gt_consume_flow1, GT_CONSUMERS_PER_QUEUE)

    # Greenthread 2: Flow 2 consumer (own connection)
    gt.spawn_consumers("p4_flow2_consumer", _gt_consume_flow2, GT_CONSUMERS_PER_QUEUE)

    # Greenthread 3: Flow 3 consumer (from Process 2 via Remote Node 4 pipeline)
    gt.spawn_consumers("p4_flow3_consumer", _gt_consume_flow3, GT_CONSUMERS_PER_QUEUE)

    # Flow 4 consumer (create device)
    gt.spawn_consumers("p4_flow4_consumer", _gt_consume_flow4, GT_CONSUMERS_PER_QUEUE)

    # Flow 5 consumer (query devices)
    gt.spawn_consumers("p4_flow5_consumer", _gt_consume_flow5, GT_CONSUMERS_PER_QUEUE)

    # Flow 6 direct consumer (from P3 direct path)
    gt.spawn_consumers("p4_flow6_direct_consumer", _gt_consume_flow6_direct, GT_CONSUMERS_PER_QUEUE)

    # Flow 6 remote consumer (from P2 after edge-sync)
    gt.spawn_consumers("p4_flow6_remote_consumer", _gt_consume_flow6_remote, GT_CONSUMERS_PER_QUEUE)

    # Flow 7 consumer (soft-delete device)
    gt.spawn_consumers("p4_flow7_consumer", _gt_consume_flow7, GT_CONSUMERS_PER_QUEUE)

    # ZooKeeper health reporter
    gt.periodic("p4_zk_health", _gt_zk_health_reporter, 30)

    # Dedup reaper (cleans stale ZK dedup znodes)
    gt.spawn("p4_dedup_reaper", _gt_dedup_reaper, restart_on_error=True)

    # Metrics reporter
    gt.start_metrics_reporter("process4")

    log.info(f"All greenthreads started: {gt.list_greenthreads()}")

    gt.stop_event.wait()
    log.info("Shutdown signal — waiting for greenthreads to finish...")
    gt.wait_all()


def main():
    log.info(f"Starting (node={NODE_NAME}, pool={gt.GT_POOL_SIZE}) ...")
    metrics.start("process4")

    # MariaDB schema init
    try:
        init_database()
        log.info("MariaDB schema ready.")
    except Exception as e:
        log.warning(f"MariaDB init failed ({e}).")

    # ZooKeeper
    try:
        zklib.register_service("process4", NODE_NAME)
        log.info(f"ZK service registered.")
    except Exception as e:
        log.warning(f"ZK unavailable ({e}) — degraded mode.")

    # Leader election
    try:
        election = zklib.LeaderElection("process4", NODE_NAME)
        election.run(_consume)
    except Exception as e:
        log.warning(f"ZK election failed ({e}) — single-node mode.")
        _consume()
    finally:
        zklib.close_client()


if __name__ == "__main__":
    main()
