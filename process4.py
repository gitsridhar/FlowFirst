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
    QUEUE_FLOW1_P3_TO_P4,
    QUEUE_FLOW2_P3_REFLECTED,
    QUEUE_FLOW3_P2_TO_P4,
)
from db import init_database, insert_processed_message
import swift_storage
import zk as zklib


def handle_flow1_message(ch, method, properties, body):
    """
    Flow 1 final handler — runs inside the flow1 consumer greenthread.
    ZK dedup barrier prevents double-inserts on Pacemaker failover re-delivery.
    """
    try:
        data   = json.loads(body.decode("utf-8"))
        msg_id = data.get("message_id", "")
        print(f"\n[P4][gt] [Flow 1] Consumed item #{data.get('item_id')} (msg_id={msg_id})")

        # ZooKeeper dedup barrier (atomic create — first caller wins)
        if not zklib.check_and_mark_processed(msg_id):
            print(f"[P4][gt] [Flow 1] DUPLICATE via ZK dedup — skipping insert for {msg_id}")
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
            print(f"[P4][gt] [Flow 1] Stored in Swift object storage: {swift_res}")
        else:
            print(f"[P4][gt] [Flow 1] Swift store note: {swift_res}")

        print(f"[P4][gt] [Flow 1] Persisted to MariaDB (row_id={row_id}, "
              f"counter={data.get('counter')}, steps={len(data.get('history', []))})")
        ch.basic_ack(delivery_tag=method.delivery_tag)
        gt.sleep(0)
    except Exception as e:
        print(f"[P4][gt] [Flow 1] Error: {e}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)


def handle_flow2_message(ch, method, properties, body):
    """
    Flow 2 final handler — runs inside the flow2 consumer greenthread.
    ZK dedup barrier prevents double-inserts.
    """
    try:
        data   = json.loads(body.decode("utf-8"))
        msg_id = data.get("message_id", "")
        print(f"\n[P4][gt] [Flow 2] Consumed item #{data.get('item_id')} (msg_id={msg_id})")

        if not zklib.check_and_mark_processed(msg_id):
            print(f"[P4][gt] [Flow 2] DUPLICATE via ZK dedup — skipping insert for {msg_id}")
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
            print(f"[P4][gt] [Flow 2] Stored in Swift object storage: {swift_res}")
        else:
            print(f"[P4][gt] [Flow 2] Swift store note: {swift_res}")

        print(f"[P4][gt] [Flow 2] Persisted (row_id={row_id}, "
              f"status={data.get('examined_status')}, value={data.get('value')})")
        for step in data.get("history", []):
            desc = step.get("modification") or step.get("status") or step.get("database_action")
            print(f"  [{step.get('stage')}] @ {step.get('timestamp')}: {desc}")
        ch.basic_ack(delivery_tag=method.delivery_tag)
        gt.sleep(0)
    except Exception as e:
        print(f"[P4][gt] [Flow 2] Error: {e}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)


def handle_flow3_message(ch, method, properties, body):
    """
    Flow 3 final handler — runs inside the flow3 consumer greenthread.
    Persists data completed from P1 -> P5 (Node 4) -> P2 -> P4 into MariaDB & Swift.
    """
    try:
        data   = json.loads(body.decode("utf-8"))
        msg_id = data.get("message_id", "")
        print(f"\n[P4][gt] [Flow 3 - Remote Pipeline] Consumed item #{data.get('item_id')} (msg_id={msg_id})")

        if not zklib.check_and_mark_processed(msg_id):
            print(f"[P4][gt] [Flow 3] DUPLICATE via ZK dedup — skipping insert for {msg_id}")
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
            print(f"[P4][gt] [Flow 3] Stored in Swift object storage: {swift_res}")
        else:
            print(f"[P4][gt] [Flow 3] Swift store note: {swift_res}")

        print(f"[P4][gt] [Flow 3] Persisted to MariaDB (row_id={row_id}, "
              f"remote_val={data.get('remote_computed_value')}, steps={len(data.get('history', []))})")
        ch.basic_ack(delivery_tag=method.delivery_tag)
        gt.sleep(0)
    except Exception as e:
        print(f"[P4][gt] [Flow 3] Error: {e}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)


def _gt_consume_flow1():
    """Greenthread: dedicated consumer for QUEUE_FLOW1_P3_TO_P4 (own connection)."""
    conn = gt.get_rmq_connection_with_retry("p4-flow1")
    ch   = conn.channel()
    setup_queues(ch)
    ch.basic_qos(prefetch_count=1)
    ch.basic_consume(queue=QUEUE_FLOW1_P3_TO_P4, on_message_callback=handle_flow1_message, auto_ack=False)
    print(f"[P4][gt] flow1-consumer greenthread started — consuming '{QUEUE_FLOW1_P3_TO_P4}'")
    try:
        while not gt.is_stopping():
            conn.process_data_events(time_limit=1)
            gt.sleep(0)
    finally:
        conn.close()
        print("[P4][gt] flow1-consumer greenthread stopped.")


def _gt_consume_flow2():
    """Greenthread: dedicated consumer for QUEUE_FLOW2_P3_REFLECTED (own connection)."""
    conn = gt.get_rmq_connection_with_retry("p4-flow2")
    ch   = conn.channel()
    setup_queues(ch)
    ch.basic_qos(prefetch_count=1)
    ch.basic_consume(queue=QUEUE_FLOW2_P3_REFLECTED, on_message_callback=handle_flow2_message, auto_ack=False)
    print(f"[P4][gt] flow2-consumer greenthread started — consuming '{QUEUE_FLOW2_P3_REFLECTED}'")
    try:
        while not gt.is_stopping():
            conn.process_data_events(time_limit=1)
            gt.sleep(0)
    finally:
        conn.close()
        print("[P4][gt] flow2-consumer greenthread stopped.")


def _gt_consume_flow3():
    """Greenthread: dedicated consumer for QUEUE_FLOW3_P2_TO_P4 (own connection)."""
    conn = gt.get_rmq_connection_with_retry("p4-flow3")
    ch   = conn.channel()
    setup_queues(ch)
    ch.basic_qos(prefetch_count=1)
    ch.basic_consume(queue=QUEUE_FLOW3_P2_TO_P4, on_message_callback=handle_flow3_message, auto_ack=False)
    print(f"[P4][gt] flow3-consumer greenthread started — consuming '{QUEUE_FLOW3_P2_TO_P4}'")
    try:
        while not gt.is_stopping():
            conn.process_data_events(time_limit=1)
            gt.sleep(0)
    finally:
        conn.close()
        print("[P4][gt] flow3-consumer greenthread stopped.")


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
                print(f"[P4][gt] dedup-reaper: removed {reaped} expired znodes "
                      f"(ttl={ttl_ms}ms, interval={reap_interval}s)")
        except Exception as e:
            print(f"[P4][gt] dedup-reaper error: {e}")


def _consume():
    """Leader callback: launch all greenthreads for Process 4."""
    gt.register_signal_handlers()

    # Greenthread 1: Flow 1 consumer (own connection)
    gt.spawn("p4_flow1_consumer", _gt_consume_flow1, restart_on_error=True)

    # Greenthread 2: Flow 2 consumer (own connection)
    gt.spawn("p4_flow2_consumer", _gt_consume_flow2, restart_on_error=True)

    # Greenthread 3: Flow 3 consumer (from Process 2 via Remote Node 4 pipeline)
    gt.spawn("p4_flow3_consumer", _gt_consume_flow3, restart_on_error=True)

    # Greenthread 4: ZooKeeper health reporter
    gt.periodic("p4_zk_health", _gt_zk_health_reporter, 30)

    # Greenthread 5: Dedup reaper (cleans stale ZK dedup znodes)
    gt.spawn("p4_dedup_reaper", _gt_dedup_reaper, restart_on_error=True)

    # Greenthread 6: Metrics reporter
    gt.start_metrics_reporter("process4")

    print(f"[P4][gt] All greenthreads started: {gt.list_greenthreads()}")

    gt.stop_event.wait()
    print("[P4][gt] Shutdown signal — waiting for greenthreads to finish...")
    gt.wait_all()


def main():
    print(f"[P4][gt] Starting (node={NODE_NAME}, pool={gt.GT_POOL_SIZE}) ...")

    # MariaDB schema init
    try:
        init_database()
        print("[P4][gt] MariaDB schema ready.")
    except Exception as e:
        print(f"[P4][gt] WARNING: MariaDB init failed ({e}).")

    # ZooKeeper
    try:
        zklib.register_service("process4", NODE_NAME)
        print(f"[P4][gt] ZK service registered.")
    except Exception as e:
        print(f"[P4][gt] WARNING: ZK unavailable ({e}) — degraded mode.")

    # Leader election
    try:
        election = zklib.LeaderElection("process4", NODE_NAME)
        election.run(_consume)
    except Exception as e:
        print(f"[P4][gt] ZK election failed ({e}) — single-node mode.")
        _consume()
    finally:
        zklib.close_client()


if __name__ == "__main__":
    main()
