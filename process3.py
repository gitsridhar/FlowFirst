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
    QUEUE_FLOW1_P2_TO_P3,
    QUEUE_FLOW1_P3_TO_P4,
    QUEUE_FLOW2_P2_TO_P3,
    QUEUE_FLOW2_P3_REFLECTED,
)
import zk as zklib


def handle_flow1_message(ch, method, properties, body):
    """
    Flow 1 handler — runs inside the flow1 consumer greenthread.
    Process 3 adds audit note and forwards to Process 4.
    """
    try:
        data = json.loads(body.decode("utf-8"))
        print(f"\n[P3][gt] [Flow 1] Picked up item #{data.get('item_id')} (counter={data.get('counter')})")

        data["process3_acknowledged"] = True
        data["history"].append({
            "stage": "process3_received_and_forwarded",
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "processed_by": NODE_NAME,
            "status": "forwarded_to_process4",
        })

        ch.basic_publish(
            exchange="",
            routing_key=QUEUE_FLOW1_P3_TO_P4,
            body=json.dumps(data, indent=2),
            properties=pika.BasicProperties(
                delivery_mode=pika.DeliveryMode.Persistent,
                content_type="application/json",
            ),
        )
        gt.metrics_counter["flow1_forwarded"] += 1
        print(f"[P3][gt] [Flow 1] Forwarded item #{data.get('item_id')} to '{QUEUE_FLOW1_P3_TO_P4}'")
        ch.basic_ack(delivery_tag=method.delivery_tag)
        gt.sleep(0)
    except Exception as e:
        print(f"[P3][gt] [Flow 1] Error: {e}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)


def handle_flow2_message(ch, method, properties, body):
    """
    Flow 2 handler — runs inside the flow2 consumer greenthread.
    Process 3 verifies and reflects back to QUEUE_FLOW2_P3_REFLECTED for Process 4.
    """
    try:
        data = json.loads(body.decode("utf-8"))
        print(f"\n[P3][gt] [Flow 2] Received item #{data.get('item_id')} "
              f"(status={data.get('examined_status')}, value={data.get('value')})")

        data["verified_by"] = "process3"
        data["completed"]   = True
        data["history"].append({
            "stage": "process3_reflected",
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "processed_by": NODE_NAME,
            "modification": "Added verification seal and marked completed",
        })

        ch.basic_publish(
            exchange="",
            routing_key=QUEUE_FLOW2_P3_REFLECTED,
            body=json.dumps(data, indent=2),
            properties=pika.BasicProperties(
                delivery_mode=pika.DeliveryMode.Persistent,
                content_type="application/json",
            ),
        )
        gt.metrics_counter["flow2_reflected"] += 1
        print(f"[P3][gt] [Flow 2] Reflected to '{QUEUE_FLOW2_P3_REFLECTED}'")
        ch.basic_ack(delivery_tag=method.delivery_tag)
        gt.sleep(0)
    except Exception as e:
        print(f"[P3][gt] [Flow 2] Error: {e}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)


def _gt_consume_flow1():
    """Greenthread: dedicated consumer for QUEUE_FLOW1_P2_TO_P3 (own connection)."""
    conn = gt.get_rmq_connection_with_retry("p3-flow1")
    ch   = conn.channel()
    setup_queues(ch)
    ch.basic_qos(prefetch_count=1)
    ch.basic_consume(queue=QUEUE_FLOW1_P2_TO_P3, on_message_callback=handle_flow1_message, auto_ack=False)
    print(f"[P3][gt] flow1-consumer greenthread started — consuming '{QUEUE_FLOW1_P2_TO_P3}'")
    try:
        while not gt.is_stopping():
            conn.process_data_events(time_limit=1)
            gt.sleep(0)
    finally:
        conn.close()
        print("[P3][gt] flow1-consumer greenthread stopped.")


def _gt_consume_flow2():
    """Greenthread: dedicated consumer for QUEUE_FLOW2_P2_TO_P3 (own connection)."""
    conn = gt.get_rmq_connection_with_retry("p3-flow2")
    ch   = conn.channel()
    setup_queues(ch)
    ch.basic_qos(prefetch_count=1)
    ch.basic_consume(queue=QUEUE_FLOW2_P2_TO_P3, on_message_callback=handle_flow2_message, auto_ack=False)
    print(f"[P3][gt] flow2-consumer greenthread started — consuming '{QUEUE_FLOW2_P2_TO_P3}'")
    try:
        while not gt.is_stopping():
            conn.process_data_events(time_limit=1)
            gt.sleep(0)
    finally:
        conn.close()
        print("[P3][gt] flow2-consumer greenthread stopped.")


def _gt_zk_health_reporter():
    while not gt.is_stopping():
        try:
            zklib._update_health("process3", NODE_NAME, "leader")
        except Exception:
            pass
        gt.sleep(30)


def _consume():
    """Leader callback: launch all greenthreads for Process 3."""
    gt.register_signal_handlers()

    gt.spawn("p3_flow1_consumer", _gt_consume_flow1, restart_on_error=True)
    gt.spawn("p3_flow2_consumer", _gt_consume_flow2, restart_on_error=True)
    gt.periodic("p3_zk_health",  _gt_zk_health_reporter, 30)
    gt.start_metrics_reporter("process3")

    print(f"[P3][gt] All greenthreads started: {gt.list_greenthreads()}")
    print(f"[P3][gt] Forwarding Flow 1 → '{QUEUE_FLOW1_P3_TO_P4}'")
    print(f"[P3][gt] Reflecting Flow 2 → '{QUEUE_FLOW2_P3_REFLECTED}'")

    gt.stop_event.wait()
    print("[P3][gt] Shutdown signal — waiting for greenthreads to finish...")
    gt.wait_all()


def main():
    print(f"[P3][gt] Starting (node={NODE_NAME}, pool={gt.GT_POOL_SIZE}) ...")

    try:
        zklib.register_service("process3", NODE_NAME)
        print(f"[P3][gt] ZK service registered.")
    except Exception as e:
        print(f"[P3][gt] WARNING: ZK unavailable ({e}) — degraded mode.")

    try:
        election = zklib.LeaderElection("process3", NODE_NAME)
        election.run(_consume)
    except Exception as e:
        print(f"[P3][gt] ZK election failed ({e}) — single-node mode.")
        _consume()
    finally:
        zklib.close_client()


if __name__ == "__main__":
    main()
