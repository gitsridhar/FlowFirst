#!/usr/bin/env python3
"""Inspect and replay messages from the FlowFirst dead-letter queue.

    python3 scripts/replay_dead_letters.py                 list what is waiting (read-only: nothing is removed)
    python3 scripts/replay_dead_letters.py --replay        send each message back to the queue it was rejected from
    python3 scripts/replay_dead_letters.py --replay --limit 10
    python3 scripts/replay_dead_letters.py --discard       delete the messages (after you decided they are not worth keeping)

A message lands here when a pipeline process rejected it (nack, requeue=False) AND the policy from
scripts/setup_rabbitmq_dlx.sh is active. Find out WHY first (the process log has the exception) - replaying a message
that fails for a permanent reason just puts it back here.

Replay is safe against duplicates at the database: Process 4 checks a ZooKeeper dedup barrier on message_id, and the
insert is keyed on it. Replay uses publisher confirms: a message is only removed from the dead-letter queue after the
broker confirmed the republish, so a failure here leaves it in the queue.

Connects with the same settings as the pipeline (.env, including TLS and the client certificate).
"""
import argparse
import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

import pika  # noqa: E402
from dotenv import load_dotenv  # noqa: E402

for _candidate in (os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".env"), "/opt/flowfirst/.env"):
    if os.path.isfile(_candidate):
        load_dotenv(_candidate)
        break

from src.config.config import ALL_QUEUES, QUEUE_DEAD_LETTERS, get_connection, setup_queues  # noqa: E402


def origin_queue(properties):
    """Queue the message was rejected from, taken from the broker's x-death header (None if absent/unknown)."""
    for death in (properties.headers or {}).get("x-death", []):
        queue = death.get("queue")
        if queue in ALL_QUEUES:
            return queue
    return None


def reason(properties):
    deaths = (properties.headers or {}).get("x-death", [])
    return deaths[0].get("reason", "?") if deaths else "?"


def describe(body):
    try:
        data = json.loads(body.decode("utf-8"))
        return "message_id=%s item_id=%s flow=%s" % (data.get("message_id"), data.get("item_id"), data.get("flow"))
    except Exception:
        return "unparseable body (%d bytes)" % len(body)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    mode = ap.add_mutually_exclusive_group()
    mode.add_argument("--replay", action="store_true", help="republish each message to its original queue")
    mode.add_argument("--discard", action="store_true", help="delete the messages")
    ap.add_argument("--limit", type=int, default=0, help="handle at most N messages (default: all that are waiting now)")
    args = ap.parse_args()

    conn = get_connection()
    try:
        ch = conn.channel()
        setup_queues(ch)                       # declares the DLX objects too; enables publisher confirms
        waiting = ch.queue_declare(queue=QUEUE_DEAD_LETTERS, passive=True).method.message_count
        todo = min(waiting, args.limit) if args.limit > 0 else waiting
        print("%d message(s) in %s; handling %d (%s)" % (
            waiting, QUEUE_DEAD_LETTERS, todo, "replay" if args.replay else "discard" if args.discard else "list only"))

        done = skipped = 0
        for _ in range(todo):
            method, props, body = ch.basic_get(queue=QUEUE_DEAD_LETTERS, auto_ack=False)
            if method is None:
                break
            queue = origin_queue(props)
            print("  - %s | from %s | reason=%s" % (describe(body), queue or "UNKNOWN", reason(props)))
            if args.replay:
                if queue is None:
                    print("    cannot replay: original queue unknown, left in the dead-letter queue")
                    skipped += 1
                    continue
                try:
                    ch.basic_publish(exchange="", routing_key=queue, body=body, mandatory=True,
                                     properties=pika.BasicProperties(delivery_mode=pika.DeliveryMode.Persistent,
                                                                     content_type="application/json"))
                except (pika.exceptions.NackError, pika.exceptions.UnroutableError) as exc:
                    print("    republish refused by the broker (%s): left in the dead-letter queue" % exc.__class__.__name__)
                    skipped += 1
                    continue
                ch.basic_ack(method.delivery_tag)
                done += 1
            elif args.discard:
                ch.basic_ack(method.delivery_tag)
                done += 1
            # list-only and skipped messages are simply not acknowledged; they are released together below
        # Release everything still unacknowledged in ONE call (nacking one by one would hand the same message back to
        # the next basic_get). delivery_tag 0 + multiple=True means "all outstanding".
        ch.basic_nack(delivery_tag=0, multiple=True, requeue=True)
        if args.replay or args.discard:
            print("done: %d %s, %d left in the queue" % (done, "replayed" if args.replay else "discarded", skipped + waiting - todo))
    finally:
        conn.close()


if __name__ == "__main__":
    main()
