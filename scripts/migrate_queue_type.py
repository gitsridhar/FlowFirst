#!/usr/bin/env python3
"""Move the FlowFirst RabbitMQ queues from one queue type to another, keeping every waiting message.

    python3 scripts/migrate_queue_type.py --to quorum --dry-run     show what would happen, change nothing
    python3 scripts/migrate_queue_type.py --to quorum               migrate classic -> quorum (replicated)
    python3 scripts/migrate_queue_type.py --to classic              roll back

WHY A SCRIPT: RabbitMQ refuses to redeclare an existing queue with a different type (PRECONDITION_FAILED), so the type can
only be changed by creating a new queue. For each of the nine pipeline queues and the dead-letter queue this script:
  1. declares a temporary queue "migrate_<name>" of the target type,
  2. moves every message from the old queue into it (publisher confirms; a message is acked off the old queue only after
     the broker confirmed the copy),
  3. deletes the old queue (only if it is empty),
  4. declares the queue again with the target type under its original name,
  5. moves the messages back and deletes the temporary queue.
Properties, headers (including x-death) and persistence are copied unchanged.

STOP THE PIPELINE FIRST (Processes 1-5 on all nodes). The script refuses to run while any queue has a consumer, because a
consumer that sees the queue disappear would fail, and API Gateway (Process 1) would publish into a queue that is being replaced. Run it
once, on any node. Afterwards set RABBITMQ_QUEUE_TYPE=<target> in .env on EVERY node BEFORE starting the processes; a process
with the old value will refuse to start (RabbitMQ rejects the declaration and config.setup_queues() says why).

Delivery is at-least-once: if the script is killed between "published to the new queue" and "acked from the old one", that
message exists twice after a re-run. DB Updater (Process 4)'s ZooKeeper dedup barrier and the unique message_id key discard the second
copy at the database. The script is safe to re-run: every step checks the current state first.

Needs the same .env as the pipeline (TLS included).
"""
import argparse
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

import pika  # noqa: E402
from dotenv import load_dotenv  # noqa: E402

for _candidate in (os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".env"), "/opt/flowfirst/.env"):
    if os.path.isfile(_candidate):
        load_dotenv(_candidate)
        break

TEMP_PREFIX = "migrate_"


def _args(target):
    return {"x-queue-type": target}


def _declare(conn, name, target):
    """('ok', message_count, consumer_count) if the queue exists with the target type (it is created if missing),
    ('mismatch', None, None) if it exists with another type. Uses a throw-away channel: a refused declaration closes it."""
    ch = conn.channel()
    try:
        ok = ch.queue_declare(queue=name, durable=True, arguments=_args(target)).method
        return "ok", ok.message_count, ok.consumer_count
    except pika.exceptions.ChannelClosedByBroker as exc:
        if exc.reply_code == 406:
            return "mismatch", None, None
        raise
    finally:
        if ch.is_open:
            ch.close()


def _exists(conn, name):
    """(message_count, consumer_count) of an existing queue of ANY type, or None."""
    ch = conn.channel()
    try:
        ok = ch.queue_declare(queue=name, passive=True).method
        return ok.message_count, ok.consumer_count
    except pika.exceptions.ChannelClosedByBroker as exc:
        if exc.reply_code == 404:
            return None
        raise
    finally:
        if ch.is_open:
            ch.close()


def _move(conn, source, destination):
    """Move every message from source to destination, one at a time, ack only after the publisher confirm."""
    ch = conn.channel()
    ch.confirm_delivery()
    moved = 0
    try:
        while True:
            method, props, body = ch.basic_get(queue=source, auto_ack=False)
            if method is None:
                return moved
            ch.basic_publish(exchange="", routing_key=destination, body=body, properties=props, mandatory=True)
            ch.basic_ack(method.delivery_tag)
            moved += 1
    finally:
        if ch.is_open:
            ch.close()


def _delete_if_empty(conn, name):
    # quorum queues do not support queue.delete(if_empty=True); check message count first instead.
    state = _exists(conn, name)
    if state is None:
        return True                         # already gone
    if state[0] > 0:
        return False                        # messages arrived; caller drains and retries
    ch = conn.channel()
    try:
        ch.queue_delete(queue=name)
        return True
    except pika.exceptions.ChannelClosedByBroker as exc:
        if exc.reply_code in (404, 406):    # gone already, or a message snuck in (recheck next iteration)
            return exc.reply_code == 404
        raise
    finally:
        if ch.is_open:
            ch.close()


def migrate_one(conn, name, target, dry_run):
    temp = TEMP_PREFIX + name
    state, _, _ = _declare(conn, name, target) if not dry_run else _peek(conn, name, target)
    left_over = _exists(conn, temp)
    if state == "ok" and left_over is None:
        print("  %-28s already %s: nothing to do" % (name, target))
        return 0
    if dry_run:
        old = _exists(conn, name)
        print("  %-28s would migrate (%s message(s) waiting%s)" % (
            name, old[0] if old else 0, ", plus %d in a leftover %s" % (left_over[0], temp) if left_over else ""))
        return old[0] if old else 0

    moved = 0
    if state == "mismatch":
        _declare(conn, temp, target)
        while True:
            moved += _move(conn, name, temp)
            if _delete_if_empty(conn, name):
                break
    _declare(conn, name, target)
    if _exists(conn, temp) is not None:
        _move(conn, temp, name)
        while not _delete_if_empty(conn, temp):
            _move(conn, temp, name)
    print("  %-28s -> %s (%d message(s) carried over)" % (name, target, moved))
    return moved


def _peek(conn, name, target):
    """Dry-run variant of _declare: never creates anything. Reports 'ok' only if the queue is missing or already matches."""
    if _exists(conn, name) is None:
        return "ok", 0, 0
    ch = conn.channel()
    try:
        ch.queue_declare(queue=name, durable=True, arguments=_args(target), passive=False)
        return "ok", 0, 0
    except pika.exceptions.ChannelClosedByBroker as exc:
        if exc.reply_code == 406:
            return "mismatch", None, None
        raise
    finally:
        if ch.is_open:
            ch.close()


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--to", required=True, choices=("quorum", "classic"), help="queue type to migrate to")
    ap.add_argument("--dry-run", action="store_true", help="report only; change nothing")
    ap.add_argument("--force", action="store_true", help="run although a queue has consumers (they WILL fail)")
    args = ap.parse_args()

    os.environ["RABBITMQ_QUEUE_TYPE"] = args.to          # so setup_queues() at the end uses the same type
    from src.config.config import ALL_QUEUES, QUEUE_DEAD_LETTERS, get_connection, setup_queues

    names = list(ALL_QUEUES) + [QUEUE_DEAD_LETTERS]
    conn = get_connection()
    try:
        busy = [(n, e[1]) for n in names for e in [_exists(conn, n)] if e and e[1] > 0]
        if busy and not args.force:
            for n, c in busy:
                print("ERROR: %s has %d consumer(s). Stop Processes 1-5 on all nodes first." % (n, c), file=sys.stderr)
            return 1
        print("%s to %s via %s" % ("Dry run: migrating" if args.dry_run else "Migrating", args.to,
                                    getattr(conn, "_connected_to", "?")))
        total = sum(migrate_one(conn, n, args.to, args.dry_run) for n in names)
        if args.dry_run:
            print("Dry run complete: %d message(s) would be carried over. Nothing was changed." % total)
            return 0
        ch = conn.channel()
        setup_queues(ch)                       # re-creates the dead-letter binding and checks everything is declared
        print("Done: %d message(s) carried over. Now set RABBITMQ_QUEUE_TYPE=%s in .env on EVERY node, then start "
              "the processes." % (total, args.to))
        return 0
    finally:
        if conn.is_open:
            conn.close()


if __name__ == "__main__":
    sys.exit(main())
