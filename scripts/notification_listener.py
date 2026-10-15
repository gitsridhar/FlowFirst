#!/usr/bin/env python3
"""FlowFirst notification listener — standalone consumer.

Connects to the RabbitMQ cluster, binds a durable queue to the
flowfirst.notifications fanout exchange, and prints every envelope
to stdout as it arrives.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  WHICH NODE TO RUN THIS ON
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  Run it on ANY machine that can reach the RabbitMQ cluster.
  The typical choices are:

    Node 1, 2, or 3  — the cluster nodes themselves.
                        The script connects to 127.0.0.1:5672 by
                        default (RMQ_HOST), so no firewall rules
                        are needed.  Recommended for quick tests.

    Node 4            — the remote worker node.  Point RMQ_HOST at
                        the VIP or any cluster node IP.

    Your laptop / CI  — set RMQ_HOST=<VIP or any node IP> and make
                        sure port 5672 (or RMQ_PORT) is reachable.

  The listener is stateless and safe to run anywhere.  Multiple
  instances each get their OWN copy of every notification because
  the exchange is a fanout and each instance creates a separately
  named queue.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  PREREQUISITES
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  pip install pika          (already installed if you ran:
                             pip install -r requirements.txt)

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  QUICK START (run on a cluster node)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  # Required — must match .env on the cluster:
  export RMQ_PASSWORD=<your RABBITMQ_PASSWORD>

  # Run (all other variables have sensible defaults for a local node):
  python3 scripts/notification_listener.py

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  QUICK START (run from a remote machine / laptop)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  export RMQ_HOST=<VIP or any cluster node IP>
  export RMQ_PORT=5672          # or 5673 if connecting through the HAProxy LB port
  export RMQ_USER=flowuser
  export RMQ_PASSWORD=<your RABBITMQ_PASSWORD>
  python3 scripts/notification_listener.py

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  ENVIRONMENT VARIABLES
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  RMQ_HOST         RabbitMQ host/IP          default: 127.0.0.1
  RMQ_PORT         AMQP port                 default: 5672
  RMQ_USER         AMQP username             default: flowuser
  RMQ_PASSWORD     AMQP password             REQUIRED (no default)
  RMQ_EXCHANGE     Fanout exchange name      default: flowfirst.notifications
  RMQ_QUEUE        Durable subscriber queue  default: flowfirst.notifications.listener
  RMQ_PREFETCH     Messages in-flight limit  default: 1
  FILTER_FLOWS     Comma-separated flows to  default: (blank = all)
                   print, e.g. "4,6,7"
  FILTER_EVENTS    Comma-separated event     default: (blank = all)
                   types, e.g. "device.created,device.updated"
  PRETTY           true/false — indent JSON  default: true
  RECONNECT_DELAY  Seconds before reconnect  default: 5

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  ENABLE NOTIFICATIONS ON THE CLUSTER FIRST
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  Add to .env on every cluster node, then restart DB Updater (Process 4):
    NOTIFICATIONS_ENABLED=true

  Under Pacemaker:
    sudo pcs resource restart flowfirst-process4
"""

import json
import os
import signal
import sys
import time

import pika
import pika.exceptions

# ---------------------------------------------------------------------------
# Configuration — all sourced from environment variables so no .env file or
# FlowFirst source tree is needed on the machine running this script.
# ---------------------------------------------------------------------------
RMQ_HOST        = os.environ.get("RMQ_HOST",        "127.0.0.1")
RMQ_PORT        = int(os.environ.get("RMQ_PORT",    "5672"))
RMQ_USER        = os.environ.get("RMQ_USER",        "flowuser")
RMQ_PASSWORD    = os.environ.get("RMQ_PASSWORD",    "")
RMQ_EXCHANGE    = os.environ.get("RMQ_EXCHANGE",    "flowfirst.notifications")
RMQ_QUEUE       = os.environ.get("RMQ_QUEUE",       "flowfirst.notifications.listener")
RMQ_PREFETCH    = int(os.environ.get("RMQ_PREFETCH", "1"))
RECONNECT_DELAY = int(os.environ.get("RECONNECT_DELAY", "5"))
PRETTY          = os.environ.get("PRETTY", "true").strip().lower() in ("1", "true", "yes")

_raw_filter_flows  = os.environ.get("FILTER_FLOWS",  "").strip()
_raw_filter_events = os.environ.get("FILTER_EVENTS", "").strip()

FILTER_FLOWS  = frozenset(int(f) for f in _raw_filter_flows.split(",")  if f.strip().isdigit())
FILTER_EVENTS = frozenset(e.strip() for e in _raw_filter_events.split(",") if e.strip())

# ---------------------------------------------------------------------------
# Colour codes (suppressed when stdout is not a TTY)
# ---------------------------------------------------------------------------
_COLOUR = sys.stdout.isatty()
_C = {
    "reset":  "\033[0m"  if _COLOUR else "",
    "bold":   "\033[1m"  if _COLOUR else "",
    "dim":    "\033[2m"  if _COLOUR else "",
    "green":  "\033[32m" if _COLOUR else "",
    "yellow": "\033[33m" if _COLOUR else "",
    "cyan":   "\033[36m" if _COLOUR else "",
    "red":    "\033[31m" if _COLOUR else "",
    "blue":   "\033[34m" if _COLOUR else "",
}

# One colour per event-type prefix so device.* / telemetry.* / metric.* stand out
_EVENT_COLOUR = {
    "device":     _C["cyan"],
    "telemetry":  _C["green"],
    "metric":     _C["yellow"],
}

_received   = 0
_filtered   = 0
_stopping   = False


def _colour_for(event_type: str) -> str:
    prefix = event_type.split(".")[0] if event_type else ""
    return _EVENT_COLOUR.get(prefix, _C["blue"])


def _banner():
    print(f"{_C['bold']}FlowFirst Notification Listener{_C['reset']}")
    print(f"  Exchange : {_C['cyan']}{RMQ_EXCHANGE}{_C['reset']}")
    print(f"  Queue    : {_C['cyan']}{RMQ_QUEUE}{_C['reset']}")
    print(f"  Broker   : {_C['cyan']}{RMQ_HOST}:{RMQ_PORT}{_C['reset']} (user={RMQ_USER})")
    if FILTER_FLOWS:
        print(f"  Flows    : {_C['yellow']}only {sorted(FILTER_FLOWS)}{_C['reset']}")
    else:
        print(f"  Flows    : {_C['dim']}all{_C['reset']}")
    if FILTER_EVENTS:
        print(f"  Events   : {_C['yellow']}only {sorted(FILTER_EVENTS)}{_C['reset']}")
    else:
        print(f"  Events   : {_C['dim']}all{_C['reset']}")
    print(f"  Pretty   : {PRETTY}")
    print(f"{_C['dim']}Press Ctrl-C to stop.{_C['reset']}\n")


def _on_message(ch, method, properties, body):
    """Called by pika for every delivered message."""
    global _received, _filtered

    try:
        envelope = json.loads(body.decode("utf-8"))
    except Exception as exc:
        print(f"{_C['red']}[ERROR] could not parse message body: {exc}{_C['reset']}")
        ch.basic_ack(delivery_tag=method.delivery_tag)
        return

    flow       = envelope.get("flow", 0)
    event_type = envelope.get("event_type", "")

    # Apply local filters — the queue receives everything; we just skip printing
    if FILTER_FLOWS and flow not in FILTER_FLOWS:
        _filtered += 1
        ch.basic_ack(delivery_tag=method.delivery_tag)
        return
    if FILTER_EVENTS and event_type not in FILTER_EVENTS:
        _filtered += 1
        ch.basic_ack(delivery_tag=method.delivery_tag)
        return

    _received += 1
    colour = _colour_for(event_type)

    # Header line
    ts          = envelope.get("occurred_at", "?")
    entity_id   = envelope.get("entity_id", "?")
    entity_type = envelope.get("entity_type", "?")
    notif_id    = envelope.get("notification_id", "?")
    node        = envelope.get("node", "?")
    msg_id      = envelope.get("message_id", "?")

    sep = f"{_C['dim']}{'─' * 72}{_C['reset']}"
    print(sep)
    print(
        f"{colour}{_C['bold']}▶ {event_type}{_C['reset']}"
        f"  {_C['dim']}flow={flow}  entity={entity_type}:{entity_id}{_C['reset']}"
    )
    print(
        f"  {_C['dim']}occurred={ts}  node={node}{_C['reset']}"
    )
    print(
        f"  {_C['dim']}notification_id={notif_id}{_C['reset']}"
    )
    print(
        f"  {_C['dim']}message_id={msg_id}{_C['reset']}"
    )

    # Payload section
    if "payload" in envelope:
        indent = 2 if PRETTY else None
        payload_str = json.dumps(envelope["payload"], indent=indent)
        print(f"  {_C['bold']}payload:{_C['reset']}")
        for line in payload_str.splitlines():
            print(f"    {line}")
    elif "payload_bytes" in envelope:
        print(f"  {_C['dim']}payload: <omitted — {envelope['payload_bytes']} bytes>{_C['reset']}")

    print(f"  {_C['dim']}[#{_received} received, #{_filtered} filtered]{_C['reset']}")

    ch.basic_ack(delivery_tag=method.delivery_tag)


def _connect():
    """Create a blocking pika connection with multi-host failover if hosts are listed."""
    credentials = pika.PlainCredentials(RMQ_USER, RMQ_PASSWORD)
    params = pika.ConnectionParameters(
        host=RMQ_HOST,
        port=RMQ_PORT,
        credentials=credentials,
        heartbeat=60,
        blocked_connection_timeout=300,
        connection_attempts=3,
        retry_delay=2,
    )
    return pika.BlockingConnection(params)


def _run_once():
    """Open one connection, consume until interrupted or disconnected."""
    global _stopping

    print(f"{_C['dim']}Connecting to {RMQ_HOST}:{RMQ_PORT} …{_C['reset']}")
    conn = _connect()
    ch   = conn.channel()

    # Declare the exchange (idempotent — safe even if Process 4 already declared it)
    ch.exchange_declare(exchange=RMQ_EXCHANGE, exchange_type="fanout", durable=True)

    # Declare OUR durable subscriber queue and bind it
    ch.queue_declare(queue=RMQ_QUEUE, durable=True)
    ch.queue_bind(queue=RMQ_QUEUE, exchange=RMQ_EXCHANGE)

    ch.basic_qos(prefetch_count=RMQ_PREFETCH)
    ch.basic_consume(queue=RMQ_QUEUE, on_message_callback=_on_message, auto_ack=False)

    print(f"{_C['green']}Connected. Waiting for notifications …{_C['reset']}\n")

    try:
        while not _stopping:
            conn.process_data_events(time_limit=1)
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _handle_signal(signum, frame):
    global _stopping
    print(f"\n{_C['yellow']}Signal received — stopping.{_C['reset']}")
    _stopping = True


def main():
    if not RMQ_PASSWORD:
        print(
            f"{_C['red']}ERROR: RMQ_PASSWORD is not set.\n"
            f"Export it before running:\n"
            f"  export RMQ_PASSWORD=<your RABBITMQ_PASSWORD from .env>{_C['reset']}",
            file=sys.stderr,
        )
        sys.exit(1)

    signal.signal(signal.SIGINT,  _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    _banner()

    while not _stopping:
        try:
            _run_once()
        except pika.exceptions.AMQPConnectionError as exc:
            if _stopping:
                break
            print(
                f"{_C['yellow']}Connection lost ({exc}). "
                f"Reconnecting in {RECONNECT_DELAY}s …{_C['reset']}"
            )
            time.sleep(RECONNECT_DELAY)
        except pika.exceptions.ChannelClosedByBroker as exc:
            if _stopping:
                break
            print(
                f"{_C['red']}Channel closed by broker ({exc.reply_code}: {exc.reply_text}). "
                f"Reconnecting in {RECONNECT_DELAY}s …{_C['reset']}"
            )
            time.sleep(RECONNECT_DELAY)
        except Exception as exc:
            if _stopping:
                break
            print(
                f"{_C['red']}Unexpected error: {type(exc).__name__}: {exc}. "
                f"Reconnecting in {RECONNECT_DELAY}s …{_C['reset']}"
            )
            time.sleep(RECONNECT_DELAY)

    total = _received + _filtered
    print(
        f"\n{_C['bold']}Stopped.{_C['reset']} "
        f"{_received} notification(s) printed, "
        f"{_filtered} filtered, "
        f"{total} total received."
    )


if __name__ == "__main__":
    main()
