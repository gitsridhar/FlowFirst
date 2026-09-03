# IMPORTANT: monkey_patch() must be the very first call, before any other import.
import gt
gt.monkey_patch()

import json
import time
import uuid
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse
import pika
from config import (
    NODE_NAME,
    API_HOST,
    API_BACKEND_PORT,
    setup_queues,
    QUEUE_FLOW1_P1_TO_P2,
    QUEUE_FLOW2_P1_TO_P2,
    QUEUE_FLOW3_P1_TO_P5,
    GT_METRICS_INTERVAL_S,
)
import zk as zklib
import swift_storage
from urllib.parse import parse_qs

# ---------------------------------------------------------------------------
# Per-process state — each greenthread owns its OWN pika connection+channel.
# The _publish_conn / _publish_ch below are owned exclusively by the HTTP
# handler greenthreads (all HTTP requests run in the same eventlet thread
# context behind GreenPool, so one shared connection is fine here).
# ---------------------------------------------------------------------------
_publish_conn = None
_publish_ch   = None
_config_watcher = None

# A semaphore limits concurrent publishes to 1 (pika channel is not re-entrant)
_publish_sem = gt._pool.resize and None  # replaced below after pool import
import eventlet
_publish_sem = eventlet.semaphore.Semaphore(1)


def _ensure_publish_channel():
    """Reconnect the shared publish connection+channel if closed."""
    global _publish_conn, _publish_ch
    if _publish_conn is None or _publish_conn.is_closed:
        _publish_conn = gt.get_rmq_connection_with_retry("p1-publish")
        _publish_ch   = _publish_conn.channel()
        setup_queues(_publish_ch)
    elif _publish_ch is None or _publish_ch.is_closed:
        _publish_ch = _publish_conn.channel()
    return _publish_ch


def publish_flow1_message(item_id: int = None, custom_data: str = None, counter: int = None) -> dict:
    """Prepare and publish a Flow 1 message. Thread-safe via semaphore."""
    counter_step = _config_watcher.get_int("flow1_counter_step") if _config_watcher else 10
    if item_id  is None: item_id  = int(time.time() * 1000) % 100000
    if counter  is None: counter  = 100 + item_id
    if custom_data is None: custom_data = f"Flow-1 original payload for item #{item_id}"

    payload = {
        "flow": 1,
        "message_id": str(uuid.uuid4()),
        "item_id": item_id,
        "initial_data": custom_data,
        "counter": counter,
        "zk_counter_step": counter_step,
        "history": [{
            "stage": "process1_created",
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "status": "prepared",
            "source": "rest_api",
            "published_by": NODE_NAME,
        }],
    }
    body = json.dumps(payload, indent=2)
    with _publish_sem:
        ch = _ensure_publish_channel()
        ch.basic_publish(
            exchange="",
            routing_key=QUEUE_FLOW1_P1_TO_P2,
            body=body,
            properties=pika.BasicProperties(
                delivery_mode=pika.DeliveryMode.Persistent,
                content_type="application/json",
            ),
        )
    gt.metrics_counter["flow1_published"] += 1
    print(f"[P1][gt] [Flow 1] Published #{item_id} (counter={counter}, step={counter_step})")
    return payload


def publish_flow2_message(item_id: int = None, custom_data: str = None, value: float = None) -> dict:
    """Prepare and publish a Flow 2 message. Thread-safe via semaphore."""
    if item_id  is None: item_id  = int(time.time() * 1000) % 100000
    if value    is None: value    = round(25.0 + (item_id % 10) * 1.5, 2)
    if custom_data is None: custom_data = f"Flow-2 raw metric for item #{item_id}"

    payload = {
        "flow": 2,
        "message_id": str(uuid.uuid4()),
        "item_id": item_id,
        "initial_data": custom_data,
        "value": value,
        "history": [{
            "stage": "process1_created",
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "status": "prepared",
            "source": "rest_api",
            "published_by": NODE_NAME,
        }],
    }
    body = json.dumps(payload, indent=2)
    with _publish_sem:
        ch = _ensure_publish_channel()
        ch.basic_publish(
            exchange="",
            routing_key=QUEUE_FLOW2_P1_TO_P2,
            body=body,
            properties=pika.BasicProperties(
                delivery_mode=pika.DeliveryMode.Persistent,
                content_type="application/json",
            ),
        )
    gt.metrics_counter["flow2_published"] += 1
    print(f"[P1][gt] [Flow 2] Published #{item_id} (value={value})")
    return payload


def publish_flow3_message(item_id: int = None, custom_data: str = None, remote_metric: float = None) -> dict:
    """Prepare and publish a Flow 3 message targeted to Remote Node 4 / Process 5."""
    if item_id  is None: item_id  = int(time.time() * 1000) % 100000
    if remote_metric is None: remote_metric = round(45.0 + (item_id % 15) * 2.2, 2)
    if custom_data is None: custom_data = f"Flow-3 remote payload for item #{item_id}"

    payload = {
        "flow": 3,
        "message_id": str(uuid.uuid4()),
        "item_id": item_id,
        "initial_data": custom_data,
        "remote_metric": remote_metric,
        "history": [{
            "stage": "process1_created",
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "status": "prepared_for_remote_node4",
            "source": "rest_api",
            "published_by": NODE_NAME,
        }],
    }
    body = json.dumps(payload, indent=2)
    with _publish_sem:
        ch = _ensure_publish_channel()
        ch.basic_publish(
            exchange="",
            routing_key=QUEUE_FLOW3_P1_TO_P5,
            body=body,
            properties=pika.BasicProperties(
                delivery_mode=pika.DeliveryMode.Persistent,
                content_type="application/json",
            ),
        )
    gt.metrics_counter["flow3_published"] += 1
    print(f"[P1][gt] [Flow 3] Published #{item_id} to Remote Node 4 (metric={remote_metric})")
    return payload


# ---------------------------------------------------------------------------
# HTTP Handler
# ---------------------------------------------------------------------------
class RestApiHandler(BaseHTTPRequestHandler):
    def _send_json(self, status: int, data: dict):
        body = json.dumps(data, indent=2).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()
        self.wfile.write(body)

    def do_OPTIONS(self):
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()

    def do_GET(self):
        path = urlparse(self.path).path.rstrip("/")
        if path in ("", "/health"):
            self._send_json(200, {
                "status": "healthy",
                "service": "Process 1 REST API Producer",
                "node": NODE_NAME,
                "greenthreads": gt.list_greenthreads(),
                "metrics": gt.metrics_snapshot(),
                "zk_config": _config_watcher.get_all() if _config_watcher else {},
                "zk_registered_workers": {
                    p: zklib.list_registered(p)
                    for p in ("process1", "process2", "process3", "process4", "process5")
                },
                "endpoints": {
                    "GET /health":       "Health + greenthread status + ZK pipeline state",
                    "GET /gt/status":    "Greenthread pool status",
                    "GET /zk/health":    "Full ZooKeeper pipeline health tree",
                    "GET /zk/config":    "Current live runtime config from ZK",
                    "GET /swift/status": "Swift object storage connection and container status",
                    "GET /swift/objects": "List stored objects in Swift (?prefix=...&limit=...)",
                    "GET /swift/object/<path>": "Retrieve stored message payload from Swift",
                    "POST /zk/config":   "Update a runtime config value in ZK",
                    "POST /api/flow1":   "Publish message to Flow 1",
                    "POST /api/flow2":   "Publish message to Flow 2",
                    "POST /api/flow3":   "Publish message to Flow 3 (Remote Node 4 / Process 5 pipeline)",
                    "POST /api/batch":   "Publish batch to all flows",
                },
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            })
        elif path == "/gt/status":
            self._send_json(200, {
                "active_greenthreads": gt.list_greenthreads(),
                "metrics": gt.metrics_snapshot(),
                "pool_size": gt.GT_POOL_SIZE,
                "is_stopping": gt.is_stopping(),
            })
        elif path == "/zk/health":
            self._send_json(200, {
                "pipeline_health": zklib.get_pipeline_health(),
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            })
        elif path == "/zk/config":
            self._send_json(200, {
                "zk_config": _config_watcher.get_all() if _config_watcher else {},
            })
        elif path == "/swift/status":
            self._send_json(200, swift_storage.get_swift_status())
        elif path == "/swift/objects":
            query = parse_qs(urlparse(self.path).query)
            prefix = query.get("prefix", [None])[0]
            limit = int(query.get("limit", [100])[0])
            ok, objs, err = swift_storage.list_message_objects(prefix=prefix, limit=limit)
            if ok:
                self._send_json(200, {
                    "status": "success",
                    "count": len(objs),
                    "prefix": prefix,
                    "objects": objs,
                })
            else:
                self._send_json(502, {"status": "error", "error": err})
        elif path.startswith("/swift/object/"):
            obj_name = path[len("/swift/object/"):]
            if not obj_name:
                self._send_json(400, {"error": "Object name required in URL path"})
                return
            ok, data, headers = swift_storage.get_message_object(obj_name)
            if ok:
                self._send_json(200, {
                    "status": "success",
                    "object_name": obj_name,
                    "payload": data,
                    "swift_headers": headers,
                })
            else:
                self._send_json(404, {
                    "status": "error",
                    "object_name": obj_name,
                    "details": headers,
                })
        else:
            self._send_json(404, {"error": "Not Found", "path": self.path})

    def do_POST(self):
        path = urlparse(self.path).path.rstrip("/")
        content_length = int(self.headers.get("Content-Length", 0))
        post_data = {}
        if content_length > 0:
            try:
                post_data = json.loads(self.rfile.read(content_length).decode())
            except Exception as e:
                self._send_json(400, {"error": "Invalid JSON", "details": str(e)})
                return
        try:
            if path == "/api/flow1":
                item_id = int(post_data["item_id"]) if "item_id" in post_data else None
                counter = int(post_data["counter"]) if "counter" in post_data else None
                p = publish_flow1_message(item_id=item_id, custom_data=post_data.get("initial_data"), counter=counter)
                self._send_json(200, {"status": "success", "flow": 1, "handled_by_node": NODE_NAME,
                                      "target_queue": QUEUE_FLOW1_P1_TO_P2, "payload": p})

            elif path == "/api/flow2":
                item_id = int(post_data["item_id"]) if "item_id" in post_data else None
                val     = float(post_data["value"])  if "value"   in post_data else None
                p = publish_flow2_message(item_id=item_id, custom_data=post_data.get("initial_data"), value=val)
                self._send_json(200, {"status": "success", "flow": 2, "handled_by_node": NODE_NAME,
                                      "target_queue": QUEUE_FLOW2_P1_TO_P2, "payload": p})

            elif path == "/api/flow3":
                item_id = int(post_data["item_id"]) if "item_id" in post_data else None
                metric  = float(post_data["remote_metric"]) if "remote_metric" in post_data else None
                p = publish_flow3_message(item_id=item_id, custom_data=post_data.get("initial_data"), remote_metric=metric)
                self._send_json(200, {"status": "success", "flow": 3, "handled_by_node": NODE_NAME,
                                      "target_queue": QUEUE_FLOW3_P1_TO_P5, "target_node": "remote_node4", "payload": p})

            elif path == "/api/batch":
                count = int(post_data.get("count", 3))
                # Spawn each publish as its own greenthread — true concurrent publishing
                gts = []
                results1, results2 = [], []
                for i in range(1, count + 1):
                    gts.append(gt._pool.spawn(lambda i=i: results1.append(publish_flow1_message(item_id=i))))
                    gts.append(gt._pool.spawn(lambda i=i: results2.append(publish_flow2_message(item_id=i))))
                for g in gts:
                    g.wait()
                self._send_json(200, {"status": "success", "handled_by_node": NODE_NAME,
                                      "message": f"Published {count} messages to both flows",
                                      "flow1_messages": results1, "flow2_messages": results2})

            elif path == "/zk/config":
                key, value = post_data.get("key"), post_data.get("value")
                if not key or value is None:
                    self._send_json(400, {"error": "Both 'key' and 'value' are required."})
                    return
                ok = zklib.write_config(key, value)
                if ok:
                    self._send_json(200, {"status": "ok", "key": key, "value": value,
                                          "note": "Propagates to all watchers within seconds."})
                else:
                    self._send_json(400, {"error": f"Unknown config key: {key}"})
            else:
                self._send_json(404, {"error": "Not Found", "path": self.path})
        except Exception as e:
            print(f"[P1][gt] POST {path} error: {e}")
            self._send_json(500, {"error": "Internal Server Error", "details": str(e)})

    def log_message(self, fmt, *args):
        print(f"[P1][gt][http] {self.address_string()} — {fmt % args}")


# ---------------------------------------------------------------------------
# Greenthread workers
# ---------------------------------------------------------------------------

def _gt_rmq_heartbeat():
    """
    Dedicated greenthread: sends a pika heartbeat every 30s to keep the
    publish connection alive during idle periods.
    Pika's BlockingConnection does not send heartbeats unless you call
    process_data_events(), which this greenthread does co-operatively.
    """
    while not gt.is_stopping():
        try:
            if _publish_conn and not _publish_conn.is_closed:
                _publish_conn.process_data_events(time_limit=0)
        except Exception as e:
            print(f"[P1][gt] heartbeat error: {e}")
        gt.sleep(30)


def _gt_zk_health_reporter():
    """Periodic greenthread: writes this node's health state to ZooKeeper."""
    import zk as _zk
    while not gt.is_stopping():
        try:
            _zk._update_health("process1", NODE_NAME, "leader")
        except Exception:
            pass
        gt.sleep(30)


def _gt_http_server():
    """Greenthread: runs the HTTP server. Each request is dispatched into the
    global GreenPool so requests are handled concurrently."""
    _ensure_publish_channel()
    # Pass bind_and_activate=False so HTTPServer doesn't create and bind a socket.
    # eventlet.listen() creates and binds the non-blocking green socket.
    httpd = HTTPServer((API_HOST, API_BACKEND_PORT), RestApiHandler, bind_and_activate=False)
    # Use GreenPool to handle each request in its own greenthread
    httpd.socket = eventlet.listen((API_HOST, API_BACKEND_PORT))
    pool = eventlet.GreenPool(size=gt.GT_WORKER_CONCURRENCY * 10)
    print(f"[P1][gt] HTTP server listening on {API_HOST}:{API_BACKEND_PORT} (GreenPool size={gt.GT_WORKER_CONCURRENCY * 10})")
    try:
        while not gt.is_stopping():
            try:
                # Set a non-blocking timeout on accept so loop checks gt.is_stopping()
                httpd.socket.settimeout(1.0)
                sock, addr = httpd.socket.accept()
                pool.spawn_n(httpd.process_request, sock, addr)
            except (eventlet.support.greenlets.GreenletExit, StopIteration):
                break
            except Exception as e:
                # Timeout is normal when idle; log only real errors
                if not gt.is_stopping() and "timed out" not in str(e).lower():
                    print(f"[P1][gt] HTTP accept error: {e}")
    finally:
        try:
            httpd.server_close()
        except Exception:
            pass
        print("[P1][gt] HTTP server stopped.")


def _run_api_server():
    """Leader callback: launch all greenthreads for Process 1."""
    gt.register_signal_handlers()

    # Greenthread 1: HTTP server (handles requests concurrently)
    gt.spawn("p1_http_server", _gt_http_server, restart_on_error=True)

    # Greenthread 2: RabbitMQ heartbeat (keeps publish connection alive)
    gt.spawn("p1_rmq_heartbeat", _gt_rmq_heartbeat, restart_on_error=True)

    # Greenthread 3: ZooKeeper health reporter
    gt.periodic("p1_zk_health", _gt_zk_health_reporter, 30)

    # Greenthread 4: Metrics reporter
    gt.start_metrics_reporter("process1")

    print(f"[P1][gt] All greenthreads started: {gt.list_greenthreads()}")
    print(f"[P1][gt] ZK config endpoints live: GET /zk/health  GET /zk/config  POST /zk/config")

    # Wait for shutdown signal then stop all greenthreads
    gt.stop_event.wait()
    print("[P1][gt] Shutdown signal — stopping all greenthreads...")
    if _publish_conn and not _publish_conn.is_closed:
        _publish_conn.close()
    gt.wait_all()


def main():
    global _config_watcher

    print(f"[P1][gt] Starting (node={NODE_NAME}, pool={gt.GT_POOL_SIZE}) ...")

    # ZooKeeper: connect, register, config watch
    try:
        zklib.register_service("process1", NODE_NAME)
        _config_watcher = zklib.ConfigWatcher()
        print(f"[P1][gt] ZK ready. Config: {_config_watcher.get_all()}")
    except Exception as e:
        print(f"[P1][gt] WARNING: ZK unavailable ({e}) — degraded mode.")
        _config_watcher = None

    # Leader election
    try:
        election = zklib.LeaderElection("process1", NODE_NAME)
        election.run(_run_api_server)
    except Exception as e:
        print(f"[P1][gt] ZK election failed ({e}) — single-node mode.")
        _run_api_server()
    finally:
        zklib.close_client()


if __name__ == "__main__":
    main()
