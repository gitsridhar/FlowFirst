import json
import threading
import time
import uuid
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse
import pika
from config import (
    NODE_NAME,
    API_HOST,
    API_PORT,
    get_connection,
    setup_queues,
    QUEUE_FLOW1_P1_TO_P2,
    QUEUE_FLOW2_P1_TO_P2,
)
import zk as zklib

# Lock for thread-safe RabbitMQ channel access across concurrent HTTP requests
publish_lock = threading.Lock()
rabbitmq_connection = None
rabbitmq_channel = None

# ZooKeeper handles — initialised in main()
_config_watcher = None


def init_rabbitmq():
    """Establish and initialize shared RabbitMQ connection & channel."""
    global rabbitmq_connection, rabbitmq_channel

    # Retry loop — Pacemaker may start this process before RabbitMQ is ready.
    for attempt in range(1, 11):
        try:
            rabbitmq_connection = get_connection()
            break
        except Exception as e:
            print(f"[Process 1 API] RabbitMQ not ready (attempt {attempt}/10): {e} — retrying in 5s...")
            time.sleep(5)
    else:
        print("[Process 1 API] Could not connect to RabbitMQ after 10 attempts. Exiting.")
        raise SystemExit(1)

    rabbitmq_channel = rabbitmq_connection.channel()
    setup_queues(rabbitmq_channel)
    print(f"[Process 1 API] Connected to RabbitMQ at {rabbitmq_connection._connected_to}")


def ensure_channel():
    """Ensure channel is open, reconnect if needed."""
    global rabbitmq_connection, rabbitmq_channel
    if rabbitmq_connection is None or rabbitmq_connection.is_closed or rabbitmq_channel is None or rabbitmq_channel.is_closed:
        print("[Process 1 API] Reconnecting to RabbitMQ...")
        init_rabbitmq()
    return rabbitmq_channel


def publish_flow1_message(item_id: int = None, custom_data: str = None, counter: int = None) -> dict:
    """Prepare and publish a Flow 1 message to RabbitMQ."""
    # Read live counter step from ZooKeeper config (hot-reloadable)
    counter_step = _config_watcher.get_int("flow1_counter_step") if _config_watcher else 10

    if item_id is None:
        item_id = int(time.time() * 1000) % 100000
    if counter is None:
        counter = 100 + item_id
    if custom_data is None:
        custom_data = f"Flow-1 original payload for item #{item_id}"

    payload = {
        "flow": 1,
        "message_id": str(uuid.uuid4()),
        "item_id": item_id,
        "initial_data": custom_data,
        "counter": counter,
        "zk_counter_step": counter_step,
        "history": [
            {
                "stage": "process1_created",
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
                "status": "prepared",
                "source": "rest_api",
                "published_by": NODE_NAME,
            }
        ],
    }

    body = json.dumps(payload, indent=2)
    with publish_lock:
        ch = ensure_channel()
        ch.basic_publish(
            exchange="",
            routing_key=QUEUE_FLOW1_P1_TO_P2,
            body=body,
            properties=pika.BasicProperties(
                delivery_mode=pika.DeliveryMode.Persistent,
                content_type="application/json",
            ),
        )
    print(f"[Process 1 API] [Flow 1] Published message #{item_id} (counter={counter}, step={counter_step}) to '{QUEUE_FLOW1_P1_TO_P2}'")
    return payload


def publish_flow2_message(item_id: int = None, custom_data: str = None, value: float = None) -> dict:
    """Prepare and publish a Flow 2 message to RabbitMQ."""
    if item_id is None:
        item_id = int(time.time() * 1000) % 100000
    if value is None:
        value = round(25.0 + (item_id % 10) * 1.5, 2)
    if custom_data is None:
        custom_data = f"Flow-2 raw metric for item #{item_id}"

    payload = {
        "flow": 2,
        "message_id": str(uuid.uuid4()),
        "item_id": item_id,
        "initial_data": custom_data,
        "value": value,
        "history": [
            {
                "stage": "process1_created",
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
                "status": "prepared",
                "source": "rest_api",
                "published_by": NODE_NAME,
            }
        ],
    }

    body = json.dumps(payload, indent=2)
    with publish_lock:
        ch = ensure_channel()
        ch.basic_publish(
            exchange="",
            routing_key=QUEUE_FLOW2_P1_TO_P2,
            body=body,
            properties=pika.BasicProperties(
                delivery_mode=pika.DeliveryMode.Persistent,
                content_type="application/json",
            ),
        )
    print(f"[Process 1 API] [Flow 2] Published message #{item_id} (value={value}) to '{QUEUE_FLOW2_P1_TO_P2}'")
    return payload


class RestApiHandler(BaseHTTPRequestHandler):
    """HTTP Request Handler providing REST endpoints for Process 1."""

    def _send_json_response(self, status_code: int, data: dict):
        response_bytes = json.dumps(data, indent=2).encode("utf-8")
        self.send_response(status_code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(response_bytes)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()
        self.wfile.write(response_bytes)

    def do_OPTIONS(self):
        """Handle CORS pre-flight."""
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()

    def do_GET(self):
        """Handle GET requests (health check, ZK pipeline health)."""
        parsed_url = urlparse(self.path)
        path = parsed_url.path.rstrip("/")

        if path in ("", "/health"):
            zk_config = _config_watcher.get_all() if _config_watcher else {}
            self._send_json_response(
                200,
                {
                    "status": "healthy",
                    "service": "Process 1 REST API Producer",
                    "node": NODE_NAME,
                    "zk_config": zk_config,
                    "zk_registered_workers": {
                        "process1": zklib.list_registered("process1"),
                        "process2": zklib.list_registered("process2"),
                        "process3": zklib.list_registered("process3"),
                        "process4": zklib.list_registered("process4"),
                    },
                    "endpoints": {
                        "GET /health":          "Health check + ZK pipeline status",
                        "GET /zk/health":       "Full ZooKeeper pipeline health tree",
                        "GET /zk/config":       "Current live runtime config from ZK",
                        "POST /zk/config":      "Update a runtime config value in ZK",
                        "POST /api/flow1":      "Publish message to Flow 1",
                        "POST /api/flow2":      "Publish message to Flow 2",
                        "POST /api/batch":      "Publish batch to both flows",
                    },
                    "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
                },
            )

        elif path == "/zk/health":
            self._send_json_response(200, {
                "pipeline_health": zklib.get_pipeline_health(),
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            })

        elif path == "/zk/config":
            self._send_json_response(200, {
                "zk_config": _config_watcher.get_all() if _config_watcher else {},
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            })

        else:
            self._send_json_response(
                404,
                {
                    "error": "Not Found",
                    "path": self.path,
                    "available_endpoints": ["/health", "/zk/health", "/zk/config", "/api/flow1", "/api/flow2", "/api/batch"],
                },
            )

    def do_POST(self):
        """Handle POST requests."""
        parsed_url = urlparse(self.path)
        path = parsed_url.path.rstrip("/")

        content_length = int(self.headers.get("Content-Length", 0))
        post_data = {}
        if content_length > 0:
            try:
                body_bytes = self.rfile.read(content_length)
                post_data = json.loads(body_bytes.decode("utf-8"))
            except Exception as e:
                self._send_json_response(400, {"error": "Invalid JSON body", "details": str(e)})
                return

        try:
            if path == "/api/flow1":
                item_id = post_data.get("item_id")
                if item_id is not None:
                    item_id = int(item_id)
                counter = post_data.get("counter")
                if counter is not None:
                    counter = int(counter)
                custom_data = post_data.get("initial_data")

                published = publish_flow1_message(item_id=item_id, custom_data=custom_data, counter=counter)
                self._send_json_response(
                    200,
                    {
                        "status": "success",
                        "flow": 1,
                        "handled_by_node": NODE_NAME,
                        "target_queue": QUEUE_FLOW1_P1_TO_P2,
                        "message": "Message successfully published to Flow 1 queue",
                        "payload": published,
                    },
                )

            elif path == "/api/flow2":
                item_id = post_data.get("item_id")
                if item_id is not None:
                    item_id = int(item_id)
                val = post_data.get("value")
                if val is not None:
                    val = float(val)
                custom_data = post_data.get("initial_data")

                published = publish_flow2_message(item_id=item_id, custom_data=custom_data, value=val)
                self._send_json_response(
                    200,
                    {
                        "status": "success",
                        "flow": 2,
                        "handled_by_node": NODE_NAME,
                        "target_queue": QUEUE_FLOW2_P1_TO_P2,
                        "message": "Message successfully published to Flow 2 queue",
                        "payload": published,
                    },
                )

            elif path == "/api/batch":
                count = int(post_data.get("count", 3))
                flow1_results = []
                flow2_results = []
                for i in range(1, count + 1):
                    flow1_results.append(publish_flow1_message(item_id=i))
                    flow2_results.append(publish_flow2_message(item_id=i))
                self._send_json_response(
                    200,
                    {
                        "status": "success",
                        "handled_by_node": NODE_NAME,
                        "message": f"Published {count} messages to both Flow 1 and Flow 2 queues",
                        "flow1_messages": flow1_results,
                        "flow2_messages": flow2_results,
                    },
                )

            elif path == "/zk/config":
                key   = post_data.get("key")
                value = post_data.get("value")
                if not key or value is None:
                    self._send_json_response(400, {"error": "Both 'key' and 'value' are required."})
                    return
                success = zklib.write_config(key, value)
                if success:
                    self._send_json_response(200, {
                        "status": "ok",
                        "key": key,
                        "value": value,
                        "note": "Change will propagate to all watching processes within seconds.",
                    })
                else:
                    self._send_json_response(400, {"error": f"Unknown config key: {key}"})

            else:
                self._send_json_response(
                    404,
                    {
                        "error": "Not Found",
                        "path": self.path,
                        "available_endpoints": ["/health", "/zk/health", "/zk/config", "/api/flow1", "/api/flow2", "/api/batch"],
                    },
                )

        except Exception as e:
            print(f"[Process 1 API] Error processing POST {path}: {e}")
            self._send_json_response(500, {"error": "Internal Server Error", "details": str(e)})

    def log_message(self, format, *args):
        print(f"[Process 1 REST API] {self.address_string()} - {format % args}")


def _run_api_server():
    """Run the HTTP server — called as the leader's active work."""
    init_rabbitmq()

    server_address = (API_HOST, API_PORT)
    httpd = HTTPServer(server_address, RestApiHandler)
    print(f"[Process 1] REST API Server is listening on http://{API_HOST}:{API_PORT}")
    print(f"[Process 1] ZK config endpoints: GET /zk/health  GET /zk/config  POST /zk/config")

    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\n[Process 1] Stopping REST API server...")
    finally:
        httpd.server_close()
        global rabbitmq_connection
        if rabbitmq_connection and not rabbitmq_connection.is_closed:
            rabbitmq_connection.close()
        print("[Process 1] Server closed and RabbitMQ connection terminated.")


def main():
    global _config_watcher

    print(f"[Process 1] Starting REST API Server on http://{API_HOST}:{API_PORT}...")
    print(f"[Process 1] Node: {NODE_NAME} — connecting to ZooKeeper...")

    # --- ZooKeeper: connect, register, watch config ---
    try:
        zklib.register_service("process1", NODE_NAME)
        _config_watcher = zklib.ConfigWatcher()
        print(f"[Process 1] ZooKeeper ready. Live config: {_config_watcher.get_all()}")
    except Exception as e:
        print(f"[Process 1] WARNING: ZooKeeper unavailable ({e}) — continuing without ZK features.")
        _config_watcher = None

    # --- Leader election: only the elected leader runs the HTTP server ---
    print(f"[Process 1] Entering ZooKeeper leader election...")
    try:
        election = zklib.LeaderElection("process1", NODE_NAME)
        election.run(_run_api_server)    # blocks until elected, then runs server
    except Exception as e:
        print(f"[Process 1] ZooKeeper election failed ({e}) — running without election (single-node mode).")
        _run_api_server()
    finally:
        zklib.close_client()


if __name__ == "__main__":
    main()
