# IMPORTANT: monkey_patch() must be the very first call, before any other import.
from src.gt import gt
gt.monkey_patch()

import itertools
import json
import logging
import time
import uuid
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse
import pika
from src.application import authz
from src.config.config import (
    NODE_NAME,
    API_HOST,
    API_BACKEND_PORT,
    TLS_API_ENABLED,
    TLS_API_CLIENT_AUTH,
    ADMIN_ALLOWED_CNS,
    ADMIN_TRUSTED_PROXY_CNS,
    ALLOW_UNAUTHENTICATED_ADMIN,
    AUTH_MODE,
    AUTH_API_KEYS,
    AUTH_TOKEN_SECRET,
    AUTH_TOKEN_TTL_S,
    AUTHZ_MODE,
    AUTHZ_ROLES,
    AUTHZ_IDENTITY_ROLES,
    AUTHZ_ACL,
    AUTHZ_POLICY,
    API_MAX_BODY_BYTES,
    API_MAX_BATCH_COUNT,
    API_MAX_LIST_LIMIT,
    API_REQUEST_TIMEOUT_S,
    API_EXPOSE_ERROR_DETAILS,
    API_PUBLISH_CONNECTIONS,
    setup_queues,
    QUEUE_FLOW1_P1_TO_P2,
    QUEUE_FLOW2_P1_TO_P2,
    QUEUE_FLOW3_P1_TO_P5,
    QUEUE_FLOW4_P1_TO_P2,
    QUEUE_FLOW5_P1_TO_P2,
    QUEUE_FLOW6_P1_TO_P2,
    QUEUE_FLOW7_P1_TO_P2,
    GT_METRICS_INTERVAL_S,
    API_REQUEST_POOL_SIZE,
    API_LISTEN_BACKLOG,
)
from src.metrics import metrics
from src.zookeeper import zk as zklib
from src.swift import swift_storage
from urllib.parse import parse_qs

log = logging.getLogger("flowfirst.p1")

# ---------------------------------------------------------------------------
# Per-process state — each greenthread owns its OWN pika connection+channel.
# HTTP request greenthreads do not share one connection: they publish through a small pool of
# _PublishSlot objects (API_PUBLISH_CONNECTIONS), each used by one greenthread at a time.
# ---------------------------------------------------------------------------
_config_watcher = None
_ADMIN_ALLOWED_CNS = authz.parse_list(ADMIN_ALLOWED_CNS)
_ADMIN_PROXY_CNS = authz.parse_list(ADMIN_TRUSTED_PROXY_CNS)
_AUTH_API_KEYS = authz.parse_api_keys(AUTH_API_KEYS)
_AUTHZ_ROLES, _AUTHZ_IDENTITY_ROLES = authz.parse_roles(AUTHZ_ROLES, AUTHZ_IDENTITY_ROLES)
_AUTHZ_ACL    = authz.parse_acl(AUTHZ_ACL)
_AUTHZ_POLICY = authz.parse_policy(AUTHZ_POLICY)

import eventlet


class _PublishSlot:
    """One publisher connection + channel, used by ONE greenthread at a time (pika channels are not re-entrant).
    The semaphore also stops the heartbeat greenthread from touching the connection while a publish is waiting
    for its confirm (it did not need to before confirms existed, because publish never waited)."""

    def __init__(self, index: int):
        self.index = index
        self.sem = eventlet.semaphore.Semaphore(1)
        self.conn = None
        self.ch = None

    def channel(self):
        """Connect/reconnect lazily. Call only while holding self.sem."""
        if self.conn is None or self.conn.is_closed:
            self.conn = gt.get_rmq_connection_with_retry(f"p1-publish-{self.index}")
            self.ch = self.conn.channel()
            setup_queues(self.ch)                 # declares queues + DLX objects and turns on publisher confirms
        elif self.ch is None or self.ch.is_closed:
            self.ch = self.conn.channel()
            setup_queues(self.ch)                 # a new channel is a new confirm-mode session: it must be re-enabled
        return self.ch


_publish_slots = [_PublishSlot(i) for i in range(API_PUBLISH_CONNECTIONS)]
_publish_next = itertools.count()


def _publish(routing_key: str, body: str) -> None:
    """Publish one persistent message and WAIT for the broker's confirm (raises if it is refused or unroutable).
    Requests are spread round-robin over the publisher connections."""
    slot = _publish_slots[next(_publish_next) % len(_publish_slots)]
    with slot.sem:
        slot.channel().basic_publish(
            exchange="",
            mandatory=True,
            routing_key=routing_key,
            body=body,
            properties=pika.BasicProperties(
                delivery_mode=pika.DeliveryMode.Persistent,
                content_type="application/json",
            ),
        )


metrics.register_gauge("flowfirst_publish_connections_configured", "Publisher connections Process 1 is set to keep.",
                       lambda: len(_publish_slots))
metrics.register_gauge("flowfirst_publish_connections_open", "Publisher connections currently open to RabbitMQ.",
                       lambda: sum(1 for s in _publish_slots if s.conn is not None and not s.conn.is_closed))


def _ensure_publish_channel():
    """Open the first publisher connection at start-up so a broker problem is seen immediately."""
    slot = _publish_slots[0]
    with slot.sem:
        return slot.channel()


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
    _publish(QUEUE_FLOW1_P1_TO_P2, body)
    gt.metrics_counter["flow1_published"] += 1
    log.info(f"[Flow 1] Published #{item_id} (counter={counter}, step={counter_step})")
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
    _publish(QUEUE_FLOW2_P1_TO_P2, body)
    gt.metrics_counter["flow2_published"] += 1
    log.info(f"[Flow 2] Published #{item_id} (value={value})")
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
    _publish(QUEUE_FLOW3_P1_TO_P5, body)
    gt.metrics_counter["flow3_published"] += 1
    log.info(f"[Flow 3] Published #{item_id} to Remote Node 4 (metric={remote_metric})")
    return payload


def publish_flow4_message(device_name: str = None, device_type: str = None, location: str = None,
                          firmware_version: str = None, tags: dict = None, owner_team: str = None) -> dict:
    """Prepare and publish a Flow 4 message (create IoT device)."""
    import uuid as _uuid
    device_id = str(_uuid.uuid4())
    if device_name is None:
        device_name = f"sensor-{int(time.time() * 1000) % 100000}"
    if device_type is None:
        device_type = "temperature_sensor"
    if location is None:
        location = "facility/zone-1"
    if firmware_version is None:
        firmware_version = "1.0.0"
    if owner_team is None:
        owner_team = "ops"

    payload = {
        "flow": 4,
        "message_id": str(uuid.uuid4()),
        "operation": "create",
        "device_id": device_id,
        "device_name": device_name,
        "device_type": device_type,
        "location": location,
        "firmware_version": firmware_version,
        "tags": tags or {},
        "status": "active",
        "owner_team": owner_team,
        "history": [{
            "stage": "process1_created",
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "published_by": NODE_NAME,
            "operation": "create_device",
        }],
    }
    body = json.dumps(payload, indent=2)
    _publish(QUEUE_FLOW4_P1_TO_P2, body)
    gt.metrics_counter["flow4_published"] += 1
    log.info(f"[Flow 4] Published create device '{device_name}' ({device_type})")
    return payload


def publish_flow5_message(filters: dict = None) -> dict:
    """Prepare and publish a Flow 5 message (query IoT devices)."""
    if filters is None:
        filters = {}

    payload = {
        "flow": 5,
        "message_id": str(uuid.uuid4()),
        "operation": "query",
        "filters": filters,
        "history": [{
            "stage": "process1_created",
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "published_by": NODE_NAME,
            "operation": "query_devices",
        }],
    }
    body = json.dumps(payload, indent=2)
    _publish(QUEUE_FLOW5_P1_TO_P2, body)
    gt.metrics_counter["flow5_published"] += 1
    log.info(f"[Flow 5] Published device query (filters={filters})")
    return payload


def publish_flow6_message(device_id: str, updates: dict = None) -> dict:
    """Prepare and publish a Flow 6 message (update IoT device)."""
    if not device_id:
        raise ValueError("device_id is required for flow 6 (update)")
    if updates is None:
        updates = {}

    payload = {
        "flow": 6,
        "message_id": str(uuid.uuid4()),
        "operation": "update",
        "device_id": device_id,
        "updates": updates,
        "history": [{
            "stage": "process1_created",
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "published_by": NODE_NAME,
            "operation": "update_device",
        }],
    }
    body = json.dumps(payload, indent=2)
    _publish(QUEUE_FLOW6_P1_TO_P2, body)
    gt.metrics_counter["flow6_published"] += 1
    log.info(f"[Flow 6] Published update for device_id={device_id}")
    return payload


def publish_flow7_message(device_id: str) -> dict:
    """Prepare and publish a Flow 7 message (soft-delete IoT device)."""
    if not device_id:
        raise ValueError("device_id is required for flow 7 (delete)")

    payload = {
        "flow": 7,
        "message_id": str(uuid.uuid4()),
        "operation": "delete",
        "device_id": device_id,
        "history": [{
            "stage": "process1_created",
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "published_by": NODE_NAME,
            "operation": "soft_delete_device",
        }],
    }
    body = json.dumps(payload, indent=2)
    _publish(QUEUE_FLOW7_P1_TO_P2, body)
    gt.metrics_counter["flow7_published"] += 1
    log.info(f"[Flow 7] Published soft-delete for device_id={device_id}")
    return payload


# ---------------------------------------------------------------------------
# HTTP Handler
# ---------------------------------------------------------------------------
class RestApiHandler(BaseHTTPRequestHandler):
    # Socket timeout for every read/write on a connection: a client that stalls while sending headers or the body
    # cannot hold one of the limited request slots (API_REQUEST_POOL_SIZE) forever.
    timeout = API_REQUEST_TIMEOUT_S

    def _fail(self, status: int, public_message: str, exc: Exception = None, **extra):
        """Answer with a GENERIC error body. The exception goes to the log with a request id; the client gets only the
        id, so a support request can be matched to the log line without leaking hosts, SQL, paths or library text.
        API_EXPOSE_ERROR_DETAILS=true adds str(exc) (development only)."""
        request_id = uuid.uuid4().hex[:12]
        if exc is not None:
            log.error(f"[http] {self.command} {self.path} -> {status} request_id={request_id}: "
                      f"{type(exc).__name__}: {exc}", exc_info=status >= 500)
        body = {"error": public_message, "request_id": request_id}
        body.update(extra)
        if API_EXPOSE_ERROR_DETAILS and exc is not None:
            body["details"] = str(exc)
        self._send_json(status, body)

    def _read_json_body(self):
        """Return (ok, data). On failure the error response has already been sent (ok is False).
        Rejects a bad/negative/over-limit Content-Length BEFORE reading anything, and a non-object JSON body."""
        raw_length = self.headers.get("Content-Length", "0")
        try:
            length = int(raw_length)
            if length < 0:
                raise ValueError("negative")
        except ValueError:
            self._fail(400, "Invalid Content-Length")
            return False, None
        if length > API_MAX_BODY_BYTES:
            # Do not read the body. Closing after the reply means a huge body is never pulled into memory.
            self.close_connection = True
            self._fail(413, "Request body too large", max_bytes=API_MAX_BODY_BYTES)
            return False, None
        if length == 0:
            return True, {}
        try:
            data = json.loads(self.rfile.read(length).decode("utf-8"))
        except Exception as e:
            self._fail(400, "Invalid JSON", e)
            return False, None
        if not isinstance(data, dict):
            self._fail(400, "JSON body must be an object")
            return False, None
        return True, data

    def _send_json(self, status: int, data: dict):
        gt.metrics_counter[f"http_{status // 100}xx"] += 1
        body = json.dumps(data, indent=2).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()
        self.wfile.write(body)

    def do_OPTIONS(self):
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()

    def do_GET(self):
        path = urlparse(self.path).path.rstrip("/")
        if path in ("", "/health"):
            # LIVENESS - what HAProxy probes every 3 s. It must answer from local state only: no ZooKeeper, RabbitMQ,
            # MariaDB or Swift call, because any of those can block longer than the probe timeout, and a dependency
            # outage would then take every healthy API node out of the pool. Dependency state: /ready, /zk/health.
            self._send_json(200, {
                "status": "healthy",
                "service": "Process 1 REST API Producer",
                "node": NODE_NAME,
                "greenthreads": gt.list_greenthreads(),
                "metrics": gt.metrics_snapshot(),
                "endpoints": {
                    "GET /health":       "Liveness: this process answers (local state only; used by HAProxy)",
                    "GET /ready":        "Readiness: RabbitMQ publisher connection and ZooKeeper state as last seen (no network call)",
                    "GET /gt/status":    "Greenthread pool status",
                    "GET /zk/health":    "ZooKeeper pipeline health tree and registered workers (calls ZooKeeper)",
                    "GET /zk/config":    "Current live runtime config from ZK",
                    "GET /swift/status": "Swift object storage connection and container status",
                    "GET /swift/objects": "List stored objects in Swift (?prefix=...&limit=...)",
                    "GET /swift/object/<path>": "Retrieve stored message payload from Swift",
                    "POST /zk/config":         "Update a runtime config value in ZK",
                    "POST /api/flow1":          "Publish message to Flow 1",
                    "POST /api/flow2":          "Publish message to Flow 2",
                    "POST /api/flow3":          "Publish message to Flow 3 (Remote Node 4 / Process 5 pipeline)",
                    "POST /api/batch":          "Publish batch to all flows",
                    "POST /api/devices":        "Create IoT device (Flow 4)",
                    "POST /api/devices/query":  "Query IoT devices (Flow 5)",
                    "PUT  /api/devices/{id}":   "Update IoT device (Flow 6, also fans out to remote node)",
                    "DELETE /api/devices/{id}": "Soft-delete IoT device (Flow 7)",
                },
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            })
        elif path == "/ready":
            # READINESS from state this process already holds - still no network call, so it cannot hang.
            # RabbitMQ decides it: without a publisher connection no request can succeed. ZooKeeper is reported but
            # does not fail it: publishing works without ZooKeeper (runtime config falls back to defaults).
            open_slots = sum(1 for s in _publish_slots if s.conn is not None and not s.conn.is_closed)
            zk_state = zklib.connection_state()
            ready = open_slots > 0 and not gt.is_stopping()
            self._send_json(200 if ready else 503, {
                "status": "ready" if ready else "not_ready",
                "node": NODE_NAME,
                "checks": {
                    "rabbitmq_publisher_connections": {"open": open_slots, "configured": len(_publish_slots), "required": 1},
                    "zookeeper": {"state": zk_state, "required": False},
                    "stopping": gt.is_stopping(),
                },
                "note": "Connections are re-opened on the next publish, so an idle node can report not_ready after a "
                        "broker restart until a request arrives.",
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
                "registered_workers": {
                    p: zklib.list_registered(p)
                    for p in ("process1", "process2", "process3", "process4", "process5")
                },
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            })
        elif path == "/zk/config":
            self._send_json(200, {
                "zk_config": _config_watcher.get_all() if _config_watcher else {},
            })
        elif path == "/swift/status":
            status = swift_storage.get_swift_status()
            if status.get("status") == "error":
                # keep the raw exception text (hosts, library messages) out of the response; it goes to the log
                log.warning(f"[http] /swift/status: object store error: {status.get('error')}")
                if not API_EXPOSE_ERROR_DETAILS:
                    status = {k: v for k, v in status.items() if k not in ("error", "auth_url")}
                    status["error"] = "Object store unreachable (see server log)"
            self._send_json(200, status)
        elif path == "/swift/objects":
            query = parse_qs(urlparse(self.path).query)
            prefix = query.get("prefix", [None])[0]
            try:
                limit = int(query.get("limit", [100])[0])
            except ValueError:
                self._fail(400, "'limit' must be a whole number")
                return
            if not 1 <= limit <= API_MAX_LIST_LIMIT:
                self._fail(400, f"'limit' must be between 1 and {API_MAX_LIST_LIMIT}", max_limit=API_MAX_LIST_LIMIT)
                return
            ok, objs, err = swift_storage.list_message_objects(prefix=prefix, limit=limit)
            if ok:
                self._send_json(200, {
                    "status": "success",
                    "count": len(objs),
                    "prefix": prefix,
                    "objects": objs,
                })
            else:
                self._fail(502, "Object store request failed", RuntimeError(err))
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
                self._fail(404, "Object not found or object store unavailable", RuntimeError(str(headers)),
                           object_name=obj_name)
        else:
            self._send_json(404, {"error": "Not Found"})

    def _caller_identity(self):
        """Certificate CN of the caller (see authz.resolve_identity), or None."""
        getpeercert = getattr(self.connection, "getpeercert", None)
        peer = getpeercert() if getpeercert else None
        return authz.resolve_identity(peer, self.headers.get("X-SSL-Client-CN"), _ADMIN_PROXY_CNS)

    def _require_admin(self) -> bool:
        """True if the caller may change runtime configuration; otherwise answers 403 and returns False."""
        identity = self._caller_identity()
        allowed, why = authz.admin_decision(identity, TLS_API_ENABLED, _ADMIN_ALLOWED_CNS, ALLOW_UNAUTHENTICATED_ADMIN)
        if allowed:
            log.info(f"[authz] admin action {self.command} {self.path} by '{identity}' — {why}")
            return True
        log.warning(f"[authz] DENIED {self.command} {self.path} from {self.address_string()} "
                    f"(identity={identity!r}) — {why}")
        self._send_json(403, {"error": "Forbidden"})
        return False

    def _peer_cert(self):
        """ssl.SSLSocket.getpeercert() result for the current connection, or None."""
        fn = getattr(self.connection, "getpeercert", None)
        return fn() if fn else None

    def _bearer_token(self):
        """Value of the Authorization: Bearer <token> header, or None."""
        auth = self.headers.get("Authorization", "")
        if auth.lower().startswith("bearer "):
            return auth[7:].strip() or None
        return None

    def _require_auth(self) -> bool:
        """Enforce AUTH_MODE on flow endpoints. Returns True if allowed; sends 401 and returns False otherwise."""
        if AUTH_MODE == "none":
            return True
        ok, label, reason = authz.check_request_auth(
            mode=AUTH_MODE,
            raw_api_key=self.headers.get("X-Api-Key"),
            bearer_token=self._bearer_token(),
            peer_cert=self._peer_cert(),
            api_keys=_AUTH_API_KEYS,
            token_secret=AUTH_TOKEN_SECRET,
        )
        if ok:
            log.debug(f"[auth] {self.command} {self.path} accepted ({reason}) label={label!r}")
            self._authed_label = label        # stash for _require_authz
            return True
        log.warning(f"[auth] {self.command} {self.path} from {self.address_string()} DENIED — {reason}")
        self._send_json(401, {"error": "Unauthorized", "detail": reason})
        return False

    def _authed_identity(self) -> str:
        """Return the authenticated identity established by _require_auth(), or cert CN / 'anonymous'."""
        label = getattr(self, "_authed_label", None)
        if label:
            return label
        # In AUTH_MODE=none there is no label; fall back to cert CN or 'anonymous'
        cn = authz.cert_common_name(self._peer_cert())
        return cn or "anonymous"

    def _require_authz(self, action: str, resource: str) -> bool:
        """Enforce AUTHZ_MODE on an endpoint. Returns True if allowed; sends 403 and returns False otherwise.

        Must be called AFTER _require_auth() so the identity is established.
        When AUTHZ_MODE=none this is a no-op (returns True immediately).
        """
        if not AUTHZ_MODE or AUTHZ_MODE == "none":
            return True
        identity = self._authed_identity()
        allowed, reason = authz.check_authz(
            authz_mode=AUTHZ_MODE,
            identity=identity,
            action=action,
            resource=resource,
            method=self.command,
            roles=_AUTHZ_ROLES,
            identity_roles=_AUTHZ_IDENTITY_ROLES,
            acl=_AUTHZ_ACL,
            policy=_AUTHZ_POLICY,
        )
        if allowed:
            log.debug(f"[authz] {self.command} {self.path} identity={identity!r} ALLOWED — {reason}")
            return True
        log.warning(f"[authz] {self.command} {self.path} from {self.address_string()} "
                    f"identity={identity!r} DENIED — {reason}")
        self._send_json(403, {"error": "Forbidden", "detail": reason})
        return False

    def do_POST(self):
        path = urlparse(self.path).path.rstrip("/")

        # /auth/token issues a bearer token; it needs an api_key credential but is NOT itself gated by _require_auth
        if path == "/auth/token":
            ok_body, post_data = self._read_json_body()
            if not ok_body:
                return
            raw_key = post_data.get("api_key", "")
            label = authz.verify_api_key(raw_key, _AUTH_API_KEYS)
            if not label:
                self._send_json(401, {"error": "Unauthorized", "detail": "invalid api_key"})
                return
            try:
                token = authz.issue_token(label, AUTH_TOKEN_TTL_S, AUTH_TOKEN_SECRET)
            except ValueError as e:
                self._fail(500, "Token issuance failed", e)
                return
            self._send_json(200, {"token": token, "expires_in": AUTH_TOKEN_TTL_S, "label": label})
            return

        ok, post_data = self._read_json_body()
        if not ok:
            return
        try:
            if path in ("/api/flow1", "/api/flow2", "/api/flow3", "/api/batch",
                        "/api/devices", "/api/devices/query"):
                if not self._require_auth():
                    return

            if path == "/api/flow1":
                if not self._require_authz("publish", "flow:1"):
                    return
                item_id = int(post_data["item_id"]) if "item_id" in post_data else None
                counter = int(post_data["counter"]) if "counter" in post_data else None
                p = publish_flow1_message(item_id=item_id, custom_data=post_data.get("initial_data"), counter=counter)
                self._send_json(200, {"status": "success", "flow": 1, "handled_by_node": NODE_NAME,
                                      "target_queue": QUEUE_FLOW1_P1_TO_P2, "payload": p})

            elif path == "/api/flow2":
                if not self._require_authz("publish", "flow:2"):
                    return
                item_id = int(post_data["item_id"]) if "item_id" in post_data else None
                val     = float(post_data["value"])  if "value"   in post_data else None
                p = publish_flow2_message(item_id=item_id, custom_data=post_data.get("initial_data"), value=val)
                self._send_json(200, {"status": "success", "flow": 2, "handled_by_node": NODE_NAME,
                                      "target_queue": QUEUE_FLOW2_P1_TO_P2, "payload": p})

            elif path == "/api/flow3":
                if not self._require_authz("publish", "flow:3"):
                    return
                item_id = int(post_data["item_id"]) if "item_id" in post_data else None
                metric  = float(post_data["remote_metric"]) if "remote_metric" in post_data else None
                p = publish_flow3_message(item_id=item_id, custom_data=post_data.get("initial_data"), remote_metric=metric)
                self._send_json(200, {"status": "success", "flow": 3, "handled_by_node": NODE_NAME,
                                      "target_queue": QUEUE_FLOW3_P1_TO_P5, "target_node": "remote_node4", "payload": p})

            elif path == "/api/batch":
                if not self._require_authz("publish", "flow:*"):
                    return
                count = int(post_data.get("count", 3))
                if not 1 <= count <= API_MAX_BATCH_COUNT:
                    self._fail(400, f"'count' must be between 1 and {API_MAX_BATCH_COUNT}", max_count=API_MAX_BATCH_COUNT)
                    return
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

            elif path == "/api/devices":
                if not self._require_authz("publish", "flow:4"):
                    return
                p = publish_flow4_message(
                    device_name=post_data.get("device_name"),
                    device_type=post_data.get("device_type"),
                    location=post_data.get("location"),
                    firmware_version=post_data.get("firmware_version"),
                    tags=post_data.get("tags"),
                    owner_team=post_data.get("owner_team"),
                )
                self._send_json(200, {"status": "success", "flow": 4, "handled_by_node": NODE_NAME,
                                      "target_queue": QUEUE_FLOW4_P1_TO_P2, "payload": p})

            elif path == "/api/devices/query":
                if not self._require_authz("publish", "flow:5"):
                    return
                p = publish_flow5_message(filters=post_data.get("filters", post_data))
                self._send_json(200, {"status": "success", "flow": 5, "handled_by_node": NODE_NAME,
                                      "target_queue": QUEUE_FLOW5_P1_TO_P2, "payload": p})

            elif path == "/zk/config":
                if not self._require_admin():
                    return
                key, value = post_data.get("key"), post_data.get("value")
                if not key or value is None:
                    self._send_json(400, {"error": "Both 'key' and 'value' are required."})
                    return
                ok = zklib.write_config(key, value)
                if ok:
                    self._send_json(200, {"status": "ok", "key": key, "value": value,
                                          "note": "Propagates to all watchers within seconds."})
                else:
                    self._fail(400, "Unknown config key")
            else:
                self._send_json(404, {"error": "Not Found"})
        except (ValueError, TypeError) as e:
            # int()/float() on a client-supplied value, e.g. {"item_id": "abc"}: the client's mistake, not a server fault
            self._fail(400, "Invalid field value: item_id, counter, value, remote_metric and count must be numbers", e)
        except Exception as e:
            self._fail(500, "Internal Server Error", e)

    def do_PUT(self):
        path = urlparse(self.path).path.rstrip("/")
        # PUT /api/devices/{device_id}  — update a device (Flow 6)
        if path.startswith("/api/devices/"):
            device_id = path[len("/api/devices/"):]
            if not device_id:
                self._send_json(400, {"error": "device_id is required in URL path"})
                return
            if not self._require_auth():
                return
            if not self._require_authz("publish", "flow:6"):
                return
            ok, put_data = self._read_json_body()
            if not ok:
                return
            try:
                p = publish_flow6_message(device_id=device_id, updates=put_data)
                self._send_json(200, {"status": "success", "flow": 6, "handled_by_node": NODE_NAME,
                                      "target_queue": QUEUE_FLOW6_P1_TO_P2, "payload": p})
            except (ValueError, TypeError) as e:
                self._fail(400, "Invalid field value", e)
            except Exception as e:
                self._fail(500, "Internal Server Error", e)
        else:
            self._send_json(404, {"error": "Not Found"})

    def do_DELETE(self):
        path = urlparse(self.path).path.rstrip("/")
        # DELETE /api/devices/{device_id}  — soft-delete a device (Flow 7)
        if path.startswith("/api/devices/"):
            device_id = path[len("/api/devices/"):]
            if not device_id:
                self._send_json(400, {"error": "device_id is required in URL path"})
                return
            if not self._require_auth():
                return
            if not self._require_authz("publish", "flow:7"):
                return
            try:
                p = publish_flow7_message(device_id=device_id)
                self._send_json(200, {"status": "success", "flow": 7, "handled_by_node": NODE_NAME,
                                      "target_queue": QUEUE_FLOW7_P1_TO_P2, "payload": p})
            except (ValueError, TypeError) as e:
                self._fail(400, "Invalid field value", e)
            except Exception as e:
                self._fail(500, "Internal Server Error", e)
        else:
            self._send_json(404, {"error": "Not Found"})

    def log_message(self, fmt, *args):
        log.info(f"[http] {self.address_string()} — {fmt % args}")


# ---------------------------------------------------------------------------
# Greenthread workers
# ---------------------------------------------------------------------------

TLS_HANDSHAKE_TIMEOUT_S = 10.0


def _process_request_tls(httpd, plain_process_request, sock, addr):
    """Wrap an accepted socket in TLS (server side) and serve it. Runs in the request's own greenthread.

    A failed handshake - no/untrusted/revoked client certificate, wrong protocol version, a plain-HTTP client,
    a client that stalls - is logged at WARNING and the connection is closed; nothing reaches the handler."""
    try:
        sock.settimeout(TLS_HANDSHAKE_TIMEOUT_S)
        tsock = httpd.tls_context.wrap_socket(sock, server_side=True)
        tsock.settimeout(None)
    except Exception as e:
        log.warning(f"[tls] {addr[0]}:{addr[1]} handshake refused — {type(e).__name__}: {e}")
        try:
            sock.close()
        except Exception:
            pass
        return
    plain_process_request(tsock, addr)


def _gt_rmq_heartbeat():
    """
    Dedicated greenthread: sends a pika heartbeat every 30s to keep the
    publish connection alive during idle periods.
    Pika's BlockingConnection does not send heartbeats unless you call
    process_data_events(), which this greenthread does co-operatively.
    """
    while not gt.is_stopping():
        try:
            for slot in _publish_slots:
                with slot.sem:                      # never run on a connection that a publish is using
                    if slot.conn and not slot.conn.is_closed:
                        slot.conn.process_data_events(time_limit=0)
        except Exception as e:
            log.error(f"heartbeat error: {e}", exc_info=True)
        gt.sleep(30)


def _gt_zk_health_reporter():
    """Periodic greenthread: writes this node's health state to ZooKeeper."""
    from src.zookeeper import zk as _zk
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
    httpd.socket = eventlet.listen((API_HOST, API_BACKEND_PORT), backlog=API_LISTEN_BACKLOG)
    scheme = "http"
    if TLS_API_ENABLED:
        # Fail closed: a bad certificate/key raises TLSConfigError and the server does not start in plaintext.
        # The TLS handshake is done per connection inside the request greenthread (_process_request_tls), so a
        # slow or hostile client cannot block the accept loop.
        from src.tls.tls import server_context
        httpd.tls_context = server_context()
        _plain = httpd.process_request            # the original bound method: handles one request, then closes
        httpd.process_request = lambda s, a: _process_request_tls(httpd, _plain, s, a)
        scheme = "https"
    pool_size = API_REQUEST_POOL_SIZE or gt.GT_WORKER_CONCURRENCY * 10
    pool = eventlet.GreenPool(size=pool_size)
    log.info(f"{scheme.upper()} server listening on {API_HOST}:{API_BACKEND_PORT} "
             f"(GreenPool size={pool_size}, listen backlog={API_LISTEN_BACKLOG}"
             + (f", client certificates: {TLS_API_CLIENT_AUTH}" if TLS_API_ENABLED else "") + ")")
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
                    log.error(f"HTTP accept error: {e}", exc_info=True)
    finally:
        try:
            httpd.server_close()
        except Exception:
            pass
        log.info("HTTP server stopped.")


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

    log.info(f"All greenthreads started: {gt.list_greenthreads()}")
    log.info(f"ZK config endpoints live: GET /zk/health  GET /zk/config  POST /zk/config")

    # Wait for shutdown signal then stop all greenthreads
    gt.stop_event.wait()
    log.info("Shutdown signal — stopping all greenthreads...")
    for slot in _publish_slots:
        try:
            if slot.conn and not slot.conn.is_closed:
                slot.conn.close()
        except Exception as e:
            log.warning(f"closing publisher connection {slot.index}: {e}")
    gt.wait_all()


def main():
    global _config_watcher

    log.info(f"Starting (node={NODE_NAME}, pool={gt.GT_POOL_SIZE}) ...")
    metrics.start("process1")

    # ZooKeeper: connect, register, config watch
    try:
        zklib.register_service("process1", NODE_NAME)
        _config_watcher = zklib.ConfigWatcher()
        log.info(f"ZK ready. Config: {_config_watcher.get_all()}")
    except Exception as e:
        log.warning(f"ZK unavailable ({e}) — degraded mode.")
        _config_watcher = None

    # Leader election
    try:
        election = zklib.LeaderElection("process1", NODE_NAME)
        election.run(_run_api_server)
    except Exception as e:
        log.warning(f"ZK election failed ({e}) — single-node mode.")
        _run_api_server()
    finally:
        zklib.close_client()


if __name__ == "__main__":
    main()
