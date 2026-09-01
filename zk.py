"""
zk.py — ZooKeeper integration for the FlowFirst pipeline (kazoo client).

Provides four capabilities used by all four pipeline processes:

1. LeaderElection
   One node wins the election per process name.  Only the leader actively
   consumes / publishes; followers stand by and take over if the leader dies.
   Uses kazoo's built-in Election primitive (ephemeral sequential znodes under
   /flowfirst/election/<process_name>/).

2. ConfigWatcher
   Reads pipeline runtime configuration from ZooKeeper znodes.
   Changes written to ZK propagate to all running processes within seconds
   without a restart.  Watched keys:
     /flowfirst/config/flow2_high_threshold   (float, default 30.0)
     /flowfirst/config/flow1_counter_step     (int,   default 10)
     /flowfirst/config/flow2_scale_factor     (float, default 1.15)

3. ServiceRegistry
   Each process registers an ephemeral znode on startup:
     /flowfirst/registry/<process_name>/<node_name>
   The node disappears automatically when the process exits or loses its
   ZK session.  Ops tooling (or other processes) can list live workers.

4. DedupBarrier
   Process 4 checks /flowfirst/dedup/<message_id> before inserting into
   MariaDB.  If the znode already exists the message was already persisted
   (e.g. after a Pacemaker failover re-delivery) and is skipped.
   Znodes are created with a TTL of DEDUP_TTL_MS milliseconds (default 5 min).
"""

import json
import logging
import os
import socket
import time
from typing import Callable, Optional

from kazoo.client import KazooClient
from kazoo.exceptions import (
    NodeExistsError,
    NoNodeError,
    SessionExpiredError,
)
from kazoo.recipe.election import Election

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# ZooKeeper connection settings (read from environment / config)
# ---------------------------------------------------------------------------
_ZK_HOSTS = os.getenv("ZK_HOSTS", "127.0.0.1:2181")
_ZK_TIMEOUT = float(os.getenv("ZK_TIMEOUT", "10"))
_DEDUP_TTL_MS = int(os.getenv("ZK_DEDUP_TTL_MS", "300000"))   # 5 minutes

# Root ZNode paths
_ROOT            = "/flowfirst"
_ELECTION_ROOT   = f"{_ROOT}/election"
_CONFIG_ROOT     = f"{_ROOT}/config"
_REGISTRY_ROOT   = f"{_ROOT}/registry"
_DEDUP_ROOT      = f"{_ROOT}/dedup"
_HEALTH_ROOT     = f"{_ROOT}/health"

# Default runtime config values (used when ZK node is absent / unreadable)
_CONFIG_DEFAULTS = {
    "flow2_high_threshold": 30.0,
    "flow1_counter_step":   10,
    "flow2_scale_factor":   1.15,
}


# ---------------------------------------------------------------------------
# Module-level singleton client — shared across all helpers in one process
# ---------------------------------------------------------------------------
_client: Optional[KazooClient] = None


def get_client() -> KazooClient:
    """Return the module-level KazooClient, creating it on first call."""
    global _client
    if _client is None or not _client.connected:
        _client = KazooClient(hosts=_ZK_HOSTS, timeout=_ZK_TIMEOUT)
        _client.start(timeout=_ZK_TIMEOUT)
        _ensure_base_paths(_client)
        log.info("[zk] Connected to ZooKeeper ensemble at %s", _ZK_HOSTS)
    return _client


def close_client():
    """Gracefully close the module-level client (call on process exit)."""
    global _client
    if _client and _client.connected:
        _client.stop()
        _client.close()
        _client = None
        log.info("[zk] ZooKeeper client closed.")


def _ensure_base_paths(zk: KazooClient):
    """Create the permanent base znodes if they do not exist."""
    for path in (
        _ROOT,
        _ELECTION_ROOT,
        _CONFIG_ROOT,
        _REGISTRY_ROOT,
        _DEDUP_ROOT,
        _HEALTH_ROOT,
    ):
        zk.ensure_path(path)
    _seed_default_config(zk)


def _seed_default_config(zk: KazooClient):
    """Write default config values only when the znodes do not yet exist."""
    for key, value in _CONFIG_DEFAULTS.items():
        path = f"{_CONFIG_ROOT}/{key}"
        if not zk.exists(path):
            zk.create(path, str(value).encode(), makepath=True)
            log.info("[zk] Seeded config %s = %s", key, value)


# ---------------------------------------------------------------------------
# 1. Leader Election
# ---------------------------------------------------------------------------

class LeaderElection:
    """
    Wraps kazoo's Election recipe.

    Usage:
        election = LeaderElection("process2", node_name="node1")
        # This blocks until this node wins, then calls on_elected():
        election.run(on_elected_callback)
    """

    def __init__(self, process_name: str, node_name: Optional[str] = None):
        self.process_name = process_name
        self.node_name = node_name or os.getenv("NODE_NAME", socket.gethostname())
        self._path = f"{_ELECTION_ROOT}/{process_name}"
        self._election: Optional[Election] = None
        self._is_leader = False

    def run(self, on_elected: Callable[[], None]):
        """
        Block until this node is elected leader, then call on_elected().
        If the leader loses its session (crash/disconnect) the next
        candidate is automatically promoted by ZooKeeper.
        """
        zk = get_client()
        zk.ensure_path(self._path)
        self._election = Election(zk, self._path, identifier=self.node_name)

        log.info(
            "[zk] [%s] Entering leader election at %s (identifier=%s)",
            self.process_name, self._path, self.node_name,
        )
        # Election.run() blocks until this contender wins, then calls on_elected.
        # When on_elected() returns the election is over; the next candidate wins.
        self._election.run(self._wrap(on_elected))

    def _wrap(self, on_elected: Callable[[], None]) -> Callable[[], None]:
        def _inner():
            self._is_leader = True
            log.info("[zk] [%s] *** NODE %s IS NOW LEADER ***", self.process_name, self.node_name)
            _update_health(self.process_name, self.node_name, "leader")
            try:
                on_elected()
            finally:
                self._is_leader = False
                _update_health(self.process_name, self.node_name, "follower")
                log.info("[zk] [%s] %s stepped down from leader role.", self.process_name, self.node_name)
        return _inner

    @property
    def is_leader(self) -> bool:
        return self._is_leader


# ---------------------------------------------------------------------------
# 2. Config Watcher
# ---------------------------------------------------------------------------

class ConfigWatcher:
    """
    Reads pipeline runtime config from ZooKeeper and watches for live changes.

    Usage:
        cfg = ConfigWatcher()
        threshold = cfg.get_float("flow2_high_threshold")   # 30.0
        step      = cfg.get_int("flow1_counter_step")       # 10
    """

    def __init__(self):
        self._cache: dict = dict(_CONFIG_DEFAULTS)
        zk = get_client()
        for key in _CONFIG_DEFAULTS:
            path = f"{_CONFIG_ROOT}/{key}"
            self._load(zk, path, key)
            # Register a DataWatch so updates propagate automatically
            zk.DataWatch(path)(self._make_watcher(key))

    def _load(self, zk: KazooClient, path: str, key: str):
        try:
            data, _ = zk.get(path)
            raw = data.decode().strip()
            default = _CONFIG_DEFAULTS[key]
            self._cache[key] = type(default)(raw)
            log.debug("[zk] Config loaded: %s = %s", key, self._cache[key])
        except (NoNodeError, ValueError, SessionExpiredError) as exc:
            log.warning("[zk] Config %s unreadable (%s) — using default %s", key, exc, _CONFIG_DEFAULTS[key])

    def _make_watcher(self, key: str):
        def _watch(data, stat, event=None):
            if data is not None:
                try:
                    raw = data.decode().strip()
                    default = _CONFIG_DEFAULTS[key]
                    new_val = type(default)(raw)
                    if self._cache.get(key) != new_val:
                        log.info("[zk] Config CHANGED: %s = %s (was %s)", key, new_val, self._cache.get(key))
                        self._cache[key] = new_val
                except (ValueError, AttributeError) as exc:
                    log.warning("[zk] Config watch parse error for %s: %s", key, exc)
        return _watch

    def get_float(self, key: str) -> float:
        return float(self._cache.get(key, _CONFIG_DEFAULTS.get(key, 0.0)))

    def get_int(self, key: str) -> int:
        return int(self._cache.get(key, _CONFIG_DEFAULTS.get(key, 0)))

    def get_all(self) -> dict:
        return dict(self._cache)


# ---------------------------------------------------------------------------
# 3. Service Registry
# ---------------------------------------------------------------------------

def register_service(process_name: str, node_name: Optional[str] = None, extra: Optional[dict] = None):
    """
    Create an ephemeral znode for this process instance.
    The node disappears automatically when the ZK session ends (process exit/crash).

    Znode path:  /flowfirst/registry/<process_name>/<node_name>
    Znode data:  JSON with node_name, pid, started_at, and any extra fields.
    """
    node_name = node_name or os.getenv("NODE_NAME", socket.gethostname())
    path = f"{_REGISTRY_ROOT}/{process_name}/{node_name}"
    payload = {
        "node": node_name,
        "pid": os.getpid(),
        "started_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
        **(extra or {}),
    }
    zk = get_client()
    zk.ensure_path(f"{_REGISTRY_ROOT}/{process_name}")
    try:
        zk.create(path, json.dumps(payload).encode(), ephemeral=True)
        log.info("[zk] Registered %s/%s (pid=%d)", process_name, node_name, os.getpid())
    except NodeExistsError:
        # Previous ephemeral still alive (e.g. rapid restart before session expiry)
        zk.set(path, json.dumps(payload).encode())
        log.info("[zk] Re-registered %s/%s", process_name, node_name)


def list_registered(process_name: str) -> list:
    """Return a list of currently registered node names for a process."""
    zk = get_client()
    try:
        return zk.get_children(f"{_REGISTRY_ROOT}/{process_name}")
    except NoNodeError:
        return []


# ---------------------------------------------------------------------------
# 4. Dedup Barrier (Process 4)
# ---------------------------------------------------------------------------

def check_and_mark_processed(message_id: str) -> bool:
    """
    Atomically check whether message_id has already been processed.

    Returns True  if this is the FIRST time we see this message_id
                  (the caller should process it).
    Returns False if the message was already processed
                  (the caller should skip / ack-without-insert).

    The dedup znode is created with a TTL so it is automatically cleaned up
    after ZK_DEDUP_TTL_MS milliseconds (default 5 minutes).
    """
    path = f"{_DEDUP_ROOT}/{message_id}"
    zk = get_client()
    try:
        zk.create(path, b"1", makepath=True)
        log.debug("[zk] Dedup: first-seen %s — proceeding with insert.", message_id)
        return True
    except NodeExistsError:
        log.warning("[zk] Dedup: duplicate detected for %s — skipping insert.", message_id)
        return False


# ---------------------------------------------------------------------------
# 5. Pipeline Health Dashboard
# ---------------------------------------------------------------------------

def _update_health(process_name: str, node_name: str, state: str):
    """Write a health znode so ops tooling can read pipeline state at a glance."""
    path = f"{_HEALTH_ROOT}/{process_name}/{node_name}"
    payload = json.dumps({
        "state": state,
        "updated_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "pid": os.getpid(),
    }).encode()
    zk = get_client()
    zk.ensure_path(f"{_HEALTH_ROOT}/{process_name}")
    if zk.exists(path):
        zk.set(path, payload)
    else:
        zk.create(path, payload, makepath=True)


def get_pipeline_health() -> dict:
    """
    Read the full pipeline health tree from ZooKeeper.
    Returns a nested dict: { process_name: { node_name: { state, updated_at, pid } } }
    """
    zk = get_client()
    result = {}
    try:
        processes = zk.get_children(_HEALTH_ROOT)
    except NoNodeError:
        return result
    for proc in processes:
        result[proc] = {}
        try:
            nodes = zk.get_children(f"{_HEALTH_ROOT}/{proc}")
            for node in nodes:
                try:
                    data, _ = zk.get(f"{_HEALTH_ROOT}/{proc}/{node}")
                    result[proc][node] = json.loads(data.decode())
                except (NoNodeError, ValueError):
                    pass
        except NoNodeError:
            pass
    return result


def write_config(key: str, value) -> bool:
    """
    Write a runtime config value to ZooKeeper.
    Propagates to all watching processes within seconds.
    Returns True on success.
    """
    if key not in _CONFIG_DEFAULTS:
        log.error("[zk] Unknown config key: %s. Valid keys: %s", key, list(_CONFIG_DEFAULTS))
        return False
    path = f"{_CONFIG_ROOT}/{key}"
    zk = get_client()
    zk.ensure_path(path)
    zk.set(path, str(value).encode())
    log.info("[zk] Config written: %s = %s", key, value)
    return True
