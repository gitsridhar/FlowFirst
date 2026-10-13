"""
gt.py — Greenthread utilities for the FlowFirst pipeline (eventlet-based).

This module provides:

1. monkey_patch()
   Call once at the very top of each process entry point, before any other
   import.  Patches stdlib socket/time/threading/select so that pika, kazoo,
   pymysql and the http.server all become co-operatively concurrent without
   any code changes to those libraries.

2. GreenPool
   Thin wrapper around eventlet.GreenPool that tracks named greenthreads,
   reports their status, and re-spawns crashed workers automatically.

3. spawn(name, fn, *args, **kwargs)
   Spawn a named greenthread via the module-level pool.

4. periodic(name, fn, interval_s)
   Spawn a greenthread that calls fn() every interval_s seconds.
   Exceptions are logged and swallowed so the periodic survives transient errors.

5. wait_all()
   Block the main greenthread until all tracked greenthreads finish
   (used at process shutdown).

6. GracefulShutdown
   Catches SIGTERM/SIGINT and sets a shared stop_event so all greenthreads
   can exit cleanly.

7. metrics_counter / metrics_snapshot
   Module-level atomic counters for per-greenthread throughput reporting.
   Each greenthread increments its own counter; the metrics reporter greenthread
   snapshots them every GT_METRICS_INTERVAL_S seconds and prints a summary.

Usage pattern (in each processX.py):
    from src.gt import gt            # import before anything else
    gt.monkey_patch()                # patch stdlib immediately

    # ... rest of imports ...

    def worker_fn():
        while not gt.stop_event.is_set():
            # ... do work ...
            gt.metrics_counter["flow1_consumed"] += 1
            gt.sleep(0)              # yield to other greenthreads

    gt.spawn("flow1_worker", worker_fn)
    gt.periodic("health_reporter", report_health, 30)
    gt.wait_all()
"""

# ---------------------------------------------------------------------------
# IMPORTANT: monkey_patch() MUST be called before any other import in the
# process entry point.  This module is safe to import early because it only
# imports eventlet at the top level (eventlet has no stdlib side-effects on
# import — only on monkey_patch()).
# ---------------------------------------------------------------------------

import logging
import os
import signal
import sys
import time as _stdlib_time
from collections import defaultdict
from typing import Callable, Optional

import eventlet
import eventlet.event
import eventlet.greenpool
import eventlet.queue

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# 1. Monkey-patching
# ---------------------------------------------------------------------------

_patched = False
_logging_configured = False


def _log_process_name() -> str:
    """'process1' for src/application/process1.py (also under `python -m`)."""
    return os.path.splitext(os.path.basename(sys.argv[0] or "flowfirst"))[0] or "flowfirst"


def configure_logging():
    """
    Send FlowFirst log records to stdout (-> journald) AND to a rotating file.

    All of gt.py / zk.py / swift_storage.py / process*.py log through the standard
    `logging` module; without this only WARNING and above would be emitted (via
    logging's last-resort handler).

    Environment (read from the process environment or the repo-root .env):
      LOG_LEVEL         DEBUG | INFO | WARNING | ERROR   (default INFO)
      LOG_DIR           directory for log files          (default /var/log/flowfirst)
                        set to an empty string to disable the file handler
      LOG_MAX_BYTES     rotate a file at this size       (default 52428800 = 50 MiB)
      LOG_BACKUP_COUNT  rotated files kept per process   (default 7)

    The file is <LOG_DIR>/<process>.log (e.g. process1.log), one per process, so
    processes never share a file. File lines carry a timestamp; the stdout format
    does not because journald adds its own. If the directory cannot be created or
    written, a warning goes to stderr and logging continues on stdout only.
    Chatty third-party libraries stay at WARNING. Uncaught exceptions are logged
    at CRITICAL with a traceback so a crash always reaches the file.
    """
    global _logging_configured
    if _logging_configured:
        return
    try:                                   # honour .env for LOG_* when run by hand
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass
    level_name = os.getenv("LOG_LEVEL", "INFO").strip().upper()
    level = getattr(logging, level_name, None)
    if not isinstance(level, int):
        level = logging.INFO
    root = logging.getLogger()
    root.setLevel(level)

    stream = logging.StreamHandler(sys.stdout)
    stream.setFormatter(logging.Formatter("%(levelname)s %(message)s"))
    root.addHandler(stream)

    log_dir = os.getenv("LOG_DIR", "/var/log/flowfirst").strip()
    log_file = None
    if log_dir:
        try:
            from logging.handlers import RotatingFileHandler
            os.makedirs(log_dir, exist_ok=True)
            log_file = os.path.join(log_dir, f"{_log_process_name()}.log")
            fh = RotatingFileHandler(
                log_file,
                maxBytes=int(os.getenv("LOG_MAX_BYTES", "52428800")),
                backupCount=int(os.getenv("LOG_BACKUP_COUNT", "7")),
                encoding="utf-8",
            )
            fh.setFormatter(logging.Formatter("%(asctime)s %(levelname)-8s %(name)s: %(message)s"))
            root.addHandler(fh)
        except (OSError, ValueError) as e:
            log_file = None
            print(f"[gt] WARNING: file logging disabled — cannot use LOG_DIR='{log_dir}': {e}",
                  file=sys.stderr)

    for noisy in ("pika", "kazoo", "urllib3", "swiftclient"):
        logging.getLogger(noisy).setLevel(max(level, logging.WARNING))

    def _log_uncaught(exc_type, exc, tb):
        if issubclass(exc_type, KeyboardInterrupt):
            sys.__excepthook__(exc_type, exc, tb)
            return
        logging.getLogger("flowfirst").critical("Uncaught exception", exc_info=(exc_type, exc, tb))
    sys.excepthook = _log_uncaught

    _logging_configured = True
    if log_file:
        logging.getLogger("flowfirst").info(
            "[gt] Logging to %s (level=%s, rotate at %s bytes, keep %s)",
            log_file, level_name, os.getenv("LOG_MAX_BYTES", "52428800"),
            os.getenv("LOG_BACKUP_COUNT", "7"))


def monkey_patch():
    """
    Apply eventlet cooperative monkey-patch to the stdlib.

    Safe to call multiple times — subsequent calls are no-ops.
    Must be called BEFORE importing pika, kazoo, pymysql, socket, threading, etc.
    """
    global _patched
    if _patched:
        return
    eventlet.monkey_patch(
        os=True,
        select=True,
        socket=True,
        thread=True,
        time=True,
        builtins=False,   # do NOT patch builtins — breaks some stdlib internals
    )
    _patched = True
    configure_logging()   # after patching, so the handler's lock is cooperative
    log.info("[gt] eventlet monkey-patch applied.")


# After monkey_patch() the stdlib sleep becomes cooperative; expose it here
# so callers can do `gt.sleep(0)` to yield without importing eventlet directly.
def sleep(seconds: float = 0):
    """Yield the current greenthread for at least `seconds` seconds."""
    eventlet.sleep(seconds)


# ---------------------------------------------------------------------------
# 2. Graceful shutdown
# ---------------------------------------------------------------------------

stop_event = eventlet.event.Event()   # set() signals all workers to stop


def _handle_signal(signum, frame):
    sig_name = "SIGTERM" if signum == signal.SIGTERM else "SIGINT"
    log.info("[gt] %s received — setting stop_event for graceful shutdown.", sig_name)
    if not stop_event.ready():
        stop_event.send(sig_name)


def register_signal_handlers():
    """Register SIGTERM and SIGINT handlers. Call once in main()."""
    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT,  _handle_signal)
    log.info("[gt] Signal handlers registered (SIGTERM, SIGINT).")


def is_stopping() -> bool:
    """Return True once a shutdown signal has been received."""
    return stop_event.ready()


# ---------------------------------------------------------------------------
# 3. Metrics counters
# ---------------------------------------------------------------------------

metrics_counter: dict = defaultdict(int)   # key → count since process start
_metrics_snapshots: list = []              # list of (timestamp, snapshot_dict)

GT_METRICS_INTERVAL_S = int(os.getenv("GT_METRICS_INTERVAL_S", "60"))
GT_POOL_SIZE          = int(os.getenv("GT_POOL_SIZE", "1000"))
GT_WORKER_CONCURRENCY = int(os.getenv("GT_WORKER_CONCURRENCY", "4"))


def metrics_snapshot() -> dict:
    """Return a copy of the current counters."""
    return dict(metrics_counter)


# ---------------------------------------------------------------------------
# 4. Named greenthread pool
# ---------------------------------------------------------------------------

_pool = eventlet.greenpool.GreenPool(size=GT_POOL_SIZE)
_greenthreads: dict = {}     # name → GreenThread handle
_restart_policy: dict = {}   # name → bool (auto-restart on exception)


def spawn(
    name: str,
    fn: Callable,
    *args,
    restart_on_error: bool = False,
    **kwargs,
):
    """
    Spawn a named greenthread.

    If restart_on_error=True, the greenthread is automatically re-spawned
    after a 2-second delay if it raises an unhandled exception.
    """
    def _wrapper():
        while True:
            try:
                fn(*args, **kwargs)
            except Exception as exc:
                log.error("[gt] Greenthread '%s' raised: %s", name, exc, exc_info=True)
                if not _restart_policy.get(name, False):
                    log.info("[gt] '%s' will NOT be restarted (restart_on_error=False).", name)
                    break
                log.info("[gt] Restarting '%s' in 2s...", name)
                eventlet.sleep(2)
            else:
                # Clean exit
                break
        _greenthreads.pop(name, None)
        log.info("[gt] Greenthread '%s' exited.", name)

    _restart_policy[name] = restart_on_error
    handle = _pool.spawn(_wrapper)
    _greenthreads[name] = handle
    log.info("[gt] Spawned greenthread '%s'.", name)
    return handle


def spawn_consumers(name: str, fn: Callable, count: int = 1):
    """
    Spawn `count` copies of a consumer greenthread (auto-restarted on error).
    The first keeps `name`; the others are named `name_2`, `name_3`, ...
    Each copy must open its own RabbitMQ connection (fn already does).
    """
    for i in range(1, max(1, int(count)) + 1):
        spawn(name if i == 1 else f"{name}_{i}", fn, restart_on_error=True)


def spawn_n(name: str, fn: Callable, *args, restart_on_error: bool = False, **kwargs):
    """Fire-and-forget variant of spawn() — returns immediately."""
    spawn(name, fn, *args, restart_on_error=restart_on_error, **kwargs)


def kill(name: str):
    """Kill a named greenthread if it is still running."""
    gt = _greenthreads.pop(name, None)
    if gt:
        gt.kill()
        log.info("[gt] Killed greenthread '%s'.", name)


def list_greenthreads() -> list:
    """Return list of currently running greenthread names."""
    return list(_greenthreads.keys())


def wait_all():
    """Block until all tracked greenthreads have finished."""
    _pool.waitall()


# ---------------------------------------------------------------------------
# 5. Periodic greenthread helper
# ---------------------------------------------------------------------------

def periodic(
    name: str,
    fn: Callable,
    interval_s: float,
    run_immediately: bool = True,
):
    """
    Spawn a greenthread that calls fn() every interval_s seconds.

    Exceptions inside fn() are logged and swallowed so the periodic
    survives transient errors (e.g. ZooKeeper blip).
    The loop exits when gt.stop_event is set.
    """
    def _loop():
        if not run_immediately:
            eventlet.sleep(interval_s)
        while not is_stopping():
            try:
                fn()
            except Exception as exc:
                log.warning("[gt] periodic '%s' error (continuing): %s", name, exc)
            eventlet.sleep(interval_s)
        log.info("[gt] periodic '%s' stopped.", name)

    return spawn(name, _loop, restart_on_error=False)


# ---------------------------------------------------------------------------
# 6. Metrics reporter (used by all processes)
# ---------------------------------------------------------------------------

def start_metrics_reporter(process_name: str):
    """
    Spawn a periodic greenthread that logs per-process throughput every
    GT_METRICS_INTERVAL_S seconds.
    """
    _prev: dict = {}

    def _report():
        current = metrics_snapshot()
        lines = [f"[gt][{process_name}] ── Throughput Report ──"]
        for key in sorted(current):
            prev_val = _prev.get(key, 0)
            delta = current[key] - prev_val
            lines.append(f"  {key}: total={current[key]}  +{delta} in last {GT_METRICS_INTERVAL_S}s")
            _prev[key] = current[key]
        lines.append(f"  active_greenthreads: {list_greenthreads()}")
        log.info("\n".join(lines))

    periodic(f"{process_name}_metrics", _report, GT_METRICS_INTERVAL_S)


# ---------------------------------------------------------------------------
# 7. RabbitMQ connection helper for greenthreads
#    Each greenthread must own its own connection — BlockingConnection is NOT
#    thread-safe.  This helper wraps get_connection() with retry so greenthreads
#    that start before RabbitMQ is ready will wait and retry co-operatively.
# ---------------------------------------------------------------------------

def get_rmq_connection_with_retry(label: str, max_attempts: int = 20):
    """
    Call config.get_connection() with co-operative retry.
    Sleeps eventlet.sleep(5) between attempts so other greenthreads run.
    Returns a connected pika.BlockingConnection.
    """
    from src.config.config import get_connection   # imported here to respect monkey-patch order
    for attempt in range(1, max_attempts + 1):
        try:
            conn = get_connection()
            log.info("[gt] [%s] RabbitMQ connected at %s", label, conn._connected_to)
            return conn
        except Exception as exc:
            log.warning(
                "[gt] [%s] RabbitMQ not ready (attempt %d/%d): %s — retrying in 5s...",
                label, attempt, max_attempts, exc,
            )
            eventlet.sleep(5)
    raise RuntimeError(f"[gt] [{label}] Could not connect to RabbitMQ after {max_attempts} attempts.")
