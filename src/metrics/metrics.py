"""Prometheus text-format metrics for Processes 1-5.

Every process calls start("processN") from main(). When METRICS_ENABLED is false (the default) nothing is opened.
When it is true the process serves GET /metrics on METRICS_PORT_BASE + N (process1 -> 9101 ... process5 -> 9105).

The listener is separate from the REST API port on purpose: it is not behind HAProxy (Prometheus must scrape each
node, including Node 4), and a problem with it can never affect the API. It uses the API certificate and
TLS_API_CLIENT_AUTH when TLS_API_ENABLED is true. The output holds counters and certificate expiry times only, never
message content. A metrics listener that cannot start (port in use, bad certificate) is logged and skipped: the
pipeline keeps running, and it never falls back to plaintext when TLS is on.
"""
import logging
import re
import time
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Callable, Dict, List, Tuple, Union
from urllib.parse import urlparse

import eventlet

from src.gt import gt
from src.config.config import (
    NODE_NAME,
    METRICS_ENABLED,
    METRICS_HOST,
    METRICS_PORT_BASE,
    TLS_API_ENABLED,
)

log = logging.getLogger("flowfirst.metrics")

CONTENT_TYPE = "text/plain; version=0.0.4; charset=utf-8"
_START_TIME = time.time()
_TLS_HANDSHAKE_TIMEOUT_S = 10.0

# Counters each process can emit. They are created at 0 when metrics start: a counter that only comes into existence
# on its first increment hides that first increment from Prometheus (increase() needs an earlier sample to compare).
_FLOWS = ("1", "2", "3")
EXPECTED_COUNTERS: Dict[str, List[str]] = {
    "process1": [f"flow{f}_published" for f in _FLOWS] + [f"http_{c}xx" for c in "2345"],
    "process2": [f"flow{f}_{e}" for f in _FLOWS for e in ("processed", "failed")],
    "process3": ["flow1_forwarded", "flow1_failed", "flow2_reflected", "flow2_failed"],
    "process4": ["dedup_reaped"] + [f"flow{f}_{e}" for f in _FLOWS
                                    for e in ("persisted", "swift_stored", "dedup_skipped", "failed")],
    "process5": ["flow3_processed", "flow3_failed"],
}

Sample = Union[float, int, List[Tuple[Dict[str, str], float]]]
_gauges: Dict[str, Tuple[str, Callable[[], Sample]]] = {}

_MESSAGES = re.compile(r"^flow(\d+)_(.+)$")      # flow1_published -> flow="1", event="published"
_HTTP = re.compile(r"^http_(\dxx)$")             # http_5xx        -> class="5xx"


def register_gauge(name: str, help_text: str, fn: Callable[[], Sample]) -> None:
    """Add a gauge evaluated at every scrape. fn returns a number, or a list of (labels, value) pairs."""
    _gauges[name] = (help_text, fn)


def _esc(value: str) -> str:
    return str(value).replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")


def _line(name: str, labels: Dict[str, str], value) -> str:
    inner = ",".join(f'{k}="{_esc(v)}"' for k, v in labels.items())
    return f"{name}{{{inner}}} {value}"


def render(process: str) -> str:
    """The whole /metrics body for this process."""
    base = {"node": NODE_NAME, "process": process}
    out: List[str] = []

    def family(name: str, kind: str, help_text: str, samples: List[Tuple[Dict[str, str], float]]) -> None:
        if not samples:
            return
        out.append(f"# HELP {name} {help_text}")
        out.append(f"# TYPE {name} {kind}")
        out.extend(_line(name, {**base, **labels}, value) for labels, value in samples)

    messages, events, http = [], [], []
    for key, value in sorted(gt.metrics_snapshot().items()):
        m, h = _MESSAGES.match(key), _HTTP.match(key)
        if m:
            messages.append(({"flow": m.group(1), "event": m.group(2)}, value))
        elif h:
            http.append(({"class": h.group(1)}, value))
        else:
            events.append(({"event": key}, value))

    family("flowfirst_messages_total", "counter",
           "Messages handled by this process since it started, by flow and event (published, processed, "
           "persisted, failed, ...).", messages)
    family("flowfirst_http_responses_total", "counter",
           "HTTP responses sent by the REST API by status class (Process 1 only).", http)
    family("flowfirst_events_total", "counter", "Other process events since start (e.g. dedup_reaped).", events)
    family("flowfirst_up", "gauge", "1 while the process is running and answering scrapes.", [({}, 1)])
    family("flowfirst_start_time_seconds", "gauge", "Unix time the process started.", [({}, int(_START_TIME))])
    family("flowfirst_greenthreads", "gauge",
           "Greenthreads this process is running. 0 on a standby (not-leader) process.",
           [({}, len(gt.list_greenthreads()))])
    family("flowfirst_stopping", "gauge", "1 once the process was asked to stop.",
           [({}, 1 if gt.is_stopping() else 0)])

    for name, (help_text, fn) in sorted(_gauges.items()):
        try:
            value = fn()
        except Exception as e:                       # one broken gauge must not blank the whole scrape
            log.warning(f"gauge {name} failed: {type(e).__name__}: {e}")
            continue
        family(name, "gauge", help_text, [({}, value)] if isinstance(value, (int, float)) else list(value))
    return "\n".join(out) + "\n"


def _cert_expiry() -> List[Tuple[Dict[str, str], float]]:
    """Seconds until each certificate this process presents or trusts expires (negative = already expired)."""
    from cryptography import x509
    from src.config import config as c

    files = {"api": c.TLS_API_CERT, "pipeline": c.TLS_CLIENT_CERT, "ca": c.TLS_CA_FILE}
    samples = []
    for label, path in files.items():
        try:
            with open(path, "rb") as fh:
                cert = x509.load_pem_x509_certificate(fh.read())
        except OSError:
            continue                                   # not present on this node / not used by this process
        samples.append(({"cert": label}, cert.not_valid_after_utc.timestamp() - time.time()))
    return samples


def _make_handler(process: str):
    class MetricsHandler(BaseHTTPRequestHandler):
        timeout = 10

        def do_GET(self):
            if urlparse(self.path).path != "/metrics":
                self.send_error(404)
                return
            body = render(process).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", CONTENT_TYPE)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, fmt, *args):
            log.debug("scrape %s", fmt % args)

    return MetricsHandler


def _serve(process: str, port: int) -> None:
    try:
        httpd = HTTPServer((METRICS_HOST, port), _make_handler(process), bind_and_activate=False)
        httpd.socket = eventlet.listen((METRICS_HOST, port), backlog=16)
        plain = httpd.process_request
        scheme = "http"
        if TLS_API_ENABLED:
            from src.tls.tls import server_context
            ctx = server_context()

            def tls_request(sock, addr):
                try:
                    sock.settimeout(_TLS_HANDSHAKE_TIMEOUT_S)
                    tsock = ctx.wrap_socket(sock, server_side=True)
                    tsock.settimeout(None)
                except Exception as e:
                    log.warning(f"[tls] metrics {addr[0]}:{addr[1]} handshake refused - {type(e).__name__}: {e}")
                    sock.close()
                    return
                plain(tsock, addr)

            httpd.process_request = tls_request
            scheme = "https"
            register_gauge("flowfirst_tls_cert_expiry_seconds",
                           "Seconds until a certificate used by this node expires (negative = expired).",
                           _cert_expiry)
    except Exception as e:
        log.error(f"metrics listener on {METRICS_HOST}:{port} NOT started: {type(e).__name__}: {e}")
        return

    pool = eventlet.GreenPool(size=8)
    log.info(f"metrics listening on {scheme}://{METRICS_HOST}:{port}/metrics")
    while not gt.is_stopping():
        try:
            httpd.socket.settimeout(1.0)
            sock, addr = httpd.socket.accept()
            pool.spawn_n(httpd.process_request, sock, addr)
        except Exception as e:
            if not gt.is_stopping() and "timed out" not in str(e).lower():
                log.error(f"metrics accept error: {e}")
    httpd.server_close()


def start(process: str) -> None:
    """Start the listener for 'process1'..'process5' unless METRICS_ENABLED is false. Call once, from main()."""
    if not METRICS_ENABLED:
        return
    for key in EXPECTED_COUNTERS.get(process, []):
        gt.metrics_counter[key] += 0
    port = METRICS_PORT_BASE + int(process[-1])
    eventlet.spawn(_serve, process, port)       # not a gt greenthread: it must never hold up gt.wait_all()
