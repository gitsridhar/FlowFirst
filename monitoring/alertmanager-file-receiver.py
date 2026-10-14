#!/usr/bin/env python3
"""FlowFirst alert file receiver.

Listens on 127.0.0.1:ALERT_RECEIVER_PORT (default 9099) for Alertmanager webhook calls and
appends each alert as a JSON line to ALERT_LOG_FILE (default /var/log/flowfirst/alerts.log).

    python3 monitoring/alertmanager-file-receiver.py

Environment variables (all optional):
    ALERT_LOG_FILE         path to the log file (default /var/log/flowfirst/alerts.log)
    ALERT_RECEIVER_PORT    port to listen on (default 9099)
    ALERT_RECEIVER_HOST    bind address (default 127.0.0.1)

Log format: one JSON object per line, each containing:
    ts          ISO-8601 UTC timestamp of receipt
    status      "firing" or "resolved"
    alerts      list of alert objects from Alertmanager
    groupLabels common labels of the group
    commonLabels common labels across all alerts in the group

The file is opened in append mode on every write (so log rotation with copytruncate or
a tool that creates a new file is fine without restarting this process).

Run under systemd alongside Alertmanager:
    ExecStart=/opt/flowfirst/.venv/bin/python3 /opt/flowfirst/monitoring/alertmanager-file-receiver.py
    Restart=on-failure
    StandardOutput=journal
    StandardError=journal
"""
import http.server
import json
import logging
import os
import sys
from datetime import datetime, timezone

LOG_FILE = os.environ.get("ALERT_LOG_FILE", "/var/log/flowfirst/alerts.log")
PORT = int(os.environ.get("ALERT_RECEIVER_PORT", "9099"))
HOST = os.environ.get("ALERT_RECEIVER_HOST", "127.0.0.1")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", stream=sys.stdout)
log = logging.getLogger("alert-receiver")


class Handler(http.server.BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        pass  # silence the default access log; we write our own lines

    def do_POST(self):
        try:
            length = int(self.headers.get("Content-Length", "0"))
            body = self.rfile.read(length)
            payload = json.loads(body) if body else {}
        except Exception as exc:
            log.warning("bad request body: %s", exc)
            self._reply(400)
            return

        try:
            entry = {
                "ts": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                "status": payload.get("status", "?"),
                "groupLabels": payload.get("groupLabels", {}),
                "commonLabels": payload.get("commonLabels", {}),
                "alerts": payload.get("alerts", []),
            }
            with open(LOG_FILE, "a", encoding="utf-8") as fh:
                fh.write(json.dumps(entry, separators=(",", ":")) + "\n")
        except Exception as exc:
            log.error("could not write to %s: %s", LOG_FILE, exc)
            # still return 200 so Alertmanager does not mark the receiver as down
        else:
            n = len(entry["alerts"])
            log.info("%s: %d alert(s) (%s) -> %s",
                     entry["status"], n,
                     ", ".join(a.get("labels", {}).get("alertname", "?") for a in entry["alerts"][:5]),
                     LOG_FILE)
        self._reply(200)

    def do_GET(self):
        # Alertmanager sends a GET / to check the webhook is alive; answer 200.
        self._reply(200, b"ok\n")

    def _reply(self, code, body=b""):
        self.send_response(code)
        self.send_header("Content-Type", "text/plain")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        if body:
            self.wfile.write(body)


def main():
    log.info("alert file receiver starting on %s:%d -> %s", HOST, PORT, LOG_FILE)
    # Ensure the log directory exists
    log_dir = os.path.dirname(LOG_FILE)
    if log_dir:
        os.makedirs(log_dir, exist_ok=True)
    server = http.server.HTTPServer((HOST, PORT), Handler)
    log.info("listening (Ctrl-C to stop)")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    log.info("stopped")


if __name__ == "__main__":
    main()
