#!/usr/bin/env python3
"""
Minimal Alertmanager webhook receiver.

Logs every alert it receives to stdout (visible via `docker logs cdc-alert-webhook`).
This is a local stand-in for a real notifier (PagerDuty/Slack/email) so the full
alerting loop is demonstrable without external credentials.
"""

import json
import logging
from http.server import BaseHTTPRequestHandler, HTTPServer

logging.basicConfig(level=logging.INFO, format="%(asctime)s [alert-webhook] %(message)s")
PORT = 5001


class Handler(BaseHTTPRequestHandler):
    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(length)
        try:
            data = json.loads(body)
            for alert in data.get("alerts", []):
                labels = alert.get("labels", {})
                ann = alert.get("annotations", {})
                logging.info(
                    "%s | severity=%s | %s :: %s",
                    alert.get("status", "?").upper(),
                    labels.get("severity", "-"),
                    labels.get("alertname", "-"),
                    ann.get("description", ann.get("summary", "")),
                )
        except (ValueError, TypeError) as exc:
            logging.error("could not parse alert payload: %s", exc)
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"ok")

    def do_GET(self):
        # Health endpoint.
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"alert-webhook up")

    def log_message(self, *args):
        pass  # silence default per-request access logging


if __name__ == "__main__":
    logging.info("listening on :%d", PORT)
    HTTPServer(("0.0.0.0", PORT), Handler).serve_forever()
