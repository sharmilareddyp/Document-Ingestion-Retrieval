"""Sample paginated API used to demonstrate ingestion and schema drift.

Normal mode returns the contracted payload. Drift mode changes `priority` from an
integer to a string, which the pipeline classifies as breaking drift.
"""

from __future__ import annotations

import json
import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

GOOD = [
    {
        "id": "rec-1",
        "title": "Retention schedule",
        "body": "Customer contracts are retained for seven years after expiration.",
        "updated_at": "2024-05-01T00:00:00Z",
        "priority": 1,
        "tags": ["legal"],
        "owner": {"name": "Records"},
    },
    {
        "id": "rec-2",
        "title": "Incident escalation",
        "body": "Severity one incidents page the primary responder within fifteen minutes.",
        "updated_at": "2024-06-15T12:30:00Z",
        "priority": 2,
        "tags": ["security"],
        "owner": {"name": "Operations"},
    },
    {
        "id": "rec-3",
        "title": "Vendor review",
        "body": "Critical vendors are reviewed every quarter for access and subprocessors.",
        "updated_at": "2024-07-01T08:00:00Z",
        "priority": 3,
        "tags": ["risk"],
        "owner": {"name": "Procurement"},
    },
]


def drifted(record: dict) -> dict:
    copied = dict(record)
    copied["priority"] = str(record["priority"])
    copied["extra_note"] = "optional field added by the upstream API"
    return copied


class Handler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:  # noqa: N802
        path, _, query = self.path.partition("?")
        params = dict(part.split("=", 1) for part in query.split("&") if "=" in part)
        if path not in {"/records", "/drift/records"}:
            self._send(404, {"error": "not found"})
            return
        page = int(params.get("page", "1"))
        page_size = int(params.get("page_size", "2"))
        records = [drifted(item) if path.startswith("/drift/") else item for item in GOOD]
        start = (page - 1) * page_size
        window = records[start : start + page_size]
        self._send(200, {"items": window, "page": page, "page_size": page_size})

    def log_message(self, fmt: str, *args) -> None:
        return

    def _send(self, status: int, payload: dict) -> None:
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def main() -> None:
    host = os.environ.get("SAMPLE_API_HOST", "127.0.0.1")
    port = int(os.environ.get("SAMPLE_API_PORT", "8090"))
    server = ThreadingHTTPServer((host, port), Handler)
    print(f"sample api listening on http://{host}:{port}")
    server.serve_forever()


if __name__ == "__main__":
    main()
