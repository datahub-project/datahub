"""Minimal threaded HTTP server that mimics GMS endpoints needed for ingestion tests."""

from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any


class _Handler(BaseHTTPRequestHandler):
    """Serves GET /config and records POST bodies (ingest batches)."""

    captured_posts: list[bytes] = []
    config_payload: dict[str, Any] = {}

    def log_message(self, _format: str, *_args: Any) -> None:
        return

    def do_GET(self) -> None:
        """Serve GET /config with the stub config payload; return 404 for all other paths."""
        if self.path.startswith("/config"):
            body = json.dumps(self.config_payload).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        self.send_response(404)
        self.end_headers()

    def do_POST(self) -> None:
        """Capture the raw POST body and respond with an empty JSON object."""
        length = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(length) if length else b""
        self.captured_posts.append(raw)
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(b"{}")


class RecordingGmsServer:
    """Threaded HTTP server; POST bodies are appended to ``captured_posts``."""

    def __init__(self) -> None:
        self._server: ThreadingHTTPServer | None = None
        self._thread: threading.Thread | None = None

    @property
    def url(self) -> str:
        """Return the base URL of the running server (e.g. ``http://127.0.0.1:PORT``)."""
        assert self._server is not None
        port = self._server.socket.getsockname()[1]
        return f"http://127.0.0.1:{port}"

    def start(self) -> None:
        """Start the server on a random free port and reset captured state."""
        _Handler.captured_posts = []
        _Handler.config_payload = {
            "noCode": "true",
            "versions": {
                "acryldata/datahub": {
                    "version": "0.12.0",
                }
            },
        }
        self._server = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        """Shut down the server and release the port; safe to call more than once."""
        if self._server is not None:
            self._server.shutdown()
            self._server.server_close()
            self._server = None
        self._thread = None

    def __enter__(self) -> RecordingGmsServer:
        self.start()
        return self

    def __exit__(self, *_: Any) -> None:
        self.stop()

    @property
    def captured_posts(self) -> list[bytes]:
        """Return a snapshot of all raw POST bodies received since the last ``start``."""
        return list(_Handler.captured_posts)
