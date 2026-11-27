"""A minimal in-process stand-in for the shared OpenSearch's REST API.

Serves only the endpoints the index-cleanup code calls, over real HTTP, so the tests
exercise the request paths and URL encoding rather than a mocked method.
"""

from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Dict, List, Optional
from urllib.parse import urlparse


class FakeOpenSearch:
    def __init__(
        self,
        indices: List[str],
        max_shards_per_node: Optional[int] = None,
        health: str = "green",
        fail_deletes_after: Optional[int] = None,
    ):
        self.indices: Dict[str, int] = {name: 1 for name in indices}
        self.max_shards_per_node = max_shards_per_node
        self.health = health
        # DELETE requests that succeed before every later one returns HTTP 500.
        self.fail_deletes_after = fail_deletes_after
        self.delete_paths: List[str] = []
        fake = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args: object) -> None:
                pass

            def _send(self, body: object, status: int = 200) -> None:
                payload = json.dumps(body).encode()
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

            def do_GET(self) -> None:
                path = urlparse(self.path).path
                if path == "/":
                    self._send({"version": {"number": "2.19.1"}})
                elif path == "/_cat/indices":
                    self._send(
                        [
                            {
                                "index": name,
                                "pri": "1",
                                "rep": "0",
                                "docs.count": "2",
                                "store.size": "100",
                            }
                            for name in fake.indices
                        ]
                    )
                elif path == "/_cluster/health":
                    self._send(
                        {
                            "status": fake.health,
                            "number_of_data_nodes": 1,
                            "active_shards": len(fake.indices),
                            "unassigned_shards": 0,
                        }
                    )
                elif path == "/_cluster/settings":
                    persistent = (
                        {"cluster.max_shards_per_node": str(fake.max_shards_per_node)}
                        if fake.max_shards_per_node is not None
                        else {}
                    )
                    self._send(
                        {
                            "persistent": persistent,
                            "transient": {},
                            "defaults": {"cluster.max_shards_per_node": "1000"},
                        }
                    )
                else:
                    self._send({"error": path}, status=404)

            def do_DELETE(self) -> None:
                url = urlparse(self.path)
                fake.delete_paths.append(url.path)
                if (
                    fake.fail_deletes_after is not None
                    and len(fake.delete_paths) > fake.fail_deletes_after
                ):
                    self._send({"error": "node failure"}, status=500)
                    return
                names = url.path.lstrip("/").split(",")
                if any("*" in name for name in names):
                    self._send({"error": "wildcard"}, status=400)
                    return
                missing = [name for name in names if name not in fake.indices]
                if missing and "ignore_unavailable=true" not in url.query:
                    self._send({"error": "index_not_found_exception"}, status=404)
                    return
                for name in names:
                    fake.indices.pop(name, None)
                self._send({"acknowledged": True})

        self._server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.port: int = self._server.server_address[1]
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)

    def __enter__(self) -> "FakeOpenSearch":
        self._thread.start()
        return self

    def __exit__(self, *exc: object) -> None:
        self._server.shutdown()
        self._server.server_close()
