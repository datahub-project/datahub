import http.server
import json
import os
import shutil
import sys
import threading
from pathlib import Path
from typing import Any, Iterator, Optional

import pytest
from fastmcp import Client
from fastmcp.client.transports import StdioTransport

from datahub_core_mcp_tools.gms_server_info import GmsServerInfo
from datahub_core_mcp_tools.stdio import build_stdio_server

_CONFIG = {
    "versions": {"acryldata/datahub": {"version": "v1.8.0"}},
    "datahub": {"serverType": "quickstart", "serverEnv": "core"},
    "authentication": {"enabled": True},
    "noCode": "true",
}
_ME = {
    "data": {
        "me": {
            "corpUser": {
                "urn": "urn:li:corpuser:alice",
                "username": "alice",
                "exists": True,
            }
        }
    }
}


class _Gms(http.server.BaseHTTPRequestHandler):
    """Answers /config and every GraphQL query with ``me`` for one token."""

    token = "alice-token"
    authorizations: list[Optional[str]] = []

    def _send(self, status: int, body: Any) -> None:
        payload = json.dumps(body).encode()
        self.send_response(status)
        self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def do_GET(self) -> None:
        if self.path.startswith("/config"):
            self._send(200, _CONFIG)
        else:
            self._send(404, {})

    def do_POST(self) -> None:
        self.rfile.read(int(self.headers.get("content-length", 0)))
        authorization = self.headers.get("authorization")
        type(self).authorizations.append(authorization)
        if authorization != f"Bearer {self.token}":
            self._send(401, {"error": "unauthorized"})
        else:
            self._send(200, _ME)

    def log_message(self, *_: object) -> None:
        pass


@pytest.fixture
def gms() -> Iterator[str]:
    _Gms.authorizations = []
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _Gms)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield f"http://127.0.0.1:{server.server_address[1]}"
    server.shutdown()


async def test_stdio_round_trip(gms: str, tmp_path: Path) -> None:
    script = shutil.which("datahub-mcp-stdio", path=str(Path(sys.executable).parent))
    assert script, "datahub-mcp-stdio is not installed in this environment"
    env = {
        "PATH": os.environ.get("PATH", ""),
        # Keep the user's ~/.datahubenv and telemetry settings out of it.
        "HOME": str(tmp_path),
        "DATAHUB_GMS_URL": gms,
        "DATAHUB_GMS_TOKEN": "alice-token",
        "DATAHUB_TELEMETRY_ENABLED": "false",
        "TOOLS_IS_MUTATION_ENABLED": "false",
    }
    async with Client(StdioTransport(script, [], env=env)) as client:
        tools = {tool.name for tool in await client.list_tools()}
        result = await client.call_tool("get_me")

    assert "search" in tools and "get_me" in tools
    assert "add_tags" not in tools
    assert result.data["data"]["corpUser"]["urn"] == "urn:li:corpuser:alice"
    assert "Bearer alice-token" in _Gms.authorizations


class _FakeGraph:
    def execute_graphql(self, query: str, **_: Any) -> Any:
        return _ME["data"]


class _FakeClient:
    _graph = _FakeGraph()


async def test_embedded_server_serves_selected_groups_with_stdio_telemetry() -> None:
    events: list[tuple[str, dict[str, Any]]] = []
    sent = threading.Event()

    def ping(name: str, properties: dict[str, Any]) -> None:
        events.append((name, properties))
        sent.set()

    mcp = build_stdio_server(
        _FakeClient(),  # type: ignore[arg-type]
        groups=["read_metadata"],
        server_info=lambda: GmsServerInfo(version="v1.8.0", server_env="core"),
        telemetry_ping=ping,
    )
    async with Client(mcp) as client:
        tools = {tool.name for tool in await client.list_tools()}
        result = await client.call_tool("get_me")

    assert "search" in tools and "get_me" in tools
    assert "add_tags" not in tools
    assert result.data["data"]["corpUser"]["urn"] == "urn:li:corpuser:alice"
    assert sent.wait(5)
    assert events[0][0] == "mcp-server-tool-call"
    assert events[0][1]["deployment"] == "stdio"
    assert events[0][1]["tool"] == "get_me"
