import http.server
import json
import os
import shutil
import sys
import threading
from pathlib import Path
from typing import Any, Iterator, Optional

import pytest
from datahub.configuration.common import OperationalError
from datahub.ingestion.graph.client import get_default_graph
from fastmcp import Client
from fastmcp.client.transports import StdioTransport

from datahub_core_mcp_tools.gms_server_info import GmsServerInfo
from datahub_core_mcp_tools.stdio import (
    build_stdio_server,
    client_from_env,
    groups_from_env,
)

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
    """Answers /config and every GraphQL query with ``me`` for one token, or
    each GraphQL query with ``post_statuses`` in turn while any are left."""

    token = "alice-token"
    authorizations: list[Optional[str]] = []
    post_statuses: list[int] = []

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
        if type(self).post_statuses:
            status = type(self).post_statuses.pop(0)
            self.send_response(status)
            if status == 429:
                self.send_header("retry-after", "0")
            self.send_header("content-length", "0")
            self.end_headers()
        elif authorization != f"Bearer {self.token}":
            self._send(401, {"error": "unauthorized"})
        else:
            self._send(200, _ME)

    def log_message(self, *_: object) -> None:
        pass


@pytest.fixture
def gms() -> Iterator[str]:
    _Gms.authorizations, _Gms.post_statuses = [], []
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


@pytest.mark.parametrize(
    ("statuses", "posts", "succeeds"),
    [
        # GMS may have run it: a 5xx is never replayed, so a mutation runs once.
        ([500], 1, False),
        ([503], 1, False),
        # GMS refused it before running it: safe to send again.
        ([429, 429], 3, True),
    ],
)
def test_the_local_client_never_replays_a_request_gms_may_have_run(
    gms: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    statuses: list[int],
    posts: int,
    succeeds: bool,
) -> None:
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setenv("DATAHUB_GMS_URL", gms)
    monkeypatch.setenv("DATAHUB_GMS_TOKEN", "alice-token")
    monkeypatch.setenv("DATAHUB_TELEMETRY_ENABLED", "false")
    # The SDK keeps one default graph per process; each case needs its own GMS.
    get_default_graph.cache_clear()
    client = client_from_env()
    _Gms.post_statuses = statuses

    try:
        if succeeds:
            assert client._graph.execute_graphql(
                "query getMe { me { corpUser { urn } } }"
            )
        else:
            with pytest.raises(OperationalError):
                client._graph.execute_graphql("query getMe { me { corpUser { urn } } }")
    finally:
        client._graph.close()
        get_default_graph.cache_clear()
    assert len(_Gms.authorizations) == posts


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


@pytest.mark.parametrize(
    ("value", "groups"),
    [
        (None, None),
        ("  ", None),
        ("read_metadata, ingestion", ["read_metadata", "ingestion"]),
    ],
)
def test_tool_groups_from_the_environment(
    monkeypatch: pytest.MonkeyPatch, value: Optional[str], groups: Optional[list[str]]
) -> None:
    if value is None:
        monkeypatch.delenv("DATAHUB_MCP_TOOL_GROUPS", raising=False)
    else:
        monkeypatch.setenv("DATAHUB_MCP_TOOL_GROUPS", value)

    assert groups_from_env() == groups


@pytest.mark.parametrize(
    ("groups", "has_ingestion"), [(None, False), (["ingestion"], True)]
)
async def test_stdio_serves_ingestion_only_when_named(
    groups: Optional[list[str]], has_ingestion: bool
) -> None:
    mcp = build_stdio_server(
        _FakeClient(),  # type: ignore[arg-type]
        groups=groups,
        server_info=lambda: GmsServerInfo(version="v1.8.0", server_env="core"),
        telemetry_ping=lambda name, properties: None,
    )
    async with Client(mcp) as client:
        tools = {tool.name for tool in await client.list_tools()}

    assert ("validate_recipe" in tools) == has_ingestion
    assert ("search" in tools) == (not has_ingestion)
