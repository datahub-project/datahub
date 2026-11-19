import asyncio
import threading
from typing import Any

import httpx
import pytest
from fastmcp import Client
from fastmcp.client.transports import StreamableHttpTransport
from helpers import FakeClientFactory, FakeGms, serve

from datahub_ai_service.app import create_app
from datahub_ai_service.config import AiServiceConfig, ResponseMode
from datahub_ai_service.mcp.telemetry import ToolCallEvent

INITIALIZE = {
    "jsonrpc": "2.0",
    "id": 1,
    "method": "initialize",
    "params": {
        "protocolVersion": "2025-06-18",
        "capabilities": {},
        "clientInfo": {"name": "test", "version": "0"},
    },
}
MCP_HEADERS = {"Accept": "application/json, text/event-stream"}


def _app(gms: FakeGms, **config: Any) -> Any:
    observers = config.pop("observers", ())
    return create_app(
        AiServiceConfig(gms_url="http://gms.invalid:8080", **config),
        client_factory=FakeClientFactory(gms),
        observers=observers,
    )


def _client(url: str, token: str, mode: str = "auto") -> Client:
    transport = StreamableHttpTransport(
        f"{url}/mcp", headers={"Authorization": f"Bearer {token}"}
    )
    return Client(transport, mode=mode)


async def test_health_needs_no_credential_or_gms(gms: FakeGms) -> None:
    gms.down = True
    async with serve(_app(gms)) as url, httpx.AsyncClient() as http:
        response = await http.get(f"{url}/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


@pytest.mark.parametrize(
    ("headers", "error"),
    [
        ({}, "missing_token"),
        ({"Authorization": "Basic YWxpY2U6cHc="}, "missing_token"),
        ({"Authorization": "Bearer not-a-real-token"}, "invalid_token"),
    ],
)
async def test_mcp_refuses_callers_without_a_valid_token(
    gms: FakeGms, headers: dict[str, str], error: str
) -> None:
    async with serve(_app(gms)) as url, httpx.AsyncClient() as http:
        response = await http.post(
            f"{url}/mcp", json=INITIALIZE, headers={**MCP_HEADERS, **headers}
        )

    assert response.status_code == 401
    assert response.json()["error"] == error
    assert response.headers["www-authenticate"].startswith("Bearer")


async def test_mcp_is_unavailable_while_gms_is_down(gms: FakeGms) -> None:
    gms.down = True
    async with serve(_app(gms)) as url, httpx.AsyncClient() as http:
        response = await http.post(
            f"{url}/mcp",
            json=INITIALIZE,
            headers={**MCP_HEADERS, "Authorization": "Bearer alice-token"},
        )

    assert response.status_code == 503


@pytest.mark.parametrize(
    ("mode", "content_type"),
    [("sse", "text/event-stream"), ("json", "application/json")],
)
async def test_response_mode(
    gms: FakeGms, mode: ResponseMode, content_type: str
) -> None:
    async with serve(_app(gms, mcp_response_mode=mode)) as url:
        async with httpx.AsyncClient() as http:
            response = await http.post(
                f"{url}/mcp",
                json=INITIALIZE,
                headers={**MCP_HEADERS, "Authorization": "Bearer alice-token"},
            )
        async with _client(url, "alice-token") as client:
            result = await client.call_tool("get_me")

    assert response.status_code == 200
    assert response.headers["content-type"].startswith(content_type)
    assert result.data["data"]["corpUser"]["urn"] == "urn:li:corpuser:alice"


@pytest.mark.parametrize("protocol", ["legacy", "auto"])
async def test_legacy_and_modern_clients(gms: FakeGms, protocol: str) -> None:
    async with serve(_app(gms)) as url, _client(url, "bob-token", protocol) as client:
        tools = await client.list_tools()
        result = await client.call_tool("get_me")

    assert [tool.name for tool in tools] == ["get_me"]
    assert result.data["data"]["corpUser"]["urn"] == "urn:li:corpuser:bob"


async def test_concurrent_callers_keep_their_own_identity(gms: FakeGms) -> None:
    # Both get_me calls must be in flight at once before either can finish.
    gms.get_me_barrier = threading.Barrier(2, timeout=10)

    async with serve(_app(gms)) as url:

        async def get_me(token: str) -> str:
            async with _client(url, token) as client:
                result = await client.call_tool("get_me")
                return result.data["data"]["corpUser"]["urn"]

        urns = await asyncio.gather(get_me("alice-token"), get_me("bob-token"))

    assert urns == ["urn:li:corpuser:alice", "urn:li:corpuser:bob"]
    assert gms.open_clients == 0


async def test_verified_tokens_are_cached(gms: FakeGms) -> None:
    async with serve(_app(gms)) as url, _client(url, "alice-token") as client:
        for _ in range(3):
            await client.call_tool("get_me")

    # One identity check for the token, then one getMe per call.
    assert gms.me_calls == 1 + 3


async def test_mcp_can_be_disabled(gms: FakeGms) -> None:
    async with serve(_app(gms, mcp_enabled=False)) as url, httpx.AsyncClient() as http:
        response = await http.post(
            f"{url}/mcp",
            json=INITIALIZE,
            headers={**MCP_HEADERS, "Authorization": "Bearer alice-token"},
        )

    assert response.status_code == 404


async def test_tool_call_observers(gms: FakeGms) -> None:
    events: list[ToolCallEvent] = []

    class Recorder:
        def on_tool_call(self, event: ToolCallEvent) -> None:
            events.append(event)

    class Broken:
        def on_tool_call(self, event: ToolCallEvent) -> None:
            raise RuntimeError("telemetry backend down")

    app = _app(gms, observers=[Broken(), Recorder()])
    async with serve(app) as url, _client(url, "alice-token") as client:
        result = await client.call_tool("get_me")

    assert result.data["success"] is True
    assert len(events) == 1
    assert events[0].tool_name == "get_me"
    assert events[0].actor_urn == "urn:li:corpuser:alice"
    assert events[0].error_type is None
