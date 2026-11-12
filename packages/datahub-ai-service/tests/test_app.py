import asyncio
import threading
from typing import Any

import httpx
import pytest
from fastmcp import Client
from fastmcp.client.transports import StreamableHttpTransport
from helpers import FakeClientFactory, FakeGms, serve
from sse_starlette import EventSourceResponse

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
        server_info=lambda: gms.server_info(),
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
    assert response.json()["status"] == "ok"
    assert response.json()["mcp"]["gmsAuth"] == "unknown"


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


async def test_sse_keepalive_without_a_progress_token(
    gms: FakeGms, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The SDK's SSE responses ping every 15 s; shorten it so the test is quick.
    monkeypatch.setattr(EventSourceResponse, "DEFAULT_PING_INTERVAL", 0.1)
    gms.get_me_seconds = 0.5
    call = {
        "jsonrpc": "2.0",
        "id": 2,
        "method": "tools/call",
        "params": {"name": "get_me", "arguments": {}},
    }

    async with serve(_app(gms)) as url, httpx.AsyncClient() as http:
        async with http.stream(
            "POST",
            f"{url}/mcp",
            json=call,
            headers={
                **MCP_HEADERS,
                "Authorization": "Bearer alice-token",
                "MCP-Protocol-Version": "2025-06-18",
            },
        ) as response:
            lines = [line async for line in response.aiter_lines()]

    pings = [line for line in lines if line.startswith(":")]
    result = next(line for line in lines if line.startswith("data:"))
    assert pings, lines
    assert lines.index(pings[0]) < lines.index(result)
    assert "urn:li:corpuser:alice" in result


async def test_cancelled_calls_are_recorded(gms: FakeGms) -> None:
    events: list[ToolCallEvent] = []

    class Recorder:
        def on_tool_call(self, event: ToolCallEvent) -> None:
            events.append(event)

    gms.get_me_seconds = 1.0
    async with serve(_app(gms, observers=[Recorder()])) as url:
        async with _client(url, "alice-token") as client:
            call = asyncio.create_task(client.call_tool("get_me"))
            await asyncio.sleep(0.3)
            call.cancel()
            with pytest.raises(asyncio.CancelledError):
                await call
        # The server notices the dropped request and finishes the worker first.
        for _ in range(50):
            if events:
                break
            await asyncio.sleep(0.1)

    assert len(events) == 1
    assert events[0].cancelled
    assert events[0].error_type is None
    # The worker kept its client until it finished; then it was closed once.
    assert gms.open_clients == 0


async def test_auth_off_needs_no_token_and_runs_as_the_system_user(
    gms: FakeGms,
) -> None:
    gms.auth_enabled = False

    async with serve(_app(gms)) as url, httpx.AsyncClient() as http:
        async with Client(StreamableHttpTransport(f"{url}/mcp")) as client:
            result = await client.call_tool("get_me")
        health = (await http.get(f"{url}/health")).json()

    assert result.data["data"]["corpUser"]["urn"] == "urn:li:corpuser:__datahub_system"
    assert "authentication is off" in result.data["message"]
    assert health["mcp"] == {
        "enabled": True,
        "authMode": "auto",
        "gmsAuth": "off",
        "available": True,
    }


@pytest.mark.parametrize(
    "headers",
    [{}, {"Authorization": "Bearer garbage"}, {"Authorization": "Bearer alice-token"}],
)
async def test_required_mode_is_unavailable_while_gms_auth_is_off(
    gms: FakeGms, headers: dict[str, str]
) -> None:
    gms.auth_enabled = False

    async with serve(_app(gms, auth_mode="required")) as url:
        async with httpx.AsyncClient() as http:
            response = await http.post(
                f"{url}/mcp", json=INITIALIZE, headers={**MCP_HEADERS, **headers}
            )
            health = (await http.get(f"{url}/health")).json()

    # Never a successful login: GMS with auth off can't check a token.
    assert response.status_code == 503
    assert response.json()["error"] == "datahub_auth_off"
    assert health["mcp"]["available"] is False


async def test_turning_gms_auth_on_drops_cached_verdicts(gms: FakeGms) -> None:
    async with serve(_app(gms)) as url, httpx.AsyncClient() as http:

        async def initialize(token: str) -> int:
            response = await http.post(
                f"{url}/mcp",
                json=INITIALIZE,
                headers={**MCP_HEADERS, "Authorization": f"Bearer {token}"},
            )
            return response.status_code

        assert await initialize("alice-token") == 200
        gms.auth_enabled = False
        assert await initialize("anything") == 200
        # Auth back on: alice is checked again, and an unknown token is refused.
        gms.auth_enabled = True
        calls = gms.me_calls
        assert await initialize("alice-token") == 200
        assert gms.me_calls == calls + 1
        assert await initialize("anything") == 401
