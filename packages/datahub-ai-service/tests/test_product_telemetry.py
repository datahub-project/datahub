import asyncio
import threading
from typing import Any, Optional

from datahub.telemetry import telemetry
from fastmcp import Client
from fastmcp.client.transports import StreamableHttpTransport
from helpers import FakeClientFactory, FakeGms, serve

from datahub_ai_service._version import __version__
from datahub_ai_service.app import create_app
from datahub_ai_service.config import AiServiceConfig


class Pings:
    """Collects events the background dispatcher sends."""

    def __init__(self) -> None:
        self.events: list[tuple[str, dict[str, Any]]] = []
        self._lock = threading.Lock()

    def __call__(self, name: str, properties: dict[str, Any]) -> None:
        with self._lock:
            self.events.append((name, properties))

    async def wait_for(self, name: str, count: int = 1) -> list[dict[str, Any]]:
        for _ in range(100):
            with self._lock:
                found = [p for n, p in self.events if n == name]
            if len(found) >= count:
                return found
            await asyncio.sleep(0.05)
        raise AssertionError(f"no {name} event in {self.events}")


def _app(gms: FakeGms, pings: Pings, **config: Any) -> Any:
    return create_app(
        AiServiceConfig(gms_url="http://gms.invalid:8080", **config),
        client_factory=FakeClientFactory(gms),
        server_info=lambda: gms.server_info(),
        telemetry_ping=pings,
    )


def _client(url: str, token: Optional[str]) -> Client:
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    return Client(StreamableHttpTransport(f"{url}/mcp", headers=headers))


async def test_tool_calls_and_startup_are_pinged(gms: FakeGms) -> None:
    pings = Pings()
    async with serve(_app(gms, pings, mcp_response_mode="json")) as url:
        [start] = await pings.wait_for("mcp-server-start")
        async with _client(url, "alice-token") as client:
            await client.call_tool("get_me")
        [call] = await pings.wait_for("mcp-server-tool-call")

    assert start["deployment"] == "datahub-ai-service"
    assert start["response_mode"] == "json"
    assert call["tool"] == "get_me"
    assert call["deployment"] == "datahub-ai-service"
    assert call["auth_mode"] == "token"
    assert call["response_mode"] == "json"
    assert call["server_version"] == "v1.7.0"
    assert call["server_env"] == "core"
    assert call["client_name"]
    assert call["http_user_agent"]
    # Product telemetry is anonymous: no caller identity or credential.
    assert "alice" not in str(call)
    assert (
        telemetry.telemetry_instance.global_properties["datahub_ai_service_version"]
        == __version__
    )


async def test_auth_mode_off_when_gms_auth_is_off(gms: FakeGms) -> None:
    gms.auth_enabled = False
    pings = Pings()
    async with serve(_app(gms, pings)) as url, _client(url, None) as client:
        await client.call_tool("get_me")
        [call] = await pings.wait_for("mcp-server-tool-call")

    assert call["auth_mode"] == "off"
