import threading
import time
from typing import Any, Callable

import mcp.types as mt
import pytest
from datahub.telemetry import telemetry
from fastmcp import Client, FastMCP
from fastmcp.exceptions import ToolError

from datahub_core_mcp_tools.gms_server_info import GmsServerInfo
from datahub_core_mcp_tools.telemetry import (
    SERVER_START_EVENT,
    TOOL_CALL_EVENT,
    TelemetryMiddleware,
    ping_server_start,
)

Event = tuple[str, dict[str, Any]]


def _now(task: Callable[[], None]) -> None:
    task()


def _server(middleware: TelemetryMiddleware) -> FastMCP:
    mcp = FastMCP[None]("test")

    @mcp.tool
    def echo(text: str) -> str:
        return text

    @mcp.tool
    def fails() -> str:
        raise ToolError("urn:li:dataset:secret is not visible to you")

    mcp.add_middleware(middleware)
    return mcp


def _recording_middleware(events: list[Event], **kwargs: Any) -> TelemetryMiddleware:
    return TelemetryMiddleware(
        deployment="stdio",
        ping=lambda name, props: events.append((name, props)),
        dispatch=_now,
        **kwargs,
    )


@pytest.mark.parametrize("mode", ["legacy", "auto"])
async def test_tool_call_event(mode: str) -> None:
    events: list[Event] = []
    mcp = _server(
        _recording_middleware(
            events,
            server_info=lambda: GmsServerInfo(version="v1.8.0", server_env="core"),
        )
    )

    async with Client(
        mcp, mode=mode, client_info=mt.Implementation(name="my-agent", version="2.1")
    ) as client:
        await client.call_tool("echo", {"text": "hello"})

    [(name, props)] = events
    assert name == TOOL_CALL_EVENT
    # Handshake-era clients name themselves in initialize, 2026-era ones in
    # each request's _meta: both reach the event.
    assert props["client_name"] == "my-agent"
    assert props["client_version"] == "2.1"
    assert props["tool"] == "echo"
    assert props["method"] == "tools/call"
    assert props["tool_result_length"] == len("hello")
    assert props["deployment"] == "stdio"
    assert props["server_version"] == "v1.8.0"
    assert props["server_env"] == "core"
    assert props["duration_seconds"] >= 0
    assert "tool_call_error" not in props
    assert "hello" not in str(props)


async def test_failed_call_reports_the_error_class_only() -> None:
    events: list[Event] = []
    mcp = _server(_recording_middleware(events))

    async with Client(mcp) as client:
        result = await client.call_tool("fails", raise_on_error=False)

    assert result.is_error
    [(_, props)] = events
    assert props["tool_call_error"] == "ToolError"
    assert props["tool_result_is_error"] is True
    assert "secret" not in str(props)
    assert "tool_call_error_message" not in props


async def test_request_properties_are_added() -> None:
    events: list[Event] = []
    mcp = _server(
        _recording_middleware(events, request_properties=lambda: {"auth_mode": "off"})
    )

    async with Client(mcp) as client:
        await client.call_tool("echo", {"text": "x"})

    assert events[0][1]["auth_mode"] == "off"


async def test_a_slow_or_broken_ping_never_holds_up_or_fails_a_call() -> None:
    sent = threading.Event()
    release = threading.Event()

    def slow_ping(name: str, props: dict[str, Any]) -> None:
        release.wait(5)
        sent.set()
        raise RuntimeError("collector unreachable")

    def broken_server_info() -> GmsServerInfo:
        raise ConnectionError("GMS down")

    # The default dispatch: a background thread.
    mcp = _server(
        TelemetryMiddleware(
            deployment="stdio", ping=slow_ping, server_info=broken_server_info
        )
    )

    started = time.perf_counter()
    async with Client(mcp) as client:
        result = await client.call_tool("echo", {"text": "fast"})
    assert time.perf_counter() - started < 4
    assert result.data == "fast"

    release.set()
    assert sent.wait(5)


def test_server_start_event() -> None:
    events: list[Event] = []

    ping_server_start(
        deployment="datahub-ai-service",
        properties={"response_mode": "sse"},
        server_info=lambda: GmsServerInfo(version="v1.7.0", server_env="core"),
        ping=lambda name, props: events.append((name, props)),
        dispatch=_now,
    )

    assert events == [
        (
            SERVER_START_EVENT,
            {
                "server_version": "v1.7.0",
                "server_env": "core",
                "server_is_cloud": False,
                "response_mode": "sse",
                "deployment": "datahub-ai-service",
            },
        )
    ]


@pytest.mark.parametrize("enabled", [True, False])
async def test_sdk_opt_out_is_honored(
    monkeypatch: pytest.MonkeyPatch, enabled: bool
) -> None:
    # DATAHUB_TELEMETRY_ENABLED=false leaves the SDK's instance disabled.
    pings: list[str] = []
    instance = telemetry.telemetry_instance
    monkeypatch.setattr(instance, "enabled", enabled)
    monkeypatch.setattr(instance, "mp", object())
    monkeypatch.setattr(instance, "ping", lambda name, props=None: pings.append(name))

    mcp = _server(TelemetryMiddleware(deployment="stdio", dispatch=_now))
    async with Client(mcp) as client:
        await client.call_tool("echo", {"text": "x"})
    ping_server_start(deployment="stdio", dispatch=_now)

    assert pings == ([TOOL_CALL_EVENT, SERVER_START_EVENT] if enabled else [])
