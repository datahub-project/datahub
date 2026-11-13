import asyncio
import threading
import time
from typing import Any, Optional

import httpx
import pytest
from fastmcp import Client
from fastmcp.client.transports import StreamableHttpTransport
from helpers import FakeClientFactory, FakeGms, serve

from datahub_ai_service.app import create_app
from datahub_ai_service.config import AiServiceConfig
from datahub_ai_service.mcp.auth import Caller
from datahub_ai_service.mcp.auth_mode import GmsAuth
from datahub_ai_service.metrics import ServiceMetrics
from datahub_ai_service.usage_events import UsageEventSender
from datahub_core_mcp_tools.telemetry import ToolCall

Posted = tuple[dict[str, Any], Optional[str]]


class Posts:
    def __init__(self) -> None:
        self.posted: list[Posted] = []
        self._lock = threading.Lock()

    def __call__(self, event: dict[str, Any], token: Optional[str]) -> None:
        with self._lock:
            self.posted.append((event, token))

    async def wait_for(self, count: int) -> list[Posted]:
        for _ in range(100):
            with self._lock:
                if len(self.posted) >= count:
                    return list(self.posted)
            await asyncio.sleep(0.05)
        raise AssertionError(f"expected {count} usage events, got {self.posted}")


def _app(gms: FakeGms, posts: Posts, **config: Any) -> Any:
    return create_app(
        AiServiceConfig(gms_url="http://gms.invalid:8080", **config),
        client_factory=FakeClientFactory(gms),
        server_info=lambda: gms.server_info(),
        telemetry_ping=lambda name, properties: None,
        usage_event_post=posts,
    )


def _client(url: str, token: Optional[str]) -> Client:
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    return Client(StreamableHttpTransport(f"{url}/mcp", headers=headers))


async def test_a_tool_call_is_recorded_for_its_caller(gms: FakeGms) -> None:
    posts = Posts()
    async with serve(_app(gms, posts)) as url, _client(url, "alice-token") as client:
        result = await client.call_tool("get_me")
        [(event, token)] = await posts.wait_for(1)

    assert token == "alice-token"
    assert event["type"] == "ToolInvocation"
    assert event["surface"] == "mcp"
    assert event["actorUrn"] == "urn:li:corpuser:alice"
    assert event["toolServer"] == "datahub"
    assert event["toolName"] == "get_me"
    assert event["toolGroup"] == "read"
    assert event["isError"] is False
    assert event["resultLength"] > 0
    assert isinstance(event["durationMs"], int)
    assert isinstance(event["timestamp"], int)
    assert event["clientName"]
    assert event["userAgent"]
    assert "errorType" not in event
    # Never the result itself.
    assert result.content[0].text not in str(event)


async def test_write_tools_and_failures(gms: FakeGms) -> None:
    posts = Posts()
    urn = "urn:li:dataset:(urn:li:dataPlatform:hive,private_table,PROD)"
    async with serve(_app(gms, posts)) as url, _client(url, "bob-token") as client:
        await client.call_tool(
            "add_tags",
            {"tag_urns": ["urn:li:tag:x"], "entity_urns": [urn]},
            raise_on_error=False,
        )
        # Not a tool: the client's name for it is not recorded.
        await client.call_tool("made_up_tool", raise_on_error=False)
        [(event, _)] = await posts.wait_for(1)
        await asyncio.sleep(0.2)

    assert len(posts.posted) == 1
    assert event["toolName"] == "add_tags"
    assert event["toolGroup"] == "write"
    assert event["actorUrn"] == "urn:li:corpuser:bob"
    # No tool arguments.
    assert "private_table" not in str(event)


async def test_concurrent_callers_are_each_recorded_as_themselves(
    gms: FakeGms,
) -> None:
    gms.get_me_barrier = threading.Barrier(2, timeout=10)
    posts = Posts()
    async with serve(_app(gms, posts)) as url:

        async def get_me(token: str) -> None:
            async with _client(url, token) as client:
                await client.call_tool("get_me")

        await asyncio.gather(get_me("alice-token"), get_me("bob-token"))
        posted = await posts.wait_for(2)

    assert sorted((event["actorUrn"], token) for event, token in posted) == [
        ("urn:li:corpuser:alice", "alice-token"),
        ("urn:li:corpuser:bob", "bob-token"),
    ]


async def test_a_client_cannot_choose_the_actor(gms: FakeGms) -> None:
    posts = Posts()
    async with serve(_app(gms, posts)) as url:
        transport = StreamableHttpTransport(
            f"{url}/mcp",
            headers={
                "Authorization": "Bearer alice-token",
                "X-DataHub-Actor": "urn:li:corpuser:bob",
            },
        )
        async with Client(transport) as client:
            await client.call_tool(
                "get_me", meta={"actorUrn": "urn:li:corpuser:bob", "token": "bob-token"}
            )
        [(event, token)] = await posts.wait_for(1)

    assert event["actorUrn"] == "urn:li:corpuser:alice"
    assert token == "alice-token"


async def test_auth_off_records_the_system_user(gms: FakeGms) -> None:
    gms.auth_enabled = False
    posts = Posts()
    async with serve(_app(gms, posts)) as url, _client(url, None) as client:
        await client.call_tool("get_me")
        [(event, token)] = await posts.wait_for(1)

    assert event["actorUrn"] == "urn:li:corpuser:__datahub_system"
    assert token is None


async def test_usage_events_can_be_disabled(gms: FakeGms) -> None:
    posts = Posts()
    app = _app(gms, posts, usage_events_enabled=False)
    async with serve(app) as url, _client(url, "alice-token") as client:
        await client.call_tool("get_me")
        await asyncio.sleep(0.3)

    assert posts.posted == []


def _call() -> ToolCall:
    return ToolCall(
        tool="get_me",
        source="client",
        type="request",
        method="tools/call",
        client_name=None,
        client_version=None,
        http_user_agent=None,
        duration_seconds=0.01,
        result_length=10,
        error_type=None,
        result_is_error=False,
        cancelled=False,
        tags=frozenset({"user"}),
    )


def test_a_full_queue_drops_and_counts_and_never_blocks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    caller = Caller(token="t", actor_urn="urn:li:corpuser:alice", gms_auth=GmsAuth.ON)
    monkeypatch.setattr(
        "datahub_ai_service.usage_events.current_caller", lambda: caller
    )
    release = threading.Event()

    def stuck_gms(event: dict[str, Any], token: Optional[str]) -> None:
        release.wait(5)
        raise ConnectionError("GMS down")

    metrics = ServiceMetrics()
    sender = UsageEventSender(
        "http://gms.invalid", metrics=metrics, max_pending=2, post=stuck_gms
    )
    started = time.perf_counter()
    for _ in range(10):
        sender(_call())
    assert time.perf_counter() - started < 1

    # One in flight, two queued, the rest dropped.
    dropped = metrics.registry.get_sample_value("datahub_ai_usage_events_dropped_total")
    assert dropped is not None and dropped >= 7
    release.set()
    sender.close()


async def test_gms_refusing_events_never_affects_calls(gms: FakeGms) -> None:
    def refuse(event: dict[str, Any], token: Optional[str]) -> None:
        raise httpx.HTTPError("GMS answered 500")

    app = create_app(
        AiServiceConfig(gms_url="http://gms.invalid:8080"),
        client_factory=FakeClientFactory(gms),
        server_info=lambda: gms.server_info(),
        telemetry_ping=lambda name, properties: None,
        usage_event_post=refuse,
    )
    async with serve(app) as url, _client(url, "alice-token") as client:
        result = await client.call_tool("get_me")

    assert result.data["data"]["corpUser"]["urn"] == "urn:li:corpuser:alice"
