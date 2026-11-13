"""The settings-page policy, applied to /mcp end to end."""

from typing import Any, Optional

import httpx
import pytest
import requests
from datahub.configuration.common import GraphError
from fastmcp import Client
from fastmcp.client.transports import StreamableHttpTransport
from fastmcp.exceptions import ToolError
from helpers import FakeClientFactory, FakeGms, serve
from test_settings import FIELD_UNDEFINED

from datahub_ai_service.app import create_app
from datahub_ai_service.config import AiServiceConfig
from datahub_ai_service.mcp.settings import McpPolicyCache
from datahub_core_mcp_tools.mcp_instructions import INTRO

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
WRITE_TOOLS = {"add_tags", "remove_tags", "create_tag", "update_description"}


class _Clock:
    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now


def _app(gms: FakeGms, clock: Optional[_Clock] = None) -> Any:
    return create_app(
        AiServiceConfig(gms_url="http://gms.invalid:8080"),
        client_factory=FakeClientFactory(gms),
        server_info=lambda: gms.server_info(),
        telemetry_ping=lambda name, properties: None,
        usage_event_post=lambda event, token: None,
        policy_cache=McpPolicyCache(clock=clock or _Clock()),
    )


def _client(url: str, token: str = "alice-token", mode: str = "auto") -> Client:
    return Client(
        StreamableHttpTransport(
            f"{url}/mcp", headers={"Authorization": f"Bearer {token}"}
        ),
        mode=mode,
    )


def _default_server(**fields: Any) -> dict[str, Any]:
    return {
        "enabled": True,
        "defaultMcpServerOverrides": {
            "enabled": True,
            "instructionSize": None,
            "customInstructions": None,
            "tools": None,
            **fields,
        },
    }


async def _initialize(http: httpx.AsyncClient, url: str, token: str) -> httpx.Response:
    return await http.post(
        f"{url}/mcp",
        json=INITIALIZE,
        headers={**MCP_HEADERS, "Authorization": f"Bearer {token}"},
    )


@pytest.mark.parametrize(
    "settings",
    [
        {"enabled": False, "defaultMcpServerOverrides": None},
        _default_server(enabled=False),
    ],
)
async def test_mcp_off_in_settings_is_404(gms: FakeGms, settings: Any) -> None:
    gms.mcp_settings = settings

    async with serve(_app(gms)) as url, httpx.AsyncClient() as http:
        response = await _initialize(http, url, "alice-token")
        health = (await http.get(f"{url}/health")).json()

    assert response.status_code == 404
    assert response.json()["error"] == "mcp_disabled"
    assert health["mcp"]["settings"] == "disabled"
    assert health["mcp"]["available"] is False


async def test_settings_are_still_behind_authentication(gms: FakeGms) -> None:
    gms.mcp_settings = {"enabled": False, "defaultMcpServerOverrides": None}

    async with serve(_app(gms)) as url, httpx.AsyncClient() as http:
        response = await _initialize(http, url, "not-a-real-token")

    assert response.status_code == 401
    assert gms.settings_calls == 0


async def test_group_selection_filters_the_list_and_direct_calls(
    gms: FakeGms,
) -> None:
    gms.mcp_settings = _default_server(
        tools=[{"refType": "GROUP", "toolId": "read_metadata", "enabled": True}]
    )

    async with serve(_app(gms)) as url, _client(url) as client:
        names = {tool.name for tool in await client.list_tools()}
        with pytest.raises(ToolError, match="turned off"):
            await client.call_tool(
                "add_tags", {"tag_urn": "urn:li:tag:x", "entity_urns": []}
            )
        me = await client.call_tool("get_me")

    assert {"search", "get_entities", "get_lineage"} <= names
    assert not names & WRITE_TOOLS
    # System tools are served whatever the selection.
    assert "get_me" in names
    assert me.data["data"]["corpUser"]["urn"] == "urn:li:corpuser:alice"


async def test_a_tool_ref_overrides_its_group(gms: FakeGms) -> None:
    gms.mcp_settings = _default_server(
        tools=[
            {"refType": "GROUP", "toolId": "read_metadata", "enabled": True},
            {"refType": "TOOL", "toolId": "add_tags", "enabled": True},
            {"refType": "TOOL", "toolId": "get_lineage", "enabled": False},
        ]
    )

    async with serve(_app(gms)) as url, _client(url) as client:
        names = {tool.name for tool in await client.list_tools()}

    assert "add_tags" in names and "search" in names
    assert "get_lineage" not in names and "remove_tags" not in names


async def test_mutation_switch_beats_the_settings(
    gms: FakeGms, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("TOOLS_IS_MUTATION_ENABLED", "false")
    gms.mcp_settings = _default_server(
        tools=[
            {"refType": "GROUP", "toolId": "write_metadata", "enabled": True},
            {"refType": "TOOL", "toolId": "add_tags", "enabled": True},
        ]
    )

    async with serve(_app(gms)) as url, _client(url) as client:
        names = {tool.name for tool in await client.list_tools()}

    assert names == {"get_me"}


@pytest.mark.parametrize(
    ("size", "custom", "expected"),
    [
        ("NONE", "Ask the data team first.", "Ask the data team first."),
        ("NONE", None, None),
        ("SMALL", None, INTRO),
        ("SMALL", "Ask first.", INTRO + "\n\nAsk first."),
    ],
)
# Legacy clients get instructions from initialize, modern ones from server/discover.
@pytest.mark.parametrize("protocol", ["legacy", "auto"])
async def test_instructions_follow_the_settings(
    gms: FakeGms,
    size: str,
    custom: Optional[str],
    expected: Optional[str],
    protocol: str,
) -> None:
    gms.mcp_settings = _default_server(instructionSize=size, customInstructions=custom)

    async with serve(_app(gms)) as url, _client(url, mode=protocol) as client:
        instructions = client.instructions

    assert instructions == expected


async def test_large_instructions_name_only_selected_tools(gms: FakeGms) -> None:
    gms.mcp_settings = _default_server(
        tools=[{"refType": "GROUP", "toolId": "read_metadata", "enabled": True}]
    )

    async with serve(_app(gms)) as url, _client(url) as client:
        instructions = client.instructions or ""

    assert "`search`" in instructions
    assert "`add_tags`" not in instructions


@pytest.mark.parametrize(
    "error",
    [
        requests.Timeout("read timed out"),
        GraphError([{"message": "Failed to retrieve MCP settings"}]),
    ],
)
async def test_unreadable_settings_at_startup_are_503(
    gms: FakeGms, error: Exception
) -> None:
    gms.settings_error = error

    async with serve(_app(gms)) as url, httpx.AsyncClient() as http:
        response = await _initialize(http, url, "alice-token")
        health = (await http.get(f"{url}/health")).json()

    assert response.status_code == 503
    assert response.json()["error"] == "mcp_settings_unavailable"
    assert health["mcp"]["settings"] == "unavailable"
    assert health["mcp"]["available"] is False


@pytest.mark.parametrize(
    "answer", [GraphError([FIELD_UNDEFINED]), None], ids=["legacy", "null"]
)
async def test_legacy_gms_and_missing_settings_serve_defaults(
    gms: FakeGms, answer: Optional[Exception]
) -> None:
    if answer is None:
        gms.mcp_settings = None
    else:
        gms.settings_error = answer

    async with serve(_app(gms)) as url, _client(url) as client:
        names = {tool.name for tool in await client.list_tools()}
        instructions = client.instructions or ""

    assert {"search", "add_tags"} <= names
    assert "Use cases:" in instructions


async def test_settings_expire_and_recover(gms: FakeGms) -> None:
    clock = _Clock()

    async with serve(_app(gms, clock)) as url, httpx.AsyncClient() as http:
        assert (await _initialize(http, url, "alice-token")).status_code == 200

        # Turned off: callers keep the cached policy for up to 60 s.
        gms.mcp_settings = {"enabled": False, "defaultMcpServerOverrides": None}
        clock.now += 59
        assert (await _initialize(http, url, "alice-token")).status_code == 200
        clock.now += 1
        assert (await _initialize(http, url, "alice-token")).status_code == 404

        # GMS stops answering: once the policy is stale, every request is a 503.
        gms.settings_error = requests.ConnectionError("refused")
        clock.now += 60
        assert (await _initialize(http, url, "alice-token")).status_code == 503

        gms.settings_error = None
        gms.mcp_settings = {"enabled": True, "defaultMcpServerOverrides": None}
        assert (await _initialize(http, url, "alice-token")).status_code == 200


async def test_two_callers_share_one_global_policy(gms: FakeGms) -> None:
    clock = _Clock()
    gms.mcp_settings = {"enabled": False, "defaultMcpServerOverrides": None}

    async with serve(_app(gms, clock)) as url, httpx.AsyncClient() as http:
        assert (await _initialize(http, url, "alice-token")).status_code == 404
        assert (await _initialize(http, url, "bob-token")).status_code == 404

    assert gms.settings_calls == 1


async def test_a_caller_refused_the_settings_never_fails_open(gms: FakeGms) -> None:
    clock = _Clock()
    gms.settings_denied_tokens = {"bob-token"}

    async with serve(_app(gms, clock)) as url, httpx.AsyncClient() as http:
        bob = await _initialize(http, url, "bob-token")
        health = (await http.get(f"{url}/health")).json()
        assert (await _initialize(http, url, "alice-token")).status_code == 200

        # Policy now off, and alice's cached copy stale: bob still gets no tools.
        gms.mcp_settings = {"enabled": False, "defaultMcpServerOverrides": None}
        clock.now += 61
        bob_again = await _initialize(http, url, "bob-token")
        assert (await _initialize(http, url, "alice-token")).status_code == 404

    assert bob.status_code == 403
    assert health["mcp"]["settings"] == "unknown"
    assert bob_again.status_code == 403
