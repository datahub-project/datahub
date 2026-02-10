from unittest.mock import patch

import anyio
import fastmcp
import pytest

from datahub_integrations.mcp.mcp_telemetry import (
    MCPTelemetryMiddleware,
)
from datahub_integrations.telemetry.mcp_events import MCPServerRequestEvent


@pytest.mark.anyio
async def test_telemetry_middleware_tool_failure() -> None:
    mcp_server = fastmcp.FastMCP[None]()

    @mcp_server.tool
    async def sleep_and_fail() -> None:
        await anyio.sleep(0.2)
        raise Exception("tool call failed")

    mcp_server.add_middleware(MCPTelemetryMiddleware())

    with patch("datahub_integrations.mcp.mcp_telemetry.track_saas_event") as mock_track:
        async with fastmcp.Client(mcp_server) as client:
            result = await client.call_tool("sleep_and_fail", raise_on_error=False)
            assert result.is_error
            assert "tool call failed" in str(result.content)

    # The MCP protocol sends multiple requests during a session (e.g. initialize,
    # tools/list, tools/call). Filter to the events we care about.
    all_events = [call[0][0] for call in mock_track.call_args_list]
    assert all(isinstance(e, MCPServerRequestEvent) for e in all_events)

    list_tools_events = [e for e in all_events if e.method == "tools/list"]
    assert len(list_tools_events) == 1
    list_tools_event = list_tools_events[0]
    assert list_tools_event.source == "client"
    assert list_tools_event.request_type == "request"
    assert list_tools_event.tool_name is None

    tool_call_events = [e for e in all_events if e.method == "tools/call"]
    assert len(tool_call_events) == 1
    tool_call_event = tool_call_events[0]
    assert tool_call_event.source == "client"
    assert tool_call_event.request_type == "request"
    assert tool_call_event.tool_name == "sleep_and_fail"
    assert tool_call_event.tool_result_is_error
    assert (
        tool_call_event.tool_result_length is not None
        and tool_call_event.tool_result_length > 0
    )
    assert tool_call_event.duration_seconds >= 0.2
