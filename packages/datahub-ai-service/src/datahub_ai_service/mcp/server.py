"""The FastMCP server datahub-ai-service serves at /mcp."""

from typing import Callable, Optional, Sequence

from fastmcp import FastMCP

from datahub_core_mcp_tools.gms_server_info import GmsServerInfo
from datahub_core_mcp_tools.mcp_instructions import InstructionSize, build_instructions
from datahub_core_mcp_tools.mcp_server import (
    disable_list_changed_capabilities,
    register_tools,
)
from datahub_core_mcp_tools.telemetry import ToolCall
from datahub_core_mcp_tools.version_filter import VersionFilterMiddleware

from .._version import __version__
from ..metrics import ServiceMetrics
from .caller import CallerClientMiddleware
from .client_factory import CallerClientFactory
from .policy import McpPolicyMiddleware
from .telemetry import ToolCallObserver, ToolCallTelemetryMiddleware


def build_mcp_server(
    client_factory: CallerClientFactory,
    *,
    server_info: Callable[[], GmsServerInfo],
    tool_time_limit_seconds: Optional[float] = None,
    on_token_rejected: Optional[Callable[[str], None]] = None,
    observers: Sequence[ToolCallObserver] = (),
    telemetry_sinks: Sequence[Callable[[ToolCall], None]] = (),
    metrics: Optional[ServiceMetrics] = None,
) -> FastMCP:
    mcp = FastMCP[None](name="datahub", version=__version__)
    served = register_tools(mcp, is_oss=True)
    # The default; McpPolicyMiddleware serves each request the settings' own.
    mcp.instructions = build_instructions(InstructionSize.LARGE, served)
    # Served statelessly, so there is no session to announce list changes on.
    disable_list_changed_capabilities(mcp)

    # The first middleware added is the outermost: the caller's client must be in
    # place before anything else (including telemetry) runs.
    mcp.add_middleware(
        CallerClientMiddleware(
            client_factory,
            server_info,
            time_limit_seconds=tool_time_limit_seconds,
            on_token_rejected=on_token_rejected,
        )
    )
    mcp.add_middleware(McpPolicyMiddleware(server_info))
    mcp.add_middleware(VersionFilterMiddleware(server_info))
    sinks = list(telemetry_sinks)
    if metrics is not None:
        sinks.append(metrics.tool_call_sink(served))
    mcp.add_middleware(ToolCallTelemetryMiddleware(observers, sinks=sinks))
    return mcp
