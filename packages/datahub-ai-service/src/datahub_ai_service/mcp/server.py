"""The FastMCP server datahub-ai-service serves at /mcp."""

from typing import Callable, Sequence

from fastmcp import FastMCP

from datahub_core_mcp_tools.gms_server_info import GmsServerInfo
from datahub_core_mcp_tools.mcp_server import (
    DEFAULT_INSTRUCTIONS,
    disable_list_changed_capabilities,
    register_tools,
)
from datahub_core_mcp_tools.version_filter import VersionFilterMiddleware

from .._version import __version__
from .auth import CallerClientFactory
from .caller import CallerClientMiddleware
from .telemetry import ToolCallObserver, ToolCallTelemetryMiddleware


def build_mcp_server(
    client_factory: CallerClientFactory,
    *,
    server_info: Callable[[], GmsServerInfo],
    observers: Sequence[ToolCallObserver] = (),
) -> FastMCP:
    mcp = FastMCP[None](
        name="datahub", instructions=DEFAULT_INSTRUCTIONS, version=__version__
    )
    register_tools(mcp, is_oss=True)
    # Served statelessly, so there is no session to announce list changes on.
    disable_list_changed_capabilities(mcp)

    # The first middleware added is the outermost: the caller's client must be in
    # place before anything else (including telemetry) runs.
    mcp.add_middleware(CallerClientMiddleware(client_factory))
    mcp.add_middleware(VersionFilterMiddleware(server_info))
    mcp.add_middleware(ToolCallTelemetryMiddleware(observers))
    return mcp
