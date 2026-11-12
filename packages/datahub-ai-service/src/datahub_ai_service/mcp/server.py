"""The FastMCP server datahub-ai-service serves at /mcp."""

from typing import Sequence

from fastmcp import FastMCP

from datahub_core_mcp_tools.mcp_server import (
    DEFAULT_INSTRUCTIONS,
    disable_list_changed_capabilities,
    register_tools,
)

from .._version import __version__
from .auth import CallerClientFactory
from .caller import CallerClientMiddleware
from .telemetry import ToolCallObserver, ToolCallTelemetryMiddleware


def build_mcp_server(
    client_factory: CallerClientFactory,
    *,
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
    mcp.add_middleware(ToolCallTelemetryMiddleware(observers))
    return mcp
