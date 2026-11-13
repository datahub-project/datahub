"""Serve DataHub's MCP tools over stdio, for one local user.

``uvx --from datahub-core-mcp-tools datahub-mcp-stdio`` runs the same tools the
datahub-ai-service serves, without installing the service: an MCP client (an
IDE, Claude Desktop, an agent) starts it as a subprocess and talks to it over
stdin/stdout. It calls DataHub as one user, with ``DATAHUB_GMS_URL`` and
``DATAHUB_GMS_TOKEN`` or the ``~/.datahubenv`` that ``datahub init`` writes (no
token is needed when DataHub's authentication is off).

The same environment switches as the service choose the tools
(``TOOLS_IS_MUTATION_ENABLED``, ``SEMANTIC_SEARCH_ENABLED`` and the per-tool
flags), and tools the connected DataHub is too old for are hidden. Each tool
call sends anonymous product telemetry with ``deployment: "stdio"``
(``DATAHUB_TELEMETRY_ENABLED=false`` turns it off); there are no usage events
and no MCP settings.
"""

import sys
from typing import Any, Callable, Collection, Optional

import anyio
from datahub.ingestion.graph.config import ClientMode
from datahub.sdk.main_client import DataHubClient
from fastmcp import FastMCP
from fastmcp.server.middleware import CallNext, Middleware, MiddlewareContext
from loguru import logger

from ._version import __version__
from .gms_server_info import GmsServerInfo, GmsServerInfoCache
from .graphql_helpers import with_datahub_client
from .mcp_instructions import InstructionSize, build_instructions
from .mcp_server import register_tools
from .telemetry import Ping, TelemetryMiddleware, ping_server_start, sdk_ping
from .tool_context import ToolContext
from .version_filter import VersionFilterMiddleware

DEPLOYMENT = "stdio"


class _ClientMiddleware(Middleware):
    """Runs every MCP message with the one local client, and the GMS's version
    in the tool context so tools compile their queries for it."""

    def __init__(
        self, client: DataHubClient, server_info: Callable[[], GmsServerInfo]
    ) -> None:
        self._client = client
        self._server_info = server_info

    def _tool_context(self) -> ToolContext:
        try:
            return ToolContext([self._server_info()])
        except Exception as e:
            # Tools then run as if the version were unknown: every selection is
            # kept and the SDK strips what GMS lacks.
            logger.warning(f"Could not read the GMS version: {e}")
            return ToolContext()

    async def on_message(
        self, context: MiddlewareContext[Any], call_next: CallNext[Any, Any]
    ) -> Any:
        tool_context = await anyio.to_thread.run_sync(self._tool_context)
        with with_datahub_client(self._client, tool_context):
            return await call_next(context)


def _gms_url(client: DataHubClient) -> str:
    return client._graph.config.server


def build_stdio_server(
    client: DataHubClient,
    *,
    groups: Optional[Collection[str]] = None,
    server_info: Optional[Callable[[], GmsServerInfo]] = None,
    telemetry_ping: Ping = sdk_ping,
) -> FastMCP:
    """A FastMCP server with DataHub's tools, calling DataHub through ``client``.

    Hosts that embed the tools (an agent, a test) can run it on any transport;
    ``main`` runs it over stdio.

    Args:
        client: The DataHub client every tool call uses.
        groups: Register only these tool groups (see tool_groups); every group
            by default.
        server_info: The connected GMS; read from its /config (cached) by default.
        telemetry_ping: Sends product telemetry; the SDK's by default.
    """
    if server_info is None:
        cache = GmsServerInfoCache()
        gms_url = _gms_url(client)

        def server_info() -> GmsServerInfo:
            return cache.get(gms_url)

    mcp = FastMCP[None](name="datahub", version=__version__)
    served = register_tools(mcp, is_oss=True, groups=groups)
    mcp.instructions = build_instructions(InstructionSize.LARGE, served)
    # The client must be in place before anything that calls DataHub.
    mcp.add_middleware(_ClientMiddleware(client, server_info))
    mcp.add_middleware(VersionFilterMiddleware(server_info))
    mcp.add_middleware(
        TelemetryMiddleware(
            deployment=DEPLOYMENT, server_info=server_info, ping=telemetry_ping
        )
    )
    return mcp


def main() -> None:
    """``datahub-mcp-stdio``: serve DataHub's tools over stdin/stdout."""
    if any(arg in ("-V", "--version") for arg in sys.argv[1:]):
        print(f"datahub-mcp-stdio {__version__}")
        return
    # stdout carries the protocol, so logs go to stderr; clients show them as
    # the server's log, where per-call debug lines are noise.
    logger.remove()
    logger.add(sys.stderr, level="INFO")
    client = DataHubClient.from_env(
        client_mode=ClientMode.SDK,
        datahub_component=f"datahub-mcp-stdio/{__version__}",
    )
    gms_url = _gms_url(client)
    cache = GmsServerInfoCache()

    def server_info() -> GmsServerInfo:
        return cache.get(gms_url)

    mcp = build_stdio_server(client, server_info=server_info)
    ping_server_start(deployment=DEPLOYMENT, server_info=server_info)
    mcp.run(transport="stdio", show_banner=False)


if __name__ == "__main__":
    main()
