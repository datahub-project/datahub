"""The verified caller of the current MCP request, and their DataHub client."""

import asyncio
from typing import Any, Callable, Optional

from fastmcp.server.dependencies import get_http_request
from fastmcp.server.middleware import CallNext, Middleware, MiddlewareContext
from loguru import logger

from datahub_core_mcp_tools.gms_server_info import GmsServerInfo
from datahub_core_mcp_tools.graphql_helpers import with_datahub_client
from datahub_core_mcp_tools.tool_context import ToolContext

from .auth import CALLER_STATE_KEY, CallerClientFactory, VerifiedCaller


def current_caller() -> Optional[VerifiedCaller]:
    """The caller McpAuthMiddleware verified for this request, if any."""
    try:
        request = get_http_request()
    except RuntimeError:
        return None
    caller = request.scope.get("state", {}).get(CALLER_STATE_KEY)
    return caller if isinstance(caller, VerifiedCaller) else None


class CallerClientMiddleware(Middleware):
    """Run each MCP message with a DataHub client built from its caller's token.

    The client lives for one message and is never cached, so nothing a caller
    did can reach another caller's request. The GMS's version info rides along
    in the tool context, so tools compile their queries for it.
    """

    def __init__(
        self,
        client_factory: CallerClientFactory,
        server_info: Callable[[], GmsServerInfo],
    ) -> None:
        self._client_factory = client_factory
        self._server_info = server_info

    async def _tool_context(self) -> ToolContext:
        try:
            return ToolContext([await asyncio.to_thread(self._server_info)])
        except Exception as e:
            # Tools then run as if the version were unknown: every selection is
            # kept and the SDK strips what GMS lacks.
            logger.warning(f"Could not read the GMS version: {e}")
            return ToolContext()

    async def on_message(
        self, context: MiddlewareContext[Any], call_next: CallNext[Any, Any]
    ) -> Any:
        caller = current_caller()
        if caller is None:
            # McpAuthMiddleware sits in front of every /mcp request, so this is a
            # wiring bug. Refuse rather than run tools without an identity.
            raise RuntimeError("MCP request reached the server without a caller")

        tool_context = await self._tool_context()
        client = self._client_factory.build(caller.token)
        try:
            with with_datahub_client(client, tool_context):
                return await call_next(context)
        finally:
            client._graph.close()
