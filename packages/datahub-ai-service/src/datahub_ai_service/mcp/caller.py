"""The caller of the current MCP request, and their DataHub client."""

import asyncio
import dataclasses
from typing import Any, Callable, Optional

from fastmcp.server.dependencies import get_http_request
from fastmcp.server.middleware import CallNext, Middleware, MiddlewareContext
from loguru import logger

from datahub_core_mcp_tools.gms_server_info import GmsServerInfo
from datahub_core_mcp_tools.graphql_helpers import with_datahub_client
from datahub_core_mcp_tools.tool_context import ToolContext

from .auth import CALLER_STATE_KEY, Caller
from .auth_mode import GmsAuth
from .client_factory import CallerClientFactory


def current_caller() -> Optional[Caller]:
    """The caller McpAuthMiddleware admitted for this request, if any."""
    try:
        request = get_http_request()
    except RuntimeError:
        return None
    caller = request.scope.get("state", {}).get(CALLER_STATE_KEY)
    return caller if isinstance(caller, Caller) else None


class CallerClientMiddleware(Middleware):
    """Run each MCP message with a DataHub client built from its caller's token.

    The client lives for one message and is never cached, so nothing a caller
    did can reach another caller's request. The GMS's version and auth mode ride
    along in the tool context, so tools compile their queries for it.
    """

    def __init__(
        self,
        client_factory: CallerClientFactory,
        server_info: Callable[[], GmsServerInfo],
        *,
        time_limit_seconds: Optional[float] = None,
        on_token_rejected: Optional[Callable[[str], None]] = None,
    ) -> None:
        self._client_factory = client_factory
        self._server_info = server_info
        self._time_limit_seconds = time_limit_seconds
        self._on_token_rejected = on_token_rejected

    async def _tool_context(self, caller: Caller) -> ToolContext:
        try:
            info = await asyncio.to_thread(self._server_info)
            if info.auth_enabled is None and caller.gms_auth is not GmsAuth.UNKNOWN:
                # An older GMS: the auth mode came from a probe.
                info = dataclasses.replace(
                    info, auth_enabled=caller.gms_auth is GmsAuth.ON
                )
            return ToolContext([info])
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

        tool_context = await self._tool_context(caller)
        token, on_rejected = caller.token, self._on_token_rejected
        client = self._client_factory.build(
            token,
            time_limit_seconds=self._time_limit_seconds,
            on_rejected=(
                (lambda: on_rejected(token))
                if on_rejected is not None and token is not None and caller.verified
                else None
            ),
        )
        try:
            with with_datahub_client(client, tool_context):
                return await call_next(context)
        finally:
            client._graph.close()
