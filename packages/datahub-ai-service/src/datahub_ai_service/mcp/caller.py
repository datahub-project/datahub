"""The verified caller of the current MCP request, and their DataHub client."""

from typing import Any, Optional

from fastmcp.server.dependencies import get_http_request
from fastmcp.server.middleware import CallNext, Middleware, MiddlewareContext

from datahub_core_mcp_tools.graphql_helpers import with_datahub_client

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
    did can reach another caller's request.
    """

    def __init__(self, client_factory: CallerClientFactory) -> None:
        self._client_factory = client_factory

    async def on_message(
        self, context: MiddlewareContext[Any], call_next: CallNext[Any, Any]
    ) -> Any:
        caller = current_caller()
        if caller is None:
            # McpAuthMiddleware sits in front of every /mcp request, so this is a
            # wiring bug. Refuse rather than run tools without an identity.
            raise RuntimeError("MCP request reached the server without a caller")

        client = self._client_factory.build(caller.token)
        try:
            with with_datahub_client(client):
                return await call_next(context)
        finally:
            client._graph.close()
