from typing import Any

import jwt
import mcp.types as mt
from datahub.metadata.urns import CorpUserUrn
from datahub.utilities.perf_timer import PerfTimer
from fastmcp.server.dependencies import get_http_headers
from fastmcp.server.middleware import CallNext, Middleware, MiddlewareContext
from loguru import logger

from datahub_integrations.mcp.mcp_server import get_datahub_client
from datahub_integrations.telemetry.mcp_events import MCPServerRequestEvent
from datahub_integrations.telemetry.telemetry import track_saas_event


class _UnknownUserError(Exception):
    pass


def _get_user() -> CorpUserUrn:
    """Attempts to parse the user info from the JWT token.

    IMPORTANT: This is purely for the purpose of telemetry and does not verify the JWT
    signature. Do not use this to verify the user's identity or for any other
    security-related purposes.

    This is fine for telemetry, since the worst case is that we misattribute the call.
    The alternative is to make a call to GMS to verify the token, but that didn't
    seem worth the extra complexity.

    Raises:
        _UnknownUserError: If the token is not present or cannot be parsed.
        Exception: If there is an unexpected error.

    Returns:
        The user urn.
    """
    try:
        client = get_datahub_client()
        return _get_user_from_token(client._graph.config.token)
    except LookupError:
        raise _UnknownUserError("No datahub client found") from None


def _get_user_from_token(token: str | None) -> CorpUserUrn:
    if not token:
        raise _UnknownUserError("No token found")

    payload = jwt.decode(token, options={"verify_signature": False})
    return CorpUserUrn(str(payload["sub"]))


class MCPTelemetryMiddleware(Middleware):
    async def on_request(
        self,
        context: MiddlewareContext[mt.Request],
        call_next: CallNext[mt.Request, Any],
    ) -> Any:
        actor = None
        try:
            actor = _get_user()
        except Exception as e:
            if not isinstance(e, _UnknownUserError):
                logger.debug(f"Failed to get user from token: {e}")

        request_headers = get_http_headers()
        user_agent = request_headers.get("user-agent")

        # See https://gofastmcp.com/servers/middleware#hook-parameters
        event = MCPServerRequestEvent(
            source=context.source,
            request_type=context.type,
            method=context.method,
            user_urn=str(actor) if actor else None,
            user_agent=user_agent,
            duration_seconds=0,  # Set afterwards.
        )
        if isinstance(context.message, mt.CallToolRequestParams):
            event.tool_name = context.message.name

        with PerfTimer() as timer:
            try:
                result = await call_next(context)

                if isinstance(result, mt.CallToolResult):
                    event.tool_result_length = sum(
                        len(block.text)
                        for block in result.content
                        if isinstance(block, mt.TextContent)
                    )
                    event.tool_result_is_error = result.isError

                return result
            except Exception as e:
                event.tool_result_is_error = True
                event.tool_result_length = len(str(e))
                raise
            finally:
                event.duration_seconds = timer.elapsed_seconds()
                track_saas_event(event)
