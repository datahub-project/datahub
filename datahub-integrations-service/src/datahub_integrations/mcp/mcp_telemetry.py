from typing import Any

import mcp.types as mt
from datahub.utilities.perf_timer import PerfTimer
from fastmcp.server.middleware import CallNext, Middleware, MiddlewareContext

from datahub_integrations.telemetry.mcp_events import MCPServerRequestEvent
from datahub_integrations.telemetry.telemetry import track_saas_event


class MCPTelemetryMiddleware(Middleware):
    async def on_request(
        self,
        context: MiddlewareContext[mt.Request],
        call_next: CallNext[mt.Request, Any],
    ) -> Any:
        with PerfTimer() as timer:
            result = await call_next(context)

        # See https://gofastmcp.com/servers/middleware#hook-parameters
        event = MCPServerRequestEvent(
            source=context.source,
            request_type=context.type,
            method=context.method,
            duration_seconds=timer.elapsed_seconds(),
        )
        if isinstance(context.message, mt.CallToolRequestParams):
            event.tool_name = context.message.name
        if isinstance(result, mt.CallToolResult):
            event.tool_result_is_error = result.isError
            event.tool_result_length = sum(
                len(block.text)
                for block in result.content
                if isinstance(block, mt.TextContent)
            )
        track_saas_event(event)

        return result
