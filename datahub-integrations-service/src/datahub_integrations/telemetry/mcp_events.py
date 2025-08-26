from typing import Literal

from datahub_integrations.telemetry.telemetry import BaseEvent


class MCPServerRequestEvent(BaseEvent):
    """Event representing a request to the MCP server."""

    type: Literal["McpServerRequest"] = "McpServerRequest"

    source: str
    request_type: str
    method: str | None
    user_agent: str | None = None
    duration_seconds: float

    # Attributes set only for tool calls.
    tool_name: str | None = None
    tool_result_length: int | None = None
    tool_result_is_error: bool | None = None
