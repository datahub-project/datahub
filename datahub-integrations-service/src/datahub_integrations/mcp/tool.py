from __future__ import annotations

import dataclasses
import json
import os
from typing import Any, Callable, List, Optional

import asyncer
import fastmcp.tools
import mlflow
import mlflow.entities
from fastmcp import FastMCP
from mcp.types import TextContent

from datahub_integrations.chat.context_reducer import TokenCountEstimator

# Maximum token count for tool responses to prevent context window issues
# As per telemetry tool result length goes upto
TOOL_RESPONSE_TOKEN_LIMIT = int(os.getenv("TOOL_RESPONSE_TOKEN_LIMIT", 80000))


class ToolRunError(Exception):
    """Error raised when a tool fails to execute."""

    pass


@dataclasses.dataclass
class ToolWrapper:
    _tool: fastmcp.tools.Tool
    name_prefix: Optional[str] = dataclasses.field(default=None)

    @property
    def name(self) -> str:
        if self.name_prefix is not None:
            return f"{self.name_prefix}__{self._tool.name}"
        else:
            return self._tool.name

    def to_bedrock_spec(self) -> dict:
        schema = self._tool.parameters
        return {
            "toolSpec": {
                "name": self.name,
                "description": self._tool.description,
                "inputSchema": {"json": schema},
            }
        }

    def run(self, arguments: dict) -> str | dict:
        """Run the tool with arguments.

        Tools with complex return types will be returned as dicts. Otherwise,
        the results will be JSON-serialized.
        """
        with mlflow.start_span(
            f"run_tool_{self.name}",
            span_type=mlflow.entities.SpanType.TOOL,
            attributes={"tool_name": self.name},
        ) as span:
            span.set_inputs(arguments)
            try:
                tool_result = asyncer.syncify(self._tool.run, raise_sync_error=False)(
                    arguments
                )
            except Exception as e:
                raise ToolRunError(f"Error executing tool {self.name}: {e}") from e
            else:
                result: str | dict
                if tool_result.structured_content is not None:
                    result = tool_result.structured_content
                else:
                    # The FastMCP framework always stringifies the results.
                    assert len(tool_result.content) == 1
                    assert isinstance(tool_result.content[0], TextContent)
                    result = tool_result.content[0].text

                token_count = TokenCountEstimator.estimate_tokens(
                    result if isinstance(result, str) else json.dumps(result)
                )

                if token_count > TOOL_RESPONSE_TOKEN_LIMIT:
                    error_message = (
                        f"Tool response too large: {token_count:,} tokens (limit: {TOOL_RESPONSE_TOKEN_LIMIT:,}). "
                        f"Please try with more specific parameters or filters to reduce the result size."
                    )
                    raise ToolRunError(
                        f"Error executing tool {self.name}: {error_message}"
                    )

                return result

    @classmethod
    def from_function(
        cls, fn: Callable[..., Any], name: str, description: str
    ) -> ToolWrapper:
        return ToolWrapper(
            fastmcp.tools.Tool.from_function(fn, name=name, description=description)
        )


def tools_from_fastmcp(mcp: FastMCP) -> List[ToolWrapper]:
    return [
        ToolWrapper(tool, name_prefix=mcp.name)
        for tool in mcp._tool_manager._tools.values()
    ]
