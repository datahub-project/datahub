from __future__ import annotations

from datahub_integrations.gen_ai.mlflow_init import MLFLOW_INITIALIZED

import dataclasses
from typing import Any, Callable, List, Optional

import asyncer
import mcp.server.fastmcp.exceptions
import mcp.server.fastmcp.tools
import mlflow
import mlflow.entities
from mcp.server.fastmcp import FastMCP

assert MLFLOW_INITIALIZED


class ToolRunError(Exception):
    """Error raised when a tool fails to execute."""

    pass


@dataclasses.dataclass
class ToolWrapper:
    _tool: mcp.server.fastmcp.tools.Tool
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

    def run(self, arguments: dict) -> Any:
        """Run the tool with arguments."""
        with mlflow.start_span(
            f"run_tool_{self.name}",
            span_type=mlflow.entities.SpanType.TOOL,
            attributes={"tool_name": self.name},
        ):
            try:
                return asyncer.syncify(self._tool.run, raise_sync_error=False)(
                    arguments
                )
            except mcp.server.fastmcp.exceptions.ToolError as e:
                # Ensure that the "Error executing tool" message is not included twice.
                raise ToolRunError(e) from e
            except Exception as e:
                raise ToolRunError(f"Error executing tool {self.name}: {e}") from e

    @classmethod
    def from_function(
        cls, fn: Callable[..., Any], name: str, description: str
    ) -> ToolWrapper:
        return ToolWrapper(
            mcp.server.fastmcp.tools.Tool.from_function(
                fn, name=name, description=description
            )
        )


def tools_from_fastmcp(mcp: FastMCP) -> List[ToolWrapper]:
    return [
        ToolWrapper(tool, name_prefix=mcp.name)
        for tool in mcp._tool_manager.list_tools()
    ]
