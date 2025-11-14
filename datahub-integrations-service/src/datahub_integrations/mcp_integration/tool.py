from __future__ import annotations

import dataclasses
import functools
import inspect
import json
from typing import Any, Awaitable, Callable, List, Optional, ParamSpec, TypeVar

import asyncer
import fastmcp.tools
import mlflow
import mlflow.entities
from fastmcp import FastMCP
from loguru import logger
from mcp.types import TextContent

from datahub_integrations.chat.context_reducer import TokenCountEstimator
from datahub_integrations.mcp.mcp_server import TOOL_RESPONSE_TOKEN_LIMIT

_P = ParamSpec("_P")
_R = TypeVar("_R")


# Note: This is intentionally duplicated from mcp_server.py (which is a copy from upstream project)
# to keep tool.py independent and mcp_server.py easy to sync with upstream.
# See https://github.com/jlowin/fastmcp/issues/864#issuecomment-3103678258
# for why we need to wrap sync functions with asyncify.
def async_background(fn: Callable[_P, _R]) -> Callable[_P, Awaitable[_R]]:
    """Wrap a synchronous function to run in a background thread.

    This prevents blocking the event loop when the function does I/O operations.
    Use this for any sync function that makes API calls or does other blocking work.

    Args:
        fn: A synchronous function to wrap

    Returns:
        An async version of the function that runs in a thread pool

    Raises:
        RuntimeError: If fn is already async (use it directly instead)
    """
    if inspect.iscoroutinefunction(fn):
        raise RuntimeError("async_background can only be used on non-async functions")

    @functools.wraps(fn)
    async def wrapper(*args: _P.args, **kwargs: _P.kwargs) -> _R:
        try:
            return await asyncer.asyncify(fn)(*args, **kwargs)
        except Exception:
            # Log with full stack trace before FastMCP catches it
            logger.exception(
                f"Tool function {fn.__name__} failed with args={args}, kwargs={kwargs}"
            )
            raise

    return wrapper


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

        This method handles both sync and async tool functions:
        - For async tools: uses syncify to run them synchronously
        - For sync tools: runs them directly without blocking async operations
        """
        with mlflow.start_span(
            f"run_tool_{self.name}",
            span_type=mlflow.entities.SpanType.TOOL,
            attributes={"tool_name": self.name},
        ) as span:
            span.set_inputs(arguments)
            try:
                # Check if _tool.run is a coroutine function (async)
                # FastMCP's Tool.run is always async, so we need to use syncify
                # The raise_sync_error=False allows this to work even if we're in an async context
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
        """Create a ToolWrapper from a function.

        Note: For sync functions that do blocking I/O (like API calls), wrap them
        with async_background() before passing to this method. See mcp_server.py
        for examples.
        """
        return ToolWrapper(
            fastmcp.tools.Tool.from_function(fn, name=name, description=description)
        )


def tools_from_fastmcp(mcp: FastMCP) -> List[ToolWrapper]:
    return [
        ToolWrapper(tool, name_prefix=mcp.name)
        for tool in mcp._tool_manager._tools.values()
    ]
