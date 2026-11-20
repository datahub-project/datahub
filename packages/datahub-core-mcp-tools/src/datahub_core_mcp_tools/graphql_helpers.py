"""Per-request DataHub client context and GraphQL execution for MCP tools.

Every tool reads its DataHub client from a context variable that the host sets
for the duration of one request. The host builds that client from the caller's
own credential, so two concurrent callers never share a client.
"""

from __future__ import annotations

import contextlib
import contextvars
from dataclasses import dataclass
from dataclasses import field as dataclass_field
from typing import Any, Dict, Iterator, Optional

from datahub.ingestion.graph.client import DataHubGraph
from datahub.sdk.main_client import DataHubClient
from loguru import logger

from .tool_context import ToolContext


@dataclass
class MCPContext:
    """Per-request context for MCP tool execution."""

    client: DataHubClient
    tool_context: ToolContext = dataclass_field(default_factory=ToolContext)
    # A DataHub Cloud concept, typed Any so this module needs no Cloud imports.
    task_context: Any = None


_mcp_context = contextvars.ContextVar[MCPContext]("_mcp_context")


def get_mcp_context() -> MCPContext:
    """Get the current MCP context. Raises LookupError if not set."""
    return _mcp_context.get()


def get_datahub_client() -> DataHubClient:
    return get_mcp_context().client


@contextlib.contextmanager
def with_datahub_client(
    client: DataHubClient,
    tool_context: ToolContext | None = None,
    task_context: Any = None,
) -> Iterator[None]:
    ctx = MCPContext(
        client=client,
        tool_context=tool_context or ToolContext(),
        task_context=task_context,
    )
    token = _mcp_context.set(ctx)
    try:
        yield
    finally:
        _mcp_context.reset(token)


def execute_graphql(
    graph: DataHubGraph,
    *,
    query: str,
    operation_name: Optional[str] = None,
    variables: Optional[Dict[str, Any]] = None,
    strip_unsupported_fields: bool = False,
) -> Any:
    """Run a GraphQL operation against the caller's DataHub.

    strip_unsupported_fields delegates schema compatibility to the SDK's
    introspection-based projection: fields the server lacks are removed before
    sending, and the caller treats them as absent.
    """
    try:
        return graph.execute_graphql(
            query=query,
            variables=variables,
            operation_name=operation_name,
            strip_unsupported_fields=strip_unsupported_fields,
        )
    except Exception as e:
        # Variables can carry user data, so they stay out of the log.
        logger.warning(f"GraphQL {operation_name or 'query'} failed: {e}")
        raise
