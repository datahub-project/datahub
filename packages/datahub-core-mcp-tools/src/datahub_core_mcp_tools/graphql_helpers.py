"""Per-request DataHub client context and GraphQL execution for MCP tools.

Every tool reads its DataHub client from a context variable that the host sets
for the duration of one request. The host builds that client from the caller's
own credential, so two concurrent callers never share a client.
"""

from __future__ import annotations

import contextlib
import contextvars
import functools
import pathlib
from dataclasses import dataclass
from dataclasses import field as dataclass_field
from typing import Any, Dict, Iterator, Optional

from datahub.ingestion.graph.client import DataHubGraph
from datahub.sdk.main_client import DataHubClient
from loguru import logger

from .gms_server_info import GmsServerInfo
from .gql_compiler import MARKER_TEXT, compile_gql
from .tool_context import ToolContext

GQL_DIR = pathlib.Path(__file__).parent / "gql"


@functools.lru_cache(maxsize=None)
def load_gql_document(name: str) -> str:
    """A packaged GraphQL document from the gql/ directory, by file name."""
    return (GQL_DIR / name).read_text(encoding="utf-8")


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

    A query with ``#[since]`` markers is first compiled for the GMS the host put
    in the request's tool context (see gql_compiler). Without one, nothing is
    known about GMS, so every selection is kept and stripped at runtime.

    strip_unsupported_fields delegates schema compatibility to the SDK's
    introspection-based projection: fields the server lacks are removed before
    sending, and the caller treats them as absent. A schema error on a known
    GMS version is raised as is, never retried with fewer fields.
    """
    if MARKER_TEXT in query:
        try:
            info = get_mcp_context().tool_context.get(GmsServerInfo)
        except LookupError:
            info = None
        compiled = compile_gql(query, info)
        query = compiled.query
        strip_unsupported_fields |= compiled.strip_unsupported_fields
        if variables and compiled.dropped_variables:
            variables = {
                k: v
                for k, v in variables.items()
                if k not in compiled.dropped_variables
            }
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
