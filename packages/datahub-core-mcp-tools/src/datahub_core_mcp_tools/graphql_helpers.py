"""Per-request DataHub client context and GraphQL execution for MCP tools.

Every tool reads its DataHub client from a context variable that the host sets
for the duration of one request. The host builds that client from the caller's
own credential, so two concurrent callers never share a client.
"""

from __future__ import annotations

import contextlib
import contextvars
import functools
import os
import pathlib
import re
from dataclasses import dataclass
from dataclasses import field as dataclass_field
from typing import Any, Dict, Iterator, Optional

import requests
from datahub.configuration.common import GraphError
from datahub.ingestion.graph.client import DataHubGraph
from datahub.sdk.main_client import DataHubClient
from datahub.sdk.search_filters import Filter
from fastmcp.exceptions import ToolError
from loguru import logger

from .gms_server_info import GmsServerInfo
from .gql_compiler import MARKER_TEXT, compile_gql
from .search_filter_parser import parse_filter_string
from .tool_context import ToolContext

GQL_DIR = pathlib.Path(__file__).parent / "gql"


def _env_limit(name: str, default: int) -> int:
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    try:
        value = int(raw)
    except ValueError:
        raise ValueError(f"{name} must be a whole number, not {raw!r}") from None
    if value <= 0:
        raise ValueError(f"{name} must be positive, not {raw!r}")
    return value


# Size limits on what tools return, each overridable by the environment variable
# named. They keep one response inside an LLM's context window.

# An entity's own description.
DESCRIPTION_LENGTH_HARD_LIMIT = _env_limit("DESCRIPTION_LENGTH_LIMIT", 5000)
# An entity's description when a caller asks for it by name (about 7K tokens).
REQUESTED_DESCRIPTION_LENGTH_LIMIT = _env_limit(
    "REQUESTED_DESCRIPTION_LENGTH_LIMIT", 20000
)
# Each customProperties value: some carry base64 blobs or whole configs.
CUSTOM_PROPERTY_VALUE_HARD_LIMIT = _env_limit("CUSTOM_PROPERTY_VALUE_LIMIT", 5000)
# SQL statements and view definitions.
QUERY_LENGTH_HARD_LIMIT = _env_limit("QUERY_LENGTH_LIMIT", 5000)
# Document content.
DOCUMENT_CONTENT_CHAR_LIMIT = _env_limit("DOCUMENT_CONTENT_CHAR_LIMIT", 8000)
# Every piece of per-column text (description, edited description, deprecation
# note), so a column reads the same length whichever aspect its text came from.
SCHEMA_FIELD_TEXT_LENGTH_LIMIT = _env_limit("FIELD_DESCRIPTION_LENGTH_LIMIT", 120)
# An assertion's source-defined key/value lists (run nativeResults and error
# properties, nativeParameters, customProperties) have no size limit (compiled SQL,
# sampled rows), so both the key count and each value are capped.
MAX_ASSERTION_KEY_VALUE_ENTRIES = _env_limit("MAX_ASSERTION_KEY_VALUE_ENTRIES", 10)
MAX_ASSERTION_KEY_VALUE_CHARS = _env_limit("MAX_ASSERTION_KEY_VALUE_CHARS", 200)
# Data quality members per data contract: each carries its latest run, and a
# contract can group hundreds of assertions.
MAX_CONTRACT_DATA_QUALITY_ASSERTIONS = _env_limit(
    "MAX_CONTRACT_DATA_QUALITY_ASSERTIONS", 25
)
# One response, kept under common MCP client output limits (about 25K tokens).
MCP_CLIENT_OUTPUT_CHAR_BUDGET = 60000
# All grep excerpts in one call: one match has no natural size limit.
GREP_OUTPUT_CHAR_BUDGET = _env_limit(
    "GREP_OUTPUT_CHAR_BUDGET", MCP_CLIENT_OUTPUT_CHAR_BUDGET
)
TOOL_RESPONSE_TOKEN_LIMIT = _env_limit("TOOL_RESPONSE_TOKEN_LIMIT", 80000)
# Schema fields per entity: about five entities share TOOL_RESPONSE_TOKEN_LIMIT.
ENTITY_SCHEMA_TOKEN_BUDGET = _env_limit("ENTITY_SCHEMA_TOKEN_BUDGET", 16000)


def truncate_with_ellipsis(
    text: str,
    max_length: int,
    suffix: str = "...",
    *,
    word_boundary: bool = False,
    dropped_count: bool = False,
) -> str:
    """Cut ``text`` to ``max_length`` characters, marking the cut.

    With ``word_boundary`` the cut moves back to the last space, unless that space
    is early in the budget (a long unbroken token is hard-cut instead).

    With ``dropped_count`` the marker says how much was cut ("... [+403 chars]")
    and goes after the ``max_length`` characters kept, so a reader knows the value
    is partial. Otherwise the suffix counts toward ``max_length``; a limit
    shorter than the suffix gets a plain cut.
    """
    if not text or len(text) <= max_length:
        return text

    if dropped_count:
        cut = text[:max_length]
        if word_boundary:
            space = cut.rfind(" ")
            if space >= max_length * 0.6:
                cut = cut[:space]
        cut = cut.rstrip()
        return f"{cut}… [+{len(text) - len(cut)} chars]"

    budget = max_length - len(suffix)
    if budget <= 0:
        return text[:max_length]
    cut = text[:budget]
    if word_boundary:
        space = cut.rfind(" ")
        if space >= budget * 0.6:
            cut = cut[:space].rstrip()
    return cut + suffix


def parse_filter_input(filter_str: Optional[str]) -> Optional[Filter]:
    """A tool's SQL-like ``filter`` argument as a Filter; None when it is blank."""
    stripped = (filter_str or "").strip()
    return parse_filter_string(stripped) if stripped else None


def parenthesized_filter(filter_str: Optional[str]) -> Optional[str]:
    """Wrap a caller's filter so clauses can be ANDed onto all of it.

    Returns None for None or blank input. A filter such as ``a) OR (b`` parses only
    once wrapped, and the added clauses would then constrain only its last branch,
    so the filter must parse on its own first.
    """
    stripped = (filter_str or "").strip()
    if not stripped:
        return None
    parse_filter_string(stripped)
    return f"({stripped})"


_BASE64_IMAGE = re.compile(r"data:image/[^;]+;base64,[A-Za-z0-9+/=]+")
_BASE64_MARKDOWN_IMAGE = re.compile(r"!\[[^\]]*\]\(data:image/[^)]+\)")


def clean_gql_response(response: Any) -> Any:
    """A GraphQL response without the noise a model doesn't need.

    Recursively drops ``__typename``, None values, and lists and dicts that are
    empty (after cleaning), and replaces base64-encoded images in descriptions,
    which can run to megabytes, with ``[image removed]``.
    """
    if isinstance(response, dict):
        cleaned_response = {}
        for k, v in response.items():
            if k == "__typename" or v is None or v == []:
                continue
            cleaned_v = clean_gql_response(v)
            if (
                k == "description"
                and isinstance(cleaned_v, str)
                and "base64" in cleaned_v
            ):
                cleaned_v = _BASE64_IMAGE.sub("[image removed]", cleaned_v)
                cleaned_v = _BASE64_MARKDOWN_IMAGE.sub("[image removed]", cleaned_v)
            if cleaned_v is not None and cleaned_v != {}:
                cleaned_response[k] = cleaned_v
        return cleaned_response
    elif isinstance(response, list):
        return [clean_gql_response(item) for item in response]
    return response


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
    format_exception: bool = True,
) -> Any:
    """Run a GraphQL operation against the caller's DataHub.

    A query with ``#[since]`` markers is first compiled for the GMS the host put
    in the request's tool context (see gql_compiler). Without one, nothing is
    known about GMS, so every selection is kept and stripped at runtime.

    strip_unsupported_fields delegates schema compatibility to the SDK's
    introspection-based projection: fields the server lacks are removed before
    sending, and the caller treats them as absent. A schema error on a known
    GMS version is raised, never retried with fewer fields.

    Failures are raised as ToolError, whose message reaches the model, unless
    ``format_exception`` is False: then the SDK's GraphError is raised with GMS's
    raw errors list as ``args[0]``, for callers that act on error types.
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
            format_exception=format_exception,
        )
    except Exception as e:
        # Variables can carry user data, so they stay out of the log.
        logger.warning(f"GraphQL {operation_name or 'query'} failed: {e}")
        if not format_exception or isinstance(e, ToolError):
            raise
        raise ToolError(describe_datahub_error(e)) from e


def http_status(error: BaseException) -> Optional[int]:
    """The HTTP status behind an SDK error, if any (the SDK chains requests' HTTPError)."""
    seen: set[int] = set()
    cause: Optional[BaseException] = error
    while cause is not None and id(cause) not in seen:
        seen.add(id(cause))
        status = getattr(getattr(cause, "response", None), "status_code", None)
        if isinstance(status, int):
            return status
        cause = cause.__cause__ or cause.__context__
    return None


def describe_datahub_error(error: Exception) -> str:
    """A message about a failed DataHub call that a model can act on."""
    status = http_status(error)
    if status in (401, 403):
        return (
            f"DataHub refused the request (HTTP {status}): the token is invalid, "
            "expired, or lacks the privileges this needs."
        )
    if isinstance(error, requests.RequestException) or (
        status is not None and status >= 500
    ):
        return f"DataHub could not be reached or failed ({status or type(error).__name__}). Try again shortly."
    if isinstance(error, GraphError):
        return str(error)
    return f"DataHub request failed: {error}"
