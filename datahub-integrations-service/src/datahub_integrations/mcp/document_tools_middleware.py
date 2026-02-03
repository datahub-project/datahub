"""
Middleware and utilities to conditionally hide document tools based on catalog contents.

This module provides:
1. A middleware that intercepts MCP `list_tools` requests and filters out
   document-related tools when no documents exist in the catalog
2. A helper function `filter_document_tools` that can be used to filter tools
   in non-MCP contexts (e.g., agent tool composition)

Why this is needed:
- Document tools are only useful when the catalog has documents
- Showing unavailable tools creates confusion for users
- This provides a cleaner experience by hiding irrelevant tools

How it works:
1. When tools are listed, check if any documents exist in the catalog (with caching)
2. If no documents exist, filter out document-related tools
3. The result is cached for 1 minute to avoid repeated queries

Environment Variables:
- DATAHUB_MCP_DOCUMENT_TOOLS_DISABLED: Set to "true" to completely disable document
  tools (search_documents, grep_documents). Useful if document tools negatively
  impact chatbot behavior. Default: false (document tools enabled).

Usage (MCP middleware):
    from .document_tools_middleware import DocumentToolsMiddleware
    mcp_instance.add_middleware(DocumentToolsMiddleware())

Usage (tool filtering):
    from .document_tools_middleware import filter_document_tools
    filtered_tools = filter_document_tools(tools)
"""

import os
from typing import Any, Sequence, TypeVar

import cachetools
from fastmcp.server.middleware import CallNext, Middleware, MiddlewareContext
from loguru import logger

# Names of tools that should be hidden when no documents exist in the catalog
DOCUMENT_TOOL_NAMES = frozenset({"search_documents", "grep_documents"})

# Environment variable to completely disable document tools
DATAHUB_MCP_DOCUMENT_TOOLS_DISABLED_ENV_VAR = "DATAHUB_MCP_DOCUMENT_TOOLS_DISABLED"


def _are_document_tools_disabled() -> bool:
    """Check if document tools are disabled via environment variable."""
    return (
        os.environ.get(DATAHUB_MCP_DOCUMENT_TOOLS_DISABLED_ENV_VAR, "").lower()
        == "true"
    )


# How long to cache the "documents exist" check (in seconds)
# This prevents querying DataHub on every list_tools request
DOCUMENT_CHECK_CACHE_TTL_SECONDS = 60  # 1 minute


# NOTE: If document visibility ever becomes user-specific (e.g., based on permissions),
# we'll need to add the current user ID as a parameter to this function so that
# caching works properly per user instead of globally.
@cachetools.cached(
    cache=cachetools.TTLCache(maxsize=1, ttl=DOCUMENT_CHECK_CACHE_TTL_SECONDS)
)
def _query_documents_exist_cached() -> bool:
    """
    Query DataHub to check if any documents exist (cached).

    This function is decorated with @cachetools.cached to automatically
    cache the result for DOCUMENT_CHECK_CACHE_TTL_SECONDS. The TTLCache
    handles automatic expiration.

    Uses a lightweight search query with count=1 to get just the
    total count without fetching actual document data.

    Requires that a DataHub client is set via set_datahub_client() before
    calling this function.

    Returns:
        True if at least one document exists, False otherwise.

    Raises:
        LookupError: If no DataHub client is set in the context
        Exception: If the GraphQL query fails
    """
    # Import here to avoid circular imports at module load time
    from .mcp_server import execute_graphql, get_datahub_client
    from .tools.documents import document_search_gql

    logger.debug("Document check cache miss, querying DataHub")

    client = get_datahub_client()

    # Execute a minimal search query to get total document count
    # We use count=1 (minimum valid value) and only care about the 'total' field
    response = execute_graphql(
        client._graph,
        query=document_search_gql,
        variables={
            "query": "*",
            "orFilters": [],
            "count": 1,
            "start": 0,
            "viewUrn": None,
        },
        operation_name="documentSearch",
    )

    # Extract total count from response
    # Response structure: {"searchAcrossEntities": {"total": N, ...}}
    search_result = response.get("searchAcrossEntities", {})
    total = search_result.get("total", 0)

    logger.debug(f"Document count query returned total={total}")

    has_documents = total > 0
    logger.info(
        f"Document check result: {has_documents} (cached for {DOCUMENT_CHECK_CACHE_TTL_SECONDS}s)"
    )
    return has_documents


# TypeVar for generic tool filtering - works with any tool type that has a 'name' attribute
T = TypeVar("T")


def filter_document_tools(tools: Sequence[T]) -> list[T]:
    """
    Filter out document tools based on environment variable or catalog contents.

    Document tools are filtered out if:
    1. DOCUMENT_TOOLS_DISABLED env var is set to "true" (always filter)
    2. No documents exist in the catalog (dynamic check)

    This helper function can be used to filter tools in any context, not just
    MCP middleware. It uses the same cached document existence check.

    Each tool must have a 'name' attribute that will be checked against
    DOCUMENT_TOOL_NAMES.

    On error (e.g., DataHub unavailable), fails open by returning all tools
    unchanged - this is safer than incorrectly hiding tools.

    Args:
        tools: Sequence of tool objects with a 'name' attribute

    Returns:
        Filtered list of tools, with document tools removed if disabled or no documents exist.
        Returns all tools unchanged if documents exist or on error.

    Example:
        >>> from .document_tools_middleware import filter_document_tools
        >>> filtered = filter_document_tools(mcp._tool_manager._tools.values())
    """
    # Check if document tools are disabled via environment variable
    if _are_document_tools_disabled():
        logger.info(
            f"Document tools disabled via {DATAHUB_MCP_DOCUMENT_TOOLS_DISABLED_ENV_VAR}, "
            f"filtering out tools: {DOCUMENT_TOOL_NAMES}"
        )
        return [
            tool
            for tool in tools
            if getattr(tool, "name", None) not in DOCUMENT_TOOL_NAMES
        ]

    # Check if documents exist in the catalog
    try:
        has_documents = _query_documents_exist_cached()
    except Exception as e:
        # Treat errors as "no documents" - most likely the environment doesn't support
        # the Document entity type (e.g., "Unknown type 'Document'" GraphQL error)
        logger.info(
            f"Failed to check if documents exist (treating as no documents), "
            f"filtering out tools: {DOCUMENT_TOOL_NAMES}. Error: {e}"
        )
        has_documents = False

    if has_documents:
        logger.debug("Documents exist in catalog, returning all tools")
        return list(tools)

    # No documents - filter out document tools
    logger.info(f"No documents in catalog, filtering out tools: {DOCUMENT_TOOL_NAMES}")
    return [
        tool for tool in tools if getattr(tool, "name", None) not in DOCUMENT_TOOL_NAMES
    ]


class DocumentToolsMiddleware(Middleware):
    """
    FastMCP middleware that hides document tools when no documents exist.

    This middleware hooks into the `on_list_tools` lifecycle event to filter
    out document-related tools when the DataHub catalog has no Document entities.

    The check for document existence is cached for DOCUMENT_CHECK_CACHE_TTL_SECONDS
    to avoid making a GraphQL query on every tool listing request.

    Example:
        >>> middleware = DocumentToolsMiddleware()
        >>> mcp.add_middleware(middleware)
    """

    async def on_list_tools(
        self,
        context: MiddlewareContext,
        call_next: CallNext,
    ) -> Any:
        """
        Intercept the list_tools request and filter out document tools if needed.

        This method is called by FastMCP whenever a client requests the list
        of available tools. We delegate to filter_document_tools to do the
        actual filtering.

        Args:
            context: The middleware context containing request information
            call_next: Function to call the next middleware or handler

        Returns:
            The list of tools, with document tools filtered out if no documents exist
        """
        # First, get the full list of tools from the next handler
        tools = await call_next(context)

        # Filter document tools using the shared helper function
        return filter_document_tools(tools)
