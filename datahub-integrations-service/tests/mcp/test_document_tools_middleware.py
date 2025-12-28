"""
Tests for DocumentToolsMiddleware.

This module tests the middleware that conditionally hides document tools
(search_documents, grep_documents) when no documents exist in the catalog.

Test scenarios:
1. Documents exist -> all tools visible
2. No documents -> document tools hidden
3. Query error -> treat as no documents (hide tools)
4. Cache behavior -> cachetools TTLCache
"""

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from datahub_integrations.mcp.document_tools_middleware import (
    DOCUMENT_CHECK_CACHE_TTL_SECONDS,
    DOCUMENT_TOOL_NAMES,
    DocumentToolsMiddleware,
    _query_documents_exist_cached,
)


class TestDocumentToolsMiddleware:
    """Tests for the DocumentToolsMiddleware class."""

    # =========================================================================
    # Fixtures
    # =========================================================================

    @pytest.fixture
    def middleware(self):
        """Create a fresh middleware instance for each test."""
        return DocumentToolsMiddleware()

    @pytest.fixture
    def mock_tools(self):
        """
        Create a mock list of tools that includes document tools.

        Returns a list of SimpleNamespace objects with 'name' attributes,
        simulating the tool objects returned by FastMCP.
        """
        return [
            SimpleNamespace(name="search"),
            SimpleNamespace(name="get_entities"),
            SimpleNamespace(name="search_documents"),
            SimpleNamespace(name="grep_documents"),
            SimpleNamespace(name="get_lineage"),
        ]

    @pytest.fixture
    def mock_context(self):
        """Create a mock middleware context."""
        return MagicMock()

    @pytest.fixture(autouse=True)
    def clear_cache(self):
        """Clear the TTLCache before each test to ensure isolation."""
        # The cache is accessed via the wrapper's cache attribute
        _query_documents_exist_cached.cache.clear()
        yield
        _query_documents_exist_cached.cache.clear()

    # =========================================================================
    # Test: Tools shown when documents exist
    # =========================================================================

    @pytest.mark.asyncio
    @patch(
        "datahub_integrations.mcp.document_tools_middleware._query_documents_exist_cached"
    )
    async def test_all_tools_visible_when_documents_exist(
        self, mock_query, middleware, mock_tools, mock_context
    ):
        """
        When documents exist in the catalog, all tools should be visible.

        The middleware should pass through all tools unchanged when the
        document count query returns a positive result.
        """
        # Arrange: Documents exist
        mock_query.return_value = True
        mock_call_next = AsyncMock(return_value=mock_tools)

        # Act: Call the middleware
        result = await middleware.on_list_tools(mock_context, mock_call_next)

        # Assert: All tools are returned
        assert len(result) == 5
        tool_names = {tool.name for tool in result}
        assert "search_documents" in tool_names
        assert "grep_documents" in tool_names

    # =========================================================================
    # Test: Document tools hidden when no documents exist
    # =========================================================================

    @pytest.mark.asyncio
    @patch(
        "datahub_integrations.mcp.document_tools_middleware._query_documents_exist_cached"
    )
    async def test_document_tools_hidden_when_no_documents(
        self, mock_query, middleware, mock_tools, mock_context
    ):
        """
        When no documents exist in the catalog, document tools should be hidden.

        The middleware should filter out search_documents and grep_documents
        when the document count query returns zero.
        """
        # Arrange: No documents exist
        mock_query.return_value = False
        mock_call_next = AsyncMock(return_value=mock_tools)

        # Act: Call the middleware
        result = await middleware.on_list_tools(mock_context, mock_call_next)

        # Assert: Document tools are filtered out
        assert len(result) == 3  # 5 - 2 document tools
        tool_names = {tool.name for tool in result}
        assert "search_documents" not in tool_names
        assert "grep_documents" not in tool_names
        # Other tools should still be present
        assert "search" in tool_names
        assert "get_entities" in tool_names
        assert "get_lineage" in tool_names

    # =========================================================================
    # Test: Error handling - treat as no documents
    # =========================================================================

    @pytest.mark.asyncio
    @patch(
        "datahub_integrations.mcp.document_tools_middleware._query_documents_exist_cached"
    )
    async def test_error_treated_as_no_documents(
        self, mock_query, middleware, mock_tools, mock_context
    ):
        """
        When the document query fails, treat it as "no documents exist".

        This is appropriate because errors typically occur when the environment
        doesn't support the Document entity type (e.g., "Unknown type 'Document'"
        GraphQL error). In such cases, document tools should be hidden.
        """
        # Arrange: Query throws an exception (e.g., unknown type error)
        mock_query.side_effect = Exception("Unknown type 'Document'")
        mock_call_next = AsyncMock(return_value=mock_tools)

        # Act: Call the middleware
        result = await middleware.on_list_tools(mock_context, mock_call_next)

        # Assert: Document tools are filtered out (same as no documents)
        assert len(result) == 3  # 5 - 2 document tools
        tool_names = {tool.name for tool in result}
        assert "search_documents" not in tool_names
        assert "grep_documents" not in tool_names
        # Other tools should still be present
        assert "search" in tool_names
        assert "get_entities" in tool_names
        assert "get_lineage" in tool_names

    # =========================================================================
    # Test: Cache behavior (using cachetools TTLCache)
    # =========================================================================

    @pytest.mark.asyncio
    @patch("datahub_integrations.mcp.mcp_server.execute_graphql")
    @patch("datahub_integrations.mcp.mcp_server.get_datahub_client")
    async def test_cache_prevents_repeated_queries(
        self,
        mock_get_client,
        mock_execute_graphql,
        middleware,
        mock_tools,
        mock_context,
    ):
        """
        The middleware should cache the document check result.

        Multiple calls to on_list_tools within the cache TTL should only
        result in a single query to DataHub.

        Note: We patch mcp_server because that's where the imports come from
        (the middleware imports from mcp_server at function call time).
        """
        # Arrange: GraphQL returns documents exist
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client
        mock_execute_graphql.return_value = {
            "searchAcrossEntities": {"total": 5, "searchResults": []}
        }
        mock_call_next = AsyncMock(return_value=mock_tools)

        # Act: Call the middleware multiple times
        await middleware.on_list_tools(mock_context, mock_call_next)
        await middleware.on_list_tools(mock_context, mock_call_next)
        await middleware.on_list_tools(mock_context, mock_call_next)

        # Assert: GraphQL was only called once (subsequent calls used cache)
        assert mock_execute_graphql.call_count == 1

    @pytest.mark.asyncio
    @patch("datahub_integrations.mcp.mcp_server.execute_graphql")
    @patch("datahub_integrations.mcp.mcp_server.get_datahub_client")
    async def test_cache_expires_after_ttl(
        self,
        mock_get_client,
        mock_execute_graphql,
        middleware,
        mock_tools,
        mock_context,
    ):
        """
        The cache should expire after DOCUMENT_CHECK_CACHE_TTL_SECONDS.

        After the TTL expires, the next call should trigger a fresh query.

        Note: We test this by clearing the cache to simulate TTL expiry,
        since actually waiting for the TTL would make the test too slow.
        """
        # Arrange: GraphQL returns documents exist
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client
        mock_execute_graphql.return_value = {
            "searchAcrossEntities": {"total": 5, "searchResults": []}
        }
        mock_call_next = AsyncMock(return_value=mock_tools)

        # Act: First call populates cache
        await middleware.on_list_tools(mock_context, mock_call_next)
        assert mock_execute_graphql.call_count == 1

        # Simulate cache expiry by clearing the cache
        _query_documents_exist_cached.cache.clear()

        # Act: Second call should trigger a new query
        await middleware.on_list_tools(mock_context, mock_call_next)

        # Assert: Query was called again after cache expired
        assert mock_execute_graphql.call_count == 2

    # =========================================================================
    # Test: Query implementation
    # =========================================================================

    @patch("datahub_integrations.mcp.mcp_server.get_datahub_client")
    @patch("datahub_integrations.mcp.mcp_server.execute_graphql")
    def test_query_returns_true_when_documents_exist(
        self, mock_execute_graphql, mock_get_client
    ):
        """
        _query_documents_exist_cached should return True when total > 0.

        Note: We patch mcp_server because that's where the imports come from
        (the middleware imports from mcp_server at function call time).
        """
        # Arrange: GraphQL returns total=5
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client
        mock_execute_graphql.return_value = {
            "searchAcrossEntities": {"total": 5, "searchResults": []}
        }

        # Act
        result = _query_documents_exist_cached()

        # Assert
        assert result is True

    @patch("datahub_integrations.mcp.mcp_server.get_datahub_client")
    @patch("datahub_integrations.mcp.mcp_server.execute_graphql")
    def test_query_returns_false_when_no_documents(
        self, mock_execute_graphql, mock_get_client
    ):
        """
        _query_documents_exist_cached should return False when total = 0.

        Note: We patch mcp_server because that's where the imports come from
        (the middleware imports from mcp_server at function call time).
        """
        # Arrange: GraphQL returns total=0
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client
        mock_execute_graphql.return_value = {
            "searchAcrossEntities": {"total": 0, "searchResults": []}
        }

        # Act
        result = _query_documents_exist_cached()

        # Assert
        assert result is False

    # =========================================================================
    # Test: Constants are correctly defined
    # =========================================================================

    def test_document_tool_names_constant(self):
        """
        Verify the DOCUMENT_TOOL_NAMES constant contains expected tools.
        """
        assert "search_documents" in DOCUMENT_TOOL_NAMES
        assert "grep_documents" in DOCUMENT_TOOL_NAMES
        assert len(DOCUMENT_TOOL_NAMES) == 2

    def test_cache_ttl_is_reasonable(self):
        """
        Verify the cache TTL is a reasonable value (not too short or long).
        """
        # Should be at least 1 minute to avoid too many queries
        assert DOCUMENT_CHECK_CACHE_TTL_SECONDS >= 60
        # Should be at most 1 hour to ensure relatively fresh data
        assert DOCUMENT_CHECK_CACHE_TTL_SECONDS <= 3600
