"""Tests for semantic search functionality in the MCP server."""

import json
from contextlib import contextmanager
from typing import Any, Iterator, Type, TypeVar
from unittest import mock

import pytest
from datahub.sdk.main_client import DataHubClient
from fastmcp import Client, FastMCP
from mcp.types import TextContent

from datahub_integrations.mcp.mcp_server import (
    _search_implementation,
    register_search_tools,
    search_gql,
    semantic_search_gql,
    with_datahub_client,
)

T = TypeVar("T")


def assert_type(expected_type: Type[T], obj: Any) -> T:
    """Assert that obj is of expected_type and return it properly typed."""
    assert isinstance(obj, expected_type), (
        f"Expected {expected_type.__name__}, got {type(obj).__name__}"
    )
    return obj


@contextmanager
def with_test_mcp_server(enabled: bool) -> Iterator[FastMCP]:
    """Create a test MCP server with desired semantic search configuration.

    This creates a completely isolated MCP instance for testing, avoiding
    any global state modification or complex cleanup logic.

    Args:
        enabled: Whether to mock semantic search as enabled or disabled

    Yields:
        FastMCP: A test MCP server instance with the desired configuration
    """

    # Create a completely separate MCP instance for testing
    test_mcp = FastMCP[None](name="test-datahub")

    with mock.patch(
        "datahub_integrations.mcp.mcp_server._is_semantic_search_enabled",
        return_value=enabled,
    ):
        # Register tools on our test instance using production logic
        register_search_tools(test_mcp)
        yield test_mcp
    # No cleanup needed - test instance just gets garbage collected


class TestSearchImplementation:
    """Test the core search implementation logic."""

    @mock.patch("datahub_integrations.mcp.mcp_server.get_datahub_client")
    @mock.patch("datahub_integrations.mcp.mcp_server._execute_graphql")
    def test_search_implementation_semantic_strategy(
        self, mock_execute_graphql: mock.Mock, mock_get_client: mock.Mock
    ) -> None:
        """Test that semantic strategy uses the correct GraphQL query and parameters."""
        # Setup mocks
        mock_graph = mock.Mock()
        mock_client = mock.Mock()
        mock_client._graph = mock_graph
        mock_get_client.return_value = mock_client

        mock_response = {
            "semanticSearchAcrossEntities": {
                "count": 5,
                "total": 100,
                "searchResults": [],
                "facets": [],
            }
        }
        mock_execute_graphql.return_value = mock_response

        # Call the function
        result = _search_implementation(
            query="customer data",
            filters=None,
            num_results=10,
            search_strategy="semantic",
        )

        # Verify correct GraphQL query was used
        # Note: _execute_graphql is called twice now - first for getGlobalViewsSettings, then for search
        assert mock_execute_graphql.call_count == 2

        # First call should be for getGlobalViewsSettings
        first_call = mock_execute_graphql.call_args_list[0]
        assert "getGlobalViewsSettings" in first_call[1]["query"]

        # Second call should be the semantic search
        second_call = mock_execute_graphql.call_args_list[1]
        assert second_call[0][0] == mock_graph  # First arg is the graph
        assert second_call[1]["query"] == semantic_search_gql  # Semantic GraphQL query
        assert second_call[1]["operation_name"] == "semanticSearch"

        # Verify variables for the search call
        variables = second_call[1]["variables"]
        assert variables["query"] == "customer data"
        assert variables["count"] == 10
        assert "scrollId" not in variables  # Semantic search doesn't use scrollId
        assert "viewUrn" in variables  # Should include viewUrn (even if None)

        # Verify response processing
        assert result["count"] == 5
        assert result["total"] == 100

    @mock.patch("datahub_integrations.mcp.mcp_server.get_datahub_client")
    @mock.patch("datahub_integrations.mcp.mcp_server._execute_graphql")
    def test_search_implementation_keyword_strategy(
        self, mock_execute_graphql: mock.Mock, mock_get_client: mock.Mock
    ) -> None:
        """Test that keyword strategy uses the correct GraphQL query and parameters."""
        # Setup mocks
        mock_graph = mock.Mock()
        mock_client = mock.Mock()
        mock_client._graph = mock_graph
        mock_get_client.return_value = mock_client

        mock_response = {
            "scrollAcrossEntities": {
                "count": 3,
                "total": 50,
                "searchResults": [],
                "facets": [],
            }
        }
        mock_execute_graphql.return_value = mock_response

        # Call the function
        _search_implementation(
            query="user_events", filters=None, num_results=5, search_strategy="keyword"
        )

        # Verify correct GraphQL query was used
        # Note: _execute_graphql is called twice now - first for getGlobalViewsSettings, then for search
        assert mock_execute_graphql.call_count == 2

        # First call should be for getGlobalViewsSettings
        first_call = mock_execute_graphql.call_args_list[0]
        assert "getGlobalViewsSettings" in first_call[1]["query"]

        # Second call should be the keyword search
        second_call = mock_execute_graphql.call_args_list[1]
        assert second_call[0][0] == mock_graph
        assert second_call[1]["query"] == search_gql  # Keyword GraphQL query
        assert second_call[1]["operation_name"] == "search"

        # Verify variables for the search call
        variables = second_call[1]["variables"]
        assert variables["query"] == "user_events"
        assert variables["count"] == 5
        assert variables["scrollId"] is None  # Keyword search includes scrollId
        assert "viewUrn" in variables  # Should include viewUrn (even if None)

    @mock.patch("datahub_integrations.mcp.mcp_server.get_datahub_client")
    @mock.patch("datahub_integrations.mcp.mcp_server._execute_graphql")
    def test_search_implementation_default_strategy(
        self, mock_execute_graphql: mock.Mock, mock_get_client: mock.Mock
    ) -> None:
        """Test that None/default strategy defaults to keyword search."""
        # Setup mocks
        mock_graph = mock.Mock()
        mock_client = mock.Mock()
        mock_client._graph = mock_graph
        mock_get_client.return_value = mock_client

        mock_response = {
            "scrollAcrossEntities": {
                "count": 1,
                "total": 10,
                "searchResults": [],
                "facets": [],
            }
        }
        mock_execute_graphql.return_value = mock_response

        # Call without search_strategy (should default to keyword)
        _search_implementation(
            query="test", filters=None, num_results=1, search_strategy=None
        )

        # Should use keyword search
        call_args = mock_execute_graphql.call_args
        assert call_args[1]["query"] == search_gql
        assert call_args[1]["operation_name"] == "search"

    @mock.patch("datahub_integrations.mcp.mcp_server.get_datahub_client")
    @mock.patch("datahub_integrations.mcp.mcp_server._execute_graphql")
    @mock.patch("datahub_integrations.mcp.mcp_server.load_filters")
    @mock.patch("datahub_integrations.mcp.mcp_server.compile_filters")
    def test_search_implementation_with_filters(
        self,
        mock_compile_filters: mock.Mock,
        mock_load_filters: mock.Mock,
        mock_execute_graphql: mock.Mock,
        mock_get_client: mock.Mock,
    ) -> None:
        """Test that filters are properly processed and passed through."""
        # Setup mocks
        mock_graph = mock.Mock()
        mock_client = mock.Mock()
        mock_client._graph = mock_graph
        mock_get_client.return_value = mock_client

        mock_response = {
            "semanticSearchAcrossEntities": {
                "count": 2,
                "total": 20,
                "searchResults": [],
                "facets": [],
            }
        }
        mock_execute_graphql.return_value = mock_response

        # Mock filter compilation
        mock_compile_filters.return_value = (["DATASET"], [{"platform": "snowflake"}])

        # Test with filter string (gets parsed)
        filters = '{"platform": ["snowflake"]}'

        _search_implementation(
            query="analytics",
            filters=filters,
            num_results=10,
            search_strategy="semantic",
        )

        # Verify filters were processed
        expected_dict = {"platform": ["snowflake"]}
        mock_load_filters.assert_called_once_with(expected_dict)
        mock_compile_filters.assert_called_once()

        call_args = mock_execute_graphql.call_args
        variables = call_args[1]["variables"]

        # Should have compiled filters
        assert "orFilters" in variables
        assert variables["query"] == "analytics"
        assert variables["types"] == ["DATASET"]
        assert variables["orFilters"] == [{"platform": "snowflake"}]

    @mock.patch("datahub_integrations.mcp.mcp_server.get_datahub_client")
    @mock.patch("datahub_integrations.mcp.mcp_server._execute_graphql")
    def test_search_implementation_num_results_zero_hack(
        self, mock_execute_graphql: mock.Mock, mock_get_client: mock.Mock
    ) -> None:
        """Test the num_results=0 hack works correctly."""
        # Setup mocks
        mock_graph = mock.Mock()
        mock_client = mock.Mock()
        mock_client._graph = mock_graph
        mock_get_client.return_value = mock_client

        mock_response = {
            "semanticSearchAcrossEntities": {
                "count": 5,
                "total": 100,
                "searchResults": [{"entity": {"urn": "test"}}],
                "facets": [
                    {"field": "platform", "displayName": "Platform", "aggregations": []}
                ],
            }
        }
        mock_execute_graphql.return_value = mock_response

        # Call with num_results=0
        result = _search_implementation(
            query="test", filters=None, num_results=0, search_strategy="semantic"
        )

        # Verify the hack: searchResults and count should be removed
        assert "searchResults" not in result
        assert "count" not in result
        assert "total" in result  # total should remain
        assert "facets" in result  # facets should remain (non-empty so not cleaned out)

    @mock.patch("datahub_integrations.mcp.mcp_server.get_datahub_client")
    @mock.patch("datahub_integrations.mcp.mcp_server._execute_graphql")
    def test_search_implementation_with_sorting(
        self, mock_execute_graphql: mock.Mock, mock_get_client: mock.Mock
    ) -> None:
        """Test that sorting parameters are correctly passed to GraphQL."""
        # Setup mocks
        mock_graph = mock.Mock()
        mock_client = mock.Mock()
        mock_client._graph = mock_graph
        mock_get_client.return_value = mock_client

        mock_response = {
            "scrollAcrossEntities": {
                "count": 5,
                "total": 100,
                "searchResults": [],
                "facets": [],
            }
        }
        mock_execute_graphql.return_value = mock_response

        # Call with sorting parameters
        _search_implementation(
            query="*",
            filters=None,
            num_results=10,
            search_strategy="keyword",
            sort_by="queryCountLast30DaysFeature",
            sort_order="desc",
        )

        # Verify correct sorting parameters in variables
        assert mock_execute_graphql.call_count == 2
        search_call = mock_execute_graphql.call_args_list[1]
        variables = search_call[1]["variables"]

        # Check sortInput is present and correct
        assert "sortInput" in variables
        assert variables["sortInput"] == {
            "sortCriteria": [
                {"field": "queryCountLast30DaysFeature", "sortOrder": "DESCENDING"}
            ]
        }

    @mock.patch("datahub_integrations.mcp.mcp_server.get_datahub_client")
    @mock.patch("datahub_integrations.mcp.mcp_server._execute_graphql")
    def test_search_implementation_with_ascending_sort(
        self, mock_execute_graphql: mock.Mock, mock_get_client: mock.Mock
    ) -> None:
        """Test that ascending sort order is correctly mapped."""
        # Setup mocks
        mock_graph = mock.Mock()
        mock_client = mock.Mock()
        mock_client._graph = mock_graph
        mock_get_client.return_value = mock_client

        mock_response = {
            "scrollAcrossEntities": {
                "count": 5,
                "total": 100,
                "searchResults": [],
                "facets": [],
            }
        }
        mock_execute_graphql.return_value = mock_response

        # Call with ascending sort order
        _search_implementation(
            query="*",
            filters=None,
            num_results=10,
            search_strategy="keyword",
            sort_by="sizeInBytesFeature",
            sort_order="asc",
        )

        # Verify ASCENDING is used
        search_call = mock_execute_graphql.call_args_list[1]
        variables = search_call[1]["variables"]

        assert variables["sortInput"] == {
            "sortCriteria": [{"field": "sizeInBytesFeature", "sortOrder": "ASCENDING"}]
        }

    @mock.patch("datahub_integrations.mcp.mcp_server.get_datahub_client")
    @mock.patch("datahub_integrations.mcp.mcp_server._execute_graphql")
    def test_search_implementation_without_sorting(
        self, mock_execute_graphql: mock.Mock, mock_get_client: mock.Mock
    ) -> None:
        """Test that sortInput is not included when sort_by is None."""
        # Setup mocks
        mock_graph = mock.Mock()
        mock_client = mock.Mock()
        mock_client._graph = mock_graph
        mock_get_client.return_value = mock_client

        mock_response = {
            "scrollAcrossEntities": {
                "count": 5,
                "total": 100,
                "searchResults": [],
                "facets": [],
            }
        }
        mock_execute_graphql.return_value = mock_response

        # Call without sorting parameters
        _search_implementation(
            query="*",
            filters=None,
            num_results=10,
            search_strategy="keyword",
            sort_by=None,
        )

        # Verify sortInput is NOT in variables (preserves default behavior)
        search_call = mock_execute_graphql.call_args_list[1]
        variables = search_call[1]["variables"]

        assert "sortInput" not in variables


@pytest.mark.anyio
async def test_tool_binding_basic_search() -> None:
    """Test that 'search' tool binding works correctly in basic search mode.

    This test creates an isolated MCP server with semantic search disabled
    to verify the registration logic correctly registers the basic search tool.
    """
    with with_test_mcp_server(enabled=False) as test_mcp:
        # Mock response for search implementation
        mock_search_response = {"count": 3, "total": 50, "searchResults": []}

        # Create mock with automatic call tracking
        mock_search_impl = mock.Mock(return_value=mock_search_response)

        # Set up mock DataHub client context
        mock_client = mock.Mock(spec=DataHubClient)
        with with_datahub_client(mock_client):
            async with Client(test_mcp) as mcp_client:
                tools = await mcp_client.list_tools()
                search_tools = [t for t in tools if t.name == "search"]

                # Verify exactly one search tool exists
                assert len(search_tools) == 1
                assert search_tools[0].name == "search"

                # Mock the search implementation function
                with mock.patch(
                    "datahub_integrations.mcp.mcp_server._search_implementation",
                    mock_search_impl,
                ):
                    # Verify tool works (basic keyword search functionality)
                    result = await mcp_client.call_tool(
                        "search", {"query": "*", "num_results": 3}
                    )
                    assert result.content, "Tool result should have content"
                    content = assert_type(TextContent, result.content[0])
                    res = json.loads(content.text)
                    assert isinstance(res, dict)
                    assert "count" in res
                    assert "total" in res


@pytest.mark.anyio
async def test_tool_binding_enhanced_search() -> None:
    """Test that 'search' tool binding works correctly in enhanced mode.

    This test creates an isolated MCP server with semantic search enabled
    to verify the registration logic correctly registers the enhanced search tool.
    It also verifies that the search_strategy parameter is correctly passed through
    to the _search_implementation function.
    """
    with with_test_mcp_server(enabled=True) as test_mcp:
        # Mock response for search implementation
        mock_search_response = {"count": 5, "total": 100, "searchResults": []}

        # Create mock with automatic call tracking
        mock_search_impl = mock.Mock(return_value=mock_search_response)

        # Test tool binding with isolated MCP server
        print("Testing tool binding with enhanced search enabled...")
        mock_client = mock.Mock(spec=DataHubClient)
        with with_datahub_client(mock_client):
            async with Client(test_mcp) as mcp_client:
                tools = await mcp_client.list_tools()
                search_tools = [t for t in tools if t.name == "search"]

                # Verify exactly one search tool exists
                assert len(search_tools) == 1
                assert search_tools[0].name == "search"

                # Mock the search implementation function
                with mock.patch(
                    "datahub_integrations.mcp.mcp_server._search_implementation",
                    mock_search_impl,
                ):
                    # Test keyword search strategy
                    print("Testing keyword search strategy parameter passing...")
                    result = await mcp_client.call_tool(
                        "search",
                        {"query": "*", "search_strategy": "keyword", "num_results": 3},
                    )
                    assert result.content, "Tool result should have content"
                    content = assert_type(TextContent, result.content[0])
                    res = json.loads(content.text)
                    assert isinstance(res, dict)
                    assert "count" in res
                    assert "total" in res

                    # Verify keyword search passed correct parameters to _search_implementation
                    calls = mock_search_impl.call_args_list
                    assert len(calls) == 1, (
                        "Should have made exactly one search implementation call"
                    )
                    keyword_call = calls[0]
                    assert keyword_call.args[0] == "*", (
                        "Query should be passed through correctly"
                    )
                    assert keyword_call.args[1] is None, (
                        "Filters should be passed through correctly"
                    )
                    assert keyword_call.args[2] == 3, (
                        "num_results should be passed through correctly"
                    )
                    assert keyword_call.args[3] == "keyword", (
                        "search_strategy should be 'keyword'"
                    )

                    mock_search_impl.reset_mock()  # Reset for semantic search test

                    # Test semantic search strategy
                    print("Testing semantic search strategy parameter passing...")
                    result = await mcp_client.call_tool(
                        "search",
                        {
                            "query": "customer data",
                            "search_strategy": "semantic",
                            "num_results": 5,
                        },
                    )
                    assert result.content, (
                        "Tool result should have content for semantic search"
                    )
                    content = assert_type(TextContent, result.content[0])
                    res = json.loads(content.text)
                    assert isinstance(res, dict)
                    assert "count" in res
                    assert "total" in res

                    # Verify semantic search passed correct parameters to _search_implementation
                    calls = mock_search_impl.call_args_list
                    assert len(calls) == 1, (
                        "Should have made exactly one search implementation call"
                    )
                    semantic_call = calls[0]
                    assert semantic_call.args[0] == "customer data", (
                        "Query should be passed through correctly"
                    )
                    assert semantic_call.args[1] is None, (
                        "Filters should be passed through correctly"
                    )
                    assert semantic_call.args[2] == 5, (
                        "num_results should be passed through correctly"
                    )
                    assert semantic_call.args[3] == "semantic", (
                        "search_strategy should be 'semantic'"
                    )

                    mock_search_impl.reset_mock()  # Reset for default strategy test

                    # Test default search strategy (should default to None, letting implementation decide)
                    print("Testing default search strategy parameter passing...")
                    result = await mcp_client.call_tool(
                        "search", {"query": "test", "num_results": 2}
                    )
                    assert result.content, (
                        "Tool result should have content for default search"
                    )

                    # Verify default search behavior
                    calls = mock_search_impl.call_args_list
                    assert len(calls) == 1, (
                        "Should have made exactly one search implementation call"
                    )
                    default_call = calls[0]
                    assert default_call.args[0] == "test", (
                        "Query should be passed through correctly"
                    )
                    assert default_call.args[1] is None, (
                        "Filters should be passed through correctly"
                    )
                    assert default_call.args[2] == 2, (
                        "num_results should be passed through correctly"
                    )
                    assert default_call.args[3] is None, (
                        "search_strategy should be None when not specified"
                    )

        print("Search strategy parameter verification completed successfully!")
        print("Isolated MCP server test completed successfully!")


if __name__ == "__main__":
    pytest.main([__file__])
