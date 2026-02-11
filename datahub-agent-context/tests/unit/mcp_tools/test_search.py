"""Tests for search tool."""

from unittest.mock import Mock

import pytest

from datahub_agent_context.context import DataHubContext
from datahub_agent_context.mcp_tools.search import search


@pytest.fixture
def mock_client():
    """Create a mock DataHubClient."""
    mock = Mock()
    mock._graph = Mock()
    mock.graph.execute_graphql = Mock()
    mock.graph.frontend_base_url = "http://localhost:9002"  # For cloud detection
    return mock


@pytest.fixture
def mock_search_response():
    """Sample search response."""
    return {
        "searchAcrossEntities": {
            "start": 0,
            "count": 2,
            "total": 10,
            "searchResults": [
                {
                    "entity": {
                        "urn": "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.users,PROD)",
                        "properties": {"name": "users"},
                    }
                },
                {
                    "entity": {
                        "urn": "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.customers,PROD)",
                        "properties": {"name": "customers"},
                    }
                },
            ],
            "facets": [
                {
                    "field": "platform",
                    "displayName": "Platform",
                    "aggregations": [
                        {"value": "snowflake", "count": 10, "displayName": "Snowflake"}
                    ],
                }
            ],
        }
    }


def test_basic_search(mock_client, mock_search_response):
    """Test basic search with query."""
    mock_client._graph.execute_graphql.return_value = mock_search_response

    with DataHubContext(mock_client):
        result = search(query="users")

    assert "total" in result
    assert result["total"] == 10
    assert "searchResults" in result
    assert len(result["searchResults"]) == 2


def test_search_with_filters(mock_client, mock_search_response):
    """Test search with entity type filter."""
    mock_client._graph.execute_graphql.return_value = mock_search_response

    with DataHubContext(mock_client):
        result = search(query="*", filters={"entity_type": ["dataset"]})

    assert result is not None
    # Note: execute_graphql is called twice - once for fetch_global_default_view, once for search
    assert mock_client._graph.execute_graphql.called


def test_search_with_pagination(mock_client, mock_search_response):
    """Test search pagination parameters."""
    mock_client._graph.execute_graphql.return_value = mock_search_response

    with DataHubContext(mock_client):
        search(query="*", num_results=20, offset=10)

    call_args = mock_client._graph.execute_graphql.call_args
    variables = call_args.kwargs["variables"]
    assert variables["count"] == 20
    assert variables["start"] == 10


def test_search_with_sorting(mock_client, mock_search_response):
    """Test search with sorting."""
    mock_client._graph.execute_graphql.return_value = mock_search_response

    with DataHubContext(mock_client):
        search(query="*", sort_by="lastModified", sort_order="desc")

    call_args = mock_client._graph.execute_graphql.call_args
    variables = call_args.kwargs["variables"]
    assert "sortInput" in variables
    assert variables["sortInput"]["sortCriteria"][0]["field"] == "lastModified"
    assert variables["sortInput"]["sortCriteria"][0]["sortOrder"] == "DESCENDING"


def test_search_num_results_capped_at_50(mock_client, mock_search_response):
    """Test that num_results is capped at 50."""
    mock_client._graph.execute_graphql.return_value = mock_search_response

    with DataHubContext(mock_client):
        search(query="*", num_results=100)

    call_args = mock_client._graph.execute_graphql.call_args
    variables = call_args.kwargs["variables"]
    assert variables["count"] == 50


def test_search_facet_only_query(mock_client, mock_search_response):
    """Test facet-only query with num_results=0."""
    mock_client._graph.execute_graphql.return_value = mock_search_response

    with DataHubContext(mock_client):
        result = search(query="*", num_results=0)

    # Verify searchResults is removed for facet-only queries
    assert "searchResults" not in result
    assert "count" not in result
    assert "facets" in result


def test_search_with_complex_filters(mock_client, mock_search_response):
    """Test search with complex AND/OR filters."""
    mock_client._graph.execute_graphql.return_value = mock_search_response

    filters = {
        "and": [
            {"entity_type": ["dataset"]},
            {"platform": ["snowflake"]},
        ]
    }
    with DataHubContext(mock_client):
        result = search(query="*", filters=filters)

    assert result is not None
    # Note: execute_graphql is called twice - once for fetch_global_default_view, once for search
    assert mock_client._graph.execute_graphql.called


def test_search_with_string_filters(mock_client, mock_search_response):
    """Test search with JSON string filters."""
    import json

    mock_client._graph.execute_graphql.return_value = mock_search_response

    filters_str = json.dumps({"entity_type": ["dataset"]})
    with DataHubContext(mock_client):
        result = search(query="*", filters=filters_str)

    assert result is not None
    # Note: execute_graphql is called twice - once for fetch_global_default_view, once for search
    assert mock_client._graph.execute_graphql.called


def test_search_response_cleaned(mock_client):
    """Test that response is cleaned (no __typename)."""
    mock_client._graph.execute_graphql.return_value = {
        "searchAcrossEntities": {
            "__typename": "SearchResults",
            "total": 1,
            "searchResults": [
                {
                    "__typename": "SearchResult",
                    "entity": {
                        "__typename": "Dataset",
                        "urn": "urn:li:dataset:test",
                    },
                }
            ],
        }
    }

    with DataHubContext(mock_client):
        result = search(query="*")

    # Verify __typename is removed
    assert "__typename" not in result
    assert "__typename" not in result["searchResults"][0]
    assert "__typename" not in result["searchResults"][0]["entity"]
