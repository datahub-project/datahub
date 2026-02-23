"""Tests for query tools."""

from unittest.mock import Mock

import pytest

from datahub_agent_context.context import DataHubContext
from datahub_agent_context.mcp_tools.queries import get_dataset_queries


@pytest.fixture
def mock_client():
    """Create a mock DataHubClient."""
    mock = Mock()
    mock._graph = Mock()
    mock.graph.execute_graphql = Mock()
    return mock


@pytest.fixture
def mock_gql_response():
    """Sample GraphQL response for listQueries."""
    return {
        "listQueries": {
            "start": 0,
            "count": 2,
            "total": 2,
            "queries": [
                {
                    "urn": "urn:li:query:test1",
                    "properties": {
                        "name": "Monthly Orders",
                        "source": "MANUAL",
                        "statement": {
                            "value": "SELECT * FROM orders WHERE created_at > '2024-01-01'",
                            "language": "SQL",
                        },
                    },
                    "platform": {"name": "snowflake"},
                    "subjects": [
                        {"dataset": {"urn": "urn:li:dataset:(...)"}},
                    ],
                },
                {
                    "urn": "urn:li:query:test2",
                    "properties": {
                        "name": "Dashboard Query",
                        "source": "SYSTEM",
                        "statement": {
                            "value": "SELECT COUNT(*) FROM orders GROUP BY month",
                            "language": "SQL",
                        },
                    },
                    "platform": {"name": "snowflake"},
                    "subjects": [
                        {"dataset": {"urn": "urn:li:dataset:(...)"}},
                    ],
                },
            ],
        }
    }


class TestGetDatasetQueriesParameters:
    """Tests for get_dataset_queries parameter handling."""

    def test_get_dataset_queries_with_source_manual(
        self, mock_client, mock_gql_response
    ):
        """Test get_dataset_queries with source='MANUAL' filter."""
        mock_client._graph.execute_graphql.return_value = mock_gql_response

        with DataHubContext(mock_client):
            result = get_dataset_queries(
                urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.orders,PROD)",
                source="MANUAL",
                count=10,
            )

        # Verify GraphQL was called with source filter
        call_args = mock_client._graph.execute_graphql.call_args
        variables = call_args.kwargs["variables"]
        assert variables["input"]["source"] == "MANUAL"
        assert "orFilters" in variables["input"]
        assert result is not None

    def test_get_dataset_queries_with_source_system(
        self, mock_client, mock_gql_response
    ):
        """Test get_dataset_queries with source='SYSTEM' filter."""
        mock_client._graph.execute_graphql.return_value = mock_gql_response

        with DataHubContext(mock_client):
            result = get_dataset_queries(
                urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.orders,PROD)",
                source="SYSTEM",
                count=10,
            )

        # Verify GraphQL was called with source filter
        call_args = mock_client._graph.execute_graphql.call_args
        variables = call_args.kwargs["variables"]
        assert variables["input"]["source"] == "SYSTEM"
        assert result is not None

    def test_get_dataset_queries_no_source_filter(self, mock_client, mock_gql_response):
        """Test get_dataset_queries without source filter (both types)."""
        mock_client._graph.execute_graphql.return_value = mock_gql_response

        with DataHubContext(mock_client):
            result = get_dataset_queries(
                urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.orders,PROD)",
                count=10,
            )

        # Verify GraphQL was called without source filter
        call_args = mock_client._graph.execute_graphql.call_args
        variables = call_args.kwargs["variables"]
        assert "source" not in variables["input"]
        assert result is not None

    def test_get_dataset_queries_with_column(self, mock_client, mock_gql_response):
        """Test get_dataset_queries with column parameter."""
        mock_client._graph.execute_graphql.return_value = mock_gql_response

        with DataHubContext(mock_client):
            result = get_dataset_queries(
                urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.orders,PROD)",
                column="created_at",
                count=5,
            )

        # Should convert URN to schema field URN and query for it
        assert result is not None
        assert mock_client._graph.execute_graphql.called

    def test_get_dataset_queries_pagination(self, mock_client, mock_gql_response):
        """Test pagination with start and count parameters."""
        mock_client._graph.execute_graphql.return_value = mock_gql_response

        with DataHubContext(mock_client):
            result = get_dataset_queries(
                urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.orders,PROD)",
                start=10,
                count=20,
            )

        call_args = mock_client._graph.execute_graphql.call_args
        variables = call_args.kwargs["variables"]
        assert variables["input"]["start"] == 10
        assert variables["input"]["count"] == 20
        assert result is not None


class TestGetDatasetQueriesResponse:
    """Tests for response processing."""

    def test_subjects_deduplicated(self, mock_client):
        """Test that subjects are deduplicated to dataset URNs."""
        mock_client._graph.execute_graphql.return_value = {
            "listQueries": {
                "start": 0,
                "count": 1,
                "total": 1,
                "queries": [
                    {
                        "urn": "urn:li:query:test1",
                        "properties": {
                            "statement": {
                                "value": "SELECT * FROM table",
                                "language": "SQL",
                            }
                        },
                        "subjects": [
                            {"dataset": {"urn": "urn:li:dataset:1"}},
                            {"dataset": {"urn": "urn:li:dataset:1"}},  # Duplicate
                            {"dataset": {"urn": "urn:li:dataset:2"}},
                        ],
                    }
                ],
            }
        }

        with DataHubContext(mock_client):
            result = get_dataset_queries(
                urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.orders,PROD)",
            )

        # Subjects should be deduplicated
        subjects = result["queries"][0]["subjects"]
        assert len(subjects) == 2
        assert "urn:li:dataset:1" in subjects
        assert "urn:li:dataset:2" in subjects

    def test_long_queries_truncated(self, mock_client):
        """Test that long SQL queries are truncated."""
        long_query = "SELECT * FROM table WHERE " + "x = 1 AND " * 1000  # Very long

        mock_client._graph.execute_graphql.return_value = {
            "listQueries": {
                "start": 0,
                "count": 1,
                "total": 1,
                "queries": [
                    {
                        "urn": "urn:li:query:test1",
                        "properties": {
                            "statement": {"value": long_query, "language": "SQL"}
                        },
                        "subjects": [],
                    }
                ],
            }
        }

        with DataHubContext(mock_client):
            result = get_dataset_queries(
                urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.orders,PROD)",
            )

        # Query should be truncated
        returned_query = result["queries"][0]["properties"]["statement"]["value"]
        assert len(returned_query) < len(long_query)
        assert "truncated" in returned_query

    def test_response_cleaned(self, mock_client):
        """Test that response is cleaned (no __typename)."""
        mock_client._graph.execute_graphql.return_value = {
            "listQueries": {
                "__typename": "QueryResults",
                "start": 0,
                "count": 1,
                "total": 1,
                "queries": [
                    {
                        "__typename": "Query",
                        "urn": "urn:li:query:test1",
                        "properties": {
                            "__typename": "QueryProperties",
                            "statement": {"value": "SELECT 1", "language": "SQL"},
                        },
                    }
                ],
            }
        }

        with DataHubContext(mock_client):
            result = get_dataset_queries(
                urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.orders,PROD)",
            )

        # Verify __typename is removed
        assert "__typename" not in result
        assert "__typename" not in result["queries"][0]
        assert "__typename" not in result["queries"][0]["properties"]


class TestGetDatasetQueriesEdgeCases:
    """Tests for edge cases."""

    def test_empty_queries_list(self, mock_client):
        """Test handling of empty queries list."""
        mock_client._graph.execute_graphql.return_value = {
            "listQueries": {
                "start": 0,
                "count": 0,
                "total": 0,
                "queries": [],
            }
        }

        with DataHubContext(mock_client):
            result = get_dataset_queries(
                urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.orders,PROD)",
            )

        assert result["total"] == 0
        # Empty arrays are removed by clean_gql_response, so queries key won't exist
        assert "queries" not in result

    def test_queries_without_subjects(self, mock_client):
        """Test handling of queries without subjects."""
        mock_client._graph.execute_graphql.return_value = {
            "listQueries": {
                "start": 0,
                "count": 1,
                "total": 1,
                "queries": [
                    {
                        "urn": "urn:li:query:test1",
                        "properties": {
                            "statement": {"value": "SELECT 1", "language": "SQL"}
                        },
                        # No subjects field
                    }
                ],
            }
        }

        with DataHubContext(mock_client):
            result = get_dataset_queries(
                urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.orders,PROD)",
            )

        # Should not fail, subjects just won't be in result
        assert "queries" in result
        assert len(result["queries"]) == 1
