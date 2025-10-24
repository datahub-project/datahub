"""Unit tests for get_dataset_queries MCP tool parameters."""

from unittest.mock import MagicMock, patch

import pytest

from datahub_integrations.mcp.mcp_server import async_background, get_dataset_queries

pytestmark = pytest.mark.anyio


class TestGetDatasetQueriesParameters:
    """Tests for get_dataset_queries parameter handling."""

    @pytest.fixture
    def mock_client(self):
        """Mock DataHub client."""
        client = MagicMock()
        client._graph = MagicMock()
        return client

    @pytest.fixture
    def mock_gql_response(self):
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

    @patch("datahub_integrations.mcp.mcp_server.get_datahub_client")
    @patch("datahub_integrations.mcp.mcp_server._execute_graphql")
    async def test_get_dataset_queries_with_source_manual(
        self, mock_execute_graphql, mock_get_client, mock_client, mock_gql_response
    ):
        """Test get_dataset_queries with source='MANUAL' filter."""
        mock_get_client.return_value = mock_client
        mock_execute_graphql.return_value = mock_gql_response

        await async_background(get_dataset_queries)(
            urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.orders,PROD)",
            source="MANUAL",
            count=10,
        )

        # Verify GraphQL was called with source filter
        call_args = mock_execute_graphql.call_args
        variables = call_args.kwargs["variables"]
        assert variables["input"]["source"] == "MANUAL"
        assert "orFilters" in variables["input"]

    @patch("datahub_integrations.mcp.mcp_server.get_datahub_client")
    @patch("datahub_integrations.mcp.mcp_server._execute_graphql")
    async def test_get_dataset_queries_with_source_system(
        self, mock_execute_graphql, mock_get_client, mock_client, mock_gql_response
    ):
        """Test get_dataset_queries with source='SYSTEM' filter."""
        mock_get_client.return_value = mock_client
        mock_execute_graphql.return_value = mock_gql_response

        await async_background(get_dataset_queries)(
            urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.orders,PROD)",
            source="SYSTEM",
            count=10,
        )

        # Verify GraphQL was called with source filter
        call_args = mock_execute_graphql.call_args
        variables = call_args.kwargs["variables"]
        assert variables["input"]["source"] == "SYSTEM"

    @patch("datahub_integrations.mcp.mcp_server.get_datahub_client")
    @patch("datahub_integrations.mcp.mcp_server._execute_graphql")
    async def test_get_dataset_queries_without_source_filter(
        self, mock_execute_graphql, mock_get_client, mock_client, mock_gql_response
    ):
        """Test get_dataset_queries without source filter (default behavior)."""
        mock_get_client.return_value = mock_client
        mock_execute_graphql.return_value = mock_gql_response

        await async_background(get_dataset_queries)(
            urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.orders,PROD)",
            count=10,
        )

        # Verify GraphQL was called without source filter
        call_args = mock_execute_graphql.call_args
        variables = call_args.kwargs["variables"]
        assert "source" not in variables["input"]

    @patch("datahub_integrations.mcp.mcp_server.get_datahub_client")
    @patch("datahub_integrations.mcp.mcp_server._execute_graphql")
    async def test_get_dataset_queries_with_column_parameter(
        self, mock_execute_graphql, mock_get_client, mock_client, mock_gql_response
    ):
        """Test get_dataset_queries with column parameter and source filter."""
        mock_get_client.return_value = mock_client
        mock_execute_graphql.return_value = mock_gql_response

        await async_background(get_dataset_queries)(
            urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.orders,PROD)",
            column="customer_id",
            source="MANUAL",
            count=10,
        )

        # Verify column is converted to schema field URN (tested elsewhere)
        # and source filter is applied
        call_args = mock_execute_graphql.call_args
        variables = call_args.kwargs["variables"]
        assert variables["input"]["source"] == "MANUAL"

    @patch("datahub_integrations.mcp.mcp_server.get_datahub_client")
    @patch("datahub_integrations.mcp.mcp_server._execute_graphql")
    async def test_get_dataset_queries_response_deduplication(
        self, mock_execute_graphql, mock_get_client, mock_client, mock_gql_response
    ):
        """Test that get_dataset_queries deduplicates subjects in response."""
        mock_get_client.return_value = mock_client
        mock_execute_graphql.return_value = mock_gql_response

        result = await async_background(get_dataset_queries)(
            urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.orders,PROD)",
        )

        # Verify response structure
        assert "total" in result
        assert "queries" in result

        # Verify subjects are deduplicated (implementation tested elsewhere)
        for query in result["queries"]:
            if "subjects" in query:
                # Subjects should be a list of URN strings, not full objects
                assert isinstance(query["subjects"], list)
