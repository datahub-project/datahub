"""Tests for get_entities function - batch entity retrieval.

Note: get_entities is wrapped with async_background in tests (same as production MCP registration).
"""

from unittest.mock import Mock, patch

import pytest
from datahub.errors import ItemNotFoundError

from datahub_integrations.mcp.mcp_server import async_background

pytestmark = pytest.mark.anyio


@pytest.fixture
def mock_client():
    """Create a mock DataHubClient."""
    client = Mock()
    client._graph = Mock()
    return client


@pytest.fixture
def sample_entity_response():
    """Sample entity response from GraphQL."""
    return {
        "urn": "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table,PROD)",
        "type": "DATASET",
        "name": "table",
        "description": "A sample table",
        "properties": {"created": "2024-01-01"},
    }


class TestGetEntitiesSingleURN:
    """Tests for get_entities with single URN (backward compatibility)."""

    async def test_single_urn_as_string(self, mock_client, sample_entity_response):
        """Test passing a single URN as string returns a single dict."""
        urn = "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table,PROD)"

        with patch(
            "datahub_integrations.mcp.mcp_server.get_datahub_client",
            return_value=mock_client,
        ):
            mock_client._graph.exists.return_value = True

            with patch(
                "datahub_integrations.mcp.mcp_server._execute_graphql"
            ) as mock_gql:
                mock_gql.return_value = {"entity": sample_entity_response}

                from datahub_integrations.mcp.mcp_server import get_entities

                result = await async_background(get_entities)(urn)

                assert isinstance(result, dict)
                assert result["urn"] == urn
                assert result["name"] == "table"
                mock_client._graph.exists.assert_called_once_with(urn)
                mock_gql.assert_called_once()

    async def test_single_urn_not_found_raises_error(self, mock_client):
        """Test that single URN not found raises ItemNotFoundError."""
        urn = (
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.nonexistent,PROD)"
        )

        with patch(
            "datahub_integrations.mcp.mcp_server.get_datahub_client",
            return_value=mock_client,
        ):
            mock_client._graph.exists.return_value = False

            from datahub_integrations.mcp.mcp_server import get_entities

            with pytest.raises(ItemNotFoundError, match="Entity .* not found"):
                await async_background(get_entities)(urn)

    async def test_single_urn_graphql_error_raises(self, mock_client):
        """Test that GraphQL error for single URN raises exception."""
        urn = "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table,PROD)"

        with patch(
            "datahub_integrations.mcp.mcp_server.get_datahub_client",
            return_value=mock_client,
        ):
            mock_client._graph.exists.return_value = True

            with patch(
                "datahub_integrations.mcp.mcp_server._execute_graphql"
            ) as mock_gql:
                mock_gql.side_effect = Exception("GraphQL error")

                from datahub_integrations.mcp.mcp_server import get_entities

                with pytest.raises(Exception, match="GraphQL error"):
                    await async_background(get_entities)(urn)


class TestGetEntitiesMultipleURNs:
    """Tests for get_entities with multiple URNs (batch mode)."""

    async def test_multiple_urns_returns_list(
        self, mock_client, sample_entity_response
    ):
        """Test passing multiple URNs returns a list of dicts."""
        urns = [
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table1,PROD)",
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table2,PROD)",
        ]

        with patch(
            "datahub_integrations.mcp.mcp_server.get_datahub_client",
            return_value=mock_client,
        ):
            mock_client._graph.exists.return_value = True

            with patch(
                "datahub_integrations.mcp.mcp_server._execute_graphql"
            ) as mock_gql:
                mock_gql.side_effect = [
                    {
                        "entity": {
                            **sample_entity_response,
                            "urn": urns[0],
                            "name": "table1",
                        }
                    },
                    {
                        "entity": {
                            **sample_entity_response,
                            "urn": urns[1],
                            "name": "table2",
                        }
                    },
                ]

                from datahub_integrations.mcp.mcp_server import get_entities

                result = await async_background(get_entities)(urns)

                assert isinstance(result, list)
                assert len(result) == 2
                assert result[0]["urn"] == urns[0]
                assert result[0]["name"] == "table1"
                assert result[1]["urn"] == urns[1]
                assert result[1]["name"] == "table2"
                assert mock_client._graph.exists.call_count == 2
                assert mock_gql.call_count == 2

    async def test_empty_list_returns_empty_list(self, mock_client):
        """Test passing empty list returns empty list."""
        with patch(
            "datahub_integrations.mcp.mcp_server.get_datahub_client",
            return_value=mock_client,
        ):
            from datahub_integrations.mcp.mcp_server import get_entities

            result = await async_background(get_entities)([])

            assert isinstance(result, list)
            assert len(result) == 0

    async def test_mixed_success_and_failure(self, mock_client, sample_entity_response):
        """Test that mix of successful and failed fetches returns partial results with errors."""
        urns = [
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table1,PROD)",
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.nonexistent,PROD)",
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table3,PROD)",
        ]

        with patch(
            "datahub_integrations.mcp.mcp_server.get_datahub_client",
            return_value=mock_client,
        ):
            mock_client._graph.exists.side_effect = [True, False, True]

            with patch(
                "datahub_integrations.mcp.mcp_server._execute_graphql"
            ) as mock_gql:
                mock_gql.side_effect = [
                    {
                        "entity": {
                            **sample_entity_response,
                            "urn": urns[0],
                            "name": "table1",
                        }
                    },
                    {
                        "entity": {
                            **sample_entity_response,
                            "urn": urns[2],
                            "name": "table3",
                        }
                    },
                ]

                from datahub_integrations.mcp.mcp_server import get_entities

                result = await async_background(get_entities)(urns)

                assert isinstance(result, list)
                assert len(result) == 3
                assert result[0]["urn"] == urns[0]
                assert result[0]["name"] == "table1"
                assert "error" not in result[0]
                assert "error" in result[1]
                assert result[1]["urn"] == urns[1]
                assert "not found" in result[1]["error"]
                assert result[2]["urn"] == urns[2]
                assert result[2]["name"] == "table3"
                assert "error" not in result[2]

    async def test_graphql_error_in_batch_includes_error_dict(
        self, mock_client, sample_entity_response
    ):
        """Test that GraphQL error for one URN in batch adds error dict, not raises."""
        urns = [
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table1,PROD)",
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table2,PROD)",
        ]

        with patch(
            "datahub_integrations.mcp.mcp_server.get_datahub_client",
            return_value=mock_client,
        ):
            mock_client._graph.exists.return_value = True

            with patch(
                "datahub_integrations.mcp.mcp_server._execute_graphql"
            ) as mock_gql:
                mock_gql.side_effect = [
                    {
                        "entity": {
                            **sample_entity_response,
                            "urn": urns[0],
                            "name": "table1",
                        }
                    },
                    Exception("GraphQL timeout"),
                ]

                from datahub_integrations.mcp.mcp_server import get_entities

                result = await async_background(get_entities)(urns)

                assert isinstance(result, list)
                assert len(result) == 2
                assert result[0]["urn"] == urns[0]
                assert "error" not in result[0]
                assert "error" in result[1]
                assert result[1]["urn"] == urns[1]
                assert "GraphQL timeout" in result[1]["error"]

    async def test_all_urns_not_found(self, mock_client):
        """Test that all URNs not found returns list of error dicts."""
        urns = [
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.missing1,PROD)",
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.missing2,PROD)",
        ]

        with patch(
            "datahub_integrations.mcp.mcp_server.get_datahub_client",
            return_value=mock_client,
        ):
            mock_client._graph.exists.return_value = False

            from datahub_integrations.mcp.mcp_server import get_entities

            result = await async_background(get_entities)(urns)

            assert isinstance(result, list)
            assert len(result) == 2
            assert all("error" in item for item in result)
            assert all("not found" in item["error"] for item in result)
            assert result[0]["urn"] == urns[0]
            assert result[1]["urn"] == urns[1]


class TestGetEntitiesIntegration:
    """Integration tests for get_entities with URL injection and cleaning."""

    async def test_urls_injected_for_single_urn(
        self, mock_client, sample_entity_response
    ):
        """Test that URLs are injected for single URN result."""
        urn = "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table,PROD)"

        with patch(
            "datahub_integrations.mcp.mcp_server.get_datahub_client",
            return_value=mock_client,
        ):
            mock_client._graph.exists.return_value = True

            with patch(
                "datahub_integrations.mcp.mcp_server._execute_graphql"
            ) as mock_gql:
                mock_gql.return_value = {"entity": sample_entity_response}

                with patch(
                    "datahub_integrations.mcp.mcp_server.inject_urls_for_urns"
                ) as mock_inject:
                    from datahub_integrations.mcp.mcp_server import get_entities

                    await async_background(get_entities)(urn)

                    mock_inject.assert_called_once()
                    call_args = mock_inject.call_args[0]
                    assert call_args[0] == mock_client._graph
                    assert isinstance(call_args[1], dict)

    async def test_descriptions_truncated(self, mock_client):
        """Test that descriptions are truncated for all results."""
        entity_with_long_desc = {
            "urn": "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table,PROD)",
            "description": "x" * 2000,
        }

        with patch(
            "datahub_integrations.mcp.mcp_server.get_datahub_client",
            return_value=mock_client,
        ):
            mock_client._graph.exists.return_value = True

            with patch(
                "datahub_integrations.mcp.mcp_server._execute_graphql"
            ) as mock_gql:
                mock_gql.return_value = {"entity": entity_with_long_desc}

                with patch(
                    "datahub_integrations.mcp.mcp_server.truncate_descriptions"
                ) as mock_truncate:
                    from datahub_integrations.mcp.mcp_server import get_entities

                    await async_background(get_entities)(
                        "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table,PROD)"
                    )

                    mock_truncate.assert_called_once()

    async def test_response_cleaned(self, mock_client):
        """Test that response is cleaned (removes __typename, null values, etc.)."""
        entity_with_typename = {
            "__typename": "Dataset",
            "urn": "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table,PROD)",
            "name": "table",
            "description": None,
            "tags": [],
        }

        with patch(
            "datahub_integrations.mcp.mcp_server.get_datahub_client",
            return_value=mock_client,
        ):
            mock_client._graph.exists.return_value = True

            with patch(
                "datahub_integrations.mcp.mcp_server._execute_graphql"
            ) as mock_gql:
                mock_gql.return_value = {"entity": entity_with_typename}

                from datahub_integrations.mcp.mcp_server import get_entities

                result = await async_background(get_entities)(
                    "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table,PROD)"
                )

                assert "__typename" not in result
                assert "description" not in result
                assert "tags" not in result
                assert result["urn"] == entity_with_typename["urn"]
                assert result["name"] == entity_with_typename["name"]
