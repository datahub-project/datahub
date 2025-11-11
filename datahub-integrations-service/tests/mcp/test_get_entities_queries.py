"""
Unit tests for Query entity handling in get_entities().
"""

from unittest.mock import MagicMock, patch

import pytest
from datahub.ingestion.graph.client import DataHubGraph
from datahub.sdk.main_client import DataHubClient

from datahub_integrations.mcp.mcp_server import get_entities, with_datahub_client


@pytest.fixture
def mock_client():
    """Create a mock DataHub client for testing."""
    graph = MagicMock(spec=DataHubGraph)
    client = MagicMock(spec=DataHubClient)
    client._graph = graph
    return client


def test_get_entities_with_query_urn(mock_client):
    """Test that get_entities() correctly handles Query URNs."""

    query_urn = "urn:li:query:abc123"

    # Mock QueryEntity response
    mock_response = {
        "entity": {
            "urn": query_urn,
            "type": "QUERY",
            "properties": {
                "name": "Test Query",
                "description": "A test query",
                "source": "SYSTEM",
                "statement": {
                    "value": "SELECT * FROM table",
                    "language": "SQL",
                },
                "lastModified": {
                    "actor": "urn:li:corpuser:datahub",
                },
            },
            "platform": {
                "urn": "urn:li:dataPlatform:snowflake",
                "name": "snowflake",
            },
            "subjects": [
                {
                    "dataset": {
                        "urn": "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.table,PROD)",
                        "name": "table",
                    }
                }
            ],
        }
    }

    mock_client._graph.exists.return_value = True
    mock_client._graph.url_for = lambda x: f"https://example.com/{x}"

    with patch(
        "datahub_integrations.mcp.mcp_server._execute_graphql",
        return_value=mock_response,
    ) as mock_gql:
        with with_datahub_client(mock_client):
            result = get_entities(query_urn)

    # Verify it used GetQueryEntity operation
    mock_gql.assert_called_once()
    call_kwargs = mock_gql.call_args[1]
    assert call_kwargs["operation_name"] == "GetQueryEntity"

    # Verify response structure
    assert result["urn"] == query_urn
    assert result["type"] == "QUERY"
    assert result["properties"]["source"] == "SYSTEM"
    assert result["properties"]["statement"]["value"] == "SELECT * FROM table"
    assert result["properties"]["statement"]["language"] == "SQL"
    assert result["platform"]["name"] == "snowflake"
    assert len(result["subjects"]) == 1


def test_get_entities_with_dataset_urn(mock_client):
    """Test that get_entities() uses GetEntity for non-Query URNs."""

    dataset_urn = "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.table,PROD)"

    # Mock Dataset response
    mock_response = {
        "entity": {
            "urn": dataset_urn,
            "type": "DATASET",
            "name": "table",
            "properties": {
                "name": "table",
                "description": "A test table",
            },
        }
    }

    mock_client._graph.exists.return_value = True
    mock_client._graph.url_for = lambda x: f"https://example.com/{x}"

    with patch(
        "datahub_integrations.mcp.mcp_server._execute_graphql",
        return_value=mock_response,
    ) as mock_gql:
        with with_datahub_client(mock_client):
            result = get_entities(dataset_urn)

    # Verify it used GetEntity operation (not GetQueryEntity)
    mock_gql.assert_called_once()
    call_kwargs = mock_gql.call_args[1]
    assert call_kwargs["operation_name"] == "GetEntity"

    # Verify response
    assert result["urn"] == dataset_urn
    assert result["type"] == "DATASET"


def test_get_entities_mixed_urns(mock_client):
    """Test that get_entities() handles mixed URN types (query + dataset)."""

    query_urn = "urn:li:query:abc123"
    dataset_urn = "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.table,PROD)"

    # Mock responses
    query_response = {
        "entity": {
            "urn": query_urn,
            "type": "QUERY",
            "properties": {
                "source": "SYSTEM",
                "statement": {"value": "SELECT 1", "language": "SQL"},
            },
            "platform": {"name": "snowflake"},
            "subjects": [],
        }
    }

    dataset_response = {
        "entity": {
            "urn": dataset_urn,
            "type": "DATASET",
            "name": "table",
            "properties": {"name": "table"},
        }
    }

    mock_client._graph.exists.return_value = True
    mock_client._graph.url_for = lambda x: f"https://example.com/{x}"

    with patch(
        "datahub_integrations.mcp.mcp_server._execute_graphql",
        side_effect=[query_response, dataset_response],
    ) as mock_gql:
        with with_datahub_client(mock_client):
            results = get_entities([query_urn, dataset_urn])

    # Verify two different operations were called
    assert mock_gql.call_count == 2

    # First call should be GetQueryEntity
    first_call_kwargs = mock_gql.call_args_list[0][1]
    assert first_call_kwargs["operation_name"] == "GetQueryEntity"

    # Second call should be GetEntity
    second_call_kwargs = mock_gql.call_args_list[1][1]
    assert second_call_kwargs["operation_name"] == "GetEntity"

    # Verify results
    assert len(results) == 2
    assert results[0]["type"] == "QUERY"
    assert results[1]["type"] == "DATASET"


def test_get_entities_query_urn_edge_cases(mock_client):
    """Test query URN detection with various formats."""

    # Mock response
    mock_response = {
        "entity": {
            "urn": "urn:li:query:test",
            "type": "QUERY",
            "properties": {
                "source": "MANUAL",
                "statement": {"value": "SELECT 1", "language": "SQL"},
            },
            "platform": {"name": "bigquery"},
            "subjects": [],
        }
    }

    mock_client._graph.exists.return_value = True
    mock_client._graph.url_for = lambda x: f"https://example.com/{x}"

    # Test various query URN formats
    test_urns = [
        "urn:li:query:simple",
        "urn:li:query:abc123def456",
        "urn:li:query:e039cdc97f9c2f66e3223691eb21d3633b2d49088c3fb761ce1574980b2549ee",
    ]

    for test_urn in test_urns:
        mock_response["entity"]["urn"] = test_urn

        with patch(
            "datahub_integrations.mcp.mcp_server._execute_graphql",
            return_value=mock_response,
        ) as mock_gql:
            with with_datahub_client(mock_client):
                result = get_entities(test_urn)

        # Verify GetQueryEntity was used
        call_kwargs = mock_gql.call_args[1]
        assert call_kwargs["operation_name"] == "GetQueryEntity"
        assert result["urn"] == test_urn
