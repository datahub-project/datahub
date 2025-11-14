"""
Unit tests for get_lineage_paths_between functionality.
"""

from unittest.mock import MagicMock, patch

import pytest
from datahub.errors import ItemNotFoundError
from datahub.ingestion.graph.client import DataHubGraph
from datahub.sdk.main_client import DataHubClient

from datahub_integrations.mcp.mcp_server import (
    get_lineage_paths_between,
    with_datahub_client,
)


@pytest.fixture
def mock_client():
    """Create a mock DataHub client for testing."""
    graph = MagicMock(spec=DataHubGraph)
    client = MagicMock(spec=DataHubClient)
    client._graph = graph
    return client


def test_get_lineage_paths_column_level_with_queries(mock_client):
    """Test fetching column-level lineage path with transformation queries."""

    # Mock response with column path containing queries
    # NOTE: Mocking upstream query from target (final_table) finding source (base_table)
    # Path will be reversed by implementation to show source → target
    mock_response = {
        "searchAcrossLineage": {
            "total": 1,
            "searchResults": [
                {
                    "entity": {
                        "urn": "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.base_table,PROD)",
                        "type": "DATASET",
                        "name": "base_table",
                    },
                    "paths": [
                        {
                            "path": [
                                # Path in upstream order (target → source), will be reversed
                                {
                                    "urn": "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:snowflake,db.final_table,PROD),customer_id)",
                                    "type": "SCHEMA_FIELD",
                                    "fieldPath": "customer_id",
                                    "parent": {
                                        "urn": "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.final_table,PROD)",
                                        "type": "DATASET",
                                    },
                                },
                                {
                                    "urn": "urn:li:query:abc123",
                                    "type": "QUERY",
                                },
                                {
                                    "urn": "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:snowflake,db.base_table,PROD),user_id)",
                                    "type": "SCHEMA_FIELD",
                                    "fieldPath": "user_id",
                                    "parent": {
                                        "urn": "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.base_table,PROD)",
                                        "type": "DATASET",
                                    },
                                },
                            ]
                        }
                    ],
                    "degree": 1,
                }
            ],
        }
    }

    with patch(
        "datahub_integrations.mcp.mcp_server._execute_graphql",
        return_value=mock_response,
    ):
        mock_client._graph.url_for = lambda x: f"https://example.com/{x}"

        with with_datahub_client(mock_client):
            result = get_lineage_paths_between(
                source_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,db.base_table,PROD)",
                target_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,db.final_table,PROD)",
                source_column="user_id",
                target_column="customer_id",
                direction="downstream",
            )

    # Verify response structure
    assert (
        result["source"]["urn"]
        == "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.base_table,PROD)"
    )
    assert result["source"]["column"] == "user_id"
    assert (
        result["target"]["urn"]
        == "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.final_table,PROD)"
    )
    assert result["target"]["column"] == "customer_id"
    assert result["pathCount"] == 1

    # Verify path structure (should be reversed to source → target)
    path = result["paths"][0]["path"]
    assert len(path) == 3
    assert path[0]["type"] == "SCHEMA_FIELD"
    assert path[0]["fieldPath"] == "user_id"  # Source column first
    assert path[1]["type"] == "QUERY"
    assert path[1]["urn"] == "urn:li:query:abc123"
    assert path[2]["type"] == "SCHEMA_FIELD"
    assert path[2]["fieldPath"] == "customer_id"  # Target column last

    # Verify metadata
    assert result["metadata"]["queryType"] == "lineage-path-trace"
    assert result["metadata"]["direction"] == "downstream"
    assert result["metadata"]["pathType"] == "column-level"

    # Verify conditional queryEntities field (should be present with queries)
    assert "queryEntities" in result["metadata"]["fields"]["paths"]
    assert "get_entities" in result["metadata"]["fields"]["paths"]["queryEntities"]


def test_get_lineage_paths_column_level_without_queries(mock_client):
    """Test fetching column-level lineage path without transformation queries."""

    # Mock response with direct column mapping (no queries)
    # NOTE: Mocking upstream query from target (bigquery) finding source (kafka)
    mock_response = {
        "searchAcrossLineage": {
            "total": 1,
            "searchResults": [
                {
                    "entity": {
                        "urn": "urn:li:dataset:(urn:li:dataPlatform:kafka,db.table,PROD)",
                        "type": "DATASET",
                        "name": "table",
                    },
                    "paths": [
                        {
                            "path": [
                                # Path in upstream order (target → source), will be reversed
                                {
                                    "urn": "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:bigquery,db.table,PROD),user_id)",
                                    "type": "SCHEMA_FIELD",
                                    "fieldPath": "user_id",
                                    "parent": {
                                        "urn": "urn:li:dataset:(urn:li:dataPlatform:bigquery,db.table,PROD)",
                                        "type": "DATASET",
                                    },
                                },
                                {
                                    "urn": "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:kafka,db.table,PROD),user_id)",
                                    "type": "SCHEMA_FIELD",
                                    "fieldPath": "user_id",
                                    "parent": {
                                        "urn": "urn:li:dataset:(urn:li:dataPlatform:kafka,db.table,PROD)",
                                        "type": "DATASET",
                                    },
                                },
                            ]
                        }
                    ],
                    "degree": 1,
                }
            ],
        }
    }

    with patch(
        "datahub_integrations.mcp.mcp_server._execute_graphql",
        return_value=mock_response,
    ):
        mock_client._graph.url_for = lambda x: f"https://example.com/{x}"

        with with_datahub_client(mock_client):
            result = get_lineage_paths_between(
                source_urn="urn:li:dataset:(urn:li:dataPlatform:kafka,db.table,PROD)",
                target_urn="urn:li:dataset:(urn:li:dataPlatform:bigquery,db.table,PROD)",
                source_column="user_id",
                target_column="user_id",
                direction="downstream",
            )

    # Verify path has no QUERY entities
    path = result["paths"][0]["path"]
    assert len(path) == 2
    assert all(entity["type"] == "SCHEMA_FIELD" for entity in path)

    # Verify conditional queryEntities field (should be ABSENT without queries)
    assert "queryEntities" not in result["metadata"]["fields"]["paths"]


def test_get_lineage_paths_dataset_level(mock_client):
    """Test fetching dataset-level lineage path."""

    # Mock response for dataset-level lineage
    # NOTE: Mocking upstream query from target finding source
    mock_response = {
        "searchAcrossLineage": {
            "total": 1,
            "searchResults": [
                {
                    "entity": {
                        "urn": "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.source,PROD)",
                        "type": "DATASET",
                        "name": "source",
                    },
                    "paths": [
                        {
                            "path": [
                                # Path in upstream order (target → source), will be reversed
                                {
                                    "urn": "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.target,PROD)",
                                    "type": "DATASET",
                                    "name": "target",
                                },
                                {
                                    "urn": "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.source,PROD)",
                                    "type": "DATASET",
                                    "name": "source",
                                },
                            ]
                        }
                    ],
                    "degree": 1,
                }
            ],
        }
    }

    with patch(
        "datahub_integrations.mcp.mcp_server._execute_graphql",
        return_value=mock_response,
    ):
        mock_client._graph.url_for = lambda x: f"https://example.com/{x}"

        with with_datahub_client(mock_client):
            result = get_lineage_paths_between(
                source_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,db.source,PROD)",
                target_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,db.target,PROD)",
                direction="downstream",
            )

    # Verify no column info
    assert "column" not in result["source"]
    assert "column" not in result["target"]

    # Verify metadata
    assert result["metadata"]["pathType"] == "dataset-level"


def test_get_lineage_paths_auto_discover_downstream(mock_client):
    """Test auto-discovery finds path in downstream direction."""

    # Mock upstream query from target finding source (tries downstream first)
    mock_response = {
        "searchAcrossLineage": {
            "total": 1,
            "searchResults": [
                {
                    "entity": {
                        "urn": "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.source,PROD)",
                        "type": "DATASET",
                    },
                    "paths": [{"path": [{"type": "DATASET"}]}],
                    "degree": 1,
                }
            ],
        }
    }

    with patch(
        "datahub_integrations.mcp.mcp_server._execute_graphql",
        return_value=mock_response,
    ):
        mock_client._graph.url_for = lambda x: f"https://example.com/{x}"

        with with_datahub_client(mock_client):
            result = get_lineage_paths_between(
                source_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,db.source,PROD)",
                target_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,db.target,PROD)",
                # No direction - should auto-discover
            )

    # Verify direction was auto-discovered (semantic direction is downstream)
    assert result["metadata"]["direction"] == "auto-discovered-downstream"
    assert "note" in result["metadata"]
    assert "automatically discovered" in result["metadata"]["note"]


def test_get_lineage_paths_auto_discover_upstream(mock_client):
    """Test auto-discovery falls back to upstream when downstream fails."""

    # Mock response for upstream semantic direction (source depends on target)
    # When trying downstream fails, fallback to upstream
    mock_upstream_response = {
        "searchAcrossLineage": {
            "total": 1,
            "searchResults": [
                {
                    "entity": {
                        "urn": "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.target,PROD)",
                        "type": "DATASET",
                    },
                    "paths": [{"path": [{"type": "DATASET"}]}],
                    "degree": 1,
                }
            ],
        }
    }

    mock_empty_response = {
        "searchAcrossLineage": {
            "total": 0,
            "searchResults": [],
        }
    }

    # First call (try downstream: query target's upstream) returns empty
    # Second call (fallback upstream: query source's upstream) returns results
    with patch(
        "datahub_integrations.mcp.mcp_server._execute_graphql",
        side_effect=[mock_empty_response, mock_upstream_response],
    ):
        mock_client._graph.url_for = lambda x: f"https://example.com/{x}"

        with with_datahub_client(mock_client):
            result = get_lineage_paths_between(
                source_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,db.source,PROD)",
                target_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,db.target,PROD)",
                # No direction - should try both
            )

    # Verify direction was auto-discovered as upstream
    assert result["metadata"]["direction"] == "auto-discovered-upstream"


def test_get_lineage_paths_explicit_direction(mock_client):
    """Test explicit direction bypasses auto-discovery."""

    # Mock upstream query (implementation always queries upstream)
    mock_response = {
        "searchAcrossLineage": {
            "total": 1,
            "searchResults": [
                {
                    "entity": {
                        "urn": "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.target,PROD)",
                        "type": "DATASET",
                    },
                    "paths": [{"path": [{"type": "DATASET"}]}],
                    "degree": 1,
                }
            ],
        }
    }

    with patch(
        "datahub_integrations.mcp.mcp_server._execute_graphql",
        return_value=mock_response,
    ) as mock_gql:
        mock_client._graph.url_for = lambda x: f"https://example.com/{x}"

        with with_datahub_client(mock_client):
            result = get_lineage_paths_between(
                source_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,db.source,PROD)",
                target_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,db.target,PROD)",
                direction="upstream",
            )

    # Verify only one GraphQL call (no auto-discovery retry)
    assert mock_gql.call_count == 1

    # Verify direction is exact, not auto-discovered
    assert result["metadata"]["direction"] == "upstream"
    assert "note" not in result["metadata"]  # No auto-discovery note


def test_get_lineage_paths_target_not_found(mock_client):
    """Test error when target is not in lineage."""

    mock_response = {
        "searchAcrossLineage": {
            "total": 1,
            "searchResults": [
                {
                    "entity": {
                        "urn": "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.other_table,PROD)",
                        "type": "DATASET",
                    },
                    "paths": [{"path": []}],
                    "degree": 1,
                }
            ],
        }
    }

    with patch(
        "datahub_integrations.mcp.mcp_server._execute_graphql",
        return_value=mock_response,
    ):
        mock_client._graph.url_for = lambda x: f"https://example.com/{x}"

        with with_datahub_client(mock_client):
            with pytest.raises(ItemNotFoundError) as exc_info:
                get_lineage_paths_between(
                    source_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,db.source,PROD)",
                    target_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,db.target,PROD)",
                    direction="downstream",
                )

    assert "No lineage path found" in str(exc_info.value)
    assert "db.target" in str(exc_info.value)


def test_get_lineage_paths_no_results(mock_client):
    """Test error when no lineage exists at all."""

    mock_response = {
        "searchAcrossLineage": {
            "total": 0,
            "searchResults": [],
        }
    }

    with patch(
        "datahub_integrations.mcp.mcp_server._execute_graphql",
        return_value=mock_response,
    ):
        with with_datahub_client(mock_client):
            with pytest.raises(ItemNotFoundError) as exc_info:
                get_lineage_paths_between(
                    source_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,db.source,PROD)",
                    target_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,db.target,PROD)",
                    direction="downstream",
                )

    assert "No lineage found" in str(exc_info.value)


def test_get_lineage_paths_multiple_paths(mock_client):
    """Test handling multiple paths to same target."""

    # Mock upstream query from target finding source (multiple paths)
    mock_response = {
        "searchAcrossLineage": {
            "total": 1,
            "searchResults": [
                {
                    "entity": {
                        "urn": "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.source,PROD)",
                        "type": "DATASET",
                    },
                    "paths": [
                        {
                            "path": [
                                # Paths in upstream order (target → source), will be reversed
                                {"type": "DATASET", "urn": "target"},
                                {"type": "DATASET", "urn": "source1"},
                            ]
                        },
                        {
                            "path": [
                                {"type": "DATASET", "urn": "target"},
                                {"type": "DATASET", "urn": "intermediate"},
                                {"type": "DATASET", "urn": "source1"},
                            ]
                        },
                    ],
                    "degree": 1,
                }
            ],
        }
    }

    with patch(
        "datahub_integrations.mcp.mcp_server._execute_graphql",
        return_value=mock_response,
    ):
        mock_client._graph.url_for = lambda x: f"https://example.com/{x}"

        with with_datahub_client(mock_client):
            result = get_lineage_paths_between(
                source_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,db.source,PROD)",
                target_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,db.target,PROD)",
                direction="downstream",
            )

    # Verify both paths are returned
    assert result["pathCount"] == 2
    assert len(result["paths"]) == 2
    assert len(result["paths"][0]["path"]) == 2  # Direct path
    assert len(result["paths"][1]["path"]) == 3  # Via intermediate


def test_get_lineage_paths_invalid_column_combination(mock_client):
    """Test error when only one column is specified."""

    with with_datahub_client(mock_client):
        with pytest.raises(ValueError) as exc_info:
            get_lineage_paths_between(
                source_urn="urn:li:dataset:(...)",
                target_urn="urn:li:dataset:(...)",
                source_column="user_id",
                # Missing target_column
            )

    assert "Both source_column and target_column must be provided" in str(
        exc_info.value
    )


def test_get_lineage_paths_null_string_normalization(mock_client):
    """Test that string 'null' is normalized to None."""

    mock_response = {
        "searchAcrossLineage": {
            "total": 1,
            "searchResults": [
                {
                    "entity": {
                        "urn": "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.target,PROD)",
                        "type": "DATASET",
                    },
                    "paths": [{"path": [{"type": "DATASET"}]}],
                    "degree": 1,
                }
            ],
        }
    }

    with patch(
        "datahub_integrations.mcp.mcp_server._execute_graphql",
        return_value=mock_response,
    ):
        mock_client._graph.url_for = lambda x: f"https://example.com/{x}"

        with with_datahub_client(mock_client):
            result = get_lineage_paths_between(
                source_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,db.source,PROD)",
                target_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,db.target,PROD)",
                source_column="null",  # String "null" should be treated as None
                target_column="",  # Empty string should be treated as None
            )

    # Should be treated as dataset-level (no columns)
    assert "column" not in result["source"]
    assert "column" not in result["target"]
    assert result["metadata"]["pathType"] == "dataset-level"


def test_get_lineage_paths_safe_null_handling(mock_client):
    """Test safe handling of null paths in response."""

    # Mock upstream query with some null/malformed paths
    mock_response = {
        "searchAcrossLineage": {
            "total": 1,
            "searchResults": [
                {
                    "entity": {
                        "urn": "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.source,PROD)",
                        "type": "DATASET",
                    },
                    "paths": [
                        None,  # Null path object
                        {"path": None},  # Null path array
                        {"path": [{"type": "DATASET", "urn": "source"}]},  # Valid path
                    ],
                    "degree": 1,
                }
            ],
        }
    }

    with patch(
        "datahub_integrations.mcp.mcp_server._execute_graphql",
        return_value=mock_response,
    ):
        mock_client._graph.url_for = lambda x: f"https://example.com/{x}"

        with with_datahub_client(mock_client):
            result = get_lineage_paths_between(
                source_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,db.source,PROD)",
                target_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,db.target,PROD)",
                direction="downstream",
            )

    # Should handle gracefully and find the valid path
    assert result["pathCount"] >= 1
