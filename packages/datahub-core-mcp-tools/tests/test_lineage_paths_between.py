"""get_lineage_paths_between: direction handling, path reversal and no-path answers.

Most tests run through the GraphQL read backend with execute_graphql patched, so
they also cover which operation is sent.
"""

from typing import Any, List, Optional, cast
from unittest.mock import MagicMock, patch

import pytest
from datahub.ingestion.graph.client import DataHubGraph
from datahub.sdk.main_client import DataHubClient
from fastmcp.exceptions import ToolError

from datahub_core_mcp_tools.graphql_helpers import with_datahub_client
from datahub_core_mcp_tools.read_backend import JsonObject, ReadBackendBinding
from datahub_core_mcp_tools.search_flags import SearchFlagsOverride
from datahub_core_mcp_tools.tool_context import ToolContext
from datahub_core_mcp_tools.tools.lineage import get_lineage_paths_between


@pytest.fixture
def mock_client():
    """Create a mock DataHub client for testing."""
    graph = MagicMock(spec=DataHubGraph)
    graph.exists.return_value = True
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
        "datahub_core_mcp_tools.graphql_helpers.execute_graphql",
        return_value=mock_response,
    ):
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
        "datahub_core_mcp_tools.graphql_helpers.execute_graphql",
        return_value=mock_response,
    ):
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
        "datahub_core_mcp_tools.graphql_helpers.execute_graphql",
        return_value=mock_response,
    ) as mock_gql:
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

    # This tool returns only `paths` and matches on entity.urn, so it must use the
    # paths-only projection. Reverting to GetEntityLineage makes GMS build a full
    # entityPreview for up to 100 nodes and discard all of it (~12x the response),
    # which every other assertion here would still pass through.
    assert mock_gql.call_args.kwargs["operation_name"] == "GetEntityLineagePaths"


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
        "datahub_core_mcp_tools.graphql_helpers.execute_graphql",
        return_value=mock_response,
    ):
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
        "datahub_core_mcp_tools.graphql_helpers.execute_graphql",
        side_effect=[mock_empty_response, mock_upstream_response],
    ):
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
        "datahub_core_mcp_tools.graphql_helpers.execute_graphql",
        return_value=mock_response,
    ) as mock_gql:
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


def _upstream_response(
    urns: list[str], total: int, is_partial: Optional[bool] = None
) -> dict:
    return {
        "searchAcrossLineage": {
            "total": total,
            "isPartial": is_partial,
            "searchResults": [
                {
                    "entity": {"urn": urn, "type": "DATASET"},
                    "paths": [{"path": [{"urn": urn, "type": "DATASET"}]}],
                    "degree": 1,
                }
                for urn in urns
            ],
        }
    }


def test_get_lineage_paths_target_not_found(mock_client):
    """A target outside the source's lineage is a no-path result, not an error."""

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
        "datahub_core_mcp_tools.graphql_helpers.execute_graphql",
        return_value=mock_response,
    ):
        with with_datahub_client(mock_client):
            result = get_lineage_paths_between(
                source_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,db.source,PROD)",
                target_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,db.target,PROD)",
                direction="downstream",
            )

    assert result["pathCount"] == 0
    assert result["paths"] == []
    assert result["metadata"]["direction"] == "downstream"
    assert result["metadata"]["noPath"]["searchComplete"] is True
    assert (
        result["target"]["urn"]
        == "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.target,PROD)"
    )


def test_get_lineage_paths_no_results(mock_client):
    """An entity with no upstream lineage at all is a no-path result."""

    mock_response = {
        "searchAcrossLineage": {
            "total": 0,
            "searchResults": [],
        }
    }

    with patch(
        "datahub_core_mcp_tools.graphql_helpers.execute_graphql",
        return_value=mock_response,
    ):
        with with_datahub_client(mock_client):
            result = get_lineage_paths_between(
                source_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,db.source,PROD)",
                target_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,db.target,PROD)",
                direction="downstream",
            )

    assert result["pathCount"] == 0
    assert result["metadata"]["noPath"]["searchComplete"] is True


def test_get_lineage_paths_truncated_search_is_not_conclusive(mock_client):
    """When the result cap cuts the search short, no-path says a path may exist."""

    mock_response = _upstream_response(
        ["urn:li:dataset:(urn:li:dataPlatform:snowflake,db.other_table,PROD)"],
        total=250,
    )

    with patch(
        "datahub_core_mcp_tools.graphql_helpers.execute_graphql",
        return_value=mock_response,
    ):
        with with_datahub_client(mock_client):
            result = get_lineage_paths_between(
                source_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,db.source,PROD)",
                target_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,db.target,PROD)",
                direction="upstream",
            )

    assert result["pathCount"] == 0
    assert result["metadata"]["noPath"]["searchComplete"] is False


def test_get_lineage_paths_auto_discover_no_path_either_direction(mock_client):
    """Auto-discovery searches both directions and returns one no-path result."""

    empty = _upstream_response([], total=0)
    unrelated = _upstream_response(
        ["urn:li:dataset:(urn:li:dataPlatform:snowflake,db.other_table,PROD)"],
        total=1,
    )

    with patch(
        "datahub_core_mcp_tools.graphql_helpers.execute_graphql",
        side_effect=[empty, unrelated],
    ) as mock_gql:
        with with_datahub_client(mock_client):
            result = get_lineage_paths_between(
                source_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,db.source,PROD)",
                target_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,db.target,PROD)",
            )

    assert mock_gql.call_count == 2
    assert result["pathCount"] == 0
    assert result["metadata"]["direction"] == "searched-both"
    assert result["metadata"]["noPath"]["searchComplete"] is True


def test_get_lineage_paths_target_found_without_path_details(mock_client):
    """A connected target with no path details stops auto-discovery, without error."""

    mock_response = {
        "searchAcrossLineage": {
            "total": 1,
            "searchResults": [
                {
                    "entity": {
                        "urn": "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.source,PROD)",
                        "type": "DATASET",
                    },
                    "degree": 1,
                }
            ],
        }
    }

    with patch(
        "datahub_core_mcp_tools.graphql_helpers.execute_graphql",
        return_value=mock_response,
    ) as mock_gql:
        with with_datahub_client(mock_client):
            result = get_lineage_paths_between(
                source_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,db.source,PROD)",
                target_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,db.target,PROD)",
            )

    assert mock_gql.call_count == 1
    assert result["pathCount"] == 0
    assert result["metadata"]["direction"] == "auto-discovered-downstream"
    assert "pathsUnavailable" in result["metadata"]
    assert "noPath" not in result["metadata"]


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
        "datahub_core_mcp_tools.graphql_helpers.execute_graphql",
        return_value=mock_response,
    ):
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
        with pytest.raises(ToolError) as exc_info:
            get_lineage_paths_between(
                source_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,db.source,PROD)",
                target_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,db.target,PROD)",
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
        "datahub_core_mcp_tools.graphql_helpers.execute_graphql",
        return_value=mock_response,
    ):
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
        "datahub_core_mcp_tools.graphql_helpers.execute_graphql",
        return_value=mock_response,
    ):
        with with_datahub_client(mock_client):
            result = get_lineage_paths_between(
                source_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,db.source,PROD)",
                target_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,db.target,PROD)",
                direction="downstream",
            )

    # Should handle gracefully and find the valid path
    assert result["pathCount"] >= 1


SOURCE = "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.source,PROD)"
TARGET = "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.target,PROD)"
OTHER = "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.other_table,PROD)"


@pytest.mark.parametrize(
    "urns",
    [[], [OTHER]],
    ids=["partial-empty", "partial-nonempty-total-matches"],
)
def test_get_lineage_paths_partial_traversal_is_not_conclusive(mock_client, urns):
    """A traversal GMS stopped early (isPartial) can't prove the two are unconnected,
    even when `total` equals the results returned."""

    with patch(
        "datahub_core_mcp_tools.graphql_helpers.execute_graphql",
        return_value=_upstream_response(urns, total=len(urns), is_partial=True),
    ):
        with with_datahub_client(mock_client):
            result = get_lineage_paths_between(
                source_urn=SOURCE, target_urn=TARGET, direction="upstream"
            )

    assert result["pathCount"] == 0
    assert result["metadata"]["noPath"]["searchComplete"] is False


def test_get_lineage_paths_empty_result_with_positive_total_is_not_conclusive(
    mock_client,
):
    with patch(
        "datahub_core_mcp_tools.graphql_helpers.execute_graphql",
        return_value=_upstream_response([], total=5),
    ):
        with with_datahub_client(mock_client):
            result = get_lineage_paths_between(
                source_urn=SOURCE, target_urn=TARGET, direction="upstream"
            )

    assert result["metadata"]["noPath"]["searchComplete"] is False


@pytest.mark.parametrize(
    "first_partial, second_partial",
    [(True, False), (False, True)],
    ids=["downstream-partial", "upstream-partial"],
)
def test_get_lineage_paths_auto_discover_partial_search_stays_incomplete(
    mock_client, first_partial, second_partial
):
    """searched-both is only complete when neither direction's search was cut short."""

    with patch(
        "datahub_core_mcp_tools.graphql_helpers.execute_graphql",
        side_effect=[
            _upstream_response([OTHER], total=1, is_partial=first_partial),
            _upstream_response([OTHER], total=1, is_partial=second_partial),
        ],
    ):
        with with_datahub_client(mock_client):
            result = get_lineage_paths_between(source_urn=SOURCE, target_urn=TARGET)

    assert result["metadata"]["direction"] == "searched-both"
    assert result["metadata"]["noPath"]["searchComplete"] is False


def test_get_lineage_paths_requests_is_partial(mock_client):
    """The paths query must ask GMS for isPartial; without it every partial
    traversal reads as complete."""

    with patch(
        "datahub_core_mcp_tools.graphql_helpers.execute_graphql",
        return_value=_upstream_response([], total=0),
    ) as mock_gql:
        with with_datahub_client(mock_client):
            get_lineage_paths_between(
                source_urn=SOURCE, target_urn=TARGET, direction="upstream"
            )

    assert mock_gql.call_args.kwargs["operation_name"] == "GetEntityLineagePaths"
    assert "isPartial" in mock_gql.call_args.kwargs["query"]


@pytest.mark.parametrize(
    "side_effect",
    [
        [RuntimeError("GMS read timeout")],
        [_upstream_response([], total=0), RuntimeError("GMS read timeout")],
    ],
    ids=["first-search", "after-first-no-path"],
)
def test_get_lineage_paths_backend_errors_propagate(mock_client, side_effect):
    """A GMS failure is a failed call, not a no-path answer."""

    with patch(
        "datahub_core_mcp_tools.graphql_helpers.execute_graphql",
        side_effect=side_effect,
    ):
        with with_datahub_client(mock_client):
            with pytest.raises(RuntimeError):
                get_lineage_paths_between(source_urn=SOURCE, target_urn=TARGET)


class FakeBackend:
    """A ReadBackend answering each paths search in turn; records what was sent."""

    def __init__(self, *responses: dict, missing: tuple[str, ...] = ()) -> None:
        self.responses = list(responses)
        self.missing = missing
        self.sent: List[dict[str, Any]] = []

    def exists(self, urn: str) -> bool:
        return urn not in self.missing

    def dataset_usage(self, urn: str, time_range: str) -> JsonObject | None:
        return None

    def list_queries(self, variables: JsonObject) -> JsonObject:
        return {}

    def lineage_paths(self, variables: JsonObject) -> JsonObject:
        self.sent.append(dict(variables))
        return cast(JsonObject, self.responses.pop(0)["searchAcrossLineage"])

    def search_lineage(self, variables: JsonObject) -> JsonObject:
        raise AssertionError("paths use the paths-only query")

    def entity(self, urn: str, *, include_contract: bool = False) -> None:
        return None

    def related_documents(self, urn: str, start: int, count: int) -> JsonObject:
        return {}

    def aspects(self, urn: str) -> dict[str, JsonObject] | None:
        return None

    def search_entities(self, variables: JsonObject) -> JsonObject:
        return {}

    def search_documents(self, variables: JsonObject, *, semantic: bool) -> JsonObject:
        return {}

    def document_contents(self, urns: list[str]) -> list[JsonObject | None]:
        return []


def _run(backend: FakeBackend, *items: object, **kwargs: Any) -> dict:
    context = ToolContext([ReadBackendBinding(backend), *items])
    with with_datahub_client(MagicMock(), tool_context=context):
        return get_lineage_paths_between(**kwargs)


def test_auto_discover_searches_upstream_of_each_end_with_the_override() -> None:
    # Downstream is answered from the target's upstreams, upstream from the source's.
    backend = FakeBackend(
        _upstream_response([OTHER], total=1), _upstream_response([TARGET], total=1)
    )

    result = _run(
        backend,
        SearchFlagsOverride({"skipCache": True}),
        source_urn=SOURCE,
        target_urn=TARGET,
    )

    assert [sent["urn"] for sent in backend.sent] == [TARGET, SOURCE]
    assert all(sent["direction"] == "UPSTREAM" for sent in backend.sent)
    assert all(sent["searchFlags"]["skipCache"] is True for sent in backend.sent)
    assert backend.sent[0]["count"] == 100
    assert result["metadata"]["direction"] == "auto-discovered-upstream"
    assert result["pathCount"] == 1


def test_column_paths_search_from_the_target_column() -> None:
    backend = FakeBackend(_upstream_response([], total=0))

    _run(
        backend,
        source_urn=SOURCE,
        target_urn=TARGET,
        source_column="user_id",
        target_column="customer_id",
        direction="downstream",
    )

    assert backend.sent[0]["urn"] == f"urn:li:schemaField:({TARGET},customer_id)"


def test_a_missing_entity_is_an_error_not_a_missing_path() -> None:
    backend = FakeBackend(missing=(TARGET,))

    with pytest.raises(ToolError, match="not found"):
        _run(backend, source_urn=SOURCE, target_urn=TARGET)

    assert backend.sent == []


def test_a_name_instead_of_a_urn_is_rejected() -> None:
    with pytest.raises(ToolError, match="target_urn"):
        _run(FakeBackend(), source_urn=SOURCE, target_urn="orders")
