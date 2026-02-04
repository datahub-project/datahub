"""Tests for lineage tools."""

from unittest.mock import Mock

import pytest

from datahub.errors import ItemNotFoundError
from datahub_agent_context.context import DataHubContext
from datahub_agent_context.mcp_tools.lineage import (
    AssetLineageAPI,
    AssetLineageDirective,
    get_lineage,
    get_lineage_paths_between,
)


@pytest.fixture
def mock_client():
    """Create a mock DataHubClient."""
    mock = Mock()
    mock._graph = Mock()
    mock.graph.execute_graphql = Mock()
    return mock


@pytest.fixture
def sample_lineage_response():
    """Sample lineage response from GraphQL."""
    return {
        "searchAcrossLineage": {
            "start": 0,
            "count": 2,
            "total": 10,
            "searchResults": [
                {
                    "entity": {
                        "urn": "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.upstream1,PROD)",
                        "type": "DATASET",
                        "name": "upstream1",
                    },
                    "degree": 1,
                },
                {
                    "entity": {
                        "urn": "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.upstream2,PROD)",
                        "type": "DATASET",
                        "name": "upstream2",
                    },
                    "degree": 1,
                },
            ],
            "facets": [],
        }
    }


class TestAssetLineageAPI:
    """Tests for AssetLineageAPI class."""

    def test_get_degree_filter_single_hop(self, mock_client):
        """Test degree filter for max_hops=1."""
        with DataHubContext(mock_client):
            api = AssetLineageAPI()
            filter = api.get_degree_filter(max_hops=1)

        assert filter is not None

    def test_get_degree_filter_two_hops(self, mock_client):
        """Test degree filter for max_hops=2."""
        with DataHubContext(mock_client):
            api = AssetLineageAPI()
            filter = api.get_degree_filter(max_hops=2)

        assert filter is not None

    def test_get_degree_filter_three_plus_hops(self, mock_client):
        """Test degree filter for max_hops>=3."""
        with DataHubContext(mock_client):
            api = AssetLineageAPI()
            filter = api.get_degree_filter(max_hops=3)

        assert filter is not None

    def test_get_degree_filter_invalid_hops(self, mock_client):
        """Test that invalid max_hops raises ValueError."""
        with DataHubContext(mock_client):
            api = AssetLineageAPI()

            with pytest.raises(ValueError):
                api.get_degree_filter(max_hops=0)

    def test_get_lineage_upstream(self, mock_client, sample_lineage_response):
        """Test getting upstream lineage."""
        mock_client._graph.execute_graphql.return_value = sample_lineage_response

        with DataHubContext(mock_client):
            api = AssetLineageAPI()
            directive = AssetLineageDirective(
                urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table,PROD)",
                upstream=True,
                downstream=False,
                max_hops=1,
                extra_filters=None,
                max_results=30,
            )

            result = api.get_lineage(directive)

        assert "upstreams" in result
        assert "downstreams" not in result

    def test_get_lineage_downstream(self, mock_client, sample_lineage_response):
        """Test getting downstream lineage."""
        mock_client._graph.execute_graphql.return_value = sample_lineage_response

        with DataHubContext(mock_client):
            api = AssetLineageAPI()
            directive = AssetLineageDirective(
                urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table,PROD)",
                upstream=False,
                downstream=True,
                max_hops=1,
                extra_filters=None,
                max_results=30,
            )

            result = api.get_lineage(directive)

        assert "downstreams" in result
        assert "upstreams" not in result


class TestGetLineage:
    """Tests for get_lineage function."""

    def test_basic_upstream_lineage(self, mock_client, sample_lineage_response):
        """Test basic upstream lineage query."""
        mock_client._graph.execute_graphql.return_value = sample_lineage_response

        with DataHubContext(mock_client):
            result = get_lineage(
                urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table,PROD)",
                upstream=True,
                max_hops=1,
                max_results=30,
            )

        assert "upstreams" in result
        assert "searchResults" in result["upstreams"]

    def test_basic_downstream_lineage(self, mock_client, sample_lineage_response):
        """Test basic downstream lineage query."""
        mock_client._graph.execute_graphql.return_value = sample_lineage_response

        with DataHubContext(mock_client):
            result = get_lineage(
                urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table,PROD)",
                upstream=False,
                max_hops=1,
                max_results=30,
            )

        assert "downstreams" in result

    def test_column_level_lineage(self, mock_client):
        """Test column-level lineage with paths."""
        mock_client._graph.execute_graphql.return_value = {
            "searchAcrossLineage": {
                "start": 0,
                "count": 1,
                "total": 1,
                "searchResults": [
                    {
                        "entity": {
                            "urn": "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.upstream,PROD)",
                            "type": "DATASET",
                        },
                        "degree": 1,
                        "paths": [
                            {
                                "path": [
                                    {"type": "SCHEMA_FIELD", "fieldPath": "source_col"},
                                    {"type": "SCHEMA_FIELD", "fieldPath": "target_col"},
                                ]
                            }
                        ],
                    }
                ],
            }
        }

        with DataHubContext(mock_client):
            result = get_lineage(
                urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table,PROD)",
                column="user_id",
                upstream=True,
                max_hops=1,
            )

        assert "metadata" in result
        assert result["metadata"]["queryType"] == "column-level-lineage"

    def test_lineage_with_query_filter(self, mock_client, sample_lineage_response):
        """Test lineage with query parameter."""
        mock_client._graph.execute_graphql.return_value = sample_lineage_response

        with DataHubContext(mock_client):
            get_lineage(
                urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table,PROD)",
                query="specific_table",
                upstream=True,
            )

        # Verify query parameter was passed
        call_args = mock_client._graph.execute_graphql.call_args
        variables = call_args.kwargs["variables"]
        assert variables["input"]["query"] == "specific_table"

    def test_lineage_with_pagination(self, mock_client, sample_lineage_response):
        """Test lineage pagination with offset."""
        mock_client._graph.execute_graphql.return_value = sample_lineage_response

        with DataHubContext(mock_client):
            result = get_lineage(
                urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table,PROD)",
                upstream=True,
                offset=10,
                max_results=20,
            )

        assert "upstreams" in result
        # Check pagination metadata
        upstreams = result["upstreams"]
        assert "offset" in upstreams
        assert "returned" in upstreams
        assert "hasMore" in upstreams

    def test_column_normalization(self, mock_client, sample_lineage_response):
        """Test that column='null' is normalized to None."""
        mock_client._graph.execute_graphql.return_value = sample_lineage_response

        with DataHubContext(mock_client):
            get_lineage(
                urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table,PROD)",
                column="null",  # Should be normalized to None
                upstream=True,
            )

        # Function should complete without error (column normalized to None)


class TestGetLineagePathsBetween:
    """Tests for get_lineage_paths_between function."""

    @pytest.fixture
    def sample_path_response(self):
        """Sample lineage path response."""
        return {
            "searchAcrossLineage": {
                "searchResults": [
                    {
                        "entity": {
                            "urn": "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.target,PROD)"
                        },
                        "paths": [
                            {
                                "path": [
                                    {
                                        "urn": "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.source,PROD)",
                                        "type": "DATASET",
                                    },
                                    {"urn": "urn:li:query:transform1", "type": "QUERY"},
                                    {
                                        "urn": "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.target,PROD)",
                                        "type": "DATASET",
                                    },
                                ]
                            }
                        ],
                    }
                ]
            }
        }

    @pytest.mark.xfail(
        reason="Requires complex mocking of _find_lineage_path internal calls"
    )
    def test_dataset_level_path_downstream(self, mock_client, sample_path_response):
        """Test finding dataset-level path downstream."""
        mock_client._graph.execute_graphql.return_value = sample_path_response

        with DataHubContext(mock_client):
            result = get_lineage_paths_between(
                source_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.source,PROD)",
                target_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.target,PROD)",
                direction="downstream",
            )

        assert "paths" in result
        assert "pathCount" in result
        assert result["pathCount"] > 0
        assert result["source"]["urn"]
        assert result["target"]["urn"]

    @pytest.mark.xfail(
        reason="Requires complex mocking of URN parsing and _find_lineage_path"
    )
    def test_column_level_path(self, mock_client):
        """Test finding column-level path."""
        mock_client._graph.execute_graphql.return_value = {
            "searchAcrossLineage": {
                "searchResults": [
                    {
                        "entity": {"urn": "urn:li:dataset:target"},
                        "paths": [
                            {
                                "path": [
                                    {
                                        "urn": "urn:li:schemaField:(urn:li:dataset:source,user_id)",
                                        "type": "SCHEMA_FIELD",
                                    },
                                    {
                                        "urn": "urn:li:schemaField:(urn:li:dataset:target,customer_id)",
                                        "type": "SCHEMA_FIELD",
                                    },
                                ]
                            }
                        ],
                    }
                ]
            }
        }

        with DataHubContext(mock_client):
            result = get_lineage_paths_between(
                source_urn="urn:li:dataset:source",
                target_urn="urn:li:dataset:target",
                source_column="user_id",
                target_column="customer_id",
                direction="downstream",
            )

        assert result["metadata"]["pathType"] == "column-level"
        assert result["source"]["column"] == "user_id"
        assert result["target"]["column"] == "customer_id"

    def test_auto_discover_direction(self, mock_client, sample_path_response):
        """Test auto-discovery of lineage direction."""
        mock_client._graph.execute_graphql.return_value = sample_path_response

        with DataHubContext(mock_client):
            result = get_lineage_paths_between(
                source_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.source,PROD)",
                target_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.target,PROD)",
                direction=None,  # Auto-discover
            )

        assert "direction" in result["metadata"]
        assert "auto-discovered" in result["metadata"]["direction"]

    def test_path_not_found_raises_error(self, mock_client):
        """Test that no path found raises ItemNotFoundError."""
        mock_client._graph.execute_graphql.return_value = {
            "searchAcrossLineage": {"searchResults": []}
        }

        with pytest.raises(ItemNotFoundError, match="No lineage"):
            with DataHubContext(mock_client):
                get_lineage_paths_between(
                    source_urn="urn:li:dataset:source",
                    target_urn="urn:li:dataset:target",
                    direction="downstream",
                )

    def test_column_mismatch_raises_error(self, mock_client):
        """Test that mismatched column parameters raise ValueError."""
        with pytest.raises(
            ValueError, match="Both source_column and target_column must be provided"
        ):
            with DataHubContext(mock_client):
                get_lineage_paths_between(
                    source_urn="urn:li:dataset:source",
                    target_urn="urn:li:dataset:target",
                    source_column="user_id",
                    target_column=None,  # Mismatch
                )

    @pytest.mark.xfail(
        reason="Requires complex mocking of _find_lineage_path internal calls"
    )
    def test_query_entities_in_path(self, mock_client, sample_path_response):
        """Test that QUERY entities in path trigger metadata note."""
        mock_client._graph.execute_graphql.return_value = sample_path_response

        with DataHubContext(mock_client):
            result = get_lineage_paths_between(
                source_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.source,PROD)",
                target_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.target,PROD)",
                direction="downstream",
            )

        # Should have metadata about query entities
        assert "queryEntities" in result["metadata"]["fields"]["paths"]
