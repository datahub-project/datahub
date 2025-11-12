"""
Test retry functionality for the lineage features source.
"""

from unittest.mock import Mock, patch

import pytest
from opensearchpy.exceptions import (
    ConnectionError as OpenSearchConnectionError,
    ConnectionTimeout,
)

from acryl_datahub_cloud.lineage_features.source import (
    DataHubLineageFeaturesSource,
    LineageFeaturesSourceConfig,
)
from datahub.ingestion.api.common import PipelineContext


class TestLineageFeaturesRetry:
    """Test retry functionality for lineage features source."""

    def test_config_validation(self) -> None:
        """Test that config validation works correctly."""
        # Valid config
        config = LineageFeaturesSourceConfig(
            max_retries=3, retry_delay_seconds=5, retry_backoff_multiplier=2.0
        )
        assert config.max_retries == 3
        assert config.retry_delay_seconds == 5
        assert config.retry_backoff_multiplier == 2.0

        # Invalid configs should raise ValueError
        with pytest.raises(ValueError, match="max_retries must be at least 1"):
            LineageFeaturesSourceConfig(max_retries=0)

        with pytest.raises(ValueError, match="retry_delay_seconds must be at least 1"):
            LineageFeaturesSourceConfig(retry_delay_seconds=0)

        with pytest.raises(
            ValueError, match="retry_backoff_multiplier must be at least 1.0"
        ):
            LineageFeaturesSourceConfig(retry_backoff_multiplier=0.5)

    @patch("acryl_datahub_cloud.lineage_features.source.OpenSearch")
    def test_create_opensearch_client_retry(self, mock_opensearch: Mock) -> None:
        """Test that OpenSearch client creation retries on failure."""
        # Mock OpenSearch to fail twice then succeed
        mock_client = Mock()
        mock_opensearch.side_effect = [
            ConnectionTimeout("Connection timeout", None, None),
            OpenSearchConnectionError("Connection error", None, None),
            mock_client,
        ]

        config = LineageFeaturesSourceConfig(max_retries=3)
        ctx = Mock(spec=PipelineContext)
        source = DataHubLineageFeaturesSource(config, ctx)

        # Should succeed after retries
        result = source._create_opensearch_client_with_retry()
        assert result == mock_client
        assert mock_opensearch.call_count == 3

    @patch("acryl_datahub_cloud.lineage_features.source.OpenSearch")
    def test_create_pit_retry(self, mock_opensearch: Mock) -> None:
        """Test that PIT creation retries on failure."""
        mock_client = Mock()
        mock_opensearch.return_value = mock_client

        # Mock PIT creation to fail twice then succeed
        mock_client.create_pit.side_effect = [
            ConnectionTimeout("Connection timeout", None, None),
            OpenSearchConnectionError("Connection error", None, None),
            {"pit_id": "test_pit_id"},
        ]

        config = LineageFeaturesSourceConfig(max_retries=3)
        ctx = Mock(spec=PipelineContext)
        source = DataHubLineageFeaturesSource(config, ctx)

        # Should succeed after retries
        result = source._create_pit_with_retry(mock_client, "test_index")
        assert result == "test_pit_id"
        assert mock_client.create_pit.call_count == 3

    @patch("acryl_datahub_cloud.lineage_features.source.OpenSearch")
    def test_search_retry(self, mock_opensearch: Mock) -> None:
        """Test that search operations retry on failure."""
        mock_client = Mock()
        mock_opensearch.return_value = mock_client

        # Mock search to fail twice then succeed
        mock_client.search.side_effect = [
            ConnectionTimeout("Connection timeout", None, None),
            OpenSearchConnectionError("Connection error", None, None),
            {"hits": {"hits": []}},
        ]

        config = LineageFeaturesSourceConfig(max_retries=3)
        ctx = Mock(spec=PipelineContext)
        source = DataHubLineageFeaturesSource(config, ctx)

        # Should succeed after retries
        result = source._search_with_retry(mock_client, {"query": "test"}, 100)
        assert result == {"hits": {"hits": []}}
        assert mock_client.search.call_count == 3

    @patch("acryl_datahub_cloud.lineage_features.source.OpenSearch")
    def test_delete_pit_retry(self, mock_opensearch: Mock) -> None:
        """Test that PIT deletion retries on failure."""
        mock_client = Mock()
        mock_opensearch.return_value = mock_client

        # Mock PIT deletion to fail twice then succeed
        mock_client.delete_pit.side_effect = [
            ConnectionTimeout("Connection timeout", None, None),
            OpenSearchConnectionError("Connection error", None, None),
            None,  # Success
        ]

        config = LineageFeaturesSourceConfig(max_retries=3)
        ctx = Mock(spec=PipelineContext)
        source = DataHubLineageFeaturesSource(config, ctx)

        # Should succeed after retries
        source._delete_pit_with_retry(mock_client, "test_pit_id")
        assert mock_client.delete_pit.call_count == 3

    @patch("acryl_datahub_cloud.lineage_features.source.OpenSearch")
    def test_max_retries_exceeded(self, mock_opensearch: Mock) -> None:
        """Test that exceptions are raised after max retries exceeded."""
        mock_client = Mock()
        mock_opensearch.return_value = mock_client

        # Mock to always fail
        mock_client.create_pit.side_effect = ConnectionTimeout(
            "Connection timeout", None, None
        )

        config = LineageFeaturesSourceConfig(max_retries=2)
        ctx = Mock(spec=PipelineContext)
        source = DataHubLineageFeaturesSource(config, ctx)

        # Should raise exception after max retries
        with pytest.raises(ConnectionTimeout):
            source._create_pit_with_retry(mock_client, "test_index")

        assert mock_client.create_pit.call_count == 2

    @patch("acryl_datahub_cloud.lineage_features.source.OpenSearch")
    def test_get_workunits_query_structure(self, mock_opensearch: Mock) -> None:
        """Test that get_workunits constructs query without sort field."""
        mock_client = Mock()
        mock_opensearch.return_value = mock_client

        # Mock PIT creation
        mock_client.create_pit.return_value = {"pit_id": "test_pit_id"}

        # Mock search to return empty results (completes on first call)
        mock_client.search.return_value = {"hits": {"hits": []}}

        # Mock PIT deletion
        mock_client.delete_pit.return_value = None

        # Mock context and graph
        mock_graph = Mock()
        mock_graph.get_urns_by_filter.return_value = []

        mock_ctx = Mock(spec=PipelineContext)
        mock_ctx.require_graph.return_value = mock_graph

        config = LineageFeaturesSourceConfig(max_retries=1)
        source = DataHubLineageFeaturesSource(config, mock_ctx)

        # Execute get_workunits
        list(source.get_workunits())

        # Verify search was called
        assert mock_client.search.call_count == 1

        # Get the query that was passed to search
        search_call_args = mock_client.search.call_args
        query = search_call_args[1]["body"]

        # Verify query structure does not contain 'sort' field
        assert "sort" not in query, "Query should not contain 'sort' field"

        # Verify query has required fields
        assert "query" in query
        assert "pit" in query
        assert query["pit"]["id"] == "test_pit_id"

        # Verify query has correct relationship types
        assert "bool" in query["query"]
        assert "should" in query["query"]["bool"]
        relationship_types = [
            term["term"]["relationshipType"]
            for term in query["query"]["bool"]["should"]
            if "term" in term and "relationshipType" in term["term"]
        ]
        assert "Consumes" in relationship_types
        assert "DownstreamOf" in relationship_types

        # Verify PIT was cleaned up
        mock_client.delete_pit.assert_called_once()
