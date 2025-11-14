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
    def test_scroll_retry(self, mock_opensearch: Mock) -> None:
        """Test that scroll operations retry on failure."""
        mock_client = Mock()
        mock_opensearch.return_value = mock_client

        # Mock scroll to fail twice then succeed
        mock_client.scroll.side_effect = [
            ConnectionTimeout("Connection timeout", None, None),
            OpenSearchConnectionError("Connection error", None, None),
            {"hits": {"hits": []}, "_scroll_id": "test_scroll_id"},
        ]

        config = LineageFeaturesSourceConfig(max_retries=3)
        ctx = Mock(spec=PipelineContext)
        source = DataHubLineageFeaturesSource(config, ctx)

        # Should succeed after retries
        result = source._scroll_with_retry(mock_client, "test_scroll_id")
        assert result == {"hits": {"hits": []}, "_scroll_id": "test_scroll_id"}
        assert mock_client.scroll.call_count == 3

    @patch("acryl_datahub_cloud.lineage_features.source.OpenSearch")
    def test_search_retry(self, mock_opensearch: Mock) -> None:
        """Test that search operations retry on failure."""
        mock_client = Mock()
        mock_opensearch.return_value = mock_client

        # Mock search to fail twice then succeed
        mock_client.search.side_effect = [
            ConnectionTimeout("Connection timeout", None, None),
            OpenSearchConnectionError("Connection error", None, None),
            {"hits": {"hits": []}, "_scroll_id": "test_scroll_id"},
        ]

        config = LineageFeaturesSourceConfig(max_retries=3)
        ctx = Mock(spec=PipelineContext)
        source = DataHubLineageFeaturesSource(config, ctx)

        # Should succeed after retries
        result = source._search_with_retry(
            mock_client, "test_index", {"query": "test"}, 100
        )
        assert result == {"hits": {"hits": []}, "_scroll_id": "test_scroll_id"}
        assert mock_client.search.call_count == 3

    @patch("acryl_datahub_cloud.lineage_features.source.OpenSearch")
    def test_clear_scroll_retry(self, mock_opensearch: Mock) -> None:
        """Test that scroll clearing retries on failure."""
        mock_client = Mock()
        mock_opensearch.return_value = mock_client

        # Mock scroll clearing to fail twice then succeed
        mock_client.clear_scroll.side_effect = [
            ConnectionTimeout("Connection timeout", None, None),
            OpenSearchConnectionError("Connection error", None, None),
            None,  # Success
        ]

        config = LineageFeaturesSourceConfig(max_retries=3)
        ctx = Mock(spec=PipelineContext)
        source = DataHubLineageFeaturesSource(config, ctx)

        # Should succeed after retries
        source._clear_scroll_with_retry(mock_client, "test_scroll_id")
        assert mock_client.clear_scroll.call_count == 3

    @patch("acryl_datahub_cloud.lineage_features.source.OpenSearch")
    def test_max_retries_exceeded(self, mock_opensearch: Mock) -> None:
        """Test that exceptions are raised after max retries exceeded."""
        mock_client = Mock()
        mock_opensearch.return_value = mock_client

        # Mock to always fail
        mock_client.scroll.side_effect = ConnectionTimeout(
            "Connection timeout", None, None
        )

        config = LineageFeaturesSourceConfig(max_retries=2)
        ctx = Mock(spec=PipelineContext)
        source = DataHubLineageFeaturesSource(config, ctx)

        # Should raise exception after max retries
        with pytest.raises(ConnectionTimeout):
            source._scroll_with_retry(mock_client, "test_scroll_id")

        assert mock_client.scroll.call_count == 2

    @patch("acryl_datahub_cloud.lineage_features.source.OpenSearch")
    def test_get_workunits_query_structure(self, mock_opensearch: Mock) -> None:
        """Test that get_workunits constructs query and uses scroll."""
        mock_client = Mock()
        mock_opensearch.return_value = mock_client

        # Mock data node count to return 0 (single slice path)
        mock_client.nodes.info.return_value = {"nodes": {}}

        # Mock search to return empty results with scroll_id (completes on first call)
        mock_client.search.return_value = {
            "hits": {"hits": []},
            "_scroll_id": "test_scroll_id",
        }

        # Mock clear_scroll
        mock_client.clear_scroll.return_value = None

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
        query = search_call_args.kwargs["body"]

        # Verify query structure does not contain 'sort' or 'pit' field
        assert "sort" not in query, "Query should not contain 'sort' field"
        assert "pit" not in query, "Query should not contain 'pit' field"

        # Verify query has required fields
        assert "query" in query

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

        # Verify scroll parameter was passed
        search_params = search_call_args.kwargs.get("params", {})
        assert "scroll" in search_params, "Search should include scroll parameter"

        # Verify scroll was cleared
        mock_client.clear_scroll.assert_called_once_with(scroll_id="test_scroll_id")
