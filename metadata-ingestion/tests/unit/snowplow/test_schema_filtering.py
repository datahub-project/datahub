"""Unit tests for Snowplow schema filtering logic."""

from unittest.mock import Mock, patch

import pytest

from datahub.ingestion.source.snowplow.snowplow import SnowplowSource
from datahub.ingestion.source.snowplow.snowplow_models import (
    DataStructure,
    SchemaMetadata,
)


class TestSchemaFiltering:
    """Test schema filtering using schema_pattern configuration."""

    @pytest.fixture
    def source_with_allow_pattern(self):
        """Create source with allow pattern configured."""
        from datahub.configuration.common import AllowDenyPattern
        from datahub.ingestion.source.snowplow.snowplow_config import (
            SnowplowBDPConnectionConfig,
            SnowplowSourceConfig,
        )

        config = SnowplowSourceConfig(
            bdp_connection=SnowplowBDPConnectionConfig(
                organization_id="test-org",
                api_key_id="test-key",
                api_key="test-secret",
            ),
            schema_pattern=AllowDenyPattern(allow=["com.acme/.*"], deny=[]),
        )

        mock_ctx = Mock()
        mock_ctx.graph = None
        mock_ctx.pipeline_name = None

        with patch("datahub.ingestion.source.snowplow.snowplow.SnowplowBDPClient"):
            source = SnowplowSource(config=config, ctx=mock_ctx)
            return source

    @pytest.fixture
    def source_with_deny_pattern(self):
        """Create source with deny pattern configured."""
        from datahub.configuration.common import AllowDenyPattern
        from datahub.ingestion.source.snowplow.snowplow_config import (
            SnowplowBDPConnectionConfig,
            SnowplowSourceConfig,
        )

        config = SnowplowSourceConfig(
            bdp_connection=SnowplowBDPConnectionConfig(
                organization_id="test-org",
                api_key_id="test-key",
                api_key="test-secret",
            ),
            schema_pattern=AllowDenyPattern(deny=[r".*/test_.*"]),
        )

        mock_ctx = Mock()
        mock_ctx.graph = None
        mock_ctx.pipeline_name = None

        with patch("datahub.ingestion.source.snowplow.snowplow.SnowplowBDPClient"):
            source = SnowplowSource(config=config, ctx=mock_ctx)
            return source

    @pytest.fixture
    def mock_data_structures(self):
        """Create mock data structures for testing."""
        return [
            DataStructure(
                vendor="com.acme",
                name="checkout_started",
                hash="hash1",
                meta=SchemaMetadata(schema_type="event"),
                deployments=[],
            ),
            DataStructure(
                vendor="com.acme",
                name="page_view",
                hash="hash2",
                meta=SchemaMetadata(schema_type="event"),
                deployments=[],
            ),
            DataStructure(
                vendor="com.other",
                name="user_action",
                hash="hash3",
                meta=SchemaMetadata(schema_type="event"),
                deployments=[],
            ),
            DataStructure(
                vendor="com.acme",
                name="test_event",
                hash="hash4",
                meta=SchemaMetadata(schema_type="event"),
                deployments=[],
            ),
        ]

    def test_filter_by_allow_pattern(
        self, source_with_allow_pattern, mock_data_structures
    ):
        """Test filtering with allow pattern - only matching schemas included."""
        source = source_with_allow_pattern

        # Mock get_data_structures to return our test data
        with patch.object(
            source.bdp_client, "get_data_structures", return_value=mock_data_structures
        ):
            filtered = source._get_data_structures_filtered()

        # Should only include com.acme schemas (3 out of 4)
        assert len(filtered) == 3
        assert all(ds.vendor == "com.acme" for ds in filtered)

        # Verify com.other was filtered out
        vendors = {ds.vendor for ds in filtered}
        assert "com.other" not in vendors

    def test_filter_by_deny_pattern(
        self, source_with_deny_pattern, mock_data_structures
    ):
        """Test filtering with deny pattern - matching schemas excluded."""
        source = source_with_deny_pattern

        # Mock get_data_structures to return our test data
        with patch.object(
            source.bdp_client, "get_data_structures", return_value=mock_data_structures
        ):
            filtered = source._get_data_structures_filtered()

        # Should exclude test_event (3 remaining)
        assert len(filtered) == 3

        # Verify test_event was filtered out
        names = {ds.name for ds in filtered}
        assert "test_event" not in names

    def test_allow_all_when_no_pattern(self, mock_data_structures):
        """Test that all schemas pass when no pattern configured."""
        from datahub.ingestion.source.snowplow.snowplow_config import (
            SnowplowBDPConnectionConfig,
            SnowplowSourceConfig,
        )

        config = SnowplowSourceConfig(
            bdp_connection=SnowplowBDPConnectionConfig(
                organization_id="test-org",
                api_key_id="test-key",
                api_key="test-secret",
            )
        )

        mock_ctx = Mock()
        mock_ctx.graph = None
        mock_ctx.pipeline_name = None

        with patch("datahub.ingestion.source.snowplow.snowplow.SnowplowBDPClient"):
            source = SnowplowSource(config=config, ctx=mock_ctx)

        # Mock get_data_structures
        with patch.object(
            source.bdp_client, "get_data_structures", return_value=mock_data_structures
        ):
            filtered = source._get_data_structures_filtered()

        # Should include all schemas
        assert len(filtered) == len(mock_data_structures)

    def test_filter_reports_correctly(
        self, source_with_allow_pattern, mock_data_structures
    ):
        """Test that filtered schemas are reported correctly."""
        source = source_with_allow_pattern

        # Mock get_data_structures
        with patch.object(
            source.bdp_client, "get_data_structures", return_value=mock_data_structures
        ):
            source._get_data_structures_filtered()

        # One schema should be filtered (com.other/user_action)
        assert source.report.num_event_schemas_filtered == 1
        assert "com.other/user_action" in source.report.filtered_schemas

    def test_complex_allow_deny_pattern(self, mock_data_structures):
        """Test complex pattern with both allow and deny."""
        from datahub.configuration.common import AllowDenyPattern
        from datahub.ingestion.source.snowplow.snowplow_config import (
            SnowplowBDPConnectionConfig,
            SnowplowSourceConfig,
        )

        # Allow com.acme but deny test_*
        config = SnowplowSourceConfig(
            bdp_connection=SnowplowBDPConnectionConfig(
                organization_id="test-org",
                api_key_id="test-key",
                api_key="test-secret",
            ),
            schema_pattern=AllowDenyPattern(allow=["com.acme/.*"], deny=[".*test_.*"]),
        )

        mock_ctx = Mock()
        mock_ctx.graph = None
        mock_ctx.pipeline_name = None

        with patch("datahub.ingestion.source.snowplow.snowplow.SnowplowBDPClient"):
            source = SnowplowSource(config=config, ctx=mock_ctx)

        with patch.object(
            source.bdp_client, "get_data_structures", return_value=mock_data_structures
        ):
            filtered = source._get_data_structures_filtered()

        # Should include only com.acme schemas excluding test_event (2 schemas)
        assert len(filtered) == 2
        names = {ds.name for ds in filtered}
        assert names == {"checkout_started", "page_view"}

    def test_filter_with_special_regex_chars(self):
        """Test filtering with regex special characters."""
        from datahub.configuration.common import AllowDenyPattern
        from datahub.ingestion.source.snowplow.snowplow_config import (
            SnowplowBDPConnectionConfig,
            SnowplowSourceConfig,
        )

        # Test escaping dots in vendor names
        config = SnowplowSourceConfig(
            bdp_connection=SnowplowBDPConnectionConfig(
                organization_id="test-org",
                api_key_id="test-key",
                api_key="test-secret",
            ),
            schema_pattern=AllowDenyPattern(allow=["com\\.acme/.*"]),
        )

        mock_ctx = Mock()
        mock_ctx.graph = None
        mock_ctx.pipeline_name = None

        with patch("datahub.ingestion.source.snowplow.snowplow.SnowplowBDPClient"):
            source = SnowplowSource(config=config, ctx=mock_ctx)

        test_structures = [
            DataStructure(
                vendor="com.acme",
                name="event1",
                hash="hash1",
                meta=SchemaMetadata(schema_type="event"),
                deployments=[],
            ),
            DataStructure(
                vendor="comXacme",  # Should NOT match escaped dot
                name="event2",
                hash="hash2",
                meta=SchemaMetadata(schema_type="event"),
                deployments=[],
            ),
        ]

        with patch.object(
            source.bdp_client, "get_data_structures", return_value=test_structures
        ):
            filtered = source._get_data_structures_filtered()

        # Only com.acme should match (escaped dot)
        assert len(filtered) == 1
        assert filtered[0].vendor == "com.acme"
