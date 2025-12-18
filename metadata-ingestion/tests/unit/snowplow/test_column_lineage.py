"""Unit tests for Snowplow column-level lineage extraction."""

from unittest.mock import Mock, patch

import pytest

from datahub.ingestion.source.snowplow.snowplow import SnowplowSource
from datahub.metadata.schema_classes import (
    FineGrainedLineageClass,
    SchemaFieldDataTypeClass,
    SchemaFieldClass,
    SchemaMetadataClass,
    StringTypeClass,
)


class TestColumnLineageMapping:
    """Test column lineage mapping from Iglu schemas to Snowflake VARIANT columns."""

    @pytest.fixture
    def source(self):
        """Create a SnowplowSource instance with mocked dependencies."""
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

        # Create a mock context
        mock_ctx = Mock()
        mock_ctx.graph = None
        mock_ctx.pipeline_name = None

        # Mock the BDP client creation to avoid authentication
        with patch(
            "datahub.ingestion.source.snowplow.snowplow.SnowplowBDPClient"
        ):
            source = SnowplowSource(config=config, ctx=mock_ctx)
            return source

    def test_map_schema_to_snowflake_column_basic(self, source):
        """Test basic vendor/name/version to Snowflake column name mapping."""
        # Test basic mapping
        column_name = source._map_schema_to_snowflake_column(
            vendor="com.acme", name="checkout_started", version="1-0-0"
        )

        assert column_name == "contexts_com_acme_checkout_started_1"

    def test_map_schema_to_snowflake_column_complex_vendor(self, source):
        """Test mapping with complex vendor names containing dots."""
        # Test vendor with multiple dots
        column_name = source._map_schema_to_snowflake_column(
            vendor="com.company.subdomain",
            name="user_action",
            version="2-1-0",
        )

        assert column_name == "contexts_com_company_subdomain_user_action_2"

    def test_map_schema_to_snowflake_column_version_formats(self, source):
        """Test handling of different version formats."""
        # Test hyphenated version (1-0-0)
        column_name1 = source._map_schema_to_snowflake_column(
            vendor="com.test", name="event", version="1-0-0"
        )
        assert column_name1 == "contexts_com_test_event_1"

        # Test dotted version (1.0.0)
        column_name2 = source._map_schema_to_snowflake_column(
            vendor="com.test", name="event", version="1.0.0"
        )
        assert column_name2 == "contexts_com_test_event_1"

        # Test major version only
        column_name3 = source._map_schema_to_snowflake_column(
            vendor="com.test", name="event", version="5"
        )
        assert column_name3 == "contexts_com_test_event_5"

    def test_map_schema_to_snowflake_column_name_with_dots(self, source):
        """Test schema names containing dots."""
        # Test name with dots
        column_name = source._map_schema_to_snowflake_column(
            vendor="com.test", name="page.view.event", version="1-0-0"
        )

        assert column_name == "contexts_com_test_page_view_event_1"

    def test_map_schema_to_snowflake_column_special_chars(self, source):
        """Test handling of special characters in vendor/name."""
        # Test forward slashes (should be replaced with underscores)
        column_name = source._map_schema_to_snowflake_column(
            vendor="com/test", name="event/action", version="1-0-0"
        )

        assert column_name == "contexts_com_test_event_action_1"


class TestColumnLineageExtraction:
    """Test column lineage extraction logic."""

    @pytest.fixture
    def source_with_warehouse(self):
        """Create a SnowplowSource for testing column lineage."""
        from datahub.ingestion.source.snowplow.snowplow_config import (
            SnowplowBDPConnectionConfig,
            SnowplowSourceConfig,
        )

        # Create config with BDP connection (column lineage is used for enrichments)
        config = SnowplowSourceConfig(
            bdp_connection=SnowplowBDPConnectionConfig(
                organization_id="test-org",
                api_key_id="test-key",
                api_key="test-secret",
            ),
        )

        # Create mock context
        mock_ctx = Mock()
        mock_ctx.graph = None
        mock_ctx.pipeline_name = None

        # Mock the BDP client creation to avoid authentication
        with patch(
            "datahub.ingestion.source.snowplow.snowplow.SnowplowBDPClient"
        ):
            source = SnowplowSource(config=config, ctx=mock_ctx)
            return source

    def test_emit_column_lineage_creates_fine_grained_lineage(self, source_with_warehouse):
        """Test that _emit_column_lineage creates FineGrainedLineage with correct structure."""
        source = source_with_warehouse

        # Create schema metadata with fields
        schema_metadata = SchemaMetadataClass(
            schemaName="com.test.event",
            platform="urn:li:dataPlatform:snowplow",
            version=0,
            hash="",
            platformSchema=None,
            fields=[
                SchemaFieldClass(
                    fieldPath="field1",
                    nativeDataType="string",
                    type=SchemaFieldDataTypeClass(type=StringTypeClass()),
                ),
                SchemaFieldClass(
                    fieldPath="field2",
                    nativeDataType="string",
                    type=SchemaFieldDataTypeClass(type=StringTypeClass()),
                ),
            ],
        )

        # Mock _get_warehouse_table_urn
        source._get_warehouse_table_urn = lambda: "urn:li:dataset:(urn:li:dataPlatform:snowflake,test_db.test_schema.events,PROD)"

        # Set _parsed_events_urn (required for column lineage to be emitted)
        source.state.parsed_events_urn = "urn:li:dataset:(urn:li:dataPlatform:snowplow,event_core,PROD)"

        # Get lineage work units
        dataset_urn = "urn:li:dataset:(urn:li:dataPlatform:snowplow,com.test.event,PROD)"
        work_units = list(
            source._emit_column_lineage(
                dataset_urn=dataset_urn,
                vendor="com.test",
                name="event",
                version="1-0-0",
                schema_metadata=schema_metadata,
            )
        )

        # Should emit one lineage work unit
        assert len(work_units) == 1

        # Verify it's an upstream lineage aspect
        wu = work_units[0]
        assert wu.metadata.entityUrn == "urn:li:dataset:(urn:li:dataPlatform:snowflake,test_db.test_schema.events,PROD)"
        assert wu.metadata.aspectName == "upstreamLineage"

        # Verify fine-grained lineage structure
        upstream_lineage = wu.metadata.aspect
        assert upstream_lineage is not None
        assert len(upstream_lineage.fineGrainedLineages) == 1

        fine_grained = upstream_lineage.fineGrainedLineages[0]
        assert len(fine_grained.upstreams) == 2  # Two fields from Iglu schema
        assert len(fine_grained.downstreams) == 1  # One VARIANT column
        assert fine_grained.downstreams[0].endswith("contexts_com_test_event_1)")

    @pytest.fixture
    def source_without_warehouse(self):
        """Create a SnowplowSource without warehouse connection."""
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

        # Create mock context
        mock_ctx = Mock()
        mock_ctx.graph = None
        mock_ctx.pipeline_name = None

        # Mock the BDP client creation to avoid authentication
        with patch(
            "datahub.ingestion.source.snowplow.snowplow.SnowplowBDPClient"
        ):
            source = SnowplowSource(config=config, ctx=mock_ctx)
            return source

    def test_emit_column_lineage_no_warehouse_returns_empty(self, source_without_warehouse):
        """Test that _emit_column_lineage returns empty when no warehouse configured."""
        source = source_without_warehouse

        schema_metadata = SchemaMetadataClass(
            schemaName="com.test.event",
            platform="urn:li:dataPlatform:snowplow",
            version=0,
            hash="",
            platformSchema=None,
            fields=[],
        )

        # Should return empty when no warehouse
        work_units = list(
            source._emit_column_lineage(
                dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowplow,com.test.event,PROD)",
                vendor="com.test",
                name="event",
                version="1-0-0",
                schema_metadata=schema_metadata,
            )
        )

        assert len(work_units) == 0

    def test_emit_column_lineage_no_fields_returns_empty(self, source_with_warehouse):
        """Test that _emit_column_lineage returns empty when schema has no fields."""
        source = source_with_warehouse
        source._get_warehouse_table_urn = lambda: "urn:li:dataset:(urn:li:dataPlatform:snowflake,test_db.test_schema.events,PROD)"

        # Schema with no fields
        schema_metadata = SchemaMetadataClass(
            schemaName="com.test.event",
            platform="urn:li:dataPlatform:snowplow",
            version=0,
            hash="",
            platformSchema=None,
            fields=[],
        )

        work_units = list(
            source._emit_column_lineage(
                dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowplow,com.test.event,PROD)",
                vendor="com.test",
                name="event",
                version="1-0-0",
                schema_metadata=schema_metadata,
            )
        )

        assert len(work_units) == 0
