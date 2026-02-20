"""Unit tests for ColumnLineageBuilder service."""

from unittest.mock import Mock

import pytest

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.source.snowplow.dependencies import IngestionState
from datahub.ingestion.source.snowplow.services.column_lineage_builder import (
    ColumnLineageBuilder,
)
from datahub.ingestion.source.snowplow.snowplow_config import (
    SnowplowBDPConnectionConfig,
    SnowplowSourceConfig,
)
from datahub.metadata.schema_classes import (
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    SchemaMetadataClass,
    StringTypeClass,
    UpstreamLineageClass,
)


class TestColumnLineageBuilder:
    """Tests for ColumnLineageBuilder service."""

    @pytest.fixture
    def config(self):
        """Create test config."""
        return SnowplowSourceConfig(
            bdp_connection=SnowplowBDPConnectionConfig(
                organization_id="test-org",
                api_key_id="test-key-id",
                api_key="test-secret",
            ),
            platform_instance="test-instance",
            env="PROD",
        )

    @pytest.fixture
    def mock_urn_factory(self):
        """Create mock URN factory."""
        return Mock()

    @pytest.fixture
    def state_with_warehouse(self):
        """Create state with warehouse table URN configured."""
        state = IngestionState()
        state.warehouse_table_urn = "urn:li:dataset:(urn:li:dataPlatform:snowflake,warehouse.atomic.events,PROD)"
        state.parsed_events_urn = (
            "urn:li:dataset:(urn:li:dataPlatform:snowplow,parsed_events,PROD)"
        )
        return state

    @pytest.fixture
    def state_without_warehouse(self):
        """Create state without warehouse table URN."""
        return IngestionState()

    @pytest.fixture
    def sample_schema_metadata(self):
        """Create sample schema metadata with fields."""
        return SchemaMetadataClass(
            schemaName="test_schema",
            platform="urn:li:dataPlatform:snowplow",
            version=0,
            hash="",
            platformSchema=Mock(),
            fields=[
                SchemaFieldClass(
                    fieldPath="user_id",
                    type=SchemaFieldDataTypeClass(type=StringTypeClass()),
                    nativeDataType="string",
                ),
                SchemaFieldClass(
                    fieldPath="email",
                    type=SchemaFieldDataTypeClass(type=StringTypeClass()),
                    nativeDataType="string",
                ),
                SchemaFieldClass(
                    fieldPath="created_at",
                    type=SchemaFieldDataTypeClass(type=StringTypeClass()),
                    nativeDataType="string",
                ),
            ],
        )

    def test_get_warehouse_table_urn_returns_configured_urn(
        self, config, mock_urn_factory, state_with_warehouse
    ):
        """Test that get_warehouse_table_urn returns the configured URN."""
        builder = ColumnLineageBuilder(
            config=config,
            urn_factory=mock_urn_factory,
            state=state_with_warehouse,
        )

        result = builder.get_warehouse_table_urn()

        assert result == state_with_warehouse.warehouse_table_urn

    def test_get_warehouse_table_urn_returns_none_when_not_configured(
        self, config, mock_urn_factory, state_without_warehouse
    ):
        """Test that get_warehouse_table_urn returns None when not configured."""
        builder = ColumnLineageBuilder(
            config=config,
            urn_factory=mock_urn_factory,
            state=state_without_warehouse,
        )

        result = builder.get_warehouse_table_urn()

        assert result is None


class TestColumnLineageBuilderEmitColumnLineage:
    """Tests for emit_column_lineage method."""

    @pytest.fixture
    def config(self):
        """Create test config."""
        return SnowplowSourceConfig(
            bdp_connection=SnowplowBDPConnectionConfig(
                organization_id="test-org",
                api_key_id="test-key-id",
                api_key="test-secret",
            ),
            platform_instance="test-instance",
            env="PROD",
        )

    @pytest.fixture
    def mock_urn_factory(self):
        """Create mock URN factory."""
        return Mock()

    @pytest.fixture
    def state_with_warehouse(self):
        """Create state with all required URNs configured."""
        state = IngestionState()
        state.warehouse_table_urn = "urn:li:dataset:(urn:li:dataPlatform:snowflake,warehouse.atomic.events,PROD)"
        state.parsed_events_urn = (
            "urn:li:dataset:(urn:li:dataPlatform:snowplow,parsed_events,PROD)"
        )
        return state

    @pytest.fixture
    def sample_schema_metadata(self):
        """Create sample schema metadata with fields."""
        return SchemaMetadataClass(
            schemaName="test_schema",
            platform="urn:li:dataPlatform:snowplow",
            version=0,
            hash="",
            platformSchema=Mock(),
            fields=[
                SchemaFieldClass(
                    fieldPath="user_id",
                    type=SchemaFieldDataTypeClass(type=StringTypeClass()),
                    nativeDataType="string",
                ),
                SchemaFieldClass(
                    fieldPath="email",
                    type=SchemaFieldDataTypeClass(type=StringTypeClass()),
                    nativeDataType="string",
                ),
            ],
        )

    def test_emits_upstream_lineage_when_warehouse_configured(
        self, config, mock_urn_factory, state_with_warehouse, sample_schema_metadata
    ):
        """Test that upstream lineage is emitted when warehouse is configured."""
        builder = ColumnLineageBuilder(
            config=config,
            urn_factory=mock_urn_factory,
            state=state_with_warehouse,
        )

        workunits = list(
            builder.emit_column_lineage(
                dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowplow,com.acme.checkout,PROD)",
                vendor="com.acme",
                name="checkout",
                version="1-0-0",
                schema_metadata=sample_schema_metadata,
            )
        )

        assert len(workunits) == 1
        assert hasattr(workunits[0].metadata, "aspect")
        assert isinstance(workunits[0].metadata.aspect, UpstreamLineageClass)

    def test_emits_nothing_when_warehouse_not_configured(
        self, config, mock_urn_factory, sample_schema_metadata
    ):
        """Test that nothing is emitted when warehouse is not configured."""
        state = IngestionState()  # No warehouse URN
        builder = ColumnLineageBuilder(
            config=config,
            urn_factory=mock_urn_factory,
            state=state,
        )

        workunits = list(
            builder.emit_column_lineage(
                dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowplow,com.acme.checkout,PROD)",
                vendor="com.acme",
                name="checkout",
                version="1-0-0",
                schema_metadata=sample_schema_metadata,
            )
        )

        assert len(workunits) == 0

    def test_emits_nothing_when_parsed_events_not_configured(
        self, config, mock_urn_factory, sample_schema_metadata
    ):
        """Test that nothing is emitted when parsed events URN is not configured."""
        state = IngestionState()
        state.warehouse_table_urn = "urn:li:dataset:(urn:li:dataPlatform:snowflake,warehouse.atomic.events,PROD)"
        # parsed_events_urn is None

        builder = ColumnLineageBuilder(
            config=config,
            urn_factory=mock_urn_factory,
            state=state,
        )

        workunits = list(
            builder.emit_column_lineage(
                dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowplow,com.acme.checkout,PROD)",
                vendor="com.acme",
                name="checkout",
                version="1-0-0",
                schema_metadata=sample_schema_metadata,
            )
        )

        assert len(workunits) == 0

    def test_emits_nothing_when_schema_has_no_fields(
        self, config, mock_urn_factory, state_with_warehouse
    ):
        """Test that nothing is emitted when schema has no fields."""
        builder = ColumnLineageBuilder(
            config=config,
            urn_factory=mock_urn_factory,
            state=state_with_warehouse,
        )

        empty_schema = SchemaMetadataClass(
            schemaName="empty_schema",
            platform="urn:li:dataPlatform:snowplow",
            version=0,
            hash="",
            platformSchema=Mock(),
            fields=[],
        )

        workunits = list(
            builder.emit_column_lineage(
                dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowplow,com.acme.checkout,PROD)",
                vendor="com.acme",
                name="checkout",
                version="1-0-0",
                schema_metadata=empty_schema,
            )
        )

        assert len(workunits) == 0

    def test_fine_grained_lineage_maps_all_fields_to_variant_column(
        self, config, mock_urn_factory, state_with_warehouse, sample_schema_metadata
    ):
        """Test that fine-grained lineage maps all Iglu fields to VARIANT column."""
        builder = ColumnLineageBuilder(
            config=config,
            urn_factory=mock_urn_factory,
            state=state_with_warehouse,
        )

        workunits = list(
            builder.emit_column_lineage(
                dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowplow,com.acme.checkout,PROD)",
                vendor="com.acme",
                name="checkout",
                version="1-0-0",
                schema_metadata=sample_schema_metadata,
            )
        )

        assert isinstance(workunits[0].metadata, MetadataChangeProposalWrapper)
        assert isinstance(workunits[0].metadata.aspect, UpstreamLineageClass)
        upstream_lineage = workunits[0].metadata.aspect
        assert upstream_lineage.fineGrainedLineages is not None
        assert len(upstream_lineage.fineGrainedLineages) == 1

        fine_grained = upstream_lineage.fineGrainedLineages[0]
        # Should have 2 upstream fields (user_id, email)
        assert fine_grained.upstreams is not None
        assert len(fine_grained.upstreams) == 2
        # Should have 1 downstream (VARIANT column)
        assert fine_grained.downstreams is not None
        assert len(fine_grained.downstreams) == 1

    def test_lineage_attached_to_warehouse_table(
        self, config, mock_urn_factory, state_with_warehouse, sample_schema_metadata
    ):
        """Test that lineage is attached to the warehouse table (downstream)."""
        builder = ColumnLineageBuilder(
            config=config,
            urn_factory=mock_urn_factory,
            state=state_with_warehouse,
        )

        workunits = list(
            builder.emit_column_lineage(
                dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowplow,com.acme.checkout,PROD)",
                vendor="com.acme",
                name="checkout",
                version="1-0-0",
                schema_metadata=sample_schema_metadata,
            )
        )

        # Lineage should be attached to warehouse table
        assert isinstance(workunits[0].metadata, MetadataChangeProposalWrapper)
        assert (
            workunits[0].metadata.entityUrn == state_with_warehouse.warehouse_table_urn
        )


class TestMapSchemaToSnowflakeColumn:
    """Tests for map_schema_to_snowflake_column method."""

    @pytest.fixture
    def builder(self):
        """Create builder for testing."""
        config = SnowplowSourceConfig(
            bdp_connection=SnowplowBDPConnectionConfig(
                organization_id="test-org",
                api_key_id="test-key-id",
                api_key="test-secret",
            ),
        )
        return ColumnLineageBuilder(
            config=config,
            urn_factory=Mock(),
            state=IngestionState(),
        )

    def test_simple_vendor_and_name(self, builder):
        """Test column mapping with simple vendor and name."""
        result = builder.map_schema_to_snowflake_column(
            vendor="com.acme",
            name="checkout",
            version="1-0-0",
        )

        assert result == "contexts_com_acme_checkout_1"

    def test_vendor_with_dots_converted_to_underscores(self, builder):
        """Test that vendor dots are converted to underscores."""
        result = builder.map_schema_to_snowflake_column(
            vendor="com.snowplowanalytics.snowplow",
            name="web_page",
            version="1-0-0",
        )

        assert result == "contexts_com_snowplowanalytics_snowplow_web_page_1"

    def test_only_major_version_used_with_dash_format(self, builder):
        """Test that only major version is used for dash-separated versions."""
        result = builder.map_schema_to_snowflake_column(
            vendor="com.acme",
            name="checkout",
            version="2-1-3",
        )

        assert result == "contexts_com_acme_checkout_2"

    def test_only_major_version_used_with_dot_format(self, builder):
        """Test that only major version is used for dot-separated versions."""
        result = builder.map_schema_to_snowflake_column(
            vendor="com.acme",
            name="checkout",
            version="3.2.1",
        )

        assert result == "contexts_com_acme_checkout_3"

    def test_name_with_underscores_preserved(self, builder):
        """Test that underscores in name are preserved."""
        result = builder.map_schema_to_snowflake_column(
            vendor="com.acme",
            name="checkout_started",
            version="1-0-0",
        )

        assert result == "contexts_com_acme_checkout_started_1"

    def test_vendor_with_slashes_converted(self, builder):
        """Test that slashes in vendor are converted to underscores."""
        result = builder.map_schema_to_snowflake_column(
            vendor="com/acme/internal",
            name="checkout",
            version="1-0-0",
        )

        assert result == "contexts_com_acme_internal_checkout_1"

    def test_real_snowplow_example(self, builder):
        """Test with real Snowplow schema example."""
        result = builder.map_schema_to_snowflake_column(
            vendor="com.snowplowanalytics.snowplow",
            name="link_click",
            version="1-0-1",
        )

        assert result == "contexts_com_snowplowanalytics_snowplow_link_click_1"
