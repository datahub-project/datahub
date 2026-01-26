"""Unit tests for DataStructureBuilder service."""

from typing import Optional
from unittest.mock import Mock

import pytest

from datahub.ingestion.source.snowplow.dependencies import (
    IngestionState,
    ProcessorDependencies,
)
from datahub.ingestion.source.snowplow.models.snowplow_models import (
    DataStructure,
    DataStructureDeployment,
    SchemaData,
    SchemaMetadata,
    SchemaSelf,
)
from datahub.ingestion.source.snowplow.services.data_structure_builder import (
    DataStructureBuilder,
)
from datahub.ingestion.source.snowplow.snowplow_config import (
    FieldTaggingConfig,
    SnowplowBDPConnectionConfig,
    SnowplowSourceConfig,
)
from datahub.ingestion.source.snowplow.snowplow_report import SnowplowSourceReport


def make_schema_data(
    vendor: str = "com.acme",
    name: str = "event1",
    version: str = "1-0-0",
    description: str = "Test event",
    properties: Optional[dict] = None,
) -> SchemaData:
    """Helper to create valid SchemaData objects."""
    return SchemaData(
        schema_ref="http://iglucentral.com/schemas/com.snowplowanalytics.self-desc/schema/jsonschema/1-0-0#",
        type="object",
        description=description,
        properties=properties or {},
        self_descriptor=SchemaSelf(
            vendor=vendor,
            name=name,
            format="jsonschema",
            version=version,
        ),
    )


class TestDataStructureBuilder:
    """Tests for DataStructureBuilder service."""

    @pytest.fixture
    def config_with_bdp(self):
        """Create config with BDP connection."""
        return SnowplowSourceConfig(
            bdp_connection=SnowplowBDPConnectionConfig(
                organization_id="test-org-123",
                api_key_id="test-key-id",
                api_key="test-secret",
            ),
            platform_instance="test-instance",
            env="PROD",
            field_tagging=FieldTaggingConfig(enabled=False),
        )

    @pytest.fixture
    def mock_deps(self, config_with_bdp):
        """Create mock processor dependencies."""
        from datahub.ingestion.source.snowplow.builders.lineage_builder import (
            LineageBuilder,
        )
        from datahub.ingestion.source.snowplow.builders.ownership_builder import (
            OwnershipBuilder,
        )
        from datahub.ingestion.source.snowplow.builders.urn_factory import (
            SnowplowURNFactory,
        )
        from datahub.ingestion.source.snowplow.enrichment_lineage.registry import (
            EnrichmentLineageRegistry,
        )
        from datahub.ingestion.source.snowplow.services.error_handler import (
            ErrorHandler,
        )
        from datahub.ingestion.source.snowplow.services.field_tagging import FieldTagger
        from datahub.ingestion.source.snowplow.utils.cache_manager import CacheManager

        report = SnowplowSourceReport()
        urn_factory = SnowplowURNFactory(
            platform="snowplow",
            config=config_with_bdp,
        )

        return ProcessorDependencies(
            config=config_with_bdp,
            report=report,
            cache=CacheManager(),
            platform="snowplow",
            urn_factory=urn_factory,
            ownership_builder=OwnershipBuilder(
                config=config_with_bdp, user_cache={}, user_name_cache={}
            ),
            lineage_builder=LineageBuilder(),
            enrichment_lineage_registry=EnrichmentLineageRegistry(),
            error_handler=ErrorHandler(report=report),
            field_tagger=FieldTagger(config_with_bdp.field_tagging),
            bdp_client=Mock(),
        )

    @pytest.fixture
    def state(self):
        """Create fresh ingestion state."""
        return IngestionState()

    @pytest.fixture
    def mock_column_lineage_builder(self):
        """Create mock column lineage builder."""
        builder = Mock()
        builder.emit_column_lineage.return_value = iter([])
        return builder

    @pytest.fixture
    def builder(self, mock_deps, state, mock_column_lineage_builder):
        """Create DataStructureBuilder instance."""
        return DataStructureBuilder(
            deps=mock_deps,
            state=state,
            column_lineage_builder=mock_column_lineage_builder,
        )

    @pytest.fixture
    def sample_data_structure(self):
        """Create sample data structure with full metadata."""
        return DataStructure(
            hash="test-hash-123",
            vendor="com.acme",
            name="checkout_started",
            meta=SchemaMetadata(
                hidden=False,
                schema_type="event",
                custom_data={"source": "test"},
            ),
            data=make_schema_data(
                vendor="com.acme",
                name="checkout_started",
                version="1-0-0",
                description="Checkout started event",
                properties={"cart_value": {"type": "number"}},
            ),
            deployments=[
                DataStructureDeployment(
                    version="1-0-0",
                    env="PROD",
                    ts="2024-01-01T00:00:00Z",
                )
            ],
        )

    def test_process_data_structure_emits_workunits(
        self, builder, sample_data_structure
    ):
        """Test that processing a data structure emits work units."""
        workunits = list(builder.process_data_structure(sample_data_structure))

        # Should emit at least dataset workunits
        assert len(workunits) > 0

    def test_process_data_structure_reports_schema_found(
        self, builder, sample_data_structure
    ):
        """Test that processing reports schema as found."""
        list(builder.process_data_structure(sample_data_structure))

        assert builder.report.num_event_schemas_found == 1

    def test_process_data_structure_reports_schema_extracted(
        self, builder, sample_data_structure
    ):
        """Test that processing reports schema as extracted."""
        list(builder.process_data_structure(sample_data_structure))

        assert builder.report.num_event_schemas_extracted == 1

    def test_validation_skips_missing_vendor(self, builder, state):
        """Test that schemas with missing vendor are skipped."""
        data_structure = DataStructure(
            hash="test-hash",
            vendor=None,  # Missing
            name="event1",
            meta=SchemaMetadata(),
            data=None,
        )

        workunits = list(builder.process_data_structure(data_structure))

        assert len(workunits) == 0

    def test_validation_skips_missing_name(self, builder):
        """Test that schemas with missing name are skipped."""
        data_structure = DataStructure(
            hash="test-hash",
            vendor="com.acme",
            name=None,  # Missing
            meta=SchemaMetadata(),
            data=None,
        )

        workunits = list(builder.process_data_structure(data_structure))

        assert len(workunits) == 0

    def test_validation_skips_missing_meta(self, builder):
        """Test that schemas with missing meta are skipped."""
        data_structure = DataStructure(
            hash="test-hash",
            vendor="com.acme",
            name="event1",
            meta=None,  # Missing
            data=None,
        )

        workunits = list(builder.process_data_structure(data_structure))

        assert len(workunits) == 0

    def test_schema_filtering_by_pattern(
        self, mock_deps, state, mock_column_lineage_builder
    ):
        """Test that schemas are filtered by schema_pattern."""
        from datahub.configuration.common import AllowDenyPattern

        # Configure to deny all com.acme schemas
        mock_deps.config.schema_pattern = AllowDenyPattern(deny=["com\\.acme.*"])

        builder = DataStructureBuilder(
            deps=mock_deps,
            state=state,
            column_lineage_builder=mock_column_lineage_builder,
        )

        data_structure = DataStructure(
            hash="test-hash",
            vendor="com.acme",
            name="event1",
            meta=SchemaMetadata(schema_type="event"),
            data=make_schema_data(),
        )

        workunits = list(builder.process_data_structure(data_structure))

        # Should be filtered out
        assert len(workunits) == 0
        assert builder.report.num_event_schemas_filtered == 1

    def test_hidden_schema_skipped_by_default(self, builder):
        """Test that hidden schemas are skipped by default."""
        data_structure = DataStructure(
            hash="test-hash",
            vendor="com.acme",
            name="hidden_event",
            meta=SchemaMetadata(hidden=True, schema_type="event"),
            data=make_schema_data(name="hidden_event"),
        )

        workunits = list(builder.process_data_structure(data_structure))

        # Should be skipped
        assert len(workunits) == 0
        assert builder.report.num_hidden_schemas_skipped == 1

    def test_hidden_schema_processed_when_enabled(
        self, mock_deps, state, mock_column_lineage_builder
    ):
        """Test that hidden schemas are processed when include_hidden_schemas=True."""
        mock_deps.config.include_hidden_schemas = True

        builder = DataStructureBuilder(
            deps=mock_deps,
            state=state,
            column_lineage_builder=mock_column_lineage_builder,
        )

        data_structure = DataStructure(
            hash="test-hash",
            vendor="com.acme",
            name="hidden_event",
            meta=SchemaMetadata(hidden=True, schema_type="event"),
            data=make_schema_data(
                name="hidden_event",
                description="A hidden event",
            ),
        )

        workunits = list(builder.process_data_structure(data_structure))

        # Should be processed
        assert len(workunits) > 0
        # Hidden schema tracked but not skipped
        assert builder.report.num_hidden_schemas == 1
        assert builder.report.num_hidden_schemas_skipped == 0

    def test_version_extraction_from_schema_data(self, builder):
        """Test version extraction when schema data is available."""
        data_structure = DataStructure(
            hash="test-hash",
            vendor="com.acme",
            name="event1",
            meta=SchemaMetadata(schema_type="event"),
            data=make_schema_data(version="2-1-0"),
            deployments=[
                DataStructureDeployment(
                    version="1-0-0", env="PROD", ts="2024-01-01T00:00:00Z"
                )
            ],
        )

        version = builder._get_schema_version(data_structure)

        # Should use version from schema data, not deployment
        assert version == "2-1-0"

    def test_version_extraction_from_deployment(self, builder):
        """Test version extraction when schema data is not available."""
        data_structure = DataStructure(
            hash="test-hash",
            vendor="com.acme",
            name="event1",
            meta=SchemaMetadata(schema_type="event"),
            data=None,  # No schema data
            deployments=[
                DataStructureDeployment(
                    version="1-0-0", env="DEV", ts="2024-01-01T00:00:00Z"
                ),
                DataStructureDeployment(
                    version="1-1-0", env="PROD", ts="2024-01-15T00:00:00Z"
                ),
            ],
        )

        version = builder._get_schema_version(data_structure)

        # Should use version from latest deployment
        assert version == "1-1-0"

    def test_version_extraction_returns_none_when_unavailable(self, builder):
        """Test version extraction returns None when no source available."""
        data_structure = DataStructure(
            hash="test-hash",
            vendor="com.acme",
            name="event1",
            meta=SchemaMetadata(schema_type="event"),
            data=None,
            deployments=[],  # No deployments either
        )

        version = builder._get_schema_version(data_structure)

        assert version is None

    def test_dataset_name_without_version(self, builder):
        """Test dataset name excludes version by default."""
        name = builder._build_dataset_name("com.acme", "checkout_started", "1-0-0")

        assert name == "com.acme.checkout_started"
        assert "1-0-0" not in name

    def test_dataset_name_with_version(
        self, mock_deps, state, mock_column_lineage_builder
    ):
        """Test dataset name includes version when configured."""
        mock_deps.config.include_version_in_urn = True

        builder = DataStructureBuilder(
            deps=mock_deps,
            state=state,
            column_lineage_builder=mock_column_lineage_builder,
        )

        name = builder._build_dataset_name("com.acme", "checkout_started", "1-0-0")

        assert name == "com.acme.checkout_started.1-0-0"

    def test_parent_container_built_with_bdp_connection(self, builder):
        """Test parent container is built when BDP connection exists."""
        containers = builder._build_parent_container()

        assert containers is not None
        assert len(containers) == 1

    def test_no_parent_container_without_bdp_connection(
        self, mock_deps, state, mock_column_lineage_builder
    ):
        """Test no parent container when no BDP connection."""
        from datahub.ingestion.source.snowplow.snowplow_config import (
            IgluConnectionConfig,
        )

        # Create config without BDP connection
        config = SnowplowSourceConfig(
            iglu_connection=IgluConnectionConfig(
                iglu_server_url="https://iglu.example.com"
            ),
            field_tagging=FieldTaggingConfig(enabled=False),
        )

        # Update mock_deps config to not have bdp_connection
        mock_deps.config = config

        builder = DataStructureBuilder(
            deps=mock_deps,
            state=state,
            column_lineage_builder=mock_column_lineage_builder,
        )

        containers = builder._build_parent_container()

        assert containers is None

    def test_external_url_generation_with_bdp(self, builder):
        """Test external URL is generated for BDP connection."""
        url = builder._get_schema_url(
            vendor="com.acme",
            name="checkout_started",
            version="1-0-0",
            schema_hash="abc123",
        )

        assert url is not None
        assert "test-org-123" in url
        assert "abc123" in url
        assert "version=1-0-0" in url
        assert "console.snowplowanalytics.com" in url

    def test_external_url_none_without_hash(self, builder):
        """Test external URL is None when hash is missing."""
        url = builder._get_schema_url(
            vendor="com.acme",
            name="checkout_started",
            version="1-0-0",
            schema_hash=None,  # Missing hash
        )

        assert url is None

    def test_captures_first_event_schema(self, builder, state):
        """Test that first event schema vendor/name are captured for naming."""
        data_structure = DataStructure(
            hash="test-hash",
            vendor="com.acme",
            name="checkout_started",
            meta=SchemaMetadata(schema_type="event"),
            data=make_schema_data(
                vendor="com.acme",
                name="checkout_started",
            ),
        )

        list(builder.process_data_structure(data_structure))

        assert state.first_event_schema_vendor == "com.acme"
        assert state.first_event_schema_name == "checkout_started"

    def test_does_not_overwrite_first_event_schema(self, builder, state):
        """Test that subsequent event schemas don't overwrite first one."""
        # Pre-set first event schema
        state.first_event_schema_vendor = "com.original"
        state.first_event_schema_name = "original_event"

        data_structure = DataStructure(
            hash="test-hash",
            vendor="com.acme",
            name="new_event",
            meta=SchemaMetadata(schema_type="event"),
            data=make_schema_data(
                vendor="com.acme",
                name="new_event",
            ),
        )

        list(builder.process_data_structure(data_structure))

        # Should not be overwritten
        assert state.first_event_schema_vendor == "com.original"
        assert state.first_event_schema_name == "original_event"

    def test_caches_extracted_schema_urn(self, builder, state, sample_data_structure):
        """Test that extracted schema URN is cached in state."""
        list(builder.process_data_structure(sample_data_structure))

        assert len(state.extracted_schema_urns) == 1
        assert "com.acme.checkout_started" in state.extracted_schema_urns[0]

    def test_custom_properties_include_required_fields(
        self, builder, sample_data_structure
    ):
        """Test that custom properties include all required fields."""
        props = builder._build_custom_properties(
            data_structure=sample_data_structure,
            vendor="com.acme",
            name="checkout_started",
            version="1-0-0",
            schema_type="event",
            schema_meta=sample_data_structure.meta,
        )

        assert props["vendor"] == "com.acme"
        assert props["schemaVersion"] == "1-0-0"
        assert props["schema_type"] == "event"
        assert props["hidden"] == "False"
        assert "igluUri" in props
        assert props["igluUri"] == "iglu:com.acme/checkout_started/jsonschema/1-0-0"

    def test_fetch_full_schema_when_data_missing(
        self, mock_deps, state, mock_column_lineage_builder
    ):
        """Test fetching full schema definition when data is missing."""
        # Setup mock BDP client to return full schema
        full_schema = DataStructure(
            hash="test-hash",
            vendor="com.acme",
            name="event1",
            data=make_schema_data(
                description="Full schema",
                properties={"field1": {"type": "string"}},
            ),
        )
        mock_deps.bdp_client.get_data_structure_version.return_value = full_schema

        builder = DataStructureBuilder(
            deps=mock_deps,
            state=state,
            column_lineage_builder=mock_column_lineage_builder,
        )

        # Data structure without schema data
        data_structure = DataStructure(
            hash="test-hash",
            vendor="com.acme",
            name="event1",
            meta=SchemaMetadata(schema_type="event"),
            data=None,  # Missing
            deployments=[
                DataStructureDeployment(
                    version="1-0-0", env="PROD", ts="2024-01-01T00:00:00Z"
                )
            ],
        )

        result = builder._fetch_full_schema_definition(data_structure)

        # Should fetch full schema
        mock_deps.bdp_client.get_data_structure_version.assert_called_once_with(
            "test-hash", "1-0-0", "PROD"
        )
        assert result.data is not None
        assert result.data.description == "Full schema"
