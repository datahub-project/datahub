"""Unit tests for PipelineProcessor."""

from unittest.mock import Mock

import pytest

from datahub.ingestion.source.snowplow.processors.pipeline_processor import (
    WAREHOUSE_PLATFORM_MAP,
    PipelineProcessor,
)


class TestWarehousePlatformMapping:
    """Tests for warehouse platform mapping."""

    def test_snowflake_mapping(self):
        """Snowflake maps to snowflake platform."""
        assert WAREHOUSE_PLATFORM_MAP["snowflake"] == "snowflake"

    def test_bigquery_mapping(self):
        """BigQuery maps to bigquery platform."""
        assert WAREHOUSE_PLATFORM_MAP["bigquery"] == "bigquery"

    def test_redshift_mapping(self):
        """Redshift maps to redshift platform."""
        assert WAREHOUSE_PLATFORM_MAP["redshift"] == "redshift"

    def test_postgres_variants_map_to_same_platform(self):
        """Both postgres and postgresql map to postgres."""
        assert WAREHOUSE_PLATFORM_MAP["postgres"] == "postgres"
        assert WAREHOUSE_PLATFORM_MAP["postgresql"] == "postgres"


class TestPipelineProcessorIsEnabled:
    """Tests for is_enabled() method."""

    @pytest.fixture
    def mock_deps(self):
        """Create mock dependencies."""
        deps = Mock()
        deps.config = Mock()
        deps.config.extract_pipelines = True
        deps.config.extract_enrichments = False
        deps.bdp_client = Mock()
        deps.report = Mock()
        deps.cache = Mock()
        deps.urn_factory = Mock()
        return deps

    @pytest.fixture
    def mock_state(self):
        """Create mock state."""
        state = Mock()
        state.physical_pipeline = None
        state.pipeline_dataflow_urn = None
        return state

    def test_is_enabled_when_extract_pipelines_true(self, mock_deps, mock_state):
        """Processor is enabled when extract_pipelines is True."""
        processor = PipelineProcessor(mock_deps, mock_state)
        assert processor.is_enabled() is True

    def test_is_enabled_when_extract_enrichments_true(self, mock_deps, mock_state):
        """Processor is enabled when extract_enrichments is True."""
        mock_deps.config.extract_pipelines = False
        mock_deps.config.extract_enrichments = True
        processor = PipelineProcessor(mock_deps, mock_state)
        assert processor.is_enabled() is True

    def test_is_disabled_when_both_config_false(self, mock_deps, mock_state):
        """Processor is disabled when both config options are False."""
        mock_deps.config.extract_pipelines = False
        mock_deps.config.extract_enrichments = False
        processor = PipelineProcessor(mock_deps, mock_state)
        assert processor.is_enabled() is False

    def test_is_disabled_when_no_bdp_client(self, mock_deps, mock_state):
        """Processor is disabled when no BDP client."""
        mock_deps.bdp_client = None
        processor = PipelineProcessor(mock_deps, mock_state)
        assert processor.is_enabled() is False


class TestPipelineProcessorExtract:
    """Tests for extract() method."""

    @pytest.fixture
    def mock_deps(self):
        """Create mock dependencies."""
        deps = Mock()
        deps.config = Mock()
        deps.config.extract_pipelines = True
        deps.config.extract_enrichments = False
        deps.config.bdp_connection = Mock()
        deps.config.bdp_connection.organization_id = "test-org"
        deps.bdp_client = Mock()
        deps.report = Mock()
        deps.cache = Mock()
        deps.urn_factory = Mock()
        deps.error_handler = Mock()
        deps.platform = "snowplow"
        return deps

    @pytest.fixture
    def mock_state(self):
        """Create mock state."""
        state = Mock()
        state.physical_pipeline = None
        state.pipeline_dataflow_urn = None
        state.emitted_event_spec_urns = []
        return state

    def test_extract_returns_empty_when_no_bdp_client(self, mock_deps, mock_state):
        """Extract returns nothing when BDP client is not configured."""
        mock_deps.bdp_client = None
        mock_deps.config.extract_pipelines = False
        mock_deps.config.extract_enrichments = False
        processor = PipelineProcessor(mock_deps, mock_state)

        workunits = list(processor.extract())
        assert workunits == []

    def test_extract_handles_api_error_gracefully(self, mock_deps, mock_state):
        """Extract handles API errors without crashing."""
        mock_deps.bdp_client.get_event_specifications.side_effect = Exception(
            "API Error"
        )
        processor = PipelineProcessor(mock_deps, mock_state)

        workunits = list(processor.extract())

        # Should handle error gracefully
        assert workunits == []
        mock_deps.error_handler.handle_api_error.assert_called_once()

    def test_extract_handles_empty_event_specs(self, mock_deps, mock_state):
        """Extract handles empty event specifications list."""
        mock_deps.bdp_client.get_event_specifications.return_value = []
        mock_deps.bdp_client.get_pipelines.return_value = []

        processor = PipelineProcessor(mock_deps, mock_state)
        workunits = list(processor.extract())

        # Should not crash with empty event specs
        assert workunits == []


class TestPipelineProcessorEnrichments:
    """Tests for enrichment extraction."""

    @pytest.fixture
    def mock_deps(self):
        """Create mock dependencies."""
        deps = Mock()
        deps.config = Mock()
        deps.config.extract_pipelines = False
        deps.config.extract_enrichments = True
        deps.config.bdp_connection = Mock()
        deps.config.bdp_connection.organization_id = "test-org"
        deps.bdp_client = Mock()
        deps.report = Mock()
        deps.cache = Mock()
        deps.urn_factory = Mock()
        deps.error_handler = Mock()
        deps.platform = "snowplow"
        deps.ownership_builder = Mock()
        deps.ownership_builder.build_owners = Mock(return_value=[])
        deps.enrichment_lineage_registry = Mock()
        deps.enrichment_lineage_registry.get_columns = Mock(return_value=[])
        return deps

    @pytest.fixture
    def mock_state(self):
        """Create mock state."""
        state = Mock()
        state.physical_pipeline = None
        state.pipeline_dataflow_urn = None
        state.emitted_event_spec_urns = []
        return state

    def test_extract_enrichments_requires_physical_pipeline(
        self, mock_deps, mock_state
    ):
        """Extract enrichments warns when no physical pipeline."""
        # No physical pipeline in state
        mock_state.physical_pipeline = None

        processor = PipelineProcessor(mock_deps, mock_state)
        workunits = list(processor.extract())

        # Should not produce enrichment work units without pipeline
        assert workunits == []


class TestPipelineProcessorDataFlowCreation:
    """Tests for DataFlow creation and filtering."""

    @pytest.fixture
    def mock_deps(self):
        """Create mock dependencies."""
        deps = Mock()
        deps.config = Mock()
        deps.config.extract_pipelines = True
        deps.config.extract_enrichments = False
        deps.config.env = "PROD"
        deps.config.platform_instance = None
        deps.config.bdp_connection = Mock()
        deps.config.bdp_connection.organization_id = "test-org"
        deps.bdp_client = Mock()
        deps.report = Mock()
        deps.cache = Mock()
        deps.urn_factory = Mock()
        deps.urn_factory.make_organization_urn = Mock(
            return_value="urn:li:container:org"
        )
        deps.error_handler = Mock()
        deps.platform = "snowplow"
        return deps

    @pytest.fixture
    def mock_state(self):
        """Create mock state with emitted event spec IDs."""
        from datahub.ingestion.source.snowplow.dependencies import IngestionState

        return IngestionState()

    def test_dataflow_creation_filters_by_emitted_event_specs(
        self, mock_deps, mock_state
    ):
        """DataFlow creation should only process event specs in emitted_event_spec_ids."""
        from datahub.ingestion.source.snowplow.models.snowplow_models import (
            EventSchemaDetail,
            EventSpecification,
            Pipeline as SnowplowPipeline,
        )

        # Simulate EventSpecProcessor having emitted only spec1
        mock_state.emitted_event_spec_ids.add("spec1")

        # Create two event specs - only spec1 should get a DataFlow
        spec1 = EventSpecification(
            id="spec1",
            name="Event Spec 1",
            event=EventSchemaDetail(source="iglu:com.acme/event1/jsonschema/1-0-0"),
        )
        spec2 = EventSpecification(
            id="spec2",
            name="Event Spec 2",
            event=EventSchemaDetail(source="iglu:com.acme/event2/jsonschema/1-0-0"),
        )

        mock_pipeline = SnowplowPipeline(
            id="pipeline-123",
            name="Test Pipeline",
            status="active",
            workspace_id="workspace-123",
        )

        mock_deps.bdp_client.get_event_specifications.return_value = [spec1, spec2]
        mock_deps.bdp_client.get_pipelines.return_value = [mock_pipeline]

        processor = PipelineProcessor(mock_deps, mock_state)
        list(processor.extract())  # Consume iterator to trigger extraction

        # Should only create DataFlow for spec1
        assert "spec1" in mock_state.event_spec_dataflow_urns
        assert "spec2" not in mock_state.event_spec_dataflow_urns

    def test_dataflow_creation_supports_legacy_format(self, mock_deps, mock_state):
        """DataFlow creation should support LEGACY format event specs."""
        from datahub.ingestion.source.snowplow.models.snowplow_models import (
            EventSchemaReference,
            EventSpecification,
            Pipeline as SnowplowPipeline,
        )

        # Simulate EventSpecProcessor having emitted this event spec
        mock_state.emitted_event_spec_ids.add("legacy-spec")

        # Create LEGACY format event spec (no event.source, only eventSchemas)
        legacy_spec = EventSpecification(
            id="legacy-spec",
            name="Legacy Event Spec",
            event=None,  # No event field (LEGACY format)
            event_schemas=[
                EventSchemaReference(
                    vendor="com.acme",
                    name="checkout",
                    version="1-0-0",
                )
            ],
        )

        mock_pipeline = SnowplowPipeline(
            id="pipeline-123",
            name="Test Pipeline",
            status="active",
            workspace_id="workspace-123",
        )

        mock_deps.bdp_client.get_event_specifications.return_value = [legacy_spec]
        mock_deps.bdp_client.get_pipelines.return_value = [mock_pipeline]

        processor = PipelineProcessor(mock_deps, mock_state)
        list(processor.extract())  # Consume iterator to trigger extraction

        # Should create DataFlow even for LEGACY format
        assert "legacy-spec" in mock_state.event_spec_dataflow_urns

    def test_dataflow_creation_skips_empty_event_specs(self, mock_deps, mock_state):
        """DataFlow creation should skip event specs with no schema info."""
        from datahub.ingestion.source.snowplow.models.snowplow_models import (
            EventSpecification,
            Pipeline as SnowplowPipeline,
        )

        # Simulate EventSpecProcessor having emitted this event spec
        mock_state.emitted_event_spec_ids.add("empty-spec")

        # Create empty event spec (no event.source, no eventSchemas)
        empty_spec = EventSpecification(
            id="empty-spec",
            name="Empty Event Spec",
            event=None,
            event_schemas=[],
        )

        mock_pipeline = SnowplowPipeline(
            id="pipeline-123",
            name="Test Pipeline",
            status="active",
            workspace_id="workspace-123",
        )

        mock_deps.bdp_client.get_event_specifications.return_value = [empty_spec]
        mock_deps.bdp_client.get_pipelines.return_value = [mock_pipeline]

        processor = PipelineProcessor(mock_deps, mock_state)
        list(processor.extract())  # Consume iterator to trigger extraction

        # Should NOT create DataFlow for empty event spec
        assert "empty-spec" not in mock_state.event_spec_dataflow_urns
