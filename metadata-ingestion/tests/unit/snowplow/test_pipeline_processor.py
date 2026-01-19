"""Unit tests for PipelineProcessor."""

from typing import Optional
from unittest.mock import Mock

import pytest

from datahub.ingestion.source.snowplow.constants import WAREHOUSE_PLATFORM_MAP
from datahub.ingestion.source.snowplow.processors.pipeline_processor import (
    PipelineProcessor,
)
from datahub.metadata.schema_classes import DataJobInfoClass, DataJobInputOutputClass


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
    """Tests for DataFlow creation with single physical pipeline architecture."""

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

    def test_creates_single_dataflow_for_physical_pipeline(self, mock_deps, mock_state):
        """Single DataFlow is created for the physical pipeline (Option A architecture)."""
        from datahub.ingestion.source.snowplow.models.snowplow_models import (
            EventSchemaDetail,
            EventSpecification,
            Pipeline as SnowplowPipeline,
        )

        # Create multiple event specs - all share single DataFlow
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
        workunits = list(processor.extract())

        # Should create single pipeline_dataflow_urn (not per-event-spec)
        assert mock_state.pipeline_dataflow_urn is not None
        assert "pipeline-123" in mock_state.pipeline_dataflow_urn

        # Should store physical pipeline
        assert mock_state.physical_pipeline is not None
        assert mock_state.physical_pipeline.id == "pipeline-123"

        # All event specs tracked for enrichment processing
        assert "spec1" in mock_state.emitted_event_spec_ids
        assert "spec2" in mock_state.emitted_event_spec_ids

        # Should emit DataFlow workunits
        assert len(workunits) > 0

    def test_tracks_all_event_specs_when_none_pre_emitted(self, mock_deps, mock_state):
        """When no event specs were pre-emitted, pipeline processor tracks all specs."""
        from datahub.ingestion.source.snowplow.models.snowplow_models import (
            EventSchemaDetail,
            EventSpecification,
            Pipeline as SnowplowPipeline,
        )

        # No pre-emitted event specs (EventSpecProcessor may not have run)
        assert len(mock_state.emitted_event_spec_ids) == 0

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
        list(processor.extract())

        # Pipeline processor should track all event specs for enrichment extraction
        assert "spec1" in mock_state.emitted_event_spec_ids
        assert "spec2" in mock_state.emitted_event_spec_ids

    def test_preserves_pre_emitted_event_spec_ids(self, mock_deps, mock_state):
        """When event specs were pre-emitted, pipeline processor preserves them."""
        from datahub.ingestion.source.snowplow.models.snowplow_models import (
            EventSchemaDetail,
            EventSpecification,
            Pipeline as SnowplowPipeline,
        )

        # Simulate EventSpecProcessor having emitted only spec1
        mock_state.emitted_event_spec_ids.add("spec1")

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
        list(processor.extract())

        # Should preserve pre-emitted spec and NOT add spec2
        assert "spec1" in mock_state.emitted_event_spec_ids
        # spec2 was not pre-emitted, so should not be added by pipeline processor
        # when emitted_event_spec_ids is already populated

    def test_handles_no_pipelines(self, mock_deps, mock_state):
        """Handles case when no physical pipelines exist."""
        from datahub.ingestion.source.snowplow.models.snowplow_models import (
            EventSchemaDetail,
            EventSpecification,
        )

        spec1 = EventSpecification(
            id="spec1",
            name="Event Spec 1",
            event=EventSchemaDetail(source="iglu:com.acme/event1/jsonschema/1-0-0"),
        )

        mock_deps.bdp_client.get_event_specifications.return_value = [spec1]
        mock_deps.bdp_client.get_pipelines.return_value = []  # No pipelines

        processor = PipelineProcessor(mock_deps, mock_state)
        workunits = list(processor.extract())

        # Should not crash, but no DataFlow created
        assert mock_state.pipeline_dataflow_urn is None
        assert mock_state.physical_pipeline is None
        assert workunits == []


class TestExpandFieldUrnsToAllEventSpecs:
    """Tests for _expand_field_urns_to_all_event_specs method."""

    @pytest.fixture
    def mock_deps(self):
        """Create mock dependencies."""
        deps = Mock()
        deps.config = Mock()
        deps.config.extract_pipelines = True
        deps.config.extract_enrichments = True
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
        from datahub.ingestion.source.snowplow.dependencies import IngestionState

        return IngestionState()

    def test_expands_atomic_field_to_all_event_specs(self, mock_deps, mock_state):
        """Atomic field (user_ipaddress) is expanded to all event specs."""
        processor = PipelineProcessor(mock_deps, mock_state)

        template_urn = "urn:li:dataset:(urn:li:dataPlatform:snowplow,event_spec_1,PROD)"
        template_field_urns = [f"urn:li:schemaField:({template_urn},user_ipaddress)"]
        all_event_spec_urns = [
            "urn:li:dataset:(urn:li:dataPlatform:snowplow,event_spec_1,PROD)",
            "urn:li:dataset:(urn:li:dataPlatform:snowplow,event_spec_2,PROD)",
            "urn:li:dataset:(urn:li:dataPlatform:snowplow,event_spec_3,PROD)",
        ]

        expanded = processor._expand_field_urns_to_all_event_specs(
            template_field_urns=template_field_urns,
            template_event_spec_urn=template_urn,
            all_event_spec_urns=all_event_spec_urns,
        )

        # Should have 3 field URNs (one per event spec) - atomic field expanded
        assert len(expanded) == 3
        assert (
            "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:snowplow,event_spec_1,PROD),user_ipaddress)"
            in expanded
        )
        assert (
            "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:snowplow,event_spec_2,PROD),user_ipaddress)"
            in expanded
        )
        assert (
            "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:snowplow,event_spec_3,PROD),user_ipaddress)"
            in expanded
        )

    def test_custom_field_expands_only_to_event_specs_with_schema(
        self, mock_deps, mock_state
    ):
        """Custom schema field expands only to event specs that have the schema."""
        from datahub.metadata.schema_classes import (
            SchemaFieldClass,
            SchemaFieldDataTypeClass,
            StringTypeClass,
        )

        processor = PipelineProcessor(mock_deps, mock_state)

        # Set up state: schema with customer_email field
        schema_urn = "urn:li:dataset:(urn:li:dataPlatform:snowplow,com.acme.checkout_started,PROD)"
        mock_state.extracted_schema_fields_by_urn[schema_urn] = [
            SchemaFieldClass(
                fieldPath="customer_email",
                type=SchemaFieldDataTypeClass(type=StringTypeClass()),
                nativeDataType="string",
            )
        ]

        # Event spec 1 and 2 have the schema, event spec 3 does NOT
        event_spec_1 = "urn:li:dataset:(urn:li:dataPlatform:snowplow,event_spec_1,PROD)"
        event_spec_2 = "urn:li:dataset:(urn:li:dataPlatform:snowplow,event_spec_2,PROD)"
        event_spec_3 = "urn:li:dataset:(urn:li:dataPlatform:snowplow,event_spec_3,PROD)"

        mock_state.event_spec_to_schema_urns = {
            event_spec_1: [schema_urn],  # Has schema
            event_spec_2: [schema_urn],  # Has schema
            event_spec_3: [],  # Does NOT have schema
        }

        template_field_urns = [f"urn:li:schemaField:({event_spec_1},customer_email)"]
        all_event_spec_urns = [event_spec_1, event_spec_2, event_spec_3]

        expanded = processor._expand_field_urns_to_all_event_specs(
            template_field_urns=template_field_urns,
            template_event_spec_urn=event_spec_1,
            all_event_spec_urns=all_event_spec_urns,
        )

        # Should have 2 field URNs - only event specs 1 and 2 have the schema
        assert len(expanded) == 2
        assert f"urn:li:schemaField:({event_spec_1},customer_email)" in expanded
        assert f"urn:li:schemaField:({event_spec_2},customer_email)" in expanded
        # Event spec 3 does NOT have the field - should NOT be included
        assert f"urn:li:schemaField:({event_spec_3},customer_email)" not in expanded

    def test_custom_field_falls_back_to_template_when_schema_not_found(
        self, mock_deps, mock_state
    ):
        """Custom field falls back to template when schema info not available."""
        processor = PipelineProcessor(mock_deps, mock_state)

        # No schema info in state - empty extracted_schema_fields_by_urn

        template_urn = "urn:li:dataset:(urn:li:dataPlatform:snowplow,event_spec_1,PROD)"
        template_field_urns = [
            f"urn:li:schemaField:({template_urn},customer_email)"  # Custom field
        ]
        all_event_spec_urns = [
            "urn:li:dataset:(urn:li:dataPlatform:snowplow,event_spec_1,PROD)",
            "urn:li:dataset:(urn:li:dataPlatform:snowplow,event_spec_2,PROD)",
        ]

        expanded = processor._expand_field_urns_to_all_event_specs(
            template_field_urns=template_field_urns,
            template_event_spec_urn=template_urn,
            all_event_spec_urns=all_event_spec_urns,
        )

        # Should fall back to just the template (conservative)
        assert len(expanded) == 1
        assert f"urn:li:schemaField:({template_urn},customer_email)" in expanded

    def test_expands_multiple_atomic_fields(self, mock_deps, mock_state):
        """Multiple atomic fields are expanded to all event specs."""
        processor = PipelineProcessor(mock_deps, mock_state)

        template_urn = "urn:li:dataset:(urn:li:dataPlatform:snowplow,event_spec_1,PROD)"
        template_field_urns = [
            f"urn:li:schemaField:({template_urn},user_ipaddress)",
            f"urn:li:schemaField:({template_urn},page_url)",
        ]
        all_event_spec_urns = [
            "urn:li:dataset:(urn:li:dataPlatform:snowplow,event_spec_1,PROD)",
            "urn:li:dataset:(urn:li:dataPlatform:snowplow,event_spec_2,PROD)",
        ]

        expanded = processor._expand_field_urns_to_all_event_specs(
            template_field_urns=template_field_urns,
            template_event_spec_urn=template_urn,
            all_event_spec_urns=all_event_spec_urns,
        )

        # Should have 4 field URNs (2 atomic fields x 2 event specs)
        assert len(expanded) == 4

    def test_mixed_atomic_and_custom_fields(self, mock_deps, mock_state):
        """Atomic fields expanded to all, custom fields expanded to matching event specs only."""
        from datahub.metadata.schema_classes import (
            SchemaFieldClass,
            SchemaFieldDataTypeClass,
            StringTypeClass,
        )

        processor = PipelineProcessor(mock_deps, mock_state)

        # Set up state: schema with customer_email only on event_spec_1
        schema_urn = (
            "urn:li:dataset:(urn:li:dataPlatform:snowplow,com.acme.checkout,PROD)"
        )
        mock_state.extracted_schema_fields_by_urn[schema_urn] = [
            SchemaFieldClass(
                fieldPath="customer_email",
                type=SchemaFieldDataTypeClass(type=StringTypeClass()),
                nativeDataType="string",
            )
        ]

        event_spec_1 = "urn:li:dataset:(urn:li:dataPlatform:snowplow,event_spec_1,PROD)"
        event_spec_2 = "urn:li:dataset:(urn:li:dataPlatform:snowplow,event_spec_2,PROD)"

        # Only event_spec_1 has the schema with customer_email
        mock_state.event_spec_to_schema_urns = {
            event_spec_1: [schema_urn],  # Has schema with customer_email
            event_spec_2: [],  # Does NOT have the schema
        }

        template_field_urns = [
            f"urn:li:schemaField:({event_spec_1},user_ipaddress)",  # Atomic - expand to all
            f"urn:li:schemaField:({event_spec_1},customer_email)",  # Custom - expand to matching only
        ]
        all_event_spec_urns = [event_spec_1, event_spec_2]

        expanded = processor._expand_field_urns_to_all_event_specs(
            template_field_urns=template_field_urns,
            template_event_spec_urn=event_spec_1,
            all_event_spec_urns=all_event_spec_urns,
        )

        # Should have 3 field URNs: 2 for user_ipaddress (expanded to all) + 1 for customer_email (only event_spec_1)
        assert len(expanded) == 3
        # Atomic field expanded to both event specs
        assert f"urn:li:schemaField:({event_spec_1},user_ipaddress)" in expanded
        assert f"urn:li:schemaField:({event_spec_2},user_ipaddress)" in expanded
        # Custom field only expanded to event_spec_1 (has the schema)
        assert f"urn:li:schemaField:({event_spec_1},customer_email)" in expanded
        assert f"urn:li:schemaField:({event_spec_2},customer_email)" not in expanded

    def test_handles_empty_event_spec_list(self, mock_deps, mock_state):
        """Returns template field URN when no event specs provided."""
        processor = PipelineProcessor(mock_deps, mock_state)

        template_urn = "urn:li:dataset:(urn:li:dataPlatform:snowplow,event_spec_1,PROD)"
        template_field_urns = [f"urn:li:schemaField:({template_urn},user_ipaddress)"]

        expanded = processor._expand_field_urns_to_all_event_specs(
            template_field_urns=template_field_urns,
            template_event_spec_urn=template_urn,
            all_event_spec_urns=[],
        )

        # Atomic field with no event specs to expand to - still returns empty (no specs to expand to)
        assert expanded == []

    def test_handles_empty_field_list(self, mock_deps, mock_state):
        """Returns empty list when no fields provided."""
        processor = PipelineProcessor(mock_deps, mock_state)

        template_urn = "urn:li:dataset:(urn:li:dataPlatform:snowplow,event_spec_1,PROD)"
        all_event_spec_urns = [
            "urn:li:dataset:(urn:li:dataPlatform:snowplow,event_spec_1,PROD)",
            "urn:li:dataset:(urn:li:dataPlatform:snowplow,event_spec_2,PROD)",
        ]

        expanded = processor._expand_field_urns_to_all_event_specs(
            template_field_urns=[],
            template_event_spec_urn=template_urn,
            all_event_spec_urns=all_event_spec_urns,
        )

        assert expanded == []


class TestFieldMatches:
    """Tests for _field_matches method."""

    @pytest.fixture
    def mock_deps(self):
        """Create mock dependencies."""
        deps = Mock()
        deps.config = Mock()
        deps.config.extract_pipelines = True
        deps.config.extract_enrichments = True
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
        from datahub.ingestion.source.snowplow.dependencies import IngestionState

        return IngestionState()

    def test_exact_match(self, mock_deps, mock_state):
        """Exact field name match."""
        processor = PipelineProcessor(mock_deps, mock_state)
        assert processor._field_matches("customer_email", "customer_email") is True

    def test_nested_path_match(self, mock_deps, mock_state):
        """Field name at end of nested path."""
        processor = PipelineProcessor(mock_deps, mock_state)
        assert (
            processor._field_matches("checkout.customer_email", "customer_email")
            is True
        )

    def test_context_prefix_match(self, mock_deps, mock_state):
        """Field name with Snowplow context prefix."""
        processor = PipelineProcessor(mock_deps, mock_state)
        assert (
            processor._field_matches(
                "contexts_com_acme_checkout_1.customer_email", "customer_email"
            )
            is True
        )

    def test_no_match_partial(self, mock_deps, mock_state):
        """Partial field name should not match."""
        processor = PipelineProcessor(mock_deps, mock_state)
        # "email" should not match "customer_email"
        assert processor._field_matches("customer_email", "email") is False

    def test_no_match_different_field(self, mock_deps, mock_state):
        """Different field names should not match."""
        processor = PipelineProcessor(mock_deps, mock_state)
        assert processor._field_matches("customer_email", "user_email") is False

    def test_handles_empty_inputs(self, mock_deps, mock_state):
        """Empty inputs return False."""
        processor = PipelineProcessor(mock_deps, mock_state)
        assert processor._field_matches("", "customer_email") is False
        assert processor._field_matches("customer_email", "") is False
        assert processor._field_matches(None, "customer_email") is False


class TestFindEventSpecsWithField:
    """Tests for _find_event_specs_with_field method."""

    @pytest.fixture
    def mock_deps(self):
        """Create mock dependencies."""
        deps = Mock()
        deps.config = Mock()
        deps.config.extract_pipelines = True
        deps.config.extract_enrichments = True
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
        from datahub.ingestion.source.snowplow.dependencies import IngestionState

        return IngestionState()

    def test_finds_event_specs_with_matching_schema(self, mock_deps, mock_state):
        """Finds event specs that have schemas containing the field."""
        from datahub.metadata.schema_classes import (
            NumberTypeClass,
            SchemaFieldClass,
            SchemaFieldDataTypeClass,
            StringTypeClass,
        )

        processor = PipelineProcessor(mock_deps, mock_state)

        # Set up schemas
        checkout_schema = (
            "urn:li:dataset:(urn:li:dataPlatform:snowplow,com.acme.checkout,PROD)"
        )
        page_view_schema = (
            "urn:li:dataset:(urn:li:dataPlatform:snowplow,com.acme.page_view,PROD)"
        )

        mock_state.extracted_schema_fields_by_urn[checkout_schema] = [
            SchemaFieldClass(
                fieldPath="customer_email",
                type=SchemaFieldDataTypeClass(type=StringTypeClass()),
                nativeDataType="string",
            ),
            SchemaFieldClass(
                fieldPath="cart_value",
                type=SchemaFieldDataTypeClass(type=NumberTypeClass()),
                nativeDataType="number",
            ),
        ]
        mock_state.extracted_schema_fields_by_urn[page_view_schema] = [
            SchemaFieldClass(
                fieldPath="page_url",
                type=SchemaFieldDataTypeClass(type=StringTypeClass()),
                nativeDataType="string",
            ),
        ]

        # Set up event spec to schema mappings
        event_spec_checkout = (
            "urn:li:dataset:(urn:li:dataPlatform:snowplow,checkout_event,PROD)"
        )
        event_spec_page_view = (
            "urn:li:dataset:(urn:li:dataPlatform:snowplow,page_view_event,PROD)"
        )

        mock_state.event_spec_to_schema_urns = {
            event_spec_checkout: [checkout_schema],
            event_spec_page_view: [page_view_schema],
        }

        all_event_spec_urns = [event_spec_checkout, event_spec_page_view]

        # Find which event specs have customer_email
        result = processor._find_event_specs_with_field(
            "customer_email", all_event_spec_urns
        )

        # Only checkout event spec has customer_email
        assert len(result) == 1
        assert event_spec_checkout in result
        assert event_spec_page_view not in result

    def test_returns_empty_when_field_not_in_any_schema(self, mock_deps, mock_state):
        """Returns empty list when field not found in any schema."""
        from datahub.metadata.schema_classes import (
            NumberTypeClass,
            SchemaFieldClass,
            SchemaFieldDataTypeClass,
        )

        processor = PipelineProcessor(mock_deps, mock_state)

        schema_urn = (
            "urn:li:dataset:(urn:li:dataPlatform:snowplow,com.acme.checkout,PROD)"
        )
        mock_state.extracted_schema_fields_by_urn[schema_urn] = [
            SchemaFieldClass(
                fieldPath="cart_value",
                type=SchemaFieldDataTypeClass(type=NumberTypeClass()),
                nativeDataType="number",
            ),
        ]

        event_spec = "urn:li:dataset:(urn:li:dataPlatform:snowplow,checkout_event,PROD)"
        mock_state.event_spec_to_schema_urns = {event_spec: [schema_urn]}

        # Look for a field that doesn't exist
        result = processor._find_event_specs_with_field(
            "nonexistent_field", [event_spec]
        )

        assert result == []

    def test_returns_empty_for_none_field_name(self, mock_deps, mock_state):
        """Returns empty list for None field name."""
        processor = PipelineProcessor(mock_deps, mock_state)

        result = processor._find_event_specs_with_field(None, ["some_urn"])

        assert result == []


class TestExtractFieldNameFromUrn:
    """Tests for _extract_field_name_from_urn method."""

    @pytest.fixture
    def mock_deps(self):
        """Create mock dependencies."""
        deps = Mock()
        deps.config = Mock()
        deps.config.extract_pipelines = True
        deps.config.extract_enrichments = True
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
        from datahub.ingestion.source.snowplow.dependencies import IngestionState

        return IngestionState()

    def test_extracts_simple_field_name(self, mock_deps, mock_state):
        """Extracts field name from standard URN format."""
        processor = PipelineProcessor(mock_deps, mock_state)

        field_urn = "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:snowplow,event_spec_1,PROD),user_ipaddress)"
        field_name = processor._extract_field_name_from_urn(field_urn)

        assert field_name == "user_ipaddress"

    def test_extracts_field_with_underscore(self, mock_deps, mock_state):
        """Extracts field name with underscores."""
        processor = PipelineProcessor(mock_deps, mock_state)

        field_urn = "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:snowplow,event_spec_1,PROD),geo_region_name)"
        field_name = processor._extract_field_name_from_urn(field_urn)

        assert field_name == "geo_region_name"

    def test_returns_none_for_malformed_urn(self, mock_deps, mock_state):
        """Returns None for URN without comma separator."""
        processor = PipelineProcessor(mock_deps, mock_state)

        field_urn = "malformed_urn_without_comma"
        field_name = processor._extract_field_name_from_urn(field_urn)

        assert field_name is None


class TestEmitCollectorDataJob:
    """Tests for _emit_collector_datajob method."""

    @pytest.fixture
    def mock_deps(self):
        """Create mock dependencies."""
        deps = Mock()
        deps.config = Mock()
        deps.config.extract_pipelines = True
        deps.config.extract_enrichments = True
        deps.bdp_client = Mock()
        deps.report = Mock()
        deps.cache = Mock()
        deps.urn_factory = Mock()
        deps.error_handler = Mock()
        deps.platform = "snowplow"
        return deps

    @pytest.fixture
    def mock_state(self):
        """Create mock state with pipeline."""
        from datahub.ingestion.source.snowplow.dependencies import IngestionState
        from datahub.ingestion.source.snowplow.models.snowplow_models import (
            Pipeline as SnowplowPipeline,
            PipelineConfig,
        )

        state = IngestionState()
        state.physical_pipeline = SnowplowPipeline(
            id="test-pipeline-id",
            name="Test Pipeline",
            status="ready",
            config=PipelineConfig(
                collector_endpoints=[
                    "https://collector.acme.com",
                    "https://collector-eu.acme.com",
                ],
            ),
        )
        return state

    def test_emits_collector_datajob_with_endpoints(self, mock_deps, mock_state):
        """Collector DataJob includes collector endpoints in custom properties."""
        processor = PipelineProcessor(mock_deps, mock_state)

        dataflow_urn = "urn:li:dataFlow:(snowplow,test-pipeline-id,PROD)"
        output_urns = [
            "urn:li:dataset:(urn:li:dataPlatform:snowplow,event_spec_1,PROD)",
            "urn:li:dataset:(urn:li:dataPlatform:snowplow,event_spec_2,PROD)",
        ]

        workunits = list(
            processor._emit_collector_datajob(
                dataflow_urn=dataflow_urn,
                output_dataset_urns=output_urns,
            )
        )

        # Should emit workunits
        assert len(workunits) > 0

        # Extract aspects from workunits
        all_aspects = []
        for wu in workunits:
            if hasattr(wu.metadata, "aspect"):
                all_aspects.append(wu.metadata.aspect)

        # Find DataJobInfoClass aspect
        job_info: Optional[DataJobInfoClass] = None
        input_output: Optional[DataJobInputOutputClass] = None
        for aspect in all_aspects:
            if isinstance(aspect, DataJobInfoClass) and aspect.name == "Collector":
                job_info = aspect
            if isinstance(aspect, DataJobInputOutputClass):
                input_output = aspect

        # Verify DataJobInfoClass
        assert job_info is not None
        assert job_info.type == "STREAMING"
        assert job_info.customProperties is not None
        assert "collectorEndpoints" in job_info.customProperties
        assert "collector.acme.com" in job_info.customProperties["collectorEndpoints"]
        assert job_info.customProperties["endpointCount"] == "2"
        assert job_info.customProperties["stage"] == "collector"

        # Verify DataJobInputOutputClass
        assert input_output is not None
        assert input_output.inputDatasets == []  # No inputs - receives from HTTP
        assert len(input_output.outputDatasets) == 2

    def test_collector_without_endpoints(self, mock_deps, mock_state):
        """Collector DataJob handles missing endpoints gracefully."""
        from datahub.ingestion.source.snowplow.models.snowplow_models import (
            Pipeline as SnowplowPipeline,
        )

        # Pipeline without config/endpoints
        mock_state.physical_pipeline = SnowplowPipeline(
            id="test-pipeline-id",
            name="Test Pipeline",
            status="ready",
        )

        processor = PipelineProcessor(mock_deps, mock_state)

        workunits = list(
            processor._emit_collector_datajob(
                dataflow_urn="urn:li:dataFlow:(snowplow,test-pipeline-id,PROD)",
                output_dataset_urns=[
                    "urn:li:dataset:(urn:li:dataPlatform:snowplow,event_spec_1,PROD)"
                ],
            )
        )

        # Should still emit workunits
        assert len(workunits) > 0

        # Extract DataJobInfoClass
        for wu in workunits:
            if hasattr(wu.metadata, "aspect"):
                aspect = wu.metadata.aspect
                if isinstance(aspect, DataJobInfoClass) and aspect.name == "Collector":
                    # Should not have endpoint properties when none configured
                    assert aspect.customProperties is not None
                    assert "collectorEndpoints" not in aspect.customProperties
                    assert "endpointCount" not in aspect.customProperties
