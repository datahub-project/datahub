"""
Tests for feature flag combinations in Snowplow connector.

These tests verify that different combinations of extraction flags work correctly,
especially when processors depend on shared state from other processors.

Bug Prevention:
- Bug 1: Enrichments not created when extract_event_specifications=False
- Bug 2: Only first event spec processed due to loop mutation
"""

from typing import List
from unittest.mock import Mock

import pytest

from datahub.ingestion.source.snowplow.dependencies import IngestionState
from datahub.ingestion.source.snowplow.models.snowplow_models import (
    Enrichment,
    EnrichmentContent,
    EnrichmentContentData,
    EventSchemaDetail,
    EventSpecification,
    Pipeline as SnowplowPipeline,
    PipelineConfig,
)
from datahub.ingestion.source.snowplow.processors.event_spec_processor import (
    EventSpecProcessor,
)
from datahub.ingestion.source.snowplow.processors.pipeline_processor import (
    PipelineProcessor,
)


class TestEnrichmentsWithoutEventSpecProcessor:
    """
    Test that enrichments work when EventSpecProcessor is disabled.

    This tests Bug 1: When extract_event_specifications=False but extract_enrichments=True,
    enrichments should still be created by populating event spec IDs in PipelineProcessor.
    """

    @pytest.fixture
    def mock_deps(self):
        """Create mock dependencies for pipeline processor."""
        deps = Mock()
        deps.config = Mock()
        # Key config: extract_event_specifications is FALSE but extract_enrichments is TRUE
        deps.config.extract_event_specifications = False
        deps.config.extract_pipelines = True
        deps.config.extract_enrichments = True
        deps.config.env = "PROD"
        deps.config.platform_instance = None
        deps.config.bdp_connection = Mock()
        deps.config.bdp_connection.organization_id = "test-org"
        deps.config.enrichment_owner = None
        deps.bdp_client = Mock()
        deps.report = Mock()
        deps.cache = Mock()
        deps.cache.get = Mock(return_value=None)
        deps.cache.set = Mock()
        deps.urn_factory = Mock()
        deps.urn_factory.make_organization_urn = Mock(
            return_value="urn:li:container:org"
        )
        deps.urn_factory.make_event_spec_dataset_urn = Mock(
            side_effect=lambda id: f"urn:li:dataset:(urn:li:dataPlatform:snowplow,event_spec_{id},PROD)"
        )
        deps.error_handler = Mock()
        deps.platform = "snowplow"
        deps.ownership_builder = Mock()
        deps.ownership_builder.build_owners = Mock(return_value=[])
        deps.enrichment_lineage_registry = Mock()
        deps.enrichment_lineage_registry.get_extractor = Mock(return_value=None)
        deps.lineage_builder = Mock()
        return deps

    @pytest.fixture
    def mock_state(self) -> IngestionState:
        """Create real IngestionState (not mocked)."""
        return IngestionState()

    @pytest.fixture
    def sample_event_specs(self) -> List[EventSpecification]:
        """Create sample event specifications."""
        return [
            EventSpecification(
                id="spec-1",
                name="Checkout Events",
                event=EventSchemaDetail(
                    source="iglu:com.acme/checkout_started/jsonschema/1-0-0"
                ),
            ),
            EventSpecification(
                id="spec-2",
                name="Product Events",
                event=EventSchemaDetail(
                    source="iglu:com.acme/product_viewed/jsonschema/1-0-0"
                ),
            ),
            EventSpecification(
                id="spec-3",
                name="User Events",
                event=EventSchemaDetail(
                    source="iglu:com.acme/user_signup/jsonschema/1-0-0"
                ),
            ),
        ]

    @pytest.fixture
    def sample_pipeline(self) -> SnowplowPipeline:
        """Create sample pipeline."""
        return SnowplowPipeline(
            id="pipeline-123",
            name="Test Pipeline",
            status="active",
            workspace_id="workspace-123",
            config=PipelineConfig(
                collector_endpoints=["collector.example.com"],
                incomplete_stream_deployed=False,
                enrich_accept_invalid=False,
            ),
        )

    @pytest.fixture
    def sample_enrichments(self) -> List[Enrichment]:
        """Create sample enrichments."""
        return [
            Enrichment(
                id="enrichment-1",
                filename="ip_lookup.json",
                enabled=True,
                last_update="2024-01-01T00:00:00Z",
                content=EnrichmentContent(
                    data=EnrichmentContentData(
                        enabled=True,
                        name="ip_lookups",
                        vendor="com.snowplowanalytics.snowplow",
                        parameters={},
                    ),
                    schema_ref="iglu:com.snowplowanalytics.snowplow/ip_lookups/jsonschema/2-0-0",
                ),
            ),
            Enrichment(
                id="enrichment-2",
                filename="ua_parser.json",
                enabled=True,
                last_update="2024-01-01T00:00:00Z",
                content=EnrichmentContent(
                    data=EnrichmentContentData(
                        enabled=True,
                        name="ua_parser_config",
                        vendor="com.snowplowanalytics.snowplow",
                        parameters={},
                    ),
                    schema_ref="iglu:com.snowplowanalytics.snowplow/ua_parser_config/jsonschema/1-0-1",
                ),
            ),
        ]

    def test_enrichments_created_when_event_spec_processor_disabled(
        self,
        mock_deps,
        mock_state,
        sample_event_specs,
        sample_pipeline,
        sample_enrichments,
    ):
        """
        Test that enrichments are created even when extract_event_specifications=False.

        With OPTION A (Single Physical Pipeline) architecture:
        1. PipelineProcessor._extract_pipelines() creates single pipeline DataFlow
        2. PipelineProcessor._extract_pipelines() populates emitted_event_spec_ids
        3. PipelineProcessor._extract_enrichments() creates enrichments ONCE for pipeline
        """
        # Setup mocks
        mock_deps.bdp_client.get_event_specifications.return_value = sample_event_specs
        mock_deps.bdp_client.get_pipelines.return_value = [sample_pipeline]
        mock_deps.bdp_client.get_enrichments.return_value = sample_enrichments
        mock_deps.bdp_client.get_destinations.return_value = []

        # Create processor and extract
        processor = PipelineProcessor(mock_deps, mock_state)
        list(processor.extract())  # Consume the generator

        # Verify all event specs were registered (even though EventSpecProcessor is disabled)
        assert len(mock_state.emitted_event_spec_ids) == 3
        assert "spec-1" in mock_state.emitted_event_spec_ids
        assert "spec-2" in mock_state.emitted_event_spec_ids
        assert "spec-3" in mock_state.emitted_event_spec_ids

        # Verify single pipeline DataFlow URN was created (not per-event-spec)
        assert mock_state.pipeline_dataflow_urn is not None
        assert "pipeline-123" in mock_state.pipeline_dataflow_urn

        # Verify enrichments were extracted ONCE for the pipeline
        # Option A: 2 enrichments (shared by all 3 event specs)
        assert mock_deps.report.report_enrichment_found.call_count == 2
        assert mock_deps.report.report_enrichment_extracted.call_count == 2

    def test_single_pipeline_dataflow_created(
        self,
        mock_deps,
        mock_state,
        sample_event_specs,
        sample_pipeline,
    ):
        """
        Test that a SINGLE pipeline DataFlow is created (Option A architecture).

        With Option A, we create one DataFlow for the physical pipeline,
        not one per event spec. All event specs share this DataFlow.
        """
        # Setup mocks (no enrichments, just test DataFlow creation)
        mock_deps.config.extract_enrichments = False
        mock_deps.bdp_client.get_event_specifications.return_value = sample_event_specs
        mock_deps.bdp_client.get_pipelines.return_value = [sample_pipeline]

        # Create processor and extract
        processor = PipelineProcessor(mock_deps, mock_state)
        list(processor.extract())

        # Verify SINGLE pipeline DataFlow created (not per-event-spec)
        assert mock_state.pipeline_dataflow_urn is not None
        assert "pipeline-123" in mock_state.pipeline_dataflow_urn

        # All event specs tracked for enrichment input
        assert len(mock_state.emitted_event_spec_ids) == 3
        assert "spec-1" in mock_state.emitted_event_spec_ids
        assert "spec-2" in mock_state.emitted_event_spec_ids
        assert "spec-3" in mock_state.emitted_event_spec_ids

        # Single pipeline extracted
        assert mock_deps.report.report_pipeline_extracted.call_count == 1

    def test_event_spec_processor_state_preserved_by_pipeline_processor(
        self,
        mock_deps,
        mock_state,
        sample_event_specs,
        sample_pipeline,
    ):
        """
        Test that when EventSpecProcessor has already run, PipelineProcessor preserves state.

        When extract_event_specifications=True and EventSpecProcessor has already run,
        PipelineProcessor should preserve the emitted_event_spec_ids.
        """
        # Enable EventSpecProcessor
        mock_deps.config.extract_event_specifications = True
        mock_deps.config.extract_enrichments = False

        # Simulate EventSpecProcessor having emitted only spec-1 and spec-3
        mock_state.emitted_event_spec_ids.add("spec-1")
        mock_state.emitted_event_spec_ids.add("spec-3")

        # Setup mocks
        mock_deps.bdp_client.get_event_specifications.return_value = sample_event_specs
        mock_deps.bdp_client.get_pipelines.return_value = [sample_pipeline]

        # Create processor and extract
        processor = PipelineProcessor(mock_deps, mock_state)
        list(processor.extract())

        # Verify single pipeline DataFlow created
        assert mock_state.pipeline_dataflow_urn is not None

        # Verify emitted_event_spec_ids preserved (not overwritten)
        assert len(mock_state.emitted_event_spec_ids) == 2
        assert "spec-1" in mock_state.emitted_event_spec_ids
        assert "spec-3" in mock_state.emitted_event_spec_ids
        assert "spec-2" not in mock_state.emitted_event_spec_ids

        # Single pipeline extracted
        assert mock_deps.report.report_pipeline_extracted.call_count == 1


class TestAllEventSpecsProcessed:
    """
    Test that ALL event specs are processed by EventSpecProcessor.

    This tests Bug 2: The previous test test_extract_tracks_first_event_spec_for_naming
    only verified the FIRST event spec was tracked, not that ALL were processed.
    """

    @pytest.fixture
    def mock_deps(self):
        """Create mock dependencies for event spec processor."""
        deps = Mock()
        deps.config = Mock()
        deps.config.extract_event_specifications = True
        deps.config.bdp_connection = Mock()
        deps.config.bdp_connection.organization_id = "test-org"
        deps.config.event_spec_pattern = Mock()
        deps.config.event_spec_pattern.allowed = Mock(return_value=True)
        deps.bdp_client = Mock()
        deps.report = Mock()
        deps.cache = Mock()
        deps.urn_factory = Mock()
        deps.urn_factory.make_event_spec_dataset_urn = Mock(
            side_effect=lambda id: f"urn:li:dataset:(urn:li:dataPlatform:snowplow,{id},PROD)"
        )
        deps.error_handler = Mock()
        deps.ownership_builder = Mock()
        deps.ownership_builder.build_owners = Mock(return_value=[])
        deps.platform = "snowplow"
        return deps

    @pytest.fixture
    def mock_state(self) -> IngestionState:
        """Create real IngestionState (not mocked)."""
        return IngestionState()

    def test_all_event_spec_ids_are_registered(self, mock_deps, mock_state):
        """
        Test that ALL event spec IDs are added to emitted_event_spec_ids.

        Previous test only verified first event spec was captured for naming.
        This test verifies ALL event specs are tracked for downstream processors.
        """
        event_specs = [
            EventSpecification(id="first-id", name="first-event"),
            EventSpecification(id="second-id", name="second-event"),
            EventSpecification(id="third-id", name="third-event"),
            EventSpecification(id="fourth-id", name="fourth-event"),
        ]
        mock_deps.bdp_client.get_event_specifications.return_value = event_specs

        processor = EventSpecProcessor(mock_deps, mock_state)
        list(processor.extract())

        # Verify ALL event spec IDs are in the set
        assert len(mock_state.emitted_event_spec_ids) == 4
        assert "first-id" in mock_state.emitted_event_spec_ids
        assert "second-id" in mock_state.emitted_event_spec_ids
        assert "third-id" in mock_state.emitted_event_spec_ids
        assert "fourth-id" in mock_state.emitted_event_spec_ids

    def test_all_event_spec_urns_are_registered(self, mock_deps, mock_state):
        """Test that ALL event spec URNs are tracked."""
        event_specs = [
            EventSpecification(id="spec-a", name="Event A"),
            EventSpecification(id="spec-b", name="Event B"),
            EventSpecification(id="spec-c", name="Event C"),
        ]
        mock_deps.bdp_client.get_event_specifications.return_value = event_specs

        processor = EventSpecProcessor(mock_deps, mock_state)
        list(processor.extract())

        # Verify ALL event spec URNs are tracked
        assert len(mock_state.emitted_event_spec_urns) == 3

    def test_report_tracks_all_event_specs(self, mock_deps, mock_state):
        """Test that report tracks ALL event specs found and extracted."""
        event_specs = [
            EventSpecification(id="id-1", name="Event 1"),
            EventSpecification(id="id-2", name="Event 2"),
            EventSpecification(id="id-3", name="Event 3"),
        ]
        mock_deps.bdp_client.get_event_specifications.return_value = event_specs

        processor = EventSpecProcessor(mock_deps, mock_state)
        list(processor.extract())

        # Verify report was called for ALL event specs
        assert mock_deps.report.report_event_spec_found.call_count == 3


class TestProcessorCoordination:
    """
    Test that processors coordinate correctly via shared state.

    Tests the full flow: EventSpecProcessor → PipelineProcessor
    """

    @pytest.fixture
    def event_spec_deps(self):
        """Create mock dependencies for EventSpecProcessor."""
        deps = Mock()
        deps.config = Mock()
        deps.config.extract_event_specifications = True
        deps.config.bdp_connection = Mock()
        deps.config.bdp_connection.organization_id = "test-org"
        deps.config.event_spec_pattern = Mock()
        deps.config.event_spec_pattern.allowed = Mock(return_value=True)
        deps.bdp_client = Mock()
        deps.report = Mock()
        deps.cache = Mock()
        deps.urn_factory = Mock()
        deps.urn_factory.make_event_spec_dataset_urn = Mock(
            side_effect=lambda id: f"urn:li:dataset:(urn:li:dataPlatform:snowplow,{id},PROD)"
        )
        deps.error_handler = Mock()
        deps.ownership_builder = Mock()
        deps.ownership_builder.build_owners = Mock(return_value=[])
        deps.platform = "snowplow"
        return deps

    @pytest.fixture
    def pipeline_deps(self):
        """Create mock dependencies for PipelineProcessor."""
        deps = Mock()
        deps.config = Mock()
        deps.config.extract_event_specifications = True
        deps.config.extract_pipelines = True
        deps.config.extract_enrichments = True
        deps.config.env = "PROD"
        deps.config.platform_instance = None
        deps.config.bdp_connection = Mock()
        deps.config.bdp_connection.organization_id = "test-org"
        deps.config.enrichment_owner = None
        deps.bdp_client = Mock()
        deps.report = Mock()
        deps.cache = Mock()
        deps.cache.get = Mock(return_value=None)
        deps.cache.set = Mock()
        deps.urn_factory = Mock()
        deps.urn_factory.make_organization_urn = Mock(
            return_value="urn:li:container:org"
        )
        deps.urn_factory.make_event_spec_dataset_urn = Mock(
            side_effect=lambda id: f"urn:li:dataset:(urn:li:dataPlatform:snowplow,event_spec_{id},PROD)"
        )
        deps.error_handler = Mock()
        deps.platform = "snowplow"
        deps.ownership_builder = Mock()
        deps.ownership_builder.build_owners = Mock(return_value=[])
        deps.enrichment_lineage_registry = Mock()
        deps.enrichment_lineage_registry.get_extractor = Mock(return_value=None)
        deps.lineage_builder = Mock()
        return deps

    @pytest.fixture
    def shared_state(self) -> IngestionState:
        """Create shared IngestionState for both processors."""
        return IngestionState()

    def test_event_spec_processor_state_available_to_pipeline_processor(
        self, event_spec_deps, pipeline_deps, shared_state
    ):
        """
        Test that EventSpecProcessor populates state that PipelineProcessor can use.

        This simulates the real flow where EventSpecProcessor runs first,
        then PipelineProcessor uses the populated state.

        With Option A (Single Physical Pipeline) architecture:
        - Single DataFlow is created for the pipeline
        - All event specs' URNs become inputs to enrichments
        """
        # Setup event specs
        event_specs = [
            EventSpecification(
                id="spec-1",
                name="Event 1",
                event=EventSchemaDetail(source="iglu:com.acme/event1/jsonschema/1-0-0"),
            ),
            EventSpecification(
                id="spec-2",
                name="Event 2",
                event=EventSchemaDetail(source="iglu:com.acme/event2/jsonschema/1-0-0"),
            ),
        ]

        sample_pipeline = SnowplowPipeline(
            id="pipeline-123",
            name="Test Pipeline",
            status="active",
            workspace_id="workspace-123",
        )

        enrichments = [
            Enrichment(
                id="enrichment-1",
                filename="ip_lookup.json",
                enabled=True,
                last_update="2024-01-01T00:00:00Z",
            ),
        ]

        # Setup mocks for both processors
        event_spec_deps.bdp_client.get_event_specifications.return_value = event_specs
        pipeline_deps.bdp_client.get_event_specifications.return_value = event_specs
        pipeline_deps.bdp_client.get_pipelines.return_value = [sample_pipeline]
        pipeline_deps.bdp_client.get_enrichments.return_value = enrichments
        pipeline_deps.bdp_client.get_destinations.return_value = []

        # Step 1: Run EventSpecProcessor
        event_spec_processor = EventSpecProcessor(event_spec_deps, shared_state)
        list(event_spec_processor.extract())

        # Verify state is populated
        assert len(shared_state.emitted_event_spec_ids) == 2
        assert "spec-1" in shared_state.emitted_event_spec_ids
        assert "spec-2" in shared_state.emitted_event_spec_ids

        # Step 2: Run PipelineProcessor
        pipeline_processor = PipelineProcessor(pipeline_deps, shared_state)
        list(pipeline_processor.extract())

        # Verify PipelineProcessor created single pipeline DataFlow (Option A)
        assert shared_state.pipeline_dataflow_urn is not None
        assert "pipeline-123" in shared_state.pipeline_dataflow_urn

        # Verify enrichments were created ONCE for the pipeline
        # Option A: 1 enrichment (shared by all 2 event specs)
        assert pipeline_deps.report.report_enrichment_extracted.call_count == 1

    def test_filtered_event_specs_affect_enrichment_inputs(
        self, event_spec_deps, pipeline_deps, shared_state
    ):
        """
        Test that filtered event specs don't become enrichment inputs.

        If EventSpecProcessor filters out an event spec (via pattern),
        PipelineProcessor should only use allowed event specs as enrichment inputs.

        With Option A: Enrichments are created once, but only allowed event specs
        are used as inputs.
        """
        # Setup event specs
        event_specs = [
            EventSpecification(
                id="spec-allowed",
                name="Allowed Event",
                event=EventSchemaDetail(
                    source="iglu:com.acme/allowed/jsonschema/1-0-0"
                ),
            ),
            EventSpecification(
                id="spec-filtered",
                name="Filtered Event",
                event=EventSchemaDetail(
                    source="iglu:com.acme/filtered/jsonschema/1-0-0"
                ),
            ),
        ]

        sample_pipeline = SnowplowPipeline(
            id="pipeline-123",
            name="Test Pipeline",
            status="active",
            workspace_id="workspace-123",
        )

        enrichments = [
            Enrichment(
                id="enrichment-1",
                filename="ip_lookup.json",
                enabled=True,
                last_update="2024-01-01T00:00:00Z",
            ),
        ]

        # Configure filter to only allow "Allowed Event"
        def pattern_filter(name):
            return name == "Allowed Event"

        event_spec_deps.config.event_spec_pattern.allowed = Mock(
            side_effect=pattern_filter
        )

        # Setup mocks
        event_spec_deps.bdp_client.get_event_specifications.return_value = event_specs
        pipeline_deps.bdp_client.get_event_specifications.return_value = event_specs
        pipeline_deps.bdp_client.get_pipelines.return_value = [sample_pipeline]
        pipeline_deps.bdp_client.get_enrichments.return_value = enrichments
        pipeline_deps.bdp_client.get_destinations.return_value = []

        # Run EventSpecProcessor
        event_spec_processor = EventSpecProcessor(event_spec_deps, shared_state)
        list(event_spec_processor.extract())

        # Verify only allowed event spec is in state
        assert len(shared_state.emitted_event_spec_ids) == 1
        assert "spec-allowed" in shared_state.emitted_event_spec_ids
        assert "spec-filtered" not in shared_state.emitted_event_spec_ids

        # Run PipelineProcessor
        pipeline_processor = PipelineProcessor(pipeline_deps, shared_state)
        list(pipeline_processor.extract())

        # Verify single pipeline DataFlow created (Option A)
        assert shared_state.pipeline_dataflow_urn is not None

        # Verify enrichments created once (only allowed event spec as input)
        # Option A: 1 enrichment
        assert pipeline_deps.report.report_enrichment_extracted.call_count == 1
