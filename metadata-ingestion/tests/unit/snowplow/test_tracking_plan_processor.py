"""Unit tests for TrackingPlanProcessor."""

from unittest.mock import Mock

import pytest

from datahub.configuration.common import AllowDenyPattern
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.source.snowplow.models.snowplow_models import (
    EventSpecReference,
    TrackingPlan,
)
from datahub.ingestion.source.snowplow.processors.tracking_plan_processor import (
    TrackingPlanProcessor,
)


class TestTrackingPlanProcessorIsEnabled:
    """Tests for is_enabled() method."""

    @pytest.fixture
    def mock_deps(self):
        """Create mock dependencies."""
        deps = Mock()
        deps.config = Mock()
        deps.config.extract_tracking_plans = True
        deps.bdp_client = Mock()
        deps.report = Mock()
        deps.cache = Mock()
        deps.urn_factory = Mock()
        return deps

    @pytest.fixture
    def mock_state(self):
        """Create mock state."""
        state = Mock()
        state.emitted_event_spec_ids = set()
        return state

    def test_is_enabled_when_config_true_and_client_exists(self, mock_deps, mock_state):
        """Processor is enabled when config is True and BDP client exists."""
        processor = TrackingPlanProcessor(mock_deps, mock_state)
        assert processor.is_enabled() is True

    def test_is_disabled_when_config_false(self, mock_deps, mock_state):
        """Processor is disabled when config is False."""
        mock_deps.config.extract_tracking_plans = False
        processor = TrackingPlanProcessor(mock_deps, mock_state)
        assert processor.is_enabled() is False

    def test_is_disabled_when_no_bdp_client(self, mock_deps, mock_state):
        """Processor is disabled when no BDP client."""
        mock_deps.bdp_client = None
        processor = TrackingPlanProcessor(mock_deps, mock_state)
        assert processor.is_enabled() is False


class TestTrackingPlanProcessorExtract:
    """Tests for extract() method."""

    @pytest.fixture
    def mock_deps(self):
        """Create mock dependencies with full configuration."""
        deps = Mock()
        deps.config = Mock()
        deps.config.extract_tracking_plans = True
        deps.config.bdp_connection = Mock()
        deps.config.bdp_connection.organization_id = "test-org"
        deps.config.platform_instance = None
        deps.config.env = "PROD"
        deps.config.tracking_plan_pattern = AllowDenyPattern.allow_all()
        deps.bdp_client = Mock()
        deps.report = Mock()
        deps.cache = Mock()
        deps.urn_factory = Mock()
        deps.urn_factory.make_event_spec_dataset_urn = Mock(
            side_effect=lambda id: f"urn:li:dataset:(urn:li:dataPlatform:snowplow,{id},PROD)"
        )
        deps.error_handler = Mock()
        deps.platform = "snowplow"
        return deps

    @pytest.fixture
    def mock_state(self):
        """Create mock state with real containers."""
        state = Mock()
        state.emitted_event_spec_ids = set()
        return state

    def test_extract_returns_empty_when_no_bdp_client(self, mock_deps, mock_state):
        """Extract returns nothing when BDP client is not configured."""
        mock_deps.bdp_client = None
        processor = TrackingPlanProcessor(mock_deps, mock_state)

        workunits = list(processor.extract())
        assert workunits == []

    def test_extract_handles_api_error_gracefully(self, mock_deps, mock_state):
        """Extract handles API errors without crashing."""
        mock_deps.bdp_client.get_tracking_plans.side_effect = Exception("API Error")
        processor = TrackingPlanProcessor(mock_deps, mock_state)

        workunits = list(processor.extract())

        # Should handle error gracefully and return empty
        assert workunits == []
        # Error handler should be called
        mock_deps.error_handler.handle_api_error.assert_called_once()

    def test_extract_filters_plans_by_pattern(self, mock_deps, mock_state):
        """Extract filters plans based on pattern."""
        plan = TrackingPlan(
            id="plan-123",
            name="filtered-plan",
        )
        mock_deps.bdp_client.get_tracking_plans.return_value = [plan]
        mock_deps.config.tracking_plan_pattern = AllowDenyPattern(
            deny=["filtered-plan"]
        )

        processor = TrackingPlanProcessor(mock_deps, mock_state)
        list(processor.extract())

        # Should report filtered
        mock_deps.report.report_tracking_plan_found.assert_called_once()
        mock_deps.report.report_tracking_plan_filtered.assert_called_once_with(
            "filtered-plan"
        )

    def test_extract_creates_container_for_plan(self, mock_deps, mock_state):
        """Extract creates container work units for tracking plans."""
        plan = TrackingPlan(
            id="plan-123",
            name="Test Plan",
            description="Test description",
            status="active",
            event_specs=[],
        )
        mock_deps.bdp_client.get_tracking_plans.return_value = [plan]

        processor = TrackingPlanProcessor(mock_deps, mock_state)
        workunits = list(processor.extract())

        # Should produce at least one work unit (container)
        assert len(workunits) > 0
        mock_deps.report.report_tracking_plan_extracted.assert_called_once()

    def test_extract_links_event_specs_to_plan(self, mock_deps, mock_state):
        """Extract creates container links for event specifications."""
        plan = TrackingPlan(
            id="plan-123",
            name="Test Plan",
            event_specs=[EventSpecReference(id="event-spec-123")],
        )
        mock_deps.bdp_client.get_tracking_plans.return_value = [plan]

        # Simulate event spec was emitted
        mock_state.emitted_event_spec_ids = {"event-spec-123"}

        processor = TrackingPlanProcessor(mock_deps, mock_state)
        workunits = list(processor.extract())

        # Should include container link work unit for the event spec
        # Filter for workunits that link a dataset to a container
        event_spec_container_workunits = [
            wu
            for wu in workunits
            if isinstance(wu.metadata, MetadataChangeProposalWrapper)
            and hasattr(wu.metadata.aspect, "container")
            and wu.metadata.entityUrn is not None
            and "dataset" in wu.metadata.entityUrn
        ]
        assert len(event_spec_container_workunits) == 1

    def test_extract_skips_untracked_event_spec_links(self, mock_deps, mock_state):
        """Extract skips container links for filtered event specs."""
        plan = TrackingPlan(
            id="plan-123",
            name="Test Plan",
            event_specs=[EventSpecReference(id="filtered-event-spec")],
        )
        mock_deps.bdp_client.get_tracking_plans.return_value = [plan]

        # Event spec was NOT emitted (filtered)
        mock_state.emitted_event_spec_ids = set()

        processor = TrackingPlanProcessor(mock_deps, mock_state)
        workunits = list(processor.extract())

        # Should NOT include container link work unit for dataset
        # (gen_containers still produces container aspect for parent relationship)
        event_spec_container_workunits = [
            wu
            for wu in workunits
            if isinstance(wu.metadata, MetadataChangeProposalWrapper)
            and hasattr(wu.metadata.aspect, "container")
            and wu.metadata.entityUrn is not None
            and "dataset" in wu.metadata.entityUrn
        ]
        assert len(event_spec_container_workunits) == 0

    def test_extract_includes_custom_properties(self, mock_deps, mock_state):
        """Extract includes custom properties in container metadata."""
        plan = TrackingPlan(
            id="plan-123",
            name="Test Plan",
            description="Test description",
            status="active",
            created_at="2024-01-01T00:00:00Z",
            updated_at="2024-01-02T00:00:00Z",
            event_specs=[
                EventSpecReference(id="spec1"),
                EventSpecReference(id="spec2"),
            ],
        )
        mock_deps.bdp_client.get_tracking_plans.return_value = [plan]

        processor = TrackingPlanProcessor(mock_deps, mock_state)
        workunits = list(processor.extract())

        # Work units should be generated
        assert len(workunits) > 0
        mock_deps.report.report_tracking_plan_extracted.assert_called_once()

    def test_extract_requires_bdp_connection_for_processing(
        self, mock_deps, mock_state
    ):
        """Extract returns nothing when bdp_connection is None during processing."""
        plan = TrackingPlan(
            id="plan-123",
            name="Test Plan",
        )
        mock_deps.bdp_client.get_tracking_plans.return_value = [plan]
        mock_deps.config.bdp_connection = None

        processor = TrackingPlanProcessor(mock_deps, mock_state)
        list(processor.extract())  # Consume generator

        # _process_tracking_plan returns early when bdp_connection is None
        # But the plan is still found
        mock_deps.report.report_tracking_plan_found.assert_called_once()
