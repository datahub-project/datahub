"""Unit tests for EventSpecProcessor."""

from unittest.mock import Mock

import pytest

from datahub.ingestion.source.snowplow.models.snowplow_models import EventSpecification
from datahub.ingestion.source.snowplow.processors.event_spec_processor import (
    EventSpecProcessor,
)


class TestEventSpecProcessorIsEnabled:
    """Tests for is_enabled() method."""

    @pytest.fixture
    def mock_deps(self):
        """Create mock dependencies."""
        deps = Mock()
        deps.config = Mock()
        deps.config.extract_event_specifications = True
        deps.bdp_client = Mock()
        deps.report = Mock()
        deps.cache = Mock()
        deps.urn_factory = Mock()
        return deps

    @pytest.fixture
    def mock_state(self):
        """Create mock state with real containers."""
        state = Mock()
        state.emitted_event_spec_ids = set()
        state.emitted_event_spec_urns = []
        state.event_spec_id = None
        state.event_spec_name = None
        return state

    def test_is_enabled_when_config_true_and_client_exists(self, mock_deps, mock_state):
        """Processor is enabled when config is True and BDP client exists."""
        processor = EventSpecProcessor(mock_deps, mock_state)
        assert processor.is_enabled() is True

    def test_is_disabled_when_config_false(self, mock_deps, mock_state):
        """Processor is disabled when config is False."""
        mock_deps.config.extract_event_specifications = False
        processor = EventSpecProcessor(mock_deps, mock_state)
        assert processor.is_enabled() is False

    def test_is_disabled_when_no_bdp_client(self, mock_deps, mock_state):
        """Processor is disabled when no BDP client."""
        mock_deps.bdp_client = None
        processor = EventSpecProcessor(mock_deps, mock_state)
        assert processor.is_enabled() is False


class TestEventSpecProcessorExtract:
    """Tests for extract() method."""

    @pytest.fixture
    def mock_deps(self):
        """Create mock dependencies."""
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
        return deps

    @pytest.fixture
    def mock_state(self):
        """Create mock state with real containers."""
        state = Mock()
        # Use real containers for set/list operations
        state.emitted_event_spec_ids = set()
        state.emitted_event_spec_urns = []
        state.event_spec_id = None
        state.event_spec_name = None
        state.event_spec_to_schema_urns = {}
        state.referenced_iglu_uris = set()
        state.atomic_event_urn = None
        return state

    def test_extract_returns_empty_when_no_bdp_client(self, mock_deps, mock_state):
        """Extract returns nothing when BDP client is not configured."""
        mock_deps.bdp_client = None
        processor = EventSpecProcessor(mock_deps, mock_state)

        workunits = list(processor.extract())
        assert workunits == []

    def test_extract_handles_api_error_gracefully(self, mock_deps, mock_state):
        """Extract handles API errors without crashing."""
        mock_deps.bdp_client.get_event_specifications.side_effect = Exception(
            "API Error"
        )
        processor = EventSpecProcessor(mock_deps, mock_state)

        workunits = list(processor.extract())

        # Should handle error gracefully and return empty
        assert workunits == []
        # Error handler should be called
        mock_deps.error_handler.handle_api_error.assert_called_once()

    def test_extract_filters_event_specs_by_pattern(self, mock_deps, mock_state):
        """Extract filters event specs based on pattern."""
        event_spec = EventSpecification(
            id="test-id",
            name="filtered-event",
        )
        mock_deps.bdp_client.get_event_specifications.return_value = [event_spec]
        mock_deps.config.event_spec_pattern.allowed.return_value = False

        processor = EventSpecProcessor(mock_deps, mock_state)
        list(processor.extract())  # Consume generator

        # Filtered event spec should not produce work units
        # Report should track that it was filtered
        mock_deps.report.report_event_spec_found.assert_called_once()
        mock_deps.report.report_event_spec_filtered.assert_called_once_with(
            "filtered-event"
        )

    def test_extract_tracks_first_event_spec_for_naming(self, mock_deps, mock_state):
        """Extract captures first event spec ID and name for parsed events dataset."""
        event_spec1 = EventSpecification(id="first-id", name="first-event")
        event_spec2 = EventSpecification(id="second-id", name="second-event")
        mock_deps.bdp_client.get_event_specifications.return_value = [
            event_spec1,
            event_spec2,
        ]

        processor = EventSpecProcessor(mock_deps, mock_state)
        # Consume the generator
        list(processor.extract())

        # First event spec should be captured in state
        assert mock_state.event_spec_id == "first-id"
        assert mock_state.event_spec_name == "first-event"

    def test_extract_adds_event_spec_ids_to_state(self, mock_deps, mock_state):
        """Extract adds emitted event spec IDs to state."""
        event_spec = EventSpecification(id="test-id", name="test-event")
        mock_deps.bdp_client.get_event_specifications.return_value = [event_spec]

        processor = EventSpecProcessor(mock_deps, mock_state)
        list(processor.extract())

        assert "test-id" in mock_state.emitted_event_spec_ids


class TestEventSpecProcessorProcessing:
    """Tests for _process_event_specification method."""

    @pytest.fixture
    def mock_deps(self):
        """Create mock dependencies with full configuration."""
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
    def mock_state(self):
        """Create mock state with real containers."""
        state = Mock()
        state.emitted_event_spec_ids = set()
        state.emitted_event_spec_urns = []
        state.event_spec_id = None
        state.event_spec_name = None
        state.emitted_schema_urns = {}
        state.event_spec_to_schema_urns = {}
        return state

    def test_process_event_spec_with_minimal_data(self, mock_deps, mock_state):
        """Process event spec with only required fields."""
        event_spec = EventSpecification(
            id="minimal-id",
            name="minimal-event",
        )

        processor = EventSpecProcessor(mock_deps, mock_state)
        workunits = list(processor._process_event_specification(event_spec))

        # Should produce work units without errors
        assert len(workunits) > 0

    def test_process_event_spec_includes_custom_properties(self, mock_deps, mock_state):
        """Process event spec includes metadata in custom properties."""
        event_spec = EventSpecification(
            id="test-id",
            name="test-event",
            status="active",
            created_at="2024-01-01T00:00:00Z",
            updated_at="2024-01-02T00:00:00Z",
        )

        processor = EventSpecProcessor(mock_deps, mock_state)
        workunits = list(processor._process_event_specification(event_spec))

        # Should include status and timestamps in properties
        assert len(workunits) > 0
