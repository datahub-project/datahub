"""Unit tests for DataProductProcessor."""

from unittest.mock import Mock

import pytest

from datahub.configuration.common import AllowDenyPattern
from datahub.ingestion.source.snowplow.processors.data_product_processor import (
    DataProductProcessor,
)


class TestDataProductProcessorIsEnabled:
    """Tests for is_enabled() method."""

    @pytest.fixture
    def mock_deps(self):
        """Create mock dependencies."""
        deps = Mock()
        deps.config = Mock()
        deps.config.extract_data_products = True
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
        processor = DataProductProcessor(mock_deps, mock_state)
        assert processor.is_enabled() is True

    def test_is_disabled_when_config_false(self, mock_deps, mock_state):
        """Processor is disabled when config is False."""
        mock_deps.config.extract_data_products = False
        processor = DataProductProcessor(mock_deps, mock_state)
        assert processor.is_enabled() is False

    def test_is_disabled_when_no_bdp_client(self, mock_deps, mock_state):
        """Processor is disabled when no BDP client."""
        mock_deps.bdp_client = None
        processor = DataProductProcessor(mock_deps, mock_state)
        assert processor.is_enabled() is False


class TestDataProductProcessorExtract:
    """Tests for extract() method."""

    @pytest.fixture
    def mock_deps(self):
        """Create mock dependencies with full configuration."""
        deps = Mock()
        deps.config = Mock()
        deps.config.extract_data_products = True
        deps.config.bdp_connection = Mock()
        deps.config.bdp_connection.organization_id = "test-org"
        deps.config.data_product_pattern = AllowDenyPattern.allow_all()
        deps.bdp_client = Mock()
        deps.report = Mock()
        deps.report.num_data_products_found = 0
        deps.report.num_data_products_filtered = 0
        deps.report.num_data_products_extracted = 0
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
        processor = DataProductProcessor(mock_deps, mock_state)

        workunits = list(processor.extract())
        assert workunits == []

    def test_extract_handles_api_error_gracefully(self, mock_deps, mock_state):
        """Extract handles API errors without crashing."""
        mock_deps.bdp_client.get_data_products.side_effect = Exception("API Error")
        processor = DataProductProcessor(mock_deps, mock_state)

        workunits = list(processor.extract())

        # Should handle error gracefully and return empty
        assert workunits == []
        # Error handler should be called
        mock_deps.error_handler.handle_api_error.assert_called_once()

    def test_extract_filters_products_by_pattern(self, mock_deps, mock_state):
        """Extract filters data products based on pattern."""
        product = Mock()
        product.id = "filtered-product"
        product.name = "Filtered Product"
        product.description = None
        product.owner = None
        product.event_specs = []
        mock_deps.bdp_client.get_data_products.return_value = [product]
        mock_deps.config.data_product_pattern = AllowDenyPattern(
            deny=["filtered-product"]
        )

        processor = DataProductProcessor(mock_deps, mock_state)
        list(processor.extract())

        # Filtered product should not produce work units
        assert mock_deps.report.num_data_products_filtered == 1
        assert mock_deps.report.num_data_products_extracted == 0

    def test_extract_creates_container_for_product(self, mock_deps, mock_state):
        """Extract creates container work units for data products."""
        product = Mock()
        product.id = "product-123"
        product.name = "Test Product"
        product.description = "Test description"
        product.owner = None
        product.event_specs = []
        product.access_instructions = None
        product.source_applications = None
        product.type = None
        product.lock_status = None
        product.status = None
        product.created_at = None
        product.updated_at = None
        mock_deps.bdp_client.get_data_products.return_value = [product]

        processor = DataProductProcessor(mock_deps, mock_state)
        workunits = list(processor.extract())

        # Should produce at least one work unit (container)
        assert len(workunits) > 0
        assert mock_deps.report.num_data_products_extracted == 1

    def test_extract_adds_ownership_when_owner_specified(self, mock_deps, mock_state):
        """Extract adds ownership aspect when product has owner."""
        product = Mock()
        product.id = "product-123"
        product.name = "Test Product"
        product.description = None
        product.owner = "alice@example.com"
        product.event_specs = []
        product.access_instructions = None
        product.source_applications = None
        product.type = None
        product.lock_status = None
        product.status = None
        product.created_at = None
        product.updated_at = None
        mock_deps.bdp_client.get_data_products.return_value = [product]

        processor = DataProductProcessor(mock_deps, mock_state)
        workunits = list(processor.extract())

        # Should produce ownership work unit in addition to container
        # gen_containers produces multiple workunits, plus ownership
        assert len(workunits) >= 1
        # Check that at least one workunit has ownership aspect
        ownership_workunits = [
            wu
            for wu in workunits
            if hasattr(wu.metadata, "aspect") and hasattr(wu.metadata.aspect, "owners")
        ]
        assert len(ownership_workunits) == 1

    def test_extract_links_event_specs_to_container(self, mock_deps, mock_state):
        """Extract creates container links for event specifications."""
        event_spec_ref = Mock()
        event_spec_ref.id = "event-spec-123"

        product = Mock()
        product.id = "product-123"
        product.name = "Test Product"
        product.description = None
        product.owner = None
        product.event_specs = [event_spec_ref]
        product.access_instructions = None
        product.source_applications = None
        product.type = None
        product.lock_status = None
        product.status = None
        product.created_at = None
        product.updated_at = None
        mock_deps.bdp_client.get_data_products.return_value = [product]

        # Simulate event spec was emitted
        mock_state.emitted_event_spec_ids = {"event-spec-123"}

        processor = DataProductProcessor(mock_deps, mock_state)
        workunits = list(processor.extract())

        # Should include container link work unit
        container_workunits = [
            wu
            for wu in workunits
            if hasattr(wu.metadata, "aspect")
            and hasattr(wu.metadata.aspect, "container")
        ]
        assert len(container_workunits) == 1

    def test_extract_skips_untracked_event_spec_links(self, mock_deps, mock_state):
        """Extract skips container links for filtered event specs."""
        event_spec_ref = Mock()
        event_spec_ref.id = "filtered-event-spec"

        product = Mock()
        product.id = "product-123"
        product.name = "Test Product"
        product.description = None
        product.owner = None
        product.event_specs = [event_spec_ref]
        product.access_instructions = None
        product.source_applications = None
        product.type = None
        product.lock_status = None
        product.status = None
        product.created_at = None
        product.updated_at = None
        mock_deps.bdp_client.get_data_products.return_value = [product]

        # Event spec was NOT emitted (filtered)
        mock_state.emitted_event_spec_ids = set()

        processor = DataProductProcessor(mock_deps, mock_state)
        workunits = list(processor.extract())

        # Should NOT include container link work unit
        container_workunits = [
            wu
            for wu in workunits
            if hasattr(wu.metadata, "aspect")
            and hasattr(wu.metadata.aspect, "container")
        ]
        assert len(container_workunits) == 0

    def test_extract_includes_custom_properties(self, mock_deps, mock_state):
        """Extract includes custom properties in container metadata."""
        product = Mock()
        product.id = "product-123"
        product.name = "Test Product"
        product.description = "Test description"
        product.owner = None
        product.event_specs = []
        product.access_instructions = "Contact team X"
        product.source_applications = ["web", "mobile"]
        product.type = "tracking_plan"
        product.lock_status = "unlocked"
        product.status = "active"
        product.created_at = "2024-01-01T00:00:00Z"
        product.updated_at = "2024-01-02T00:00:00Z"
        mock_deps.bdp_client.get_data_products.return_value = [product]

        processor = DataProductProcessor(mock_deps, mock_state)
        workunits = list(processor.extract())

        # Work units should be generated with the properties
        assert len(workunits) > 0
        assert mock_deps.report.num_data_products_extracted == 1
