"""Unit tests for WarehouseLineageProcessor."""

from unittest.mock import Mock

import pytest

from datahub.ingestion.source.snowplow.processors.warehouse_lineage_processor import (
    WarehouseLineageProcessor,
)
from datahub.ingestion.source.snowplow.snowplow_config import (
    SnowplowSourceConfig,
    WarehouseLineageConfig,
)
from datahub.ingestion.source.snowplow.snowplow_models import (
    DataModel,
    DataProduct,
)
from datahub.metadata.schema_classes import UpstreamLineageClass


class TestWarehouseLineageProcessor:
    """Test warehouse lineage extraction business logic."""

    @pytest.fixture
    def mock_deps(self):
        """Create mock dependencies."""
        deps = Mock()
        deps.bdp_client = Mock()
        deps.graph = Mock()
        deps.platform = "snowplow"
        return deps

    @pytest.fixture
    def mock_state(self):
        """Create mock state."""
        state = Mock()
        state.warehouse_table_urn = (
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.events,PROD)"
        )
        return state

    @pytest.fixture
    def config_with_lineage(self):
        """Create config with warehouse lineage enabled."""
        config = Mock(spec=SnowplowSourceConfig)
        config.warehouse_lineage = WarehouseLineageConfig(
            enabled=True,
            validate_urns=False,
            env="PROD",
        )
        config.bdp_connection = Mock()
        config.bdp_connection.organization_id = "test-org"
        return config

    @pytest.fixture
    def processor(self, mock_deps, mock_state, config_with_lineage):
        """Create processor instance."""
        processor = WarehouseLineageProcessor(mock_deps, mock_state)
        processor.config = config_with_lineage
        processor.urn_factory = Mock()
        processor.report = Mock()
        return processor

    def test_is_enabled_when_warehouse_lineage_enabled_and_bdp_client_available(
        self, processor
    ):
        """Test processor is enabled when warehouse lineage config is enabled and BDP client available."""
        assert processor.is_enabled() is True

    def test_is_enabled_when_warehouse_lineage_disabled(
        self, mock_deps, mock_state, config_with_lineage
    ):
        """Test processor is disabled when warehouse lineage config is disabled."""
        config_with_lineage.warehouse_lineage.enabled = False
        processor = WarehouseLineageProcessor(mock_deps, mock_state)
        processor.config = config_with_lineage

        assert processor.is_enabled() is False

    def test_is_enabled_when_bdp_client_unavailable(
        self, mock_state, config_with_lineage
    ):
        """Test processor is disabled when BDP client is unavailable."""
        deps_no_client = Mock()
        deps_no_client.bdp_client = None

        processor = WarehouseLineageProcessor(deps_no_client, mock_state)
        processor.config = config_with_lineage

        assert processor.is_enabled() is False

    def test_extract_creates_lineage_for_valid_data_model(self, processor, mock_deps):
        """Test that lineage is created for data model with valid destination and table."""
        # Setup data product and data model
        data_product = DataProduct(
            id="product-123",
            name="Test Product",
            description="Test product",
            eventSpecIds=["spec-1"],
        )

        data_model = DataModel(
            id="model-123",
            organization_id="test-org",
            data_product_id="product-123",
            name="Test Model",
            query_engine="snowflake",
            destination="dest-123",
            table_name="analytics.derived.checkout_events",
        )

        mock_deps.bdp_client.get_data_products.return_value = [data_product]
        mock_deps.bdp_client.get_data_models.return_value = [data_model]

        processor.urn_factory.construct_warehouse_urn.return_value = "urn:li:dataset:(urn:li:dataPlatform:snowflake,analytics.derived.checkout_events,PROD)"

        # Extract lineage
        workunits = list(processor.extract())

        # Verify lineage was created
        assert len(workunits) == 1
        workunit = workunits[0]

        # Verify it's an upstream lineage aspect
        assert isinstance(workunit.metadata.aspect, UpstreamLineageClass)
        lineage = workunit.metadata.aspect
        assert len(lineage.upstreams) == 1
        assert lineage.upstreams[0].dataset == processor.state.warehouse_table_urn

    def test_extract_skips_data_model_without_destination(self, processor, mock_deps):
        """Test that data models without destination are skipped."""
        data_product = DataProduct(
            id="product-123",
            name="Test Product",
            description="Test product",
            eventSpecIds=["spec-1"],
        )

        # Data model missing destination
        data_model = DataModel(
            id="model-123",
            organization_id="test-org",
            data_product_id="product-123",
            name="Test Model",
            query_engine="snowflake",
            destination=None,
            table_name="analytics.derived.checkout_events",
        )

        mock_deps.bdp_client.get_data_products.return_value = [data_product]
        mock_deps.bdp_client.get_data_models.return_value = [data_model]

        # Extract lineage
        workunits = list(processor.extract())

        # No lineage should be created
        assert len(workunits) == 0

    def test_extract_skips_data_model_without_table_name(self, processor, mock_deps):
        """Test that data models without table_name are skipped."""
        data_product = DataProduct(
            id="product-123",
            name="Test Product",
            description="Test product",
            eventSpecIds=["spec-1"],
        )

        # Data model missing table_name
        data_model = DataModel(
            id="model-123",
            organization_id="test-org",
            data_product_id="product-123",
            name="Test Model",
            query_engine="snowflake",
            destination="dest-123",
            table_name=None,
        )

        mock_deps.bdp_client.get_data_products.return_value = [data_product]
        mock_deps.bdp_client.get_data_models.return_value = [data_model]

        # Extract lineage
        workunits = list(processor.extract())

        # No lineage should be created
        assert len(workunits) == 0

    def test_extract_skips_data_model_without_query_engine(self, processor, mock_deps):
        """Test that data models without query_engine are skipped."""
        data_product = DataProduct(
            id="product-123",
            name="Test Product",
            description="Test product",
            eventSpecIds=["spec-1"],
        )

        # Data model missing query_engine
        data_model = DataModel(
            id="model-123",
            organization_id="test-org",
            data_product_id="product-123",
            name="Test Model",
            query_engine=None,
            destination="dest-123",
            table_name="analytics.derived.checkout_events",
        )

        mock_deps.bdp_client.get_data_products.return_value = [data_product]
        mock_deps.bdp_client.get_data_models.return_value = [data_model]

        # Extract lineage
        workunits = list(processor.extract())

        # No lineage should be created
        assert len(workunits) == 0

    def test_extract_handles_data_product_without_data_models(
        self, processor, mock_deps
    ):
        """Test graceful handling when data product has no data models."""
        data_product = DataProduct(
            id="product-123",
            name="Test Product",
            description="Test product",
            eventSpecIds=["spec-1"],
        )

        mock_deps.bdp_client.get_data_products.return_value = [data_product]
        mock_deps.bdp_client.get_data_models.return_value = []

        # Extract lineage
        workunits = list(processor.extract())

        # No lineage should be created
        assert len(workunits) == 0

    def test_extract_handles_no_source_warehouse_urn(self, processor, mock_deps):
        """Test that lineage creation fails gracefully when source warehouse URN not available."""
        processor.state.warehouse_table_urn = None

        data_product = DataProduct(
            id="product-123",
            name="Test Product",
            description="Test product",
            eventSpecIds=["spec-1"],
        )

        data_model = DataModel(
            id="model-123",
            organization_id="test-org",
            data_product_id="product-123",
            name="Test Model",
            query_engine="snowflake",
            destination="dest-123",
            table_name="analytics.derived.checkout_events",
        )

        mock_deps.bdp_client.get_data_products.return_value = [data_product]
        mock_deps.bdp_client.get_data_models.return_value = [data_model]

        processor.urn_factory.construct_warehouse_urn.return_value = "urn:li:dataset:(urn:li:dataPlatform:snowflake,analytics.derived.checkout_events,PROD)"

        # Extract lineage
        workunits = list(processor.extract())

        # No lineage should be created when source URN missing
        assert len(workunits) == 0

    def test_extract_validates_urns_when_enabled(self, processor, mock_deps):
        """Test URN validation when validate_urns is enabled."""
        processor.config.warehouse_lineage.validate_urns = True
        mock_deps.graph.exists.return_value = False

        data_product = DataProduct(
            id="product-123",
            name="Test Product",
            description="Test product",
            eventSpecIds=["spec-1"],
        )

        data_model = DataModel(
            id="model-123",
            organization_id="test-org",
            data_product_id="product-123",
            name="Test Model",
            query_engine="snowflake",
            destination="dest-123",
            table_name="analytics.derived.checkout_events",
        )

        mock_deps.bdp_client.get_data_products.return_value = [data_product]
        mock_deps.bdp_client.get_data_models.return_value = [data_model]

        warehouse_urn = "urn:li:dataset:(urn:li:dataPlatform:snowflake,analytics.derived.checkout_events,PROD)"
        processor.urn_factory.construct_warehouse_urn.return_value = warehouse_urn

        # Extract lineage
        workunits = list(processor.extract())

        # Lineage still created even if validation fails (just warning logged)
        assert len(workunits) == 1
        # Verify validation was attempted
        mock_deps.graph.exists.assert_called_once_with(warehouse_urn)

    def test_extract_handles_api_failure_gracefully(self, processor, mock_deps):
        """Test graceful handling when BDP API fails."""
        mock_deps.bdp_client.get_data_products.side_effect = Exception(
            "API connection failed"
        )

        # Extract should not raise, just log warning
        workunits = list(processor.extract())

        # No workunits but no exception raised
        assert len(workunits) == 0
        # Verify warning was reported
        processor.report.warning.assert_called_once()

    def test_extract_processes_multiple_data_models(self, processor, mock_deps):
        """Test that multiple data models are processed correctly."""
        data_product = DataProduct(
            id="product-123",
            name="Test Product",
            description="Test product",
            eventSpecIds=["spec-1"],
        )

        data_models = [
            DataModel(
                id="model-1",
                organization_id="test-org",
                data_product_id="product-123",
                name="Model 1",
                query_engine="snowflake",
                destination="dest-1",
                table_name="analytics.derived.table1",
            ),
            DataModel(
                id="model-2",
                organization_id="test-org",
                data_product_id="product-123",
                name="Model 2",
                query_engine="bigquery",
                destination="dest-2",
                table_name="analytics.derived.table2",
            ),
        ]

        mock_deps.bdp_client.get_data_products.return_value = [data_product]
        mock_deps.bdp_client.get_data_models.return_value = data_models

        processor.urn_factory.construct_warehouse_urn.side_effect = [
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,analytics.derived.table1,PROD)",
            "urn:li:dataset:(urn:li:dataPlatform:bigquery,analytics.derived.table2,PROD)",
        ]

        # Extract lineage
        workunits = list(processor.extract())

        # Two lineage edges should be created
        assert len(workunits) == 2
