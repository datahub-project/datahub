from unittest.mock import patch

import pytest

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.report import EntityFilterReport
from datahub.ingestion.source.unity.config import UnityCatalogSourceConfig
from datahub.ingestion.source.unity.source import UnityCatalogSource


class TestUnityCatalogSource:
    @pytest.fixture
    def minimal_config(self):
        """Create a minimal config for testing."""
        return UnityCatalogSourceConfig.model_validate(
            {
                "token": "test_token",
                "workspace_url": "https://test.databricks.com",
                "warehouse_id": "test_warehouse",
                "include_hive_metastore": False,
            }
        )

    @pytest.fixture
    def config_with_page_size(self):
        """Create a config with custom page size."""
        return UnityCatalogSourceConfig.model_validate(
            {
                "token": "test_token",
                "workspace_url": "https://test.databricks.com",
                "warehouse_id": "test_warehouse",
                "include_hive_metastore": False,
                "databricks_api_page_size": 75,
            }
        )

    @pytest.fixture
    def config_with_ml_model_settings(self):
        """Create a config with ML model settings."""
        return UnityCatalogSourceConfig.model_validate(
            {
                "token": "test_token",
                "workspace_url": "https://test.databricks.com",
                "warehouse_id": "test_warehouse",
                "include_hive_metastore": False,
                "include_ml_model_aliases": True,
                "ml_model_max_results": 500,
                "databricks_api_page_size": 100,
            }
        )

    @patch("datahub.ingestion.source.unity.source.UnityCatalogApiProxy")
    @patch("datahub.ingestion.source.unity.source.HiveMetastoreProxy")
    def test_source_constructor_passes_default_page_size_to_proxy(
        self, mock_hive_proxy, mock_unity_proxy, minimal_config
    ):
        """Test that UnityCatalogSource passes default databricks_api_page_size to proxy."""
        # Create a mock context
        ctx = PipelineContext(run_id="test_run")
        source = UnityCatalogSource.create(minimal_config, ctx)

        # Verify proxy was created with correct parameters including page size
        mock_unity_proxy.assert_called_once_with(
            minimal_config.workspace_url,
            minimal_config.token,
            minimal_config.warehouse_id,
            report=source.report,
            hive_metastore_proxy=source.hive_metastore_proxy,
            lineage_data_source=minimal_config.lineage_data_source,
            usage_data_source=minimal_config.usage_data_source,
            databricks_api_page_size=0,  # Default value
        )

    @patch("datahub.ingestion.source.unity.source.UnityCatalogApiProxy")
    @patch("datahub.ingestion.source.unity.source.HiveMetastoreProxy")
    def test_source_constructor_passes_custom_page_size_to_proxy(
        self, mock_hive_proxy, mock_unity_proxy, config_with_page_size
    ):
        """Test that UnityCatalogSource passes custom databricks_api_page_size to proxy."""
        ctx = PipelineContext(run_id="test_run")
        source = UnityCatalogSource.create(config_with_page_size, ctx)

        # Verify proxy was created with correct parameters including custom page size
        mock_unity_proxy.assert_called_once_with(
            config_with_page_size.workspace_url,
            config_with_page_size.token,
            config_with_page_size.warehouse_id,
            report=source.report,
            hive_metastore_proxy=source.hive_metastore_proxy,
            lineage_data_source=config_with_page_size.lineage_data_source,
            usage_data_source=config_with_page_size.usage_data_source,
            databricks_api_page_size=75,  # Custom value
        )

    @patch("datahub.ingestion.source.unity.source.UnityCatalogApiProxy")
    @patch("datahub.ingestion.source.unity.source.HiveMetastoreProxy")
    def test_source_config_page_size_available_to_source(
        self, mock_hive_proxy, mock_unity_proxy, config_with_page_size
    ):
        """Test that UnityCatalogSource has access to databricks_api_page_size config."""
        ctx = PipelineContext(run_id="test_run")
        source = UnityCatalogSource.create(config_with_page_size, ctx)

        # Verify the source has access to the configuration value
        assert source.config.databricks_api_page_size == 75

    @patch("datahub.ingestion.source.unity.source.UnityCatalogApiProxy")
    @patch("datahub.ingestion.source.unity.source.HiveMetastoreProxy")
    def test_source_with_hive_metastore_disabled(
        self, mock_hive_proxy, mock_unity_proxy
    ):
        """Test that UnityCatalogSource works with hive metastore disabled."""
        config = UnityCatalogSourceConfig.model_validate(
            {
                "token": "test_token",
                "workspace_url": "https://test.databricks.com",
                "warehouse_id": "test_warehouse",
                "include_hive_metastore": False,
                "databricks_api_page_size": 200,
            }
        )

        ctx = PipelineContext(run_id="test_run")
        source = UnityCatalogSource.create(config, ctx)

        # Verify proxy was created with correct page size even when hive metastore is disabled
        mock_unity_proxy.assert_called_once_with(
            config.workspace_url,
            config.token,
            config.warehouse_id,
            report=source.report,
            hive_metastore_proxy=None,  # Should be None when disabled
            lineage_data_source=config.lineage_data_source,
            usage_data_source=config.usage_data_source,
            databricks_api_page_size=200,
        )

    def test_test_connection_with_page_size_config(self):
        """Test that test_connection properly handles databricks_api_page_size."""
        config_dict = {
            "token": "test_token",
            "workspace_url": "https://test.databricks.com",
            "warehouse_id": "test_warehouse",
            "databricks_api_page_size": 300,
        }

        with patch(
            "datahub.ingestion.source.unity.source.UnityCatalogConnectionTest"
        ) as mock_connection_test:
            mock_connection_test.return_value.get_connection_test.return_value = (
                "test_report"
            )

            result = UnityCatalogSource.test_connection(config_dict)

            # Verify connection test was created with correct config
            assert result == "test_report"
            mock_connection_test.assert_called_once()

            # Get the config that was passed to UnityCatalogConnectionTest
            connection_test_config = mock_connection_test.call_args[0][0]
            assert connection_test_config.databricks_api_page_size == 300

    @patch("datahub.ingestion.source.unity.source.UnityCatalogApiProxy")
    @patch("datahub.ingestion.source.unity.source.HiveMetastoreProxy")
    def test_source_report_includes_ml_model_stats(
        self, mock_hive_proxy, mock_unity_proxy
    ):
        """Test that source report properly tracks ML model statistics."""
        from datahub.ingestion.api.common import PipelineContext

        # Setup mocks
        mock_unity_instance = mock_unity_proxy.return_value
        mock_unity_instance.catalogs.return_value = []
        mock_unity_instance.check_basic_connectivity.return_value = True

        config = UnityCatalogSourceConfig.model_validate(
            {
                "token": "test_token",
                "workspace_url": "https://test.databricks.com",
                "warehouse_id": "test_warehouse",
                "include_hive_metastore": False,
                "databricks_api_page_size": 200,
            }
        )

        ctx = PipelineContext(run_id="test_run")
        source = UnityCatalogSource.create(config, ctx)

        # Verify report has proper ML model tracking attributes
        assert hasattr(source.report, "ml_models")
        assert hasattr(source.report, "ml_model_versions")

        # Verify they are EntityFilterReport objects
        assert isinstance(source.report.ml_models, EntityFilterReport)
        assert isinstance(source.report.ml_model_versions, EntityFilterReport)

    def test_test_connection_with_ml_model_configs(self):
        """Test that test_connection properly handles ML model configs."""
        config_dict = {
            "token": "test_token",
            "workspace_url": "https://test.databricks.com",
            "warehouse_id": "test_warehouse",
            "include_ml_model_aliases": True,
            "ml_model_max_results": 750,
            "databricks_api_page_size": 200,
        }

        with patch(
            "datahub.ingestion.source.unity.source.UnityCatalogConnectionTest"
        ) as mock_connection_test:
            mock_connection_test.return_value.get_connection_test.return_value = (
                "test_report"
            )

            result = UnityCatalogSource.test_connection(config_dict)

            # Verify connection test was created with correct config
            assert result == "test_report"
            mock_connection_test.assert_called_once()

            # Get the config that was passed to UnityCatalogConnectionTest
            connection_test_config = mock_connection_test.call_args[0][0]
            assert connection_test_config.include_ml_model_aliases is True
            assert connection_test_config.ml_model_max_results == 750
            assert connection_test_config.databricks_api_page_size == 200

    @patch("datahub.ingestion.source.unity.source.UnityCatalogApiProxy")
    @patch("datahub.ingestion.source.unity.source.HiveMetastoreProxy")
    def test_process_ml_model_generates_workunits(
        self, mock_hive_proxy, mock_unity_proxy
    ):
        """Test that process_ml_model generates proper workunits."""
        from datetime import datetime

        from datahub.ingestion.api.common import PipelineContext
        from datahub.ingestion.source.unity.proxy_types import (
            Catalog,
            Metastore,
            Model,
            ModelVersion,
            Schema,
        )

        config = UnityCatalogSourceConfig.model_validate(
            {
                "token": "test_token",
                "workspace_url": "https://test.databricks.com",
                "warehouse_id": "test_warehouse",
                "include_hive_metastore": False,
            }
        )

        ctx = PipelineContext(run_id="test_run")
        source = UnityCatalogSource.create(config, ctx)

        # Create test schema
        metastore = Metastore(
            id="metastore",
            name="metastore",
            comment=None,
            global_metastore_id=None,
            metastore_id=None,
            owner=None,
            region=None,
            cloud=None,
        )
        catalog = Catalog(
            id="test_catalog",
            name="test_catalog",
            metastore=metastore,
            comment=None,
            owner=None,
            type=None,
        )
        schema = Schema(
            id="test_catalog.test_schema",
            name="test_schema",
            catalog=catalog,
            comment=None,
            owner=None,
        )

        # Create test model
        test_model = Model(
            id="test_catalog.test_schema.test_model",
            name="test_model",
            description="Test description",
            schema_name="test_schema",
            catalog_name="test_catalog",
            created_at=datetime(2023, 1, 1),
            updated_at=datetime(2023, 1, 2),
        )

        # Create test model version
        test_model_version = ModelVersion(
            id="test_catalog.test_schema.test_model_1",
            name="test_model_1",
            model=test_model,
            version="1",
            aliases=["prod"],
            description="Version 1",
            created_at=datetime(2023, 1, 3),
            updated_at=datetime(2023, 1, 4),
            created_by="test_user",
        )

        # Process the model
        ml_model_workunits = list(source.process_ml_model(test_model, schema))

        # Should generate workunits (MLModelGroup creation and container assignment)
        assert len(ml_model_workunits) > 0

        assert len(source.report.ml_models.processed_entities) == 1
        assert (
            source.report.ml_models.processed_entities[0][1]
            == "test_catalog.test_schema.test_model"
        )

        # Process the model version
        model_urn = source.gen_ml_model_urn(test_model.id)
        ml_model_version_workunits = list(
            source.process_ml_model_version(model_urn, test_model_version, schema)
        )

        # Should generate workunits (MLModel creation and container assignment)
        assert len(ml_model_version_workunits) > 0

        # Verify the report was updated
        assert len(source.report.ml_model_versions.processed_entities) == 1
        assert (
            source.report.ml_model_versions.processed_entities[0][1]
            == "test_catalog.test_schema.test_model_1"
        )
