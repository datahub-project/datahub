from unittest.mock import patch

import pytest

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.unity.config import UnityCatalogSourceConfig
from datahub.ingestion.source.unity.source import UnityCatalogSource


class TestUnityCatalogSource:
    @pytest.fixture
    def minimal_config(self):
        """Create a minimal config for testing."""
        return UnityCatalogSourceConfig.parse_obj(
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
        return UnityCatalogSourceConfig.parse_obj(
            {
                "token": "test_token",
                "workspace_url": "https://test.databricks.com",
                "warehouse_id": "test_warehouse",
                "include_hive_metastore": False,
                "databricks_api_page_size": 75,
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
        config = UnityCatalogSourceConfig.parse_obj(
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
