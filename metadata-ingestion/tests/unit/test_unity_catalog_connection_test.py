from unittest.mock import patch

import pytest

from datahub.ingestion.api.source import SourceCapability
from datahub.ingestion.source.unity.config import UnityCatalogSourceConfig
from datahub.ingestion.source.unity.connection_test import UnityCatalogConnectionTest


class TestUnityCatalogConnectionTest:
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
                "databricks_api_page_size": 50,
            }
        )

    @patch("datahub.ingestion.source.unity.connection_test.UnityCatalogApiProxy")
    def test_constructor_passes_page_size_to_proxy(
        self, mock_proxy_class, minimal_config
    ):
        """Test that UnityCatalogConnectionTest passes databricks_api_page_size to proxy."""
        connection_test = UnityCatalogConnectionTest(minimal_config)

        mock_proxy_class.assert_called_once_with(
            minimal_config.workspace_url,
            minimal_config.token,
            minimal_config.profiling.warehouse_id,
            report=connection_test.report,
            databricks_api_page_size=minimal_config.databricks_api_page_size,
        )
        assert minimal_config.databricks_api_page_size == 0

    @patch("datahub.ingestion.source.unity.connection_test.UnityCatalogApiProxy")
    def test_constructor_with_custom_page_size(
        self, mock_proxy_class, config_with_page_size
    ):
        """Test that UnityCatalogConnectionTest passes custom databricks_api_page_size to proxy."""
        connection_test = UnityCatalogConnectionTest(config_with_page_size)

        mock_proxy_class.assert_called_once_with(
            config_with_page_size.workspace_url,
            config_with_page_size.token,
            config_with_page_size.profiling.warehouse_id,
            report=connection_test.report,
            databricks_api_page_size=50,
        )
        assert config_with_page_size.databricks_api_page_size == 50

    @patch("datahub.ingestion.source.unity.connection_test.UnityCatalogApiProxy")
    def test_basic_connectivity_successful(self, mock_proxy_class, minimal_config):
        """Test basic connectivity when proxy returns True."""
        mock_proxy = mock_proxy_class.return_value
        mock_proxy.check_basic_connectivity.return_value = True

        connection_test = UnityCatalogConnectionTest(minimal_config)
        capability_report = connection_test.basic_connectivity()

        assert capability_report.capable is True
        assert capability_report.failure_reason is None
        mock_proxy.check_basic_connectivity.assert_called_once()

    @patch("datahub.ingestion.source.unity.connection_test.UnityCatalogApiProxy")
    def test_basic_connectivity_failed(self, mock_proxy_class, minimal_config):
        """Test basic connectivity when proxy returns False."""
        mock_proxy = mock_proxy_class.return_value
        mock_proxy.check_basic_connectivity.return_value = False

        connection_test = UnityCatalogConnectionTest(minimal_config)
        capability_report = connection_test.basic_connectivity()

        assert capability_report.capable is False
        assert capability_report.failure_reason is None
        mock_proxy.check_basic_connectivity.assert_called_once()

    @patch("datahub.ingestion.source.unity.connection_test.UnityCatalogApiProxy")
    def test_basic_connectivity_exception(self, mock_proxy_class, minimal_config):
        """Test basic connectivity when proxy raises an exception."""
        mock_proxy = mock_proxy_class.return_value
        mock_proxy.check_basic_connectivity.side_effect = Exception("Connection failed")

        connection_test = UnityCatalogConnectionTest(minimal_config)
        capability_report = connection_test.basic_connectivity()

        assert capability_report.capable is False
        assert capability_report.failure_reason == "Connection failed"
        mock_proxy.check_basic_connectivity.assert_called_once()

    @patch("datahub.ingestion.source.unity.connection_test.UnityCatalogApiProxy")
    def test_usage_connectivity_disabled(self, mock_proxy_class):
        """Test usage connectivity when usage statistics are disabled."""
        config = UnityCatalogSourceConfig.parse_obj(
            {
                "token": "test_token",
                "workspace_url": "https://test.databricks.com",
                "warehouse_id": "test_warehouse",
                "include_usage_statistics": False,
            }
        )

        connection_test = UnityCatalogConnectionTest(config)
        capability_report = connection_test.usage_connectivity()

        assert capability_report is None

    @patch("datahub.ingestion.source.unity.connection_test.UnityCatalogApiProxy")
    def test_profiling_connectivity_disabled(self, mock_proxy_class):
        """Test profiling connectivity when profiling is disabled."""
        config = UnityCatalogSourceConfig.parse_obj(
            {
                "token": "test_token",
                "workspace_url": "https://test.databricks.com",
                "warehouse_id": "test_warehouse",
                "profiling": {"enabled": False},
            }
        )

        connection_test = UnityCatalogConnectionTest(config)
        capability_report = connection_test.profiling_connectivity()

        assert capability_report is None

    @patch("datahub.ingestion.source.unity.connection_test.UnityCatalogApiProxy")
    def test_profiling_connectivity_enabled(self, mock_proxy_class):
        """Test profiling connectivity when profiling is enabled."""
        config = UnityCatalogSourceConfig.parse_obj(
            {
                "token": "test_token",
                "workspace_url": "https://test.databricks.com",
                "warehouse_id": "test_warehouse",
                "profiling": {
                    "enabled": True,
                    "method": "ge",
                    "warehouse_id": "test_warehouse",
                },
            }
        )

        mock_proxy = mock_proxy_class.return_value
        mock_proxy.check_profiling_connectivity.return_value = True

        connection_test = UnityCatalogConnectionTest(config)
        capability_report = connection_test.profiling_connectivity()

        assert capability_report is not None
        assert capability_report.capable is True
        mock_proxy.check_profiling_connectivity.assert_called_once()

    @patch("datahub.ingestion.source.unity.connection_test.UnityCatalogApiProxy")
    def test_get_connection_test_full(self, mock_proxy_class):
        """Test get_connection_test returns complete TestConnectionReport."""
        config = UnityCatalogSourceConfig.parse_obj(
            {
                "token": "test_token",
                "workspace_url": "https://test.databricks.com",
                "warehouse_id": "test_warehouse",
                "include_usage_statistics": True,
                "profiling": {
                    "enabled": True,
                    "method": "ge",
                    "warehouse_id": "test_warehouse",
                },
                "databricks_api_page_size": 100,
            }
        )

        mock_proxy = mock_proxy_class.return_value
        mock_proxy.check_basic_connectivity.return_value = True
        mock_proxy.check_profiling_connectivity.return_value = True
        mock_proxy.query_history.return_value = iter(["dummy_query"])

        connection_test = UnityCatalogConnectionTest(config)
        test_report = connection_test.get_connection_test()

        assert test_report.basic_connectivity is not None
        assert test_report.basic_connectivity.capable is True
        assert test_report.capability_report is not None
        assert len(test_report.capability_report) == 2
        assert (
            test_report.capability_report[SourceCapability.USAGE_STATS].capable is True
        )
        assert (
            test_report.capability_report[SourceCapability.DATA_PROFILING].capable
            is True
        )

        # Verify proxy was created with correct page size
        mock_proxy_class.assert_called_once_with(
            config.workspace_url,
            config.token,
            config.profiling.warehouse_id,
            report=connection_test.report,
            databricks_api_page_size=100,
        )
