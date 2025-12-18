"""Unit tests for Hive Metastore Thrift connector components.

These tests verify:
1. Config validation for connection_type="thrift"
2. Thrift client behavior and row format compliance
3. ThriftDataFetcher functionality
4. Connection type routing
"""

from typing import Any, Dict
from unittest.mock import MagicMock, patch

import pytest

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.sql.hive.hive_metastore_source import (
    HiveMetastore,
    HiveMetastoreConnectionType,
    HiveMetastoreSource,
)
from datahub.ingestion.source.sql.hive.hive_thrift_client import (
    HiveMetastoreThriftClient,
    ThriftConnectionConfig,
    ThriftInspectorAdapter,
)
from datahub.ingestion.source.sql.hive.hive_thrift_fetcher import ThriftDataFetcher


class TestThriftInspectorAdapter:
    """Tests for the Inspector adapter."""

    def test_inspector_mimics_sqlalchemy_interface(self):
        """Test that adapter provides engine.url.database for compatibility."""
        adapter = ThriftInspectorAdapter(database="my_database")

        assert adapter.engine.url.database == "my_database"


class TestHiveMetastoreThriftConfig:
    """Tests for source config validation with connection_type="thrift"."""

    def test_config_defaults_and_kerberos_settings(self):
        """Test config defaults and Kerberos configuration."""
        # Default config with thrift connection
        config = HiveMetastore.model_validate(
            {"connection_type": "thrift", "host_port": "hms.company.com:9083"}
        )
        assert config.host_port == "hms.company.com:9083"
        assert config.use_kerberos is False  # Safe default
        assert config.kerberos_service_name == "hive"
        assert config.kerberos_hostname_override is None
        assert config.connection_type == HiveMetastoreConnectionType.thrift

        # Custom Kerberos settings
        config_krb = HiveMetastore.model_validate(
            {
                "connection_type": "thrift",
                "host_port": "hms-lb.company.com:9083",
                "use_kerberos": True,
                "kerberos_service_name": "custom_hive",
                "kerberos_hostname_override": "hms-int.company.com",
            }
        )
        assert config_krb.use_kerberos is True
        assert config_krb.kerberos_service_name == "custom_hive"
        assert config_krb.kerberos_hostname_override == "hms-int.company.com"

    def test_presto_trino_modes_not_supported_with_thrift(self):
        """Test that presto/trino modes raise error with Thrift connection."""
        for mode in ["presto", "presto-on-hive", "trino"]:
            with pytest.raises(ValueError) as exc_info:
                HiveMetastore.model_validate(
                    {
                        "host_port": "hms:9083",
                        "connection_type": "thrift",
                        "mode": mode,
                    }
                )
            assert f"mode: {mode}" in str(exc_info.value)
            assert "not supported" in str(exc_info.value)


class TestHiveMetastoreThriftClient:
    """Tests for the Thrift client."""

    def test_iter_table_rows_format(self):
        """Test that iter_table_rows returns correctly formatted rows."""
        # Create mock Thrift objects
        mock_field = MagicMock()
        mock_field.name = "col1"
        mock_field.type = "string"
        mock_field.comment = "Test column"

        mock_partition_key = MagicMock()
        mock_partition_key.name = "dt"
        mock_partition_key.type = "string"
        mock_partition_key.comment = "Partition column"

        mock_sd = MagicMock()
        mock_sd.location = "s3://bucket/path"

        mock_table = MagicMock()
        mock_table.tableName = "test_table"
        mock_table.tableType = "EXTERNAL_TABLE"
        mock_table.createTime = 1609459200
        mock_table.sd = mock_sd
        mock_table.partitionKeys = [mock_partition_key]
        mock_table.parameters = {"comment": "Test table"}
        mock_table.viewOriginalText = None
        mock_table.viewExpandedText = None

        # Create client with mocked underlying Thrift client
        config = ThriftConnectionConfig(host="localhost", port=9083, use_kerberos=False)
        client = HiveMetastoreThriftClient(config)

        mock_thrift_client = MagicMock()
        mock_thrift_client.get_all_tables.return_value = ["test_table"]
        mock_thrift_client.get_table.return_value = mock_table
        mock_thrift_client.get_fields.return_value = [mock_field]
        client._client = mock_thrift_client

        # Get rows
        rows = list(client.iter_table_rows(["test_db"]))

        # Should have 2 rows: 1 regular column + 1 partition column
        assert len(rows) == 2

        # Check regular column row format
        regular_row = rows[0]
        assert regular_row["schema_name"] == "test_db"
        assert regular_row["table_name"] == "test_table"
        assert regular_row["table_type"] == "EXTERNAL_TABLE"
        assert regular_row["col_name"] == "col1"
        assert regular_row["col_type"] == "string"
        assert regular_row["is_partition_col"] == 0
        assert regular_row["table_location"] == "s3://bucket/path"

        # Check partition column row format
        partition_row = rows[1]
        assert partition_row["col_name"] == "dt"
        assert partition_row["is_partition_col"] == 1


class TestThriftDataFetcher:
    """Tests for the ThriftDataFetcher class."""

    def test_fetcher_creates_thrift_config(self):
        """Test that ThriftDataFetcher creates correct Thrift config."""
        config = HiveMetastore.model_validate(
            {
                "connection_type": "thrift",
                "host_port": "hms.company.com:9083",
                "use_kerberos": True,
                "kerberos_service_name": "hive",
                "timeout_seconds": 120,
            }
        )

        fetcher = ThriftDataFetcher(config)

        assert fetcher._thrift_config.host == "hms.company.com"
        assert fetcher._thrift_config.port == 9083
        assert fetcher._thrift_config.use_kerberos is True
        assert fetcher._thrift_config.kerberos_service_name == "hive"
        assert fetcher._thrift_config.timeout_seconds == 120


class TestHiveMetastoreSourceWithThrift:
    """Tests for HiveMetastoreSource with connection_type="thrift"."""

    @pytest.fixture
    def source_config(self) -> Dict[str, Any]:
        """Create source config dict with thrift connection."""
        return {
            "connection_type": "thrift",
            "host_port": "localhost:9083",
            "use_kerberos": False,
            "database_pattern": {"allow": ["default"]},
        }

    def test_source_creation_with_thrift_connection(
        self, source_config: Dict[str, Any]
    ) -> None:
        """Test source creation with connection_type=thrift."""
        ctx = PipelineContext(run_id="test")
        source = HiveMetastoreSource.create(source_config, ctx)

        assert source.platform == "hive"
        assert source.config.host_port == "localhost:9083"
        assert source.config.connection_type == HiveMetastoreConnectionType.thrift

        # Should use ThriftDataFetcher
        assert isinstance(source._fetcher, ThriftDataFetcher)

    def test_test_connection_with_thrift(self) -> None:
        """Test connection test for Thrift mode."""
        config_dict = {
            "connection_type": "thrift",
            "host_port": "localhost:9083",
            "use_kerberos": False,
        }

        # Success
        with patch(
            "datahub.ingestion.source.sql.hive.hive_thrift_client.HiveMetastoreThriftClient"
        ) as mock_cls:
            mock_cls.return_value.get_all_databases.return_value = ["db1", "db2"]
            report = HiveMetastoreSource.test_connection(config_dict)
            assert report.basic_connectivity is not None
            assert report.basic_connectivity.capable is True

        # Failure
        with patch(
            "datahub.ingestion.source.sql.hive.hive_thrift_client.HiveMetastoreThriftClient"
        ) as mock_cls:
            mock_cls.return_value.connect.side_effect = ConnectionError(
                "Connection refused"
            )
            report = HiveMetastoreSource.test_connection(config_dict)
            assert report.basic_connectivity is not None
            assert report.basic_connectivity.capable is False


class TestConnectionTypeRouting:
    """Test connection_type routing to correct data fetcher."""

    def test_connection_type_thrift_uses_thrift_fetcher(self):
        """Verify that connection_type='thrift' creates ThriftDataFetcher."""
        ctx = PipelineContext(run_id="test")
        config = {
            "connection_type": "thrift",
            "host_port": "localhost:9083",
            "use_kerberos": False,
        }

        source = HiveMetastoreSource.create(config, ctx)

        # Must use ThriftDataFetcher
        assert isinstance(source._fetcher, ThriftDataFetcher)
        assert source.config.connection_type == HiveMetastoreConnectionType.thrift

    @patch("datahub.ingestion.source.sql.hive.hive_sql_fetcher.SQLAlchemyClient")
    def test_connection_type_sql_uses_sql_fetcher(self, mock_client):
        """Verify that connection_type='sql' creates SQLAlchemyDataFetcher."""
        from datahub.ingestion.source.sql.hive.hive_sql_fetcher import (
            SQLAlchemyDataFetcher,
        )

        ctx = PipelineContext(run_id="test")
        config = {
            "connection_type": "sql",
            "host_port": "localhost:3306",
            "username": "user",
            "password": "pass",
        }

        source = HiveMetastoreSource.create(config, ctx)

        # Must use SQLAlchemyDataFetcher
        assert isinstance(source._fetcher, SQLAlchemyDataFetcher)
        assert source.config.connection_type == HiveMetastoreConnectionType.sql

    @patch("datahub.ingestion.source.sql.hive.hive_sql_fetcher.SQLAlchemyClient")
    def test_default_connection_type_is_sql(self, mock_client):
        """Verify that default connection_type is 'sql' (backward compatibility)."""
        from datahub.ingestion.source.sql.hive.hive_sql_fetcher import (
            SQLAlchemyDataFetcher,
        )

        ctx = PipelineContext(run_id="test")
        config = {
            # No connection_type specified
            "host_port": "localhost:3306",
            "username": "user",
            "password": "pass",
        }

        source = HiveMetastoreSource.create(config, ctx)

        # Should default to SQL
        assert isinstance(source._fetcher, SQLAlchemyDataFetcher)
        assert source.config.connection_type == HiveMetastoreConnectionType.sql


class TestHiveMetastoreThriftClientCatalogSupport:
    """Tests for HMS 3.x catalog support in the Thrift client."""

    @pytest.fixture
    def mock_client(self) -> HiveMetastoreThriftClient:
        """Create client with mocked Thrift backend."""
        config = ThriftConnectionConfig(host="localhost", port=9083, use_kerberos=False)
        client = HiveMetastoreThriftClient(config)
        client._client = MagicMock()
        return client

    def test_get_catalogs_success_and_fallback(self, mock_client):
        """Test get_catalogs for HMS 3.x and HMS 2.x."""
        # HMS 3.x: returns catalog list
        mock_response = MagicMock()
        mock_response.names = ["hive", "spark_catalog"]
        mock_client._client.get_catalogs.return_value = mock_response

        assert mock_client.get_catalogs() == ["hive", "spark_catalog"]
        assert mock_client.supports_catalogs() is True

        # HMS 2.x: AttributeError triggers empty list fallback
        mock_client._client.get_catalogs.side_effect = AttributeError("no attribute")
        assert mock_client.get_catalogs() == []
        assert mock_client.supports_catalogs() is False

    def test_get_all_databases_with_and_without_catalog(self, mock_client):
        """Test database listing with optional catalog parameter."""
        mock_client._client.get_all_databases.return_value = ["db1", "db2"]

        assert mock_client.get_all_databases() == ["db1", "db2"]
        mock_client._client.get_all_databases.assert_called()

    def test_get_table_returns_expected_format(self, mock_client):
        """Test get_table returns correctly formatted dict."""
        mock_sd = MagicMock()
        mock_sd.location = "s3://bucket/path"

        mock_table = MagicMock()
        mock_table.tableName = "my_table"
        mock_table.tableType = "EXTERNAL_TABLE"
        mock_table.createTime = 1609459200
        mock_table.sd = mock_sd
        mock_table.partitionKeys = []
        mock_table.parameters = {"key": "value"}
        mock_table.viewOriginalText = None
        mock_table.viewExpandedText = None

        mock_client._client.get_table.return_value = mock_table

        result = mock_client.get_table("test_db", "my_table")

        assert result["table_name"] == "my_table"
        assert result["table_type"] == "EXTERNAL_TABLE"
        assert result["location"] == "s3://bucket/path"
        assert result["parameters"] == {"key": "value"}


class TestThriftClientBehavior:
    """Tests for Thrift client behaviors."""

    def test_partition_columns_ordered_after_regular_columns(self):
        """Partition columns must appear after regular columns."""
        mock_field1 = MagicMock(name="id", type="int", comment="")
        mock_field1.name = "id"
        mock_field1.type = "int"
        mock_field1.comment = ""

        mock_field2 = MagicMock(name="value", type="string", comment="")
        mock_field2.name = "value"
        mock_field2.type = "string"
        mock_field2.comment = ""

        mock_partition = MagicMock(name="dt", type="string", comment="")
        mock_partition.name = "dt"
        mock_partition.type = "string"
        mock_partition.comment = ""

        mock_sd = MagicMock()
        mock_sd.location = "s3://bucket/path"

        mock_table = MagicMock()
        mock_table.tableName = "partitioned_table"
        mock_table.tableType = "EXTERNAL_TABLE"
        mock_table.createTime = 1609459200
        mock_table.sd = mock_sd
        mock_table.partitionKeys = [mock_partition]
        mock_table.parameters = {}
        mock_table.viewOriginalText = None
        mock_table.viewExpandedText = None

        config = ThriftConnectionConfig(host="localhost", port=9083, use_kerberos=False)
        client = HiveMetastoreThriftClient(config)

        mock_thrift = MagicMock()
        mock_thrift.get_all_tables.return_value = ["partitioned_table"]
        mock_thrift.get_table.return_value = mock_table
        mock_thrift.get_fields.return_value = [mock_field1, mock_field2]
        client._client = mock_thrift

        rows = list(client.iter_table_rows(["test_db"]))

        assert len(rows) == 3
        assert rows[0]["col_name"] == "id" and rows[0]["is_partition_col"] == 0
        assert rows[1]["col_name"] == "value" and rows[1]["is_partition_col"] == 0
        assert rows[2]["col_name"] == "dt" and rows[2]["is_partition_col"] == 1

    def test_views_excluded_from_table_rows(self):
        """iter_table_rows must exclude VIRTUAL_VIEWs."""
        mock_sd = MagicMock()
        mock_sd.location = "s3://bucket/path"

        mock_field = MagicMock(name="col1", type="string", comment="")
        mock_field.name = "col1"
        mock_field.type = "string"
        mock_field.comment = ""

        mock_table = MagicMock()
        mock_table.tableName = "regular_table"
        mock_table.tableType = "EXTERNAL_TABLE"
        mock_table.createTime = 1609459200
        mock_table.sd = mock_sd
        mock_table.partitionKeys = []
        mock_table.parameters = {}
        mock_table.viewOriginalText = None
        mock_table.viewExpandedText = None

        mock_view = MagicMock()
        mock_view.tableName = "my_view"
        mock_view.tableType = "VIRTUAL_VIEW"
        mock_view.createTime = 1609459200
        mock_view.sd = mock_sd
        mock_view.partitionKeys = []
        mock_view.parameters = {}
        mock_view.viewOriginalText = "SELECT * FROM t"
        mock_view.viewExpandedText = "SELECT col1 FROM db.t"

        config = ThriftConnectionConfig(host="localhost", port=9083, use_kerberos=False)
        client = HiveMetastoreThriftClient(config)

        mock_thrift = MagicMock()
        mock_thrift.get_all_tables.return_value = ["regular_table", "my_view"]
        mock_thrift.get_table.side_effect = (
            lambda db, name: mock_view if name == "my_view" else mock_table
        )
        mock_thrift.get_fields.return_value = [mock_field]
        client._client = mock_thrift

        # Table rows should only contain the table
        table_rows = list(client.iter_table_rows(["test_db"]))
        table_names = {r["table_name"] for r in table_rows}
        assert "regular_table" in table_names
        assert "my_view" not in table_names

        # View rows should only contain the view
        view_rows = list(client.iter_view_rows(["test_db"]))
        view_names = {r["table_name"] for r in view_rows}
        assert "my_view" in view_names
        assert "regular_table" not in view_names


class TestHMS3CatalogSupport:
    """
    Tests for HMS 3.x catalog support.

    These tests verify the fixes for catalog-aware APIs:
    1. Cache keys include catalog to handle multi-catalog scenarios
    2. Failure reporting includes catalog context
    3. get_all_tables without catalog uses standard API

    Note: Tests that trigger pymetastore imports are skipped if pymetastore is not installed.
    The HMS 3.x catalog APIs are tested in integration tests with actual HMS 3.x.
    """

    @pytest.fixture
    def mock_client(self) -> HiveMetastoreThriftClient:
        """Create client with mocked Thrift backend."""
        config = ThriftConnectionConfig(host="localhost", port=9083, use_kerberos=False)
        client = HiveMetastoreThriftClient(config)
        client._client = MagicMock()
        return client

    def test_get_all_tables_without_catalog_uses_standard_api(self, mock_client):
        """Test that get_all_tables without catalog uses standard HMS 2.x API."""
        mock_client._client.get_all_tables.return_value = ["table1"]

        tables = mock_client.get_all_tables("my_db")

        mock_client._client.get_all_tables.assert_called_with("my_db")
        assert tables == ["table1"]
        # get_tables_ext should NOT be called
        mock_client._client.get_tables_ext.assert_not_called()

    def test_get_fields_without_catalog_uses_standard_api(self, mock_client):
        """Test that get_fields without catalog uses standard get_fields API."""
        mock_field = MagicMock()
        mock_field.name = "col1"
        mock_field.type = "string"
        mock_field.comment = ""
        mock_client._client.get_fields.return_value = [mock_field]

        result = mock_client.get_fields("my_db", "my_table")

        mock_client._client.get_fields.assert_called_with("my_db", "my_table")
        # get_table_req should NOT be called
        mock_client._client.get_table_req.assert_not_called()
        # Verify the result
        assert len(result) == 1
        assert result[0]["col_name"] == "col1"

    def test_cache_key_structure_includes_catalog(self, mock_client):
        """Test that cache keys include catalog to handle multi-catalog scenarios.

        This tests the internal cache structure without triggering HMS API calls.
        """
        from datahub.ingestion.source.sql.hive.hive_thrift_client import TableKey

        # Manually populate the cache with entries from different catalogs
        key1 = TableKey(catalog_name="catalog1", db_name="my_db", table_name="my_table")
        key2 = TableKey(catalog_name="catalog2", db_name="my_db", table_name="my_table")
        mock_client._table_cache[key1] = {
            "table_name": "my_table",
            "table_type": "EXTERNAL_TABLE",
            "location": "s3://bucket1/path",
        }
        mock_client._table_cache[key2] = {
            "table_name": "my_table",
            "table_type": "EXTERNAL_TABLE",
            "location": "s3://bucket2/path",  # Different location!
        }

        # Verify both entries exist and are distinct
        assert len(mock_client._table_cache) == 2
        assert mock_client._table_cache[key1]["location"] == "s3://bucket1/path"
        assert mock_client._table_cache[key2]["location"] == "s3://bucket2/path"

    def test_database_failure_key_structure_includes_catalog(self, mock_client):
        """Test that database failure keys include catalog."""
        from datahub.ingestion.source.sql.hive.hive_thrift_client import DatabaseKey

        # Set failures with different catalogs
        key1 = DatabaseKey(catalog_name="catalog1", db_name="my_db")
        key2 = DatabaseKey(catalog_name="catalog2", db_name="my_db")
        mock_client._database_failures[key1] = "Error 1"
        mock_client._database_failures[key2] = "Error 2"

        # Verify both are tracked separately
        assert len(mock_client._database_failures) == 2
        assert mock_client._database_failures[key1] == "Error 1"
        assert mock_client._database_failures[key2] == "Error 2"

    def test_table_failure_key_structure_includes_catalog(self, mock_client):
        """Test that table failure keys include catalog."""
        from datahub.ingestion.source.sql.hive.hive_thrift_client import TableKey

        # Set failures with different catalogs
        key1 = TableKey(catalog_name="catalog1", db_name="my_db", table_name="my_table")
        key2 = TableKey(catalog_name="catalog2", db_name="my_db", table_name="my_table")
        mock_client._table_failures[key1] = "Error 1"
        mock_client._table_failures[key2] = "Error 2"

        # Verify both are tracked separately
        assert len(mock_client._table_failures) == 2

    def test_database_failures_include_catalog_in_display(self, mock_client):
        """Test that failure reporting includes catalog context."""
        from datahub.ingestion.source.sql.hive.hive_thrift_client import DatabaseKey

        # Manually set a database failure with catalog
        key = DatabaseKey(catalog_name="spark_catalog", db_name="my_db")
        mock_client._database_failures[key] = "Connection error"

        failures = mock_client.get_database_failures()

        assert len(failures) == 1
        # Display name should include catalog
        assert failures[0][0] == "spark_catalog.my_db"
        assert failures[0][1] == "Connection error"

    def test_table_failures_include_catalog_in_display(self, mock_client):
        """Test that table failure reporting includes catalog context."""
        from datahub.ingestion.source.sql.hive.hive_thrift_client import TableKey

        # Manually set a table failure with catalog
        key = TableKey(
            catalog_name="spark_catalog", db_name="my_db", table_name="my_table"
        )
        mock_client._table_failures[key] = "Table not found"

        failures = mock_client.get_table_failures()

        assert len(failures) == 1
        # Display name should include catalog
        assert failures[0][0] == "spark_catalog.my_db"
        assert failures[0][1] == "my_table"
        assert failures[0][2] == "Table not found"

    def test_failures_without_catalog_display_correctly(self, mock_client):
        """Test that failures without catalog display without prefix."""
        from datahub.ingestion.source.sql.hive.hive_thrift_client import (
            DatabaseKey,
            TableKey,
        )

        # Set failures without catalog (None)
        db_key = DatabaseKey(catalog_name=None, db_name="my_db")
        table_key = TableKey(catalog_name=None, db_name="my_db", table_name="my_table")
        mock_client._database_failures[db_key] = "Error"
        mock_client._table_failures[table_key] = "Error"

        db_failures = mock_client.get_database_failures()
        table_failures = mock_client.get_table_failures()

        # Should NOT have catalog prefix
        assert db_failures[0][0] == "my_db"
        assert table_failures[0][0] == "my_db"

    def test_clear_failures_clears_catalog_keyed_data(self, mock_client):
        """Test that clear_failures clears catalog-keyed failure data."""
        from datahub.ingestion.source.sql.hive.hive_thrift_client import (
            DatabaseKey,
            TableKey,
        )

        # Populate some data
        db_key = DatabaseKey(catalog_name="catalog1", db_name="db1")
        table_key = TableKey(
            catalog_name="catalog1", db_name="db1", table_name="table1"
        )
        mock_client._database_failures[db_key] = "Error"
        mock_client._table_failures[table_key] = "Error"
        mock_client._table_cache[table_key] = {"data": "value"}

        # Clear
        mock_client.clear_failures()

        # Verify all cleared
        assert len(mock_client._database_failures) == 0
        assert len(mock_client._table_failures) == 0
        assert len(mock_client._table_cache) == 0


class TestCatalogConfigIntegration:
    """Test catalog configuration integration."""

    def test_catalog_name_in_config(self):
        """Test that catalog_name is properly set in config."""
        ctx = PipelineContext(run_id="test")
        config_dict = {
            "connection_type": "thrift",
            "host_port": "localhost:9083",
            "use_kerberos": False,
            "catalog_name": "spark_catalog",
        }

        source = HiveMetastoreSource.create(config_dict, ctx)

        assert source.config.catalog_name == "spark_catalog"

    def test_include_catalog_name_in_ids(self):
        """Test include_catalog_name_in_ids configuration."""
        ctx = PipelineContext(run_id="test")
        config_dict = {
            "connection_type": "thrift",
            "host_port": "localhost:9083",
            "use_kerberos": False,
            "catalog_name": "spark_catalog",
            "include_catalog_name_in_ids": True,
        }

        source = HiveMetastoreSource.create(config_dict, ctx)

        assert source.config.include_catalog_name_in_ids is True
        assert source.config.catalog_name == "spark_catalog"


# =============================================================================
# Error Handling and Wrapping Tests
# =============================================================================


class TestErrorWrapping:
    """Tests for error wrapping logic that provides helpful messages."""

    def test_broken_pipe_without_kerberos_suggests_enabling_kerberos(self):
        """BrokenPipeError without Kerberos should suggest enabling it."""
        from datahub.ingestion.source.sql.hive.hive_thrift_client import (
            HMSConnectionError,
            _wrap_thrift_error,
        )

        original_error = BrokenPipeError("Connection closed")
        wrapped = _wrap_thrift_error(original_error, use_kerberos=False)

        assert isinstance(wrapped, HMSConnectionError)
        assert "use_kerberos" in str(wrapped).lower()
        assert "sasl" in str(wrapped).lower() or "kerberos" in str(wrapped).lower()

    def test_broken_pipe_with_kerberos_suggests_checking_ticket(self):
        """BrokenPipeError with Kerberos should suggest checking ticket."""
        from datahub.ingestion.source.sql.hive.hive_thrift_client import (
            HMSConnectionError,
            _wrap_thrift_error,
        )

        original_error = BrokenPipeError("Connection closed")
        wrapped = _wrap_thrift_error(original_error, use_kerberos=True)

        assert isinstance(wrapped, HMSConnectionError)
        assert "klist" in str(wrapped) or "ticket" in str(wrapped).lower()

    def test_read_zero_bytes_detected_as_auth_error(self):
        """'read 0 bytes' error should be detected as auth-related."""
        from datahub.ingestion.source.sql.hive.hive_thrift_client import (
            HMSConnectionError,
            _wrap_thrift_error,
        )

        original_error = Exception("TTransportException: read 0 bytes")
        wrapped = _wrap_thrift_error(original_error, use_kerberos=False)

        assert isinstance(wrapped, HMSConnectionError)

    def test_connection_reset_detected_as_auth_error(self):
        """'connection reset' error should be detected as auth-related."""
        from datahub.ingestion.source.sql.hive.hive_thrift_client import (
            HMSConnectionError,
            _wrap_thrift_error,
        )

        original_error = Exception("Connection reset by peer")
        wrapped = _wrap_thrift_error(original_error, use_kerberos=False)

        assert isinstance(wrapped, HMSConnectionError)

    def test_non_auth_error_passes_through(self):
        """Non-auth errors should pass through unchanged."""
        from datahub.ingestion.source.sql.hive.hive_thrift_client import (
            _wrap_thrift_error,
        )

        original_error = ValueError("Some other error")
        wrapped = _wrap_thrift_error(original_error, use_kerberos=False)

        # Should return the original error, not wrapped
        assert wrapped is original_error


class TestRetryLogic:
    """Tests for retry decision logic."""

    def test_retry_decorator_is_callable(self):
        """Retry decorator should be created successfully."""
        from datahub.ingestion.source.sql.hive.hive_thrift_client import (
            HiveMetastoreThriftClient,
            ThriftConnectionConfig,
        )

        config = ThriftConnectionConfig(host="localhost", port=9083)
        client = HiveMetastoreThriftClient(config)

        # Get the retry decorator - verify it can be created
        decorator = client._get_retry_decorator()
        assert callable(decorator)

    def test_transport_exception_type(self):
        """Verify TTransportException can be instantiated for retry testing."""
        from thrift.transport import TTransport

        # TTransportException should be a proper exception class
        error = TTransport.TTransportException(message="Connection failed")
        assert isinstance(error, TTransport.TTransportException)
        assert isinstance(error, Exception)


class TestDateFormatting:
    """Tests for date formatting utility."""

    def test_format_create_date_with_valid_timestamp(self):
        """Valid timestamp should be formatted as YYYY-MM-DD."""
        from datahub.ingestion.source.sql.hive.hive_thrift_client import (
            HiveMetastoreThriftClient,
        )

        # Unix timestamp for 2024-01-15
        timestamp = 1705276800
        result = HiveMetastoreThriftClient._format_create_date(timestamp)

        assert result == "2024-01-15"

    def test_format_create_date_with_none(self):
        """None should return empty string."""
        from datahub.ingestion.source.sql.hive.hive_thrift_client import (
            HiveMetastoreThriftClient,
        )

        result = HiveMetastoreThriftClient._format_create_date(None)
        assert result == ""

    def test_format_create_date_with_zero(self):
        """Zero should return empty string (falsy)."""
        from datahub.ingestion.source.sql.hive.hive_thrift_client import (
            HiveMetastoreThriftClient,
        )

        result = HiveMetastoreThriftClient._format_create_date(0)
        assert result == ""


class TestTestConnectionErrorHandling:
    """Tests for test_connection error detection and mitigation messages."""

    def test_kerberos_error_provides_mitigation(self):
        """Kerberos errors should provide helpful mitigation message."""
        from datahub.ingestion.source.sql.hive.hive_metastore_source import (
            HiveMetastoreSource,
        )

        config_dict = {
            "connection_type": "thrift",
            "host_port": "localhost:9083",
            "use_kerberos": True,
        }

        # Mock the client to raise a Kerberos error
        with patch(
            "datahub.ingestion.source.sql.hive.hive_thrift_client.HiveMetastoreThriftClient.connect"
        ) as mock_connect:
            mock_connect.side_effect = Exception("GSSAPI Error: No credentials")

            report = HiveMetastoreSource.test_connection(config_dict)

            assert report.basic_connectivity is not None
            assert report.basic_connectivity.capable is False
            assert report.basic_connectivity.mitigation_message is not None
            assert "kinit" in report.basic_connectivity.mitigation_message

    def test_connection_refused_provides_mitigation(self):
        """Connection refused errors should provide helpful mitigation."""
        from datahub.ingestion.source.sql.hive.hive_metastore_source import (
            HiveMetastoreSource,
        )

        config_dict = {
            "connection_type": "thrift",
            "host_port": "localhost:9083",
        }

        with patch(
            "datahub.ingestion.source.sql.hive.hive_thrift_client.HiveMetastoreThriftClient.connect"
        ) as mock_connect:
            mock_connect.side_effect = Exception("Connection refused")

            report = HiveMetastoreSource.test_connection(config_dict)

            assert report.basic_connectivity is not None
            assert report.basic_connectivity.capable is False
            assert report.basic_connectivity.mitigation_message is not None
            assert "host_port" in report.basic_connectivity.mitigation_message

    def test_timeout_error_provides_mitigation(self):
        """Timeout errors should provide helpful mitigation."""
        from datahub.ingestion.source.sql.hive.hive_metastore_source import (
            HiveMetastoreSource,
        )

        config_dict = {
            "connection_type": "thrift",
            "host_port": "localhost:9083",
        }

        with patch(
            "datahub.ingestion.source.sql.hive.hive_thrift_client.HiveMetastoreThriftClient.connect"
        ) as mock_connect:
            mock_connect.side_effect = Exception("Connection timed out")

            report = HiveMetastoreSource.test_connection(config_dict)

            assert report.basic_connectivity is not None
            assert report.basic_connectivity.capable is False
            assert report.basic_connectivity.mitigation_message is not None
            assert "firewall" in report.basic_connectivity.mitigation_message.lower()


class TestSourceCloseFailureReporting:
    """Tests for failure reporting during source close()."""

    def test_close_reports_database_failures(self):
        """Database failures from ThriftDataFetcher should be reported."""
        from datahub.ingestion.source.sql.hive.hive_thrift_client import DatabaseKey

        ctx = PipelineContext(run_id="test")
        config_dict = {
            "connection_type": "thrift",
            "host_port": "localhost:9083",
        }

        source = HiveMetastoreSource.create(config_dict, ctx)

        # Manually inject a failure into the client
        mock_client = MagicMock()
        db_key = DatabaseKey(catalog_name=None, db_name="failed_db")
        mock_client._database_failures = {db_key: "Connection error"}
        mock_client._table_failures = {}
        mock_client.get_database_failures.return_value = [
            ("failed_db", "Connection error")
        ]
        mock_client.get_table_failures.return_value = []
        mock_client.close = MagicMock()

        source._fetcher._client = mock_client  # type: ignore[attr-defined]

        source.close()

        # Check that warning was reported
        assert len(source.report.warnings) > 0

    def test_close_reports_table_failures(self):
        """Table failures from ThriftDataFetcher should be reported."""
        from datahub.ingestion.source.sql.hive.hive_thrift_client import TableKey

        ctx = PipelineContext(run_id="test")
        config_dict = {
            "connection_type": "thrift",
            "host_port": "localhost:9083",
        }

        source = HiveMetastoreSource.create(config_dict, ctx)

        # Manually inject a failure into the client
        mock_client = MagicMock()
        table_key = TableKey(
            catalog_name=None, db_name="my_db", table_name="failed_table"
        )
        mock_client._database_failures = {}
        mock_client._table_failures = {table_key: "Table not found"}
        mock_client.get_database_failures.return_value = []
        mock_client.get_table_failures.return_value = [
            ("my_db", "failed_table", "Table not found")
        ]
        mock_client.close = MagicMock()

        source._fetcher._client = mock_client  # type: ignore[attr-defined]

        source.close()

        # Check that warning was reported
        assert len(source.report.warnings) > 0


class TestThriftDataFetcherMethods:
    """Tests for ThriftDataFetcher fetch methods."""

    def test_get_database_failures_returns_empty_when_not_connected(self):
        """get_database_failures should return empty list when client is None."""
        from datahub.ingestion.source.sql.hive.hive_thrift_fetcher import (
            ThriftDataFetcher,
        )

        mock_config = MagicMock()
        mock_config.host_port = "localhost:9083"
        mock_config.use_kerberos = False
        mock_config.kerberos_service_name = "hive"
        mock_config.kerberos_hostname_override = None
        mock_config.timeout_seconds = 60

        fetcher = ThriftDataFetcher(mock_config)
        # Client is None by default

        failures = fetcher.get_database_failures()
        assert failures == []

    def test_get_table_failures_returns_empty_when_not_connected(self):
        """get_table_failures should return empty list when client is None."""
        from datahub.ingestion.source.sql.hive.hive_thrift_fetcher import (
            ThriftDataFetcher,
        )

        mock_config = MagicMock()
        mock_config.host_port = "localhost:9083"
        mock_config.use_kerberos = False
        mock_config.kerberos_service_name = "hive"
        mock_config.kerberos_hostname_override = None
        mock_config.timeout_seconds = 60

        fetcher = ThriftDataFetcher(mock_config)

        failures = fetcher.get_table_failures()
        assert failures == []

    def test_close_when_not_connected(self):
        """close() should handle case when client is None."""
        from datahub.ingestion.source.sql.hive.hive_thrift_fetcher import (
            ThriftDataFetcher,
        )

        mock_config = MagicMock()
        mock_config.host_port = "localhost:9083"
        mock_config.use_kerberos = False
        mock_config.kerberos_service_name = "hive"
        mock_config.kerberos_hostname_override = None
        mock_config.timeout_seconds = 60

        fetcher = ThriftDataFetcher(mock_config)

        # Should not raise
        fetcher.close()
        assert fetcher._client is None

    def test_get_catalog_name_returns_config_value(self):
        """_get_catalog_name should return catalog from config."""
        from datahub.ingestion.source.sql.hive.hive_thrift_fetcher import (
            ThriftDataFetcher,
        )

        mock_config = MagicMock()
        mock_config.host_port = "localhost:9083"
        mock_config.use_kerberos = False
        mock_config.kerberos_service_name = "hive"
        mock_config.kerberos_hostname_override = None
        mock_config.timeout_seconds = 60
        mock_config.catalog_name = "spark_catalog"

        fetcher = ThriftDataFetcher(mock_config)

        assert fetcher._get_catalog_name() == "spark_catalog"

    def test_get_catalog_name_returns_none_when_not_set(self):
        """_get_catalog_name should return None when catalog not configured."""
        from datahub.ingestion.source.sql.hive.hive_thrift_fetcher import (
            ThriftDataFetcher,
        )

        mock_config = MagicMock()
        mock_config.host_port = "localhost:9083"
        mock_config.use_kerberos = False
        mock_config.kerberos_service_name = "hive"
        mock_config.kerberos_hostname_override = None
        mock_config.timeout_seconds = 60
        mock_config.catalog_name = None

        fetcher = ThriftDataFetcher(mock_config)

        assert fetcher._get_catalog_name() is None
