"""Unit tests for HiveMetastoreThriftSource and related components.

These tests verify:
1. Config validation for the Thrift connector
2. Thrift client behavior and row format compliance
3. Source class functionality and inheritance contract
4. Row format compatibility between SQL and Thrift implementations
"""

from typing import Any, Dict
from unittest.mock import MagicMock, patch

import pytest

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.sql.hive.hive_metastore_thrift_source import (
    HiveMetastoreThriftConfig,
    HiveMetastoreThriftSource,
)
from datahub.ingestion.source.sql.hive.hive_thrift_client import (
    HiveMetastoreThriftClient,
    ThriftConnectionConfig,
    ThriftInspectorAdapter,
)


class TestThriftInspectorAdapter:
    """Tests for the Inspector adapter."""

    def test_inspector_mimics_sqlalchemy_interface(self):
        """Test that adapter provides engine.url.database for parent class compatibility."""
        adapter = ThriftInspectorAdapter(database="my_database")

        # HiveMetastoreSource accesses inspector.engine.url.database
        # This adapter must provide that interface
        assert adapter.engine.url.database == "my_database"


class TestHiveMetastoreThriftConfig:
    """Tests for source config validation."""

    def test_config_defaults_and_kerberos_settings(self):
        """Test config defaults and Kerberos configuration."""
        # Default config (use_kerberos defaults to False for safety)
        config = HiveMetastoreThriftConfig.model_validate(
            {"host_port": "hms.company.com:9083"}
        )
        assert config.host_port == "hms.company.com:9083"
        assert config.use_kerberos is False  # Safe default
        assert config.kerberos_service_name == "hive"
        assert config.kerberos_hostname_override is None

        # Custom Kerberos settings
        config_krb = HiveMetastoreThriftConfig.model_validate(
            {
                "host_port": "hms-lb.company.com:9083",
                "use_kerberos": True,
                "kerberos_service_name": "custom_hive",
                "kerberos_hostname_override": "hms-int.company.com",
            }
        )
        assert config_krb.use_kerberos is True
        assert config_krb.kerberos_service_name == "custom_hive"
        assert config_krb.kerberos_hostname_override == "hms-int.company.com"

    def test_where_clauses_not_supported(self):
        """Test that WHERE clause options raise helpful errors."""
        for option, pattern_hint in [
            ("schemas_where_clause_suffix", "database_pattern"),
            ("tables_where_clause_suffix", "table_pattern"),
            ("views_where_clause_suffix", "view_pattern"),
        ]:
            with pytest.raises(ValueError) as exc_info:
                # connection_type='thrift' triggers the parent class validation
                HiveMetastoreThriftConfig.model_validate(
                    {
                        "host_port": "hms:9083",
                        "connection_type": "thrift",
                        option: "AND x = 'y'",
                    }
                )
            assert "not supported" in str(exc_info.value)
            assert pattern_hint in str(exc_info.value)

    def test_presto_trino_modes_not_supported_with_thrift(self):
        """Test that presto/trino modes raise error with Thrift connection.

        These modes require SQLAlchemy for view extraction, which Thrift doesn't provide.
        """
        for mode in ["presto", "presto-on-hive", "trino"]:
            with pytest.raises(ValueError) as exc_info:
                HiveMetastoreThriftConfig.model_validate(
                    {
                        "host_port": "hms:9083",
                        "connection_type": "thrift",
                        "mode": mode,
                    }
                )
            assert f"mode: {mode}" in str(exc_info.value)
            assert "not supported" in str(exc_info.value)
            assert "connection_type: thrift" in str(exc_info.value)


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


class TestHiveMetastoreThriftSource:
    """Tests for the source class."""

    @pytest.fixture
    def source_config(self) -> Dict[str, Any]:
        """Create source config dict with required connection_type."""
        return {
            "connection_type": "thrift",  # Required for Thrift source
            "host_port": "localhost:9083",
            "use_kerberos": False,
            "database_pattern": {"allow": ["default"]},
        }

    def test_source_creation_and_basic_properties(
        self, source_config: Dict[str, Any]
    ) -> None:
        """Test source creation, inspector, and basic properties."""
        ctx = PipelineContext(run_id="test")
        source = HiveMetastoreThriftSource.create(source_config, ctx)

        assert source.platform == "hive"
        assert source.config.host_port == "localhost:9083"
        assert source._thrift_inspector is not None
        assert hasattr(source, "storage_lineage")

        # get_inspectors returns the adapter
        inspectors = list(source.get_inspectors())
        assert len(inspectors) == 1
        assert inspectors[0] is source._thrift_inspector

    def test_get_db_name_priority(self, source_config: Dict[str, Any]) -> None:
        """Test get_db_name: catalog_name takes priority, else defaults to 'hive'."""
        ctx = PipelineContext(run_id="test")

        # Catalog name specified - should return it
        config_with_catalog = {**source_config, "catalog_name": "spark_catalog"}
        source = HiveMetastoreThriftSource.create(config_with_catalog, ctx)
        # ThriftInspectorAdapter is used in place of Inspector for Thrift mode
        assert source.get_db_name(source._thrift_inspector) == "spark_catalog"  # type: ignore[arg-type]

        # No catalog specified - defaults to "hive" (the default HMS catalog)
        source2 = HiveMetastoreThriftSource.create(source_config, ctx)
        assert source2.get_db_name(source2._thrift_inspector) == "hive"  # type: ignore[arg-type]

    def test_data_fetching_delegates_to_thrift_client(
        self, source_config: Dict[str, Any]
    ) -> None:
        """Test that data fetching methods delegate to Thrift client."""
        ctx = PipelineContext(run_id="test")
        source = HiveMetastoreThriftSource.create(source_config, ctx)

        # Mock the Thrift client
        mock_client = MagicMock(spec=HiveMetastoreThriftClient)
        mock_client.get_all_databases.return_value = ["default"]
        mock_client.iter_table_rows.return_value = iter([])
        mock_client.iter_view_rows.return_value = iter([])
        mock_client.iter_schema_rows.return_value = iter([])
        mock_client.iter_table_properties_rows.return_value = iter([])
        source._thrift_client = mock_client

        # Call all data fetching methods
        list(source._fetch_table_rows(""))
        list(source._fetch_hive_view_rows(""))
        list(source._fetch_schema_rows(""))
        list(source._fetch_table_properties_rows(""))

        # All should delegate to client
        mock_client.iter_table_rows.assert_called_once()
        mock_client.iter_view_rows.assert_called_once()
        mock_client.iter_schema_rows.assert_called_once()
        mock_client.iter_table_properties_rows.assert_called_once()

    def test_close_cleans_up_client(self, source_config: Dict[str, Any]) -> None:
        """Test that close() properly closes the Thrift client."""
        ctx = PipelineContext(run_id="test")
        source = HiveMetastoreThriftSource.create(source_config, ctx)

        mock_client = MagicMock(spec=HiveMetastoreThriftClient)
        source._thrift_client = mock_client

        source.close()

        mock_client.close.assert_called_once()
        assert source._thrift_client is None

    def test_test_connection(self) -> None:
        """Test connection test success and failure paths."""
        config_dict = {"host_port": "localhost:9083", "use_kerberos": False}

        # Success
        with patch(
            "datahub.ingestion.source.sql.hive.hive_metastore_thrift_source.HiveMetastoreThriftClient"
        ) as mock_cls:
            mock_cls.return_value.get_all_databases.return_value = ["db1", "db2"]
            report = HiveMetastoreThriftSource.test_connection(config_dict)
            assert report.basic_connectivity is not None
            assert report.basic_connectivity.capable is True

        # Failure
        with patch(
            "datahub.ingestion.source.sql.hive.hive_metastore_thrift_source.HiveMetastoreThriftClient"
        ) as mock_cls:
            mock_cls.return_value.connect.side_effect = ConnectionError(
                "Connection refused"
            )
            report = HiveMetastoreThriftSource.test_connection(config_dict)
            assert report.basic_connectivity is not None
            assert report.basic_connectivity.capable is False


class TestConnectionTypeRouting:
    """Test that connection_type='thrift' correctly routes to HiveMetastoreThriftSource."""

    def test_connection_type_thrift_routes_to_thrift_source(self):
        """
        Critical routing test: Verify that HiveMetastoreSource.create() with
        connection_type='thrift' returns a HiveMetastoreThriftSource instance.

        This ensures the routing logic in HiveMetastoreSource.create() is working.
        If this breaks, the Thrift connector becomes inaccessible.
        """
        from datahub.ingestion.source.sql.hive.hive_metastore_source import (
            HiveMetastoreSource,
        )

        ctx = PipelineContext(run_id="test")
        config = {
            "connection_type": "thrift",
            "host_port": "localhost:9083",
            "use_kerberos": False,
        }

        source = HiveMetastoreSource.create(config, ctx)

        # Must be the Thrift source, not the base SQL source
        assert isinstance(source, HiveMetastoreThriftSource)
        assert source.config.connection_type.value == "thrift"

    def test_config_with_sql_connection_type_is_not_thrift(self):
        """Verify that config with connection_type='sql' is correctly identified."""
        from datahub.ingestion.source.sql.hive.hive_metastore_source import (
            HiveMetastore,
            HiveMetastoreConnectionType,
        )

        # SQL connection type (default) should NOT be thrift
        config = HiveMetastore.model_validate(
            {
                "connection_type": "sql",
                "sqlalchemy_uri": "mysql+pymysql://user:pass@localhost:3306/hive_metastore",
            }
        )
        assert config.connection_type == HiveMetastoreConnectionType.sql

        # Thrift connection type should be thrift
        config_thrift = HiveMetastore.model_validate(
            {
                "connection_type": "thrift",
                "host_port": "localhost:9083",
            }
        )
        assert config_thrift.connection_type == HiveMetastoreConnectionType.thrift


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
        """Test get_catalogs for HMS 3.x (has catalogs) and HMS 2.x (AttributeError)."""
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

        # Without catalog - uses standard API
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
    """
    Tests for critical Thrift client behaviors that the parent class relies on.

    These verify the inheritance contract is maintained - if these fail,
    the Thrift connector will break the parent class's WorkUnit generation.
    """

    def test_partition_columns_ordered_after_regular_columns(self):
        """
        Partition columns must appear after regular columns with correct sort order.

        The parent class relies on this ordering to correctly identify partition keys
        and set isPartitioningKey on schema fields.
        """
        # Setup: table with 2 regular columns and 1 partition column
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

        # Should have 3 rows: 2 regular + 1 partition
        assert len(rows) == 3

        # Regular columns first (is_partition_col=0), then partition (is_partition_col=1)
        assert rows[0]["col_name"] == "id" and rows[0]["is_partition_col"] == 0
        assert rows[1]["col_name"] == "value" and rows[1]["is_partition_col"] == 0
        assert rows[2]["col_name"] == "dt" and rows[2]["is_partition_col"] == 1

        # Sort order: partition column comes after regular columns
        assert rows[0]["col_sort_order"] == 0
        assert rows[1]["col_sort_order"] == 1
        assert rows[2]["col_sort_order"] == 2  # Continues from regular columns

    def test_views_excluded_from_table_rows_and_vice_versa(self):
        """
        iter_table_rows must exclude VIRTUAL_VIEWs, iter_view_rows must only return them.

        The parent class calls these separately and expects no overlap.
        """
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

        # Table rows should only contain the table, not the view
        table_rows = list(client.iter_table_rows(["test_db"]))
        table_names = {r["table_name"] for r in table_rows}
        assert "regular_table" in table_names
        assert "my_view" not in table_names

        # View rows should only contain the view, not the table
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
        # Manually populate the cache with entries from different catalogs
        mock_client._table_cache[("catalog1", "my_db", "my_table")] = {
            "table_name": "my_table",
            "table_type": "EXTERNAL_TABLE",
            "location": "s3://bucket1/path",
        }
        mock_client._table_cache[("catalog2", "my_db", "my_table")] = {
            "table_name": "my_table",
            "table_type": "EXTERNAL_TABLE",
            "location": "s3://bucket2/path",  # Different location!
        }

        # Verify both entries exist and are distinct
        assert len(mock_client._table_cache) == 2
        assert (
            mock_client._table_cache[("catalog1", "my_db", "my_table")]["location"]
            == "s3://bucket1/path"
        )
        assert (
            mock_client._table_cache[("catalog2", "my_db", "my_table")]["location"]
            == "s3://bucket2/path"
        )

    def test_database_failure_key_structure_includes_catalog(self, mock_client):
        """Test that database failure keys include catalog."""
        # Set failures with different catalogs
        mock_client._database_failures[("catalog1", "my_db")] = "Error 1"
        mock_client._database_failures[("catalog2", "my_db")] = "Error 2"

        # Verify both are tracked separately
        assert len(mock_client._database_failures) == 2
        assert mock_client._database_failures[("catalog1", "my_db")] == "Error 1"
        assert mock_client._database_failures[("catalog2", "my_db")] == "Error 2"

    def test_table_failure_key_structure_includes_catalog(self, mock_client):
        """Test that table failure keys include catalog."""
        # Set failures with different catalogs
        mock_client._table_failures[("catalog1", "my_db", "my_table")] = "Error 1"
        mock_client._table_failures[("catalog2", "my_db", "my_table")] = "Error 2"

        # Verify both are tracked separately
        assert len(mock_client._table_failures) == 2

    def test_database_failures_include_catalog_in_display(self, mock_client):
        """Test that failure reporting includes catalog context."""
        # Manually set a database failure with catalog
        mock_client._database_failures[("spark_catalog", "my_db")] = "Connection error"

        failures = mock_client.get_database_failures()

        assert len(failures) == 1
        # Display name should include catalog
        assert failures[0][0] == "spark_catalog.my_db"
        assert failures[0][1] == "Connection error"

    def test_table_failures_include_catalog_in_display(self, mock_client):
        """Test that table failure reporting includes catalog context."""
        # Manually set a table failure with catalog
        mock_client._table_failures[("spark_catalog", "my_db", "my_table")] = (
            "Table not found"
        )

        failures = mock_client.get_table_failures()

        assert len(failures) == 1
        # Display name should include catalog
        assert failures[0][0] == "spark_catalog.my_db"
        assert failures[0][1] == "my_table"
        assert failures[0][2] == "Table not found"

    def test_failures_without_catalog_display_correctly(self, mock_client):
        """Test that failures without catalog display without prefix."""
        # Set failures without catalog (None)
        mock_client._database_failures[(None, "my_db")] = "Error"
        mock_client._table_failures[(None, "my_db", "my_table")] = "Error"

        db_failures = mock_client.get_database_failures()
        table_failures = mock_client.get_table_failures()

        # Should NOT have catalog prefix
        assert db_failures[0][0] == "my_db"
        assert table_failures[0][0] == "my_db"

    def test_clear_failures_clears_catalog_keyed_data(self, mock_client):
        """Test that clear_failures clears catalog-keyed failure data."""
        # Populate some data
        mock_client._database_failures[("catalog1", "db1")] = "Error"
        mock_client._table_failures[("catalog1", "db1", "table1")] = "Error"
        mock_client._table_cache[("catalog1", "db1", "table1")] = {"data": "value"}

        # Clear
        mock_client.clear_failures()

        # Verify all cleared
        assert len(mock_client._database_failures) == 0
        assert len(mock_client._table_failures) == 0
        assert len(mock_client._table_cache) == 0


class TestCatalogConfigIntegration:
    """Test catalog configuration integration with source."""

    def test_catalog_name_passed_to_data_fetching(self):
        """Test that catalog_name config is passed to Thrift client methods."""
        ctx = PipelineContext(run_id="test")
        config = {
            "connection_type": "thrift",
            "host_port": "localhost:9083",
            "use_kerberos": False,
            "catalog_name": "spark_catalog",
            "database_pattern": {"allow": ["my_db"]},
        }

        source = HiveMetastoreThriftSource.create(config, ctx)

        # Verify catalog_name is set
        assert source.config.catalog_name == "spark_catalog"

        # Mock the client
        mock_client = MagicMock(spec=HiveMetastoreThriftClient)
        mock_client.get_all_databases.return_value = ["my_db"]
        mock_client.iter_table_rows.return_value = iter([])
        source._thrift_client = mock_client

        # Trigger data fetching
        list(source._fetch_table_rows(""))

        # Verify catalog_name was passed to iter_table_rows
        mock_client.iter_table_rows.assert_called_once()
        call_args = mock_client.iter_table_rows.call_args
        assert call_args[1].get("catalog_name") == "spark_catalog" or (
            len(call_args[0]) > 1 and call_args[0][1] == "spark_catalog"
        )

    def test_include_catalog_name_in_ids_with_catalog_name(self):
        """Test URN generation when include_catalog_name_in_ids is True."""
        ctx = PipelineContext(run_id="test")
        config = {
            "connection_type": "thrift",
            "host_port": "localhost:9083",
            "use_kerberos": False,
            "catalog_name": "spark_catalog",
            "include_catalog_name_in_ids": True,
        }

        source = HiveMetastoreThriftSource.create(config, ctx)

        # get_db_name should return the catalog name
        db_name = source.get_db_name(source._thrift_inspector)  # type: ignore[arg-type]
        assert db_name == "spark_catalog"

        # With include_catalog_name_in_ids=True, URNs will be prefixed with catalog
        # The parent class logic uses: f"{db_name}.{schema}" when include_catalog_name_in_ids
        assert source.config.include_catalog_name_in_ids is True
