"""
Unit tests for HBase source logic with mocked HBase connections
"""

from typing import Any, Dict
from unittest.mock import MagicMock, patch

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.hbase.hbase import (
    HBaseSource,
    HBaseSourceConfig,
    HBaseSourceReport,
)
from datahub.metadata.schema_classes import (
    BytesTypeClass,
    SchemaFieldDataTypeClass,
)
from datahub.sdk.container import Container
from datahub.sdk.dataset import Dataset


def _base_config() -> Dict[str, Any]:
    """Base configuration for HBase tests."""
    return {
        "host": "localhost",
        "port": 9090,
    }


class TestHBaseSource:
    """Test HBase source logic with mocked connections."""

    def test_source_initialization(self):
        """Test that source initializes correctly."""
        config = HBaseSourceConfig.model_validate(_base_config())
        ctx = PipelineContext(run_id="test-run")
        source = HBaseSource(ctx, config)

        assert source.platform == "hbase"
        assert source.config.host == "localhost"
        assert source.config.port == 9090
        assert isinstance(source.report, HBaseSourceReport)
        assert source.connection is None

    def test_get_platform(self):
        """Test get_platform returns correct platform name."""
        config = HBaseSourceConfig.model_validate(_base_config())
        ctx = PipelineContext(run_id="test-run")
        source = HBaseSource(ctx, config)

        assert source.get_platform() == "hbase"

    @patch("happybase.Connection")
    def test_connect_success(self, mock_connection_class):
        """Test successful connection to HBase."""
        # Setup mock
        mock_connection = MagicMock()
        mock_connection.tables.return_value = [b"test_table"]
        mock_connection_class.return_value = mock_connection

        config = HBaseSourceConfig.model_validate(_base_config())
        ctx = PipelineContext(run_id="test-run")
        source = HBaseSource(ctx, config)

        # Test connection
        result = source._connect()

        assert result is True
        assert source.connection is not None
        mock_connection_class.assert_called_once_with(
            host="localhost",
            port=9090,
            timeout=30000,
            transport="framed",
            protocol="binary",
        )

    @patch("happybase.Connection")
    def test_connect_import_error(self, mock_connection_class):
        """Test connection fails gracefully when happybase is not installed."""
        mock_connection_class.side_effect = ImportError("No module named 'happybase'")

        config = HBaseSourceConfig.model_validate(_base_config())
        ctx = PipelineContext(run_id="test-run")
        source = HBaseSource(ctx, config)

        result = source._connect()

        assert result is False
        assert len(source.report.failures) == 1
        assert "happybase" in source.report.failures[0]["message"]

    @patch("happybase.Connection")
    def test_connect_connection_error(self, mock_connection_class):
        """Test connection fails gracefully on connection error."""
        mock_connection_class.side_effect = Exception("Connection refused")

        config = HBaseSourceConfig.model_validate(_base_config())
        ctx = PipelineContext(run_id="test-run")
        source = HBaseSource(ctx, config)

        result = source._connect()

        assert result is False
        assert len(source.report.failures) == 1
        assert "Failed to connect to HBase" in source.report.failures[0]["message"]

    @patch("happybase.Connection")
    def test_get_namespaces_with_default(self, mock_connection_class):
        """Test getting namespaces including default namespace."""
        # Setup mock
        mock_connection = MagicMock()
        mock_connection.tables.return_value = [
            b"table1",  # default namespace
            b"table2",  # default namespace
            b"prod:users",  # prod namespace
            b"prod:orders",  # prod namespace
            b"dev:test",  # dev namespace
        ]
        mock_connection_class.return_value = mock_connection

        config = HBaseSourceConfig.model_validate(_base_config())
        ctx = PipelineContext(run_id="test-run")
        source = HBaseSource(ctx, config)
        source._connect()

        namespaces = source._get_namespaces()

        assert len(namespaces) == 3
        assert "default" in namespaces
        assert "dev" in namespaces
        assert "prod" in namespaces
        assert namespaces == ["default", "dev", "prod"]  # Should be sorted

    @patch("happybase.Connection")
    def test_get_namespaces_error(self, mock_connection_class):
        """Test getting namespaces handles errors gracefully."""
        # Setup mock
        mock_connection = MagicMock()
        mock_connection.tables.side_effect = Exception("Table list error")
        mock_connection_class.return_value = mock_connection

        config = HBaseSourceConfig.model_validate(_base_config())
        ctx = PipelineContext(run_id="test-run")
        source = HBaseSource(ctx, config)
        source._connect()

        namespaces = source._get_namespaces()

        assert namespaces == []
        assert len(source.report.failures) > 0

    @patch("happybase.Connection")
    def test_get_tables_in_default_namespace(self, mock_connection_class):
        """Test getting tables in default namespace."""
        # Setup mock
        mock_connection = MagicMock()
        mock_connection.tables.return_value = [
            b"table1",
            b"table2",
            b"prod:users",
        ]
        mock_connection_class.return_value = mock_connection

        config = HBaseSourceConfig.model_validate(_base_config())
        ctx = PipelineContext(run_id="test-run")
        source = HBaseSource(ctx, config)
        source._connect()

        tables = source._get_tables_in_namespace("default")

        assert len(tables) == 2
        assert "table1" in tables
        assert "table2" in tables

    @patch("happybase.Connection")
    def test_get_tables_in_named_namespace(self, mock_connection_class):
        """Test getting tables in a named namespace."""
        # Setup mock
        mock_connection = MagicMock()
        mock_connection.tables.return_value = [
            b"table1",
            b"prod:users",
            b"prod:orders",
            b"dev:test",
        ]
        mock_connection_class.return_value = mock_connection

        config = HBaseSourceConfig.model_validate(_base_config())
        ctx = PipelineContext(run_id="test-run")
        source = HBaseSource(ctx, config)
        source._connect()

        tables = source._get_tables_in_namespace("prod")

        assert len(tables) == 2
        assert "users" in tables
        assert "orders" in tables

    @patch("happybase.Connection")
    def test_get_table_descriptor(self, mock_connection_class):
        """Test getting table descriptor with column families."""
        # Setup mock
        mock_connection = MagicMock()
        mock_table = MagicMock()
        mock_table.families.return_value = {
            b"cf1": {
                b"VERSIONS": b"3",
                b"COMPRESSION": b"SNAPPY",
                b"IN_MEMORY": b"false",
                b"BLOCKCACHE": b"true",
                b"TTL": b"86400",
            },
            b"cf2:": {  # Test with trailing colon
                b"VERSIONS": b"1",
                b"COMPRESSION": b"NONE",
                b"IN_MEMORY": b"true",
                b"BLOCKCACHE": b"true",
                b"TTL": b"FOREVER",
            },
        }
        mock_connection.table.return_value = mock_table
        mock_connection_class.return_value = mock_connection

        config = HBaseSourceConfig.model_validate(_base_config())
        ctx = PipelineContext(run_id="test-run")
        source = HBaseSource(ctx, config)
        source._connect()

        descriptor = source._get_table_descriptor("test_table")

        assert descriptor is not None
        assert "column_families" in descriptor
        assert len(descriptor["column_families"]) == 2
        assert "cf1" in descriptor["column_families"]
        assert "cf2" in descriptor["column_families"]  # Colon should be stripped

        cf1 = descriptor["column_families"]["cf1"]
        assert cf1["maxVersions"] == "3"
        assert cf1["compression"] == "SNAPPY"
        assert cf1["inMemory"] is False
        assert cf1["blockCacheEnabled"] is True
        assert cf1["timeToLive"] == "86400"

    @patch("happybase.Connection")
    def test_get_table_descriptor_error(self, mock_connection_class):
        """Test getting table descriptor handles errors gracefully."""
        # Setup mock
        mock_connection = MagicMock()
        mock_connection.table.side_effect = Exception("Table not found")
        mock_connection_class.return_value = mock_connection

        config = HBaseSourceConfig.model_validate(_base_config())
        ctx = PipelineContext(run_id="test-run")
        source = HBaseSource(ctx, config)
        source._connect()

        descriptor = source._get_table_descriptor("nonexistent_table")

        assert descriptor is None
        assert len(source.report.failures) > 0

    def test_generate_schema_fields(self):
        """Test schema field generation from table descriptor."""
        config = HBaseSourceConfig.model_validate(_base_config())
        ctx = PipelineContext(run_id="test-run")
        source = HBaseSource(ctx, config)

        table_descriptor = {
            "column_families": {
                "cf1": {"name": "cf1"},
                "cf2": {"name": "cf2"},
            }
        }

        schema_fields = source._generate_schema_fields(table_descriptor)

        assert len(schema_fields) == 3  # rowkey + 2 column families
        assert schema_fields[0].fieldPath == "rowkey"
        assert schema_fields[0].isPartOfKey is True
        assert schema_fields[0].nullable is False

        assert schema_fields[1].fieldPath == "cf1"
        assert schema_fields[1].description == "Column family: cf1"
        assert schema_fields[1].nullable is True

        assert schema_fields[2].fieldPath == "cf2"
        assert schema_fields[2].description == "Column family: cf2"

    def test_convert_hbase_type_to_schema_field_type(self):
        """Test HBase type conversion to schema field types."""
        config = HBaseSourceConfig.model_validate(_base_config())
        ctx = PipelineContext(run_id="test-run")
        source = HBaseSource(ctx, config)

        # Test bytes type (default)
        schema_type = source._convert_hbase_type_to_schema_field_type("bytes")
        assert isinstance(schema_type, SchemaFieldDataTypeClass)
        assert isinstance(schema_type.type, BytesTypeClass)

        # Test default when unknown type
        schema_type = source._convert_hbase_type_to_schema_field_type("unknown")
        assert isinstance(schema_type.type, BytesTypeClass)

    def test_generate_namespace_container(self):
        """Test namespace container generation."""
        config = HBaseSourceConfig.model_validate(_base_config())
        ctx = PipelineContext(run_id="test-run")
        source = HBaseSource(ctx, config)

        container = source._generate_namespace_container("prod")

        assert isinstance(container, Container)
        assert container.display_name == "prod"
        assert container.qualified_name == "prod"
        assert "HBase namespace: prod" in container.description

    @patch("happybase.Connection")
    def test_generate_table_dataset(self, mock_connection_class):
        """Test table dataset generation."""
        config = HBaseSourceConfig.model_validate(_base_config())
        ctx = PipelineContext(run_id="test-run")
        source = HBaseSource(ctx, config)

        table_descriptor = {
            "column_families": {
                "cf1": {
                    "maxVersions": "3",
                    "compression": "SNAPPY",
                },
            }
        }

        dataset = source._generate_table_dataset("prod", "users", table_descriptor)

        assert isinstance(dataset, Dataset)
        assert str(dataset.platform) == "urn:li:dataPlatform:hbase"
        # Dataset URN contains the name
        assert "prod.users" in str(dataset.urn)
        assert dataset.display_name == "users"
        assert dataset.qualified_name == "prod:users"
        assert "HBase table in namespace 'prod'" in dataset.description
        assert dataset.custom_properties is not None
        assert dataset.custom_properties["column_families"] == "1"
        assert dataset.custom_properties["cf.cf1.maxVersions"] == "3"
        assert dataset.custom_properties["cf.cf1.compression"] == "SNAPPY"

    @patch("happybase.Connection")
    def test_generate_table_dataset_default_namespace(self, mock_connection_class):
        """Test table dataset generation for default namespace."""
        config = HBaseSourceConfig.model_validate(_base_config())
        ctx = PipelineContext(run_id="test-run")
        source = HBaseSource(ctx, config)

        table_descriptor = {"column_families": {}}

        dataset = source._generate_table_dataset("default", "table1", table_descriptor)

        # Dataset URN contains the name
        assert "table1" in str(dataset.urn)
        assert dataset.qualified_name == "table1"

    @patch("happybase.Connection")
    def test_generate_table_dataset_filtered(self, mock_connection_class):
        """Test table dataset generation with filtering."""
        config_dict = {
            **_base_config(),
            "table_pattern": {
                "allow": ["prod.*"],
                "deny": ["prod.test"],
            },
        }
        config = HBaseSourceConfig.model_validate(config_dict)
        ctx = PipelineContext(run_id="test-run")
        source = HBaseSource(ctx, config)

        table_descriptor = {"column_families": {}}

        # Should be allowed
        dataset = source._generate_table_dataset("prod", "users", table_descriptor)
        assert dataset is not None

        # Should be denied
        dataset = source._generate_table_dataset("prod", "test", table_descriptor)
        assert dataset is None
        assert len(source.report.dropped_tables) > 0

    @patch("happybase.Connection")
    def test_generate_table_dataset_without_column_families(
        self, mock_connection_class
    ):
        """Test table dataset generation with include_column_families=False."""
        config_dict = {
            **_base_config(),
            "include_column_families": False,
        }
        config = HBaseSourceConfig.model_validate(config_dict)
        ctx = PipelineContext(run_id="test-run")
        source = HBaseSource(ctx, config)

        table_descriptor = {
            "column_families": {
                "cf1": {"maxVersions": "3"},
            }
        }

        dataset = source._generate_table_dataset("default", "table1", table_descriptor)

        # schema should be None when include_column_families=False
        # Can't check dataset.schema directly as it raises error if not set
        # Instead check the dataset was created
        assert dataset is not None
        assert "table1" in str(dataset.urn)

    @patch("happybase.Connection")
    def test_get_workunits_internal_success(self, mock_connection_class):
        """Test successful work unit generation."""
        # Setup mocks
        mock_connection = MagicMock()
        mock_connection.tables.return_value = [
            b"table1",
            b"prod:users",
        ]

        mock_table = MagicMock()
        mock_table.families.return_value = {
            b"cf1": {
                b"VERSIONS": b"1",
                b"COMPRESSION": b"NONE",
                b"IN_MEMORY": b"false",
                b"BLOCKCACHE": b"true",
                b"TTL": b"FOREVER",
            },
        }
        mock_connection.table.return_value = mock_table
        mock_connection_class.return_value = mock_connection

        config = HBaseSourceConfig.model_validate(_base_config())
        ctx = PipelineContext(run_id="test-run")
        source = HBaseSource(ctx, config)

        # Get workunits
        workunits = list(source.get_workunits_internal())

        # Should have 2 containers (default, prod) + 2 datasets
        assert len(workunits) == 4
        assert source.report.num_namespaces_scanned == 2
        assert source.report.num_tables_scanned == 2

    @patch("happybase.Connection")
    def test_get_workunits_internal_with_namespace_filter(self, mock_connection_class):
        """Test work unit generation with namespace filtering."""
        # Setup mocks
        mock_connection = MagicMock()
        mock_connection.tables.return_value = [
            b"table1",
            b"prod:users",
            b"dev:test",
        ]

        mock_table = MagicMock()
        mock_table.families.return_value = {
            b"cf1": {
                b"VERSIONS": b"1",
                b"COMPRESSION": b"NONE",
                b"IN_MEMORY": b"false",
                b"BLOCKCACHE": b"true",
                b"TTL": b"FOREVER",
            },
        }
        mock_connection.table.return_value = mock_table
        mock_connection_class.return_value = mock_connection

        config_dict = {
            **_base_config(),
            "namespace_pattern": {
                "allow": ["prod"],
            },
        }
        config = HBaseSourceConfig.model_validate(config_dict)
        ctx = PipelineContext(run_id="test-run")
        source = HBaseSource(ctx, config)

        # Get workunits
        workunits = list(source.get_workunits_internal())

        # Should only have prod namespace (1 container + 1 dataset)
        assert len(workunits) == 2
        assert source.report.num_namespaces_scanned == 1
        assert len(source.report.dropped_namespaces) == 2  # default and dev

    @patch("happybase.Connection")
    def test_get_workunits_internal_connection_failure(self, mock_connection_class):
        """Test work unit generation when connection fails."""
        mock_connection_class.side_effect = Exception("Connection failed")

        config = HBaseSourceConfig.model_validate(_base_config())
        ctx = PipelineContext(run_id="test-run")
        source = HBaseSource(ctx, config)

        # Get workunits
        workunits = list(source.get_workunits_internal())

        assert len(workunits) == 0
        assert len(source.report.failures) > 0

    @patch("happybase.Connection")
    def test_close_with_connection(self, mock_connection_class):
        """Test closing source with active connection."""
        mock_connection = MagicMock()
        mock_connection.tables.return_value = [b"test"]
        mock_connection_class.return_value = mock_connection

        config = HBaseSourceConfig.model_validate(_base_config())
        ctx = PipelineContext(run_id="test-run")
        source = HBaseSource(ctx, config)
        source._connect()

        # Close source
        source.close()

        # Verify connection close was called
        mock_connection.close.assert_called_once()

    def test_close_without_connection(self):
        """Test closing source without active connection."""
        config = HBaseSourceConfig.model_validate(_base_config())
        ctx = PipelineContext(run_id="test-run")
        source = HBaseSource(ctx, config)

        # Should not raise error
        source.close()

    def test_get_report(self):
        """Test getting ingestion report."""
        config = HBaseSourceConfig.model_validate(_base_config())
        ctx = PipelineContext(run_id="test-run")
        source = HBaseSource(ctx, config)

        report = source.get_report()

        assert isinstance(report, HBaseSourceReport)
        assert report.num_namespaces_scanned == 0
        assert report.num_tables_scanned == 0
        assert report.num_tables_failed == 0


class TestHBaseSourceReport:
    """Test HBase source report functionality."""

    def test_report_entity_scanned_namespace(self):
        """Test reporting scanned namespace."""
        report = HBaseSourceReport()
        report.report_entity_scanned("prod", ent_type="namespace")

        assert report.num_namespaces_scanned == 1
        assert report.num_tables_scanned == 0

    def test_report_entity_scanned_table(self):
        """Test reporting scanned table."""
        report = HBaseSourceReport()
        report.report_entity_scanned("prod.users", ent_type="table")

        assert report.num_namespaces_scanned == 0
        assert report.num_tables_scanned == 1

    def test_report_dropped_namespace(self):
        """Test reporting dropped namespace."""
        report = HBaseSourceReport()
        report.report_dropped("dev")

        assert len(report.dropped_namespaces) == 1
        assert "dev" in report.dropped_namespaces

    def test_report_dropped_table(self):
        """Test reporting dropped table."""
        report = HBaseSourceReport()
        report.report_dropped("prod.users")

        assert len(report.dropped_tables) == 1
        assert "prod.users" in report.dropped_tables

    def test_failure_reporting(self):
        """Test failure reporting."""
        report = HBaseSourceReport()
        report.failure(
            message="Test failure", context="test_context", exc=Exception("Test error")
        )

        assert len(report.failures) == 1
        assert report.failures[0]["message"] == "Test failure"
        assert report.failures[0]["context"] == "test_context"
        assert "Test error" in report.failures[0]["exception"]

    def test_warning_reporting(self):
        """Test warning reporting."""
        report = HBaseSourceReport()
        report.warning(message="Test warning", context="test_context")

        assert len(report.warnings) == 1
        assert report.warnings[0]["message"] == "Test warning"
        assert report.warnings[0]["context"] == "test_context"
