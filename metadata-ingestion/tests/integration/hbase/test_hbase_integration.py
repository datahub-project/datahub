"""
Integration tests for HBase source

NOTE: These tests use mocked HBase connections (no real HBase required).
They test the integration logic and data flow, while unit tests validate individual components.
Real HBase testing requires manual verification with an actual HBase cluster.
"""

from typing import List
from unittest import mock

import pytest

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.hbase.hbase import HBaseSource, HBaseSourceConfig
from datahub.sdk.container import Container
from datahub.sdk.dataset import Dataset
from datahub.sdk.entity import Entity

# Mark all tests in this module as integration tests
pytestmark = pytest.mark.integration


@pytest.fixture
def mock_hbase_connection():
    """Mock happybase connection for testing without real HBase."""
    with mock.patch("happybase.Connection") as mock_conn:
        # Mock connection instance
        conn_instance = mock.MagicMock()

        # Mock tables() to return test tables
        conn_instance.tables.return_value = [
            b"test_table1",
            b"test_table2",
        ]

        # Mock table() to return table instance with families()
        def mock_table(name):
            table_mock = mock.MagicMock()
            if name == b"test_table1":
                table_mock.families.return_value = {
                    b"cf1": {b"MAX_VERSIONS": b"3"},
                    b"cf2": {b"MAX_VERSIONS": b"1"},
                }
            elif name == b"test_table2":
                table_mock.families.return_value = {
                    b"info": {b"MAX_VERSIONS": b"1"},
                    b"data": {b"MAX_VERSIONS": b"1"},
                }
            return table_mock

        conn_instance.table.side_effect = mock_table
        mock_conn.return_value = conn_instance

        yield mock_conn


class TestHBaseIntegration:
    """Integration tests for HBase source with mocked connections."""

    def test_connection_to_hbase(self, mock_hbase_connection):
        """Test that we can connect to HBase."""
        config = HBaseSourceConfig(host="localhost", port=9090)
        ctx = PipelineContext(run_id="test-integration")
        source = HBaseSource(ctx, config)

        result = source._connect()
        assert result is True
        assert source.connection is not None

        # Verify connection was called with correct params
        mock_hbase_connection.assert_called_once()
        call_kwargs = mock_hbase_connection.call_args[1]
        assert call_kwargs["host"] == "localhost"
        assert call_kwargs["port"] == 9090

        source.close()

    def test_get_namespaces(self, mock_hbase_connection):
        """Test getting namespaces from HBase."""
        config = HBaseSourceConfig(host="localhost", port=9090)
        ctx = PipelineContext(run_id="test-integration")
        source = HBaseSource(ctx, config)

        source._connect()
        namespaces = source._get_namespaces()

        # Should extract default namespace from table names
        assert "default" in namespaces
        assert len(namespaces) >= 1

        source.close()

    def test_get_tables_in_default_namespace(self, mock_hbase_connection):
        """Test getting tables in default namespace."""
        config = HBaseSourceConfig(host="localhost", port=9090)
        ctx = PipelineContext(run_id="test-integration")
        source = HBaseSource(ctx, config)

        source._connect()
        tables = source._get_tables_in_namespace("default")

        assert "test_table1" in tables
        assert "test_table2" in tables
        assert len(tables) >= 2

        source.close()

    def test_get_table_descriptor(self, mock_hbase_connection):
        """Test getting table descriptor with column families."""
        config = HBaseSourceConfig(host="localhost", port=9090)
        ctx = PipelineContext(run_id="test-integration")
        source = HBaseSource(ctx, config)

        source._connect()
        descriptor = source._get_table_descriptor("test_table1")

        assert descriptor is not None
        assert "column_families" in descriptor
        assert "cf1" in descriptor["column_families"]
        assert "cf2" in descriptor["column_families"]

        source.close()

    def test_full_ingestion(self, mock_hbase_connection):
        """Test full ingestion process."""
        config = HBaseSourceConfig(host="localhost", port=9090)
        ctx = PipelineContext(run_id="test-integration")
        source = HBaseSource(ctx, config)

        workunits: List[Entity] = list(source.get_workunits_internal())

        # Should have containers for namespaces and datasets for tables
        containers = [wu for wu in workunits if isinstance(wu, Container)]
        datasets = [wu for wu in workunits if isinstance(wu, Dataset)]

        # At least default namespace container
        assert len(containers) >= 1

        # At least our 2 test tables
        assert len(datasets) >= 2

        # Check that default namespace is present
        namespace_names = {c.display_name for c in containers}
        assert "default" in namespace_names

        # Check that test tables are present
        table_names = {d.display_name for d in datasets}
        assert "test_table1" in table_names
        assert "test_table2" in table_names

        # Check report
        report = source.get_report()
        assert report.num_namespaces_scanned >= 1
        assert report.num_tables_scanned >= 2
        assert report.num_tables_failed == 0

        source.close()

    def test_ingestion_with_namespace_filter(self, mock_hbase_connection):
        """Test ingestion with namespace filtering."""
        config = HBaseSourceConfig(
            host="localhost",
            port=9090,
            namespace_pattern={"allow": ["default"]},
        )
        ctx = PipelineContext(run_id="test-integration")
        source = HBaseSource(ctx, config)

        workunits: List[Entity] = list(source.get_workunits_internal())

        containers = [wu for wu in workunits if isinstance(wu, Container)]
        datasets = [wu for wu in workunits if isinstance(wu, Dataset)]

        # Should only have default namespace
        assert len(containers) == 1
        assert containers[0].display_name == "default"

        # Should have default namespace tables
        assert len(datasets) >= 2
        table_names = {d.display_name for d in datasets}
        assert "test_table1" in table_names
        assert "test_table2" in table_names

        source.close()

    def test_ingestion_with_table_filter(self, mock_hbase_connection):
        """Test ingestion with table filtering."""
        config = HBaseSourceConfig(
            host="localhost",
            port=9090,
            table_pattern={"allow": ["test_table1"]},
        )
        ctx = PipelineContext(run_id="test-integration")
        source = HBaseSource(ctx, config)

        workunits: List[Entity] = list(source.get_workunits_internal())

        datasets = [wu for wu in workunits if isinstance(wu, Dataset)]

        # Should only have 1 table
        assert len(datasets) == 1
        table_names = {d.display_name for d in datasets}
        assert "test_table1" in table_names

        source.close()

    def test_schema_extraction(self, mock_hbase_connection):
        """Test schema extraction from column families."""
        config = HBaseSourceConfig(host="localhost", port=9090)
        ctx = PipelineContext(run_id="test-integration")
        source = HBaseSource(ctx, config)

        source._connect()
        descriptor = source._get_table_descriptor("test_table1")

        # Verify column families are extracted
        assert "cf1" in descriptor["column_families"]
        assert "cf2" in descriptor["column_families"]

        source.close()

    def test_custom_properties(self, mock_hbase_connection):
        """Test custom properties are captured."""
        config = HBaseSourceConfig(host="localhost", port=9090)
        ctx = PipelineContext(run_id="test-integration")
        source = HBaseSource(ctx, config)

        workunits: List[Entity] = list(source.get_workunits_internal())
        datasets = [wu for wu in workunits if isinstance(wu, Dataset)]

        # Verify datasets have custom properties
        assert len(datasets) > 0
        for dataset in datasets:
            assert hasattr(dataset, "custom_properties")
            # Should have some custom properties populated
            assert len(dataset.custom_properties) > 0

        source.close()
