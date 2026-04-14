import os
from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

from datahub.ingestion.source.unity.proxy import (
    ExternalUpstream,
    TableLineageInfo,
    TableUpstream,
    UnityCatalogApiProxy,
)
from datahub.ingestion.source.unity.proxy_patch import _basic_proxy_auth_header
from datahub.ingestion.source.unity.report import UnityCatalogReport


class TestUnityCatalogProxy:
    @pytest.fixture
    def mock_proxy(self):
        """Create a mock UnityCatalogApiProxy for testing."""
        from databricks.sdk import WorkspaceClient

        mock_workspace_client = MagicMock(spec=WorkspaceClient)
        mock_workspace_client.config.host = "https://test.databricks.com"
        mock_workspace_client.config.token = "test_token"
        mock_workspace_client.config.warehouse_id = "test_warehouse"

        proxy = UnityCatalogApiProxy(
            workspace_client=mock_workspace_client,
            report=UnityCatalogReport(),
        )
        return proxy

    def test_build_datetime_where_conditions_empty(self, mock_proxy):
        """Test datetime conditions with no start/end time."""
        result = mock_proxy._build_datetime_where_conditions()
        assert result == ""

    def test_build_datetime_where_conditions_start_only(self, mock_proxy):
        """Test datetime conditions with only start time."""
        start_time = datetime(2023, 1, 1, 12, 0, 0)
        result = mock_proxy._build_datetime_where_conditions(start_time=start_time)
        expected = " AND event_time >= '2023-01-01T12:00:00'"
        assert result == expected

    def test_build_datetime_where_conditions_end_only(self, mock_proxy):
        """Test datetime conditions with only end time."""
        end_time = datetime(2023, 12, 31, 23, 59, 59)
        result = mock_proxy._build_datetime_where_conditions(end_time=end_time)
        expected = " AND event_time <= '2023-12-31T23:59:59'"
        assert result == expected

    def test_build_datetime_where_conditions_both(self, mock_proxy):
        """Test datetime conditions with both start and end time."""
        start_time = datetime(2023, 1, 1, 12, 0, 0)
        end_time = datetime(2023, 12, 31, 23, 59, 59)
        result = mock_proxy._build_datetime_where_conditions(
            start_time=start_time, end_time=end_time
        )
        expected = " AND event_time >= '2023-01-01T12:00:00' AND event_time <= '2023-12-31T23:59:59'"
        assert result == expected

    @patch(
        "datahub.ingestion.source.unity.proxy.UnityCatalogApiProxy._execute_sql_query"
    )
    def test_get_catalog_table_lineage_empty(self, mock_execute, mock_proxy):
        """Test get_catalog_table_lineage with no results."""
        mock_execute.return_value = []

        result = mock_proxy.get_catalog_table_lineage_via_system_tables("test_catalog")

        assert len(result) == 0
        mock_execute.assert_called_once()

    @patch(
        "datahub.ingestion.source.unity.proxy.UnityCatalogApiProxy._execute_sql_query"
    )
    def test_get_catalog_table_lineage_with_datetime_filter(
        self, mock_execute, mock_proxy
    ):
        """Test get_catalog_table_lineage with datetime filtering."""
        mock_execute.return_value = []
        start_time = datetime(2023, 1, 1)
        end_time = datetime(2023, 12, 31)

        mock_proxy.get_catalog_table_lineage_via_system_tables(
            "test_catalog", start_time=start_time, end_time=end_time
        )

        # Verify the query contains datetime conditions
        call_args = mock_execute.call_args
        query = call_args[0][0]
        assert "event_time >= '2023-01-01T00:00:00'" in query
        assert "event_time <= '2023-12-31T00:00:00'" in query

    @patch(
        "datahub.ingestion.source.unity.proxy.UnityCatalogApiProxy._execute_sql_query"
    )
    def test_get_catalog_table_lineage_data_processing(self, mock_execute, mock_proxy):
        """Test get_catalog_table_lineage with sample data."""
        mock_data = [
            # Regular table upstream
            {
                "entity_type": "TABLE",
                "entity_id": "entity_1",
                "source_table_full_name": "other_catalog.schema.source_table",
                "source_path": None,
                "source_type": "TABLE",
                "target_table_full_name": "test_catalog.schema.target_table",
                "target_type": "TABLE",
                "last_updated": datetime(2023, 1, 1),
            },
            # External PATH upstream
            {
                "entity_type": "TABLE",
                "entity_id": "path_1",
                "source_table_full_name": None,
                "source_path": "s3://bucket/path/to/file",
                "source_type": "PATH",
                "target_table_full_name": "test_catalog.schema.external_target",
                "target_type": "TABLE",
                "last_updated": datetime(2023, 1, 2),
            },
            # Notebook upstream (notebook writes to table) - source_table_full_name is None
            {
                "entity_type": "NOTEBOOK",
                "entity_id": "notebook_123",
                "source_table_full_name": None,
                "source_path": None,
                "source_type": None,
                "target_table_full_name": "test_catalog.schema.downstream_table",
                "target_type": "TABLE",
                "last_updated": datetime(2023, 1, 3),
            },
            # Notebook downstream (table read by notebook) - target_table_full_name is None
            {
                "entity_type": "NOTEBOOK",
                "entity_id": "notebook_456",
                "source_table_full_name": "test_catalog.schema.upstream_table",
                "source_path": None,
                "source_type": "TABLE",
                "target_table_full_name": None,
                "target_type": None,
                "last_updated": datetime(2023, 1, 4),
            },
        ]
        mock_execute.return_value = mock_data

        result = mock_proxy.get_catalog_table_lineage_via_system_tables("test_catalog")

        # Verify tables are initialized
        assert "test_catalog.schema.target_table" in result
        assert "test_catalog.schema.external_target" in result
        assert "test_catalog.schema.downstream_table" in result
        assert "test_catalog.schema.upstream_table" in result

        # Check table upstream
        target_lineage = result["test_catalog.schema.target_table"]
        assert len(target_lineage.upstreams) == 1
        assert (
            target_lineage.upstreams[0].table_name
            == "other_catalog.schema.source_table"
        )
        assert target_lineage.upstreams[0].source_type == "TABLE"

        # Check external upstream
        external_lineage = result["test_catalog.schema.external_target"]
        assert len(external_lineage.external_upstreams) == 1
        assert external_lineage.external_upstreams[0].path == "s3://bucket/path/to/file"
        assert external_lineage.external_upstreams[0].source_type == "PATH"

        # Check notebook upstream (notebook writes to table)
        downstream_lineage = result["test_catalog.schema.downstream_table"]
        assert len(downstream_lineage.upstream_notebooks) == 1
        notebook_ref = downstream_lineage.upstream_notebooks[0]
        assert notebook_ref.id == "notebook_123"

        # Check notebook downstream (table read by notebook)
        upstream_lineage = result["test_catalog.schema.upstream_table"]
        assert len(upstream_lineage.downstream_notebooks) == 1
        notebook_ref = upstream_lineage.downstream_notebooks[0]
        assert notebook_ref.id == "notebook_456"

    @patch(
        "datahub.ingestion.source.unity.proxy.UnityCatalogApiProxy._execute_sql_query"
    )
    def test_get_catalog_column_lineage_empty(self, mock_execute, mock_proxy):
        """Test get_catalog_column_lineage with no results."""
        mock_execute.return_value = []

        result = mock_proxy.get_catalog_column_lineage_via_system_tables("test_catalog")

        assert len(result) == 0
        mock_execute.assert_called_once()

    @patch(
        "datahub.ingestion.source.unity.proxy.UnityCatalogApiProxy._execute_sql_query"
    )
    def test_get_catalog_column_lineage_with_datetime_filter(
        self, mock_execute, mock_proxy
    ):
        """Test get_catalog_column_lineage with datetime filtering."""
        mock_execute.return_value = []
        start_time = datetime(2023, 1, 1)
        end_time = datetime(2023, 12, 31)

        mock_proxy.get_catalog_column_lineage_via_system_tables(
            "test_catalog", start_time=start_time, end_time=end_time
        )

        # Verify the query contains datetime conditions
        call_args = mock_execute.call_args
        query = call_args[0][0]
        assert "event_time >= '2023-01-01T00:00:00'" in query
        assert "event_time <= '2023-12-31T00:00:00'" in query

    @patch(
        "datahub.ingestion.source.unity.proxy.UnityCatalogApiProxy._execute_sql_query"
    )
    def test_get_catalog_column_lineage_data_processing(self, mock_execute, mock_proxy):
        """Test get_catalog_column_lineage with sample data."""
        mock_data = [
            {
                "source_table_catalog": "source_catalog",
                "source_table_schema": "source_schema",
                "source_table_name": "source_table",
                "source_column_name": "source_col",
                "source_type": "TABLE",
                "target_table_schema": "target_schema",
                "target_table_name": "target_table",
                "target_column_name": "target_col",
                "last_updated": datetime(2023, 1, 1),
            }
        ]
        mock_execute.return_value = mock_data

        result = mock_proxy.get_catalog_column_lineage_via_system_tables("test_catalog")

        # Verify nested dictionary structure
        assert "target_schema" in result
        assert "target_table" in result["target_schema"]
        assert "target_col" in result["target_schema"]["target_table"]

        column_lineage = result["target_schema"]["target_table"]["target_col"]
        assert len(column_lineage) == 1
        assert column_lineage[0]["catalog_name"] == "source_catalog"
        assert column_lineage[0]["schema_name"] == "source_schema"
        assert column_lineage[0]["table_name"] == "source_table"
        assert column_lineage[0]["name"] == "source_col"

    def test_dataclass_creation(self):
        """Test creation of lineage dataclasses."""
        # Test TableUpstream
        table_upstream = TableUpstream(
            table_name="catalog.schema.table",
            source_type="TABLE",
            last_updated=datetime(2023, 1, 1),
        )
        assert table_upstream.table_name == "catalog.schema.table"
        assert table_upstream.source_type == "TABLE"
        assert table_upstream.last_updated == datetime(2023, 1, 1)

        # Test ExternalUpstream
        external_upstream = ExternalUpstream(
            path="s3://bucket/path",
            source_type="PATH",
            last_updated=datetime(2023, 1, 2),
        )
        assert external_upstream.path == "s3://bucket/path"
        assert external_upstream.source_type == "PATH"
        assert external_upstream.last_updated == datetime(2023, 1, 2)

        # Test TableLineageInfo with defaults
        lineage_info = TableLineageInfo()
        assert lineage_info.upstreams == []
        assert lineage_info.external_upstreams == []
        assert lineage_info.upstream_notebooks == []
        assert lineage_info.downstream_notebooks == []

    @patch(
        "datahub.ingestion.source.unity.proxy.UnityCatalogApiProxy._execute_sql_query"
    )
    def test_sql_execution_error_handling(self, mock_execute, mock_proxy):
        """Test error handling in lineage methods."""
        mock_execute.side_effect = Exception("SQL execution failed")

        # Test table lineage error handling
        result = mock_proxy.get_catalog_table_lineage_via_system_tables("test_catalog")
        assert len(result) == 0

        # Test column lineage error handling
        result = mock_proxy.get_catalog_column_lineage_via_system_tables("test_catalog")
        assert len(result) == 0

    @patch(
        "datahub.ingestion.source.unity.proxy.UnityCatalogApiProxy.get_catalog_table_lineage_via_system_tables"
    )
    def test_process_system_table_lineage(self, mock_get_lineage, mock_proxy):
        """Test _process_system_table_lineage method."""
        from datetime import datetime

        from datahub.ingestion.source.unity.proxy_types import (
            Catalog,
            Metastore,
            NotebookReference,
            Schema,
            Table,
        )

        # Create mock table object
        metastore = Metastore(
            id="test_metastore",
            name="test_metastore",
            comment=None,
            global_metastore_id="global_123",
            metastore_id="meta_123",
            owner="owner",
            region="us-west-2",
            cloud="aws",
        )

        catalog = Catalog(
            id="test_catalog",
            name="test_catalog",
            metastore=metastore,
            comment=None,
            owner="owner",
            type=None,
        )

        schema = Schema(
            id="test_catalog.test_schema",
            name="test_schema",
            catalog=catalog,
            comment=None,
            owner="owner",
        )

        table = Table(
            id="test_catalog.test_schema.test_table",
            name="test_table",
            schema=schema,
            columns=[],
            storage_location="/path/to/table",
            data_source_format=None,
            table_type=None,
            owner="owner",
            generation=None,
            created_at=None,
            created_by=None,
            updated_at=None,
            updated_by=None,
            table_id="table_123",
            view_definition=None,
            properties={},
            comment=None,
        )

        # Mock lineage data
        mock_lineage_info = TableLineageInfo(
            upstreams=[
                TableUpstream(
                    table_name="source_catalog.source_schema.source_table",
                    source_type="TABLE",
                    last_updated=datetime(2023, 1, 1),
                ),
                TableUpstream(
                    table_name="invalid_table_name",  # Should be skipped due to invalid format
                    source_type="TABLE",
                    last_updated=datetime(2023, 1, 2),
                ),
            ],
            external_upstreams=[
                ExternalUpstream(
                    path="s3://bucket/path/to/file",
                    source_type="PATH",
                    last_updated=datetime(2023, 1, 3),
                )
            ],
            upstream_notebooks=[
                NotebookReference(id=123, last_updated=datetime(2023, 1, 4))
            ],
            downstream_notebooks=[
                NotebookReference(id=456, last_updated=datetime(2023, 1, 5))
            ],
        )

        mock_get_lineage.return_value = {
            "test_catalog.test_schema.test_table": mock_lineage_info
        }

        # Test the method
        start_time = datetime(2023, 1, 1)
        end_time = datetime(2023, 12, 31)
        mock_proxy._process_system_table_lineage(table, start_time, end_time)

        # Verify get_catalog_table_lineage was called with correct parameters
        mock_get_lineage.assert_called_once_with("test_catalog", start_time, end_time)

        # Verify table upstreams were processed correctly
        assert len(table.upstreams) == 1
        table_ref = list(table.upstreams.keys())[0]
        assert table_ref.catalog == "source_catalog"
        assert table_ref.schema == "source_schema"
        assert table_ref.table == "source_table"
        assert table_ref.metastore == "test_metastore"
        assert table_ref.last_updated == datetime(2023, 1, 1)

        # Verify external upstreams were processed
        assert len(table.external_upstreams) == 1
        external_ref = list(table.external_upstreams)[0]
        assert external_ref.path == "s3://bucket/path/to/file"
        assert external_ref.storage_location == "s3://bucket/path/to/file"
        assert external_ref.has_permission is True
        assert external_ref.last_updated == datetime(2023, 1, 3)

        # Verify notebook lineage was processed
        assert len(table.upstream_notebooks) == 1
        assert 123 in table.upstream_notebooks
        upstream_notebook = table.upstream_notebooks[123]
        assert upstream_notebook.id == 123
        assert upstream_notebook.last_updated == datetime(2023, 1, 4)

        assert len(table.downstream_notebooks) == 1
        assert 456 in table.downstream_notebooks
        downstream_notebook = table.downstream_notebooks[456]
        assert downstream_notebook.id == 456
        assert downstream_notebook.last_updated == datetime(2023, 1, 5)

    @patch(
        "datahub.ingestion.source.unity.proxy.UnityCatalogApiProxy.get_catalog_table_lineage_via_system_tables"
    )
    @patch("datahub.ingestion.source.unity.proxy.logger")
    def test_process_system_table_lineage_invalid_table_name(
        self, mock_logger, mock_get_lineage, mock_proxy
    ):
        """Test _process_system_table_lineage with invalid table names."""
        from datahub.ingestion.source.unity.proxy_types import (
            Catalog,
            Metastore,
            Schema,
            Table,
        )

        # Create minimal table object
        metastore = Metastore(
            id="test_metastore",
            name="test_metastore",
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
        table = Table(
            id="test_table",
            name="test_table",
            schema=schema,
            columns=[],
            storage_location=None,
            data_source_format=None,
            table_type=None,
            owner=None,
            generation=None,
            created_at=None,
            created_by=None,
            updated_at=None,
            updated_by=None,
            table_id=None,
            view_definition=None,
            properties={},
            comment=None,
        )

        # Mock lineage with invalid table name format
        mock_lineage_info = TableLineageInfo(
            upstreams=[
                TableUpstream(
                    table_name="invalid.table",  # Only 2 parts, should be skipped
                    source_type="TABLE",
                    last_updated=None,
                )
            ]
        )

        mock_get_lineage.return_value = {
            "test_catalog.test_schema.test_table": mock_lineage_info
        }

        # Test the method
        mock_proxy._process_system_table_lineage(table)

        # Verify warning was logged for invalid table name
        mock_logger.warning.assert_called_once()
        warning_call = mock_logger.warning.call_args[0][0]
        assert "Unexpected upstream table format" in warning_call
        assert "invalid.table" in warning_call

        # Verify no upstreams were added
        assert len(table.upstreams) == 0

    @patch(
        "datahub.ingestion.source.unity.proxy.UnityCatalogApiProxy.get_catalog_table_lineage_via_system_tables"
    )
    def test_process_system_table_lineage_no_lineage_data(
        self, mock_get_lineage, mock_proxy
    ):
        """Test _process_system_table_lineage when no lineage data exists."""
        from datahub.ingestion.source.unity.proxy_types import (
            Catalog,
            Metastore,
            Schema,
            Table,
        )

        # Create minimal table object
        metastore = Metastore(
            id="test_metastore",
            name="test_metastore",
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
        table = Table(
            id="test_table",
            name="test_table",
            schema=schema,
            columns=[],
            storage_location=None,
            data_source_format=None,
            table_type=None,
            owner=None,
            generation=None,
            created_at=None,
            created_by=None,
            updated_at=None,
            updated_by=None,
            table_id=None,
            view_definition=None,
            properties={},
            comment=None,
        )

        # Mock empty lineage data
        mock_get_lineage.return_value = {}

        # Test the method
        mock_proxy._process_system_table_lineage(table)

        # Verify no lineage was added (empty TableLineageInfo should be used)
        assert len(table.upstreams) == 0
        assert len(table.external_upstreams) == 0
        assert len(table.upstream_notebooks) == 0
        assert len(table.downstream_notebooks) == 0

    @patch("datahub.ingestion.source.unity.proxy.WorkspaceClient")
    def test_constructor_with_databricks_api_page_size(self, mock_workspace_client):
        """Test UnityCatalogApiProxy constructor with databricks_api_page_size parameter."""
        from databricks.sdk import WorkspaceClient

        mock_client = MagicMock(spec=WorkspaceClient)
        mock_client.config.host = "https://test.databricks.com"
        mock_client.config.token = "test_token"
        mock_client.config.warehouse_id = "test_warehouse"
        mock_workspace_client.return_value = mock_client

        # Test with default page size (0)
        proxy = UnityCatalogApiProxy(
            workspace_client=mock_client,
            report=UnityCatalogReport(),
        )
        assert proxy.databricks_api_page_size == 0

        # Test with custom page size
        proxy = UnityCatalogApiProxy(
            workspace_client=mock_client,
            report=UnityCatalogReport(),
            databricks_api_page_size=500,
        )
        assert proxy.databricks_api_page_size == 500

    @patch("datahub.ingestion.source.unity.proxy.WorkspaceClient")
    def test_check_basic_connectivity_with_page_size(self, mock_workspace_client):
        """Test check_basic_connectivity passes page size to catalogs.list()."""
        # Setup mock
        mock_client = mock_workspace_client.return_value
        mock_client.catalogs.list.return_value = ["catalog1"]
        mock_client.config.warehouse_id = "test_warehouse"

        proxy = UnityCatalogApiProxy(
            workspace_client=mock_client,
            report=UnityCatalogReport(),
            databricks_api_page_size=100,
        )

        result = proxy.check_basic_connectivity()

        assert result is True
        mock_client.catalogs.list.assert_called_once_with(
            include_browse=True, max_results=100
        )

    @patch("datahub.ingestion.source.unity.proxy.WorkspaceClient")
    def test_catalogs_with_page_size(self, mock_workspace_client):
        """Test catalogs() method passes page size to catalogs.list()."""
        # Setup mock
        mock_client = mock_workspace_client.return_value
        mock_client.catalogs.list.return_value = []
        mock_client.metastores.summary.return_value = None
        mock_client.config.warehouse_id = "test_warehouse"

        proxy = UnityCatalogApiProxy(
            workspace_client=mock_client,
            report=UnityCatalogReport(),
            databricks_api_page_size=200,
        )

        list(proxy.catalogs(metastore=None))

        mock_client.catalogs.list.assert_called_with(
            include_browse=True, max_results=200
        )

    @patch("datahub.ingestion.source.unity.proxy.WorkspaceClient")
    def test_schemas_with_page_size(self, mock_workspace_client):
        """Test schemas() method passes page size to schemas.list()."""
        from datahub.ingestion.source.unity.proxy_types import Catalog, Metastore

        # Setup mock
        mock_client = mock_workspace_client.return_value
        mock_client.schemas.list.return_value = []
        mock_client.config.warehouse_id = "test_warehouse"

        proxy = UnityCatalogApiProxy(
            workspace_client=mock_client,
            report=UnityCatalogReport(),
            databricks_api_page_size=300,
        )

        # Create test catalog
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

        list(proxy.schemas(catalog))

        mock_client.schemas.list.assert_called_with(
            catalog_name="test_catalog", include_browse=True, max_results=300
        )

    @patch("datahub.ingestion.source.unity.proxy.WorkspaceClient")
    def test_tables_with_page_size(self, mock_workspace_client):
        """Test tables() method passes page size to tables.list()."""
        from datahub.ingestion.source.unity.proxy_types import (
            Catalog,
            Metastore,
            Schema,
        )

        # Setup mock
        mock_client = mock_workspace_client.return_value
        mock_client.tables.list.return_value = []
        mock_client.config.warehouse_id = "test_warehouse"

        proxy = UnityCatalogApiProxy(
            workspace_client=mock_client,
            report=UnityCatalogReport(),
            databricks_api_page_size=400,
        )

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
            id="test_schema",
            name="test_schema",
            catalog=catalog,
            comment=None,
            owner=None,
        )

        list(proxy.tables(schema))

        mock_client.tables.list.assert_called_with(
            catalog_name="test_catalog",
            schema_name="test_schema",
            include_browse=True,
            max_results=400,
        )

    @patch("datahub.ingestion.source.unity.proxy.WorkspaceClient")
    def test_workspace_notebooks_with_page_size(self, mock_workspace_client):
        """Test workspace_notebooks() method passes page size to workspace.list()."""
        # Setup mock
        mock_client = mock_workspace_client.return_value
        mock_client.workspace.list.return_value = []
        mock_client.config.warehouse_id = "test_warehouse"

        proxy = UnityCatalogApiProxy(
            workspace_client=mock_client,
            report=UnityCatalogReport(),
            databricks_api_page_size=250,
        )

        list(proxy.workspace_notebooks())

        mock_client.workspace.list.assert_called_with(
            "/", recursive=True, max_results=250
        )

    @patch("datahub.ingestion.source.unity.proxy.WorkspaceClient")
    def test_query_history_with_page_size(self, mock_workspace_client):
        """Test _query_history() method uses databricks_api_page_size for max_results."""
        from datahub.ingestion.source.unity.proxy import QueryFilterWithStatementTypes

        # Setup mock
        mock_client = mock_workspace_client.return_value
        mock_client.api_client.do.return_value = {"res": []}
        mock_client.config.warehouse_id = "test_warehouse"

        proxy = UnityCatalogApiProxy(
            workspace_client=mock_client,
            report=UnityCatalogReport(),
            databricks_api_page_size=150,
        )

        from databricks.sdk.service.sql import QueryStatementType

        filter_query = QueryFilterWithStatementTypes(
            statement_types=[QueryStatementType.SELECT]
        )

        list(proxy._query_history(filter_query))

        # Verify the API call was made with the correct max_results
        mock_client.api_client.do.assert_called_once()
        call_args = mock_client.api_client.do.call_args
        assert call_args[1]["body"]["max_results"] == 150

    # Additional test methods to add to TestUnityCatalogProxy class

    @patch("datahub.ingestion.source.unity.proxy.WorkspaceClient")
    def test_ml_models_with_max_results(self, mock_workspace_client):
        """Test ml_models() method calls registered_models.list() with max_results parameter."""
        from datahub.ingestion.source.unity.proxy_types import (
            Catalog,
            Metastore,
            Schema,
        )

        # Setup mock
        mock_client = mock_workspace_client.return_value
        mock_client.registered_models.list.return_value = []
        mock_client.config.warehouse_id = "test_warehouse"

        proxy = UnityCatalogApiProxy(
            workspace_client=mock_client,
            report=UnityCatalogReport(),
            databricks_api_page_size=150,
        )

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

        list(proxy.ml_models(schema, max_results=500))

        mock_client.registered_models.list.assert_called_with(
            catalog_name="test_catalog", schema_name="test_schema", max_results=500
        )

    @patch("datahub.ingestion.source.unity.proxy.WorkspaceClient")
    def test_ml_models_without_max_results(self, mock_workspace_client):
        """Test ml_models() method calls registered_models.list() without max_results."""
        from datahub.ingestion.source.unity.proxy_types import (
            Catalog,
            Metastore,
            Schema,
        )

        # Setup mock
        mock_client = mock_workspace_client.return_value
        mock_client.registered_models.list.return_value = []
        mock_client.config.warehouse_id = "test_warehouse"

        proxy = UnityCatalogApiProxy(
            workspace_client=mock_client,
            report=UnityCatalogReport(),
        )

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

        list(proxy.ml_models(schema))

        mock_client.registered_models.list.assert_called_with(
            catalog_name="test_catalog", schema_name="test_schema", max_results=None
        )

    @patch("datahub.ingestion.source.unity.proxy.WorkspaceClient")
    def test_ml_model_versions_with_include_aliases_true(self, mock_workspace_client):
        """Test ml_model_versions() method with include_aliases=True."""
        from databricks.sdk.service.catalog import ModelVersionInfo

        from datahub.ingestion.source.unity.proxy_types import Model, ModelVersion

        # Setup mock
        mock_client = mock_workspace_client.return_value
        mock_version = ModelVersionInfo(version=1, comment="Test version")
        mock_client.model_versions.list.return_value = [mock_version]
        mock_client.config.warehouse_id = "test_warehouse"

        # Mock get response with aliases
        mock_detailed_version = ModelVersionInfo(
            version=1, comment="Test version", aliases=[]
        )
        mock_client.model_versions.get.return_value = mock_detailed_version

        proxy = UnityCatalogApiProxy(
            workspace_client=mock_client,
            report=UnityCatalogReport(),
            databricks_api_page_size=75,
        )

        # Create test model
        model = Model(
            id="test_catalog.test_schema.test_model",
            name="test_model",
            description=None,
            schema_name="test_schema",
            catalog_name="test_catalog",
            created_at=None,
            updated_at=None,
        )

        # Mock the _create_ml_model_version method
        with patch.object(proxy, "_create_ml_model_version") as mock_create:
            mock_create.return_value = ModelVersion(
                id="test_model_1",
                name="test_model_1",
                model=model,
                version="1",
                aliases=[],
                description="Test version",
                created_at=None,
                updated_at=None,
                created_by=None,
                run_details=None,
                signature=None,
            )

            list(proxy.ml_model_versions(model, include_aliases=True))

            # Verify list was called
            mock_client.model_versions.list.assert_called_with(
                full_name="test_catalog.test_schema.test_model",
                include_browse=True,
                max_results=75,
            )

            # Verify get was called for the version when include_aliases=True
            mock_client.model_versions.get.assert_called_with(
                "test_catalog.test_schema.test_model", 1, include_aliases=True
            )

    @patch("datahub.ingestion.source.unity.proxy.WorkspaceClient")
    def test_ml_model_versions_with_include_aliases_false(self, mock_workspace_client):
        """Test ml_model_versions() method with include_aliases=False."""
        from databricks.sdk.service.catalog import ModelVersionInfo

        from datahub.ingestion.source.unity.proxy_types import Model, ModelVersion

        # Setup mock
        mock_client = mock_workspace_client.return_value
        mock_version = ModelVersionInfo(version=1, comment="Test version")
        mock_client.model_versions.list.return_value = [mock_version]
        mock_client.config.warehouse_id = "test_warehouse"

        proxy = UnityCatalogApiProxy(
            workspace_client=mock_client,
            report=UnityCatalogReport(),
            databricks_api_page_size=75,
        )

        # Create test model
        model = Model(
            id="test_catalog.test_schema.test_model",
            name="test_model",
            description=None,
            schema_name="test_schema",
            catalog_name="test_catalog",
            created_at=None,
            updated_at=None,
        )

        # Mock the _create_ml_model_version method
        with patch.object(proxy, "_create_ml_model_version") as mock_create:
            mock_create.return_value = ModelVersion(
                id="test_model_1",
                name="test_model_1",
                model=model,
                version="1",
                aliases=[],
                description="Test version",
                created_at=None,
                updated_at=None,
                created_by=None,
                run_details=None,
                signature=None,
            )

            list(proxy.ml_model_versions(model, include_aliases=False))

            # Verify list was called
            mock_client.model_versions.list.assert_called_with(
                full_name="test_catalog.test_schema.test_model",
                include_browse=True,
                max_results=75,
            )

            # Verify get was NOT called when include_aliases=False
            mock_client.model_versions.get.assert_not_called()

    @patch("datahub.ingestion.source.unity.proxy.WorkspaceClient")
    def test_create_ml_model_with_missing_full_name(self, mock_workspace_client):
        """Test _create_ml_model() returns None when full_name is missing."""
        from databricks.sdk.service.catalog import RegisteredModelInfo

        from datahub.ingestion.source.unity.proxy_types import (
            Catalog,
            Metastore,
            Schema,
        )

        mock_client = mock_workspace_client.return_value
        mock_client.config.warehouse_id = "test_warehouse"
        proxy = UnityCatalogApiProxy(
            workspace_client=mock_client,
            report=UnityCatalogReport(),
        )

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

        # Test with missing full_name
        model_info = RegisteredModelInfo(name="test_model", full_name=None)
        result = proxy._create_ml_model(schema, model_info)
        assert result is None
        assert proxy.report.num_ml_models_missing_name == 1

    @patch("datahub.ingestion.source.unity.proxy.WorkspaceClient")
    def test_create_ml_model_version_with_none_version(self, mock_workspace_client):
        """Test _create_ml_model_version() returns None when version is None."""
        from databricks.sdk.service.catalog import ModelVersionInfo

        from datahub.ingestion.source.unity.proxy_types import Model

        mock_client = mock_workspace_client.return_value
        mock_client.config.warehouse_id = "test_warehouse"
        proxy = UnityCatalogApiProxy(
            workspace_client=mock_client,
            report=UnityCatalogReport(),
        )

        # Create test model
        model = Model(
            id="test_catalog.test_schema.test_model",
            name="test_model",
            description=None,
            schema_name="test_schema",
            catalog_name="test_catalog",
            created_at=None,
            updated_at=None,
        )

        # Test with None version
        version_info = ModelVersionInfo(version=None, comment="Test")
        result = proxy._create_ml_model_version(model, version_info)
        assert result is None

    @patch("datahub.ingestion.source.unity.proxy.WorkspaceClient")
    def test_create_ml_model_success(self, mock_workspace_client):
        """Test _create_ml_model() successfully creates a model."""

        from databricks.sdk.service.catalog import RegisteredModelInfo

        from datahub.ingestion.source.unity.proxy_types import (
            Catalog,
            Metastore,
            Schema,
        )

        mock_client = mock_workspace_client.return_value
        mock_client.config.warehouse_id = "test_warehouse"
        proxy = UnityCatalogApiProxy(
            workspace_client=mock_client,
            report=UnityCatalogReport(),
        )

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

        # Mock with valid model info
        model_info = RegisteredModelInfo(
            name="test_model",
            full_name="test_catalog.test_schema.test_model",
            comment="Test comment",
            created_at=1640995200000,  # 2022-01-01 timestamp in milliseconds
            updated_at=1640995200000,
        )

        result = proxy._create_ml_model(schema, model_info)

        assert result is not None
        assert result.id == "test_catalog.test_schema.test_model"
        assert result.name == "test_model"
        assert result.description == "Test comment"
        assert result.schema_name == "test_schema"
        assert result.catalog_name == "test_catalog"

    @patch("datahub.ingestion.source.unity.proxy.WorkspaceClient")
    def test_create_ml_model_version_success(self, mock_workspace_client):
        """Test _create_ml_model_version() successfully creates a model version."""
        from databricks.sdk.service.catalog import (
            ModelVersionInfo,
            RegisteredModelAlias,
        )

        from datahub.ingestion.source.unity.proxy_types import Model

        mock_client = mock_workspace_client.return_value
        mock_client.config.warehouse_id = "test_warehouse"
        proxy = UnityCatalogApiProxy(
            workspace_client=mock_client,
            report=UnityCatalogReport(),
        )

        # Create test model
        model = Model(
            id="test_catalog.test_schema.test_model",
            name="test_model",
            description=None,
            schema_name="test_schema",
            catalog_name="test_catalog",
            created_at=None,
            updated_at=None,
        )

        # Create version info with aliases
        alias1 = RegisteredModelAlias(alias_name="prod")
        alias2 = RegisteredModelAlias(alias_name="latest")

        version_info = ModelVersionInfo(
            version=1,
            comment="Version 1",
            created_at=1640995200000,  # 2022-01-01 timestamp in milliseconds
            updated_at=1640995200000,
            aliases=[alias1, alias2],
            created_by="test_user",
            catalog_name="test_catalog",
            schema_name="test_schema",
            model_name="test_model",
        )

        # Mock the signature extraction since _create_ml_model_version now calls it
        with patch.object(
            proxy, "_extract_signature_from_files_api", return_value=None
        ):
            result = proxy._create_ml_model_version(model, version_info)

        assert result is not None
        assert result.id == "test_catalog.test_schema.test_model_1"
        assert result.name == "test_model_1"
        assert result.model == model
        assert result.version == "1"
        assert result.aliases == ["prod", "latest"]
        assert result.description == "Version 1"
        assert result.created_by == "test_user"
        assert result.signature is None  # Verify signature field exists

    @patch("datahub.ingestion.source.unity.proxy.WorkspaceClient")
    def test_get_run_details_success(self, mock_workspace_client):
        """Test get_run_details() successfully retrieves MLflow run details."""
        from databricks.sdk.service.ml import (
            Metric,
            Param,
            Run,
            RunData,
            RunInfo,
            RunInfoStatus,
            RunTag,
        )

        mock_client = mock_workspace_client.return_value
        mock_client.config.warehouse_id = "test_warehouse"
        proxy = UnityCatalogApiProxy(
            workspace_client=mock_client,
            report=UnityCatalogReport(),
        )

        # Mock MLflow run response
        mock_run_info = RunInfo(
            run_id="test_run_123",
            experiment_id="exp_456",
            status=RunInfoStatus.FINISHED,
            start_time=1640995200000,
            end_time=1640998800000,
            user_id="test_user@example.com",
        )

        mock_run_data = RunData(
            metrics=[
                Metric(key="accuracy", value=0.95),
                Metric(key="loss", value=0.05),
            ],
            params=[
                Param(key="learning_rate", value="0.001"),
                Param(key="batch_size", value="32"),
            ],
            tags=[
                RunTag(key="mlflow.user", value="test_user"),
                RunTag(key="mlflow.source.type", value="NOTEBOOK"),
            ],
        )

        mock_run = Run(info=mock_run_info, data=mock_run_data)

        mock_response = MagicMock()
        mock_response.run = mock_run

        with patch.object(
            proxy._experiments_api, "get_run", return_value=mock_response
        ):
            result = proxy.get_run_details("test_run_123")

        assert result is not None
        assert result.run_id == "test_run_123"
        assert result.experiment_id == "exp_456"
        assert result.status == "FINISHED"
        assert result.user_id == "test_user@example.com"
        assert result.metrics == {"accuracy": 0.95, "loss": 0.05}
        assert result.parameters == {"learning_rate": "0.001", "batch_size": "32"}
        assert result.tags == {
            "mlflow.user": "test_user",
            "mlflow.source.type": "NOTEBOOK",
        }
        assert result.start_time is not None
        assert result.end_time is not None

    @patch("datahub.ingestion.source.unity.proxy.WorkspaceClient")
    def test_get_run_details_with_missing_run(self, mock_workspace_client):
        """Test get_run_details() handles missing run gracefully."""
        mock_client = mock_workspace_client.return_value
        mock_client.config.warehouse_id = "test_warehouse"
        proxy = UnityCatalogApiProxy(
            workspace_client=mock_client,
            report=UnityCatalogReport(),
        )

        mock_response = MagicMock()
        mock_response.run = None

        with patch.object(
            proxy._experiments_api, "get_run", return_value=mock_response
        ):
            result = proxy.get_run_details("missing_run_id")

        assert result is None

    @patch("datahub.ingestion.source.unity.proxy.WorkspaceClient")
    def test_get_run_details_with_empty_metrics_params(self, mock_workspace_client):
        """Test get_run_details() handles empty metrics and parameters."""
        from databricks.sdk.service.ml import Run, RunData, RunInfo, RunInfoStatus

        mock_client = mock_workspace_client.return_value
        mock_client.config.warehouse_id = "test_warehouse"
        proxy = UnityCatalogApiProxy(
            workspace_client=mock_client,
            report=UnityCatalogReport(),
        )

        mock_run_info = RunInfo(
            run_id="test_run_123",
            experiment_id="exp_456",
            status=RunInfoStatus.RUNNING,
            start_time=1640995200000,
            end_time=None,
            user_id=None,
        )

        # Empty data
        mock_run_data = RunData(metrics=[], params=[], tags=[])
        mock_run = Run(info=mock_run_info, data=mock_run_data)

        mock_response = MagicMock()
        mock_response.run = mock_run

        with patch.object(
            proxy._experiments_api, "get_run", return_value=mock_response
        ):
            result = proxy.get_run_details("test_run_123")

        assert result is not None
        assert result.metrics == {}
        assert result.parameters == {}
        assert result.tags == {}
        assert result.end_time is None
        assert result.user_id is None

    @patch("datahub.ingestion.source.unity.proxy.WorkspaceClient")
    def test_get_run_details_with_api_error(self, mock_workspace_client):
        """Test get_run_details() handles API errors gracefully."""
        mock_client = mock_workspace_client.return_value
        mock_client.config.warehouse_id = "test_warehouse"
        proxy = UnityCatalogApiProxy(
            workspace_client=mock_client,
            report=UnityCatalogReport(),
        )

        with patch.object(
            proxy._experiments_api, "get_run", side_effect=Exception("API Error")
        ):
            result = proxy.get_run_details("test_run_123")

        assert result is None
        # Verify warning was reported
        assert len(proxy.report.warnings) > 0

    @patch("datahub.ingestion.source.unity.proxy.WorkspaceClient")
    def test_extract_signature_from_files_api_success(self, mock_workspace_client):
        """Test _extract_signature_from_files_api() successfully extracts signature."""
        from io import BytesIO

        from databricks.sdk.service.catalog import ModelVersionInfo

        mock_client = mock_workspace_client.return_value
        mock_client.config.warehouse_id = "test_warehouse"
        proxy = UnityCatalogApiProxy(
            workspace_client=mock_client,
            report=UnityCatalogReport(),
        )

        model_version = ModelVersionInfo(
            catalog_name="test_catalog",
            schema_name="test_schema",
            model_name="test_model",
            version=1,
        )

        # Mock MLmodel YAML content
        mlmodel_yaml = """
artifact_path: model
flavors:
  python_function:
    env: conda.yaml
    loader_module: mlflow.sklearn
    model_path: model.pkl
    python_version: 3.8.10
mlflow_version: 2.0.1
model_uuid: abc123
signature:
  inputs: '[{"name": "feature1", "type": "double"}, {"name": "feature2", "type": "long"}]'
  outputs: '[{"name": "prediction", "type": "double"}]'
"""

        mock_download_response = MagicMock()
        mock_download_response.contents = BytesIO(mlmodel_yaml.encode("utf-8"))

        with patch.object(
            proxy._files_api, "download", return_value=mock_download_response
        ):
            result = proxy._extract_signature_from_files_api(model_version)

        assert result is not None
        assert result.inputs is not None
        assert len(result.inputs) == 2
        assert result.inputs[0]["name"] == "feature1"
        assert result.inputs[0]["type"] == "double"
        assert result.outputs is not None
        assert len(result.outputs) == 1
        assert result.outputs[0]["name"] == "prediction"

    @patch("datahub.ingestion.source.unity.proxy.WorkspaceClient")
    def test_extract_signature_from_files_api_missing_file(self, mock_workspace_client):
        """Test _extract_signature_from_files_api() handles missing MLmodel file."""
        from databricks.sdk.service.catalog import ModelVersionInfo

        mock_client = mock_workspace_client.return_value
        mock_client.config.warehouse_id = "test_warehouse"
        proxy = UnityCatalogApiProxy(
            workspace_client=mock_client,
            report=UnityCatalogReport(),
        )

        model_version = ModelVersionInfo(
            catalog_name="test_catalog",
            schema_name="test_schema",
            model_name="test_model",
            version=1,
        )

        with patch.object(
            proxy._files_api, "download", side_effect=Exception("File not found")
        ):
            result = proxy._extract_signature_from_files_api(model_version)

        assert result is None
        # Verify warning was reported
        assert len(proxy.report.warnings) > 0

    @patch("datahub.ingestion.source.unity.proxy.WorkspaceClient")
    def test_extract_signature_from_files_api_no_signature(self, mock_workspace_client):
        """Test _extract_signature_from_files_api() handles MLmodel without signature."""
        from io import BytesIO

        from databricks.sdk.service.catalog import ModelVersionInfo

        mock_client = mock_workspace_client.return_value
        mock_client.config.warehouse_id = "test_warehouse"
        proxy = UnityCatalogApiProxy(
            workspace_client=mock_client,
            report=UnityCatalogReport(),
        )

        model_version = ModelVersionInfo(
            catalog_name="test_catalog",
            schema_name="test_schema",
            model_name="test_model",
            version=1,
        )

        # Mock MLmodel YAML content without signature
        mlmodel_yaml = """
artifact_path: model
flavors:
  python_function:
    env: conda.yaml
    loader_module: mlflow.sklearn
    model_path: model.pkl
mlflow_version: 2.0.1
"""

        mock_download_response = MagicMock()
        mock_download_response.contents = BytesIO(mlmodel_yaml.encode("utf-8"))

        with patch.object(
            proxy._files_api, "download", return_value=mock_download_response
        ):
            result = proxy._extract_signature_from_files_api(model_version)

        assert result is None

    @patch("datahub.ingestion.source.unity.proxy.WorkspaceClient")
    def test_extract_signature_with_parameters(self, mock_workspace_client):
        """Test _extract_signature_from_files_api() extracts signature with parameters."""
        from io import BytesIO

        from databricks.sdk.service.catalog import ModelVersionInfo

        mock_client = mock_workspace_client.return_value
        mock_client.config.warehouse_id = "test_warehouse"
        proxy = UnityCatalogApiProxy(
            workspace_client=mock_client,
            report=UnityCatalogReport(),
        )

        model_version = ModelVersionInfo(
            catalog_name="test_catalog",
            schema_name="test_schema",
            model_name="test_model",
            version=1,
        )

        # Mock MLmodel YAML content with parameters
        mlmodel_yaml = """
signature:
  inputs: '[{"name": "text", "type": "string"}]'
  outputs: '[{"name": "label", "type": "string"}]'
  params: '[{"name": "temperature", "type": "float", "default": 0.7}]'
"""

        mock_download_response = MagicMock()
        mock_download_response.contents = BytesIO(mlmodel_yaml.encode("utf-8"))

        with patch.object(
            proxy._files_api, "download", return_value=mock_download_response
        ):
            result = proxy._extract_signature_from_files_api(model_version)

        assert result is not None
        assert result.inputs is not None
        assert result.outputs is not None
        assert result.parameters is not None
        assert len(result.parameters) == 1
        assert result.parameters[0]["name"] == "temperature"

    @patch("datahub.ingestion.source.unity.proxy.WorkspaceClient")
    def test_extract_signature_with_malformed_json(self, mock_workspace_client):
        """Test _extract_signature_from_files_api() handles malformed JSON gracefully."""
        from io import BytesIO

        from databricks.sdk.service.catalog import ModelVersionInfo

        mock_client = mock_workspace_client.return_value
        mock_client.config.warehouse_id = "test_warehouse"
        proxy = UnityCatalogApiProxy(
            workspace_client=mock_client,
            report=UnityCatalogReport(),
        )

        model_version = ModelVersionInfo(
            catalog_name="test_catalog",
            schema_name="test_schema",
            model_name="test_model",
            version=1,
        )

        # Mock MLmodel YAML content with malformed JSON
        mlmodel_yaml = """
signature:
  inputs: '[{"name": "feature1", "type": "double"'
  outputs: '[{"name": "prediction"}]'
"""

        mock_download_response = MagicMock()
        mock_download_response.contents = BytesIO(mlmodel_yaml.encode("utf-8"))

        with patch.object(
            proxy._files_api, "download", return_value=mock_download_response
        ):
            result = proxy._extract_signature_from_files_api(model_version)

        # Should still return a signature object even if some fields failed to parse
        # The method handles JSON decode errors gracefully
        assert result is None or result.inputs is None or result.outputs is None


class TestUnityCatalogProxyAuthentication:
    def test_basic_proxy_auth_header(self):
        proxy_url = "http://user:pass@proxy.example.com:8080"
        auth_info = _basic_proxy_auth_header(proxy_url)

        assert auth_info is not None
        assert auth_info["proxy_url"] == "http://proxy.example.com:8080"
        assert auth_info["auth_string"] == "user:pass"
        assert "proxy-authorization" in auth_info["proxy_headers"]

    def test_basic_proxy_auth_header_no_credentials(self):
        proxy_url = "http://proxy.example.com:8080"
        auth_info = _basic_proxy_auth_header(proxy_url)
        assert auth_info is None

    @patch("datahub.ingestion.source.unity.proxy.connect")
    def test_execute_sql_query_with_proxy(self, mock_connect):
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [("result",)]
        mock_connection.cursor.return_value.__enter__.return_value = mock_cursor
        mock_connect.return_value.__enter__.return_value = mock_connection

        with (
            patch.dict(
                os.environ,
                {"HTTPS_PROXY": "http://user:pass@proxy.com:8080"},
                clear=True,
            ),
        ):
            mock_client = MagicMock()
            mock_client.config.host = "https://test.databricks.com"
            mock_client.config.token = "test-token"
            mock_client.config.warehouse_id = "test-warehouse"

            proxy = UnityCatalogApiProxy(
                workspace_client=mock_client,
                report=UnityCatalogReport(),
            )

            result = proxy._execute_sql_query("SELECT * FROM test_table")
            assert result == [("result",)]
            mock_connect.assert_called_once()

    def test_apply_databricks_proxy_fix(self):
        with patch("datahub.ingestion.source.unity.proxy_patch.logger") as mock_logger:
            from datahub.ingestion.source.unity.proxy_patch import (
                apply_databricks_proxy_fix,
            )

            apply_databricks_proxy_fix()

            log_calls = [call.args[0] for call in mock_logger.info.call_args_list]
            assert any(
                "databricks-sql proxy authentication fix" in msg for msg in log_calls
            )


class TestUnityCatalogProxyUsageSystemTables:
    """Test suite for system tables query history functionality."""

    @pytest.fixture
    def mock_proxy(self):
        """Create a mock UnityCatalogApiProxy for testing."""
        from databricks.sdk import WorkspaceClient

        mock_workspace_client = MagicMock(spec=WorkspaceClient)
        mock_workspace_client.config.host = "https://test.databricks.com"
        mock_workspace_client.config.token = "test_token"
        mock_workspace_client.config.warehouse_id = "test_warehouse"

        proxy = UnityCatalogApiProxy(
            workspace_client=mock_workspace_client,
            report=UnityCatalogReport(),
        )
        return proxy

    @patch(
        "datahub.ingestion.source.unity.proxy.UnityCatalogApiProxy._execute_sql_query"
    )
    def test_get_query_history_via_system_tables_empty(self, mock_execute, mock_proxy):
        """Test get_query_history_via_system_tables with no results."""
        mock_execute.return_value = []
        start_time = datetime(2023, 1, 1)
        end_time = datetime(2023, 1, 31)

        result = list(
            mock_proxy.get_query_history_via_system_tables(start_time, end_time)
        )

        assert len(result) == 0
        mock_execute.assert_called_once()
        call_args = mock_execute.call_args
        query = call_args[0][0]
        assert "system.query.history" in query
        assert "execution_status = 'FINISHED'" in query

    @patch(
        "datahub.ingestion.source.unity.proxy.UnityCatalogApiProxy._execute_sql_query"
    )
    def test_get_query_history_via_system_tables_with_data(
        self, mock_execute, mock_proxy
    ):
        """Test get_query_history_via_system_tables with sample data."""
        from databricks.sql.types import Row

        mock_data = [
            Row(
                statement_id="query_123",
                statement_text="SELECT * FROM test_table",
                statement_type="SELECT",
                start_time=datetime(2023, 1, 1, 10, 0, 0),
                end_time=datetime(2023, 1, 1, 10, 0, 30),
                executed_by="user@example.com",
                executed_as="user@example.com",
                executed_by_user_id=123,
                executed_as_user_id=123,
            ),
            Row(
                statement_id="query_124",
                statement_text="INSERT INTO target_table SELECT * FROM source_table",
                statement_type="INSERT",
                start_time=datetime(2023, 1, 1, 11, 0, 0),
                end_time=datetime(2023, 1, 1, 11, 0, 45),
                executed_by="service@example.com",
                executed_as="service@example.com",
                executed_by_user_id=456,
                executed_as_user_id=456,
            ),
        ]
        mock_execute.return_value = mock_data

        start_time = datetime(2023, 1, 1)
        end_time = datetime(2023, 1, 31)
        result = list(
            mock_proxy.get_query_history_via_system_tables(start_time, end_time)
        )

        assert len(result) == 2
        assert result[0].query_id == "query_123"
        assert result[0].query_text == "SELECT * FROM test_table"
        assert result[0].user_name == "user@example.com"
        assert result[0].user_id == 123
        assert result[1].query_id == "query_124"
        assert "INSERT INTO target_table" in result[1].query_text

    @patch(
        "datahub.ingestion.source.unity.proxy.UnityCatalogApiProxy._execute_sql_query"
    )
    def test_get_query_history_via_system_tables_filters_statement_types(
        self, mock_execute, mock_proxy
    ):
        """Test that query includes proper statement type filtering."""
        mock_execute.return_value = []
        start_time = datetime(2023, 1, 1)
        end_time = datetime(2023, 1, 31)

        list(mock_proxy.get_query_history_via_system_tables(start_time, end_time))

        call_args = mock_execute.call_args
        query = call_args[0][0]
        assert "statement_type IN (" in query
        assert "'SELECT'" in query
        assert "'INSERT'" in query
        assert "'UPDATE'" in query
        assert "'DELETE'" in query
        assert "'MERGE'" in query
        assert "'CREATE'" in query
        assert "'OTHER'" in query

    @patch(
        "datahub.ingestion.source.unity.proxy.UnityCatalogApiProxy._execute_sql_query"
    )
    def test_get_query_history_via_system_tables_handles_null_values(
        self, mock_execute, mock_proxy
    ):
        """Test handling of NULL values in query results."""
        from databricks.sql.types import Row

        mock_data = [
            Row(
                statement_id="query_125",
                statement_text="SELECT 1",
                statement_type=None,
                start_time=datetime(2023, 1, 1, 10, 0, 0),
                end_time=datetime(2023, 1, 1, 10, 0, 5),
                executed_by=None,
                executed_as=None,
                executed_by_user_id=None,
                executed_as_user_id=None,
            )
        ]
        mock_execute.return_value = mock_data

        start_time = datetime(2023, 1, 1)
        end_time = datetime(2023, 1, 31)
        result = list(
            mock_proxy.get_query_history_via_system_tables(start_time, end_time)
        )

        assert len(result) == 1
        assert result[0].statement_type is None
        assert result[0].user_name is None
        assert result[0].user_id is None

    @patch(
        "datahub.ingestion.source.unity.proxy.UnityCatalogApiProxy._execute_sql_query"
    )
    def test_get_query_history_via_system_tables_error_handling(
        self, mock_execute, mock_proxy
    ):
        """Test error handling when SQL query fails."""
        mock_execute.side_effect = Exception("SQL execution failed")

        start_time = datetime(2023, 1, 1)
        end_time = datetime(2023, 1, 31)
        result = list(
            mock_proxy.get_query_history_via_system_tables(start_time, end_time)
        )

        assert len(result) == 0
        assert len(mock_proxy.report.failures) > 0

    @patch(
        "datahub.ingestion.source.unity.proxy.UnityCatalogApiProxy._execute_sql_query"
    )
    def test_get_query_history_via_system_tables_row_parse_error(
        self, mock_execute, mock_proxy
    ):
        """Test error handling when individual row parsing fails."""
        from databricks.sql.types import Row

        mock_data = [
            Row(
                statement_id="query_126",
                statement_text="SELECT * FROM valid_table",
                statement_type="SELECT",
                start_time=datetime(2023, 1, 1, 10, 0, 0),
                end_time=datetime(2023, 1, 1, 10, 0, 30),
                executed_by="user@example.com",
                executed_as="user@example.com",
                executed_by_user_id=123,
                executed_as_user_id=123,
            )
        ]
        mock_execute.return_value = mock_data

        with patch(
            "datahub.ingestion.source.unity.proxy_types.Query.__init__",
            side_effect=Exception("Query creation failed"),
        ):
            start_time = datetime(2023, 1, 1)
            end_time = datetime(2023, 1, 31)
            result = list(
                mock_proxy.get_query_history_via_system_tables(start_time, end_time)
            )

            assert len(result) == 0
            assert len(mock_proxy.report.warnings) > 0

    @patch(
        "datahub.ingestion.source.unity.proxy.UnityCatalogApiProxy._execute_sql_query"
    )
    def test_get_query_history_via_system_tables_time_parameters(
        self, mock_execute, mock_proxy
    ):
        """Test that start and end time are passed correctly as parameters."""
        mock_execute.return_value = []
        start_time = datetime(2023, 5, 15, 8, 30, 0)
        end_time = datetime(2023, 5, 20, 18, 45, 0)

        list(mock_proxy.get_query_history_via_system_tables(start_time, end_time))

        call_args = mock_execute.call_args
        params = call_args[0][1]
        assert params == (start_time, end_time)
