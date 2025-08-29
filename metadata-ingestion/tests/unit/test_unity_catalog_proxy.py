from datetime import datetime
from unittest.mock import patch

import pytest

from datahub.ingestion.source.unity.proxy import (
    ExternalUpstream,
    TableLineageInfo,
    TableUpstream,
    UnityCatalogApiProxy,
)
from datahub.ingestion.source.unity.report import UnityCatalogReport


class TestUnityCatalogProxy:
    @pytest.fixture
    def mock_proxy(self):
        """Create a mock UnityCatalogApiProxy for testing."""
        with patch("datahub.ingestion.source.unity.proxy.WorkspaceClient"):
            proxy = UnityCatalogApiProxy(
                workspace_url="https://test.databricks.com",
                personal_access_token="test_token",
                warehouse_id="test_warehouse",
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
                "source_type": "TABLE",
                "target_table_full_name": "test_catalog.schema.target_table",
                "target_type": "TABLE",
                "last_updated": datetime(2023, 1, 1),
            },
            # External PATH upstream
            {
                "entity_type": "TABLE",
                "entity_id": "path_1",
                "source_table_full_name": "s3://bucket/path/to/file",
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

    def test_constructor_with_databricks_api_page_size(self):
        """Test UnityCatalogApiProxy constructor with databricks_api_page_size parameter."""
        with patch("datahub.ingestion.source.unity.proxy.WorkspaceClient"):
            # Test with default page size (0)
            proxy = UnityCatalogApiProxy(
                workspace_url="https://test.databricks.com",
                personal_access_token="test_token",
                warehouse_id="test_warehouse",
                report=UnityCatalogReport(),
            )
            assert proxy.databricks_api_page_size == 0

            # Test with custom page size
            proxy = UnityCatalogApiProxy(
                workspace_url="https://test.databricks.com",
                personal_access_token="test_token",
                warehouse_id="test_warehouse",
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

        proxy = UnityCatalogApiProxy(
            workspace_url="https://test.databricks.com",
            personal_access_token="test_token",
            warehouse_id="test_warehouse",
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

        proxy = UnityCatalogApiProxy(
            workspace_url="https://test.databricks.com",
            personal_access_token="test_token",
            warehouse_id="test_warehouse",
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

        proxy = UnityCatalogApiProxy(
            workspace_url="https://test.databricks.com",
            personal_access_token="test_token",
            warehouse_id="test_warehouse",
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

        proxy = UnityCatalogApiProxy(
            workspace_url="https://test.databricks.com",
            personal_access_token="test_token",
            warehouse_id="test_warehouse",
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

        proxy = UnityCatalogApiProxy(
            workspace_url="https://test.databricks.com",
            personal_access_token="test_token",
            warehouse_id="test_warehouse",
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

        proxy = UnityCatalogApiProxy(
            workspace_url="https://test.databricks.com",
            personal_access_token="test_token",
            warehouse_id="test_warehouse",
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

    @patch("datahub.ingestion.source.unity.proxy.WorkspaceClient")
    def test_models_with_page_size(self, mock_workspace_client):
        """Test models() method calls registered_models.list() with correct parameters."""
        from datahub.ingestion.source.unity.proxy_types import (
            Catalog,
            Metastore,
            Schema,
        )

        # Setup mock
        mock_client = mock_workspace_client.return_value
        mock_client.registered_models.list.return_value = []

        proxy = UnityCatalogApiProxy(
            workspace_url="https://test.databricks.com",
            personal_access_token="test_token",
            warehouse_id="test_warehouse",
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

        list(proxy.models(schema))

        mock_client.registered_models.list.assert_called_with(
            catalog_name="test_catalog", schema_name="test_schema"
        )

    @patch("datahub.ingestion.source.unity.proxy.WorkspaceClient")
    def test_model_versions_with_page_size(self, mock_workspace_client):
        """Test model_versions() method passes page size to model_versions.list()."""
        from datahub.ingestion.source.unity.proxy_types import Model

        # Setup mock
        mock_client = mock_workspace_client.return_value
        mock_client.model_versions.list.return_value = []

        proxy = UnityCatalogApiProxy(
            workspace_url="https://test.databricks.com",
            personal_access_token="test_token",
            warehouse_id="test_warehouse",
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

        list(proxy.model_versions(model))

        mock_client.model_versions.list.assert_called_with(
            full_name="test_catalog.test_schema.test_model",
            include_browse=True,
            max_results=75,
        )

    @patch("datahub.ingestion.source.unity.proxy.WorkspaceClient")
    def test_model_versions_with_get_call(self, mock_workspace_client):
        """Test model_versions() method calls model_versions.get() for each version to get aliases."""
        from databricks.sdk.service.catalog import ModelVersionInfo

        from datahub.ingestion.source.unity.proxy_types import Model, ModelVersion

        # Setup mock
        mock_client = mock_workspace_client.return_value

        # Mock list response with one version
        mock_version = ModelVersionInfo(version=1, comment="Test version")
        mock_client.model_versions.list.return_value = [mock_version]

        # Mock get response with aliases
        mock_detailed_version = ModelVersionInfo(
            version=1, comment="Test version", aliases=[]
        )
        mock_client.model_versions.get.return_value = mock_detailed_version

        proxy = UnityCatalogApiProxy(
            workspace_url="https://test.databricks.com",
            personal_access_token="test_token",
            warehouse_id="test_warehouse",
            report=UnityCatalogReport(),
            databricks_api_page_size=100,
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

        # Mock the _create_model_version method to return a simple result
        with patch.object(proxy, "_create_model_version") as mock_create:
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
            )

            result = list(proxy.model_versions(model))

            # Verify list was called
            mock_client.model_versions.list.assert_called_with(
                full_name="test_catalog.test_schema.test_model",
                include_browse=True,
                max_results=100,
            )

            # Verify get was called for the version
            mock_client.model_versions.get.assert_called_with(
                "test_catalog.test_schema.test_model", 1, include_aliases=True
            )

            # Should return one model version
            assert len(result) == 1

    def test_dataclass_creation_ml_models(self):
        """Test creation of ML model related dataclasses."""
        from datetime import datetime

        from datahub.ingestion.source.unity.proxy_types import Model, ModelVersion

        # Test Model
        model = Model(
            id="test_catalog.test_schema.test_model",
            name="test_model",
            description="Test model description",
            schema_name="test_schema",
            catalog_name="test_catalog",
            created_at=datetime(2023, 1, 1),
            updated_at=datetime(2023, 1, 2),
        )
        assert model.id == "test_catalog.test_schema.test_model"
        assert model.name == "test_model"
        assert model.description == "Test model description"
        assert model.schema_name == "test_schema"
        assert model.catalog_name == "test_catalog"

        # Test ModelVersion
        version = ModelVersion(
            id="test_catalog.test_schema.test_model_1",
            name="test_model_1",
            model=model,
            version="1",
            aliases=["prod", "latest"],
            description="Version 1",
            created_at=datetime(2023, 1, 3),
            updated_at=datetime(2023, 1, 4),
            created_by="test_user",
        )
        assert version.id == "test_catalog.test_schema.test_model_1"
        assert version.name == "test_model_1"
        assert version.model == model
        assert version.version == "1"
        assert version.aliases == ["prod", "latest"]
        assert version.created_by == "test_user"
