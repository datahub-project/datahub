from unittest.mock import MagicMock, patch

from datahub.ingestion.api.source import SourceReport
from datahub.ingestion.source.dataform.dataform_api import DataformAPI
from datahub.ingestion.source.dataform.dataform_config import (
    DataformCloudConfig,
    DataformCoreConfig,
    DataformSourceConfig,
)
from datahub.ingestion.source.dataform.dataform_models import DataformTable


class TestDataformAPI:
    def test_cloud_mode_detection(self):
        """Test cloud mode detection."""
        cloud_config = DataformSourceConfig(
            cloud_config=DataformCloudConfig(
                project_id="test-project",
                repository_id="test-repo",
            ),
            target_platform="bigquery",
        )

        api = DataformAPI(cloud_config, SourceReport())
        assert api.config.is_cloud_mode() is True

    def test_core_mode_detection(self):
        """Test core mode detection."""
        core_config = DataformSourceConfig(
            core_config=DataformCoreConfig(
                project_path="/path/to/project",
            ),
            target_platform="postgres",
        )

        api = DataformAPI(core_config, SourceReport())
        assert api.config.is_core_mode() is True
        # Connection config is internal implementation detail

    @patch(
        "datahub.ingestion.source.dataform.dataform_api.dataform_v1beta1.DataformClient"
    )
    def test_get_client_success(self, mock_client_class):
        """Test successful client creation."""
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client

        config = DataformSourceConfig(
            cloud_config=DataformCloudConfig(
                project_id="test-project",
                repository_id="test-repo",
            ),
            target_platform="bigquery",
        )

        api = DataformAPI(config, SourceReport())

        with patch.object(api, "get_client", return_value=mock_client):
            client = api.get_client()
            assert client == mock_client

    def test_get_client_core_mode_returns_none(self):
        """Test that get_client returns None for core mode."""
        config = DataformSourceConfig(
            core_config=DataformCoreConfig(
                project_path="/path/to/project",
            ),
            target_platform="postgres",
        )

        api = DataformAPI(config, SourceReport())
        client = api.get_client()
        assert client is None

    def test_extract_entities_from_compilation_result(self):
        """Test entity extraction from compilation result."""
        config = DataformSourceConfig(
            cloud_config=DataformCloudConfig(
                project_id="test-project",
                repository_id="test-repo",
            ),
            target_platform="bigquery",
        )

        api = DataformAPI(config, SourceReport())

        # Mock compilation result
        compilation_result = {
            "compiledGraph": {
                "tables": {
                    "analytics.customer_metrics": {
                        "target": {
                            "database": "my-project",
                            "schema": "analytics",
                            "name": "customer_metrics",
                        },
                        "type": "table",
                        "description": "Customer metrics table",
                        "columns": [
                            {
                                "name": "customer_id",
                                "type": "STRING",
                                "description": "Customer identifier",
                            },
                            {
                                "name": "total_orders",
                                "type": "INT64",
                                "description": "Total number of orders",
                            },
                        ],
                        "dependencyTargets": [],
                        "tags": ["production", "analytics"],
                        "fileName": "definitions/analytics/customer_metrics.sqlx",
                    }
                },
                "assertions": {
                    "analytics.customer_metrics_assertion": {
                        "description": "Customer metrics data quality check",
                        "query": "SELECT * FROM ${ref('customer_metrics')} WHERE customer_id IS NULL",
                        "dependencyTargets": ["analytics.customer_metrics"],
                        "tags": ["data-quality"],
                        "fileName": "definitions/assertions/customer_metrics_assertion.sqlx",
                    }
                },
                "operations": {
                    "analytics.refresh_customer_cache": {
                        "description": "Refresh customer cache",
                        "queries": [
                            "DELETE FROM customer_cache",
                            "INSERT INTO customer_cache SELECT * FROM customers",
                        ],
                        "dependencyTargets": ["analytics.customers"],
                        "tags": ["maintenance"],
                        "fileName": "definitions/operations/refresh_customer_cache.sqlx",
                    }
                },
                "declarations": {
                    "raw.customers": {
                        "target": {
                            "database": "my-project",
                            "schema": "raw",
                            "name": "customers",
                        },
                        "description": "Raw customer data",
                        "columns": [
                            {
                                "name": "id",
                                "type": "STRING",
                                "description": "Customer ID",
                            },
                            {
                                "name": "name",
                                "type": "STRING",
                                "description": "Customer name",
                            },
                        ],
                        "tags": ["raw-data"],
                    }
                },
            }
        }

        entities = api.extract_entities(compilation_result)

        # Verify tables
        assert len(entities.tables) == 1
        table = entities.tables[0]
        assert table.name == "customer_metrics"
        assert table.schema == "analytics"
        assert table.database == "my-project"
        assert table.type == "table"
        assert table.description == "Customer metrics table"
        assert len(table.columns) == 2
        assert table.columns[0].name == "customer_id"
        assert table.columns[0].type == "STRING"
        assert "production" in table.tags
        assert "analytics" in table.tags

        # Verify assertions
        assert len(entities.assertions) == 1
        assertion = entities.assertions[0]
        assert assertion.name == "analytics.customer_metrics_assertion"
        assert assertion.description == "Customer metrics data quality check"
        assert "data-quality" in assertion.tags

        # Verify operations
        assert len(entities.operations) == 1
        operation = entities.operations[0]
        assert operation.name == "analytics.refresh_customer_cache"
        assert operation.description == "Refresh customer cache"
        assert "maintenance" in operation.tags

        # Verify declarations
        assert len(entities.declarations) == 1
        declaration = entities.declarations[0]
        assert declaration.name == "customers"
        assert declaration.schema == "raw"
        assert declaration.database == "my-project"
        assert "raw-data" in declaration.tags

    def test_parse_table_with_dependencies(self):
        """Test parsing table with dependencies."""
        config = DataformSourceConfig(
            cloud_config=DataformCloudConfig(
                project_id="test-project",
                repository_id="test-repo",
            ),
            target_platform="bigquery",
        )

        api = DataformAPI(config, SourceReport())

        table_info = {
            "target": {
                "database": "my-project",
                "schema": "analytics",
                "name": "customer_summary",
            },
            "type": "view",
            "description": "Customer summary view",
            "query": "SELECT * FROM ${ref('customers')} JOIN ${ref('orders')} USING (customer_id)",
            "columns": [
                {"name": "customer_id", "type": "STRING", "description": "Customer ID"}
            ],
            "dependencyTargets": [
                {"schema": "raw", "name": "customers"},
                {"schema": "raw", "name": "orders"},
            ],
            "tags": ["analytics", "summary"],
            "fileName": "definitions/analytics/customer_summary.sqlx",
        }

        table = api._parse_table("analytics.customer_summary", table_info)

        assert table is not None
        assert table.name == "customer_summary"
        assert table.schema == "analytics"
        assert table.type == "view"
        assert len(table.dependencies) == 2
        assert "raw.customers" in table.dependencies
        assert "raw.orders" in table.dependencies
        assert "analytics" in table.tags
        assert "summary" in table.tags

    def test_should_include_table_filtering(self):
        """Test table filtering logic."""
        from datahub.configuration.common import AllowDenyPattern

        config = DataformSourceConfig(
            cloud_config=DataformCloudConfig(
                project_id="test-project",
                repository_id="test-repo",
            ),
            target_platform="bigquery",
            table_pattern=AllowDenyPattern(allow=["analytics.*"], deny=[".*_temp"]),
            schema_pattern=AllowDenyPattern(allow=["analytics", "marts"]),
        )

        api = DataformAPI(config, SourceReport())

        # Should include - matches allow pattern and schema
        table1 = DataformTable(
            name="customer_metrics", schema_name="analytics", type="table"
        )
        assert api._should_include_table(table1) is True

        # Should exclude - matches deny pattern
        table2 = DataformTable(
            name="customer_metrics_temp", schema_name="analytics", type="table"
        )
        assert api._should_include_table(table2) is False

        # Should exclude - schema not allowed
        table3 = DataformTable(name="test_table", schema_name="staging", type="table")
        assert api._should_include_table(table3) is False

    @patch("subprocess.run")
    @patch("os.chdir")
    @patch("os.getcwd")
    def test_compile_dataform_project(self, mock_getcwd, mock_chdir, mock_subprocess):
        """Test Dataform project compilation."""
        mock_getcwd.return_value = "/original/path"
        mock_subprocess.return_value = MagicMock(returncode=0)

        config = DataformSourceConfig(
            core_config=DataformCoreConfig(
                project_path="/path/to/dataform/project", target_name="production"
            ),
            target_platform="postgres",
        )

        api = DataformAPI(config, SourceReport())

        with patch.object(
            api, "_load_compiled_definitions", return_value={"compiledGraph": {}}
        ):
            result = api._compile_dataform_project()

            # Verify subprocess was called with correct arguments
            mock_subprocess.assert_called_once()
            call_args = mock_subprocess.call_args[0][0]
            assert call_args == ["dataform", "compile", "--target", "production"]

            # Verify directory changes
            mock_chdir.assert_called_with("/path/to/dataform/project")

            assert result is not None

    def test_parse_assertion_from_compilation_result(self):
        """Test parsing assertion from compilation result."""
        config = DataformSourceConfig(
            cloud_config=DataformCloudConfig(
                project_id="test-project",
                repository_id="test-repo",
            ),
            target_platform="bigquery",
        )

        api = DataformAPI(config, SourceReport())

        assertion_info = {
            "description": "Check for null customer IDs",
            "query": "SELECT COUNT(*) FROM ${ref('customers')} WHERE customer_id IS NULL",
            "dependencyTargets": ["raw.customers"],
            "tags": ["data-quality", "critical"],
            "fileName": "definitions/assertions/null_customer_check.sqlx",
        }

        assertion = api._parse_assertion("null_customer_check", assertion_info)

        assert assertion is not None
        assert assertion.name == "null_customer_check"
        assert assertion.description == "Check for null customer IDs"
        assert (
            assertion.sql_query
            == "SELECT COUNT(*) FROM ${ref('customers')} WHERE customer_id IS NULL"
        )
        assert len(assertion.dependencies) == 1
        assert "raw.customers" in assertion.dependencies
        assert "data-quality" in assertion.tags
        assert "critical" in assertion.tags
        assert assertion.file_path == "definitions/assertions/null_customer_check.sqlx"
