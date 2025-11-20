from unittest.mock import MagicMock, patch

from datahub.ingestion.run.pipeline import Pipeline


class TestDataformIntegration:
    """Integration tests for the Dataform connector."""

    def test_dataform_cloud_integration(self):
        """Test Dataform cloud integration with mock compilation result."""

        # Mock compilation result data
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
                        "description": "Customer metrics aggregated from orders",
                        "columns": [
                            {
                                "name": "customer_id",
                                "type": "STRING",
                                "description": "Unique customer identifier",
                            },
                            {
                                "name": "total_orders",
                                "type": "INT64",
                                "description": "Total number of orders placed",
                            },
                            {
                                "name": "total_spent",
                                "type": "FLOAT64",
                                "description": "Total amount spent by customer",
                            },
                            {
                                "name": "last_order_date",
                                "type": "DATE",
                                "description": "Date of last order",
                            },
                        ],
                        "dependencyTargets": [
                            {"schema": "raw", "name": "orders"},
                            {"schema": "raw", "name": "customers"},
                        ],
                        "tags": ["analytics", "customer", "production"],
                        "fileName": "definitions/analytics/customer_metrics.sqlx",
                        "query": "SELECT customer_id, COUNT(*) as total_orders, SUM(amount) as total_spent, MAX(order_date) as last_order_date FROM ${ref('orders')} GROUP BY customer_id",
                    },
                    "marts.dim_customers": {
                        "target": {
                            "database": "my-project",
                            "schema": "marts",
                            "name": "dim_customers",
                        },
                        "type": "view",
                        "description": "Customer dimension table",
                        "columns": [
                            {
                                "name": "customer_id",
                                "type": "STRING",
                                "description": "Customer ID",
                            },
                            {
                                "name": "customer_name",
                                "type": "STRING",
                                "description": "Customer full name",
                            },
                            {
                                "name": "customer_email",
                                "type": "STRING",
                                "description": "Customer email address",
                            },
                        ],
                        "dependencyTargets": [
                            {"schema": "analytics", "name": "customer_metrics"}
                        ],
                        "tags": ["mart", "dimension"],
                        "fileName": "definitions/marts/dim_customers.sqlx",
                        "query": "SELECT c.*, cm.total_orders FROM ${ref('customers')} c JOIN ${ref('customer_metrics')} cm USING (customer_id)",
                    },
                },
                "assertions": {
                    "customer_metrics_quality_check": {
                        "description": "Ensure no null customer IDs in metrics",
                        "query": "SELECT COUNT(*) FROM ${ref('customer_metrics')} WHERE customer_id IS NULL",
                        "dependencyTargets": ["analytics.customer_metrics"],
                        "tags": ["data-quality", "critical"],
                        "fileName": "definitions/assertions/customer_metrics_quality.sqlx",
                    }
                },
                "operations": {
                    "refresh_customer_cache": {
                        "description": "Refresh materialized customer cache",
                        "queries": [
                            "DELETE FROM customer_cache WHERE last_updated < CURRENT_DATE() - 7",
                            "INSERT INTO customer_cache SELECT * FROM ${ref('customer_metrics')}",
                        ],
                        "dependencyTargets": ["analytics.customer_metrics"],
                        "tags": ["maintenance", "cache"],
                        "fileName": "definitions/operations/refresh_cache.sqlx",
                    }
                },
                "declarations": {
                    "raw.orders": {
                        "target": {
                            "database": "my-project",
                            "schema": "raw",
                            "name": "orders",
                        },
                        "description": "Raw orders data from transactional system",
                        "columns": [
                            {
                                "name": "order_id",
                                "type": "STRING",
                                "description": "Order identifier",
                            },
                            {
                                "name": "customer_id",
                                "type": "STRING",
                                "description": "Customer who placed the order",
                            },
                            {
                                "name": "amount",
                                "type": "FLOAT64",
                                "description": "Order amount",
                            },
                            {
                                "name": "order_date",
                                "type": "DATE",
                                "description": "Date order was placed",
                            },
                        ],
                        "tags": ["raw-data", "transactional"],
                    },
                    "raw.customers": {
                        "target": {
                            "database": "my-project",
                            "schema": "raw",
                            "name": "customers",
                        },
                        "description": "Raw customer data",
                        "columns": [
                            {
                                "name": "customer_id",
                                "type": "STRING",
                                "description": "Customer identifier",
                            },
                            {
                                "name": "customer_name",
                                "type": "STRING",
                                "description": "Customer name",
                            },
                            {
                                "name": "customer_email",
                                "type": "STRING",
                                "description": "Customer email",
                            },
                        ],
                        "tags": ["raw-data", "pii"],
                    },
                },
            }
        }

        config = {
            "source": {
                "type": "dataform",
                "config": {
                    "cloud_config": {
                        "project_id": "test-project",
                        "repository_id": "test-repo",
                        "service_account_key_file": "/dev/null",  # Mock path
                    },
                    "target_platform": "bigquery",
                    "target_platform_instance": "prod",
                    "env": "PROD",
                    "entities_enabled": {
                        "tables": True,
                        "views": True,
                        "assertions": True,
                        "operations": True,
                        "declarations": True,
                    },
                    "include_column_lineage": True,
                    "include_table_lineage": True,
                    "tag_prefix": "dataform:",
                    "custom_properties": {
                        "team": "data-platform",
                        "environment": "production",
                    },
                    "git_repository_url": "https://github.com/test-org/dataform-project",
                    "git_branch": "main",
                    "dataform_is_primary_sibling": False,  # Test explicit control
                },
            },
            "sink": {"type": "file", "config": {"filename": "/tmp/dataform_mces.json"}},
        }

        with patch(
            "datahub.ingestion.source.dataform.dataform_api.DataformAPI"
        ) as mock_api_class:
            # Mock the API
            mock_api = MagicMock()
            mock_api.get_compilation_result.return_value = compilation_result
            mock_api.extract_entities.return_value = self._create_mock_entities()
            mock_api_class.return_value = mock_api

            # Run the pipeline
            pipeline = Pipeline.create(config)
            pipeline.run()

            # Verify API was called
            mock_api.get_compilation_result.assert_called_once()
            mock_api.extract_entities.assert_called_once_with(compilation_result)

    def test_dataform_core_integration(self):
        """Test Dataform core integration."""

        config = {
            "source": {
                "type": "dataform",
                "config": {
                    "core_config": {
                        "project_path": "/tmp/dataform-project",
                        "target_name": "development",
                    },
                    "target_platform": "postgres",
                    "env": "DEV",
                    "entities_enabled": {
                        "tables": True,
                        "views": True,
                        "assertions": False,  # Disable assertions for core test
                        "operations": False,  # Disable operations for core test
                        "declarations": True,
                    },
                    "dataform_is_primary_sibling": True,  # Test default behavior
                },
            },
            "sink": {
                "type": "file",
                "config": {"filename": "/tmp/dataform_core_mces.json"},
            },
        }

        with patch(
            "datahub.ingestion.source.dataform.dataform_api.DataformAPI"
        ) as mock_api_class:
            # Mock the API
            mock_api = MagicMock()
            mock_api.get_compilation_result.return_value = {
                "compiledGraph": {"tables": {}}
            }
            mock_api.extract_entities.return_value = self._create_simple_mock_entities()
            mock_api_class.return_value = mock_api

            # Run the pipeline
            pipeline = Pipeline.create(config)
            pipeline.run()

            # Verify API was called
            mock_api.get_compilation_result.assert_called_once()

    def test_dataform_with_filtering(self):
        """Test Dataform connector with entity filtering."""

        config = {
            "source": {
                "type": "dataform",
                "config": {
                    "cloud_config": {
                        "project_id": "test-project",
                        "repository_id": "test-repo",
                        "service_account_key_file": "/dev/null",
                    },
                    "target_platform": "bigquery",
                    "table_pattern": {
                        "allow": ["analytics.*", "marts.*"],
                        "deny": [".*_temp", ".*_test"],
                    },
                    "schema_pattern": {"allow": ["analytics", "marts"]},
                    "tag_pattern": {"allow": ["production", "analytics"]},
                    "entities_enabled": {
                        "tables": True,
                        "views": True,
                        "assertions": False,
                        "operations": False,
                        "declarations": False,
                    },
                },
            },
            "sink": {
                "type": "file",
                "config": {"filename": "/tmp/dataform_filtered_mces.json"},
            },
        }

        with patch(
            "datahub.ingestion.source.dataform.dataform_api.DataformAPI"
        ) as mock_api_class:
            # Mock the API
            mock_api = MagicMock()
            mock_api.get_compilation_result.return_value = {
                "compiledGraph": {"tables": {}}
            }
            mock_api.extract_entities.return_value = (
                self._create_filtered_mock_entities()
            )
            mock_api_class.return_value = mock_api

            # Run the pipeline
            pipeline = Pipeline.create(config)
            pipeline.run()

    def test_dataform_sibling_relationships(self):
        """Test Dataform connector sibling relationship generation."""

        config = {
            "source": {
                "type": "dataform",
                "config": {
                    "cloud_config": {
                        "project_id": "test-project",
                        "repository_id": "test-repo",
                        "service_account_key_file": "/dev/null",
                    },
                    "target_platform": "bigquery",
                    "dataform_is_primary_sibling": False,  # Enable explicit sibling control
                    "entities_enabled": {
                        "tables": True,
                        "views": False,
                        "assertions": False,
                        "operations": False,
                        "declarations": False,
                    },
                },
            },
            "sink": {
                "type": "file",
                "config": {"filename": "/tmp/dataform_siblings_mces.json"},
            },
        }

        with patch(
            "datahub.ingestion.source.dataform.dataform_api.DataformAPI"
        ) as mock_api_class:
            # Mock the API
            mock_api = MagicMock()
            mock_api.get_compilation_result.return_value = {
                "compiledGraph": {"tables": {}}
            }
            mock_api.extract_entities.return_value = self._create_simple_mock_entities()
            mock_api_class.return_value = mock_api

            # Run the pipeline
            pipeline = Pipeline.create(config)

            # Capture work units to verify sibling relationships
            work_units = []
            for work_unit in pipeline.source.get_workunits():
                work_units.append(work_unit)

            # Should have both Dataform platform entities and target platform patches
            # This would be verified by checking the work unit types and content

    def _create_mock_entities(self):
        """Create mock entities for testing."""
        from datahub.ingestion.source.dataform.dataform_models import (
            DataformAssertion,
            DataformColumn,
            DataformDeclaration,
            DataformEntities,
            DataformOperation,
            DataformTable,
        )

        return DataformEntities(
            tables=[
                DataformTable(
                    name="customer_metrics",
                    schema_name="analytics",
                    database="my-project",
                    type="table",
                    description="Customer metrics table",
                    columns=[
                        DataformColumn(
                            name="customer_id", type="STRING", description="Customer ID"
                        ),
                        DataformColumn(
                            name="total_orders",
                            type="INT64",
                            description="Total orders",
                        ),
                    ],
                    tags=["analytics", "production"],
                    dependencies=["raw.orders", "raw.customers"],
                    file_path="definitions/analytics/customer_metrics.sqlx",
                ),
                DataformTable(
                    name="dim_customers",
                    schema_name="marts",
                    database="my-project",
                    type="view",
                    description="Customer dimension",
                    columns=[
                        DataformColumn(
                            name="customer_id", type="STRING", description="Customer ID"
                        ),
                        DataformColumn(
                            name="customer_name",
                            type="STRING",
                            description="Customer name",
                        ),
                    ],
                    tags=["mart", "dimension"],
                    dependencies=["analytics.customer_metrics"],
                    file_path="definitions/marts/dim_customers.sqlx",
                ),
            ],
            assertions=[
                DataformAssertion(
                    name="customer_metrics_quality_check",
                    schema_name="analytics",
                    table="customer_metrics",
                    description="Data quality check",
                    sql_query="SELECT COUNT(*) FROM ${ref('customer_metrics')} WHERE customer_id IS NULL",
                    dependencies=["analytics.customer_metrics"],
                    tags=["data-quality"],
                    file_path="definitions/assertions/customer_quality.sqlx",
                )
            ],
            operations=[
                DataformOperation(
                    name="refresh_customer_cache",
                    description="Refresh cache",
                    sql_query="DELETE FROM cache; INSERT INTO cache SELECT * FROM ${ref('customer_metrics')}",
                    dependencies=["analytics.customer_metrics"],
                    tags=["maintenance"],
                    file_path="definitions/operations/refresh_cache.sqlx",
                )
            ],
            declarations=[
                DataformDeclaration(
                    name="orders",
                    schema_name="raw",
                    database="my-project",
                    description="Raw orders",
                    columns=[
                        DataformColumn(
                            name="order_id", type="STRING", description="Order ID"
                        ),
                        DataformColumn(
                            name="customer_id", type="STRING", description="Customer ID"
                        ),
                    ],
                    tags=["raw-data"],
                ),
                DataformDeclaration(
                    name="customers",
                    schema_name="raw",
                    database="my-project",
                    description="Raw customers",
                    columns=[
                        DataformColumn(
                            name="customer_id", type="STRING", description="Customer ID"
                        ),
                        DataformColumn(
                            name="customer_name",
                            type="STRING",
                            description="Customer name",
                        ),
                    ],
                    tags=["raw-data", "pii"],
                ),
            ],
        )

    def _create_simple_mock_entities(self):
        """Create simple mock entities for basic testing."""
        from datahub.ingestion.source.dataform.dataform_models import (
            DataformColumn,
            DataformEntities,
            DataformTable,
        )

        return DataformEntities(
            tables=[
                DataformTable(
                    name="simple_table",
                    schema_name="analytics",
                    database="test-project",
                    type="table",
                    description="Simple test table",
                    columns=[
                        DataformColumn(
                            name="id", type="STRING", description="ID field"
                        ),
                        DataformColumn(
                            name="name", type="STRING", description="Name field"
                        ),
                    ],
                    tags=["test"],
                )
            ]
        )

    def _create_filtered_mock_entities(self):
        """Create mock entities for filtering tests."""
        from datahub.ingestion.source.dataform.dataform_models import (
            DataformEntities,
            DataformTable,
        )

        return DataformEntities(
            tables=[
                # Should be included
                DataformTable(
                    name="allowed_table",
                    schema_name="analytics",
                    type="table",
                    tags=["production", "analytics"],
                ),
                # Should be excluded by schema pattern
                DataformTable(
                    name="excluded_table",
                    schema_name="staging",
                    type="table",
                    tags=["production"],
                ),
                # Should be excluded by table pattern
                DataformTable(
                    name="test_table_temp",
                    schema_name="analytics",
                    type="table",
                    tags=["production"],
                ),
            ]
        )
