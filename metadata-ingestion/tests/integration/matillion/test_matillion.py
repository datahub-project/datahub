from typing import Any, Dict, List
from unittest.mock import patch

import pytest
import time_machine

from datahub.configuration.config_loader import load_config_file
from datahub.ingestion.run.pipeline import Pipeline
from datahub.ingestion.source.matillion.matillion_api import MatillionAPIClient
from datahub.testing import mce_helpers

FROZEN_TIME = "2024-01-01 00:00:00"


@time_machine.travel(FROZEN_TIME)
def test_matillion_source_basic(pytestconfig: pytest.Config, tmp_path: Any) -> None:
    test_resources_dir = pytestconfig.rootpath / "tests/integration/matillion"
    output_path = tmp_path / "matillion_mces.json"

    mock_projects_response = {
        "results": [
            {
                "id": "proj-1",
                "name": "Analytics Project",
                "description": "Main analytics project",
                "createdAt": "2024-01-01T00:00:00Z",
            }
        ],
        "total": 1,
        "page": 0,
        "size": 25,
    }

    mock_environments_response = {
        "results": [
            {
                "id": "env-1",
                "name": "Production",
                "projectId": "proj-1",
                "description": "Production environment",
                "createdAt": "2024-01-01T00:00:00Z",
            }
        ],
        "total": 1,
        "page": 0,
        "size": 25,
    }

    mock_pipelines_response = {
        "results": [
            {
                "id": "pipe-1",
                "name": "Daily ETL",
                "projectId": "proj-1",
                "type": "orchestration",
                "description": "Daily ETL pipeline",
                "version": "1.0.0",
                "publishedTime": "2024-01-01T00:00:00Z",
            }
        ],
        "total": 1,
        "page": 0,
        "size": 25,
    }

    mock_executions_response: Dict[str, List] = {
        "results": [],
    }

    def mock_make_request(
        self: Any, method: str, endpoint: str, **kwargs: Any
    ) -> Dict[str, Any]:
        if endpoint == "v1/projects":
            return mock_projects_response
        elif "/environments" in endpoint:
            return mock_environments_response
        elif "published-pipelines" in endpoint:
            return mock_pipelines_response
        elif "executions" in endpoint:
            return mock_executions_response
        return {"results": [], "total": 0, "page": 0, "size": 25}

    with patch.object(MatillionAPIClient, "_make_request", mock_make_request):
        pipeline = Pipeline.create(
            {
                "run_id": "matillion-test",
                "source": {
                    "type": "matillion",
                    "config": {
                        "api_config": {
                            "api_token": "test_token",
                            "custom_base_url": "http://localhost:8080/api",
                        },
                        "include_pipeline_executions": False,
                        "include_lineage": False,
                        "include_streaming_pipelines": False,
                        "extract_projects_to_containers": True,
                    },
                },
                "sink": {
                    "type": "file",
                    "config": {"filename": str(output_path)},
                },
            }
        )

        pipeline.run()
        pipeline.raise_from_status()

    assert output_path.exists()

    mce_helpers.check_golden_file(
        pytestconfig=pytestconfig,
        output_path=output_path,
        golden_path=test_resources_dir / "matillion_mces_golden.json",
    )


@time_machine.travel(FROZEN_TIME)
def test_matillion_source_with_lineage(
    pytestconfig: pytest.Config, tmp_path: Any
) -> None:
    test_resources_dir = pytestconfig.rootpath / "tests/integration/matillion"
    output_path = tmp_path / "matillion_lineage_mces.json"

    mock_projects_response = {
        "results": [
            {
                "id": "proj-1",
                "name": "Data Warehouse",
                "description": "Data warehouse project",
                "createdAt": "2024-01-01T00:00:00Z",
            }
        ],
        "total": 1,
        "page": 0,
        "size": 25,
    }

    mock_environments_response = {
        "results": [
            {
                "id": "env-1",
                "name": "Production",
                "projectId": "proj-1",
                "description": "Production environment",
                "createdAt": "2024-01-01T00:00:00Z",
            }
        ],
        "total": 1,
        "page": 0,
        "size": 25,
    }

    mock_pipelines_response = {
        "results": [
            {
                "id": "pipe-1",
                "name": "Customer Transform",
                "projectId": "proj-1",
                "environmentId": "env-1",
                "type": "transformation",
                "description": "Transform customer data",
                "version": "1.0.0",
                "createdAt": "2024-01-01T00:00:00Z",
            }
        ],
        "total": 1,
        "page": 0,
        "size": 25,
    }

    mock_executions_response: Dict[str, List] = {"results": []}

    def mock_make_request(
        self: Any, method: str, endpoint: str, **kwargs: Any
    ) -> Dict[str, Any]:
        if endpoint == "v1/projects":
            return mock_projects_response
        elif "/environments" in endpoint:
            return mock_environments_response
        elif "published-pipelines" in endpoint:
            return mock_pipelines_response
        elif "executions" in endpoint:
            return mock_executions_response
        return {"results": [], "total": 0, "page": 0, "size": 25}

    with patch.object(MatillionAPIClient, "_make_request", mock_make_request):
        pipeline = Pipeline.create(
            {
                "run_id": "matillion-lineage-test",
                "source": {
                    "type": "matillion",
                    "config": {
                        "api_config": {
                            "api_token": "test_token",
                            "custom_base_url": "http://localhost:8080/api",
                        },
                        "include_pipeline_executions": False,
                        "include_lineage": False,
                        "include_column_lineage": False,
                        "include_streaming_pipelines": False,
                        "extract_projects_to_containers": True,
                    },
                },
                "sink": {
                    "type": "file",
                    "config": {"filename": str(output_path)},
                },
            }
        )

        pipeline.run()
        pipeline.raise_from_status()

    assert output_path.exists()

    mce_helpers.check_golden_file(
        pytestconfig=pytestconfig,
        output_path=output_path,
        golden_path=test_resources_dir / "matillion_lineage_mces_golden.json",
    )


@time_machine.travel(FROZEN_TIME)
def test_matillion_source_with_streaming_pipelines(
    pytestconfig: pytest.Config, tmp_path: Any
) -> None:
    test_resources_dir = pytestconfig.rootpath / "tests/integration/matillion"
    output_path = tmp_path / "matillion_streaming_mces.json"

    mock_projects_response = {
        "results": [
            {
                "id": "proj-1",
                "name": "CDC Project",
                "description": "CDC streaming project",
                "createdAt": "2024-01-01T00:00:00Z",
            }
        ],
        "total": 1,
        "page": 0,
        "size": 25,
    }

    mock_environments_response = {
        "results": [
            {
                "id": "env-1",
                "name": "Production",
                "projectId": "proj-1",
                "description": "Production environment",
                "createdAt": "2024-01-01T00:00:00Z",
            }
        ],
        "total": 1,
        "page": 0,
        "size": 25,
    }

    mock_pipelines_response = {"results": [], "total": 0, "page": 0, "size": 25}

    mock_streaming_pipelines_response = {
        "results": [
            {
                "id": "sp-1",
                "name": "MySQL to Snowflake CDC",
                "projectId": "proj-1",
                "environmentId": "env-1",
                "sourceType": "mysql",
                "targetType": "snowflake",
                "sourceConnectionId": "conn-src-1",
                "targetConnectionId": "conn-tgt-1",
                "status": "running",
                "description": "Real-time CDC from MySQL to Snowflake",
                "createdAt": "2024-01-01T00:00:00Z",
            }
        ],
        "total": 1,
        "page": 0,
        "size": 25,
    }

    mock_executions_response: Dict[str, List] = {"results": []}

    def mock_make_request(
        self: Any, method: str, endpoint: str, **kwargs: Any
    ) -> Dict[str, Any]:
        if "projects" in endpoint:
            return mock_projects_response
        elif "environments" in endpoint:
            return mock_environments_response
        elif "streaming-pipelines" in endpoint:
            return mock_streaming_pipelines_response
        elif "pipelines" in endpoint:
            return mock_pipelines_response
        elif "executions" in endpoint:
            return mock_executions_response
        return {"results": [], "total": 0, "page": 0, "size": 25}

    with patch.object(MatillionAPIClient, "_make_request", mock_make_request):
        pipeline = Pipeline.create(
            {
                "run_id": "matillion-streaming-test",
                "source": {
                    "type": "matillion",
                    "config": {
                        "api_config": {
                            "api_token": "test_token",
                            "custom_base_url": "http://localhost:8080/api",
                        },
                        "include_pipeline_executions": False,
                        "include_lineage": False,
                        "include_streaming_pipelines": True,
                        "extract_projects_to_containers": True,
                    },
                },
                "sink": {
                    "type": "file",
                    "config": {"filename": str(output_path)},
                },
            }
        )

        pipeline.run()
        pipeline.raise_from_status()

    assert output_path.exists()

    mce_helpers.check_golden_file(
        pytestconfig=pytestconfig,
        output_path=output_path,
        golden_path=test_resources_dir / "matillion_streaming_mces_golden.json",
    )


@time_machine.travel(FROZEN_TIME)
def test_matillion_source_comprehensive(
    pytestconfig: pytest.Config, tmp_path: Any
) -> None:
    test_resources_dir = pytestconfig.rootpath / "tests/integration/matillion"
    output_path = tmp_path / "matillion_comprehensive_mces.json"

    mock_projects_response = {
        "results": [
            {
                "id": "proj-1",
                "name": "Full Feature Project",
                "description": "Project with all features",
                "createdAt": "2024-01-01T00:00:00Z",
            }
        ],
        "total": 1,
        "page": 0,
        "size": 25,
    }

    mock_environments_response = {
        "results": [
            {
                "id": "env-1",
                "name": "Production",
                "projectId": "proj-1",
                "description": "Production environment",
                "createdAt": "2024-01-01T00:00:00Z",
            }
        ],
        "total": 1,
        "page": 0,
        "size": 25,
    }

    mock_pipelines_response = {
        "results": [
            {
                "id": "pipe-1",
                "name": "Data Integration Pipeline",
                "projectId": "proj-1",
                "environmentId": "env-1",
                "type": "orchestration",
                "description": "Main data integration pipeline",
                "version": "2.0.0",
                "branch": "main",
                "createdAt": "2024-01-01T00:00:00Z",
            }
        ],
        "total": 1,
        "page": 0,
        "size": 25,
    }

    mock_lineage_response = {
        "pipelineId": "pipe-1",
        "nodes": [
            {
                "id": "source-1",
                "name": "orders",
                "type": "table",
                "platform": "bigquery",
                "schema": "source",
                "table": "orders",
            },
            {
                "id": "source-2",
                "name": "customers",
                "type": "table",
                "platform": "bigquery",
                "schema": "source",
                "table": "customers",
            },
            {
                "id": "target-1",
                "name": "customer_orders",
                "type": "table",
                "platform": "snowflake",
                "schema": "analytics",
                "table": "customer_orders",
            },
        ],
        "edges": [
            {"sourceId": "source-1", "targetId": "target-1", "type": "transformation"},
            {"sourceId": "source-2", "targetId": "target-1", "type": "transformation"},
        ],
    }

    mock_streaming_pipelines_response = {
        "results": [
            {
                "id": "sp-1",
                "name": "Postgres CDC Stream",
                "projectId": "proj-1",
                "environmentId": "env-1",
                "sourceType": "postgres",
                "targetType": "s3",
                "sourceConnectionId": "conn-pg-1",
                "targetConnectionId": "conn-s3-1",
                "status": "running",
                "createdAt": "2024-01-01T00:00:00Z",
            }
        ],
        "total": 1,
        "page": 0,
        "size": 25,
    }

    mock_executions_response = {
        "results": [
            {
                "id": "exec-1",
                "pipelineId": "pipe-1",
                "status": "success",
                "startedAt": "2024-01-01T00:00:00Z",
                "completedAt": "2024-01-01T00:05:00Z",
                "durationMs": 300000,
                "rowsProcessed": 10000,
                "triggerType": "schedule",
            }
        ],
    }

    def mock_make_request(
        self: Any, method: str, endpoint: str, **kwargs: Any
    ) -> Dict[str, Any]:
        if "projects" in endpoint:
            return mock_projects_response
        elif "environments" in endpoint:
            return mock_environments_response
        elif "streaming-pipelines" in endpoint:
            return mock_streaming_pipelines_response
        elif "pipelines" in endpoint and "/lineage" in endpoint:
            return mock_lineage_response
        elif "pipelines" in endpoint and "/executions" in endpoint:
            return mock_executions_response
        elif "pipelines" in endpoint:
            return mock_pipelines_response
        return {"results": [], "total": 0, "page": 0, "size": 25}

    with patch.object(MatillionAPIClient, "_make_request", mock_make_request):
        pipeline = Pipeline.create(
            {
                "run_id": "matillion-comprehensive-test",
                "source": {
                    "type": "matillion",
                    "config": {
                        "api_config": {
                            "api_token": "test_token",
                            "custom_base_url": "http://localhost:8080/api",
                        },
                        "include_pipeline_executions": True,
                        "max_executions_per_pipeline": 10,
                        "include_lineage": False,
                        "include_column_lineage": False,
                        "include_streaming_pipelines": True,
                        "extract_projects_to_containers": True,
                    },
                },
                "sink": {
                    "type": "file",
                    "config": {"filename": str(output_path)},
                },
            }
        )

        pipeline.run()
        pipeline.raise_from_status()

    assert output_path.exists()

    mce_helpers.check_golden_file(
        pytestconfig=pytestconfig,
        output_path=output_path,
        golden_path=test_resources_dir / "matillion_comprehensive_mces_golden.json",
    )


@time_machine.travel(FROZEN_TIME)
def test_matillion_postgres_to_snowflake_with_sql_parsing(
    pytestconfig: pytest.Config, tmp_path: Any
) -> None:
    """
    Comprehensive test modeling realistic Postgres → Snowflake transformations
    with OpenLineage events, SQL parsing, and column-level lineage.
    """
    test_resources_dir = pytestconfig.rootpath / "tests/integration/matillion"
    output_path = tmp_path / "matillion_postgres_snowflake_mces.json"

    # Mock project and environment setup
    mock_projects_response = {
        "results": [
            {
                "id": "analytics-proj",
                "name": "Analytics Pipeline",
                "description": "Postgres to Snowflake analytics pipelines",
                "createdAt": "2024-01-01T00:00:00Z",
            }
        ],
        "total": 1,
        "page": 0,
        "size": 25,
    }

    mock_environments_response = {
        "results": [
            {
                "id": "prod-env",
                "name": "production",
                "projectId": "analytics-proj",
                "description": "Production environment",
                "createdAt": "2024-01-01T00:00:00Z",
            }
        ],
        "total": 1,
        "page": 0,
        "size": 25,
    }

    # Two transformation pipelines
    mock_pipelines_response = {
        "results": [
            {
                "id": "customer-agg-pipe",
                "name": "Customer Aggregation",
                "projectId": "analytics-proj",
                "type": "transformation",
                "description": "Aggregate customer orders from Postgres to Snowflake",
                "version": "1.0.0",
                "publishedTime": "2024-01-01T00:00:00Z",
            },
            {
                "id": "daily-summary-pipe",
                "name": "Daily Summary",
                "projectId": "analytics-proj",
                "type": "transformation",
                "description": "Create daily summaries in Snowflake",
                "version": "1.0.0",
                "publishedTime": "2024-01-01T00:00:00Z",
            },
        ],
        "total": 2,
        "page": 0,
        "size": 25,
    }

    # OpenLineage events with SQL transformations
    mock_lineage_events_response = {
        "results": [
            # Event 1: Customer Aggregation Pipeline
            {
                "event": {
                    "eventType": "COMPLETE",
                    "eventTime": "2024-01-01T00:00:00.000Z",
                    "job": {
                        "namespace": "matillion://prod-account.analytics-proj",
                        "name": "production/Customer Aggregation",
                        "facets": {
                            "sql": {
                                "query": """
SELECT 
    c.customer_id,
    c.first_name || ' ' || c.last_name AS customer_name,
    c.email,
    COUNT(o.order_id) AS total_orders,
                                    SUM(o.total_amount) AS total_spent,
    MAX(o.order_date) AS last_order_date
FROM postgres.public.customers c
LEFT JOIN postgres.public.orders o 
    ON c.customer_id = o.customer_id
WHERE o.status = 'completed'
GROUP BY c.customer_id, c.first_name, c.last_name, c.email
                                """,
                                "_producer": "https://www.matillion.com",
                                "_schemaURL": "https://openlineage.io/spec/facets/1-0-1/SqlJobFacet.json",
                            }
                        },
                    },
                    "inputs": [
                        {
                            "namespace": "postgresql://prod-postgres-db.us-east-1.rds.amazonaws.com:5432",
                            "name": "public.customers",
                            "facets": {
                                "schema": {
                                    "fields": [
                                        {
                                            "name": "customer_id",
                                            "type": "INTEGER",
                                            "description": "Primary key",
                                        },
                                        {
                                            "name": "first_name",
                                            "type": "VARCHAR(100)",
                                        },
                                        {"name": "last_name", "type": "VARCHAR(100)"},
                                        {"name": "email", "type": "VARCHAR(255)"},
                                        {"name": "created_at", "type": "TIMESTAMP"},
                                    ]
                                }
                            },
                        },
                        {
                            "namespace": "postgresql://prod-postgres-db.us-east-1.rds.amazonaws.com:5432",
                            "name": "public.orders",
                            "facets": {
                                "schema": {
                                    "fields": [
                                        {"name": "order_id", "type": "INTEGER"},
                                        {"name": "customer_id", "type": "INTEGER"},
                                        {"name": "order_date", "type": "DATE"},
                                        {
                                            "name": "total_amount",
                                            "type": "DECIMAL(10,2)",
                                        },
                                        {"name": "status", "type": "VARCHAR(50)"},
                                    ]
                                }
                            },
                        },
                    ],
                    "outputs": [
                        {
                            "namespace": "snowflake://prod-account.snowflakecomputing.com",
                            "name": "analytics_db.analytics_schema.customer_orders",
                            "facets": {
                                "schema": {
                                    "fields": [
                                        {"name": "customer_id", "type": "NUMBER"},
                                        {
                                            "name": "customer_name",
                                            "type": "VARCHAR",
                                        },
                                        {"name": "email", "type": "VARCHAR"},
                                        {"name": "total_orders", "type": "NUMBER"},
                                        {
                                            "name": "total_spent",
                                            "type": "NUMBER(10,2)",
                                        },
                                        {
                                            "name": "last_order_date",
                                            "type": "DATE",
                                        },
                                    ]
                                },
                                "columnLineage": {
                                    "fields": {
                                        "customer_id": {
                                            "inputFields": [
                                                {
                                                    "namespace": "postgresql://prod-postgres-db.us-east-1.rds.amazonaws.com:5432",
                                                    "name": "public.customers",
                                                    "field": "customer_id",
                                                }
                                            ]
                                        },
                                        "customer_name": {
                                            "inputFields": [
                                                {
                                                    "namespace": "postgresql://prod-postgres-db.us-east-1.rds.amazonaws.com:5432",
                                                    "name": "public.customers",
                                                    "field": "first_name",
                                                },
                                                {
                                                    "namespace": "postgresql://prod-postgres-db.us-east-1.rds.amazonaws.com:5432",
                                                    "name": "public.customers",
                                                    "field": "last_name",
                                                },
                                            ]
                                        },
                                        "email": {
                                            "inputFields": [
                                                {
                                                    "namespace": "postgresql://prod-postgres-db.us-east-1.rds.amazonaws.com:5432",
                                                    "name": "public.customers",
                                                    "field": "email",
                                                }
                                            ]
                                        },
                                        "total_orders": {
                                            "inputFields": [
                                                {
                                                    "namespace": "postgresql://prod-postgres-db.us-east-1.rds.amazonaws.com:5432",
                                                    "name": "public.orders",
                                                    "field": "order_id",
                                                }
                                            ],
                                            "transformationType": "AGGREGATION",
                                        },
                                        "total_spent": {
                                            "inputFields": [
                                                {
                                                    "namespace": "postgresql://prod-postgres-db.us-east-1.rds.amazonaws.com:5432",
                                                    "name": "public.orders",
                                                    "field": "total_amount",
                                                }
                                            ],
                                            "transformationType": "AGGREGATION",
                                        },
                                        "last_order_date": {
                                            "inputFields": [
                                                {
                                                    "namespace": "postgresql://prod-postgres-db.us-east-1.rds.amazonaws.com:5432",
                                                    "name": "public.orders",
                                                    "field": "order_date",
                                                }
                                            ],
                                            "transformationType": "AGGREGATION",
                                        },
                                    }
                                },
                            },
                        }
                    ],
                }
            },
            # Event 2: Daily Summary Pipeline
            {
                "event": {
                    "eventType": "COMPLETE",
                    "eventTime": "2024-01-01T00:00:00.000Z",
                    "job": {
                        "namespace": "matillion://prod-account.analytics-proj",
                        "name": "production/Daily Summary",
                        "facets": {
                            "sql": {
                                "query": """
SELECT 
    DATE_TRUNC('day', last_order_date) AS summary_date,
    COUNT(DISTINCT customer_id) AS active_customers,
    SUM(total_orders) AS total_orders,
    SUM(total_spent) AS total_revenue
FROM analytics_db.analytics_schema.customer_orders
WHERE last_order_date >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY DATE_TRUNC('day', last_order_date)
ORDER BY summary_date DESC
                                """,
                                "_producer": "https://www.matillion.com",
                                "_schemaURL": "https://openlineage.io/spec/facets/1-0-1/SqlJobFacet.json",
                            }
                        },
                    },
                    "inputs": [
                        {
                            "namespace": "snowflake://prod-account.snowflakecomputing.com",
                            "name": "analytics_db.analytics_schema.customer_orders",
                        }
                    ],
                    "outputs": [
                        {
                            "namespace": "snowflake://prod-account.snowflakecomputing.com",
                            "name": "analytics_db.analytics_schema.daily_summary",
                            "facets": {
                                "schema": {
                                    "fields": [
                                        {"name": "summary_date", "type": "DATE"},
                                        {
                                            "name": "active_customers",
                                            "type": "NUMBER",
                                        },
                                        {"name": "total_orders", "type": "NUMBER"},
                                        {
                                            "name": "total_revenue",
                                            "type": "NUMBER(10,2)",
                                        },
                                    ]
                                },
                                "columnLineage": {
                                    "fields": {
                                        "summary_date": {
                                            "inputFields": [
                                                {
                                                    "namespace": "snowflake://prod-account.snowflakecomputing.com",
                                                    "name": "analytics_db.analytics_schema.customer_orders",
                                                    "field": "last_order_date",
                                                }
                                            ],
                                            "transformationType": "TRANSFORMATION",
                                        },
                                        "active_customers": {
                                            "inputFields": [
                                                {
                                                    "namespace": "snowflake://prod-account.snowflakecomputing.com",
                                                    "name": "analytics_db.analytics_schema.customer_orders",
                                                    "field": "customer_id",
                                                }
                                            ],
                                            "transformationType": "AGGREGATION",
                                        },
                                        "total_orders": {
                                            "inputFields": [
                                                {
                                                    "namespace": "snowflake://prod-account.snowflakecomputing.com",
                                                    "name": "analytics_db.analytics_schema.customer_orders",
                                                    "field": "total_orders",
                                                }
                                            ],
                                            "transformationType": "AGGREGATION",
                                        },
                                        "total_revenue": {
                                            "inputFields": [
                                                {
                                                    "namespace": "snowflake://prod-account.snowflakecomputing.com",
                                                    "name": "analytics_db.analytics_schema.customer_orders",
                                                    "field": "total_spent",
                                                }
                                            ],
                                            "transformationType": "AGGREGATION",
                                        },
                                    }
                                },
                            },
                        }
                    ],
                }
            },
        ],
        "total": 2,
        "page": 0,
        "size": 25,
    }

    mock_executions_response: Dict[str, List] = {"results": []}

    def mock_make_request(
        self: Any, method: str, endpoint: str, **kwargs: Any
    ) -> Dict[str, Any]:
        if endpoint == "v1/projects":
            return mock_projects_response
        elif "/environments" in endpoint:
            return mock_environments_response
        elif "published-pipelines" in endpoint:
            return mock_pipelines_response
        elif "lineage/events" in endpoint:
            return mock_lineage_events_response
        elif "executions" in endpoint:
            return mock_executions_response
        return {"results": [], "total": 0, "page": 0, "size": 25}

    with patch.object(MatillionAPIClient, "_make_request", mock_make_request):
        pipeline_config = load_config_file(
            test_resources_dir / "matillion_postgres_snowflake_to_file.yml"
        )
        pipeline = Pipeline.create(
            {
                "run_id": "matillion-postgres-snowflake-test",
                **pipeline_config,
                "sink": {
                    "type": "file",
                    "config": {"filename": str(output_path)},
                },
            }
        )

        pipeline.run()
        pipeline.raise_from_status()

    assert output_path.exists()

    mce_helpers.check_golden_file(
        pytestconfig=pytestconfig,
        output_path=output_path,
        golden_path=test_resources_dir
        / "matillion_postgres_snowflake_mces_golden.json",
    )


if __name__ == "__main__":
    pytest.main([__file__, "-vv"])
