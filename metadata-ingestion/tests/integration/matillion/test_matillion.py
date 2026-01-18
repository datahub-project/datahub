from typing import Any, Dict
from unittest.mock import patch

import pytest
import time_machine

from datahub.ingestion.run.pipeline import Pipeline
from datahub.ingestion.source.matillion.matillion_api import MatillionAPIClient
from datahub.testing import mce_helpers

FROZEN_TIME = "2024-01-01 00:00:00"


@time_machine.travel(FROZEN_TIME)
def test_matillion_source_basic(pytestconfig: pytest.Config, tmp_path: Any) -> None:
    test_resources_dir = pytestconfig.rootpath / "tests/integration/matillion"
    output_path = tmp_path / "matillion_mces.json"

    mock_projects_response = {
        "content": [
            {
                "id": "proj-1",
                "name": "Analytics Project",
                "description": "Main analytics project",
                "createdAt": "2024-01-01T00:00:00Z",
            }
        ],
        "totalPages": 1,
        "totalElements": 1,
        "number": 0,
        "size": 25,
    }

    mock_environments_response = {
        "content": [
            {
                "id": "env-1",
                "name": "Production",
                "projectId": "proj-1",
                "description": "Production environment",
                "createdAt": "2024-01-01T00:00:00Z",
            }
        ],
        "totalPages": 1,
        "totalElements": 1,
        "number": 0,
        "size": 25,
    }

    mock_pipelines_response = {
        "content": [
            {
                "id": "pipe-1",
                "name": "Daily ETL",
                "projectId": "proj-1",
                "environmentId": "env-1",
                "type": "orchestration",
                "description": "Daily ETL pipeline",
                "version": "1.0.0",
                "createdAt": "2024-01-01T00:00:00Z",
            }
        ],
        "totalPages": 1,
        "totalElements": 1,
        "number": 0,
        "size": 25,
    }

    mock_executions_response = {
        "content": [],
        "totalPages": 0,
        "totalElements": 0,
        "number": 0,
        "size": 25,
    }

    def mock_make_request(
        self: Any, method: str, endpoint: str, **kwargs: Any
    ) -> Dict[str, Any]:
        if "projects" in endpoint:
            return mock_projects_response
        elif "environments" in endpoint:
            return mock_environments_response
        elif "pipelines" in endpoint and "/executions" not in endpoint:
            return mock_pipelines_response
        elif "executions" in endpoint:
            return mock_executions_response
        return {"content": [], "totalPages": 0}

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
        "content": [
            {
                "id": "proj-1",
                "name": "Data Warehouse",
                "description": "Data warehouse project",
                "createdAt": "2024-01-01T00:00:00Z",
            }
        ],
        "totalPages": 1,
        "totalElements": 1,
        "number": 0,
        "size": 25,
    }

    mock_environments_response = {
        "content": [
            {
                "id": "env-1",
                "name": "Production",
                "projectId": "proj-1",
                "description": "Production environment",
                "createdAt": "2024-01-01T00:00:00Z",
            }
        ],
        "totalPages": 1,
        "totalElements": 1,
        "number": 0,
        "size": 25,
    }

    mock_pipelines_response = {
        "content": [
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
        "totalPages": 1,
        "totalElements": 1,
        "number": 0,
        "size": 25,
    }

    mock_lineage_response = {
        "pipelineId": "pipe-1",
        "nodes": [
            {
                "id": "source-1",
                "name": "raw_customers",
                "type": "table",
                "platform": "snowflake",
                "schema": "raw",
                "table": "customers",
            },
            {
                "id": "transform-1",
                "name": "transformation",
                "type": "transform",
            },
            {
                "id": "target-1",
                "name": "dim_customers",
                "type": "table",
                "platform": "snowflake",
                "schema": "curated",
                "table": "dim_customers",
            },
        ],
        "edges": [
            {"sourceId": "source-1", "targetId": "transform-1", "type": "read"},
            {"sourceId": "transform-1", "targetId": "target-1", "type": "write"},
        ],
    }

    mock_executions_response = {"content": [], "totalPages": 0}

    def mock_make_request(
        self: Any, method: str, endpoint: str, **kwargs: Any
    ) -> Dict[str, Any]:
        if "projects" in endpoint:
            return mock_projects_response
        elif "environments" in endpoint:
            return mock_environments_response
        elif "pipelines" in endpoint and "/lineage" in endpoint:
            return mock_lineage_response
        elif "pipelines" in endpoint and "/executions" not in endpoint:
            return mock_pipelines_response
        elif "executions" in endpoint:
            return mock_executions_response
        return {"content": [], "totalPages": 0}

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
                        "include_lineage": True,
                        "include_column_lineage": True,
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
        "content": [
            {
                "id": "proj-1",
                "name": "CDC Project",
                "description": "CDC streaming project",
                "createdAt": "2024-01-01T00:00:00Z",
            }
        ],
        "totalPages": 1,
        "totalElements": 1,
        "number": 0,
        "size": 25,
    }

    mock_environments_response = {
        "content": [
            {
                "id": "env-1",
                "name": "Production",
                "projectId": "proj-1",
                "description": "Production environment",
                "createdAt": "2024-01-01T00:00:00Z",
            }
        ],
        "totalPages": 1,
        "totalElements": 1,
        "number": 0,
        "size": 25,
    }

    mock_pipelines_response = {"content": [], "totalPages": 0}

    mock_streaming_pipelines_response = {
        "content": [
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
        "totalPages": 1,
        "totalElements": 1,
        "number": 0,
        "size": 25,
    }

    mock_executions_response = {"content": [], "totalPages": 0}

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
        return {"content": [], "totalPages": 0}

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
        "content": [
            {
                "id": "proj-1",
                "name": "Full Feature Project",
                "description": "Project with all features",
                "createdAt": "2024-01-01T00:00:00Z",
            }
        ],
        "totalPages": 1,
        "totalElements": 1,
        "number": 0,
        "size": 25,
    }

    mock_environments_response = {
        "content": [
            {
                "id": "env-1",
                "name": "Production",
                "projectId": "proj-1",
                "description": "Production environment",
                "createdAt": "2024-01-01T00:00:00Z",
            }
        ],
        "totalPages": 1,
        "totalElements": 1,
        "number": 0,
        "size": 25,
    }

    mock_pipelines_response = {
        "content": [
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
        "totalPages": 1,
        "totalElements": 1,
        "number": 0,
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
        "content": [
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
        "totalPages": 1,
        "totalElements": 1,
        "number": 0,
        "size": 25,
    }

    mock_executions_response = {
        "content": [
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
        "totalPages": 1,
        "totalElements": 1,
        "number": 0,
        "size": 10,
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
        return {"content": [], "totalPages": 0}

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
                        "include_lineage": True,
                        "include_column_lineage": True,
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


if __name__ == "__main__":
    pytest.main([__file__, "-vv"])
