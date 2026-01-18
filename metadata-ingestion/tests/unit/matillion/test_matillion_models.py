from datetime import datetime

import pytest
from pydantic import ValidationError

from datahub.ingestion.source.matillion.models import (
    MatillionConnection,
    MatillionEnvironment,
    MatillionPipeline,
    MatillionPipelineExecution,
    MatillionProject,
)


def test_project_model():
    data = {
        "id": "proj-123",
        "name": "Test Project",
        "description": "A test project",
        "createdAt": "2024-01-01T00:00:00Z",
    }
    project = MatillionProject.model_validate(data)
    assert project.id == "proj-123"
    assert project.name == "Test Project"
    assert project.description == "A test project"
    assert isinstance(project.created_at, datetime)


def test_project_model_minimal():
    data = {"id": "proj-123", "name": "Test Project"}
    project = MatillionProject.model_validate(data)
    assert project.id == "proj-123"
    assert project.name == "Test Project"
    assert project.description is None


def test_environment_model():
    data = {
        "id": "env-456",
        "name": "Production",
        "projectId": "proj-123",
        "description": "Production environment",
    }
    env = MatillionEnvironment.model_validate(data)
    assert env.id == "env-456"
    assert env.name == "Production"
    assert env.project_id == "proj-123"
    assert env.description == "Production environment"


def test_pipeline_model():
    data = {
        "id": "pipe-789",
        "name": "ETL Pipeline",
        "projectId": "proj-123",
        "environmentId": "env-456",
        "type": "orchestration",
        "version": "1.0.0",
        "branch": "main",
    }
    pipeline = MatillionPipeline.model_validate(data)
    assert pipeline.id == "pipe-789"
    assert pipeline.name == "ETL Pipeline"
    assert pipeline.project_id == "proj-123"
    assert pipeline.environment_id == "env-456"
    assert pipeline.pipeline_type == "orchestration"
    assert pipeline.version == "1.0.0"
    assert pipeline.branch == "main"


def test_pipeline_execution_model():
    data = {
        "id": "exec-001",
        "pipelineId": "pipe-789",
        "status": "success",
        "startedAt": "2024-01-01T10:00:00Z",
        "completedAt": "2024-01-01T10:05:00Z",
        "durationMs": 300000,
        "triggeredBy": "user@example.com",
        "triggerType": "manual",
    }
    execution = MatillionPipelineExecution.model_validate(data)
    assert execution.id == "exec-001"
    assert execution.pipeline_id == "pipe-789"
    assert execution.status == "success"
    assert isinstance(execution.started_at, datetime)
    assert isinstance(execution.completed_at, datetime)
    assert execution.duration_ms == 300000


def test_connection_model():
    data = {
        "id": "conn-111",
        "name": "Snowflake Connection",
        "type": "snowflake",
        "projectId": "proj-123",
        "environmentId": "env-456",
    }
    connection = MatillionConnection.model_validate(data)
    assert connection.id == "conn-111"
    assert connection.name == "Snowflake Connection"
    assert connection.connection_type == "snowflake"
    assert connection.project_id == "proj-123"


def test_model_with_invalid_data():
    data = {"invalid": "data"}
    with pytest.raises(ValidationError):
        MatillionProject.model_validate(data)
