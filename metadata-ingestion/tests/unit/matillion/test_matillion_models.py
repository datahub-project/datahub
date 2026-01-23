from datetime import datetime

import pytest
from pydantic import ValidationError

from datahub.ingestion.source.matillion.models import (
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
    }
    project = MatillionProject.model_validate(data)
    assert project.id == "proj-123"
    assert project.name == "Test Project"
    assert project.description == "A test project"


def test_project_model_minimal():
    data = {"id": "proj-123", "name": "Test Project"}
    project = MatillionProject.model_validate(data)
    assert project.id == "proj-123"
    assert project.name == "Test Project"
    assert project.description is None


def test_environment_model():
    data = {
        "name": "Production",
        "defaultAgentId": "agent-123",
        "defaultAgentName": "Agent 123",
    }
    env = MatillionEnvironment.model_validate(data)
    assert env.name == "Production"
    assert env.default_agent_id == "agent-123"
    assert env.default_agent_name == "Agent 123"


def test_pipeline_model():
    data = {
        "name": "ETL Pipeline",
        "publishedTime": "2024-01-01T00:00:00Z",
    }
    pipeline = MatillionPipeline.model_validate(data)
    assert pipeline.name == "ETL Pipeline"
    assert isinstance(pipeline.published_time, datetime)


def test_pipeline_execution_model():
    data = {
        "pipelineExecutionId": "exec-001",
        "pipelineName": "My Pipeline",
        "status": "SUCCESS",
        "startedAt": "2024-01-01T10:00:00Z",
        "finishedAt": "2024-01-01T10:05:00Z",
        "environmentName": "Production",
        "projectId": "proj-123",
        "trigger": "SCHEDULE",
    }
    execution = MatillionPipelineExecution.model_validate(data)
    assert execution.pipeline_execution_id == "exec-001"
    assert execution.pipeline_name == "My Pipeline"
    assert execution.status == "SUCCESS"
    assert isinstance(execution.started_at, datetime)
    assert isinstance(execution.finished_at, datetime)
    assert execution.environment_name == "Production"


def test_model_with_invalid_data():
    data = {"invalid": "data"}
    with pytest.raises(ValidationError):
        MatillionProject.model_validate(data)
