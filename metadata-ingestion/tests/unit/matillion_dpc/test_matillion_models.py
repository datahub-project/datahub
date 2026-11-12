from datetime import datetime
from typing import Any

import pytest
from pydantic import ValidationError

from datahub.ingestion.source.matillion_dpc.models import (
    MatillionEnvironment,
    MatillionPipeline,
    MatillionPipelineExecution,
    MatillionProject,
)


@pytest.mark.parametrize(
    "model_class,data,expected_attrs",
    [
        pytest.param(
            MatillionProject,
            {"id": "proj-123", "name": "Test Project", "description": "A test project"},
            {"id": "proj-123", "name": "Test Project", "description": "A test project"},
            id="project_full",
        ),
        pytest.param(
            MatillionProject,
            {"id": "proj-123", "name": "Test Project"},
            {"id": "proj-123", "name": "Test Project", "description": None},
            id="project_minimal",
        ),
        pytest.param(
            MatillionEnvironment,
            {
                "name": "Production",
                "defaultAgentId": "agent-123",
                "defaultAgentName": "Agent 123",
            },
            {
                "name": "Production",
                "default_agent_id": "agent-123",
                "default_agent_name": "Agent 123",
            },
            id="environment",
        ),
        pytest.param(
            MatillionPipeline,
            {"name": "ETL Pipeline", "publishedTime": "2024-01-01T00:00:00Z"},
            {"name": "ETL Pipeline"},
            id="pipeline",
        ),
    ],
)
def test_model_validation(
    model_class: type[Any], data: dict, expected_attrs: dict
) -> None:
    instance = model_class.model_validate(data)

    for attr, expected_value in expected_attrs.items():
        actual_value = getattr(instance, attr)
        if isinstance(actual_value, datetime):
            assert isinstance(actual_value, datetime)
        else:
            assert actual_value == expected_value


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


@pytest.mark.parametrize(
    "invalid_data",
    [
        pytest.param({"invalid": "data"}, id="completely_invalid"),
        pytest.param({}, id="empty_dict"),
        pytest.param({"id": "proj-123"}, id="missing_required_name"),
    ],
)
def test_model_validation_errors(invalid_data: dict) -> None:
    with pytest.raises(ValidationError):
        MatillionProject.model_validate(invalid_data)
