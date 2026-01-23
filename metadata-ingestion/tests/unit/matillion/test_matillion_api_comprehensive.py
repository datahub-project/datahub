import pytest
from pydantic import SecretStr
from requests import HTTPError
from requests_mock import Mocker

from datahub.ingestion.source.matillion.config import MatillionAPIConfig
from datahub.ingestion.source.matillion.matillion_api import MatillionAPIClient
from datahub.ingestion.source.matillion.models import (
    MatillionEnvironment,
    MatillionPipelineExecution,
    MatillionSchedule,
    MatillionStreamingPipeline,
)


@pytest.fixture
def api_config() -> MatillionAPIConfig:
    return MatillionAPIConfig(
        api_token=SecretStr("test_token"),
        custom_base_url="http://test.com",
    )


@pytest.fixture
def api_client(api_config: MatillionAPIConfig) -> MatillionAPIClient:
    return MatillionAPIClient(api_config)


def test_get_environments(
    api_client: MatillionAPIClient, requests_mock: Mocker
) -> None:
    mock_response = {
        "results": [
            {
                "name": "Production",
                "defaultAgentId": "agent-1",
                "defaultAgentName": "Agent 1",
            },
            {
                "name": "Staging",
                "defaultAgentId": "agent-2",
                "defaultAgentName": "Agent 2",
            },
        ],
        "total": 2,
        "page": 0,
        "size": 25,
    }

    requests_mock.get(
        "http://test.com/v1/projects/proj-1/environments?page=0&size=25",
        json=mock_response,
    )

    environments = api_client.get_environments("proj-1")

    assert len(environments) == 2
    assert environments[0].name == "Production"
    assert environments[1].name == "Staging"
    assert isinstance(environments[0], MatillionEnvironment)


def test_get_pipeline_executions(
    api_client: MatillionAPIClient, requests_mock: Mocker
) -> None:
    mock_response = {
        "results": [
            {
                "pipelineExecutionId": "exec-1",
                "pipelineName": "pipe-1",
                "status": "SUCCESS",
                "startedAt": "2024-01-01T00:00:00Z",
                "finishedAt": "2024-01-01T00:05:00Z",
                "environmentName": "Production",
                "projectId": "proj-1",
                "trigger": "SCHEDULE",
            }
        ],
    }

    requests_mock.get(
        "http://test.com/v1/pipeline-executions?pipelineName=pipe-1&limit=10",
        json=mock_response,
    )

    executions = api_client.get_pipeline_executions("pipe-1", 10)

    assert len(executions) == 1
    assert executions[0].pipeline_execution_id == "exec-1"
    assert executions[0].pipeline_name == "pipe-1"
    assert executions[0].status == "SUCCESS"
    assert isinstance(executions[0], MatillionPipelineExecution)


def test_get_schedules(api_client: MatillionAPIClient, requests_mock: Mocker) -> None:
    mock_response = {
        "results": [
            {
                "scheduleId": "sched-1",
                "name": "Daily Schedule",
                "pipelineName": "My Pipeline",
                "cronExpression": "0 0 * * *",
                "scheduleEnabled": True,
            }
        ],
        "total": 1,
        "page": 0,
        "size": 25,
    }

    requests_mock.get(
        "http://test.com/v1/projects/proj-1/schedules?page=0&size=25",
        json=mock_response,
    )

    schedules = api_client.get_schedules(project_id="proj-1")

    assert len(schedules) == 1
    assert schedules[0].schedule_id == "sched-1"
    assert schedules[0].cron_expression == "0 0 * * *"
    assert isinstance(schedules[0], MatillionSchedule)


def test_get_streaming_pipelines(
    api_client: MatillionAPIClient, requests_mock: Mocker
) -> None:
    mock_response = {
        "results": [
            {
                "streamingPipelineId": "sp-1",
                "name": "MySQL CDC",
                "projectId": "proj-1",
                "agentId": "agent-1",
            }
        ],
        "total": 1,
        "page": 0,
        "size": 25,
    }

    requests_mock.get(
        "http://test.com/v1/projects/proj-1/streaming-pipelines?page=0&size=25",
        json=mock_response,
    )

    streaming_pipelines = api_client.get_streaming_pipelines("proj-1")

    assert len(streaming_pipelines) == 1
    assert streaming_pipelines[0].streaming_pipeline_id == "sp-1"
    assert streaming_pipelines[0].name == "MySQL CDC"
    assert isinstance(streaming_pipelines[0], MatillionStreamingPipeline)


def test_pagination_multiple_pages(
    api_client: MatillionAPIClient, requests_mock: Mocker
) -> None:
    mock_page1 = {
        "results": [{"id": "proj-1", "name": "Project 1"}],
        "total": 2,
        "page": 0,
        "size": 1,
    }

    mock_page2 = {
        "results": [{"id": "proj-2", "name": "Project 2"}],
        "total": 2,
        "page": 1,
        "size": 1,
    }

    requests_mock.get(
        "http://test.com/v1/projects?page=0&size=25",
        json=mock_page1,
    )
    requests_mock.get(
        "http://test.com/v1/projects?page=1&size=25",
        json=mock_page2,
    )

    projects = api_client.get_projects()

    assert len(projects) >= 1
    assert projects[0].id == "proj-1"


def test_http_error_handling(
    api_client: MatillionAPIClient, requests_mock: Mocker
) -> None:
    requests_mock.get(
        "http://test.com/v1/projects?page=0&size=25",
        status_code=500,
    )

    with pytest.raises(HTTPError):
        api_client.get_projects()


def test_timeout_configuration(api_config: MatillionAPIConfig) -> None:
    api_config.request_timeout_sec = 60
    client = MatillionAPIClient(api_config)

    assert client.config.request_timeout_sec == 60


def test_empty_pagination_response(
    api_client: MatillionAPIClient, requests_mock: Mocker
) -> None:
    mock_response = {
        "results": [],
        "total": 0,
        "page": 0,
        "size": 25,
    }

    requests_mock.get(
        "http://test.com/v1/projects?page=0&size=25",
        json=mock_response,
    )

    projects = api_client.get_projects()

    assert len(projects) == 0


def test_authorization_header(api_client: MatillionAPIClient) -> None:
    assert "Authorization" in api_client.session.headers
    assert api_client.session.headers["Authorization"] == "Bearer test_token"


def test_content_type_header(api_client: MatillionAPIClient) -> None:
    assert "Content-Type" in api_client.session.headers
    assert api_client.session.headers["Content-Type"] == "application/json"


if __name__ == "__main__":
    pytest.main([__file__, "-vv"])
