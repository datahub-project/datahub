import pytest
from pydantic import SecretStr
from requests import HTTPError
from requests_mock import Mocker

from datahub.ingestion.source.matillion_dpc.config import (
    MatillionAPIConfig,
    MatillionRegion,
)
from datahub.ingestion.source.matillion_dpc.matillion_api import MatillionAPIClient
from datahub.ingestion.source.matillion_dpc.models import (
    MatillionEnvironment,
    MatillionPipeline,
    MatillionPipelineExecution,
    MatillionProject,
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
def api_config_with_region() -> MatillionAPIConfig:
    return MatillionAPIConfig(
        api_token=SecretStr("test_token"),
        region=MatillionRegion.EU1,
    )


@pytest.fixture
def api_client(api_config: MatillionAPIConfig) -> MatillionAPIClient:
    return MatillionAPIClient(api_config)


@pytest.fixture
def api_client_with_region(
    api_config_with_region: MatillionAPIConfig,
) -> MatillionAPIClient:
    return MatillionAPIClient(api_config_with_region)


def test_api_client_initialization(
    api_client_with_region: MatillionAPIClient,
) -> None:
    assert api_client_with_region.config.region == MatillionRegion.EU1
    assert (
        api_client_with_region.config.get_base_url()
        == "https://eu1.api.matillion.com/dpc"
    )
    assert api_client_with_region.config.api_token.get_secret_value() == "test_token"


@pytest.mark.parametrize(
    "method_name,endpoint_path,url_params,mock_result_field,expected_type",
    [
        pytest.param(
            "get_projects",
            "/v1/projects",
            "",
            {
                "id": "proj-1",
                "name": "Test Project",
                "description": "A test project",
            },
            MatillionProject,
            id="get_projects",
        ),
        pytest.param(
            "get_pipelines",
            "/v1/projects/proj-1/published-pipelines",
            "environmentName=Production&",
            {
                "name": "Test Pipeline",
                "publishedTime": "2024-01-01T00:00:00Z",
            },
            MatillionPipeline,
            id="get_pipelines",
        ),
        pytest.param(
            "get_environments",
            "/v1/projects/proj-1/environments",
            "",
            {
                "name": "Production",
                "defaultAgentId": "agent-1",
                "defaultAgentName": "Agent 1",
            },
            MatillionEnvironment,
            id="get_environments",
        ),
        pytest.param(
            "get_schedules",
            "/v1/projects/proj-1/schedules",
            "",
            {
                "scheduleId": "sched-1",
                "name": "Daily Schedule",
                "pipelineName": "My Pipeline",
                "cronExpression": "0 0 * * *",
                "scheduleEnabled": True,
            },
            MatillionSchedule,
            id="get_schedules",
        ),
        pytest.param(
            "get_streaming_pipelines",
            "/v1/projects/proj-1/streaming-pipelines",
            "",
            {
                "streamingPipelineId": "sp-1",
                "name": "MySQL CDC",
                "projectId": "proj-1",
                "agentId": "agent-1",
            },
            MatillionStreamingPipeline,
            id="get_streaming_pipelines",
        ),
    ],
)
def test_api_client_get_methods(
    api_client: MatillionAPIClient,
    requests_mock: Mocker,
    method_name: str,
    endpoint_path: str,
    url_params: str,
    mock_result_field: dict,
    expected_type: type,
) -> None:
    mock_response = {
        "results": [mock_result_field],
        "total": 1,
        "page": 0,
        "size": 25,
    }

    requests_mock.get(
        f"http://test.com{endpoint_path}?{url_params}page=0&size=25",
        json=mock_response,
    )

    method = getattr(api_client, method_name)
    if method_name == "get_pipelines":
        results = method("proj-1", "Production")
    elif method_name in [
        "get_environments",
        "get_schedules",
        "get_streaming_pipelines",
    ]:
        results = method("proj-1")
    else:
        results = method()

    assert len(results) == 1
    assert isinstance(results[0], expected_type)


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


@pytest.mark.parametrize(
    "status_code",
    [
        pytest.param(400, id="bad_request"),
        pytest.param(401, id="unauthorized"),
        pytest.param(403, id="forbidden"),
        pytest.param(404, id="not_found"),
        pytest.param(500, id="server_error"),
        pytest.param(503, id="service_unavailable"),
    ],
)
def test_http_error_handling(
    api_client: MatillionAPIClient, requests_mock: Mocker, status_code: int
) -> None:
    requests_mock.get(
        "http://test.com/v1/projects?page=0&size=25",
        status_code=status_code,
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


@pytest.mark.parametrize(
    "header_name,expected_value",
    [
        pytest.param("Authorization", "Bearer test_token", id="authorization"),
        pytest.param("Content-Type", "application/json", id="content_type"),
    ],
)
def test_session_headers(
    api_client: MatillionAPIClient, header_name: str, expected_value: str
) -> None:
    assert header_name in api_client.session.headers
    assert api_client.session.headers[header_name] == expected_value
