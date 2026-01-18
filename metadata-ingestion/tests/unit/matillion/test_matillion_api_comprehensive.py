import pytest
from pydantic import SecretStr
from requests import HTTPError
from requests_mock import Mocker

from datahub.ingestion.source.matillion.config import MatillionAPIConfig
from datahub.ingestion.source.matillion.matillion_api import MatillionAPIClient
from datahub.ingestion.source.matillion.models import (
    MatillionAgent,
    MatillionConnection,
    MatillionEnvironment,
    MatillionLineageGraph,
    MatillionPipelineExecution,
    MatillionRepository,
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
        "content": [
            {
                "id": "env-1",
                "name": "Production",
                "projectId": "proj-1",
                "description": "Production environment",
            },
            {
                "id": "env-2",
                "name": "Staging",
                "projectId": "proj-1",
            },
        ],
        "totalElements": 2,
        "number": 0,
        "size": 25,
        "totalPages": 1,
    }

    requests_mock.get(
        "http://test.com/v1/environments?projectId=proj-1&page=0&size=25",
        json=mock_response,
    )

    environments = api_client.get_environments("proj-1")

    assert len(environments) == 2
    assert environments[0].id == "env-1"
    assert environments[1].id == "env-2"
    assert isinstance(environments[0], MatillionEnvironment)


def test_get_pipeline_executions(
    api_client: MatillionAPIClient, requests_mock: Mocker
) -> None:
    mock_response = {
        "content": [
            {
                "id": "exec-1",
                "pipelineId": "pipe-1",
                "status": "success",
                "startedAt": "2024-01-01T00:00:00Z",
                "completedAt": "2024-01-01T00:05:00Z",
                "durationMs": 300000,
            }
        ],
        "totalElements": 1,
        "number": 0,
        "size": 10,
        "totalPages": 1,
    }

    requests_mock.get(
        "http://test.com/v1/pipelines/executions?pipelineId=pipe-1&size=10&page=0",
        json=mock_response,
    )

    executions = api_client.get_pipeline_executions("pipe-1", 10)

    assert len(executions) == 1
    assert executions[0].id == "exec-1"
    assert executions[0].status == "success"
    assert isinstance(executions[0], MatillionPipelineExecution)


def test_get_connections(api_client: MatillionAPIClient, requests_mock: Mocker) -> None:
    mock_response = {
        "content": [
            {
                "id": "conn-1",
                "name": "snowflake_prod",
                "type": "snowflake",
                "projectId": "proj-1",
            },
            {
                "id": "conn-2",
                "name": "bigquery_analytics",
                "type": "bigquery",
                "projectId": "proj-1",
            },
        ],
        "totalElements": 2,
        "number": 0,
        "size": 25,
        "totalPages": 1,
    }

    requests_mock.get(
        "http://test.com/v1/connections?projectId=proj-1&page=0&size=25",
        json=mock_response,
    )

    connections = api_client.get_connections("proj-1")

    assert len(connections) == 2
    assert connections[0].id == "conn-1"
    assert connections[1].name == "bigquery_analytics"
    assert isinstance(connections[0], MatillionConnection)


def test_get_agents(api_client: MatillionAPIClient, requests_mock: Mocker) -> None:
    mock_response = {
        "content": [
            {
                "id": "agent-1",
                "name": "US-East-Agent",
                "status": "online",
                "version": "1.0.0",
            }
        ],
        "totalElements": 1,
        "number": 0,
        "size": 25,
        "totalPages": 1,
    }

    requests_mock.get(
        "http://test.com/v1/agents?page=0&size=25",
        json=mock_response,
    )

    agents = api_client.get_agents()

    assert len(agents) == 1
    assert agents[0].id == "agent-1"
    assert agents[0].name == "US-East-Agent"
    assert isinstance(agents[0], MatillionAgent)


def test_get_repositories(
    api_client: MatillionAPIClient, requests_mock: Mocker
) -> None:
    mock_response = {
        "content": [
            {
                "id": "repo-1",
                "name": "matillion-pipelines",
                "projectId": "proj-1",
                "url": "https://github.com/org/matillion-pipelines",
                "provider": "github",
            }
        ],
        "totalElements": 1,
        "number": 0,
        "size": 25,
        "totalPages": 1,
    }

    requests_mock.get(
        "http://test.com/v1/repositories?size=25&page=0",
        json=mock_response,
    )

    repositories = api_client.get_repositories()

    assert len(repositories) == 1
    assert repositories[0].id == "repo-1"
    assert repositories[0].name == "matillion-pipelines"
    assert isinstance(repositories[0], MatillionRepository)


def test_get_repository_by_id(
    api_client: MatillionAPIClient, requests_mock: Mocker
) -> None:
    mock_response = {
        "id": "repo-1",
        "name": "matillion-pipelines",
        "projectId": "proj-1",
        "url": "https://github.com/org/matillion-pipelines",
        "provider": "github",
    }

    requests_mock.get(
        "http://test.com/v1/repositories/repo-1",
        json=mock_response,
    )

    repository = api_client.get_repository_by_id("repo-1")

    assert repository is not None
    assert repository.id == "repo-1"
    assert repository.name == "matillion-pipelines"
    assert isinstance(repository, MatillionRepository)


def test_get_repository_by_id_not_found(
    api_client: MatillionAPIClient, requests_mock: Mocker
) -> None:
    requests_mock.get(
        "http://test.com/v1/repositories/nonexistent",
        status_code=404,
    )

    repository = api_client.get_repository_by_id("nonexistent")

    assert repository is None


def test_get_repository_by_id_validation_error(
    api_client: MatillionAPIClient, requests_mock: Mocker
) -> None:
    requests_mock.get(
        "http://test.com/v1/repositories/bad-data",
        json={"invalid": "data"},
    )

    repository = api_client.get_repository_by_id("bad-data")

    assert repository is None


def test_get_schedules(api_client: MatillionAPIClient, requests_mock: Mocker) -> None:
    mock_response = {
        "content": [
            {
                "id": "sched-1",
                "name": "Daily Schedule",
                "pipelineId": "pipe-1",
                "cronExpression": "0 0 * * *",
                "enabled": True,
            }
        ],
        "totalElements": 1,
        "number": 0,
        "size": 25,
        "totalPages": 1,
    }

    requests_mock.get(
        "http://test.com/v1/schedules?pipelineId=pipe-1&size=25&page=0",
        json=mock_response,
    )

    schedules = api_client.get_schedules(pipeline_id="pipe-1")

    assert len(schedules) == 1
    assert schedules[0].id == "sched-1"
    assert schedules[0].cron_expression == "0 0 * * *"
    assert isinstance(schedules[0], MatillionSchedule)


def test_get_streaming_pipelines(
    api_client: MatillionAPIClient, requests_mock: Mocker
) -> None:
    mock_response = {
        "content": [
            {
                "id": "sp-1",
                "name": "MySQL CDC",
                "projectId": "proj-1",
                "environmentId": "env-1",
                "sourceType": "mysql",
                "targetType": "snowflake",
            }
        ],
        "totalElements": 1,
        "number": 0,
        "size": 25,
        "totalPages": 1,
    }

    requests_mock.get(
        "http://test.com/v1/streaming-pipelines?projectId=proj-1&page=0&size=25",
        json=mock_response,
    )

    streaming_pipelines = api_client.get_streaming_pipelines("proj-1")

    assert len(streaming_pipelines) == 1
    assert streaming_pipelines[0].id == "sp-1"
    assert streaming_pipelines[0].source_type == "mysql"
    assert isinstance(streaming_pipelines[0], MatillionStreamingPipeline)


def test_get_pipeline_lineage(
    api_client: MatillionAPIClient, requests_mock: Mocker
) -> None:
    mock_response = {
        "pipelineId": "pipe-1",
        "nodes": [
            {
                "id": "node-1",
                "name": "source_table",
                "type": "table",
                "platform": "snowflake",
                "schema": "raw",
                "table": "customers",
            }
        ],
        "edges": [
            {"sourceId": "node-1", "targetId": "node-2", "type": "transformation"}
        ],
    }

    requests_mock.get(
        "http://test.com/v1/pipelines/pipe-1/lineage",
        json=mock_response,
    )

    lineage = api_client.get_pipeline_lineage("pipe-1")

    assert lineage is not None
    assert lineage.pipeline_id == "pipe-1"
    assert len(lineage.nodes) == 1
    assert len(lineage.edges) == 1
    assert isinstance(lineage, MatillionLineageGraph)


def test_get_pipeline_lineage_not_found(
    api_client: MatillionAPIClient, requests_mock: Mocker
) -> None:
    requests_mock.get(
        "http://test.com/v1/pipelines/nonexistent/lineage",
        status_code=404,
    )

    lineage = api_client.get_pipeline_lineage("nonexistent")

    assert lineage is None


def test_get_pipeline_lineage_validation_error(
    api_client: MatillionAPIClient, requests_mock: Mocker
) -> None:
    requests_mock.get(
        "http://test.com/v1/pipelines/bad-data/lineage",
        json={"invalid": "data"},
    )

    lineage = api_client.get_pipeline_lineage("bad-data")

    assert lineage is None


def test_pagination_multiple_pages(
    api_client: MatillionAPIClient, requests_mock: Mocker
) -> None:
    mock_page1 = {
        "content": [{"id": "proj-1", "name": "Project 1"}],
        "totalElements": 2,
        "number": 0,
        "size": 1,
        "totalPages": 2,
    }

    mock_page2 = {
        "content": [{"id": "proj-2", "name": "Project 2"}],
        "totalElements": 2,
        "number": 1,
        "size": 1,
        "totalPages": 2,
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
        "content": [],
        "totalElements": 0,
        "number": 0,
        "size": 25,
        "totalPages": 0,
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
