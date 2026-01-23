import pytest
from pydantic import SecretStr
from requests import HTTPError
from requests_mock import Mocker

from datahub.ingestion.source.matillion.config import (
    MatillionAPIConfig,
    MatillionRegion,
)
from datahub.ingestion.source.matillion.matillion_api import MatillionAPIClient
from datahub.ingestion.source.matillion.models import (
    MatillionPipeline,
    MatillionProject,
)


@pytest.fixture
def api_config():
    return MatillionAPIConfig(
        api_token=SecretStr("test_token"),
        region=MatillionRegion.EU1,
    )


@pytest.fixture
def api_client(api_config):
    return MatillionAPIClient(api_config)


def test_api_client_initialization(api_client):
    assert api_client.config.region == MatillionRegion.EU1
    assert api_client.config.get_base_url() == "https://eu1.api.matillion.com/dpc"
    assert api_client.config.api_token.get_secret_value() == "test_token"


def test_get_projects(api_client: MatillionAPIClient, requests_mock: Mocker) -> None:
    mock_response = {
        "results": [
            {
                "id": "proj-1",
                "name": "Test Project",
                "description": "A test project",
            }
        ],
        "total": 1,
        "page": 0,
        "size": 25,
    }

    requests_mock.get(
        "https://eu1.api.matillion.com/dpc/v1/projects?page=0&size=25",
        json=mock_response,
    )

    projects = api_client.get_projects()

    assert len(projects) == 1
    assert projects[0].id == "proj-1"
    assert projects[0].name == "Test Project"
    assert isinstance(projects[0], MatillionProject)


def test_get_pipelines(api_client: MatillionAPIClient, requests_mock: Mocker) -> None:
    mock_response = {
        "results": [
            {
                "name": "Test Pipeline",
                "publishedTime": "2024-01-01T00:00:00Z",
            }
        ],
        "total": 1,
        "page": 0,
        "size": 25,
    }

    requests_mock.get(
        "https://eu1.api.matillion.com/dpc/v1/projects/proj-1/published-pipelines?environmentName=Production&page=0&size=25",
        json=mock_response,
    )

    pipelines = api_client.get_pipelines("proj-1", "Production")

    assert len(pipelines) == 1
    assert pipelines[0].name == "Test Pipeline"
    assert isinstance(pipelines[0], MatillionPipeline)


def test_api_error_handling(
    api_client: MatillionAPIClient, requests_mock: Mocker
) -> None:
    requests_mock.get(
        "https://eu1.api.matillion.com/dpc/v1/projects?page=0&size=25",
        status_code=401,
    )

    with pytest.raises(HTTPError):
        api_client.get_projects()
