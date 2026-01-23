import time
from unittest.mock import patch

import pytest
from pydantic import SecretStr
from requests import HTTPError
from requests_mock import Mocker

from datahub.ingestion.source.matillion_dpc.config import (
    MatillionAPIConfig,
    MatillionRegion,
)
from datahub.ingestion.source.matillion_dpc.constants import (
    OAUTH_TOKEN_REFRESH_BUFFER_SECONDS,
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
        client_id=SecretStr("test_client_id"),
        client_secret=SecretStr("test_client_secret"),
        custom_base_url="http://test.com",
    )


@pytest.fixture
def api_config_with_region() -> MatillionAPIConfig:
    return MatillionAPIConfig(
        client_id=SecretStr("test_client_id"),
        client_secret=SecretStr("test_client_secret"),
        region=MatillionRegion.EU1,
    )


@pytest.fixture
def oauth_config() -> MatillionAPIConfig:
    """Config using OAuth2 client credentials"""
    return MatillionAPIConfig(
        client_id=SecretStr("test_client_id"),
        client_secret=SecretStr("test_client_secret"),
        custom_base_url="http://test.com",
    )


@pytest.fixture
def api_client(
    api_config: MatillionAPIConfig, requests_mock: Mocker
) -> MatillionAPIClient:
    # Mock OAuth token endpoint for all tests
    requests_mock.post(
        "https://id.core.matillion.com/oauth/dpc/token",
        json={"access_token": "test_token", "token_type": "Bearer"},
    )
    return MatillionAPIClient(api_config)


@pytest.fixture
def api_client_with_region(
    api_config_with_region: MatillionAPIConfig, requests_mock: Mocker
) -> MatillionAPIClient:
    # Mock OAuth token endpoint for all tests
    requests_mock.post(
        "https://id.core.matillion.com/oauth/dpc/token",
        json={"access_token": "test_token", "token_type": "Bearer"},
    )
    return MatillionAPIClient(api_config_with_region)


def test_api_client_initialization(
    api_client_with_region: MatillionAPIClient,
) -> None:
    assert api_client_with_region.config.region == MatillionRegion.EU1
    assert (
        api_client_with_region.config.get_base_url()
        == "https://eu1.api.matillion.com/dpc"
    )
    assert (
        api_client_with_region.config.client_id.get_secret_value() == "test_client_id"
    )
    assert (
        api_client_with_region.config.client_secret.get_secret_value()
        == "test_client_secret"
    )


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


def test_timeout_configuration(
    api_config: MatillionAPIConfig, requests_mock: Mocker
) -> None:
    # Mock OAuth token endpoint
    requests_mock.post(
        "https://id.core.matillion.com/oauth/dpc/token",
        json={"access_token": "test_token", "token_type": "Bearer"},
    )

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


def test_session_headers(api_client: MatillionAPIClient) -> None:
    """Test that OAuth2 token is set in session headers"""
    assert "Authorization" in api_client.session.headers
    assert api_client.session.headers["Authorization"] == "Bearer test_token"
    assert "Content-Type" in api_client.session.headers
    assert api_client.session.headers["Content-Type"] == "application/json"


# OAuth2 Token Management Tests


def test_oauth_initialization_generates_token(
    oauth_config: MatillionAPIConfig, requests_mock: Mocker
) -> None:
    """Test that OAuth2 client generates token on initialization"""
    mock_token_response = {
        "access_token": "test_access_token_123",
        "token_type": "Bearer",
        "expires_in": 1800,
    }

    token_request = requests_mock.post(
        "https://id.core.matillion.com/oauth/dpc/token",
        json=mock_token_response,
    )

    client = MatillionAPIClient(oauth_config)

    # Verify token was requested
    assert token_request.called
    assert token_request.call_count == 1

    # Verify token was set in session headers
    assert "Authorization" in client.session.headers
    assert client.session.headers["Authorization"] == "Bearer test_access_token_123"

    # Verify internal state
    assert client._token_expiry_time is not None
    assert client._token_expiry_time > time.time()


def test_oauth_token_refresh_before_expiry(
    oauth_config: MatillionAPIConfig, requests_mock: Mocker
) -> None:
    """Test that token is proactively refreshed before expiry"""
    initial_token = "initial_token"
    refreshed_token = "refreshed_token"

    # Mock token endpoint with multiple responses
    token_request = requests_mock.post(
        "https://id.core.matillion.com/oauth/dpc/token",
        [
            {"json": {"access_token": initial_token, "token_type": "Bearer"}},
            {"json": {"access_token": refreshed_token, "token_type": "Bearer"}},
        ],
    )

    client = MatillionAPIClient(oauth_config)
    assert client.session.headers["Authorization"] == f"Bearer {initial_token}"

    # Mock API request
    requests_mock.get(
        "http://test.com/v1/projects?page=0&size=25",
        json={"results": [], "total": 0, "page": 0, "size": 25},
    )

    # Simulate token approaching expiry (within refresh buffer)
    client._token_expiry_time = time.time() + OAUTH_TOKEN_REFRESH_BUFFER_SECONDS - 10

    # Make API call - should trigger token refresh
    client.get_projects()

    # Verify token was refreshed
    assert token_request.call_count >= 2  # Initial + refresh
    assert client.session.headers["Authorization"] == f"Bearer {refreshed_token}"


def test_oauth_401_triggers_refresh_and_retry(
    oauth_config: MatillionAPIConfig, requests_mock: Mocker
) -> None:
    """Test that 401 error triggers token refresh and request retry"""
    initial_token = "initial_token"
    refreshed_token = "refreshed_token_after_401"

    # Mock initial token generation
    requests_mock.post(
        "https://id.core.matillion.com/oauth/dpc/token",
        [
            {"json": {"access_token": initial_token, "token_type": "Bearer"}},
            {"json": {"access_token": refreshed_token, "token_type": "Bearer"}},
        ],
    )

    client = MatillionAPIClient(oauth_config)

    # Mock API to return 401 first, then success
    api_mock = requests_mock.get(
        "http://test.com/v1/projects?page=0&size=25",
        [
            {"status_code": 401, "text": "Unauthorized"},
            {
                "status_code": 200,
                "json": {"results": [], "total": 0, "page": 0, "size": 25},
            },
        ],
    )

    # Make API call - should get 401, refresh token, and retry
    projects = client.get_projects()

    # Verify retry happened
    assert api_mock.call_count == 2
    assert len(projects) == 0

    # Verify token was refreshed
    assert client.session.headers["Authorization"] == f"Bearer {refreshed_token}"


def test_oauth_long_running_session_simulation(
    oauth_config: MatillionAPIConfig, requests_mock: Mocker
) -> None:
    """Test that long-running sessions (>30 min) work with automatic refresh"""
    tokens = ["token_1", "token_2", "token_3"]
    token_index = 0

    def get_token_response(request, context):
        nonlocal token_index
        token = tokens[token_index]
        token_index += 1
        return {"access_token": token, "token_type": "Bearer"}

    # Mock token endpoint
    requests_mock.post(
        "https://id.core.matillion.com/oauth/dpc/token",
        json=get_token_response,
    )

    client = MatillionAPIClient(oauth_config)
    assert client.session.headers["Authorization"] == "Bearer token_1"

    # Mock API endpoint
    requests_mock.get(
        "http://test.com/v1/projects?page=0&size=25",
        json={"results": [], "total": 0, "page": 0, "size": 25},
    )

    # Simulate 60-minute session with API calls every 20 minutes
    # t=0: token_1 generated (expires at t=30)
    # t=20: no refresh (still 10 min until expiry)
    client.get_projects()
    assert client.session.headers["Authorization"] == "Bearer token_1"

    # t=25: refresh triggered (within 5-min buffer)
    client._token_expiry_time = time.time() + OAUTH_TOKEN_REFRESH_BUFFER_SECONDS - 10
    client.get_projects()
    assert client.session.headers["Authorization"] == "Bearer token_2"

    # t=55: another refresh (new token also approaching expiry)
    client._token_expiry_time = time.time() + OAUTH_TOKEN_REFRESH_BUFFER_SECONDS - 10
    client.get_projects()
    assert client.session.headers["Authorization"] == "Bearer token_3"


def test_oauth_should_refresh_token_logic(requests_mock: Mocker) -> None:
    """Test the token refresh decision logic"""
    config = MatillionAPIConfig(
        client_id=SecretStr("test_client_id"),
        client_secret=SecretStr("test_client_secret"),
        custom_base_url="http://test.com",
    )

    with patch.object(MatillionAPIClient, "_generate_oauth_token"):
        # Need to mock since we're patching _generate_oauth_token
        requests_mock.post(
            "https://id.core.matillion.com/oauth/dpc/token",
            json={"access_token": "test_token", "token_type": "Bearer"},
        )

        client = MatillionAPIClient(config)

        # Case 1: Token expires in 10 minutes - should NOT refresh
        client._token_expiry_time = time.time() + 600  # 10 minutes
        assert not client._should_refresh_token()

        # Case 2: Token expires in 4 minutes - SHOULD refresh
        client._token_expiry_time = time.time() + 240  # 4 minutes
        assert client._should_refresh_token()

        # Case 3: Token expired - SHOULD refresh
        client._token_expiry_time = time.time() - 60  # 1 minute ago
        assert client._should_refresh_token()
