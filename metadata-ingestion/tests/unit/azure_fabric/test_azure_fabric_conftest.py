from unittest.mock import MagicMock

import pytest
from datahub_azure_fabric.source import AzureFabricSourceConfig


@pytest.fixture
def mock_config():
    return AzureFabricSourceConfig(
        tenant_id="test-tenant",
        workspace_url="https://test-workspace.com",
        workspace_name="test-workspace",
    )


@pytest.fixture
def mock_auth_client():
    client = MagicMock()
    client.get_token.return_value = "test-token"
    return client


@pytest.fixture
def mock_response():
    response = MagicMock()
    response.json.return_value = {"value": [], "nextLink": None}
    response.status_code = 200
    return response
