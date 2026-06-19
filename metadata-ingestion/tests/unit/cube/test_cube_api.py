from typing import Dict

import requests_mock as rm

from datahub.ingestion.source.cube.config import CubeSourceConfig
from datahub.ingestion.source.cube.cube_api import CubeAPIClient


def _client(**overrides: object) -> CubeAPIClient:
    base: Dict[str, object] = {
        "api_url": "https://demo.cubecloud.dev/cubejs-api",
        "api_token": "data-token",
        "deployment_type": "CLOUD",
    }
    base.update(overrides)
    return CubeAPIClient(CubeSourceConfig.model_validate(base))


def test_metadata_token_uses_api_token_without_control_plane() -> None:
    client = _client()
    assert client._metadata_api_token() == "data-token"
    assert client._auth_header_value(bearer=True) == "Bearer data-token"
    assert client._auth_header_value(bearer=False) == "data-token"


def test_metadata_token_minted_via_control_plane_and_cached(
    requests_mock: rm.Mocker,
) -> None:
    client = _client(
        cloud_api_key="cp-key",
        deployment_id="123",
        environment_id="456",
        security_context={"tenant_id": "acme"},
    )
    matcher = requests_mock.post(
        "https://demo.cubecloud.dev/api/v1/deployments/123/environments/456/tokens-for-meta-sync",
        json={"data": {"token": "minted-meta-token"}},
    )

    assert client._metadata_api_token() == "minted-meta-token"
    # Subsequent calls reuse the cached token rather than re-minting.
    assert client._metadata_api_token() == "minted-meta-token"
    assert matcher.call_count == 1

    request = matcher.last_request
    assert request is not None
    assert request.headers["Authorization"] == "Bearer cp-key"
    assert request.json() == {
        "security_context": {"tenant_id": "acme"},
        "expires_in": 86400,
    }


def test_control_plane_base_can_be_overridden(requests_mock: rm.Mocker) -> None:
    client = _client(
        cloud_api_key="cp-key",
        cloud_api_url="https://control.cubecloud.dev",
        deployment_id="d1",
        environment_id="e1",
    )
    matcher = requests_mock.post(
        "https://control.cubecloud.dev/api/v1/deployments/d1/environments/e1/tokens-for-meta-sync",
        json={"data": {"token": "tok"}},
    )
    assert client._metadata_api_token() == "tok"
    assert matcher.call_count == 1
