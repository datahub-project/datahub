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


def test_reports_and_workbooks_skipped_without_platform_credentials() -> None:
    # Core (or Cloud without a cloud_api_key + deployment_id) cannot reach the
    # Platform API, so report/workbook discovery is a no-op.
    client = _client(deployment_type="CORE", api_url="http://localhost/cubejs-api")
    assert client.get_reports() == []
    assert client.get_workbooks() == []


def test_get_reports_uses_platform_api_with_bearer_key(
    requests_mock: rm.Mocker,
) -> None:
    client = _client(cloud_api_key="cp-key", deployment_id="123")
    matcher = requests_mock.get(
        "https://demo.cubecloud.dev/api/v1/deployments/123/reports",
        json={
            "items": [
                {
                    "id": 1,
                    "publicId": "rpt1",
                    "name": "r1",
                    "title": "Report One",
                    "jsonQuery": '{"measures":["orders.count"],"dimensions":["orders.status"]}',
                    "workbookId": 9,
                    "user": {"id": 1, "email": "a@example.com"},
                }
            ],
            "pageInfo": {"hasNextPage": False, "endCursor": None},
        },
    )

    reports = client.get_reports()

    assert matcher.last_request is not None
    assert matcher.last_request.headers["Authorization"] == "Bearer cp-key"
    assert len(reports) == 1
    report = reports[0]
    assert report.referenced_entities == ["orders"]
    assert report.owner_email == "a@example.com"
    assert report.workbook_id == 9


def test_get_reports_follows_cursor_pagination(requests_mock: rm.Mocker) -> None:
    client = _client(cloud_api_key="cp-key", deployment_id="123")
    url = "https://demo.cubecloud.dev/api/v1/deployments/123/reports"
    requests_mock.get(
        url,
        [
            {
                "json": {
                    "items": [{"id": 1, "name": "r1"}],
                    "pageInfo": {"hasNextPage": True, "endCursor": "c1"},
                }
            },
            {
                "json": {
                    "items": [{"id": 2, "name": "r2"}],
                    "pageInfo": {"hasNextPage": False, "endCursor": None},
                }
            },
        ],
    )

    reports = client.get_reports()
    assert [r.id for r in reports] == [1, 2]


def test_get_workbooks_parses_report_snapshots(requests_mock: rm.Mocker) -> None:
    client = _client(cloud_api_key="cp-key", deployment_id="123")
    requests_mock.get(
        "https://demo.cubecloud.dev/api/v1/deployments/123/workbooks",
        json={
            "items": [
                {
                    "id": 9,
                    "name": "wb",
                    "dashboardPublished": {"title": "WB", "description": "desc"},
                    "publishedDashboard": {
                        "reportSnapshots": [{"reportId": 1}, {"reportId": 2}]
                    },
                    "user": {"id": 1, "email": "a@example.com"},
                }
            ],
            "pageInfo": {"hasNextPage": False},
        },
    )

    workbooks = client.get_workbooks()
    assert len(workbooks) == 1
    assert workbooks[0].report_ids == [1, 2]
    assert workbooks[0].title == "WB"


def test_get_reports_returns_empty_on_http_error(requests_mock: rm.Mocker) -> None:
    client = _client(cloud_api_key="cp-key", deployment_id="123")
    requests_mock.get(
        "https://demo.cubecloud.dev/api/v1/deployments/123/reports", status_code=403
    )
    assert client.get_reports() == []
