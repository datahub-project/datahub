from unittest.mock import MagicMock, patch

import pytest
import requests
from pydantic import SecretStr

from datahub.ingestion.source.grafana.constants import (
    FOLDER_FETCH_ERROR_TITLE,
    FOLDER_PARSE_ERROR_TITLE,
)
from datahub.ingestion.source.grafana.grafana_api import GrafanaAPIClient
from datahub.ingestion.source.grafana.models import Dashboard, Folder
from datahub.ingestion.source.grafana.report import GrafanaSourceReport


@pytest.fixture
def mock_session():
    with patch("requests.Session") as session:
        yield session.return_value


@pytest.fixture
def api_client(mock_session):
    report = GrafanaSourceReport()
    return GrafanaAPIClient(
        base_url="http://grafana.test",
        token=SecretStr("test-token"),
        verify_ssl=True,
        page_size=100,
        report=report,
    )


def test_create_session(mock_session):
    report = GrafanaSourceReport()
    GrafanaAPIClient(
        base_url="http://grafana.test",
        token=SecretStr("test-token"),
        verify_ssl=True,
        page_size=100,
        report=report,
    )

    # Verify headers were properly set
    mock_session.headers.update.assert_called_once_with(
        {
            "Authorization": "Bearer test-token",
            "Accept": "application/json",
            "Content-Type": "application/json",
        }
    )

    # Verify SSL verification was set
    assert mock_session.verify is True


def test_get_folders_success(api_client, mock_session):
    # First call returns folders
    first_response = MagicMock()
    first_response.json.return_value = [
        {"id": "1", "title": "Folder 1", "description": ""},
        {"id": "2", "title": "Folder 2", "description": ""},
    ]
    first_response.raise_for_status.return_value = None

    # Second call returns empty list to end pagination
    second_response = MagicMock()
    second_response.json.return_value = []
    second_response.raise_for_status.return_value = None

    mock_session.get.side_effect = [first_response, second_response]

    folders = api_client.get_folders()

    assert len(folders) == 2
    assert all(isinstance(f, Folder) for f in folders)
    assert folders[0].id == "1"
    assert folders[0].title == "Folder 1"


def test_get_folders_error(api_client, mock_session):
    mock_session.get.side_effect = requests.exceptions.RequestException("API Error")

    folders = api_client.get_folders()

    assert len(folders) == 0
    assert len(api_client.report.failures) == 1


def test_get_dashboard_success(api_client, mock_session):
    mock_response = MagicMock()
    mock_response.json.return_value = {
        "dashboard": {
            "uid": "test-uid",
            "title": "Test Dashboard",
            "description": "",
            "version": "1",
            "panels": [],
            "tags": [],
            "schemaVersion": "1.0",
            "timezone": "utc",
            "refresh": None,
            "meta": {"folderId": "123"},
        }
    }
    mock_session.get.return_value = mock_response

    dashboard = api_client.get_dashboard("test-uid")

    assert isinstance(dashboard, Dashboard)
    assert dashboard.uid == "test-uid"
    assert dashboard.title == "Test Dashboard"


def test_get_dashboard_error(api_client, mock_session):
    mock_session.get.side_effect = requests.exceptions.RequestException("API Error")

    dashboard = api_client.get_dashboard("test-uid")

    assert dashboard is None
    assert len(api_client.report.warnings) == 1


def test_get_dashboards_success(api_client, mock_session):
    # Mock search response
    search_response = MagicMock()
    search_response.raise_for_status.return_value = None
    search_response.json.return_value = [{"uid": "dash1"}, {"uid": "dash2"}]

    # Mock individual dashboard responses
    dash1_response = MagicMock()
    dash1_response.raise_for_status.return_value = None
    dash1_response.json.return_value = {
        "dashboard": {
            "uid": "dash1",
            "title": "Dashboard 1",
            "description": "",
            "version": "1",
            "panels": [],
            "tags": [],
            "timezone": "utc",
            "schemaVersion": "1.0",
            "meta": {"folderId": None},
        }
    }

    # Mock dashboard2 response
    dash2_response = MagicMock()
    dash2_response.raise_for_status.return_value = None
    dash2_response.json.return_value = {
        "dashboard": {
            "uid": "dash2",
            "title": "Dashboard 2",
            "description": "",
            "version": "1",
            "panels": [],
            "tags": [],
            "timezone": "utc",
            "schemaVersion": "1.0",
            "meta": {"folderId": None},
        }
    }

    # Empty response to end pagination
    empty_response = MagicMock()
    empty_response.json.return_value = []
    empty_response.raise_for_status.return_value = None

    mock_session.get.side_effect = [
        search_response,
        dash1_response,
        dash2_response,
        empty_response,
    ]

    dashboards = api_client.get_dashboards()

    assert len(dashboards) == 2
    assert dashboards[0].uid == "dash1"
    assert dashboards[0].title == "Dashboard 1"


def test_get_dashboards_error(api_client, mock_session):
    mock_session.get.side_effect = requests.exceptions.RequestException("API Error")

    dashboards = api_client.get_dashboards()

    assert len(dashboards) == 0
    assert len(api_client.report.failures) == 1


def _json_response(body: object) -> MagicMock:
    response = MagicMock()
    response.json.return_value = body
    return response


def test_get_folders_skips_malformed_folder(api_client, mock_session):
    # A bad folder first on the page must not cost the folders after it,
    # or the pages after it.
    mock_session.get.side_effect = [
        _json_response([{"id": "2"}, {"id": "1", "title": "Folder 1"}]),
        _json_response([{"id": 3, "title": "Folder 3"}]),
        _json_response([]),
    ]

    folders = api_client.get_folders()

    assert [f.id for f in folders] == ["1", "3"]
    assert not api_client.report.failures
    assert [w.title for w in api_client.report.warnings] == [FOLDER_PARSE_ERROR_TITLE]
    [context] = api_client.report.warnings[0].context
    assert context.startswith("id=2, uid=None, title=None")
    assert api_client.report.folders_unparseable == 1


def test_get_folders_stops_on_non_list_page(api_client, mock_session):
    # A non-list body (e.g. a proxy error) must end paging, not loop forever.
    mock_session.get.side_effect = [
        _json_response({"message": "bad gateway", "status": "error"}),
        AssertionError("get_folders requested another page"),
    ]

    assert api_client.get_folders() == []
    assert [f.title for f in api_client.report.failures] == [FOLDER_FETCH_ERROR_TITLE]


def test_get_folders_keeps_earlier_pages_when_later_page_fails(
    api_client, mock_session
):
    mock_session.get.side_effect = [
        _json_response([{"id": 1, "title": "Folder 1"}]),
        requests.exceptions.ConnectionError("connection reset"),
    ]

    folders = api_client.get_folders()

    assert [f.id for f in folders] == ["1"]
    [failure] = api_client.report.failures
    assert [c.split(" ")[0] for c in failure.context] == ["2"]
    # A network error is not a permission problem.
    assert api_client.report.permission_warnings == 0


def test_get_folders_counts_permission_error(api_client, mock_session):
    forbidden = requests.Response()
    forbidden.status_code = 403
    response = MagicMock()
    response.raise_for_status.side_effect = requests.exceptions.HTTPError(
        response=forbidden
    )
    mock_session.get.return_value = response

    assert api_client.get_folders() == []
    assert api_client.report.permission_warnings == 1
