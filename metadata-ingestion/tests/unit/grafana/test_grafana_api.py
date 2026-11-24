from typing import Dict
from unittest.mock import MagicMock, patch

import pytest
import requests
from pydantic import SecretStr

from datahub.ingestion.source.grafana.constants import (
    DASHBOARD_FETCH_ERROR_TITLE,
    DASHBOARD_PARSE_ERROR_TITLE,
    DASHBOARD_SEARCH_ERROR_TITLE,
    FOLDER_FETCH_ERROR_TITLE,
    FOLDER_PARSE_ERROR_TITLE,
    REPEATED_PAGE_TITLE,
)
from datahub.ingestion.source.grafana.grafana_api import GrafanaAPIClient
from datahub.ingestion.source.grafana.models import Dashboard, Folder
from datahub.ingestion.source.grafana.report import GrafanaSourceReport
from tests.unit.conftest import log_contexts, log_titles


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

    # Transient errors are retried for both schemes.
    mounted = {call.args[0]: call.args[1] for call in mock_session.mount.call_args_list}
    assert set(mounted) == {"http://", "https://"}
    assert 503 in mounted["https://"].max_retries.status_forcelist


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

    listing = api_client.get_folders()

    folders = listing.folders
    assert len(folders) == 2
    assert listing.unparseable == 0
    assert all(isinstance(f, Folder) for f in folders)
    assert folders[0].id == "1"
    assert folders[0].title == "Folder 1"


def test_get_folders_error(api_client, mock_session):
    mock_session.get.side_effect = requests.exceptions.RequestException("API Error")

    assert api_client.get_folders().folders == []
    assert len(api_client.report.failures) == 1
    # A network error is not a permission problem.
    assert api_client.report.permission_warnings == 0


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

    dashboards = api_client.get_dashboards().dashboards

    assert len(dashboards) == 2
    assert dashboards[0].uid == "dash1"
    assert dashboards[0].title == "Dashboard 1"


def test_get_dashboards_error(api_client, mock_session):
    mock_session.get.side_effect = requests.exceptions.RequestException("API Error")

    assert api_client.get_dashboards().dashboards == []
    assert len(api_client.report.failures) == 1


def _json_response(body: object) -> MagicMock:
    response = MagicMock()
    response.json.return_value = body
    return response


def _dashboard_body(uid: str) -> Dict[str, object]:
    return {"dashboard": {"uid": uid, "title": uid, "panels": []}}


def test_get_folders_reads_past_short_page(api_client, mock_session):
    # A server may cap the page below page_size, so a short page does not
    # mean the listing is complete; only an empty page does.
    api_client.page_size = 3
    mock_session.get.side_effect = [
        _json_response([{"id": 1, "title": "a"}, {"id": 2, "title": "b"}]),
        _json_response([{"id": 3, "title": "c"}]),
        _json_response([]),
    ]

    assert [f.id for f in api_client.get_folders().folders] == ["1", "2", "3"]
    assert not api_client.report.failures


@pytest.mark.parametrize(
    "repeat",
    [
        pytest.param(lambda page: page, id="same-order"),
        pytest.param(lambda page: list(reversed(page)), id="other-order"),
    ],
)
def test_get_folders_fails_when_full_page_repeats(api_client, mock_session, repeat):
    # An API that ignores the page parameter returns the same full page
    # forever, and later folders may be missing.
    api_client.page_size = 2
    page = [{"id": 1, "title": "a"}, {"id": 2, "title": "b"}]
    mock_session.get.side_effect = [
        _json_response(page),
        _json_response(repeat(page)),
        AssertionError("get_folders kept paging"),
    ]

    assert [f.id for f in api_client.get_folders().folders] == ["1", "2"]
    # A failure, so stale-entity removal does not delete what was not listed.
    assert log_titles(api_client.report.failures) == {REPEATED_PAGE_TITLE}


def test_get_folders_stops_quietly_when_short_page_repeats(api_client, mock_session):
    # A repeated page shorter than page_size already held the whole listing.
    api_client.page_size = 5
    page = [{"id": 1, "title": "a"}]
    mock_session.get.side_effect = [
        _json_response(page),
        _json_response(page),
        AssertionError("get_folders kept paging"),
    ]

    assert [f.id for f in api_client.get_folders().folders] == ["1"]
    assert not api_client.report.failures
    assert not api_client.report.warnings


def test_get_dashboards_fails_when_full_page_repeats(api_client, mock_session):
    api_client.page_size = 1
    mock_session.get.side_effect = [
        _json_response([{"uid": "a"}]),
        _json_response(_dashboard_body("a")),
        _json_response([{"uid": "a"}]),
        AssertionError("get_dashboards kept paging"),
    ]

    assert [d.uid for d in api_client.get_dashboards().dashboards] == ["a"]
    assert log_titles(api_client.report.failures) == {REPEATED_PAGE_TITLE}


def test_get_dashboards_stops_when_page_without_uids_repeats(api_client, mock_session):
    # A full page of hits without a uid never yields a dashboard, so only the
    # repeated page itself can end paging.
    api_client.page_size = 2
    page = [{"title": "a"}, {"title": "b"}]
    mock_session.get.side_effect = [
        _json_response(page),
        _json_response(page),
        AssertionError("get_dashboards kept paging"),
    ]

    listing = api_client.get_dashboards()

    assert listing.dashboards == []
    assert listing.skipped == 2
    assert REPEATED_PAGE_TITLE in log_titles(api_client.report.failures)


@pytest.mark.parametrize(
    "body",
    [
        pytest.param({"message": "Unauthorized"}, id="object"),
        pytest.param("<html>proxy error</html>", id="string"),
        pytest.param(42, id="number"),
        pytest.param({}, id="empty-object"),
        pytest.param(None, id="null"),
    ],
)
def test_get_dashboards_reports_non_list_search_page(api_client, mock_session, body):
    # A non-list body is a failure, not a list of skipped hits, and ends paging.
    api_client.page_size = 1
    mock_session.get.side_effect = [
        _json_response(body),
        AssertionError("get_dashboards kept paging"),
    ]

    assert api_client.get_dashboards().dashboards == []
    assert log_titles(api_client.report.failures) == {DASHBOARD_SEARCH_ERROR_TITLE}
    assert api_client.report.dashboard_search_hits_skipped == 0


@pytest.mark.parametrize(
    "body",
    [
        pytest.param({"dashboard": None}, id="dashboard-null"),
        pytest.param({"dashboard": ["x"]}, id="dashboard-list"),
        pytest.param({"dashboard": "x"}, id="dashboard-string"),
        pytest.param(
            {"dashboard": {"uid": "x", "title": "x", "panels": 5}}, id="panels-number"
        ),
    ],
)
def test_get_dashboard_skips_malformed_nested_body(api_client, mock_session, body):
    # These used to raise AttributeError/TypeError out of the model validator,
    # which get_dashboard did not catch, stopping ingestion.
    mock_session.get.return_value = _json_response(body)

    assert api_client.get_dashboard("x") is None
    assert api_client.report.dashboards_unparseable == 1
    assert log_titles(api_client.report.warnings) == {DASHBOARD_PARSE_ERROR_TITLE}


@pytest.mark.parametrize("meta", ["x", ["x"]])
def test_get_dashboard_ignores_non_object_meta(api_client, mock_session, meta):
    mock_session.get.return_value = _json_response(
        {"dashboard": {"uid": "x", "title": "x", "panels": []}, "meta": meta}
    )

    dashboard = api_client.get_dashboard("x")

    assert dashboard is not None
    assert dashboard.folder_id is None


def test_get_dashboards_skips_bad_search_hits_and_dashboards(api_client, mock_session):
    mock_session.get.side_effect = [
        _json_response(
            [{"title": "no uid"}, "not an object", {"uid": "bad"}, {"uid": "ok"}]
        ),
        _json_response({"dashboard": {"uid": "bad"}}),  # no title or panels
        _json_response(_dashboard_body("ok")),
        _json_response([]),
    ]

    listing = api_client.get_dashboards()

    assert [d.uid for d in listing.dashboards] == ["ok"]
    # Two results without a uid, and one dashboard that could not be parsed.
    assert listing.skipped == 3
    assert api_client.report.dashboard_search_hits_skipped == 2
    assert api_client.report.dashboards_unparseable == 1
    assert not api_client.report.failures


def test_get_dashboards_treats_404_as_deleted(api_client, mock_session):
    # A dashboard deleted between search and fetch is gone from Grafana, so it
    # must not count as skipped, which would fail a run with stale removal on.
    mock_session.get.side_effect = [
        _json_response([{"uid": "gone"}, {"uid": "ok"}]),
        _http_error_response(404),
        _json_response(_dashboard_body("ok")),
        _json_response([]),
    ]

    listing = api_client.get_dashboards()

    assert [d.uid for d in listing.dashboards] == ["ok"]
    assert listing.skipped == 0
    assert api_client.report.dashboards_deleted_after_search == 1
    assert not api_client.report.warnings


def test_get_dashboard_reports_404(api_client, mock_session):
    mock_session.get.return_value = _http_error_response(404)

    assert api_client.get_dashboard("x") is None
    assert log_titles(api_client.report.warnings) == {DASHBOARD_FETCH_ERROR_TITLE}


def test_get_dashboard_skips_non_object_body_with_skip_text_panels(
    api_client, mock_session
):
    api_client.skip_text_panels = True
    mock_session.get.return_value = _json_response(["not", "a", "dashboard"])

    assert api_client.get_dashboard("x") is None
    assert api_client.report.dashboards_unparseable == 1


def _http_error_response(status_code: int) -> MagicMock:
    error_response = requests.Response()
    error_response.status_code = status_code
    response = MagicMock()
    response.raise_for_status.side_effect = requests.exceptions.HTTPError(
        response=error_response
    )
    return response


def test_get_folders_skips_malformed_folder(api_client, mock_session):
    # A bad folder first on the page must not cost the folders after it,
    # or the pages after it.
    api_client.page_size = 2
    mock_session.get.side_effect = [
        _json_response([{"title": "No id"}, {"id": "1", "title": "Folder 1"}]),
        _json_response([{"id": 3, "title": "Folder 3"}]),
        _json_response([]),
    ]

    listing = api_client.get_folders()

    assert [f.id for f in listing.folders] == ["1", "3"]
    assert listing.unparseable == 1
    assert api_client.report.folders_unparseable == 1
    assert log_titles(api_client.report.warnings) == {FOLDER_PARSE_ERROR_TITLE}
    assert any("No id" in c for c in log_contexts(api_client.report.warnings))
    # Whether a skipped folder fails the run is the source's decision.
    assert not api_client.report.failures


@pytest.mark.parametrize(
    "body",
    [
        pytest.param({"message": "bad gateway"}, id="object"),
        pytest.param({}, id="empty-object"),
        pytest.param(None, id="null"),
    ],
)
def test_get_folders_stops_on_non_list_page(api_client, mock_session, body):
    # A non-list body (e.g. a proxy error) must end paging with a failure, even
    # when it is empty, and must never request another page.
    mock_session.get.side_effect = [
        _json_response(body),
        AssertionError("get_folders requested another page"),
    ]

    assert api_client.get_folders().folders == []
    assert log_titles(api_client.report.failures) == {FOLDER_FETCH_ERROR_TITLE}


def _json_decode_error_response() -> MagicMock:
    response = MagicMock()
    response.json.side_effect = requests.exceptions.JSONDecodeError(
        "Expecting value", "<html>", 0
    )
    return response


@pytest.mark.parametrize(
    "second_page",
    [
        pytest.param(
            requests.exceptions.ConnectionError("connection reset"), id="network"
        ),
        pytest.param(_json_response({"message": "bad gateway"}), id="not-a-list"),
        pytest.param(_json_decode_error_response(), id="not-json"),
    ],
)
def test_get_folders_keeps_earlier_pages_when_later_page_fails(
    api_client, mock_session, second_page
):
    api_client.page_size = 1
    mock_session.get.side_effect = [
        _json_response([{"id": 1, "title": "Folder 1"}]),
        second_page,
    ]

    listing = api_client.get_folders()

    assert [f.id for f in listing.folders] == ["1"]
    assert log_titles(api_client.report.failures) == {FOLDER_FETCH_ERROR_TITLE}
    assert any(c.startswith("page 2") for c in log_contexts(api_client.report.failures))


@pytest.mark.parametrize(
    "fetch",
    [
        pytest.param(lambda client: client.get_folders(), id="get_folders"),
        pytest.param(lambda client: client.get_dashboard("uid"), id="get_dashboard"),
        pytest.param(lambda client: client.get_dashboards(), id="get_dashboards"),
    ],
)
@pytest.mark.parametrize(
    "status_code, expected_permission_warnings",
    [(401, 1), (403, 1), (500, 0)],
)
def test_counts_only_permission_errors(
    api_client, mock_session, fetch, status_code, expected_permission_warnings
):
    mock_session.get.return_value = _http_error_response(status_code)

    fetch(api_client)

    assert api_client.report.permission_warnings == expected_permission_warnings
