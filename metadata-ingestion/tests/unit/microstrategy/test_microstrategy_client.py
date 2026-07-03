from typing import Any, Dict, List, Optional, Tuple
from unittest import mock

import pytest
import requests
from pytest import MonkeyPatch

from datahub.ingestion.source.microstrategy.client import (
    MicroStrategyAPIError,
    MicroStrategyClient,
)
from datahub.ingestion.source.microstrategy.config import MicroStrategyConfig
from datahub.ingestion.source.microstrategy.report import MicroStrategyReport


class FakeResponse:
    status_code = 204
    content = b""
    headers = {"X-MSTR-AuthToken": "token-1"}

    def raise_for_status(self) -> None:
        return None

    def json(self) -> Dict[str, Any]:
        return {}


class FakeNonJsonResponse:
    status_code = 200
    content = b"<html>not json</html>"

    def json(self) -> Dict[str, Any]:
        raise ValueError("invalid json")


class StatusResponse:
    def __init__(
        self,
        status_code: int,
        headers: Optional[Dict[str, str]] = None,
        payload: Optional[Dict[str, Any]] = None,
    ) -> None:
        self.status_code = status_code
        self.headers = headers or {}
        self._payload = payload or {}
        self.content = b"{}"

    def json(self) -> Dict[str, Any]:
        return self._payload

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise requests.HTTPError(f"{self.status_code} error")


def _make_client(
    config_overrides: Optional[Dict[str, Any]] = None,
) -> Tuple[MicroStrategyClient, MicroStrategyReport]:
    config_dict: Dict[str, Any] = {
        "base_url": "https://mstr.example.com/MicroStrategyLibrary"
    }
    config_dict.update(config_overrides or {})
    config = MicroStrategyConfig.model_validate(config_dict)
    report = MicroStrategyReport()
    return MicroStrategyClient(config, report), report


def test_guest_login_sets_auth_token(monkeypatch: MonkeyPatch) -> None:
    config = MicroStrategyConfig.model_validate(
        {"base_url": "https://mstr.example.com/MicroStrategyLibrary"}
    )
    client = MicroStrategyClient(config, MicroStrategyReport())
    captured_json: Optional[Dict[str, Any]] = None

    def fake_request(**kwargs: Any) -> FakeResponse:
        nonlocal captured_json
        captured_json = kwargs.get("json")
        return FakeResponse()

    monkeypatch.setattr(client.session, "request", fake_request)

    client.login()

    assert captured_json == {"loginMode": 8}
    assert client.session.headers["X-MSTR-AuthToken"] == "token-1"


def test_extract_search_results_from_nested_result() -> None:
    response = {
        "result": {
            "items": [
                {"id": "dash-1", "name": "Dashboard 1"},
                {"id": "dash-2", "name": "Dashboard 2"},
            ]
        }
    }

    assert (
        MicroStrategyClient._extract_search_results(response)
        == response["result"]["items"]
    )


def test_list_projects_handles_top_level_list_response(
    monkeypatch: MonkeyPatch,
) -> None:
    config = MicroStrategyConfig.model_validate(
        {"base_url": "https://mstr.example.com/MicroStrategyLibrary"}
    )
    client = MicroStrategyClient(config, MicroStrategyReport())

    monkeypatch.setattr(
        client,
        "_get_json",
        lambda path: [{"id": "project-1", "name": "Project 1"}],
    )

    projects = client.list_projects()

    assert len(projects) == 1
    assert projects[0].id == "project-1"


def test_metadata_search_uses_quick_search_results_endpoint(
    monkeypatch: MonkeyPatch,
) -> None:
    config = MicroStrategyConfig.model_validate(
        {"base_url": "https://mstr.example.com/MicroStrategyLibrary"}
    )
    client = MicroStrategyClient(config, MicroStrategyReport())
    captured_path: Optional[str] = None

    def fake_get_json(
        path: str,
        project_id: Optional[str] = None,
        params: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        nonlocal captured_path
        captured_path = path
        assert params is not None
        # Ancestors carry the folder path used for folder containers.
        assert params["getAncestors"] is True
        return {"result": [{"id": "dash-1", "name": "Dashboard 1"}]}

    monkeypatch.setattr(client, "_get_json", fake_get_json)

    results = list(client._metadata_search(project_id="project-1", type_filter="55"))

    assert captured_path == "/api/searches/results"
    assert results == [{"id": "dash-1", "name": "Dashboard 1"}]


def test_search_reports_uses_report_object_type(
    monkeypatch: MonkeyPatch,
) -> None:
    config = MicroStrategyConfig.model_validate(
        {"base_url": "https://mstr.example.com/MicroStrategyLibrary"}
    )
    client = MicroStrategyClient(config, MicroStrategyReport())
    captured_params: Optional[Dict[str, Any]] = None

    def fake_get_json(
        path: str,
        project_id: Optional[str] = None,
        params: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        nonlocal captured_params
        captured_params = params
        return {"result": [{"id": "report-1", "name": "Sales Report"}]}

    monkeypatch.setattr(client, "_get_json", fake_get_json)

    reports = list(client.search_reports(project_id="project-1"))

    assert captured_params is not None
    assert captured_params["type"] == "3"
    assert reports[0].id == "report-1"


def test_list_datasources_reads_datasource_management_response(
    monkeypatch: MonkeyPatch,
) -> None:
    config = MicroStrategyConfig.model_validate(
        {"base_url": "https://mstr.example.com/MicroStrategyLibrary"}
    )
    client = MicroStrategyClient(config, MicroStrategyReport())
    captured_path: Optional[str] = None

    def fake_get_json(path: str, project_id: Optional[str] = None) -> Dict[str, Any]:
        nonlocal captured_path
        captured_path = path
        assert project_id == "project-1"
        return {
            "datasources": [
                {
                    "id": "source-1",
                    "name": "Enterprise Warehouse",
                    "database": {
                        "type": "snow_flake",
                        "version": "snowflake_1x",
                    },
                    "dbms": {"name": "Snowflake"},
                }
            ]
        }

    monkeypatch.setattr(client, "_get_json", fake_get_json)

    datasources = client.list_datasources("project-1")

    assert captured_path == "/api/datasources"
    assert len(datasources) == 1
    assert datasources[0].database_type == "snow_flake"


def test_list_project_datasources_uses_project_scoped_endpoint(
    monkeypatch: MonkeyPatch,
) -> None:
    config = MicroStrategyConfig.model_validate(
        {"base_url": "https://mstr.example.com/MicroStrategyLibrary"}
    )
    client = MicroStrategyClient(config, MicroStrategyReport())
    captured_path: Optional[str] = None

    def fake_get_json(path: str, project_id: Optional[str] = None) -> Dict[str, Any]:
        nonlocal captured_path
        captured_path = path
        assert project_id == "project-1"
        return {
            "datasources": [
                {
                    "id": "source-1",
                    "name": "Sales Warehouse",
                    "database": {
                        "type": "snow_flake",
                        "connection": {"id": "conn-1"},
                    },
                    "dbms": {"name": "Snowflake"},
                }
            ]
        }

    monkeypatch.setattr(client, "_get_json", fake_get_json)

    datasources = client.list_project_datasources("project-1")

    assert captured_path == "/api/projects/project-1/datasources"
    assert datasources[0].connection_id == "conn-1"


def test_get_datasource_connection_parses_database_schema_without_raw_string(
    monkeypatch: MonkeyPatch,
) -> None:
    config = MicroStrategyConfig.model_validate(
        {"base_url": "https://mstr.example.com/MicroStrategyLibrary"}
    )
    client = MicroStrategyClient(config, MicroStrategyReport())

    monkeypatch.setattr(
        client,
        "_get_json",
        lambda path, project_id=None: {
            "id": "conn-1",
            "name": "Sales Warehouse Connection",
            "driverType": "odbc",
            "database": {"type": "snow_flake"},
            "connectionString": "DATABASE=SALES_DB;SCHEMA=SALES;UID=metadata_reader",
        },
    )

    connection = client.get_datasource_connection("conn-1", project_id="project-1")

    assert connection.connection_string_present is True
    assert connection.database_name == "SALES_DB"
    assert connection.schema_name == "SALES"
    assert "connectionString" not in connection.model_dump()


def test_get_dossier_datasets_sql_extracts_dataset_rows(
    monkeypatch: MonkeyPatch,
) -> None:
    config = MicroStrategyConfig.model_validate(
        {"base_url": "https://mstr.example.com/MicroStrategyLibrary"}
    )
    client = MicroStrategyClient(config, MicroStrategyReport())

    def fake_get_json(
        path: str,
        project_id: Optional[str] = None,
        timeout_seconds: Optional[int] = None,
    ) -> Dict[str, Any]:
        return {
            "result": [
                {
                    "id": "ds-1",
                    "name": "Sales Cube",
                    "sqlStatement": "select * from SALES_DB.SALES.fact_sales",
                }
            ]
        }

    monkeypatch.setattr(client, "_get_json", fake_get_json)

    rows = client.get_dossier_datasets_sql("project-1", "dash-1", "instance-1")

    assert rows == [
        {
            "id": "ds-1",
            "name": "Sales Cube",
            "sqlStatement": "select * from SALES_DB.SALES.fact_sales",
        }
    ]


def test_report_instance_lifecycle_uses_v2_report_endpoints(
    monkeypatch: MonkeyPatch,
) -> None:
    config = MicroStrategyConfig.model_validate(
        {"base_url": "https://mstr.example.com/MicroStrategyLibrary"}
    )
    client = MicroStrategyClient(config, MicroStrategyReport())
    calls: list[Dict[str, Any]] = []

    def fake_get_json(
        path: str,
        project_id: Optional[str] = None,
        params: Optional[Dict[str, Any]] = None,
        method: str = "GET",
        json: Optional[Dict[str, Any]] = None,
        timeout_seconds: Optional[int] = None,
    ) -> Dict[str, Any]:
        calls.append(
            {
                "path": path,
                "project_id": project_id,
                "params": params,
                "method": method,
                "json": json,
                "timeout_seconds": timeout_seconds,
            }
        )
        return {"instanceId": "instance-1"}

    monkeypatch.setattr(client, "_get_json", fake_get_json)

    instance_id = client.create_report_instance("project-1", "report-1")

    assert instance_id == "instance-1"
    assert calls == [
        {
            "path": "/api/v2/reports/report-1/instances",
            "project_id": "project-1",
            "params": {"executionStage": "resolve_prompts"},
            "method": "POST",
            "json": {},
            "timeout_seconds": config.warehouse_lineage_sql_timeout_seconds,
        }
    ]


def test_extract_search_id_from_metadata_search_response() -> None:
    assert MicroStrategyClient._extract_search_id({"id": "search-1"}) == "search-1"
    assert (
        MicroStrategyClient._extract_search_id({"result": {"searchId": "search-2"}})
        == "search-2"
    )


def test_get_json_reports_non_json_response(monkeypatch: MonkeyPatch) -> None:
    config = MicroStrategyConfig.model_validate(
        {"base_url": "https://mstr.example.com/MicroStrategyLibrary"}
    )
    report = MicroStrategyReport()
    client = MicroStrategyClient(config, report)
    monkeypatch.setattr(
        client, "_request", lambda *args, **kwargs: FakeNonJsonResponse()
    )

    try:
        client._get_json("/api/projects")
    except MicroStrategyAPIError as error:
        assert "non-JSON response" in str(error)
        assert "GET /api/projects" in str(error)
    else:
        raise AssertionError("Expected MicroStrategyAPIError")

    assert report.api_errors == 1


def test_request_retries_transient_500_then_succeeds(
    monkeypatch: MonkeyPatch,
) -> None:
    client, report = _make_client()
    responses = [StatusResponse(500), StatusResponse(200)]
    call_count = 0

    def fake_request(**kwargs: Any) -> StatusResponse:
        nonlocal call_count
        call_count += 1
        return responses[call_count - 1]

    monkeypatch.setattr(client.session, "request", fake_request)

    with mock.patch("time.sleep") as fake_sleep:
        response = client._request("GET", "/api/projects")

    assert response.status_code == 200
    assert call_count == 2
    assert fake_sleep.call_count == 1
    assert report.api_errors == 0


def test_request_raises_after_exhausting_retries_on_persistent_503(
    monkeypatch: MonkeyPatch,
) -> None:
    client, report = _make_client({"max_retries": 2})
    call_count = 0

    def fake_request(**kwargs: Any) -> StatusResponse:
        nonlocal call_count
        call_count += 1
        return StatusResponse(503)

    monkeypatch.setattr(client.session, "request", fake_request)

    with mock.patch("time.sleep"), pytest.raises(MicroStrategyAPIError):
        client._request("GET", "/api/projects")

    assert call_count == 3
    assert report.api_errors == 1


def test_request_fails_fast_on_non_retryable_401(monkeypatch: MonkeyPatch) -> None:
    client, report = _make_client()
    call_count = 0

    def fake_request(**kwargs: Any) -> StatusResponse:
        nonlocal call_count
        call_count += 1
        return StatusResponse(401)

    monkeypatch.setattr(client.session, "request", fake_request)

    with mock.patch("time.sleep") as fake_sleep, pytest.raises(MicroStrategyAPIError):
        client._request("GET", "/api/projects")

    assert call_count == 1
    assert fake_sleep.call_count == 0
    assert report.api_errors == 1


def test_retry_delay_honors_retry_after_header() -> None:
    response = mock.Mock(headers={"Retry-After": "7"})
    assert MicroStrategyClient._retry_delay(response, attempt=0) == 7.0

    response_without_header = mock.Mock(headers={})
    assert MicroStrategyClient._retry_delay(response_without_header, attempt=2) == 4.0


def test_metadata_search_paginates_until_total_items(
    monkeypatch: MonkeyPatch,
) -> None:
    client, _report = _make_client({"page_size": 2})
    pages: Dict[int, List[Dict[str, Any]]] = {
        0: [{"id": "dash-1", "name": "D1"}, {"id": "dash-2", "name": "D2"}],
        2: [{"id": "dash-3", "name": "D3"}, {"id": "dash-4", "name": "D4"}],
        4: [{"id": "dash-5", "name": "D5"}],
    }
    requested_offsets: List[int] = []

    def fake_get_json(
        path: str,
        project_id: Optional[str] = None,
        params: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        assert params is not None
        requested_offsets.append(params["offset"])
        return {"result": pages[params["offset"]], "totalItems": 5}

    monkeypatch.setattr(client, "_get_json", fake_get_json)

    items = list(client._metadata_search(project_id="project-1"))

    assert len(items) == 5
    assert requested_offsets == [0, 2, 4]


def test_login_without_auth_token_header_raises(monkeypatch: MonkeyPatch) -> None:
    client, _report = _make_client()
    monkeypatch.setattr(
        client.session,
        "request",
        lambda **kwargs: StatusResponse(204),
    )

    with pytest.raises(MicroStrategyAPIError):
        client.login()


def test_password_login_sends_standard_login_mode_and_secret(
    monkeypatch: MonkeyPatch,
) -> None:
    client, _report = _make_client(
        {
            "auth": {
                "type": "password",
                "username": "metadata-reader",
                "password": "secret",
            }
        }
    )
    captured_json: Optional[Dict[str, Any]] = None

    def fake_request(**kwargs: Any) -> FakeResponse:
        nonlocal captured_json
        captured_json = kwargs.get("json")
        return FakeResponse()

    monkeypatch.setattr(client.session, "request", fake_request)

    client.login()

    assert captured_json == {
        "loginMode": 1,
        "username": "metadata-reader",
        "password": "secret",
    }


def test_search_dashboards_skips_malformed_objects(monkeypatch: MonkeyPatch) -> None:
    client, report = _make_client()
    monkeypatch.setattr(
        client,
        "_get_json",
        lambda path, project_id=None, params=None: {
            "result": [
                {"id": "dash-1", "name": "Dashboard 1"},
                {"name": "no id"},
                {"id": "dash-2", "name": "Dashboard 2"},
            ]
        },
    )

    dashboards = list(client.search_dashboards(project_id="project-1"))

    assert [dashboard.id for dashboard in dashboards] == ["dash-1", "dash-2"]
    assert len(report.malformed_objects_skipped) == 1


@pytest.mark.parametrize(
    "method_name",
    [
        "create_dossier_instance",
        "create_document_instance",
        "create_report_instance",
    ],
)
def test_instance_creation_without_instance_id_raises(
    monkeypatch: MonkeyPatch,
    method_name: str,
) -> None:
    client, _report = _make_client()
    monkeypatch.setattr(client, "_get_json", lambda *args, **kwargs: {})

    with pytest.raises(MicroStrategyAPIError):
        getattr(client, method_name)("project-1", "object-1")
