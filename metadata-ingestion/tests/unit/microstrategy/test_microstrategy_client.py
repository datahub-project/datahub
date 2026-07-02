from typing import Any, Dict, Optional

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


def test_list_datasource_connections_does_not_preserve_connection_string(
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
            "connections": [
                {
                    "id": "conn-1",
                    "name": "Snowflake Connection",
                    "driverType": "odbc",
                    "database": {"type": "snow_flake"},
                    "connectionString": "redacted-by-test",
                }
            ]
        },
    )

    connections = client.list_datasource_connections("project-1")

    assert connections[0].connection_string_present is True
    assert "connectionString" not in connections[0].model_dump()


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
