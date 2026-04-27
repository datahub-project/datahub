import json
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from datahub.ingestion.source.hex_v2.cli_client import (
    HexCliClient,
    HexCliReport,
    _parse_project_get,
    _parse_run_record,
)
from datahub.ingestion.source.hex_v2.model import Component, Project
from tests.unit.hex_v2.conftest import load_fixture


def _mock_result(stdout: str, returncode: int = 0) -> MagicMock:
    r = MagicMock()
    r.returncode = returncode
    r.stdout = stdout
    r.stderr = ""
    return r


@pytest.fixture
def report() -> HexCliReport:
    return HexCliReport()


@pytest.fixture
def client(report: HexCliReport) -> HexCliClient:
    c = HexCliClient(
        token="test-token",
        workspace_name="test-workspace",
        report=report,
        hex_cli_path="hex",
        page_size=25,
    )
    c._authed = True  # skip actual auth in unit tests
    return c


class TestParseProjectGet:
    def test_parses_project_fields(self) -> None:
        data = load_fixture("project_get.json")
        result = _parse_project_get(data)

        assert isinstance(result, Project)
        assert result.id == "019db1be-8dd0-7001-9326-5588f745dfe8"
        assert result.title == "Assertions"
        assert result.description == "Customer-facing assertions view for CSMs/AEs."
        assert result.status is not None
        assert result.status.name == "In Progress"
        assert result.categories is not None
        assert len(result.categories) == 1
        assert result.categories[0].name == "Internal"
        assert result.creator is not None
        assert result.creator.email == "julian.martinez@acryl.io"
        assert result.analytics is not None
        assert result.analytics.appviews_all_time == 131
        assert result.analytics.appviews_last_7_days == 3
        assert result.last_published_at is not None

    def test_parses_component_type(self) -> None:
        data = load_fixture("project_get_component.json")
        result = _parse_project_get(data)

        assert isinstance(result, Component)
        assert result.id == "aabbccdd-1234-5678-9012-abcdef012345"
        assert result.status is None
        assert result.analytics is None
        assert result.last_published_at is None

    def test_null_status_and_empty_categories(self) -> None:
        data: dict[str, Any] = {
            "id": "proj-1",
            "title": "Test",
            "description": None,
            "type": "PROJECT",
            "status": None,
            "categories": [],
            "creator": None,
            "owner": None,
            "analytics": None,
            "createdAt": None,
            "lastEditedAt": None,
            "lastPublishedAt": None,
        }
        result = _parse_project_get(data)

        assert isinstance(result, Project)
        assert result.status is None
        assert result.categories == []


class TestParseRunRecord:
    def test_parses_run(self) -> None:
        data = load_fixture("runs.json")
        run = _parse_run_record(data["runs"][0])

        assert run is not None
        assert run.run_id == "019dce2f-6359-7000-b82f-e58eaa9144d7"
        assert run.status == "COMPLETED"
        assert run.elapsed_seconds == 63
        assert run.start_time.year == 2026

    def test_missing_start_time_returns_none(self) -> None:
        run = _parse_run_record(
            {"run_id": "x", "status": "COMPLETED", "start_time": None}
        )

        assert run is None


class TestHexCliClientListProjects:
    def test_yields_projects(self, client: HexCliClient, report: HexCliReport) -> None:
        fixture = load_fixture("projects_list.json")

        with patch("subprocess.run", return_value=_mock_result(json.dumps(fixture))):
            projects = list(client.list_projects())

        assert len(projects) == 2
        assert projects[0].title == "Assertions"
        assert report.list_projects_items == 2

    def test_paginates(self, client: HexCliClient) -> None:
        page1 = {
            "projects": [
                {"id": "p1", "title": "P1", "updated_at": "2026-01-01T00:00:00.000Z"}
            ],
            "pagination": {"after": "cursor-abc", "before": None},
        }
        page2 = {
            "projects": [
                {"id": "p2", "title": "P2", "updated_at": "2026-01-02T00:00:00.000Z"}
            ],
            "pagination": {"after": None, "before": None},
        }

        results = []
        with patch("subprocess.run") as mock_run:
            mock_run.side_effect = [
                _mock_result(json.dumps(page1)),
                _mock_result(json.dumps(page2)),
            ]
            results = list(client.list_projects())

        assert len(results) == 2
        assert mock_run.call_count == 2

    def test_cli_failure_emits_failure_and_stops(
        self, client: HexCliClient, report: HexCliReport
    ) -> None:
        with patch("subprocess.run", return_value=_mock_result("error", returncode=1)):
            projects = list(client.list_projects())

        assert projects == []
        assert report.failures

    def test_json_parse_error_emits_failure(
        self, client: HexCliClient, report: HexCliReport
    ) -> None:
        with patch("subprocess.run", return_value=_mock_result("not-json")):
            projects = list(client.list_projects())

        assert projects == []
        assert report.failures


class TestHexCliClientListConnections:
    def test_returns_connection_map(self, client: HexCliClient) -> None:
        fixture = load_fixture("connections.json")

        with patch("subprocess.run", return_value=_mock_result(json.dumps(fixture))):
            connections = client.list_connections()

        assert len(connections) == 2
        conn = connections["0197f074-6534-7006-8544-a0ece1dd0c3c"]
        assert conn.name == "Analytics Hub"
        assert conn.connection_type == "snowflake"

    def test_cli_failure_returns_empty_dict(
        self, client: HexCliClient, report: HexCliReport
    ) -> None:
        with patch(
            "subprocess.run", return_value=_mock_result("forbidden", returncode=1)
        ):
            connections = client.list_connections()

        assert connections == {}
        assert report.warnings


class TestHexCliClientExportProject:
    def test_returns_yaml_content(self, client: HexCliClient, tmp_path: Any) -> None:
        yaml_content = "schemaVersion: 3\nmeta:\n  projectId: proj-1\n"

        def fake_run(cmd, **kwargs):  # type: ignore[no-untyped-def]
            output_path = cmd[cmd.index("-o") + 1]
            with open(output_path, "w") as f:
                f.write(yaml_content)
            r = MagicMock()
            r.returncode = 0
            r.stdout = f"✓ Exported proj-1 to {output_path}"
            r.stderr = ""
            return r

        with patch("subprocess.run", side_effect=fake_run):
            result = client.export_project_yaml("proj-1")

        assert result == yaml_content

    def test_cli_failure_returns_none(
        self, client: HexCliClient, report: HexCliReport
    ) -> None:
        with patch(
            "subprocess.run", return_value=_mock_result("hex 1.2.2", returncode=0)
        ):
            # returncode=0 but no file written → should warn and return None
            result = client.export_project_yaml("proj-missing")

        assert result is None
        assert report.export_project_failures == 1


class TestHexCliClientGetLatestRun:
    def test_returns_latest_run(self, client: HexCliClient) -> None:
        fixture = load_fixture("runs.json")

        with patch("subprocess.run", return_value=_mock_result(json.dumps(fixture))):
            run = client.get_latest_run("proj-1")

        assert run is not None
        assert run.run_id == "019dce2f-6359-7000-b82f-e58eaa9144d7"
        assert run.status == "COMPLETED"

    def test_exit_1_returns_none(self, client: HexCliClient) -> None:
        with patch(
            "subprocess.run",
            return_value=_mock_result("An unknown error occurred.", returncode=1),
        ):
            run = client.get_latest_run("proj-no-runs")

        assert run is None

    def test_empty_runs_list_returns_none(self, client: HexCliClient) -> None:
        with patch("subprocess.run", return_value=_mock_result('{"runs": []}')):
            run = client.get_latest_run("proj-1")

        assert run is None
