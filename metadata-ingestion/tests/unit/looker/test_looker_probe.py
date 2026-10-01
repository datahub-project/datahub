import json
from typing import Any, Dict
from unittest import mock

import looker_sdk.rtl.requests_transport as looker_requests_transport
import pytest
from looker_sdk.error import SDKError
from looker_sdk.sdk.api40.models import DashboardElement, LookWithQuery, Query

from datahub.ingestion.agent.probe_methods import list_probe_methods, run_probe_method
from datahub.ingestion.agent.verdicts import ProbeConnectionError, ProbeReadFailed
from datahub.ingestion.source.looker.looker_config import LookerDashboardSourceConfig
from datahub.ingestion.source.looker.looker_probe import (
    LookerMetadataProbe,
    _ingestion_can_read,
    sdk_error_status,
)
from datahub.ingestion.source.looker.looker_source import (
    BASIC_INGEST_REQUIRED_PERMISSIONS,
    looker_folder_path,
)
from tests.unit.looker.looker_probe_fixtures import (
    API_USER_EMAIL,
    CLIENT_ID,
    CLIENT_SECRET,
    PERSONAL_FOLDER_NAME,
    fake_looker,
    install,
    recipe,
    sdk_error,
)


def _run(command: str, params: Dict[str, Any], **overrides: Any) -> Dict[str, Any]:
    with fake_looker():
        return run_probe_method("looker", recipe(**overrides), command, params).to_dict()


def test_building_the_provider_opens_no_connection() -> None:
    with mock.patch("looker_sdk.init40") as init40:
        probe = LookerMetadataProbe.for_config(
            LookerDashboardSourceConfig.model_validate(recipe())
        )
        with probe:
            pass
    init40.assert_not_called()


def test_exit_closes_the_sdk_session_once_a_command_opened_it() -> None:
    client = install(mock.MagicMock())
    transport = mock.create_autospec(
        looker_requests_transport.RequestsTransport, instance=True
    )
    transport.session = mock.MagicMock()
    client.transport = transport
    with mock.patch("looker_sdk.init40", return_value=client):
        run_probe_method("looker", recipe(), "permissions", {})
    transport.session.close.assert_called_once()


@pytest.mark.parametrize(
    "error",
    [sdk_error(401, f"bad {CLIENT_SECRET}"), SDKError(f"raw body {CLIENT_SECRET}")],
)
def test_refused_credentials_name_the_host_and_nothing_the_sdk_said(
    error: SDKError,
) -> None:
    client = install(mock.MagicMock())
    client.me.side_effect = error
    with (
        mock.patch("looker_sdk.init40", return_value=client),
        pytest.raises(ProbeConnectionError) as raised,
    ):
        run_probe_method("looker", recipe(), "permissions", {})
    text = str(raised.value)
    assert "looker.example.com" in text
    assert CLIENT_SECRET not in text and "raw body" not in text
    assert CLIENT_ID not in text


def test_a_base_url_carrying_userinfo_is_refused_without_echoing_it() -> None:
    with fake_looker(), pytest.raises(ValueError) as raised:
        run_probe_method(
            "looker",
            recipe(base_url="https://someone:hunter22@looker.example.com"),
            "permissions",
            {},
        )
    assert "hunter22" not in str(raised.value)


def test_sdk_error_status_reads_the_documentation_url_then_the_message() -> None:
    assert sdk_error_status(sdk_error(404)) == 404
    assert sdk_error_status(SDKError("Looker Not Found (404)")) == 404
    assert sdk_error_status(SDKError("no status here")) is None


def test_a_read_failure_reports_the_status_and_not_the_sdk_text() -> None:
    client = install(mock.MagicMock())
    client.user_roles.side_effect = sdk_error(500, f"boom {CLIENT_SECRET}")
    with (
        mock.patch("looker_sdk.init40", return_value=client),
        pytest.raises(ProbeReadFailed) as raised,
    ):
        run_probe_method("looker", recipe(), "permissions", {})
    assert "HTTP 500" in str(raised.value)
    assert CLIENT_SECRET not in str(raised.value) and "boom" not in str(raised.value)


def test_permissions_report_what_ingestion_needs_and_lacks() -> None:
    result = _run("permissions", {})["result"]
    assert result["granted"] == ["access_data", "explore", "see_lookml", "see_looks"]
    assert "see_users" in result["missing_for_metadata"]
    assert set(result["missing_for_metadata"]) <= BASIC_INGEST_REQUIRED_PERMISSIONS
    assert result["missing_for_usage"] == ["see_system_activity"]


def test_unreadable_roles_degrade_to_a_warning() -> None:
    client = install(mock.MagicMock())
    client.user_roles.side_effect = sdk_error(403)
    with mock.patch("looker_sdk.init40", return_value=client):
        run = run_probe_method("looker", recipe(), "permissions", {})
    assert run.result == {
        "granted": None,
        "missing_for_metadata": None,
        "missing_for_usage": None,
    }
    assert any("HTTP 403" in w for w in run.warnings)


def test_no_command_output_carries_credentials_or_the_api_user() -> None:
    dumped = json.dumps(_run("permissions", {}))
    for secret in (CLIENT_ID, CLIENT_SECRET, API_USER_EMAIL, '"7"'):
        assert secret not in dumped


def test_methods_are_advertised_with_their_kinds() -> None:
    kinds = {spec.command: spec.kind for spec in list_probe_methods("looker")}
    assert kinds["permissions"] is None
    assert kinds["dashboards"] == "Dashboard"
    assert kinds["charts"] == "Look"

def test_folder_path_joins_ancestors_and_the_folder() -> None:
    assert looker_folder_path(["Shared"], "Sales") == "Shared/Sales"
    assert looker_folder_path([], "Shared") == "Shared"


def test_dashboards_list_live_and_deleted_with_their_folder_facts() -> None:
    run = _run("dashboards", {}, folder_path_pattern={"deny": ["^Users/"]})
    by_id = {r["name"]: r for r in run["result"]}
    assert run["kind"] == "Dashboard"
    assert set(by_id) == {"1", "2", "3", "4", "5", "6"}
    assert by_id["1"] == {
        "name": "1",
        "title": "Revenue",
        "deleted": False,
        "folder_path": "Shared/Sales",
        "folder_personal": False,
        "folder_path_allowed": True,
    }
    assert by_id["4"]["deleted"] is True
    assert by_id["6"]["folder_path"] is None
    assert by_id["6"]["folder_path_allowed"] is None


def test_a_personal_folder_path_is_withheld_but_still_judged() -> None:
    run = _run("dashboards", {}, folder_path_pattern={"deny": ["^Users/"]})
    personal = next(r for r in run["result"] if r["name"] == "3")
    assert personal["folder_path"] is None
    assert personal["folder_personal"] is True
    assert personal["folder_path_allowed"] is False
    assert PERSONAL_FOLDER_NAME not in json.dumps(run)


def test_unreadable_ancestors_degrade_as_ingestion_does() -> None:
    client = install(mock.MagicMock())
    client.folder_ancestors.side_effect = sdk_error(500)
    with mock.patch("looker_sdk.init40", return_value=client):
        run = run_probe_method("looker", recipe(), "dashboards", {})
    first = next(r for r in run.result if r["name"] == "1")
    assert first["folder_path"] == "Sales"
    assert any("ancestors" in w for w in run.warnings)


def test_a_refused_deleted_listing_keeps_the_live_ones_and_warns() -> None:
    client = install(mock.MagicMock())
    client.search_dashboards.side_effect = sdk_error(403)
    with mock.patch("looker_sdk.init40", return_value=client):
        run = run_probe_method("looker", recipe(), "dashboards", {})
    assert {r["name"] for r in run.result} == {"1", "2", "3", "5", "6"}
    assert any("deleted dashboard listing returned HTTP 403" in w for w in run.warnings)


def test_the_limit_bounds_folder_lookups_and_skips_the_deleted_listing() -> None:
    client = install(mock.MagicMock())
    with mock.patch("looker_sdk.init40", return_value=client):
        run = run_probe_method("looker", recipe(), "dashboards", {"limit": 1})
    assert [r["name"] for r in run.result] == ["1"]
    assert run.truncated is True
    client.search_dashboards.assert_not_called()
    # Dashboards 1 and 2 share f-sales: one lookup, cached.
    assert client.folder_ancestors.call_count == 1


def test_charts_report_the_facts_ingestion_reads_from_each_element() -> None:
    run = _run("charts", {"dashboard": "1"})
    assert run["kind"] == "Look"
    assert run["parent_path"] == ["1"]
    by_id = {r["name"]: r for r in run["result"]}
    assert by_id["11"] == {
        "name": "11",
        "title": "chart 11",
        "type": "vis",
        "has_query": True,
        "look_id": None,
        "model": "sales",
        "explore": "orders",
    }
    assert by_id["12"]["type"] == "text"
    assert by_id["12"]["has_query"] is False


def test_charts_of_an_unknown_dashboard_are_a_bad_argument() -> None:
    with fake_looker(), pytest.raises(ValueError, match="no dashboard with id"):
        run_probe_method("looker", recipe(), "charts", {"dashboard": "999"})


def test_charts_warn_when_ingestion_drops_their_dashboard() -> None:
    personal = _run("charts", {"dashboard": "3"}, skip_personal_folders=True)
    assert any("skip_personal_folders" in w for w in personal["warnings"])
    archived = _run(
        "charts", {"dashboard": "5"}, folder_path_pattern={"deny": ["^Shared/Archive"]}
    )
    assert any("folder_path_pattern" in w for w in archived["warnings"])
    deleted = _run("charts", {"dashboard": "4"})
    assert any("include_deleted" in w for w in deleted["warnings"])
    assert PERSONAL_FOLDER_NAME not in json.dumps(personal)


def test_an_element_whose_look_has_no_query_is_unreadable_to_ingestion() -> None:
    assert _ingestion_can_read(DashboardElement(id="1", query=Query(model="m", view="v")))
    assert not _ingestion_can_read(DashboardElement(id="2", look=LookWithQuery()))
    assert not _ingestion_can_read(DashboardElement(id="3"))
