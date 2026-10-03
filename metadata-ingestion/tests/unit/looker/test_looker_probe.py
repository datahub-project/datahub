import json
from typing import Any, Dict, List
from unittest import mock

import looker_sdk.rtl.requests_transport as looker_requests_transport
import pytest
from looker_sdk.error import SDKError
from looker_sdk.sdk.api40.models import (
    Dashboard,
    DashboardElement,
    FolderBase,
    LookWithQuery,
    Query,
)

from datahub.ingestion.agent.probe_methods import (
    ProbeMethodResult,
    list_probe_methods,
    run_probe_method,
)
from datahub.ingestion.agent.verdicts import (
    ProbeArgumentError,
    ProbeConnectionError,
    ProbeReadFailed,
)
from datahub.ingestion.source.looker.looker_config import LookerDashboardSourceConfig
from datahub.ingestion.source.looker.looker_probe import (
    LookerMetadataProbe,
    sdk_error_status,
)
from datahub.ingestion.source.looker.looker_selection import element_has_query
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

_LOOKS_ON: Dict[str, Any] = {
    "extract_independent_looks": True,
    "stateful_ingestion": {"enabled": True},
}


def _rows(run: ProbeMethodResult) -> List[Dict[str, Any]]:
    assert isinstance(run.result, list)
    return run.result


def _run(command: str, params: Dict[str, Any], **overrides: Any) -> Dict[str, Any]:
    with fake_looker():
        return run_probe_method(
            "looker", recipe(**overrides), command, params
        ).to_dict()


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


def test_a_base_url_carrying_userinfo_tells_the_caller_what_to_remove() -> None:
    with (
        fake_looker(),
        pytest.raises(ProbeArgumentError, match="remove it from base_url"),
    ):
        run_probe_method(
            "looker",
            recipe(base_url="https://someone:hunter22@looker.example.com"),
            "permissions",
            {},
        )


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
    first = next(r for r in _rows(run) if r["name"] == "1")
    # Without its ancestors a folder's path is just its own name, which for a
    # personal folder is its user's name: withheld, with the verdict kept.
    assert first["folder_path"] is None
    assert first["folder_path_allowed"] is True
    assert any("ancestors" in w for w in run.warnings)


def test_a_refused_deleted_listing_keeps_the_live_ones_and_warns() -> None:
    client = install(mock.MagicMock())
    client.search_dashboards.side_effect = sdk_error(403)
    with mock.patch("looker_sdk.init40", return_value=client):
        run = run_probe_method("looker", recipe(), "dashboards", {})
    assert {r["name"] for r in _rows(run)} == {"1", "2", "3", "5", "6"}
    assert any("deleted dashboard listing returned HTTP 403" in w for w in run.warnings)


def test_the_limit_bounds_folder_lookups_and_skips_the_deleted_listing() -> None:
    client = install(mock.MagicMock())
    with mock.patch("looker_sdk.init40", return_value=client):
        run = run_probe_method("looker", recipe(), "dashboards", {"limit": 1})
    assert [r["name"] for r in _rows(run)] == ["1"]
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
        "dashboard_deleted": False,
        "dashboard_folder_path": "Shared/Sales",
        "dashboard_folder_personal": False,
        "dashboard_folder_path_allowed": True,
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
    assert element_has_query(DashboardElement(id="1", query=Query(model="m", view="v")))
    assert not element_has_query(DashboardElement(id="2", look=LookWithQuery()))
    assert not element_has_query(DashboardElement(id="3"))


def test_looks_list_live_and_deleted_with_the_facts_ingestion_skips_on() -> None:
    run = _run("looks", {}, **_LOOKS_ON)
    assert run["kind"] == "Look"
    assert run["parent_path"] == []
    by_id = {r["name"]: r for r in run["result"]}
    assert set(by_id) == {"101", "102", "103", "104", "105", "106"}
    assert by_id["102"]["folder_personal"] is True
    assert by_id["103"]["has_query"] is False
    # A query id whose look reads back without a query is no query either.
    assert by_id["106"]["has_query"] is False
    assert by_id["101"]["has_query"] is True
    assert by_id["104"]["deleted"] is True
    assert PERSONAL_FOLDER_NAME not in json.dumps(run)
    assert run["warnings"] == []


def test_looks_say_ingestion_emits_none_without_the_switch() -> None:
    run = _run("looks", {})
    assert any("extract_independent_looks" in w for w in run["warnings"])


def test_models_and_their_explores() -> None:
    models = _run("models", {})
    assert models["kind"] == "LookML Model"
    assert models["result"] == [
        {"name": "sales", "project": "proj", "explore_count": 4},
        {"name": "empty", "project": "proj", "explore_count": 0},
    ]
    explores = _run("explores", {"model": "sales"})
    assert explores["kind"] == "Explore"
    assert explores["parent_path"] == ["sales"]
    assert explores["result"] == [
        {"name": "orders", "hidden": False},
        {"name": "customers", "hidden": False},
        {"name": "archived", "hidden": False},
        {"name": "unused", "hidden": True},
    ]


def test_explores_of_an_unknown_model_are_a_bad_argument() -> None:
    with fake_looker(), pytest.raises(ValueError, match="no LookML model named"):
        run_probe_method("looker", recipe(), "explores", {"model": "nope"})


def test_a_refused_model_listing_is_empty_with_a_warning_not_a_bad_argument() -> None:
    client = install(mock.MagicMock())
    client.all_lookml_models.side_effect = sdk_error(403)
    with mock.patch("looker_sdk.init40", return_value=client):
        run = run_probe_method("looker", recipe(), "explores", {"model": "sales"})
    assert run.result == []
    assert any("HTTP 403" in w for w in run.warnings)


def test_methods_are_advertised_with_their_kinds() -> None:
    kinds = {spec.command: spec.kind for spec in list_probe_methods("looker")}
    assert kinds == {
        "permissions": None,
        "dashboards": "Dashboard",
        "charts": "Look",
        "looks": "Look",
        "models": "LookML Model",
        "explores": "Explore",
    }


def test_no_listing_carries_credentials_users_or_personal_folders() -> None:
    for command, params in (
        ("dashboards", {}),
        ("charts", {"dashboard": "3"}),
        ("looks", {}),
        ("models", {}),
        ("explores", {"model": "sales"}),
        ("explores", {"model": "sales", "trace_charts": True}),
        ("looks", {"trace_charts": True}),
        ("permissions", {}),
    ):
        dumped = json.dumps(_run(command, params, **_LOOKS_ON))
        # '"7"' is the API user's id in the fake, as a JSON string.
        for withheld in (
            CLIENT_ID,
            CLIENT_SECRET,
            API_USER_EMAIL,
            PERSONAL_FOLDER_NAME,
            '"7"',
        ):
            assert withheld not in dumped, (command, withheld)


def _used(command: str, params: Dict[str, Any], **overrides: Any) -> Dict[str, Any]:
    run = _run(command, {**params, "trace_charts": True}, **overrides)
    return {r["name"]: r["used"] for r in run["result"]}


def test_traced_explores_mark_what_kept_charts_and_looks_query() -> None:
    assert _used("explores", {"model": "sales"}) == {
        "orders": True,
        "customers": True,
        "archived": True,
        "unused": False,
    }
    # The only chart querying `archived` is dropped, or its dashboard is.
    assert (
        _used("explores", {"model": "sales"}, chart_pattern={"deny": ["^51$"]})[
            "archived"
        ]
        is False
    )
    assert (
        _used("explores", {"model": "sales"}, dashboard_pattern={"deny": ["^5$"]})[
            "archived"
        ]
        is False
    )
    # folder_path_pattern drops the dashboard only after its explores count.
    assert (
        _used(
            "explores",
            {"model": "sales"},
            folder_path_pattern={"deny": ["^Shared/Archive"]},
        )["archived"]
        is True
    )


def test_traced_models_are_used_when_any_of_their_explores_is() -> None:
    assert _used("models", {}) == {"sales": True, "empty": False}


def test_standalone_looks_count_toward_use_only_when_extracted() -> None:
    client = install(mock.MagicMock())
    # Everything on dashboards is dropped, so only look 101's query can count.
    client.look.side_effect = lambda look_id, fields=None, transport_options=None: (
        LookWithQuery(query=Query(model="sales", view="unused", fields=["unused.id"]))
    )
    config = recipe(dashboard_pattern={"deny": [".*"]})
    with mock.patch("looker_sdk.init40", return_value=client):
        off = run_probe_method(
            "looker", config, "explores", {"model": "sales", "trace_charts": True}
        )
        on = run_probe_method(
            "looker",
            {**config, **_LOOKS_ON},
            "explores",
            {"model": "sales", "trace_charts": True},
        )
    assert {r["name"]: r["used"] for r in _rows(off)}["unused"] is False
    assert {r["name"]: r["used"] for r in _rows(on)}["unused"] is True


def test_traced_looks_mark_the_ones_on_a_kept_dashboard() -> None:
    run = _run("looks", {"trace_charts": True}, **_LOOKS_ON)
    on_dashboard = {r["name"]: r["on_kept_dashboard"] for r in run["result"]}
    assert on_dashboard["105"] is True
    assert on_dashboard["101"] is False


def test_an_interrupted_trace_leaves_unfound_use_undetermined() -> None:
    client = install(mock.MagicMock())
    client.all_dashboards.side_effect = sdk_error(403)
    with mock.patch("looker_sdk.init40", return_value=client):
        run = run_probe_method(
            "looker", recipe(), "explores", {"model": "sales", "trace_charts": True}
        )
    assert {r["name"]: r["used"] for r in _rows(run)} == {
        "orders": None,
        "customers": None,
        "archived": None,
        "unused": None,
    }
    assert any("HTTP 403" in w for w in run.warnings)
    assert any("undetermined" in w for w in run.warnings)


def test_the_trace_stops_at_its_bound_and_says_so() -> None:
    with (
        fake_looker(),
        mock.patch(
            "datahub.ingestion.source.looker.looker_probe._TRACE_FETCH_LIMIT", 1
        ),
    ):
        run = run_probe_method(
            "looker", recipe(), "explores", {"model": "sales", "trace_charts": True}
        )
    used = {r["name"]: r["used"] for r in _rows(run)}
    # Dashboard 1 was read before the bound; what only later ones use is unknown.
    assert used["orders"] is True
    assert used["unused"] is None
    assert any("undetermined" in w for w in run.warnings)


def test_the_trace_reads_no_dashboard_skip_personal_folders_discards() -> None:
    with fake_looker() as client:
        run_probe_method(
            "looker",
            recipe(skip_personal_folders=True),
            "explores",
            {"model": "sales", "trace_charts": True},
        )
    read = {call.kwargs["dashboard_id"] for call in client.dashboard.call_args_list}
    assert "3" not in read
    assert "1" in read
    listing_fields = client.all_dashboards.call_args.kwargs["fields"]
    assert "is_personal_descendant" in listing_fields


def test_untraced_listings_carry_no_use_facts() -> None:
    explores = _run("explores", {"model": "sales"})
    assert all("used" not in r for r in explores["result"])


def test_charts_carry_their_dashboards_facts_without_a_personal_path() -> None:
    run = _run("charts", {"dashboard": "3"}, folder_path_pattern={"deny": ["^Users/"]})
    record = run["result"][0]
    assert record["dashboard_folder_personal"] is True
    assert record["dashboard_folder_path"] is None
    assert record["dashboard_folder_path_allowed"] is False
    assert PERSONAL_FOLDER_NAME not in json.dumps(run)


def test_a_folder_under_the_users_root_is_withheld_even_without_flags() -> None:
    client = install(mock.MagicMock())
    unflagged = FolderBase(id="f-anon", name=PERSONAL_FOLDER_NAME, parent_id="f-users")
    client.all_dashboards.side_effect = lambda fields=None, transport_options=None: [
        Dashboard(id="9", title="Unflagged", folder=unflagged)
    ]
    client.folder_ancestors.side_effect = (
        lambda folder_id, fields=None, transport_options=None: [
            FolderBase(id="f-users", name="Users")
        ]
    )
    config = recipe(folder_path_pattern={"deny": ["^Users/"]})
    with mock.patch("looker_sdk.init40", return_value=client):
        run = run_probe_method("looker", config, "dashboards", {})
    record = next(r for r in _rows(run) if r["name"] == "9")
    assert record["folder_path"] is None
    assert record["folder_path_allowed"] is False
    assert PERSONAL_FOLDER_NAME not in json.dumps(run.to_dict())


def test_listings_ask_for_the_personal_folder_flags() -> None:
    client = install(mock.MagicMock())
    with mock.patch("looker_sdk.init40", return_value=client):
        run_probe_method("looker", recipe(), "dashboards", {})
        run_probe_method("looker", recipe(**_LOOKS_ON), "looks", {})
    for listing in (client.all_dashboards, client.all_looks):
        assert "is_personal_descendant" in listing.call_args.kwargs["fields"]


def test_an_unreadable_look_leaves_the_trace_undetermined_not_failed() -> None:
    client = install(mock.MagicMock())
    client.look.side_effect = TypeError("boom")
    config = recipe(dashboard_pattern={"deny": [".*"]}, **_LOOKS_ON)
    with mock.patch("looker_sdk.init40", return_value=client):
        run = run_probe_method(
            "looker", config, "explores", {"model": "sales", "trace_charts": True}
        )
    assert {r["used"] for r in _rows(run)} == {None}
    assert any("undetermined" in w for w in run.warnings)
    assert not any("boom" in w for w in run.warnings)
