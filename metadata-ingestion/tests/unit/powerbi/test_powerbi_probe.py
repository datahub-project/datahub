from typing import Any, Dict, Iterator, List, Mapping, Optional, Sequence, Tuple
from unittest import mock

import pytest
import requests_mock as rm

from datahub.ingestion.agent.api_gate import ApiScopeError, check_api_request
from datahub.ingestion.agent.filter_check import FilterCheckResult, check_filters
from datahub.ingestion.agent.filter_input import listing_from_run
from datahub.ingestion.agent.probe_methods import list_probe_methods, run_probe_method
from datahub.ingestion.agent.verdicts import ProbeConnectionError
from datahub.ingestion.source.powerbi.config import (
    PowerBiDashboardSourceConfig,
    PowerBiEnvironment,
)
from datahub.ingestion.source.powerbi.powerbi_probe import PowerBiMetadataProbe
from datahub.ingestion.source.powerbi.rest_api_wrapper.data_resolver import (
    DataResolverBase,
)
from datahub.ingestion.source.powerbi.rest_api_wrapper.powerbi_api import (
    groups_filter,
    workspace_from_group,
)


def _mock_msal_cca(*args: Any, **kwargs: Any) -> Any:
    class MsalClient:
        def acquire_token_for_client(self, *args: Any, **kwargs: Any) -> Dict:
            return {"access_token": "dummy"}

    return MsalClient()


@pytest.fixture(autouse=True)
def _patch_msal() -> Iterator[None]:
    with mock.patch("msal.ConfidentialClientApplication", side_effect=_mock_msal_cca):
        yield


def test_no_modified_workspaces_means_no_id_filter_as_ingestion_applies_it() -> None:
    # get_workspaces only sets $filter when the list is non-empty, so an empty
    # modified list lists every workspace. Pinned because the probe reuses
    # this and must not "fix" it silently.
    assert groups_filter([]) == {}
    assert groups_filter(["ws-1", "ws-2"]) == {"$filter": "id eq ws-1 or id eq ws-2"}


def test_a_personal_workspace_gets_no_clickable_url() -> None:
    ws = workspace_from_group(
        {
            "id": "ws-3",
            "name": "PersonalWorkspace Some Person",
            "type": "PersonalGroup",
        },
        PowerBiEnvironment.COMMERCIAL,
    )
    assert (ws.id, ws.type, ws.webUrl) == ("ws-3", "PersonalGroup", None)


def test_government_environment_uses_the_government_host() -> None:
    assert DataResolverBase.my_org_url_for(PowerBiEnvironment.GOVERNMENT) == (
        "https://api.powerbigov.us/v1.0/myorg"
    )


_RECIPE: Dict[str, Any] = {
    "tenant_id": "tenant",
    "client_id": "client",
    "client_secret": "secret",
}


def _judge(
    kind: str,
    names: List[str],
    parent: List[str],
    attributes: Optional[Sequence[Mapping[str, str]]] = None,
    try_allow: Optional[Sequence[str]] = None,
    **recipe: Any,
) -> Tuple[FilterCheckResult, Dict[str, Tuple[bool, Optional[str]]]]:
    result = check_filters(
        source_type="powerbi",
        config_dict={**_RECIPE, **recipe},
        kind=kind,
        parent_path=parent,
        names=names,
        attributes=attributes,
        try_allow=try_allow,
    )
    return result, {v.name: (v.included, v.excluded_by) for v in result.results}


def test_workspaces_are_judged_by_name_pattern_on_the_bare_name() -> None:
    result, verdicts = _judge(
        "Workspace",
        ["Sales", "Scratch"],
        [],
        workspace_name_pattern={"allow": ["^Sales$"]},
    )
    assert result.pattern_field == "workspace_name_pattern"
    assert verdicts == {
        "Sales": (True, None),
        "Scratch": (False, "workspace_name_pattern"),
    }


def test_workspace_ids_are_judged_by_id_pattern_including_the_deprecated_field() -> (
    None
):
    _, verdicts = _judge(
        "Workspace",
        ["Sales", "Finance"],
        [],
        attributes=[
            {"id": "ws-1", "type": "Workspace"},
            {"id": "ws-2", "type": "Workspace"},
        ],
        workspace_id="ws-1",
    )
    assert verdicts == {
        "Sales": (True, None),
        "Finance": (False, "workspace_id_pattern"),
    }


def test_workspace_type_filter_is_judged_from_the_type_attribute() -> None:
    _, verdicts = _judge(
        "Workspace", ["Mine"], [], attributes=[{"id": "ws-3", "type": "PersonalGroup"}]
    )
    assert verdicts == {"Mine": (False, "workspace_type_filter")}


def test_a_name_exclusion_stays_the_reason_even_when_id_and_type_also_fail() -> None:
    # get_allowed_workspaces checks id and name together before the type, and
    # a caller fixing the recipe should be pointed at the name it can see.
    _, verdicts = _judge(
        "Workspace",
        ["Scratch"],
        [],
        attributes=[{"id": "ws-9", "type": "PersonalGroup"}],
        workspace_name_pattern={"deny": ["^Scratch$"]},
        workspace_id_pattern={"deny": ["^ws-9$"]},
    )
    assert verdicts == {"Scratch": (False, "workspace_name_pattern")}


def test_try_allow_reaches_the_name_half_of_the_override() -> None:
    _, verdicts = _judge(
        "Workspace",
        ["Scratch"],
        [],
        attributes=[{"id": "ws-9", "type": "Workspace"}],
        try_allow=["^Scratch$"],
        workspace_name_pattern={"allow": ["^Sales$"]},
    )
    assert verdicts == {"Scratch": (True, None)}


def test_a_workspace_that_is_not_active_is_excluded_by_its_state() -> None:
    # The admin API lists deleted workspaces with their state; the scan
    # drops every workspace that is not Active.
    _, verdicts = _judge(
        "Workspace",
        ["Sales", "Old"],
        [],
        attributes=[
            {"id": "ws-1", "type": "Workspace", "state": "Active"},
            {"id": "ws-8", "type": "Workspace", "state": "Deleted"},
        ],
    )
    assert verdicts == {"Sales": (True, None), "Old": (False, "workspace_state")}


def test_a_workspace_without_id_or_type_is_kept_with_a_warning() -> None:
    result, verdicts = _judge("Workspace", ["Sales"], [])
    assert verdicts == {"Sales": (True, None)}
    assert any("workspace_id_pattern" in w for w in result.warnings)
    assert any("workspace_type_filter" in w for w in result.warnings)


def test_reports_are_unfiltered_but_inherit_their_workspace_verdict() -> None:
    result, verdicts = _judge(
        "Report",
        ["Weekly"],
        ["Scratch"],
        workspace_name_pattern={"deny": ["^Scratch$"]},
    )
    assert result.filtering == "unfiltered"
    assert verdicts == {"Weekly": (False, "workspace_name_pattern")}


@pytest.mark.parametrize(
    "kind, switch",
    [
        ("Report", "extract_reports"),
        ("PaginatedReport", "extract_reports"),
        ("Dashboard", "extract_dashboards"),
    ],
)
def test_a_switched_off_kind_is_excluded_by_its_switch(kind: str, switch: str) -> None:
    result = check_filters(
        source_type="powerbi",
        config_dict={**_RECIPE, switch: False},
        kind=kind,
        parent_path=["Sales"],
        names=["Weekly"],
    )
    assert [(r.included, r.excluded_by) for r in result.results] == [(False, switch)]


_ORG = "https://api.powerbi.com/v1.0/myorg"
_GROUPS: List[Dict[str, Any]] = [
    {"id": "ws-1", "name": "Sales", "type": "Workspace"},
    {"id": "ws-2", "name": "Finance", "type": "Workspace"},
    {"id": "ws-3", "name": "PersonalWorkspace Some Person", "type": "PersonalGroup"},
    {"id": "ws-4", "name": "Admin Monitoring", "type": "AdminWorkspace"},
]


def _paged(requests_mock: rm.Mocker, url: str, rows: List[Dict[str, Any]]) -> None:
    # itr_pages stops on the first empty page.
    requests_mock.get(url, [{"json": {"value": rows}}, {"json": {"value": []}}])


def _probe(**recipe: Any) -> PowerBiMetadataProbe:
    return PowerBiMetadataProbe.for_config(
        PowerBiDashboardSourceConfig.model_validate({**_RECIPE, **recipe})
    )


def test_building_the_provider_opens_no_connection(requests_mock: rm.Mocker) -> None:
    with mock.patch("msal.ConfidentialClientApplication") as msal_client:
        with _probe():
            pass
    msal_client.assert_not_called()
    assert requests_mock.call_count == 0


def test_workspaces_report_denied_ones_and_flag_the_type_filter(
    requests_mock: rm.Mocker,
) -> None:
    _paged(requests_mock, f"{_ORG}/groups", _GROUPS)
    with _probe(workspace_name_pattern={"deny": ["^Finance$"]}) as probe:
        rows = probe.workspaces(limit=10)
    # Finance is denied by pattern but still reported; probe filter judges it.
    assert [(r["name"], r["id"], r["type_allowed"]) for r in rows] == [
        ("Sales", "ws-1", True),
        ("Finance", "ws-2", True),
        ("Admin Monitoring", "ws-4", False),
    ]


def test_personal_workspace_names_are_withheld_unless_the_recipe_ingests_them(
    requests_mock: rm.Mocker,
) -> None:
    _paged(requests_mock, f"{_ORG}/groups", _GROUPS)
    with _probe() as probe:
        rows = probe.workspaces(limit=10)
    # Neither the name nor the id: the id is enough to look the owner up.
    assert all("Some Person" not in str(row) for row in rows)
    assert "ws-3" not in [r["id"] for r in rows]
    assert any("1 personal workspace" in w for w in probe.warnings)
    assert all("Some Person" not in w for w in probe.warnings)

    _paged(requests_mock, f"{_ORG}/groups", _GROUPS)
    with _probe(workspace_type_filter=["Workspace", "PersonalGroup"]) as opted_in:
        names = [r["name"] for r in opted_in.workspaces(limit=10)]
    assert "PersonalWorkspace Some Person" in names
    assert opted_in.warnings == []


def test_withheld_personal_workspaces_are_not_counted_twice(
    requests_mock: rm.Mocker,
) -> None:
    _paged(requests_mock, f"{_ORG}/groups", _GROUPS)
    with _probe() as probe:
        probe.workspaces(limit=10)
        _paged(requests_mock, f"{_ORG}/groups", _GROUPS)
        probe.workspaces(limit=10)
    personal = [w for w in probe.warnings if "personal" in w]
    assert len(personal) == 1 and personal[0].startswith("1 personal workspace")


def test_admin_apis_only_lists_through_the_admin_endpoint(
    requests_mock: rm.Mocker,
) -> None:
    _paged(
        requests_mock,
        f"{_ORG}/admin/groups",
        [{**_GROUPS[0], "state": "Active"}],
    )
    with _probe(admin_apis_only=True) as probe:
        rows = probe.workspaces(limit=10)
    assert [(r["id"], r["state"]) for r in rows] == [("ws-1", "Active")]


def test_government_environment_lists_from_the_government_host(
    requests_mock: rm.Mocker,
) -> None:
    _paged(requests_mock, "https://api.powerbigov.us/v1.0/myorg/groups", _GROUPS[:1])
    with _probe(environment="GOVERNMENT") as probe:
        assert [r["id"] for r in probe.workspaces(limit=10)] == ["ws-1"]
    assert all("powerbigov.us" in r.url for r in requests_mock.request_history)


def test_the_limit_stops_paging_instead_of_reading_the_whole_tenant(
    requests_mock: rm.Mocker,
) -> None:
    _paged(requests_mock, f"{_ORG}/groups", _GROUPS)
    result = run_probe_method("powerbi", dict(_RECIPE), "workspaces", {"limit": 1})
    assert isinstance(result.result, list)
    assert len(result.result) == 1 and result.truncated
    assert requests_mock.call_count == 1  # the second (empty) page was never fetched


def test_modified_since_narrows_the_listing_as_ingestion_does(
    requests_mock: rm.Mocker,
) -> None:
    requests_mock.get(f"{_ORG}/admin/workspaces/modified", json=[{"id": "ws-1"}])
    _paged(requests_mock, f"{_ORG}/groups", _GROUPS[:1])
    with _probe(modified_since="2026-09-01T00:00:00.0000000Z") as probe:
        probe.workspaces(limit=10)
    groups_call = [
        r for r in requests_mock.request_history if r.path.endswith("/groups")
    ][0]
    assert groups_call.qs["$filter"] == ["id eq ws-1"]


def test_nothing_modified_lists_everything_and_says_so(
    requests_mock: rm.Mocker,
) -> None:
    requests_mock.get(f"{_ORG}/admin/workspaces/modified", json=[])
    _paged(requests_mock, f"{_ORG}/groups", _GROUPS[:2])
    with _probe(modified_since="2026-09-01T00:00:00.0000000Z") as probe:
        assert len(probe.workspaces(limit=10)) == 2
    groups_call = [
        r for r in requests_mock.request_history if r.path.endswith("/groups")
    ][0]
    assert "$filter" not in groups_call.qs
    assert any("no id filter" in w for w in probe.warnings)


def test_a_rejected_modified_since_is_the_callers_to_fix(
    requests_mock: rm.Mocker,
) -> None:
    requests_mock.get(
        f"{_ORG}/admin/workspaces/modified",
        status_code=400,
        json={"error": {"code": "InvalidRequest"}},
    )
    with (
        pytest.raises(ValueError, match="modified_since"),
        _probe(modified_since="2020-01-01T00:00:00.0000000Z") as probe,
    ):
        probe.workspaces(limit=10)
    assert not [r for r in requests_mock.request_history if r.path.endswith("/groups")]


def _no_token(*args: Any, **kwargs: Any) -> Any:
    class Client:
        def acquire_token_for_client(self, *a: Any, **k: Any) -> Dict:
            return {}

    return Client()


def test_an_auth_failure_is_a_connection_error_not_an_empty_listing() -> None:
    with (
        mock.patch("msal.ConfidentialClientApplication", side_effect=_no_token),
        pytest.raises(ProbeConnectionError),
    ):
        run_probe_method("powerbi", dict(_RECIPE), "workspaces", {})


def test_an_auth_failure_under_modified_since_is_not_blamed_on_the_value() -> None:
    # The regular resolver authenticates; the admin one, built for the
    # modified_since lookup, does not. Its token failure is a
    # ConfigurationError too, and must not read as a bad modified_since.
    clients = iter([_mock_msal_cca(), _no_token()])
    recipe = {**_RECIPE, "modified_since": "2026-09-01T00:00:00.0000000Z"}
    with (
        mock.patch(
            "msal.ConfidentialClientApplication",
            side_effect=lambda *a, **k: next(clients),
        ),
        pytest.raises(ProbeConnectionError),
    ):
        run_probe_method("powerbi", recipe, "workspaces", {})


@pytest.mark.parametrize("status", [401, 403])
def test_an_unreadable_modified_list_falls_back_as_ingestion_does(
    requests_mock: rm.Mocker, status: int
) -> None:
    # PowerBiAPI.get_modified_workspaces swallows this and lists everything.
    requests_mock.get(f"{_ORG}/admin/workspaces/modified", status_code=status)
    _paged(requests_mock, f"{_ORG}/groups", _GROUPS[:2])
    with _probe(modified_since="2026-09-01T00:00:00.0000000Z") as probe:
        assert len(probe.workspaces(limit=10)) == 2
    groups_call = [
        r for r in requests_mock.request_history if r.path.endswith("/groups")
    ][0]
    assert "$filter" not in groups_call.qs
    assert any(str(status) in w and "every workspace" in w for w in probe.warnings), (
        probe.warnings
    )


def test_a_forbidden_groups_listing_raises_rather_than_reporting_empty(
    requests_mock: rm.Mocker,
) -> None:
    requests_mock.get(f"{_ORG}/groups", status_code=403)
    with pytest.raises(Exception) as excinfo, _probe() as probe:
        probe.workspaces(limit=10)
    assert "403" in str(excinfo.value)


def test_probe_methods_advertises_the_commands_and_kinds() -> None:
    kinds = {s.command: s.kind for s in list_probe_methods("powerbi")}
    assert kinds["workspaces"] == "Workspace"


_REPORTS: Dict[str, Any] = {
    "value": [
        {"id": "r-1", "name": "Weekly", "reportType": "PowerBIReport"},
        {"id": "r-2", "name": "Invoice", "reportType": "PaginatedReport"},
        # PowerBI repeats app-published reports with an appId; ingestion drops them.
        {"id": "r-1", "name": "Weekly", "reportType": "PowerBIReport", "appId": "a"},
    ]
}


def test_reports_are_listed_by_workspace_name_without_app_duplicates(
    requests_mock: rm.Mocker,
) -> None:
    _paged(requests_mock, f"{_ORG}/groups", _GROUPS)
    requests_mock.get(f"{_ORG}/groups/ws-1/reports", json=_REPORTS)
    result = run_probe_method(
        "powerbi", dict(_RECIPE), "reports", {"workspace": "Sales"}
    )
    assert result.result == [
        {"name": "Weekly", "id": "r-1", "type": "Report"},
        {"name": "Invoice", "id": "r-2", "type": "PaginatedReport"},
    ]
    assert result.parent_path == ["Sales"]
    assert result.warnings == []


def test_dashboards_are_listed_by_display_name_without_app_duplicates(
    requests_mock: rm.Mocker,
) -> None:
    _paged(requests_mock, f"{_ORG}/groups", _GROUPS)
    requests_mock.get(
        f"{_ORG}/groups/ws-1/dashboards",
        json={
            "value": [
                {"id": "d-1", "displayName": "Overview"},
                {"id": "d-1", "displayName": "Overview", "appId": "a"},
            ]
        },
    )
    with _probe() as probe:
        assert probe.dashboards("Sales") == [{"name": "Overview", "id": "d-1"}]


def test_a_workspace_scoped_command_lists_workspaces_exactly_once(
    requests_mock: rm.Mocker,
) -> None:
    _paged(requests_mock, f"{_ORG}/groups", _GROUPS)
    requests_mock.get(f"{_ORG}/groups/ws-1/dashboards", json={"value": []})
    with _probe() as probe:
        probe.dashboards("Sales")
    groups_calls = [
        r for r in requests_mock.request_history if r.path.endswith("/groups")
    ]
    assert len(groups_calls) == 2  # one sweep: a page plus the empty terminator


def test_an_unknown_workspace_is_a_bad_argument(requests_mock: rm.Mocker) -> None:
    _paged(requests_mock, f"{_ORG}/groups", _GROUPS)
    with pytest.raises(ValueError, match="no workspace named"), _probe() as probe:
        probe.reports("Nope")


def test_a_withheld_personal_workspace_cannot_be_reached_by_name(
    requests_mock: rm.Mocker,
) -> None:
    _paged(requests_mock, f"{_ORG}/groups", _GROUPS)
    with (
        pytest.raises(ValueError, match="no workspace named"),
        _probe() as probe,
    ):
        probe.reports("PersonalWorkspace Some Person")
    assert not [r for r in requests_mock.request_history if "ws-3" in r.url]


def test_a_duplicated_workspace_name_is_refused_with_the_ids(
    requests_mock: rm.Mocker,
) -> None:
    _paged(
        requests_mock,
        f"{_ORG}/groups",
        [_GROUPS[0], {"id": "ws-5", "name": "Sales", "type": "Workspace"}],
    )
    with pytest.raises(ValueError, match="ws-1, ws-5"), _probe() as probe:
        probe.reports("Sales")


def test_a_404_on_one_workspace_degrades_with_a_warning(
    requests_mock: rm.Mocker,
) -> None:
    _paged(requests_mock, f"{_ORG}/groups", _GROUPS)
    requests_mock.get(f"{_ORG}/groups/ws-1/reports", status_code=404)
    with _probe() as probe:
        assert probe.reports("Sales") == []
    assert any("HTTP 404" in w for w in probe.warnings)


def test_a_401_on_a_workspace_listing_raises(requests_mock: rm.Mocker) -> None:
    _paged(requests_mock, f"{_ORG}/groups", _GROUPS)
    requests_mock.get(f"{_ORG}/groups/ws-1/reports", status_code=401)
    with pytest.raises(Exception) as excinfo, _probe() as probe:
        probe.reports("Sales")
    assert "401" in str(excinfo.value)


def test_id_and_type_exclusions_of_the_parent_are_reported(
    requests_mock: rm.Mocker,
) -> None:
    _paged(requests_mock, f"{_ORG}/groups", _GROUPS)
    requests_mock.get(f"{_ORG}/groups/ws-4/dashboards", json={"value": []})
    with _probe(workspace_id_pattern={"deny": ["^ws-4$"]}) as probe:
        probe.dashboards("Admin Monitoring")
    assert any("workspace_id_pattern" in w for w in probe.warnings)
    assert any("workspace_type_filter" in w for w in probe.warnings)


def test_disabled_extraction_is_explained(requests_mock: rm.Mocker) -> None:
    _paged(requests_mock, f"{_ORG}/groups", _GROUPS)
    requests_mock.get(f"{_ORG}/groups/ws-1/dashboards", json={"value": []})
    with _probe(extract_dashboards=False) as probe:
        probe.dashboards("Sales")
    assert any("extract_dashboards" in w for w in probe.warnings)


@pytest.mark.parametrize("status", [401, 403])
def test_admin_access_denied_is_an_answer_not_an_error(
    requests_mock: rm.Mocker, status: int
) -> None:
    requests_mock.get(f"{_ORG}/admin/groups", status_code=status)
    with _probe() as probe:
        assert probe.admin_api_access() == {
            "admin_api": "denied",
            "status": status,
            "admin_apis_only": False,
        }


def test_admin_access_reads_a_single_row(requests_mock: rm.Mocker) -> None:
    requests_mock.get(f"{_ORG}/admin/groups", json={"value": [_GROUPS[0]]})
    with _probe() as probe:
        assert probe.admin_api_access()["admin_api"] == "granted"
    assert requests_mock.call_count == 1
    assert requests_mock.request_history[0].qs["$top"] == ["1"]


def test_admin_access_unexpected_status_raises(requests_mock: rm.Mocker) -> None:
    # 400 rather than 500: the resolver's retry adapter retries 5xx with
    # backoff, which would add seconds to the test for the same branch.
    requests_mock.get(f"{_ORG}/admin/groups", status_code=400)
    with pytest.raises(Exception) as excinfo, _probe() as probe:
        probe.admin_api_access()
    assert "400" in str(excinfo.value)


def test_api_reaches_a_listed_endpoint_through_the_connector_session(
    requests_mock: rm.Mocker,
) -> None:
    requests_mock.get(f"{_ORG}/groups/ws-1/reports", json={"value": []})
    result = run_probe_method(
        "powerbi", dict(_RECIPE), "api", {"path": "/groups/ws-1/reports"}
    )
    assert result.result == {"value": []}
    assert requests_mock.request_history[0].headers["Authorization"] == "Bearer dummy"


def test_api_allows_paging_the_member_workspace_listing() -> None:
    check_api_request(
        "GET",
        "/groups?$top=10&$skip=0",
        PowerBiMetadataProbe.api_allowlist,
        base_url=_ORG,
    )


@pytest.mark.parametrize(
    "path",
    [
        "/admin/groups?$top=10",  # personal-workspace owner names
        "/groups?$expand=users",  # user emails
        "/groups/ws-1/datasets",  # configuredBy emails
        "/groups/ws-1/datasets/d-1/parameters",  # parameter values
        "/groups/ws-1/reports/r-1/datasources",  # connection details
        "/admin/workspaces/scanResult/s-1",  # M / DAX / native SQL
        "/groups/ws-1/users",
    ],
)
def test_api_withholds_pii_and_expression_bearing_endpoints(path: str) -> None:
    with pytest.raises(ApiScopeError):
        check_api_request(
            "GET", path, PowerBiMetadataProbe.api_allowlist, base_url=_ORG
        )


def test_api_base_follows_the_environment(requests_mock: rm.Mocker) -> None:
    gov = "https://api.powerbigov.us/v1.0/myorg"
    requests_mock.get(f"{gov}/groups/ws-1/dashboards", json={"value": []})
    result = run_probe_method(
        "powerbi",
        {**_RECIPE, "environment": "GOVERNMENT"},
        "api",
        {"path": "/groups/ws-1/dashboards"},
    )
    assert result.result == {"value": []}
    assert all("powerbigov.us" in r.url for r in requests_mock.request_history)


def test_a_saved_workspaces_run_judges_all_three_workspace_rules(
    requests_mock: rm.Mocker,
) -> None:
    # The round trip the docs describe: `probe run workspaces --report-to`,
    # then `probe filter --from-run`. Each rule drops exactly one workspace.
    recipe = {
        **_RECIPE,
        "workspace_name_pattern": {"deny": ["^Finance$"]},
        "workspace_id_pattern": {"deny": ["^ws-5$"]},
    }
    _paged(
        requests_mock,
        f"{_ORG}/groups",
        [*_GROUPS, {"id": "ws-5", "name": "Archive", "type": "Workspace"}],
    )
    run = run_probe_method("powerbi", recipe, "workspaces", {})
    listing = listing_from_run(run.to_dict())
    result = check_filters(
        source_type="powerbi",
        config_dict=recipe,
        kind=str(listing.kind),
        parent_path=listing.parent_path,
        names=listing.names,
        attributes=listing.attributes,
    )
    assert {v.name: v.excluded_by for v in result.results} == {
        "Sales": None,
        "Finance": "workspace_name_pattern",
        "Admin Monitoring": "workspace_type_filter",
        "Archive": "workspace_id_pattern",
    }
    assert result.warnings == []
