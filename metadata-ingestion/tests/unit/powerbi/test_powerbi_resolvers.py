"""Unit tests for the PowerBI REST resolvers and the PowerBiAPI wrapper.

The resolvers are exercised against ``requests_mock`` (the HTTP boundary);
PowerBiAPI is exercised with its resolvers patched. MSAL is always stubbed.
"""

from typing import Any, Dict, Iterator, List, Optional, Type
from unittest import mock

import pytest
import requests
import requests_mock as rm

from datahub.configuration.common import AllowDenyPattern
from datahub.ingestion.source.powerbi.config import (
    Constant,
    PowerBiDashboardSourceConfig,
    PowerBiDashboardSourceReport,
)
from datahub.ingestion.source.powerbi.rest_api_wrapper.data_classes import (
    App,
    Column,
    Dashboard,
    Measure,
    MeasureProfile,
    PowerBIDataset,
    Report,
    ReportDatasource,
    ReportType,
    Table,
    Tile,
    Workspace,
)
from datahub.ingestion.source.powerbi.rest_api_wrapper.data_resolver import (
    AdminAPIResolver,
    DataResolverBase,
    RegularAPIResolver,
    is_permission_error,
)
from datahub.ingestion.source.powerbi.rest_api_wrapper.powerbi_api import (
    PowerBiAPI,
    form_full_table_name,
)
from datahub.metadata.schema_classes import NumberTypeClass

_ORG = "https://api.powerbi.com/v1.0/myorg"


class _MsalStub:
    """Counts token acquisitions so token caching can be asserted."""

    def __init__(self, expires_in: int = 3600) -> None:
        self.expires_in = expires_in
        self.calls = 0

    def acquire_token_for_client(self, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        self.calls += 1
        return {"access_token": f"token-{self.calls}", "expires_in": self.expires_in}


@pytest.fixture
def msal_stub() -> Iterator[_MsalStub]:
    stub = _MsalStub()
    with mock.patch("msal.ConfidentialClientApplication", return_value=stub):
        yield stub


def _resolver(cls: Type[DataResolverBase] = RegularAPIResolver) -> Any:
    return cls(
        client_id="client",
        client_secret="secret",
        tenant_id="tenant",
        metadata_api_timeout=10,
    )


def _workspace(ws_id: str = "ws-1", name: str = "Sales") -> Workspace:
    return Workspace(
        id=ws_id,
        name=name,
        type="Workspace",
        webUrl=f"https://app.powerbi.com/groups/{ws_id}",
        datasets={},
        dashboards={},
        reports={},
        report_endorsements={},
        dashboard_endorsements={},
        scan_result={},
        independent_datasets={},
        app=None,
    )


def _dataset(
    dataset_id: str = "ds-1", ws: Optional[Workspace] = None
) -> PowerBIDataset:
    ws = ws or _workspace()
    return PowerBIDataset(
        id=dataset_id,
        name="Model",
        description="",
        webUrl=None,
        workspace_id=ws.id,
        workspace_name=ws.name,
        parameters={},
        tables=[],
        tags=[],
    )


def _column(name: str, hidden: bool = False) -> Column:
    return Column(
        name=name, dataType="Int64", isHidden=hidden, datahubDataType=NumberTypeClass()
    )


# --- token handling ---------------------------------------------------------


def test_access_token_is_reused_until_it_expires(msal_stub: _MsalStub) -> None:
    resolver = _resolver()
    assert msal_stub.calls == 1  # the constructor validates the connection
    assert resolver.get_access_token() == "Bearer token-1"
    assert msal_stub.calls == 1


def test_access_token_is_refreshed_once_expired() -> None:
    # expires_in below the 300s safety gap makes the token expire immediately,
    # so a long run that outlives its token must re-acquire instead of 401ing.
    stub = _MsalStub(expires_in=1)
    with mock.patch("msal.ConfidentialClientApplication", return_value=stub):
        resolver = _resolver()
        assert resolver.get_access_token() == "Bearer token-2"
    assert stub.calls == 2


# --- scan job lifecycle (admin) --------------------------------------------


def test_create_scan_job_returns_scan_id(
    msal_stub: _MsalStub, requests_mock: rm.Mocker
) -> None:
    requests_mock.post(f"{_ORG}/admin/workspaces/getInfo", json={"id": "scan-9"})
    assert _resolver(AdminAPIResolver).create_scan_job(["ws-1", "ws-2"]) == "scan-9"
    sent = requests_mock.request_history[0]
    assert sent.qs["lineage"] == ["true"]
    assert sent.qs["datasetschema"] == ["true"]


def test_wait_for_scan_polls_until_succeeded(
    msal_stub: _MsalStub, requests_mock: rm.Mocker
) -> None:
    requests_mock.get(
        f"{_ORG}/admin/workspaces/scanStatus/scan-1",
        [{"json": {"status": "Running"}}, {"json": {"status": "Succeeded"}}],
    )
    with mock.patch(
        "datahub.ingestion.source.powerbi.rest_api_wrapper.data_resolver.sleep"
    ):
        assert _resolver(AdminAPIResolver).wait_for_scan_to_complete(
            scan_id="scan-1", timeout=30
        )
    assert requests_mock.call_count == 2


def test_wait_for_scan_gives_up_when_timeout_elapses(
    msal_stub: _MsalStub, requests_mock: rm.Mocker
) -> None:
    # A timeout shorter than one poll interval still gets exactly one poll.
    requests_mock.get(
        f"{_ORG}/admin/workspaces/scanStatus/scan-1", json={"status": "Running"}
    )
    with mock.patch(
        "datahub.ingestion.source.powerbi.rest_api_wrapper.data_resolver.sleep"
    ):
        assert not _resolver(AdminAPIResolver).wait_for_scan_to_complete(
            scan_id="scan-1", timeout=1
        )
    assert requests_mock.call_count == 1


def test_get_scan_result_distinguishes_empty_from_error(
    msal_stub: _MsalStub, requests_mock: rm.Mocker
) -> None:
    resolver = _resolver(AdminAPIResolver)
    url = f"{_ORG}/admin/workspaces/scanResult/scan-1"

    requests_mock.get(url, json={"workspaces": []})
    assert resolver.get_scan_result(scan_id="scan-1") is None

    requests_mock.get(url, json={"workspaces": [{"id": "ws-1"}]})
    assert resolver.get_scan_result(scan_id="scan-1") == {
        "workspaces": [{"id": "ws-1"}]
    }

    requests_mock.get(url, status_code=404)
    with pytest.raises(ConnectionError):
        resolver.get_scan_result(scan_id="scan-1")


# --- regular vs admin endpoints -------------------------------------------


@pytest.mark.parametrize(
    "cls, path",
    [
        (RegularAPIResolver, "/groups/ws-1/dashboards/d-1/tiles"),
        (AdminAPIResolver, "/admin/dashboards/d-1/tiles"),
    ],
)
def test_tiles_are_fetched_from_the_resolver_specific_endpoint(
    msal_stub: _MsalStub,
    requests_mock: rm.Mocker,
    cls: Type[DataResolverBase],
    path: str,
) -> None:
    requests_mock.get(
        f"{_ORG}{path}",
        json={"value": [{"id": "t-1", "title": "Tile", "reportId": "r-1"}]},
    )
    dashboard = Dashboard(
        id="d-1",
        displayName="Dash",
        description="",
        embedUrl=None,
        isReadOnly=False,
        workspace_id="ws-1",
        workspace_name="Sales",
        tiles=[],
        users=[],
        tags=[],
        webUrl=None,
    )
    tiles = _resolver(cls).get_tiles(_workspace(), dashboard)
    assert [(t.id, t.report_id, t.createdFrom) for t in tiles] == [
        ("t-1", "r-1", Tile.CreatedFrom.REPORT)
    ]


def test_regular_pages_by_report_degrade_to_empty_on_http_failure(
    msal_stub: _MsalStub, requests_mock: rm.Mocker
) -> None:
    url = f"{_ORG}/groups/ws-1/reports/r-1/pages"
    resolver = _resolver()

    requests_mock.get(url, status_code=404)
    assert resolver.get_pages_by_report(_workspace(), "r-1") == []

    requests_mock.get(
        url, json={"value": [{"name": "Page 1", "displayName": "Summary", "order": 0}]}
    )
    pages = resolver.get_pages_by_report(_workspace(), "r-1")
    assert [(p.id, p.displayName) for p in pages] == [("r-1.Page_1", "Summary")]


def test_admin_get_app_pages_through_the_apps_listing(
    msal_stub: _MsalStub, requests_mock: rm.Mocker
) -> None:
    requests_mock.get(
        f"{_ORG}/admin/apps",
        [
            {"json": {"value": [{"id": "other", "name": "Other"}]}},
            {"json": {"value": [{"id": "app-1", "name": "Sales App"}]}},
            {"json": {"value": []}},
        ],
    )
    app = _resolver(AdminAPIResolver).get_app(app_id="app-1")
    assert app is not None
    assert (app.id, app.name, app.reports, app.dashboards) == (
        "app-1",
        "Sales App",
        [],
        [],
    )
    assert requests_mock.request_history[1].qs["$skip"] == ["1000"]


def test_regular_resolver_cannot_fetch_apps(msal_stub: _MsalStub) -> None:
    assert _resolver().get_app(app_id="app-1") is None


# --- profiling (regular) ---------------------------------------------------


def _mock_dax(requests_mock: rm.Mocker, column_values: Dict[str, Any]) -> None:
    def respond(request: Any, context: Any) -> Dict[str, Any]:
        query: str = request.json()["queries"][0]["query"]
        if "TOPN" in query:
            rows = [
                {"Sales[amount]": 10, "Sales[secret]": 1},
                {"Sales[amount]": 20, "Sales[secret]": 2},
            ]
        elif '"count"' in query:
            rows = [{"[count]": 42}]
        else:
            rows = [
                {"[min]": column_values["min"], "[max]": column_values["max"]},
                {"[unique_count]": column_values["unique_count"]},
            ]
        return {"results": [{"tables": [{"rows": rows}]}]}

    requests_mock.post(f"{_ORG}/groups/ws-1/datasets/ds-1/executeQueries", json=respond)


def _profiled_table(dataset: PowerBIDataset) -> Table:
    return Table(
        name="Sales",
        full_name="Model.Sales",
        columns=[_column("amount"), _column("secret", hidden=True)],
        measures=[Measure(name="total", expression="SUM(1)", isHidden=False)],
        dataset=dataset,
    )


def test_profile_dataset_fills_stats_and_skips_hidden_columns(
    msal_stub: _MsalStub, requests_mock: rm.Mocker
) -> None:
    _mock_dax(requests_mock, {"min": 1, "max": 99, "unique_count": 7})
    dataset = _dataset()
    table = _profiled_table(dataset)

    _resolver().profile_dataset(dataset, table, "Sales", AllowDenyPattern.allow_all())

    assert table.row_count == 42
    assert table.column_count == 2  # hidden column excluded
    assert table.columns is not None and table.measures is not None
    amount, secret = table.columns
    assert amount.measure_profile is not None
    assert (amount.measure_profile.min, amount.measure_profile.max) == ("1", "99")
    assert amount.measure_profile.unique_count == 7
    assert amount.measure_profile.sample_values == ["10", "20"]
    assert secret.measure_profile is None
    assert table.measures[0].measure_profile is not None


def test_profile_dataset_survives_failed_dax_queries(
    msal_stub: _MsalStub, requests_mock: rm.Mocker
) -> None:
    # A dataset the principal cannot query must not abort the whole dataset.
    requests_mock.post(
        f"{_ORG}/groups/ws-1/datasets/ds-1/executeQueries", status_code=400
    )
    dataset = _dataset()
    table = _profiled_table(dataset)

    _resolver().profile_dataset(dataset, table, "Sales", AllowDenyPattern.allow_all())

    assert table.row_count == 0
    assert table.columns is not None
    assert table.columns[0].measure_profile is not None
    assert table.columns[0].measure_profile.min is None


def test_profile_dataset_honours_the_profile_pattern(
    msal_stub: _MsalStub, requests_mock: rm.Mocker
) -> None:
    requests_mock.post(f"{_ORG}/groups/ws-1/datasets/ds-1/executeQueries", json={})
    config = PowerBiDashboardSourceConfig.model_validate(
        {
            "tenant_id": "t",
            "client_id": "c",
            "client_secret": "s",
            "profile_pattern": {"deny": ["Sales\\.Model\\..*"]},
        }
    )
    table = _profiled_table(_dataset())
    _resolver().profile_dataset(_dataset(), table, "Sales", config.profile_pattern)
    assert requests_mock.call_count == 0
    assert table.row_count is None


# --- PowerBiAPI ------------------------------------------------------------


def _api(**overrides: Any) -> PowerBiAPI:
    config = PowerBiDashboardSourceConfig.model_validate(
        {"tenant_id": "t", "client_id": "c", "client_secret": "s", **overrides}
    )
    return PowerBiAPI(config=config, reporter=PowerBiDashboardSourceReport())


def _admin(api: PowerBiAPI) -> Any:
    return api._PowerBiAPI__admin_api_resolver  # type: ignore[attr-defined]


def _http_error(status: int) -> requests.HTTPError:
    response = requests.Response()
    response.status_code = status
    return requests.HTTPError(response=response)


def test_scan_result_is_none_when_the_scan_job_cannot_be_created(
    msal_stub: _MsalStub,
) -> None:
    api = _api()
    with mock.patch.object(
        _admin(api), "create_scan_job", side_effect=_http_error(403)
    ):
        assert api._get_scan_result(["ws-1"]) is None


def test_scan_result_raises_when_the_scan_never_completes(
    msal_stub: _MsalStub,
) -> None:
    api = _api()
    with (
        mock.patch.object(_admin(api), "create_scan_job", return_value="scan-1"),
        mock.patch.object(_admin(api), "wait_for_scan_to_complete", return_value=False),
    ):
        with pytest.raises(ValueError, match="scan_timeout"):
            api._get_scan_result(["ws-1"])


def test_entity_users_are_not_fetched_when_ownership_is_disabled(
    msal_stub: _MsalStub,
) -> None:
    api = _api(extract_ownership=False)
    with mock.patch.object(_admin(api), "get_users") as get_users:
        assert api.get_report_users("ws-1", "r-1") == []
    get_users.assert_not_called()


def test_entity_users_permission_error_yields_empty_list(
    msal_stub: _MsalStub,
) -> None:
    api = _api(extract_ownership=True)
    with mock.patch.object(_admin(api), "get_users", side_effect=_http_error(401)):
        assert api.get_report_users("ws-1", "r-1") == []


def test_populate_app_details_links_app_assets_to_their_originals(
    msal_stub: _MsalStub,
) -> None:
    api = _api()
    workspace = _workspace()
    scan = {
        Constant.REPORTS: [
            {Constant.ID: "r-1", Constant.NAME: "Report"},
            {
                Constant.ID: "r-1-app",
                Constant.NAME: "Report",
                Constant.APP_ID: "app-1",
                Constant.ORIGINAL_REPORT_OBJECT_ID: "r-1",
            },
        ],
        Constant.DASHBOARDS: [
            {Constant.ID: "d-1", Constant.DISPLAY_NAME: "Dash"},
            {Constant.ID: "d-2", Constant.DISPLAY_NAME: "Not in app"},
            {
                Constant.ID: "d-1-app",
                Constant.DISPLAY_NAME: "[App] Dash",
                Constant.APP_ID: "app-1",
            },
        ],
    }
    app = App(
        id="app-1",
        name="App",
        description=None,
        last_update=None,
        dashboards=[],
        reports=[],
    )
    with mock.patch.object(api, "get_app", return_value=app) as get_app:
        api._populate_app_details(workspace=workspace, workspace_metadata=scan)

    get_app.assert_called_once_with(app_id="app-1")
    assert workspace.app is app
    assert [(r.id, r.original_report_id) for r in app.reports] == [("r-1-app", "r-1")]
    assert [(d.id, d.original_dashboard_id) for d in app.dashboards] == [
        ("d-1-app", "d-1")
    ]


def test_populate_app_details_reports_a_missing_app(msal_stub: _MsalStub) -> None:
    api = _api()
    workspace = _workspace()
    scan = {
        Constant.REPORTS: [
            {
                Constant.ID: "r-1-app",
                Constant.APP_ID: "app-1",
                Constant.ORIGINAL_REPORT_OBJECT_ID: "r-1",
            }
        ]
    }
    with mock.patch.object(api, "get_app", return_value=None):
        api._populate_app_details(workspace=workspace, workspace_metadata=scan)
    assert workspace.app is None
    assert any(i.title == "App Not Found" for i in api.reporter.infos)


def _tile(dataset: Optional[PowerBIDataset], report: Optional[Report]) -> Tile:
    return Tile(
        id="t-1",
        title="Tile",
        embedUrl=None,
        dataset_id=dataset.id if dataset else None,
        report_id=report.id if report else None,
        createdFrom=Tile.CreatedFrom.REPORT,
        dataset=dataset,
        report=report,
    )


def _report(dataset: Optional[PowerBIDataset]) -> Report:
    return Report(
        id="r-1",
        name="Report",
        type=ReportType.PowerBIReport,
        webUrl=None,
        embedUrl=None,
        description="",
        dataset_id=dataset.id if dataset else None,
        dataset=dataset,
        pages=[],
        users=[],
        tags=[],
    )


def test_independent_datasets_are_those_no_tile_or_report_uses(
    msal_stub: _MsalStub,
) -> None:
    api = _api()
    workspace = _workspace()
    by_tile, by_report, orphan = (
        _dataset("ds-tile"),
        _dataset("ds-rep"),
        _dataset("ds-x"),
    )
    workspace.datasets = {d.id: d for d in (by_tile, by_report, orphan)}
    dashboard = Dashboard(
        id="d-1",
        displayName="Dash",
        description="",
        embedUrl=None,
        isReadOnly=False,
        workspace_id="ws-1",
        workspace_name="Sales",
        tiles=[_tile(by_tile, None)],
        users=[],
        tags=[],
        webUrl=None,
    )
    workspace.dashboards = {dashboard.id: dashboard}
    workspace.reports = {"r-1": _report(by_report)}

    api._fill_independent_datasets(workspace)
    assert list(workspace.independent_datasets) == ["ds-x"]


def test_fill_dashboards_falls_back_to_rest_calls_without_scan_result(
    msal_stub: _MsalStub,
) -> None:
    api = _api(extract_reports=False)
    workspace = _workspace()
    workspace.scan_result = {}
    dataset = _dataset("ds-1")
    api.dataset_registry["ds-1"] = dataset
    dashboard = Dashboard(
        id="d-1",
        displayName="Dash",
        description="",
        embedUrl=None,
        isReadOnly=False,
        workspace_id="ws-1",
        workspace_name="Sales",
        tiles=[],
        users=[],
        tags=[],
        webUrl=None,
    )
    tile = Tile(
        id="t-1",
        title="Tile",
        embedUrl=None,
        dataset_id="ds-1",
        report_id="r-missing",
        createdFrom=Tile.CreatedFrom.REPORT,
        dataset=None,
        report=None,
    )
    resolver = api._get_resolver()
    with (
        mock.patch.object(resolver, "get_dashboards", return_value=[dashboard]),
        mock.patch.object(resolver, "get_tiles", return_value=[tile]),
        mock.patch.object(api, "get_dashboard_users", return_value=[]),
        mock.patch.object(api, "get_reports") as get_reports,
    ):
        api.fill_regular_metadata_detail(workspace=workspace)

    get_reports.assert_not_called()  # extract_reports=False
    assert workspace.dashboards["d-1"].tiles[0].dataset == dataset
    assert workspace.dashboards["d-1"].tiles[0].report is None
    assert any(i.title == "Missing Report Lineage For Tile" for i in api.reporter.infos)
    assert any(i.title == "Dashboard Scan Fallback Active" for i in api.reporter.infos)


def test_workspace_datasets_carry_endorsement_tags_and_tolerate_parameter_errors(
    msal_stub: _MsalStub,
) -> None:
    api = _api(extract_endorsements_to_tags=True, profiling={"enabled": True})
    workspace = _workspace()
    workspace.scan_result = {
        Constant.DATASETS: [
            {
                Constant.ID: "ds-1",
                Constant.NAME: "Model",
                Constant.ENDORSEMENT_DETAIL: {Constant.ENDORSEMENT: "Certified"},
                Constant.TABLES: [
                    {
                        Constant.NAME: "Sales",
                        Constant.SOURCE: [{Constant.EXPRESSION: "let x = 1 in x"}],
                        "columns": [
                            {"name": "amount", "dataType": "Int64", "isHidden": False}
                        ],
                        "measures": [],
                    }
                ],
            }
        ]
    }
    resolver = api._get_resolver()
    with (
        mock.patch.object(
            resolver, "get_dataset_parameters", side_effect=_http_error(403)
        ),
        mock.patch.object(resolver, "profile_dataset") as profile,
    ):
        datasets = api._get_workspace_datasets(workspace)

    dataset = datasets["ds-1"]
    assert dataset.tags == ["Certified"]
    assert dataset.parameters == {}
    assert [t.expression for t in dataset.tables] == ["let x = 1 in x"]
    assert dataset.tables[0].columns is not None
    assert isinstance(dataset.tables[0].columns[0].datahubDataType, NumberTypeClass)
    profile.assert_called_once()


@pytest.mark.parametrize(
    "overrides, expected",
    [
        ({}, "Model.Sales_Table"),
        ({"include_workspace_name_in_dataset_urn": True}, "my_space.Model.Sales_Table"),
        (
            {
                "include_workspace_name_in_dataset_urn": True,
                "workspace_id_as_urn_part": True,
            },
            "ws-1.Model.Sales_Table",
        ),
    ],
)
def test_full_table_name_prefixes_the_workspace_when_configured(
    overrides: Dict[str, Any], expected: str
) -> None:
    config = PowerBiDashboardSourceConfig.model_validate(
        {"tenant_id": "t", "client_id": "c", "client_secret": "s", **overrides}
    )
    name = form_full_table_name(
        config=config,
        workspace=_workspace(name="My Space"),
        dataset_name="Model",
        table_name="Sales Table",
    )
    assert name == expected


def test_get_workspaces_narrows_to_modified_ids(msal_stub: _MsalStub) -> None:
    api = _api(modified_since="2024-01-01T00:00:00.0000000Z")
    groups: List[Dict[str, Any]] = [
        {Constant.ID: "ws-1", Constant.NAME: "Sales", Constant.TYPE: "Workspace"}
    ]
    with (
        mock.patch.object(
            _admin(api), "get_modified_workspaces", return_value=["ws-1", "ws-2"]
        ),
        mock.patch.object(api._get_resolver(), "get_groups", return_value=groups) as g,
    ):
        workspaces = api.get_workspaces()
    assert [w.id for w in workspaces] == ["ws-1"]
    assert g.call_args.kwargs["filter_"] == {"$filter": "id eq ws-1 or id eq ws-2"}


def test_get_workspaces_returns_empty_when_listing_fails(msal_stub: _MsalStub) -> None:
    api = _api()
    with mock.patch.object(
        api._get_resolver(), "get_groups", side_effect=_http_error(403)
    ):
        assert api.get_workspaces() == []


# --- endorsements, apps and scan plumbing -----------------------------------


def test_endorsements_from_the_scan_become_dashboard_and_report_tags(
    msal_stub: _MsalStub,
) -> None:
    api = _api(extract_endorsements_to_tags=True)
    workspace = _workspace()
    workspace.scan_result = {
        Constant.DASHBOARDS: [
            {
                Constant.ID: "d-1",
                Constant.DISPLAY_NAME: "Dash",
                Constant.ENDORSEMENT_DETAIL: {Constant.ENDORSEMENT: "Certified"},
            },
            {Constant.ID: "d-2", Constant.DISPLAY_NAME: "Plain"},
        ],
        Constant.REPORTS: [
            {
                Constant.ID: "r-1",
                Constant.NAME: "Report",
                Constant.REPORT_TYPE: "PowerBIReport",
                Constant.ENDORSEMENT_DETAIL: {Constant.ENDORSEMENT: "Promoted"},
            },
            {Constant.NAME: "no id", Constant.ENDORSEMENT_DETAIL: {}},
        ],
    }
    resolver = api._get_resolver()
    with mock.patch.object(resolver, "get_pages_by_report", return_value=[]):
        api.fill_regular_metadata_detail(workspace=workspace)

    assert workspace.dashboards["d-1"].tags == ["Certified"]
    assert workspace.dashboards["d-2"].tags == []
    assert workspace.reports["r-1"].tags == ["Promoted"]


def test_paginated_report_without_dataset_resolves_its_datasources(
    msal_stub: _MsalStub,
) -> None:
    api = _api()
    workspace = _workspace()
    workspace.scan_result = {
        Constant.REPORTS: [
            {
                Constant.ID: "r-1",
                Constant.NAME: "RDL",
                Constant.REPORT_TYPE: "PaginatedReport",
            }
        ]
    }
    resolver = api._get_resolver()
    datasource = ReportDatasource(
        datasource_type="Sql", server="sql-host", database="sales"
    )
    with (
        mock.patch.object(resolver, "get_pages_by_report", return_value=[]),
        mock.patch.object(
            resolver, "get_report_datasources", return_value=[datasource]
        ) as get_datasources,
    ):
        api.fill_regular_metadata_detail(workspace=workspace)

    get_datasources.assert_called_once()
    assert workspace.reports["r-1"].datasources == [datasource]


def test_admin_apis_only_routes_through_the_admin_resolver(
    msal_stub: _MsalStub,
) -> None:
    assert isinstance(_api(admin_apis_only=True)._get_resolver(), AdminAPIResolver)
    assert isinstance(_api()._get_resolver(), RegularAPIResolver)


def test_modified_workspaces_are_empty_without_a_window_or_on_timeout(
    msal_stub: _MsalStub,
) -> None:
    assert _api().get_modified_workspaces() == []

    api = _api(modified_since="2024-01-01T00:00:00.0000000Z")
    with mock.patch.object(
        _admin(api),
        "get_modified_workspaces",
        side_effect=requests.exceptions.Timeout(),
    ):
        assert api.get_modified_workspaces() == []
    assert any(w.title == "Metadata API Timeout" for w in api.reporter.warnings)


def test_scan_result_is_returned_once_the_scan_completes(
    msal_stub: _MsalStub,
) -> None:
    api = _api()
    scan = {"workspaces": [{"id": "ws-1"}]}
    with (
        mock.patch.object(_admin(api), "create_scan_job", return_value="scan-1"),
        mock.patch.object(_admin(api), "wait_for_scan_to_complete", return_value=True),
        mock.patch.object(_admin(api), "get_scan_result", return_value=scan),
    ):
        assert api._get_scan_result(["ws-1"]) == scan


def test_sql_analytics_endpoints_resolve_to_lakehouse_relations() -> None:
    artifacts = PowerBiAPI._parse_fabric_artifacts(
        {
            Constant.ID: "ws-1",
            Constant.PARSING_KEY_LAKEHOUSE: [
                {Constant.ID: "lh-1", Constant.NAME: "LH"}
            ],
            Constant.PARSING_KEY_SQL_ANALYTICS_ENDPOINT: [
                {
                    Constant.ID: "ep-1",
                    Constant.NAME: "LH endpoint",
                    Constant.RELATIONS: [
                        {Constant.DEPENDENT_ON_ARTIFACT_ID: "lh-1"},
                        {Constant.DEPENDENT_ON_ARTIFACT_ID: "not-in-workspace"},
                        {},
                    ],
                }
            ],
        }
    )
    assert artifacts["ep-1"].physical_item_ids == ["lh-1"]
    assert artifacts["lh-1"].relation_dependent_ids is None


def test_workspace_datasets_are_empty_when_the_scan_lists_none(
    msal_stub: _MsalStub,
) -> None:
    workspace = _workspace()
    workspace.scan_result = {Constant.ID: "ws-1", Constant.NAME: "Sales"}
    assert _api()._get_workspace_datasets(workspace) == {}


def test_app_lookup_scans_the_admin_apps_listing(
    msal_stub: _MsalStub, requests_mock: rm.Mocker
) -> None:
    requests_mock.get(
        f"{_ORG}/admin/apps",
        [
            {"json": {"value": [{"id": "app-1", "name": "Sales App"}]}},
            {"json": {"value": []}},
        ],
    )
    api = _api()
    app = api.get_app("app-1")
    assert app is not None and app.name == "Sales App"

    requests_mock.get(
        f"{_ORG}/admin/apps",
        [{"json": {"value": [{"id": "other"}]}}, {"json": {"value": []}}],
    )
    assert api.get_app("app-1") is None


def test_app_id_is_taken_from_app_dashboards_when_no_report_is_shared(
    msal_stub: _MsalStub,
) -> None:
    api = _api()
    workspace = _workspace()
    scan = {
        Constant.DASHBOARDS: [
            {
                Constant.ID: "d-1-app",
                Constant.DISPLAY_NAME: "[App] Dash",
                "appId": "app-9",
            }
        ]
    }
    with mock.patch.object(api, "get_app", return_value=None) as get_app:
        api._populate_app_details(workspace=workspace, workspace_metadata=scan)

    get_app.assert_called_once_with(app_id="app-9")
    assert workspace.app is None
    assert any(i.title == "App Not Found" for i in api.reporter.infos)


# --- resolver endpoints -----------------------------------------------------


def test_regular_resolver_lists_dashboards_and_pages_through_groups(
    msal_stub: _MsalStub, requests_mock: rm.Mocker
) -> None:
    requests_mock.get(
        f"{_ORG}/groups/ws-1/dashboards",
        json={"value": [{"id": "d-1", "displayName": "Dash"}, {"displayName": "x"}]},
    )
    requests_mock.get(
        f"{_ORG}/groups",
        [
            {"json": {"value": [{"id": "ws-1", "name": "Sales"}]}},
            {"json": {"value": []}},
        ],
    )
    resolver = _resolver()
    dashboards = resolver.get_dashboards(_workspace())
    assert [d.id for d in dashboards] == ["d-1"]
    assert dashboards[0].webUrl == "https://app.powerbi.com/groups/ws-1/dashboards/d-1"
    assert resolver.get_groups({"$filter": "type eq 'Workspace'"}) == [
        {"id": "ws-1", "name": "Sales"}
    ]
    assert resolver.get_users("ws-1", "dashboards", "d-1") == []


def test_admin_resolver_lists_dashboards_and_users(
    msal_stub: _MsalStub, requests_mock: rm.Mocker
) -> None:
    requests_mock.get(
        f"{_ORG}/admin/groups/ws-1/dashboards",
        json={"value": [{"id": "d-1", "displayName": "Dash"}]},
    )
    requests_mock.get(
        f"{_ORG}/admin/dashboards/d-1/users",
        json={
            "value": [
                {
                    "identifier": "alice@example.com",
                    "displayName": "Alice",
                    "graphId": "g-1",
                    "principalType": "User",
                    "dashboardUserAccessRight": "Owner",
                },
                {"identifier": "", "displayName": "broken"},
            ]
        },
    )
    resolver = _resolver(AdminAPIResolver)
    assert [d.id for d in resolver.get_dashboards(_workspace())] == ["d-1"]
    users = resolver.get_users("ws-1", "dashboards", "d-1")
    assert [(u.id, u.dashboardUserAccessRight) for u in users] == [
        ("alice@example.com", "Owner")
    ]


def test_admin_resolver_has_no_datasources_parameters_or_profiling(
    msal_stub: _MsalStub,
) -> None:
    resolver = _resolver(AdminAPIResolver)
    dataset = _dataset()
    table = Table(name="Sales", full_name="Model.Sales", columns=[_column("a")])
    assert resolver.get_report_datasources(_workspace(), "r-1") == []
    assert resolver.get_dataset_parameters("ws-1", "ds-1") == {}
    resolver.profile_dataset(dataset, table, "Sales", AllowDenyPattern.allow_all())
    assert table.row_count is None


def test_profile_dataset_tolerates_malformed_dax_responses(
    msal_stub: _MsalStub, requests_mock: rm.Mocker
) -> None:
    requests_mock.post(
        f"{_ORG}/groups/ws-1/datasets/ds-1/executeQueries", json={"results": []}
    )
    dataset = _dataset()
    table = _profiled_table(dataset)

    _resolver().profile_dataset(dataset, table, "Sales", AllowDenyPattern.allow_all())

    assert table.row_count == 0
    assert table.columns is not None
    assert table.columns[0].measure_profile == MeasureProfile()


def test_is_permission_error_only_for_http_401_and_403() -> None:
    assert is_permission_error(_http_error(403))
    assert not is_permission_error(_http_error(500))
    assert not is_permission_error(ValueError("not http"))
