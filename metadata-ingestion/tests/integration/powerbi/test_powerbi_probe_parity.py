import re
from pathlib import Path
from typing import Any, Callable, Dict, Pattern, Set, Union
from unittest import mock

import pytest

from datahub.metadata.urns import DashboardUrn
from tests.integration.powerbi.test_powerbi import (
    default_source_config,
    mock_msal_cca,
    read_mock_data,
    register_mock_api,
)
from tests.test_helpers.probe_parity import (
    EmittedIndex,
    FanOut,
    ParityListing,
    assert_probe_parity,
    pipeline_ingestion,
)

pytestmark = pytest.mark.integration_batch_4

# A workspace container's subtype is its workspace type, so a personal
# workspace is not a "Workspace" container.
_WORKSPACE_TYPES = (
    "Workspace",
    "PersonalGroup",
    "Personal",
    "AdminWorkspace",
    "AdminInsights",
)
_WORKSPACES = ParityListing(
    "workspaces",
    "workspaces",
    lambda index: {n for t in _WORKSPACE_TYPES for n in index.container_names(t)},
)
_GROUPS = "https://api.powerbi.com/v1.0/myorg/groups"
# The default mock answers a scan for these two only, so every case drops the
# third workspace, "Workspace 2".
_DEMO = "64ED5CAD-7C10-4684-8180-826122881108"
_SECOND = "64ED5CAD-7C22-4684-8180-826122881108"
_WORKSPACE_2 = "64ED5CAD-7322-4684-8180-826122881108"
_DASHBOARD_PREFIX = "dashboards."
_REPORT_PREFIX = "reports."


def _ids_under(prefix: str) -> Callable[[EmittedIndex], Set[str]]:
    """Reports are emitted as dashboard entities too; the id in the URN says
    which: "reports.<id>" or "dashboards.<id>"."""

    def ids(index: EmittedIndex) -> Set[str]:
        found = (
            DashboardUrn.from_string(u).dashboard_id for u in index.urns("dashboard")
        )
        return {i[len(prefix) :] for i in found if i.startswith(prefix)}

    return ids


def _in_every_workspace(
    command: str,
    prefix: str,
    *accept: Union[str, Pattern[str]],
    expect_empty: bool = False,
) -> ParityListing:
    return ParityListing(
        command,
        command,
        _ids_under(prefix),
        identity=lambda r: r.attributes["id"],
        fan_out=FanOut("workspaces", "workspace"),
        expect_empty=expect_empty,
        accept_warnings=accept,
    )


def _nothing_in_workspace_2(requests_mock: Any, listing: str) -> None:
    # The fan-out lists under every workspace, kept or not; the shared mock
    # has no listing for the one ingestion never reads.
    requests_mock.get(f"{_GROUPS}/{_WORKSPACE_2}/{listing}", json={"value": []})


# powerbi_probe.py, `_workspace_or_raise`: `dashboards --workspace` warns on
# every run under a workspace workspace_id_pattern drops. A note on the
# parent's verdict, not a degraded fetch.
_ID_DENIED_PARENT = re.compile(
    r"workspace '[^']+' \(id [0-9A-Fa-f-]+\) is excluded by workspace_id_pattern, "
    r"so ingestion reads nothing in it"
)
# powerbi_probe.py, `reports` and `dashboards`: each says so on every run with
# its switch off. A note on the recipe, not a degraded fetch.
_REPORTS_OFF = "extract_reports is false, so ingestion emits none of these"
_DASHBOARDS_OFF = "extract_dashboards is false, so ingestion emits none of these"


def _recipe(**overrides: Any) -> Dict[str, Any]:
    config = default_source_config()
    del config["workspace_id"], config["workspace_id_pattern"]
    return {**config, "extract_workspaces_to_containers": True, **overrides}


@mock.patch("msal.ConfidentialClientApplication", side_effect=mock_msal_cca)
def test_workspace_name_verdicts_match_ingestion(
    mock_msal: mock.MagicMock,
    pytestconfig: pytest.Config,
    tmp_path: Path,
    requests_mock: Any,
) -> None:
    register_mock_api(pytestconfig=pytestconfig, request_mock=requests_mock)
    report = assert_probe_parity(
        "powerbi",
        _recipe(workspace_name_pattern={"deny": ["^Workspace 2$"]}),
        pipeline_ingestion("powerbi", tmp_path),
        [_WORKSPACES],
    )
    assert report.excluded_by("workspaces") == {"Workspace 2": "workspace_name_pattern"}


@mock.patch("msal.ConfidentialClientApplication", side_effect=mock_msal_cca)
def test_workspace_and_dashboard_id_verdicts_match_ingestion(
    mock_msal: mock.MagicMock,
    pytestconfig: pytest.Config,
    tmp_path: Path,
    requests_mock: Any,
) -> None:
    register_mock_api(pytestconfig=pytestconfig, request_mock=requests_mock)
    _nothing_in_workspace_2(requests_mock, "dashboards")
    dashboards = _in_every_workspace("dashboards", _DASHBOARD_PREFIX, _ID_DENIED_PARENT)
    report = assert_probe_parity(
        "powerbi",
        _recipe(
            workspace_name_pattern={"deny": ["^Workspace 2$"]},
            workspace_id_pattern={"deny": [f"^{_SECOND}$"]},
        ),
        pipeline_ingestion("powerbi", tmp_path),
        [_WORKSPACES, dashboards],
    )
    assert report.excluded_by("workspaces") == {
        "Workspace 2": "workspace_name_pattern",
        "second-demo-workspace": "workspace_id_pattern",
    }
    # test_dashboard2, in the id-denied workspace.
    assert report.excluded_by("dashboards") == {
        "7D668CAD-8FFC-4505-9215-655BCA5BEBAE": "workspace_id_pattern"
    }
    # Every workspace record carries its id and type, so nothing went unjudged.
    assert report.kinds["workspaces"].warnings == ()
    assert report.kinds["dashboards"].accepted_warnings == (
        f"workspace 'second-demo-workspace' (id {_SECOND}) is excluded by "
        f"workspace_id_pattern, so ingestion reads nothing in it",
    )


@mock.patch("msal.ConfidentialClientApplication", side_effect=mock_msal_cca)
def test_workspace_type_filter_matches_ingestion(
    mock_msal: mock.MagicMock,
    pytestconfig: pytest.Config,
    tmp_path: Path,
    requests_mock: Any,
) -> None:
    register_mock_api(
        pytestconfig=pytestconfig,
        request_mock=requests_mock,
        override_data=read_mock_data(
            pytestconfig.rootpath
            / "tests/integration/powerbi/mock_data/workspace_type_filter.json"
        ),
    )
    report = assert_probe_parity(
        "powerbi",
        _recipe(workspace_type_filter=["PersonalGroup"]),
        pipeline_ingestion("powerbi", tmp_path),
        [_WORKSPACES],
    )
    assert set(report.excluded_by("workspaces").values()) == {"workspace_type_filter"}
    assert report.kinds["workspaces"].warnings == ()


@mock.patch("msal.ConfidentialClientApplication", side_effect=mock_msal_cca)
def test_report_switch_matches_ingestion(
    mock_msal: mock.MagicMock,
    pytestconfig: pytest.Config,
    tmp_path: Path,
    requests_mock: Any,
) -> None:
    register_mock_api(pytestconfig=pytestconfig, request_mock=requests_mock)
    # The shared mock has no report listings, since ingestion with reports off
    # never reads them; the probe lists them all the same.
    requests_mock.get(
        f"{_GROUPS}/{_DEMO}/reports",
        json={
            "value": [
                {
                    "id": "5b218778-e7a5-4d73-8187-f10824047715",
                    "name": "SalesMarketing",
                    "reportType": "PowerBIReport",
                }
            ]
        },
    )
    requests_mock.get(
        f"{_GROUPS}/{_SECOND}/reports",
        json={
            "value": [
                {
                    "id": "e9fd6b0b-d8c8-4265-8c44-67e183aebf97",
                    "name": "Product",
                    "reportType": "PaginatedReport",
                }
            ]
        },
    )
    _nothing_in_workspace_2(requests_mock, "reports")
    report = assert_probe_parity(
        "powerbi",
        _recipe(
            workspace_name_pattern={"deny": ["^Workspace 2$"]}, extract_reports=False
        ),
        pipeline_ingestion("powerbi", tmp_path),
        [
            _WORKSPACES,
            _in_every_workspace(
                "reports", _REPORT_PREFIX, _REPORTS_OFF, expect_empty=True
            ),
        ],
    )
    # The paginated report too: `reports` lists both types under one kind,
    # and one switch turns both off.
    assert report.excluded_by("reports") == {
        "5b218778-e7a5-4d73-8187-f10824047715": "extract_reports",
        "e9fd6b0b-d8c8-4265-8c44-67e183aebf97": "extract_reports",
    }
    assert report.kinds["reports"].accepted_warnings == (_REPORTS_OFF,)


@mock.patch("msal.ConfidentialClientApplication", side_effect=mock_msal_cca)
def test_dashboard_switch_matches_ingestion(
    mock_msal: mock.MagicMock,
    pytestconfig: pytest.Config,
    tmp_path: Path,
    requests_mock: Any,
) -> None:
    register_mock_api(pytestconfig=pytestconfig, request_mock=requests_mock)
    _nothing_in_workspace_2(requests_mock, "dashboards")
    report = assert_probe_parity(
        "powerbi",
        _recipe(
            workspace_name_pattern={"deny": ["^Workspace 2$"]},
            extract_dashboards=False,
        ),
        pipeline_ingestion("powerbi", tmp_path),
        [
            _WORKSPACES,
            _in_every_workspace(
                "dashboards", _DASHBOARD_PREFIX, _DASHBOARDS_OFF, expect_empty=True
            ),
        ],
    )
    assert report.excluded_by("dashboards") == {
        "7D668CAD-7FFC-4505-9215-655BCA5BEBAE": "extract_dashboards",
        "7D668CAD-8FFC-4505-9215-655BCA5BEBAE": "extract_dashboards",
    }
    assert report.kinds["dashboards"].accepted_warnings == (_DASHBOARDS_OFF,)
