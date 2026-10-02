import re
from pathlib import Path
from typing import Any, Dict, Set
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
# The default mock answers a scan for these two only, so every case drops the
# third workspace, "Workspace 2".
_SECOND = "64ED5CAD-7C22-4684-8180-826122881108"
_WORKSPACE_2 = "64ED5CAD-7322-4684-8180-826122881108"
_DASHBOARD_PREFIX = "dashboards."


def _dashboard_ids(index: EmittedIndex) -> Set[str]:
    # Reports are emitted as dashboard entities too, under "reports.<id>".
    ids = (DashboardUrn.from_string(u).dashboard_id for u in index.urns("dashboard"))
    return {i[len(_DASHBOARD_PREFIX) :] for i in ids if i.startswith(_DASHBOARD_PREFIX)}


# powerbi_probe.py, `_workspace_or_raise`: `dashboards --workspace` warns on
# every run under a workspace workspace_id_pattern drops. A note on the
# parent's verdict, not a degraded fetch.
_ID_DENIED_PARENT = re.compile(
    r"workspace '[^']+' \(id [0-9A-Fa-f-]+\) is excluded by workspace_id_pattern, "
    r"so ingestion reads nothing in it"
)


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
    # The fan-out lists dashboards under every workspace, kept or not; the
    # shared mock has no dashboards listing for the one ingestion never reads.
    requests_mock.get(
        f"https://api.powerbi.com/v1.0/myorg/groups/{_WORKSPACE_2}/dashboards",
        json={"value": []},
    )
    dashboards = ParityListing(
        "dashboards",
        "dashboards",
        _dashboard_ids,
        identity=lambda r: r.attributes["id"],
        fan_out=FanOut("workspaces", "workspace"),
        accept_warnings=(_ID_DENIED_PARENT,),
    )
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
