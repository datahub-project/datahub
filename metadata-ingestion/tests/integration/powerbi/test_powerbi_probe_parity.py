from pathlib import Path
from typing import Any, Dict
from unittest import mock

import pytest

from tests.integration.powerbi.test_powerbi import (
    default_source_config,
    mock_msal_cca,
    read_mock_data,
    register_mock_api,
)
from tests.test_helpers.probe_parity import (
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


def _recipe(**overrides: Any) -> Dict[str, Any]:
    config = default_source_config()
    del config["workspace_id"], config["workspace_id_pattern"]
    return {**config, "extract_workspaces_to_containers": True, **overrides}


@pytest.mark.parametrize(
    "overrides, excluded",
    [
        (
            {"workspace_name_pattern": {"deny": ["^Workspace 2$"]}},
            {"Workspace 2": "workspace_name_pattern"},
        ),
        (
            {
                "workspace_name_pattern": {"deny": ["^Workspace 2$"]},
                "workspace_id_pattern": {"deny": [f"^{_SECOND}$"]},
            },
            {
                "Workspace 2": "workspace_name_pattern",
                "second-demo-workspace": "workspace_id_pattern",
            },
        ),
    ],
)
@mock.patch("msal.ConfidentialClientApplication", side_effect=mock_msal_cca)
def test_workspace_verdicts_match_ingestion(
    mock_msal: mock.MagicMock,
    pytestconfig: pytest.Config,
    tmp_path: Path,
    requests_mock: Any,
    overrides: Dict[str, Any],
    excluded: Dict[str, str],
) -> None:
    register_mock_api(pytestconfig=pytestconfig, request_mock=requests_mock)
    report = assert_probe_parity(
        "powerbi",
        _recipe(**overrides),
        pipeline_ingestion("powerbi", tmp_path),
        [_WORKSPACES],
    )
    assert report.excluded_by("workspaces") == excluded


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
