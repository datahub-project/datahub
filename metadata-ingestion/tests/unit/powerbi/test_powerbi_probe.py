from typing import Any, Dict, Iterator, List, Mapping, Optional, Sequence, Tuple
from unittest import mock

import pytest

from datahub.ingestion.agent.filter_check import FilterCheckResult, check_filters
from datahub.ingestion.source.powerbi.config import PowerBiEnvironment
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
    _, verdicts = _judge(kind, ["Weekly"], ["Sales"], **{switch: False})
    assert verdicts == {"Weekly": (False, switch)}
