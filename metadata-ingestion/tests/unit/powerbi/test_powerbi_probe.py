from typing import Any, Dict, Iterator
from unittest import mock

import pytest

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
