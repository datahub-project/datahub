import json
import pathlib
from typing import Any, Dict, List
from unittest.mock import patch

import pytest

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.identity.azure_ad import AzureADSource

SOURCE = "datahub.ingestion.source.identity.azure_ad.AzureADSource"
RESOURCES = pathlib.Path(__file__).parent
TENANT = "00000000-0000-0000-0000-000000000000"
CONFIG = {
    "client_id": TENANT,
    "tenant_id": TENANT,
    "client_secret": "client_secret",
    "authority": f"https://login.microsoftonline.com/{TENANT}",
    "token_url": f"https://login.microsoftonline.com/{TENANT}/oauth2/token",
}


def _ids(overrides: Dict[str, Any]) -> List[str]:
    users = json.loads((RESOURCES / "azure_ad_users.json").read_text())[:1]
    groups = json.loads((RESOURCES / "azure_ad_groups.json").read_text())[:1]
    with (
        patch(f"{SOURCE}.get_token", return_value="token"),
        patch(f"{SOURCE}._get_azure_ad_users", return_value=[users]),
        patch(f"{SOURCE}._get_azure_ad_groups", return_value=[groups]),
        patch(f"{SOURCE}._get_azure_ad_group_members", return_value=[users]),
    ):
        source = AzureADSource.create(
            {**CONFIG, **overrides}, PipelineContext(run_id="test-azure-ad")
        )
        return [wu.id for wu in source.get_workunits_internal()]


# Both user paths: `ingest_users`, and users found through group membership.
@pytest.mark.parametrize(
    "overrides",
    [{}, {"ingest_users": False, "ingest_groups_users": True}],
)
def test_masked_workunit_ids(overrides: Dict[str, Any]) -> None:
    assert _ids(overrides) == [
        "group-1-corpGroupInfo",
        "group-origin-1",
        "group-status-1",
        "user-snapshot-1-corpUserInfo",
        "user-snapshot-1-corpUserStatus",
        "user-snapshot-1-groupMembership",
        "user-origin-1",
        "user-status-1",
    ]


def test_unmasked_workunit_ids() -> None:
    ids = _ids({"mask_group_id": False, "mask_user_id": False})
    group, user = (
        "urn:li:corpGroup:groupDisplayName1",
        "urn:li:corpuser:johngreen@acryl.io",
    )
    assert ids == [
        f"{group}-corpGroupInfo",
        f"group-origin-{group}",
        f"group-status-{group}",
        f"user-snapshot-{user}-corpUserInfo",
        f"user-snapshot-{user}-corpUserStatus",
        f"user-snapshot-{user}-groupMembership",
        f"user-origin-{user}",
        f"user-status-{user}",
    ]
