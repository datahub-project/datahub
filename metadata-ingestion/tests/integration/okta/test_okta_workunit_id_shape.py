import json
import pathlib
import re
from typing import Dict, List
from unittest.mock import patch

import pytest
from okta.models import Group, User

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.identity.okta import OktaSource

MASKABLE_ASPECTS = {
    "corpGroupInfo",
    "corpUserInfo",
    "corpUserStatus",
    "groupMembership",
}


def _workunits(resources: pathlib.Path, mask: bool) -> List[MetadataWorkUnit]:
    users = [User(u) for u in json.loads((resources / "okta_users.json").read_text())]
    groups = [
        Group(g) for g in json.loads((resources / "okta_groups.json").read_text())
    ]
    config = {
        "okta_domain": "mock-domain.okta.com",
        "okta_api_token": "mock-okta-token",
        "okta_profile_to_username_attr": "login",
        "okta_profile_to_username_regex": "([^@]+)",
        "mask_user_id": mask,
        "mask_group_id": mask,
    }
    with (
        patch("datahub.ingestion.source.identity.okta.OktaClient"),
        patch.object(OktaSource, "_get_okta_groups", return_value=groups),
        patch.object(OktaSource, "_get_okta_group_users", return_value=users),
        patch.object(OktaSource, "_get_okta_users", return_value=users),
    ):
        source = OktaSource.create(config, PipelineContext(run_id="test-okta-ids"))
        return list(source.get_workunits())


@pytest.mark.parametrize("mask", [True, False])
def test_okta_masked_workunit_ids(pytestconfig, mask):
    resources: pathlib.Path = pytestconfig.rootpath / "tests/integration/okta"
    ids: Dict[str, int] = {}
    for wu in _workunits(resources, mask):
        assert isinstance(wu.metadata, MetadataChangeProposalWrapper)
        aspect_name = wu.metadata.aspectName
        if aspect_name not in MASKABLE_ASPECTS:
            continue
        ids[wu.id] = ids.get(wu.id, 0) + 1
        entity = "group" if aspect_name == "corpGroupInfo" else "user"
        key = r"\d+" if mask else re.escape(wu.get_urn())
        assert re.fullmatch(rf"{entity}-snapshot-{key}-{aspect_name}", wu.id), wu.id
    assert ids and all(n == 1 for n in ids.values())
