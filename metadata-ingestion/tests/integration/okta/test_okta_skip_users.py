import json
import pathlib
from typing import Dict, List, Type
from unittest.mock import patch

from okta.models import Group, User

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.identity.okta import OktaSource
from datahub.metadata.schema_classes import (
    CorpUserInfoClass,
    CorpUserStatusClass,
    GroupMembershipClass,
    OriginClass,
    StatusClass,
    _Aspect,
)

USER_ID_NOT_IN_GROUPS = "5"

USER_ASPECTS: List[Type[_Aspect]] = [
    CorpUserInfoClass,
    CorpUserStatusClass,
    GroupMembershipClass,
    OriginClass,
    StatusClass,
]


def _aspects_by_urn(
    workunits: List[MetadataWorkUnit],
) -> Dict[str, List[Type[_Aspect]]]:
    found: Dict[str, List[Type[_Aspect]]] = {}
    for wu in workunits:
        for aspect_class in USER_ASPECTS:
            if wu.get_aspect_of_type(aspect_class) is not None:
                found.setdefault(wu.get_urn(), []).append(aspect_class)
    return found


def test_okta_skip_users_without_a_group(pytestconfig):
    resources: pathlib.Path = pytestconfig.rootpath / "tests/integration/okta"
    users = [User(u) for u in json.loads((resources / "okta_users.json").read_text())]
    groups = [
        Group(g) for g in json.loads((resources / "okta_groups.json").read_text())
    ]
    group_members = [u for u in users if u.id != USER_ID_NOT_IN_GROUPS]

    config = {
        "okta_domain": "mock-domain.okta.com",
        "okta_api_token": "mock-okta-token",
        "okta_profile_to_username_attr": "login",
        "okta_profile_to_username_regex": "([^@]+)",
        "skip_users_without_a_group": True,
    }
    with (
        patch("datahub.ingestion.source.identity.okta.OktaClient"),
        patch.object(OktaSource, "_get_okta_groups", return_value=groups),
        patch.object(OktaSource, "_get_okta_group_users", return_value=group_members),
        patch.object(OktaSource, "_get_okta_users", return_value=users),
    ):
        source = OktaSource.create(config, PipelineContext(run_id="test-okta-skip"))
        workunits = list(source.get_workunits())

    report = source.get_report()
    assert not report.failures
    aspects = _aspects_by_urn(workunits)

    assert "urn:li:corpuser:good.test" not in aspects
    assert list(report.filtered) == ["urn:li:corpuser:good.test"]
    assert sorted(aspects["urn:li:corpuser:john.doe"], key=str) == sorted(
        USER_ASPECTS, key=str
    )
