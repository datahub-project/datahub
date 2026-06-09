from typing import List
from unittest import mock

from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.quicksight.processors.users_groups import (
    UsersGroupsProcessor,
)
from datahub.ingestion.source.quicksight.quicksight_config import (
    QuickSightSourceConfig,
)
from datahub.ingestion.source.quicksight.quicksight_report import (
    QuickSightSourceReport,
)
from datahub.metadata.schema_classes import (
    CorpGroupInfoClass,
    CorpUserInfoClass,
    GroupMembershipClass,
)


def _processor(api: mock.MagicMock, config_dict=None) -> UsersGroupsProcessor:
    config = QuickSightSourceConfig.model_validate(
        {"aws_region": "us-east-1", **(config_dict or {})}
    )
    return UsersGroupsProcessor(config, QuickSightSourceReport(), api)


def _aspects(workunits: List[MetadataWorkUnit], aspect_cls) -> list:
    out = []
    for wu in workunits:
        aspect = getattr(wu.metadata, "aspect", None)
        if isinstance(aspect, aspect_cls):
            out.append((wu.get_urn(), aspect))
    return out


def test_disabled_by_default():
    api = mock.MagicMock()
    workunits = list(_processor(api).get_workunits())
    assert workunits == []
    api.list_users.assert_not_called()
    api.list_groups.assert_not_called()


def test_emits_users_groups_and_membership():
    api = mock.MagicMock()
    api.list_namespaces.return_value = [{"Name": "default"}]
    api.list_groups.return_value = [
        {"GroupName": "analysts", "Description": "Analytics team"}
    ]
    api.list_group_memberships.return_value = [{"MemberName": "Alice"}]
    api.list_users.return_value = [
        {"UserName": "Alice", "Email": "alice@example.com"},
        {"UserName": "Bob", "Email": "bob@example.com"},
    ]

    workunits = list(
        _processor(api, {"extract_users_and_groups": True}).get_workunits()
    )

    groups = {u for u, _ in _aspects(workunits, CorpGroupInfoClass)}
    assert "urn:li:corpGroup:analysts" in groups

    users = {u for u, _ in _aspects(workunits, CorpUserInfoClass)}
    assert "urn:li:corpuser:Alice" in users
    assert "urn:li:corpuser:Bob" in users

    # Alice belongs to analysts; Bob has no membership aspect.
    memberships = {u: a.groups for u, a in _aspects(workunits, GroupMembershipClass)}
    assert memberships["urn:li:corpuser:Alice"] == ["urn:li:corpGroup:analysts"]
    assert "urn:li:corpuser:Bob" not in memberships
