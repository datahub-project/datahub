from typing import Any, Dict, List, Optional, Tuple
from unittest.mock import MagicMock, patch

import ldap as ldap_module
import pytest

from datahub._codegen.aspect import _Aspect
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.ldap import LDAPSource, LDAPSourceConfig
from datahub.metadata.schema_classes import (
    AuditStampClass,
    CorpGroupInfoClass,
    CorpUserInfoClass,
    CorpUserStatusClass,
    GroupMembershipClass,
    StatusClass,
)

FROZEN_MILLIS = 1615443388097

USER_DN = "uid=jdoe,ou=People,dc=example,dc=org"
MANAGER_DN = "uid=boss,ou=People,dc=example,dc=org"
GROUP_DN = "cn=eng,ou=Groups,dc=example,dc=org"
NO_LAST_NAME_DN = "uid=nolast,ou=People,dc=example,dc=org"
NO_CN_GROUP_DN = "ou=nocn,dc=example,dc=org"
UNKNOWN_CLASS_DN = "cn=printer,dc=example,dc=org"

USER_ATTRS: Dict[str, List[bytes]] = {
    "objectClass": [b"inetOrgPerson"],
    "sAMAccountName": [b"jdoe"],
    "cn": [b"John Doe"],
    "givenName": [b"John"],
    "sn": [b"Doe"],
    "displayName": [b"Johnny"],
    "mail": [b"jdoe@example.org"],
    "title": [b"Engineer"],
    "departmentNumber": [b"42"],
    "countryCode": [b"US"],
    "manager": [MANAGER_DN.encode()],
    "memberOf": [GROUP_DN.encode()],
    # 514 = NORMAL_ACCOUNT | ACCOUNTDISABLE
    "userAccountControl": [b"514"],
    "employeeType": [b"contractor"],
}
MANAGER_ATTRS: Dict[str, List[bytes]] = {
    "objectClass": [b"inetOrgPerson"],
    "sAMAccountName": [b"boss"],
    "mail": [b"boss@example.org"],
}
GROUP_ATTRS: Dict[str, List[bytes]] = {
    "objectClass": [b"groupOfNames"],
    "cn": [b"eng"],
    "mail": [b"eng@example.org"],
    "owner": [MANAGER_DN.encode()],
    "uniqueMember": [USER_DN.encode()],
    "name": [b"Engineering"],
    "info": [b"Engineering team"],
}
NO_LAST_NAME_ATTRS: Dict[str, List[bytes]] = {
    "objectClass": [b"person"],
    "sAMAccountName": [b"nolast"],
    "cn": [b"No Last"],
    "givenName": [b"No"],
}
NO_CN_GROUP_ATTRS: Dict[str, List[bytes]] = {
    "objectClass": [b"organizationalUnit"],
    "ou": [b"nocn"],
}
UNKNOWN_CLASS_ATTRS: Dict[str, List[bytes]] = {
    "objectClass": [b"device"],
    "cn": [b"printer"],
}


def _aspects_in_order(
    workunits: List[MetadataWorkUnit],
) -> List[Tuple[str, _Aspect]]:
    """(urn, aspect) in stream order; fails on any work unit that is not an MCP."""
    out: List[Tuple[str, _Aspect]] = []
    for wu in workunits:
        assert isinstance(wu.metadata, MetadataChangeProposalWrapper), wu.id
        assert wu.metadata.aspect is not None, wu.id
        out.append((wu.get_urn(), wu.metadata.aspect))
    return out


ALL_ENTRIES = [
    (USER_DN, USER_ATTRS),
    (GROUP_DN, GROUP_ATTRS),
    (NO_LAST_NAME_DN, NO_LAST_NAME_ATTRS),
    (NO_CN_GROUP_DN, NO_CN_GROUP_ATTRS),
    (UNKNOWN_CLASS_DN, UNKNOWN_CLASS_ATTRS),
]


def _run(
    mock_ldap: MagicMock,
    entries: List[Tuple[str, Dict[str, List[bytes]]]] = ALL_ENTRIES,
    **config_overrides: Any,
) -> Tuple[LDAPSource, list]:
    mock_ldap.LDAPError = ldap_module.LDAPError
    # parse_ldap_dn reads group and member names through ldap.dn.
    mock_ldap.dn = ldap_module.dn
    mock_ldap.SCOPE_SUBTREE = ldap_module.SCOPE_SUBTREE
    mock_ldap.SCOPE_BASE = ldap_module.SCOPE_BASE
    mock_client = MagicMock()
    mock_ldap.initialize.return_value = mock_client
    # The main search returns every entry; a user's manager lookup comes second.
    mock_client.result3.side_effect = [
        (None, entries, None, []),
        (None, [(MANAGER_DN, MANAGER_ATTRS)], None, []),
    ]
    config = LDAPSourceConfig.model_validate(
        {
            "ldap_server": "ldap://example.org",
            "ldap_user": "cn=admin,dc=example,dc=org",
            "ldap_password": "admin",
            "base_dn": "dc=example,dc=org",
            "pagination_enabled": False,
            "custom_props_list": ["employeeType"],
            **config_overrides,
        }
    )
    source = LDAPSource(PipelineContext(run_id="ldap-workunits"), config)
    return source, list(source.get_workunits())


def _expected_user_aspects(user_id: str, manager_id: str) -> List[Tuple[str, _Aspect]]:
    urn = f"urn:li:corpuser:{user_id}"
    return [
        (
            urn,
            CorpUserInfoClass(
                active=False,
                email="jdoe@example.org",
                fullName="John Doe",
                firstName="John",
                lastName="Doe",
                departmentId=42,
                departmentName="42",
                displayName="Johnny",
                countryCode="US",
                title="Engineer",
                managerUrn=f"urn:li:corpuser:{manager_id}",
                customProperties={"employeeType": "contractor"},
            ),
        ),
        (
            urn,
            CorpUserStatusClass(
                status="SUSPENDED",
                lastModified=AuditStampClass(
                    time=FROZEN_MILLIS, actor="urn:li:corpuser:datahub"
                ),
            ),
        ),
        # memberOf names groups by DN cn, even with use_email_as_username.
        (urn, GroupMembershipClass(groups=["urn:li:corpGroup:eng"])),
    ]


def _expected_group_aspect(group_id: str) -> Tuple[str, _Aspect]:
    return (
        f"urn:li:corpGroup:{group_id}",
        CorpGroupInfoClass(
            email="eng@example.org",
            admins=["urn:li:corpuser:boss"],
            members=["urn:li:corpuser:jdoe"],
            groups=[],
            displayName="Engineering",
            description="Engineering team",
        ),
    )


@pytest.mark.parametrize(
    "use_email_as_username, user_id, manager_id, group_id",
    [
        (False, "jdoe", "boss", "eng"),
        (True, "jdoe@example.org", "boss@example.org", "eng@example.org"),
    ],
)
@patch("datahub.ingestion.source.ldap.ldap")
def test_ldap_workunits(
    mock_ldap: MagicMock,
    mock_time: None,
    use_email_as_username: bool,
    user_id: str,
    manager_id: str,
    group_id: str,
) -> None:
    source, workunits = _run(mock_ldap, use_email_as_username=use_email_as_username)

    report = source.get_report()
    assert not report.failures
    assert not report.warnings
    assert list(report.dropped_dns) == [
        NO_LAST_NAME_DN,
        NO_CN_GROUP_DN,
        UNKNOWN_CLASS_DN,
    ]

    user_urn = f"urn:li:corpuser:{user_id}"
    group_urn = f"urn:li:corpGroup:{group_id}"
    # Each entity's aspects back to back, then one auto-status per urn (sorted) at the end.
    assert _aspects_in_order(workunits) == [
        *_expected_user_aspects(user_id, manager_id),
        _expected_group_aspect(group_id),
        (group_urn, StatusClass(removed=False)),
        (user_urn, StatusClass(removed=False)),
    ]


@patch("datahub.ingestion.source.ldap.ldap")
def test_ldap_user_without_groups_keeps_empty_membership(
    mock_ldap: MagicMock, mock_time: None
) -> None:
    attrs = {k: v for k, v in USER_ATTRS.items() if k not in ("memberOf", "manager")}
    source, workunits = _run(mock_ldap, entries=[(USER_DN, attrs)])
    aspects = _aspects_in_order(workunits)

    assert not source.get_report().failures
    membership: Optional[GroupMembershipClass] = next(
        (a for _, a in aspects if isinstance(a, GroupMembershipClass)), None
    )
    assert membership == GroupMembershipClass(groups=[])
