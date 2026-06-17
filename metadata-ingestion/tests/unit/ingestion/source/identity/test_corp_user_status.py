from unittest.mock import MagicMock

import pytest
from okta.models import UserStatus

from datahub.ingestion.source.identity.corp_user_status import (
    CORP_USER_STATUS_ACTIVE,
    CORP_USER_STATUS_SUSPENDED,
    corp_user_info_active_from_status,
    derive_corp_user_status_from_azure_ad,
    derive_corp_user_status_from_ldap,
    derive_corp_user_status_from_okta,
    make_corp_user_status_aspect,
)
from datahub.metadata.schema_classes import CorpUserStatusClass


@pytest.mark.parametrize(
    "attrs,account_status_attr,expected",
    [
        ({}, "userAccountControl", CORP_USER_STATUS_ACTIVE),
        (
            {"userAccountControl": [b"512"]},
            "userAccountControl",
            CORP_USER_STATUS_ACTIVE,
        ),
        (
            {"userAccountControl": [b"514"]},
            "userAccountControl",
            CORP_USER_STATUS_SUSPENDED,
        ),
        (
            {"userAccountControl": [b"not-a-number"]},
            "userAccountControl",
            CORP_USER_STATUS_ACTIVE,
        ),
        ({"nsAccountLock": [b"TRUE"]}, "nsAccountLock", CORP_USER_STATUS_SUSPENDED),
        ({"nsAccountLock": [b"false"]}, "nsAccountLock", CORP_USER_STATUS_ACTIVE),
        ({"nsAccountLock": ["true"]}, "nsAccountLock", CORP_USER_STATUS_SUSPENDED),
    ],
)
def test_derive_corp_user_status_from_ldap(attrs, account_status_attr, expected):
    assert derive_corp_user_status_from_ldap(attrs, account_status_attr) == expected


@pytest.mark.parametrize(
    "okta_status,expected",
    [
        (UserStatus.ACTIVE, CORP_USER_STATUS_ACTIVE),
        (UserStatus.STAGED, CORP_USER_STATUS_ACTIVE),
        (UserStatus.PROVISIONED, CORP_USER_STATUS_ACTIVE),
        (UserStatus.SUSPENDED, CORP_USER_STATUS_SUSPENDED),
        (UserStatus.DEPROVISIONED, CORP_USER_STATUS_SUSPENDED),
        (UserStatus.LOCKED_OUT, CORP_USER_STATUS_SUSPENDED),
    ],
)
def test_derive_corp_user_status_from_okta(okta_status, expected):
    okta_user = MagicMock()
    okta_user.status = okta_status
    assert derive_corp_user_status_from_okta(okta_user) == expected


@pytest.mark.parametrize(
    "azure_ad_user,expected",
    [
        ({"accountEnabled": True}, CORP_USER_STATUS_ACTIVE),
        ({"accountEnabled": False}, CORP_USER_STATUS_SUSPENDED),
        ({}, CORP_USER_STATUS_ACTIVE),
    ],
)
def test_derive_corp_user_status_from_azure_ad(azure_ad_user, expected):
    assert derive_corp_user_status_from_azure_ad(azure_ad_user) == expected


def test_corp_user_info_active_from_status():
    assert corp_user_info_active_from_status(CORP_USER_STATUS_ACTIVE) is True
    assert corp_user_info_active_from_status(CORP_USER_STATUS_SUSPENDED) is False


def test_make_corp_user_status_aspect():
    aspect = make_corp_user_status_aspect(CORP_USER_STATUS_ACTIVE)
    assert aspect.status == CORP_USER_STATUS_ACTIVE
    assert aspect.lastModified.actor == "urn:li:corpuser:datahub"
    assert aspect.lastModified.time is not None


def test_corpuser_generate_mcp_emits_corp_user_status():
    from datahub.api.entities.corpuser.corpuser import CorpUser

    user = CorpUser(id="test.user", display_name="Test User", email="test@example.com")
    mcps = list(user.generate_mcp())
    aspect_names = [mcp.aspectName for mcp in mcps if hasattr(mcp, "aspectName")]
    assert "corpUserStatus" in aspect_names
    status_mcp = next(mcp for mcp in mcps if mcp.aspectName == "corpUserStatus")
    assert isinstance(status_mcp.aspect, CorpUserStatusClass)
    assert status_mcp.aspect.status == CORP_USER_STATUS_ACTIVE
