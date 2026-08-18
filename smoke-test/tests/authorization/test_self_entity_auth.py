"""
E2E authorization smoke tests for a non-admin acting on their own corpuser entity.

Self-service profile editing is allowed by an explicit self short-circuit in ``CorpUserType``,
not by any policy. The generic write APIs have no such short-circuit and must keep denying
writes to your own user entity - otherwise any user could patch their own roleMembership and
grant themselves the Admin role.
"""

import logging
import uuid

import pytest

from datahub.metadata.schema_classes import RoleMembershipClass
from tests.consistency_utils import wait_for_writes_to_sync
from tests.privileges.utils import create_user, is_graphql_auth_denied, remove_user
from tests.utilities.domains import Domain
from tests.utils import (
    get_frontend_session,
    get_frontend_url,
    login_as,
    with_test_retry,
)

logger = logging.getLogger(__name__)

pytestmark = [
    pytest.mark.no_cypress_suite1,
    pytest.mark.domain(Domain.PLATFORM),
]

_UNIQUE = uuid.uuid4().hex[:8]
TEST_USER_EMAIL = f"self.auth.test.{_UNIQUE}@smoke.datahub.test"
TEST_USER_URN = f"urn:li:corpuser:{TEST_USER_EMAIL}"
OTHER_USER_EMAIL = f"self.auth.other.{_UNIQUE}@smoke.datahub.test"
OTHER_USER_URN = f"urn:li:corpuser:{OTHER_USER_EMAIL}"
TEST_USER_PASSWORD = "user"

ADMIN_ROLE_URN = "urn:li:dataHubRole:Admin"

UPDATE_CORP_USER_PROPERTIES_MUTATION = """
mutation updateCorpUserProperties($urn: String!, $input: CorpUserUpdateInput!) {
  updateCorpUserProperties(urn: $urn, input: $input) {
    urn
    editableProperties {
      aboutMe
      title
    }
  }
}
"""

PATCH_ENTITY_MUTATION = """
mutation patchEntity($input: PatchEntityInput!) {
  patchEntity(input: $input) {
    urn
    success
    error
  }
}
"""


@pytest.fixture(scope="module", autouse=True)
def self_auth_setup(auth_session):
    admin_session = get_frontend_session()
    admin_session = create_user(admin_session, TEST_USER_EMAIL, TEST_USER_PASSWORD)
    admin_session = create_user(admin_session, OTHER_USER_EMAIL, TEST_USER_PASSWORD)

    yield

    remove_user(admin_session, TEST_USER_URN)
    remove_user(admin_session, OTHER_USER_URN)


@with_test_retry(max_attempts=10)
def _post_graphql_as_user(email: str, password: str, payload: dict) -> dict:
    user_session = login_as(email, password)
    response = user_session.post(f"{get_frontend_url()}/api/v2/graphql", json=payload)
    response.raise_for_status()
    return response.json()


def _update_profile_payload(target_urn: str, about_me: str) -> dict:
    return {
        "query": UPDATE_CORP_USER_PROPERTIES_MUTATION,
        "variables": {
            "urn": target_urn,
            "input": {"aboutMe": about_me, "title": "Analyst"},
        },
    }


def test_user_can_edit_own_profile():
    """A user with no granted privileges can still edit their own profile."""
    about_me = f"Self edit {_UNIQUE}"
    res = _post_graphql_as_user(
        TEST_USER_EMAIL,
        TEST_USER_PASSWORD,
        _update_profile_payload(TEST_USER_URN, about_me),
    )

    assert not is_graphql_auth_denied(res), res
    editable = ((res.get("data") or {}).get("updateCorpUserProperties") or {}).get(
        "editableProperties"
    ) or {}
    assert editable.get("aboutMe") == about_me, res
    assert editable.get("title") == "Analyst", res


def test_user_cannot_edit_another_users_profile():
    """The self short-circuit must not extend to other users' profiles."""
    res = _post_graphql_as_user(
        TEST_USER_EMAIL,
        TEST_USER_PASSWORD,
        _update_profile_payload(OTHER_USER_URN, f"Cross edit {_UNIQUE}"),
    )

    assert is_graphql_auth_denied(res), res


def test_user_cannot_patch_own_role_membership(graph_client):
    """A user must not be able to grant themselves the Admin role on their own entity.

    patchEntity is gated on EDIT_ENTITY, which the default self policy must never grant.
    """
    payload = {
        "query": PATCH_ENTITY_MUTATION,
        "variables": {
            "input": {
                "urn": TEST_USER_URN,
                "entityType": "corpuser",
                "aspectName": "roleMembership",
                "patch": [
                    {
                        "op": "ADD",
                        "path": f"/roles/{ADMIN_ROLE_URN}",
                        "value": ADMIN_ROLE_URN,
                    }
                ],
                "arrayPrimaryKeys": [{"arrayField": "roles", "keys": []}],
                "forceGenericPatch": True,
            }
        },
    }
    res = _post_graphql_as_user(TEST_USER_EMAIL, TEST_USER_PASSWORD, payload)

    # PatchEntityResolver reports authorization failures in the payload rather than as a
    # GraphQL error, so check the reason too - success=False alone would also be satisfied
    # by an unrelated failure.
    result = (res.get("data") or {}).get("patchEntity") or {}
    assert result.get("success") is False, res
    assert "unauthorized" in (result.get("error") or "").lower(), res

    wait_for_writes_to_sync(mcp_only=True)
    role_membership = graph_client.get_aspect(TEST_USER_URN, RoleMembershipClass)
    assert role_membership is None or ADMIN_ROLE_URN not in role_membership.roles, (
        f"Test user was granted {ADMIN_ROLE_URN}: {role_membership}"
    )
