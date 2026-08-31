"""
E2E authorization smoke tests for PrivilegeGrantAuthorizationValidator.

Entity-level authorization ignores the aspect name, so Edit Entity on a group used to be enough to
write that group's roleMembership and hand every member the Admin role. The validator adds an
aspect-level floor: roleMembership needs Manage Policies regardless of entity-level edit rights.

Edit Entity is obtained here through group ownership rather than a purpose-made policy. The
bootstrap ``asset-owners-metadata-policy`` grants owners EDIT_ENTITY and EDIT_GROUP_MEMBERS with no
resource filter, and ownership is resolved per request from the ownership aspect - so this needs no
policy creation and no wait on the GMS policy cache refresh.
"""

import logging
import time
import uuid

import pytest

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.schema_classes import (
    OwnerClass,
    OwnershipClass,
    OwnershipTypeClass,
    RoleMembershipClass,
)
from tests.consistency_utils import wait_for_writes_to_sync
from tests.privileges.utils import (
    create_group,
    create_user,
    get_current_user_info,
    remove_group,
    remove_user,
)
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
ACTOR_EMAIL = f"grant.auth.actor.{_UNIQUE}@smoke.datahub.test"
ACTOR_URN = f"urn:li:corpuser:{ACTOR_EMAIL}"
MEMBER_EMAIL = f"grant.auth.member.{_UNIQUE}@smoke.datahub.test"
MEMBER_URN = f"urn:li:corpuser:{MEMBER_EMAIL}"
USER_PASSWORD = "user"
GROUP_NAME = f"grant-auth-group-{_UNIQUE}"

ADMIN_ROLE_URN = "urn:li:dataHubRole:Admin"
READER_ROLE_URN = "urn:li:dataHubRole:Reader"
OWNERSHIP_EFFECTIVE_TIMEOUT_SECONDS = 30.0

PATCH_ENTITY_MUTATION = """
mutation patchEntity($input: PatchEntityInput!) {
  patchEntity(input: $input) {
    urn
    success
    error
  }
}
"""

BATCH_ASSIGN_ROLE_MUTATION = """
mutation batchAssignRole($input: BatchAssignRoleInput!) {
  batchAssignRole(input: $input)
}
"""

CREATE_INVITE_TOKEN_MUTATION = """
mutation createInviteToken($input: CreateInviteTokenInput!) {
  createInviteToken(input: $input) {
    inviteToken
  }
}
"""

ACCEPT_ROLE_MUTATION = """
mutation acceptRole($input: AcceptRoleInput!) {
  acceptRole(input: $input)
}
"""

ADD_GROUP_MEMBERS_MUTATION = """
mutation addGroupMembers($input: AddGroupMembersInput!) {
  addGroupMembers(input: $input)
}
"""

OWNED_GROUP_URN = ""


@pytest.fixture(scope="module", autouse=True)
def grant_auth_setup(graph_client, auth_session):
    global OWNED_GROUP_URN
    admin_session = get_frontend_session()
    admin_session = create_user(admin_session, ACTOR_EMAIL, USER_PASSWORD)
    admin_session = create_user(admin_session, MEMBER_EMAIL, USER_PASSWORD)
    OWNED_GROUP_URN = create_group(admin_session, GROUP_NAME)

    # Ownership is what grants the actor EDIT_ENTITY on the group, via the bootstrap
    # asset-owners-metadata-policy.
    graph_client.emit_mcp(
        MetadataChangeProposalWrapper(
            entityUrn=OWNED_GROUP_URN,
            aspect=OwnershipClass(
                owners=[
                    OwnerClass(owner=ACTOR_URN, type=OwnershipTypeClass.TECHNICAL_OWNER)
                ]
            ),
        )
    )
    wait_for_writes_to_sync()

    yield

    remove_group(admin_session, OWNED_GROUP_URN)
    remove_user(admin_session, ACTOR_URN)
    remove_user(admin_session, MEMBER_URN)


@with_test_retry(max_attempts=10)
def _post_graphql_as_user(email: str, password: str, payload: dict) -> dict:
    user_session = login_as(email, password)
    response = user_session.post(f"{get_frontend_url()}/api/v2/graphql", json=payload)
    response.raise_for_status()
    return response.json()


def _patch_payload(urn: str, entity_type: str, aspect_name: str, patch: list) -> dict:
    return {
        "query": PATCH_ENTITY_MUTATION,
        "variables": {
            "input": {
                "urn": urn,
                "entityType": entity_type,
                "aspectName": aspect_name,
                "patch": patch,
                "forceGenericPatch": True,
            }
        },
    }


def _add_admin_role_payload(urn: str, entity_type: str) -> dict:
    payload = _patch_payload(
        urn,
        entity_type,
        "roleMembership",
        [{"op": "ADD", "path": f"/roles/{ADMIN_ROLE_URN}", "value": ADMIN_ROLE_URN}],
    )
    payload["variables"]["input"]["arrayPrimaryKeys"] = [
        {"arrayField": "roles", "keys": []}
    ]
    return payload


def _wait_until_group_edit_effective() -> None:
    """Block until ownership-derived EDIT_ENTITY on the group is live.

    Patches an aspect the validator does not guard. Establishing this precondition is what makes
    the roleMembership denial below attributable to the validator: without it, the denial could
    equally come from PatchEntityResolver's own EDIT_ENTITY check - the same observable outcome for
    the wrong reason.
    """
    deadline = time.time() + OWNERSHIP_EFFECTIVE_TIMEOUT_SECONDS
    last: dict = {}
    while time.time() < deadline:
        last = _post_graphql_as_user(
            ACTOR_EMAIL,
            USER_PASSWORD,
            _patch_payload(
                OWNED_GROUP_URN,
                "corpGroup",
                "corpGroupEditableInfo",
                [{"op": "ADD", "path": "/description", "value": f"probe {_UNIQUE}"}],
            ),
        )
        if ((last.get("data") or {}).get("patchEntity") or {}).get("success") is True:
            return
        time.sleep(2)
    raise AssertionError(
        f"Ownership-derived EDIT_ENTITY never became effective on {OWNED_GROUP_URN}; "
        f"last response: {last}"
    )


def _assert_admin_role_not_granted(graph_client, urn: str) -> None:
    wait_for_writes_to_sync(mcp_only=True)
    role_membership = graph_client.get_aspect(urn, RoleMembershipClass)
    assert role_membership is None or ADMIN_ROLE_URN not in role_membership.roles, (
        f"{urn} was granted {ADMIN_ROLE_URN}: {role_membership}"
    )


def test_group_owner_cannot_grant_admin_role_to_owned_group(graph_client):
    """Edit Entity on a group must not be sufficient to make that group Admin.

    This is the regression the validator exists for. Before it, a group owner could add the Admin
    role to a group they own and then join it.
    """
    _wait_until_group_edit_effective()

    res = _post_graphql_as_user(
        ACTOR_EMAIL,
        USER_PASSWORD,
        _add_admin_role_payload(OWNED_GROUP_URN, "corpGroup"),
    )

    result = (res.get("data") or {}).get("patchEntity") or {}
    assert result.get("success") is False, res

    # Naming the required privilege is what attributes the denial to the aspect validator rather
    # than to the resolver's generic "is unauthorized to update entities".
    error = (result.get("error") or "").lower()
    assert "manage_policies" in error, res

    _assert_admin_role_not_granted(graph_client, OWNED_GROUP_URN)


def test_user_cannot_grant_admin_role_to_self(graph_client):
    """Defense in depth on the original SEC-1538 vector."""
    res = _post_graphql_as_user(
        ACTOR_EMAIL, USER_PASSWORD, _add_admin_role_payload(ACTOR_URN, "corpuser")
    )

    result = (res.get("data") or {}).get("patchEntity") or {}
    assert result.get("success") is False, res
    _assert_admin_role_not_granted(graph_client, ACTOR_URN)


def test_group_owner_can_add_another_member(graph_client):
    """A group owner adding someone else to their group must keep working.

    EDIT_GROUP_MEMBERS comes from the Asset Owners policy and is therefore scoped to the group the
    actor owns - not to the member being added. addGroupMembers authorizes against the group, but
    the resulting nativeGroupMembership aspect is written on the member's corpuser entity, so a
    validator that authorizes against the aspect's own URN denies a legitimate owner.
    """
    _wait_until_group_edit_effective()

    res = _post_graphql_as_user(
        ACTOR_EMAIL,
        USER_PASSWORD,
        {
            "query": ADD_GROUP_MEMBERS_MUTATION,
            "variables": {
                "input": {"groupUrn": OWNED_GROUP_URN, "userUrns": [MEMBER_URN]}
            },
        },
    )

    assert res.get("data", {}).get("addGroupMembers") is True, res


def test_group_owner_cannot_add_self_to_owned_group(graph_client):
    """A group owner must not be able to add themselves to a group they own.

    Ownership-derived EDIT_GROUP_MEMBERS is enough to manage other members, but adding yourself is
    a privilege gain when the group carries roles, so it requires Manage Users & Groups.

    The denial surfaces as a 500 rather than a 403 because GroupService wraps the validation
    exception in a RuntimeException; assert on the outcome rather than the status.
    """
    _wait_until_group_edit_effective()

    res = _post_graphql_as_user(
        ACTOR_EMAIL,
        USER_PASSWORD,
        {
            "query": ADD_GROUP_MEMBERS_MUTATION,
            "variables": {
                "input": {"groupUrn": OWNED_GROUP_URN, "userUrns": [ACTOR_URN]}
            },
        },
    )

    assert res.get("data", {}).get("addGroupMembers") is not True, res
    assert res.get("errors"), res


def test_invite_accept_still_assigns_role(graph_client):
    """The invite-accept flow must keep working for a user with no privileges.

    acceptRole has no privilege check of its own - the invite token is the authorization - so the
    grant is issued on the system operation context. If that carve-out regresses, self-service
    signup silently stops conferring a role.
    """
    admin_session = get_frontend_session()

    invited_email = f"grant.auth.invited.{_UNIQUE}@smoke.datahub.test"
    invited_urn = f"urn:li:corpuser:{invited_email}"

    token_response = admin_session.post(
        f"{get_frontend_url()}/api/v2/graphql",
        json={
            "query": CREATE_INVITE_TOKEN_MUTATION,
            "variables": {"input": {"roleUrn": READER_ROLE_URN}},
        },
    )
    token_response.raise_for_status()
    token_res = token_response.json()
    invite_token = ((token_res.get("data") or {}).get("createInviteToken") or {}).get(
        "inviteToken"
    )
    assert invite_token, token_res

    create_user(admin_session, invited_email, USER_PASSWORD)
    try:
        res = _post_graphql_as_user(
            invited_email,
            USER_PASSWORD,
            {
                "query": ACCEPT_ROLE_MUTATION,
                "variables": {"input": {"inviteToken": invite_token}},
            },
        )
        assert res.get("data", {}).get("acceptRole") is True, res

        wait_for_writes_to_sync(mcp_only=True)
        role_membership = graph_client.get_aspect(invited_urn, RoleMembershipClass)
        assert role_membership is not None, (
            f"No roleMembership written for {invited_urn}"
        )
        assert READER_ROLE_URN in role_membership.roles, role_membership
    finally:
        remove_user(admin_session, invited_urn)


def test_admin_can_assign_role_to_self(auth_session):
    """Manage Policies holders keep assigning roles, their own included - no new friction."""
    admin_session = get_frontend_session()
    admin_urn = get_current_user_info(admin_session)["urn"]

    payload = {
        "query": BATCH_ASSIGN_ROLE_MUTATION,
        "variables": {"input": {"roleUrn": ADMIN_ROLE_URN, "actors": [admin_urn]}},
    }
    response = admin_session.post(f"{get_frontend_url()}/api/v2/graphql", json=payload)
    response.raise_for_status()
    res = response.json()

    assert res.get("data", {}).get("batchAssignRole") is True, res
