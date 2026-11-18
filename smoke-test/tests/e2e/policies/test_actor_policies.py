import uuid
from typing import Any, Dict

import pytest

from tests.e2e.privileges.utils import (
    assign_user_to_group,
    create_group,
    create_user,
    remove_group,
    remove_policy,
    remove_user,
)
from tests.e2e.utils import (
    get_frontend_session,
    get_frontend_url,
    get_root_urn,
    login_as,
    with_test_retry,
)
from utilities.domains import Domain

pytestmark = [pytest.mark.domain(Domain.PLATFORM)]

ACTOR_POLICIES_QUERY = """query actorPolicies($urn: String!) {
    corpUser(urn: $urn) {
        policies {
            view
            policies {
                urn
                actors { users groups }
                matchedBy { type group { urn name } }
            }
        }
    }
}"""


def _query_as(session, urn: str) -> Dict[str, Any]:
    response = session.post(
        f"{get_frontend_url()}/api/v2/graphql",
        json={"query": ACTOR_POLICIES_QUERY, "variables": {"urn": urn}},
    )
    response.raise_for_status()
    return response.json()


def _create_group_policy(session, name: str, group_urn: str) -> str:
    response = session.post(
        f"{get_frontend_url()}/api/v2/graphql",
        json={
            "query": "mutation createPolicy($input: PolicyUpdateInput!) { createPolicy(input: $input) }",
            "variables": {
                "input": {
                    "type": "PLATFORM",
                    "name": name,
                    "description": "Smoke test policy",
                    "state": "ACTIVE",
                    "privileges": ["VIEW_ANALYTICS"],
                    "actors": {
                        "groups": [group_urn],
                        "resourceOwners": False,
                        "allUsers": False,
                        "allGroups": False,
                    },
                }
            },
        },
    )
    response.raise_for_status()
    return response.json()["data"]["createPolicy"]


def _policy_row(result: Dict[str, Any], policy_urn: str):
    policies = result["data"]["corpUser"]["policies"]["policies"]
    return next((p for p in policies if p["urn"] == policy_urn), None)


def test_actor_policies_for_group_member():
    suffix = uuid.uuid4().hex[:8]
    email = f"actor.policies.{suffix}@smoke.datahub.test"
    password = f"Pw-{uuid.uuid4().hex}"
    user_urn = f"urn:li:corpuser:{email}"
    admin = get_frontend_session()

    group_urn = policy_urn = None
    try:
        group_urn = create_group(admin, f"Actor Policies Group {suffix}")
        create_user(admin, email, password, remove_existing=False)
        assign_user_to_group(admin, group_urn, [user_urn])
        policy_urn = _create_group_policy(
            admin, f"Actor Policies Test {suffix}", group_urn
        )

        # Admin sees the full view, with the group as the reason the policy matches.
        @with_test_retry()
        def admin_sees_policy():
            result = _query_as(admin, user_urn)
            assert not result.get("errors"), result
            assert _policy_row(result, policy_urn) is not None
            return result

        result = admin_sees_policy()
        assert result["data"]["corpUser"]["policies"]["view"] == "FULL"
        row = _policy_row(result, policy_urn)
        assert row is not None
        group_reasons = [r for r in row["matchedBy"] if r["type"] == "GROUP"]
        assert len(group_reasons) == 1
        assert group_reasons[0]["group"]["urn"] == group_urn
        # The reason's group is resolved by its own data fetcher.
        assert group_reasons[0]["group"]["name"]

        # The user sees their own policies, without the actor lists.
        member = login_as(email, password)
        result = _query_as(member, user_urn)
        assert not result.get("errors"), result
        assert result["data"]["corpUser"]["policies"]["view"] == "TRIMMED"
        row = _policy_row(result, policy_urn)
        assert row is not None
        assert all(
            p["actors"] is None
            for p in result["data"]["corpUser"]["policies"]["policies"]
        )

        # ...but cannot read another user's policies.
        denied = _query_as(member, get_root_urn())
        assert denied.get("errors"), denied
    finally:
        if policy_urn:
            remove_policy(policy_urn, admin)
        if group_urn:
            remove_group(admin, group_urn)
        remove_user(admin, user_urn)
