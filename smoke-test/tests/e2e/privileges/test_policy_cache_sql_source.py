"""Policy changes apply before the write returns when GMS loads policies from SQL.

Without POLICY_CACHE_SQL_SOURCE_ENABLED, GMS refreshes its policy cache on a timer, so a new
policy can take up to POLICY_CACHE_REFRESH_INTERVAL_SECONDS to apply. With it, the GMS that
handled the write reloads its cache before responding.

To prove that, every call here uses ``raw_post``: the plain session ``post`` and the policy
helpers in ``privileges/utils.py`` wait for Kafka consumers and the search index after each
write, which would hide a cache that is only updated later.
"""

import logging
from typing import Any, Optional

import pytest

from tests.e2e.privileges.utils import is_graphql_auth_denied, remove_policy
from tests.e2e.utils import unique_suffix
from utilities.domains import Domain
from utilities.env_vars import get_frontend_url, get_policy_cache_sql_source_enabled
from utilities.multi_user import cleanup_step_actor_user, make_step_actor_user

logger = logging.getLogger(__name__)

pytestmark = [
    pytest.mark.domain(Domain.PLATFORM),
    pytest.mark.skipif(
        not get_policy_cache_sql_source_enabled(),
        reason="needs a GMS with POLICY_CACHE_SQL_SOURCE_ENABLED=true",
    ),
]

# listPolicies requires MANAGE_POLICIES, which a new user does not have by default.
LIST_POLICIES = """
query listPolicies($input: ListPoliciesInput!) {
  listPolicies(input: $input) { total }
}
"""

CREATE_POLICY = """
mutation createPolicy($input: PolicyUpdateInput!) {
  createPolicy(input: $input)
}
"""

LIST_POLICIES_VARIABLES = {"input": {"start": 0, "count": 1}}

DELETE_POLICY = """
mutation deletePolicy($urn: String!) {
  deletePolicy(urn: $urn)
}
"""


def _graphql_no_wait(
    session, query: str, variables: Optional[dict[str, Any]] = None
) -> dict[str, Any]:
    response = session.raw_post(
        f"{get_frontend_url()}/api/v2/graphql",
        json={"query": query, "variables": variables or {}},
    )
    response.raise_for_status()
    return response.json()


def _create_user_policy_no_wait(admin_session, user_urn: str) -> str:
    res = _graphql_no_wait(
        admin_session,
        CREATE_POLICY,
        {
            "input": {
                "type": "PLATFORM",
                "name": f"policy cache smoke {unique_suffix()}",
                "description": "Grants MANAGE_POLICIES to one test user.",
                "state": "ACTIVE",
                "resources": {"filter": {"criteria": []}},
                "privileges": ["MANAGE_POLICIES"],
                "actors": {
                    "users": [user_urn],
                    "resourceOwners": False,
                    "allUsers": False,
                    "allGroups": False,
                },
            }
        },
    )
    assert not res.get("errors"), res
    return res["data"]["createPolicy"]


def test_policy_create_and_delete_apply_without_waiting(auth_session):
    user_urn, actor_session = make_step_actor_user(auth_session, "policy-cache")
    policy_urn = None
    try:
        assert is_graphql_auth_denied(
            _graphql_no_wait(actor_session, LIST_POLICIES, LIST_POLICIES_VARIABLES)
        )

        policy_urn = _create_user_policy_no_wait(auth_session, user_urn)
        # Checked straight after the createPolicy response: no sync wait, no retry.
        granted = _graphql_no_wait(
            actor_session, LIST_POLICIES, LIST_POLICIES_VARIABLES
        )
        assert not is_graphql_auth_denied(granted), granted

        deleted = _graphql_no_wait(auth_session, DELETE_POLICY, {"urn": policy_urn})
        assert not deleted.get("errors"), deleted
        policy_urn = None
        # Checked straight after the deletePolicy response: no sync wait, no retry.
        assert is_graphql_auth_denied(
            _graphql_no_wait(actor_session, LIST_POLICIES, LIST_POLICIES_VARIABLES)
        )
    finally:
        if policy_urn is not None:
            try:
                remove_policy(policy_urn, auth_session)
            except Exception:
                logger.warning("cleanup failed for %s", policy_urn, exc_info=True)
        try:
            cleanup_step_actor_user(auth_session, user_urn)
        except Exception:
            logger.warning("cleanup failed for %s", user_urn, exc_info=True)
