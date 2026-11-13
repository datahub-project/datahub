"""Policy changes apply before the write returns when GMS loads policies from SQL.

Without POLICY_CACHE_SQL_SOURCE_ENABLED, GMS refreshes its policy cache on a timer, so a new
policy can take up to POLICY_CACHE_REFRESH_INTERVAL_SECONDS to apply. With it, the GMS that
handled the write reloads its cache before responding, so no wait or retry is needed here.
"""

import logging
from typing import Any

import pytest

from tests.e2e.privileges.utils import (
    create_user_policy,
    is_graphql_auth_denied,
    remove_policy,
)
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
LIST_POLICIES_QUERY = """
query listPolicies($input: ListPoliciesInput!) {
  listPolicies(input: $input) { total }
}
"""


def _list_policies(session) -> dict[str, Any]:
    response = session.post(
        f"{get_frontend_url()}/api/v2/graphql",
        json={
            "query": LIST_POLICIES_QUERY,
            "variables": {"input": {"start": 0, "count": 1}},
        },
    )
    response.raise_for_status()
    return response.json()


def test_policy_create_and_delete_apply_without_waiting(auth_session):
    user_urn, actor_session = make_step_actor_user(auth_session, "policy-cache")
    policy_urn = None
    try:
        assert is_graphql_auth_denied(_list_policies(actor_session))

        policy_urn = create_user_policy(
            user_urn,
            ["MANAGE_POLICIES"],
            auth_session,
            name=f"policy cache smoke {unique_suffix()}",
        )
        # No retry: the policy must already be enforced when createPolicy returns.
        granted = _list_policies(actor_session)
        assert not is_graphql_auth_denied(granted), granted

        remove_policy(policy_urn, auth_session)
        policy_urn = None
        # No retry: the policy must already be gone when deletePolicy returns.
        assert is_graphql_auth_denied(_list_policies(actor_session))
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
