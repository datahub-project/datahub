import logging
import time
from typing import Any, Dict, List

import pytest

from tests.consistency_utils import wait_for_writes_to_sync
from tests.privileges.utils import create_user, remove_user
from tests.utils import (
    execute_graphql,
    get_admin_credentials,
    get_frontend_url,
    get_root_urn,
    login_as,
    with_test_retry,
)

logger = logging.getLogger(__name__)
TEST_POLICY_NAME = "Updated Platform Policy"
TEST_DENY_POLICY_NAME = "Test Deny Policy"
PERF_DENY_POLICY_PREFIX = "PerfDeny-EDIT_ENTITY_TAGS-"
PERF_NUM_QUERIES = 25
PERF_LATENCY_RATIO_THRESHOLD = 2
# Absolute slack added on top of the ratio. listPolicies baselines on a quiet local stack
# can be a few ms, where any fixed jitter (GC, ES refresh after the policy writes) dwarfs the
# real per-DENY cost and makes a pure ratio meaningless. The allowed budget is the larger of
# (baseline x ratio) and (baseline + this floor).
PERF_LATENCY_ABS_FLOOR_SECONDS = 0.05
PERF_NUM_DENY_POLICIES = 20
PERF_GROUP_PREFIX = "PerfDenyGroup-"
PERF_DECOY_GROUP_PREFIX = "PerfDenyDecoy-"
PERF_DENY_USER_EMAIL = "perf_deny_user@example.com"
PERF_DENY_USER_PASSWORD = "perf_deny_user_pw"

_CREATE_POLICY_QUERY = """mutation createPolicy($input: PolicyUpdateInput!) {
        createPolicy(input: $input) }"""
_DELETE_POLICY_QUERY = """mutation deletePolicy($urn: String!) {
        deletePolicy(urn: $urn) }"""
_CREATE_GROUP_QUERY = """mutation createGroup($input: CreateGroupInput!) {
        createGroup(input: $input) }"""
_ADD_GROUP_MEMBERS_QUERY = """mutation addGroupMembers($groupUrn: String!, $userUrns: [String!]!) {
        addGroupMembers(input: { groupUrn: $groupUrn, userUrns: $userUrns }) }"""
_REMOVE_GROUP_QUERY = """mutation removeGroup($urn: String!) {
        removeGroup(urn: $urn) }"""
_LIST_POLICIES_PERF_QUERY = """query listPolicies($input: ListPoliciesInput!) {
        listPolicies(input: $input) { start count total } }"""


@pytest.fixture(scope="module", autouse=True)
def test_frontend_list_policies(auth_session):
    """Fixture to execute setup before and tear down after all tests are run"""
    res_data = listPolicies(auth_session)

    assert res_data
    assert res_data["data"]
    assert res_data["data"]["listPolicies"]
    assert res_data["data"]["listPolicies"]["start"] == 0
    assert res_data["data"]["listPolicies"]["count"] > 0
    assert len(res_data["data"]["listPolicies"]["policies"]) > 0

    # Verify that policy to be created does not exist before the test.
    # If it does, this test class's state is tainted
    result = filter(
        lambda x: x["name"] == TEST_POLICY_NAME,
        res_data["data"]["listPolicies"]["policies"],
    )
    assert len(list(result)) == 0

    # Run remaining tests.
    yield

    res_data = listPolicies(auth_session)

    assert res_data
    assert res_data["data"]
    assert res_data["data"]["listPolicies"]

    # Verify that policy that was created is no longer in the list
    result = filter(
        lambda x: x["name"] == TEST_POLICY_NAME,
        res_data["data"]["listPolicies"]["policies"],
    )
    assert len(list(result)) == 0


@with_test_retry()
def _ensure_policy_present(auth_session, new_urn):
    res_data = listPolicies(auth_session)

    assert res_data
    assert res_data["data"]
    assert res_data["data"]["listPolicies"]

    # Verify that the updated policy appears in the list and has the appropriate changes
    result = list(
        filter(
            lambda x: x["urn"] == new_urn, res_data["data"]["listPolicies"]["policies"]
        )
    )
    logger.info(result)

    assert len(result) == 1
    assert result[0]["description"] == "Updated Metadaata Policy"
    assert result[0]["privileges"] == ["EDIT_ENTITY_TAGS", "EDIT_ENTITY_GLOSSARY_TERMS"]
    assert result[0]["actors"]["allUsers"]


def test_frontend_policy_operations(auth_session):
    create_policy_query = """mutation createPolicy($input: PolicyUpdateInput!) {
            createPolicy(input: $input) }"""
    create_policy_variables: Dict[str, Any] = {
        "input": {
            "type": "METADATA",
            "name": "Test Metadata Policy",
            "description": "My Metadaata Policy",
            "state": "ACTIVE",
            "resources": {"type": "dataset", "allResources": True},
            "privileges": ["EDIT_ENTITY_TAGS"],
            "actors": {
                "users": [get_root_urn()],
                "resourceOwners": False,
                "allUsers": False,
                "allGroups": False,
            },
        }
    }

    res_data = execute_graphql(
        auth_session, create_policy_query, create_policy_variables
    )

    assert res_data["data"]["createPolicy"]

    new_urn = res_data["data"]["createPolicy"]

    update_policy_query = """mutation updatePolicy($urn: String!, $input: PolicyUpdateInput!) {
            updatePolicy(urn: $urn, input: $input) }"""
    update_policy_variables: Dict[str, Any] = {
        "urn": new_urn,
        "input": {
            "type": "METADATA",
            "state": "ACTIVE",
            "name": "Test Metadata Policy",
            "description": "Updated Metadaata Policy",
            "privileges": ["EDIT_ENTITY_TAGS", "EDIT_ENTITY_GLOSSARY_TERMS"],
            "actors": {
                "resourceOwners": False,
                "allUsers": True,
                "allGroups": False,
            },
        },
    }

    res_data = execute_graphql(
        auth_session, update_policy_query, update_policy_variables
    )

    # Check updated was submitted successfully
    assert res_data["data"]["updatePolicy"]
    assert res_data["data"]["updatePolicy"] == new_urn

    _ensure_policy_present(auth_session, new_urn)

    # Now test that the policy can be deleted
    delete_policy_query = """mutation deletePolicy($urn: String!) {
            deletePolicy(urn: $urn) }"""
    delete_policy_variables: Dict[str, Any] = {"urn": new_urn}

    res_data = execute_graphql(
        auth_session, delete_policy_query, delete_policy_variables
    )

    res_data = listPolicies(auth_session)

    assert res_data
    assert res_data["data"]
    assert res_data["data"]["listPolicies"]

    # Verify that the URN is no longer in the list
    result = filter(
        lambda x: x["urn"] == new_urn,
        res_data["data"]["listPolicies"]["policies"],
    )
    assert len(list(result)) == 0


def test_frontend_deny_policy_operations(auth_session):
    """Verify that a DENY policy can be created, listed, and deleted."""
    create_policy_query = """mutation createPolicy($input: PolicyUpdateInput!) {
            createPolicy(input: $input) }"""
    create_policy_variables: Dict[str, Any] = {
        "input": {
            "type": "METADATA",
            "name": TEST_DENY_POLICY_NAME,
            "description": "A deny policy for smoke testing",
            "state": "ACTIVE",
            "effect": "DENY",
            "resources": {"type": "dataset", "allResources": True},
            "privileges": ["EDIT_ENTITY_TAGS"],
            "actors": {
                "users": [get_root_urn()],
                "resourceOwners": False,
                "allUsers": False,
                "allGroups": False,
            },
        }
    }

    res_data = execute_graphql(
        auth_session, create_policy_query, create_policy_variables
    )

    assert res_data["data"]["createPolicy"]
    deny_urn = res_data["data"]["createPolicy"]

    _ensure_deny_policy_present(auth_session, deny_urn)

    delete_policy_query = """mutation deletePolicy($urn: String!) {
            deletePolicy(urn: $urn) }"""
    execute_graphql(auth_session, delete_policy_query, {"urn": deny_urn})

    res_data = listPolicies(auth_session)
    result = list(
        filter(
            lambda x: x["urn"] == deny_urn,
            res_data["data"]["listPolicies"]["policies"],
        )
    )
    assert len(result) == 0


@with_test_retry()
def _ensure_deny_policy_present(auth_session, deny_urn):
    res_data = listPolicies(auth_session)

    assert res_data
    assert res_data["data"]
    assert res_data["data"]["listPolicies"]

    result = list(
        filter(
            lambda x: x["urn"] == deny_urn,
            res_data["data"]["listPolicies"]["policies"],
        )
    )
    assert len(result) == 1
    assert result[0]["name"] == TEST_DENY_POLICY_NAME
    assert result[0]["effect"] == "DENY"


def _post_graphql(session, query: str, variables: Dict[str, Any]) -> Dict[str, Any]:
    """Execute a GraphQL request by posting directly to the frontend URL.

    Unlike ``execute_graphql``, this does not require ``session.frontend_url()``, so it
    works with the plain ``requests.Session`` returned by ``login_as`` / ``create_user``
    as well as the module ``auth_session`` wrapper."""
    response = session.post(
        f"{get_frontend_url()}/api/v2/graphql",
        json={"query": query, "variables": variables},
    )
    response.raise_for_status()
    res_data = response.json()
    assert res_data, "GraphQL response is empty"
    assert "errors" not in res_data, f"GraphQL errors: {res_data.get('errors')}"
    return res_data


def _timed_list_policies(session) -> List[float]:
    """Return trimmed per-query ``listPolicies`` durations (seconds). Posts directly so
    that both the module ``auth_session`` and a plain ``login_as`` session work."""
    durations = []
    payload = {
        "query": _LIST_POLICIES_PERF_QUERY,
        "variables": {"input": {"start": 0, "count": 20}},
    }
    for _ in range(PERF_NUM_QUERIES):
        t0 = time.perf_counter()
        response = session.post(f"{get_frontend_url()}/api/v2/graphql", json=payload)
        response.raise_for_status()
        durations.append(time.perf_counter() - t0)
    durations.sort()
    trim = max(1, PERF_NUM_QUERIES // 10)
    return durations[:-trim]


def _median(values: List[float]) -> float:
    return values[len(values) // 2]


def _create_deny_policy(
    session,
    *,
    name: str,
    privileges: List[str],
    policy_type: str = "METADATA",
    user_urns=None,
    group_urns=None,
) -> str:
    """Create an ACTIVE DENY policy. METADATA policies are scoped to all datasets;
    PLATFORM policies (e.g. for MANAGE_POLICIES) carry an empty resource filter."""
    resources = (
        {"filter": {"criteria": []}}
        if policy_type == "PLATFORM"
        else {"type": "dataset", "allResources": True}
    )
    variables: Dict[str, Any] = {
        "input": {
            "type": policy_type,
            "name": name,
            "description": "Perf smoke DENY policy",
            "state": "ACTIVE",
            "effect": "DENY",
            "resources": resources,
            "privileges": privileges,
            "actors": {
                "users": user_urns or [],
                "groups": group_urns or [],
                "resourceOwners": False,
                "allUsers": False,
                "allGroups": False,
            },
        }
    }
    res = _post_graphql(session, _CREATE_POLICY_QUERY, variables)
    return res["data"]["createPolicy"]


def _assert_listpolicies_not_slowed_by_denies(session, deny_setup) -> None:
    """Measure ``listPolicies`` median latency for ``session`` before and after the
    ``deny_setup`` callback creates its DENY policies. The with-DENY median must stay
    within ``max(baseline x PERF_LATENCY_RATIO_THRESHOLD, baseline + floor)`` — the
    absolute floor keeps the check meaningful when the baseline is only a few ms and
    dominated by jitter rather than real per-DENY work. Each phase is preceded by a
    discarded warm-up run so we never compare a cold/just-written stack against a warm one."""
    _timed_list_policies(session)
    baseline_median = _median(_timed_list_policies(session))

    deny_setup()
    # The authorizer cache refresh triggered by the policy upserts is async.
    wait_for_writes_to_sync()
    time.sleep(3)

    _timed_list_policies(session)
    with_denies_median = _median(_timed_list_policies(session))
    ratio = with_denies_median / baseline_median if baseline_median > 0 else 0
    allowed = max(
        baseline_median * PERF_LATENCY_RATIO_THRESHOLD,
        baseline_median + PERF_LATENCY_ABS_FLOOR_SECONDS,
    )
    logger.info(
        "DENY perf: baseline median=%.2fms, with DENY policies median=%.2fms "
        "(ratio=%.2fx, allowed<=%.2fms)",
        baseline_median * 1000,
        with_denies_median * 1000,
        ratio,
        allowed * 1000,
    )
    assert with_denies_median <= allowed, (
        f"listPolicies median grew from {baseline_median * 1000:.2f}ms to "
        f"{with_denies_median * 1000:.2f}ms after adding DENY policies ({ratio:.2f}x), "
        f"exceeding allowed {allowed * 1000:.2f}ms."
    )


def test_deny_policy_perf_smoke(auth_session):
    """DENY policies on an unrelated privilege should not measurably slow down
    requests authorizing a different privilege. listPolicies authorizes
    MANAGE_POLICIES; the DENY policies created here target EDIT_ENTITY_TAGS."""
    deny_urns: List[str] = []

    def _setup():
        for i in range(PERF_NUM_DENY_POLICIES):
            deny_urns.append(
                _create_deny_policy(
                    auth_session,
                    name=f"{PERF_DENY_POLICY_PREFIX}{i}",
                    privileges=["EDIT_ENTITY_TAGS"],
                    user_urns=[get_root_urn()],
                )
            )

    try:
        _assert_listpolicies_not_slowed_by_denies(auth_session, _setup)
    finally:
        for urn in deny_urns:
            try:
                execute_graphql(auth_session, _DELETE_POLICY_QUERY, {"urn": urn})
            except Exception:
                logger.warning("Failed to clean up perf DENY policy %s", urn)


@pytest.mark.parametrize("num_groups", [10, 100])
def test_deny_policy_perf_with_many_group_memberships(auth_session, num_groups):
    """A corpuser who belongs to many groups must not see authorization latency blow up
    when DENY policies exist on the very privilege being authorized. The user is granted
    MANAGE_POLICIES via one of its groups (so listPolicies authorizes), and the DENY
    policies also target MANAGE_POLICIES but via *decoy* groups the user is not a member
    of. Because DENY is privilege-indexed, every one of these denies lands in the same
    ``DENY_MANAGE_POLICIES`` bucket and is evaluated on every listPolicies request — yet
    none match the user, so the ALLOW grant still wins. This is the realistic worst case
    for DENY cost (hot-path evaluation against a many-group actor), unlike denies on an
    unrelated privilege which the index skips entirely."""
    admin_username, admin_password = get_admin_credentials()
    admin_session = login_as(admin_username, admin_password)
    # create_user re-logs-in as admin and returns the fresh session; never mutate the
    # shared module auth_session here.
    admin_session = create_user(
        admin_session, PERF_DENY_USER_EMAIL, PERF_DENY_USER_PASSWORD
    )
    user_urn = f"urn:li:corpuser:{PERF_DENY_USER_EMAIL}"

    group_urns: List[str] = []
    decoy_group_urns: List[str] = []
    policy_urns: List[str] = []
    try:
        for i in range(num_groups):
            res = _post_graphql(
                admin_session,
                _CREATE_GROUP_QUERY,
                {"input": {"name": f"{PERF_GROUP_PREFIX}{num_groups}-{i}"}},
            )
            group_urn = res["data"]["createGroup"]
            group_urns.append(group_urn)
            _post_graphql(
                admin_session,
                _ADD_GROUP_MEMBERS_QUERY,
                {"groupUrn": group_urn, "userUrns": [user_urn]},
            )

        # Decoy groups the user is NOT a member of, used to scope the DENY policies so
        # they are evaluated on the hot path but never actually match the actor.
        for i in range(PERF_NUM_DENY_POLICIES):
            res = _post_graphql(
                admin_session,
                _CREATE_GROUP_QUERY,
                {"input": {"name": f"{PERF_DECOY_GROUP_PREFIX}{num_groups}-{i}"}},
            )
            decoy_group_urns.append(res["data"]["createGroup"])

        # Grant MANAGE_POLICIES to the user via its first group so listPolicies authorizes.
        grant = _post_graphql(
            admin_session,
            _CREATE_POLICY_QUERY,
            {
                "input": {
                    "type": "PLATFORM",
                    "name": "PerfDeny-grant-manage-policies",
                    "description": "Perf smoke MANAGE_POLICIES grant via group",
                    "state": "ACTIVE",
                    "effect": "ALLOW",
                    "resources": {"filter": {"criteria": []}},
                    "privileges": ["MANAGE_POLICIES"],
                    "actors": {
                        "users": [],
                        "groups": [group_urns[0]],
                        "resourceOwners": False,
                        "allUsers": False,
                        "allGroups": False,
                    },
                }
            },
        )
        policy_urns.append(grant["data"]["createPolicy"])
        wait_for_writes_to_sync()
        time.sleep(3)

        user_session = login_as(PERF_DENY_USER_EMAIL, PERF_DENY_USER_PASSWORD)

        def _setup():
            for i in range(PERF_NUM_DENY_POLICIES):
                policy_urns.append(
                    _create_deny_policy(
                        admin_session,
                        name=f"PerfDenyHot-MANAGE_POLICIES-{num_groups}-{i}",
                        privileges=["MANAGE_POLICIES"],
                        policy_type="PLATFORM",
                        group_urns=[decoy_group_urns[i % len(decoy_group_urns)]],
                    )
                )

        _assert_listpolicies_not_slowed_by_denies(user_session, _setup)
    finally:
        for urn in policy_urns:
            try:
                _post_graphql(admin_session, _DELETE_POLICY_QUERY, {"urn": urn})
            except Exception:
                logger.warning("Failed to clean up perf policy %s", urn)
        for urn in group_urns + decoy_group_urns:
            try:
                _post_graphql(admin_session, _REMOVE_GROUP_QUERY, {"urn": urn})
            except Exception:
                logger.warning("Failed to clean up perf group %s", urn)
        try:
            remove_user(admin_session, user_urn)
        except Exception:
            logger.warning("Failed to clean up perf user %s", user_urn)


def listPolicies(auth_session):
    query = """query listPolicies($input: ListPoliciesInput!) {
            listPolicies(input: $input) {
                start
                count
                total
                policies {
                    urn
                    type
                    name
                    description
                    state
                    effect
                    resources {
                      type
                      allResources
                      resources
                    }
                    privileges
                    actors {
                      users
                      groups
                      allUsers
                      allGroups
                      resourceOwners
                    }
                    editable
                }
            }
        }"""
    variables: Dict[str, Any] = {
        "input": {
            "start": 0,
            "count": 20,
        }
    }

    return execute_graphql(auth_session, query, variables)
