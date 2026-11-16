import logging
import time
from collections.abc import Callable
from typing import Any, Optional

from tests.e2e.utils import get_frontend_url
from utilities.consistency_utils import wait_for_writes_to_sync

logger = logging.getLogger(__name__)

# Smoke quickstart sets POLICY_CACHE_REFRESH_INTERVAL_SECONDS=10; allow scheduled refresh
# plus async invalidate/rebuild under full-suite CI load.
DEFAULT_POLICY_CACHE_AUTH_WAIT_SECONDS = 60


def is_graphql_auth_denied(res: dict[str, Any]) -> bool:
    errors = res.get("errors", [])
    if not errors:
        return False
    code = errors[0].get("extensions", {}).get("code")
    return code in (401, 403)


def wait_until_graphql_auth_denied(
    request_fn: Callable[[], dict[str, Any]],
    *,
    timeout_seconds: float = DEFAULT_POLICY_CACHE_AUTH_WAIT_SECONDS,
    poll_interval_seconds: float = 1.0,
    description: str = "authorization denial",
) -> dict[str, Any]:
    """Poll until request_fn returns a GraphQL authorization error.

    GMS policy cache refresh is async; after deletePolicy, authorization may remain
    permissive until the cache is rebuilt.
    """
    deadline = time.time() + timeout_seconds
    last_res: dict[str, Any] = {}
    while time.time() < deadline:
        last_res = request_fn()
        if is_graphql_auth_denied(last_res):
            return last_res
        logger.info(
            "Waiting for policy cache to reflect %s (%.0fs remaining)",
            description,
            max(0.0, deadline - time.time()),
        )
        time.sleep(poll_interval_seconds)
    raise AssertionError(
        f"Timed out after {timeout_seconds}s waiting for {description}; "
        f"last response: {last_res}"
    )


def get_current_user_info(session):
    """
    Get information about the currently authenticated user (whoami equivalent).

    Returns a dict with user info and platform privileges, or None if the query fails.
    """
    me_query = {
        "query": """query me {
            me {
                corpUser {
                    urn
                    username
                    info {
                        fullName
                        email
                    }
                }
                platformPrivileges {
                    managePolicies
                    manageIdentities
                    manageUserCredentials
                    generatePersonalAccessTokens
                    viewAnalytics
                    manageSecrets
                    manageIngestion
                }
            }
        }"""
    }

    me_response = session.post(f"{get_frontend_url()}/api/v2/graphql", json=me_query)
    logger.debug(f"whoami (me query) status: {me_response.status_code}")

    if me_response.status_code == 200:
        me_data = me_response.json()
        if me_data.get("data") and me_data["data"].get("me"):
            me_info = me_data["data"]["me"]
            corp_user = me_info.get("corpUser", {})
            privileges = me_info.get("platformPrivileges", {})

            user_info = {
                "username": corp_user.get("username", "UNKNOWN"),
                "urn": corp_user.get("urn", "UNKNOWN"),
                "email": corp_user.get("info", {}).get("email", "UNKNOWN"),
                "fullName": corp_user.get("info", {}).get("fullName", "UNKNOWN"),
                "privileges": privileges,
            }

            logger.debug(f"Authenticated as user: {user_info['username']}")
            logger.debug(f"User URN: {user_info['urn']}")
            logger.debug(f"User email: {user_info['email']}")
            logger.debug(
                f"managePolicies privilege: {privileges.get('managePolicies', 'UNKNOWN')}"
            )
            logger.debug(
                f"manageIdentities privilege: {privileges.get('manageIdentities', 'UNKNOWN')}"
            )
            logger.debug(
                f"manageSecrets privilege: {privileges.get('manageSecrets', 'UNKNOWN')}"
            )
            logger.debug(
                f"manageIngestion privilege: {privileges.get('manageIngestion', 'UNKNOWN')}"
            )

            return user_info
        else:
            logger.debug(f"me query returned unexpected structure: {me_data}")
            return None
    else:
        logger.debug(f"me query failed: {me_response.text}")
        return None


BASE_PLATFORM_PRIVILEGES_POLICY_URN = "urn:li:dataHubPolicy:7"
VIEW_DATASET_SENSITIVE_POLICY_URN = "urn:li:dataHubPolicy:view-dataset-sensitive"
VIEW_ENTITY_PAGE_POLICY_URN = "urn:li:dataHubPolicy:view-entity-page-all"

_LIST_POLICIES_QUERY = """
query listPolicies($input: ListPoliciesInput!) {
  listPolicies(input: $input) {
    total
    policies {
      urn
      type
      name
      state
      description
      privileges
      actors { users groups resourceOwners resourceOwnersTypes allUsers allGroups }
      resources {
        allResources
        resources
        filter { criteria { field values { value } condition } }
      }
    }
  }
}
"""


def _fetch_policy(session, urn):
    """Read a policy's live definition, or None if it is not present."""
    start, count = 0, 100
    while True:
        response = session.post(
            f"{get_frontend_url()}/api/v2/graphql",
            json={
                "query": _LIST_POLICIES_QUERY,
                "variables": {"input": {"start": start, "count": count}},
            },
        )
        response.raise_for_status()
        result = (response.json().get("data") or {}).get("listPolicies") or {}
        policies = result.get("policies") or []
        for policy in policies:
            if policy.get("urn") == urn:
                return policy
        start += count
        if not policies or start >= result.get("total", 0):
            return None


def _policy_to_update_input(policy, state):
    """Turn a Policy read back from GraphQL into a PolicyUpdateInput, changing only state."""
    actors = policy.get("actors") or {}
    update_input = {
        "type": policy["type"],
        "state": state,
        "name": policy["name"],
        "description": policy.get("description"),
        "privileges": policy.get("privileges") or [],
        "actors": {
            "users": actors.get("users"),
            "groups": actors.get("groups"),
            "resourceOwners": bool(actors.get("resourceOwners")),
            "resourceOwnersTypes": actors.get("resourceOwnersTypes"),
            "allUsers": bool(actors.get("allUsers")),
            "allGroups": bool(actors.get("allGroups")),
        },
    }

    resources = policy.get("resources")
    if resources:
        resource_filter = resources.get("filter")
        update_input["resources"] = {
            "allResources": resources.get("allResources"),
            "resources": resources.get("resources"),
            "filter": {
                "criteria": [
                    {
                        "field": criterion["field"],
                        "values": [v["value"] for v in criterion.get("values") or []],
                        "condition": criterion.get("condition"),
                    }
                    for criterion in (resource_filter.get("criteria") or [])
                ]
            }
            if resource_filter
            else None,
        }
    return update_input


def _set_policy_status(urn, status, session):
    """Flip one bootstrap policy's state, preserving everything else about it.

    These helpers are used as save/restore pairs around tests that need an "all users" policy
    temporarily disabled. They used to write a hardcoded policy definition on the way back, which
    made "restore" a misnomer: whatever the deployment actually had was replaced by the constant
    baked into this file, for the rest of the pytest process.

    Both directions of drift were live here, and both leak into unrelated tests:

    * ``dataHubPolicy:7`` gained MANAGE_USERS_AND_GROUPS for ALL users. That is the self floor
      for adding yourself to a group, so test_group_owner_cannot_add_self_to_owned_group then saw
      its self-add legitimately succeed and reported a privilege escalation that never happened.
    * ``view-dataset-sensitive`` lost VIEW_DATASET_OPERATIONS and ``view-entity-page-all`` lost
      ES_EXPLAIN_QUERY_PRIVILEGE, silently revoking them from all users.

    Reading the live definition also keeps this correct across deployments, which ship different
    privilege sets on these bootstrap policies.
    """
    policy = _fetch_policy(session, urn)
    assert policy is not None, (
        f"{urn} not found; cannot toggle it without knowing its definition, and writing a "
        f"guessed one would change what all users can do."
    )

    response = session.post(
        f"{get_frontend_url()}/api/v2/graphql",
        json={
            "query": """mutation updatePolicy($urn: String!, $input: PolicyUpdateInput!) {\n
            updatePolicy(urn: $urn, input: $input) }""",
            "variables": {"urn": urn, "input": _policy_to_update_input(policy, status)},
        },
    )
    response.raise_for_status()
    res_data = response.json()
    assert res_data["data"]["updatePolicy"] == urn, res_data


def set_base_platform_privileges_policy_status(status, session):
    _set_policy_status(BASE_PLATFORM_PRIVILEGES_POLICY_URN, status, session)


def set_view_dataset_sensitive_info_policy_status(status, session):
    _set_policy_status(VIEW_DATASET_SENSITIVE_POLICY_URN, status, session)


def set_view_entity_profile_privileges_policy_status(status, session):
    _set_policy_status(VIEW_ENTITY_PAGE_POLICY_URN, status, session)


def create_user(session, email, password):
    """Create a native user via /signUp; ``email`` must be valid if enforceValidEmail is enabled."""
    # Remove user if exists
    res_data = remove_user(session, f"urn:li:corpuser:{email}")
    assert res_data
    assert "error" not in res_data

    # Get the invite token
    get_invite_token_json = {
        "query": """query getInviteToken($input: GetInviteTokenInput!) {\n
            getInviteToken(input: $input){\n
              inviteToken\n
            }\n
        }""",
        "variables": {"input": {}},
    }
    get_invite_token_response = session.post(
        f"{get_frontend_url()}/api/v2/graphql", json=get_invite_token_json
    )
    get_invite_token_response.raise_for_status()
    get_invite_token_res_data = get_invite_token_response.json()

    # Log the response for debugging CI failures
    logger.debug(
        f"getInviteToken response status: {get_invite_token_response.status_code}"
    )
    logger.debug(f"getInviteToken response data: {get_invite_token_res_data}")

    # Check if the response structure is as expected before accessing
    if not get_invite_token_res_data.get("data") or not get_invite_token_res_data[
        "data"
    ].get("getInviteToken"):
        logger.error(
            f"getInviteToken returned unexpected structure. Full response: {get_invite_token_res_data}"
        )
        raise RuntimeError(
            f"getInviteToken query failed or returned null. Response: {get_invite_token_res_data}"
        )

    invite_token = get_invite_token_res_data["data"]["getInviteToken"]["inviteToken"]
    assert invite_token is not None
    assert "error" not in invite_token
    # Snapshot admin cookies before /signUp overwrites them. Prefer restore over a
    # fresh admin login_as — parallel xdist workers otherwise stampede /logIn and
    # can get intermittent 400s while also invalidating each other's sessions.
    admin_cookies = session.cookies.copy()
    # Create a new user using the invite token
    sign_up_json = {
        "fullName": "Test User",
        "email": email,
        "password": password,
        "title": "Data Engineer",
        "inviteToken": invite_token,
    }
    sign_up_response = session.post(f"{get_frontend_url()}/signUp", json=sign_up_json)
    sign_up_response.raise_for_status()
    assert sign_up_response
    assert "error" not in sign_up_response
    wait_for_writes_to_sync(mae_only=True)
    # /signUp rotates cookies to the new user; put the admin cookies back on the
    # same session object so callers (including TestSessionWrapper) stay admin.
    session.cookies.clear()
    session.cookies.update(admin_cookies)
    return session


def remove_user(session, urn):
    json = {
        "query": """mutation removeUser($urn: String!) {\n
            removeUser(urn: $urn)
        }""",
        "variables": {"urn": urn},
    }
    response = session.post(f"{get_frontend_url()}/api/v2/graphql", json=json)
    response.raise_for_status()
    wait_for_writes_to_sync(mae_only=True)
    return response.json()


def create_group(session, name):
    json = {
        "query": """mutation createGroup($input: CreateGroupInput!) {\n
            createGroup(input: $input)
        }""",
        "variables": {"input": {"name": name}},
    }
    response = session.post(f"{get_frontend_url()}/api/v2/graphql", json=json)
    response.raise_for_status()
    res_data = response.json()
    assert res_data
    assert res_data["data"]
    assert res_data["data"]["createGroup"]
    wait_for_writes_to_sync(mae_only=True)
    return res_data["data"]["createGroup"]


def remove_group(session, urn):
    json = {
        "query": """mutation removeGroup($urn: String!) {\n
            removeGroup(urn: $urn)
        }""",
        "variables": {"urn": urn},
    }
    response = session.post(f"{get_frontend_url()}/api/v2/graphql", json=json)
    response.raise_for_status()
    res_data = response.json()
    assert res_data
    assert res_data["data"]
    assert res_data["data"]["removeGroup"]
    wait_for_writes_to_sync(mae_only=True)
    return res_data["data"]["removeGroup"]


def assign_user_to_group(session, group_urn, user_urns):
    json = {
        "query": """mutation addGroupMembers($groupUrn: String!, $userUrns: [String!]!) {\n
            addGroupMembers(input: { groupUrn: $groupUrn, userUrns: $userUrns })
        }""",
        "variables": {"groupUrn": group_urn, "userUrns": user_urns},
    }
    response = session.post(f"{get_frontend_url()}/api/v2/graphql", json=json)
    response.raise_for_status()
    res_data = response.json()
    assert res_data
    assert res_data["data"]
    assert res_data["data"]["addGroupMembers"]
    wait_for_writes_to_sync(mae_only=True)
    return res_data["data"]["addGroupMembers"]


def assign_role(session, role_urn, actor_urns):
    json = {
        "query": """mutation batchAssignRole($input: BatchAssignRoleInput!) {\n
            batchAssignRole(input: $input)
        }""",
        "variables": {"input": {"roleUrn": role_urn, "actors": actor_urns}},
    }

    response = session.post(f"{get_frontend_url()}/api/v2/graphql", json=json)
    response.raise_for_status()
    res_data = response.json()
    assert res_data
    assert res_data["data"]
    assert res_data["data"]["batchAssignRole"]
    wait_for_writes_to_sync(mae_only=True)
    return res_data["data"]["batchAssignRole"]


def create_metadata_policy(
    session,
    *,
    name: str,
    description: str,
    privileges: list,
    user_urn: str,
    resource_urn: Optional[str] = None,
    resource_type: Optional[str] = None,
):
    """Create an active METADATA policy scoped to a user and optional resource URN."""
    resources: dict
    if resource_urn:
        resources = {
            "filter": {
                "criteria": [
                    {
                        "field": "URN",
                        "values": [resource_urn],
                        "condition": "EQUALS",
                    }
                ]
            }
        }
    elif resource_type:
        resources = {"type": resource_type, "allResources": True}
    else:
        resources = {"filter": {"criteria": []}}

    policy = {
        "query": """mutation createPolicy($input: PolicyUpdateInput!) {
            createPolicy(input: $input) }""",
        "variables": {
            "input": {
                "type": "METADATA",
                "name": name,
                "description": description,
                "state": "ACTIVE",
                "resources": resources,
                "privileges": privileges,
                "actors": {
                    "users": [user_urn],
                    "resourceOwners": False,
                    "allUsers": False,
                    "allGroups": False,
                },
            }
        },
    }

    response = session.post(f"{get_frontend_url()}/api/v2/graphql", json=policy)
    response.raise_for_status()
    res_data = response.json()
    assert res_data.get("data") and res_data["data"].get("createPolicy"), (
        f"createPolicy failed: {res_data}"
    )
    wait_for_writes_to_sync(mae_only=True)
    return res_data["data"]["createPolicy"]


def create_domain_scoped_metadata_policy(
    session,
    *,
    name: str,
    description: str,
    privileges: list,
    user_urn: str,
    domain_urn: str,
    resource_type: Optional[str] = None,
):
    """Create an ACTIVE METADATA policy scoped to a DOMAIN resource filter."""
    criteria = [
        {
            "field": "DOMAIN",
            "values": [domain_urn],
            "condition": "EQUALS",
        }
    ]
    if resource_type:
        criteria.append(
            {
                "field": "TYPE",
                "values": [resource_type],
                "condition": "EQUALS",
            }
        )
    policy = {
        "query": """mutation createPolicy($input: PolicyUpdateInput!) {
            createPolicy(input: $input) }""",
        "variables": {
            "input": {
                "type": "METADATA",
                "name": name,
                "description": description,
                "state": "ACTIVE",
                "resources": {
                    "allResources": False,
                    "filter": {"criteria": criteria},
                },
                "privileges": privileges,
                "actors": {
                    "users": [user_urn],
                    "resourceOwners": False,
                    "allUsers": False,
                    "allGroups": False,
                },
            }
        },
    }

    response = session.post(f"{get_frontend_url()}/api/v2/graphql", json=policy)
    response.raise_for_status()
    res_data = response.json()
    assert res_data.get("data") and res_data["data"].get("createPolicy"), (
        f"createPolicy (domain-scoped) failed: {res_data}"
    )
    wait_for_writes_to_sync()
    return res_data["data"]["createPolicy"]


def create_user_policy(
    user_urn,
    privileges,
    session,
    *,
    name: str = "Test Policy Name",
    description: str = "Test Policy Description",
):
    """Create a platform policy for a single user."""
    policy = {
        "query": """mutation createPolicy($input: PolicyUpdateInput!) {\n
            createPolicy(input: $input) }""",
        "variables": {
            "input": {
                "type": "PLATFORM",
                "name": name,
                "description": description,
                "state": "ACTIVE",
                "resources": {"filter": {"criteria": []}},
                "privileges": privileges,
                "actors": {
                    "users": [user_urn],
                    "resourceOwners": False,
                    "allUsers": False,
                    "allGroups": False,
                },
            }
        },
    }

    response = session.post(f"{get_frontend_url()}/api/v2/graphql", json=policy)
    response.raise_for_status()
    res_data = response.json()

    assert res_data
    assert res_data["data"]
    assert res_data["data"]["createPolicy"]

    wait_for_writes_to_sync(mae_only=True)
    return res_data["data"]["createPolicy"]


def remove_policy(urn, session):
    remove_policy_json = {
        "query": """mutation deletePolicy($urn: String!) {\n
            deletePolicy(urn: $urn) }""",
        "variables": {"urn": urn},
    }

    response = session.post(
        f"{get_frontend_url()}/api/v2/graphql", json=remove_policy_json
    )
    response.raise_for_status()
    res_data = response.json()

    assert res_data
    assert res_data["data"]
    assert res_data["data"]["deletePolicy"]
    assert res_data["data"]["deletePolicy"] == urn

    wait_for_writes_to_sync(mae_only=True)


def list_policies(session):
    """
    List all active editable policies.
    Returns the full policy list response.
    """
    list_policy_json = {
        "query": """query listPolicies($input: ListPoliciesInput!) {
                      listPolicies(input: $input) {
                        start
                        count
                        total
                        policies {
                          urn
                          editable
                          name
                          description
                          state
                          privileges
                          __typename
                        }
                        __typename
                      }
                    }""",
        "variables": {
            "input": {
                "count": 100,
                "start": 0,
                "orFilters": [
                    {
                        "and": [
                            {
                                "field": "state",
                                "values": ["ACTIVE"],
                                "condition": "EQUAL",
                            },
                            {
                                "field": "editable",
                                "values": ["true"],
                                "condition": "EQUAL",
                            },
                        ]
                    }
                ],
            }
        },
    }

    response = session.post(
        f"{get_frontend_url()}/api/v2/graphql", json=list_policy_json
    )
    response.raise_for_status()
    res_data = response.json()

    assert res_data
    assert res_data["data"]
    assert res_data["data"]["listPolicies"]

    return res_data["data"]["listPolicies"]


def log_user_privileges(session, context=""):
    """
    Log the current user's platform privileges for debugging.
    Returns the user_info dict.
    """
    user_info = get_current_user_info(session)
    if user_info:
        privileges = user_info["privileges"]
        logger.info(f"User privileges {context}:")
        logger.info(f"  User: {user_info['username']} ({user_info['urn']})")
        logger.info(f"  managePolicies: {privileges.get('managePolicies')}")
        logger.info(f"  manageSecrets: {privileges.get('manageSecrets')}")
        logger.info(f"  manageIngestion: {privileges.get('manageIngestion')}")
        logger.info(
            f"  generatePersonalAccessTokens: {privileges.get('generatePersonalAccessTokens')}"
        )
    else:
        logger.warning(f"Could not retrieve user info {context}")
    return user_info


def log_policies(session, context=""):
    """
    Log all active editable policies for debugging.
    """
    logger.info(f"Listing active editable policies {context}")

    policies_data = list_policies(session)
    total = policies_data["total"]
    policies = policies_data["policies"]

    logger.info(f"Found {total} active editable policies")

    for policy in policies:
        logger.info(f"  - {policy['name']}")
        logger.info(f"    URN: {policy['urn']}")
        logger.info(f"    Privileges: {policy.get('privileges', [])}")
        if policy["description"]:
            desc_preview = policy["description"][:100]
            if len(policy["description"]) > 100:
                desc_preview += "..."
            logger.info(f"    Description: {desc_preview}")


def clear_polices(
    session,
    *,
    name_prefix: str | None = None,
    name_prefixes: list[str] | None = None,
) -> None:
    logger.info("Starting policy cleanup (clear_polices)")

    if name_prefixes is not None:
        prefixes = name_prefixes
    elif name_prefix is not None:
        prefixes = [name_prefix]
    else:
        prefixes = []

    policies_data = list_policies(session)
    policies = policies_data["policies"]

    deleted_count = 0
    for policy in policies:
        name = policy.get("name") or ""
        description = policy.get("description") or ""
        if prefixes:
            if not any(name.startswith(prefix) for prefix in prefixes):
                continue
        elif "test" not in name.lower() and "test" not in description.lower():
            continue
        logger.info(f"Deleting test policy: {name} ({policy['urn']})")
        remove_policy(policy["urn"], session)
        deleted_count += 1

    logger.info(f"Policy cleanup complete. Deleted {deleted_count} test policies")

    if deleted_count > 0:
        wait_for_writes_to_sync(mae_only=True)


def remove_secret(session, urn):
    remove_secret = {
        "query": """mutation deleteSecret($urn: String!) {\n
            deleteSecret(urn: $urn)\n}""",
        "variables": {"urn": urn},
    }

    response = session.post(f"{get_frontend_url()}/api/v2/graphql", json=remove_secret)
    response.raise_for_status()
    wait_for_writes_to_sync(mae_only=True)
