import logging
import os
import time
from typing import Dict, List, Set

import pytest

from tests.tokens.token_utils import (
    assert_graphql_mutation_succeeded,
    removeUser,
    revoke_tokens_matching,
    token_name_filter,
    wait_for_no_tokens_matching,
    wait_for_user_in_list,
)
from tests.utils import (
    get_admin_credentials,
    get_frontend_url,
    login_as,
    wait_for_writes_to_sync,
)

logger = logging.getLogger(__name__)

pytestmark = pytest.mark.no_cypress_suite1

# Disable telemetry
os.environ["DATAHUB_TELEMETRY_ENABLED"] = "false"

(admin_user, admin_pass) = get_admin_credentials()
# Valid email for auth.native.signUp.enforceValidEmail (Play EmailValidator).
AUDIT_SUITE_USER_EMAIL = "audit.events.user@smoke.datahub.test"
AUDIT_SUITE_USER_URN = f"urn:li:corpuser:{AUDIT_SUITE_USER_EMAIL}"
AUDIT_SUITE_TOKEN_NAME = "audit-suite-token"


@pytest.fixture()
def suite_token_filter():
    return [token_name_filter(AUDIT_SUITE_TOKEN_NAME)]


@pytest.fixture(scope="module", autouse=True)
def custom_user_setup():
    admin_session = login_as(admin_user, admin_pass)
    try:
        """Fixture to execute setup before and tear down after all tests are run"""
        res_data = removeUser(admin_session, AUDIT_SUITE_USER_URN)
        assert res_data
        assert "error" not in res_data
        wait_for_writes_to_sync()
        wait_for_user_in_list(admin_session, AUDIT_SUITE_USER_EMAIL, present=False)

        # Regenerate invite token (createInviteToken) so sign-up gets a fresh token.
        get_invite_token_json = {
            "query": """mutation createInviteToken($input: CreateInviteTokenInput!) {
                createInviteToken(input: $input){
                  inviteToken
                }
            }""",
            "variables": {"input": {}},
        }

        get_invite_token_response = admin_session.post(
            f"{get_frontend_url()}/api/v2/graphql", json=get_invite_token_json
        )
        get_invite_token_response.raise_for_status()
        get_invite_token_res_data = get_invite_token_response.json()

        assert get_invite_token_res_data
        assert get_invite_token_res_data["data"]
        invite_token = get_invite_token_res_data["data"]["createInviteToken"][
            "inviteToken"
        ]
        assert invite_token is not None

        # Pass the invite token when creating the user
        sign_up_json = {
            "fullName": "Test User",
            "email": AUDIT_SUITE_USER_EMAIL,
            "password": "user",
            "title": "Data Engineer",
            "inviteToken": invite_token,
        }

        sign_up_response = admin_session.post(
            f"{get_frontend_url()}/signUp", json=sign_up_json
        )
        sign_up_response.raise_for_status()
        # signUp will override the session cookie to the new user to be signed up.
        admin_session.cookies.clear()
        admin_session = login_as(admin_user, admin_pass)

        wait_for_writes_to_sync()
        wait_for_user_in_list(admin_session, AUDIT_SUITE_USER_EMAIL, present=True)
        admin_session.cookies.clear()

        yield

    finally:
        # Delete created user
        admin_session = login_as(admin_user, admin_pass)
        res_data = removeUser(admin_session, AUDIT_SUITE_USER_URN)
        assert res_data
        assert res_data["data"]
        assert res_data["data"]["removeUser"] is True
        wait_for_writes_to_sync()
        wait_for_user_in_list(admin_session, AUDIT_SUITE_USER_EMAIL, present=False)


@pytest.fixture(autouse=True)
def access_token_setup(suite_token_filter):
    """Revoke only this suite's tokens so parallel workers do not interfere."""
    admin_session = login_as(admin_user, admin_pass)

    wait_for_no_tokens_matching(admin_session, suite_token_filter)

    yield

    revoke_tokens_matching(admin_session, suite_token_filter)


def test_audit_token_events():
    user_session = login_as(AUDIT_SUITE_USER_EMAIL, "user")

    # Normal user should be able to generate token for himself.
    res_data = generateAccessToken_v2(user_session, AUDIT_SUITE_USER_URN)
    assert_graphql_mutation_succeeded(res_data)
    assert res_data["data"]["createAccessToken"]
    assert res_data["data"]["createAccessToken"]["accessToken"]
    assert (
        res_data["data"]["createAccessToken"]["metadata"]["actorUrn"]
        == AUDIT_SUITE_USER_URN
    )
    user_token_id = res_data["data"]["createAccessToken"]["metadata"]["id"]
    # Sleep for eventual consistency
    wait_for_writes_to_sync(consumer_group="datahub-usage-event-consumer-job-client")

    # User should be able to revoke his own token
    res_data = revokeAccessToken(user_session, user_token_id)
    assert res_data
    assert res_data["data"]
    assert res_data["data"]["revokeAccessToken"]
    assert res_data["data"]["revokeAccessToken"] is True
    wait_for_writes_to_sync(consumer_group="datahub-usage-event-consumer-job-client")

    # Audit events for create & revoke should show
    res_data = searchForAuditEvents(
        user_session,
        2,
        [
            "CreateAccessTokenEvent",
            "RevokeAccessTokenEvent",
        ],
        [AUDIT_SUITE_USER_URN],
        [],
    )
    logger.info(res_data)
    assert res_data
    assert res_data["usageEvents"]
    assert len(res_data["usageEvents"]) == 2
    assert res_data["usageEvents"][0]["eventType"] == "RevokeAccessTokenEvent"
    assert (
        res_data["usageEvents"][0]["entityUrn"]
        == f"urn:li:dataHubAccessToken:{user_token_id}"
    )
    assert res_data["usageEvents"][1]["eventType"] == "CreateAccessTokenEvent"
    assert (
        res_data["usageEvents"][1]["entityUrn"]
        == f"urn:li:dataHubAccessToken:{user_token_id}"
    )


def test_login_events():
    user_session = login_as(AUDIT_SUITE_USER_EMAIL, "user")
    time.sleep(10)

    # Audit events for create & revoke should show
    res_data = searchForAuditEvents(
        user_session,
        1,
        [
            "LogInEvent",
        ],
        [AUDIT_SUITE_USER_URN],
        [],
    )
    logger.info(res_data)
    assert res_data
    assert res_data["usageEvents"]
    assert len(res_data["usageEvents"]) == 1
    assert res_data["usageEvents"][0]["eventType"] == "LogInEvent"
    assert res_data["usageEvents"][0]["loginSource"] == "PASSWORD_LOGIN"
    user_session.cookies.clear()


def test_failed_login_events():
    try:
        user_session = login_as(AUDIT_SUITE_USER_EMAIL, "NOTMYPASSWORD")
    except Exception:
        pass

    time.sleep(10)
    user_session = login_as(admin_user, admin_pass)
    wait_for_writes_to_sync(consumer_group="datahub-usage-event-consumer-job-client")

    # Audit events for failed login should show, search for both to rule out any previous failures from any other tests
    res_data = searchForAuditEvents(
        user_session,
        1,
        ["FailedLogInEvent", "LogInEvent"],
        [AUDIT_SUITE_USER_URN],
        [],
    )
    logger.info(res_data)
    assert res_data
    assert res_data["usageEvents"]
    assert len(res_data["usageEvents"]) == 1
    assert res_data["usageEvents"][0]["eventType"] == "FailedLogInEvent"
    assert res_data["usageEvents"][0]["loginSource"] == "PASSWORD_LOGIN"
    user_session.cookies.clear()


def test_policy_events():
    user_session = login_as(admin_user, admin_pass)
    json = {
        "query": """mutation createPolicy($input: PolicyUpdateInput!) {\n
            createPolicy(input: $input) }""",
        "variables": {
            "input": {
                "type": "METADATA",
                "name": "Test Metadata Policy",
                "description": "My Metadaata Policy",
                "state": "ACTIVE",
                "resources": {"type": "dataset", "allResources": True},
                "privileges": ["EDIT_ENTITY_TAGS"],
                "actors": {
                    "users": ["urn:li:corpuser:datahub", "urn:li:corpuser:admin"],
                    "resourceOwners": False,
                    "allUsers": False,
                    "allGroups": False,
                },
            }
        },
    }

    response = user_session.post(f"{get_frontend_url()}/api/v2/graphql", json=json)
    response.raise_for_status()
    res_data = response.json()

    assert res_data
    assert res_data["data"]
    assert res_data["data"]["createPolicy"]

    wait_for_writes_to_sync(consumer_group="datahub-usage-event-consumer-job-client")

    new_urn = res_data["data"]["createPolicy"]

    update_json = {
        "query": """mutation updatePolicy($urn: String!, $input: PolicyUpdateInput!) {\n
            updatePolicy(urn: $urn, input: $input) }""",
        "variables": {
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
        },
    }

    response = user_session.post(
        f"{get_frontend_url()}/api/v2/graphql", json=update_json
    )
    response.raise_for_status()
    res_data = response.json()

    # Check updated was submitted successfully
    assert res_data
    assert res_data["data"]
    assert res_data["data"]["updatePolicy"]
    assert res_data["data"]["updatePolicy"] == new_urn

    wait_for_writes_to_sync(consumer_group="datahub-usage-event-consumer-job-client")
    wait_for_audit_event_types_for_entity(
        user_session,
        new_urn,
        {"CreatePolicyEvent", "UpdatePolicyEvent"},
        ["CreatePolicyEvent", "UpdatePolicyEvent"],
        ["urn:li:corpuser:datahub", "urn:li:corpuser:admin"],
    )
    user_session.cookies.clear()


def test_ingestion_source_events():
    user_session = login_as(admin_user, admin_pass)
    json = {
        "query": """mutation createIngestionSource($input: UpdateIngestionSourceInput!) {\n
            createIngestionSource(input: $input)\n}""",
        "variables": {
            "input": {
                "type": "snowflake",
                "name": "test",
                "config": {
                    "recipe": """{\"source\":{\"type\":\"snowflake\",\"config\":{
                    \"account_id\":null,
                    \"include_table_lineage\":true,
                    \"include_view_lineage\":true,
                    \"include_tables\":true,
                    \"include_views\":true,
                    \"profiling\":{\"enabled\":true,\"profile_table_level_only\":true},
                    \"stateful_ingestion\":{\"enabled\":true}}}}""",
                    "executorId": "default",
                    "debugMode": False,
                    "extraArgs": [],
                },
            }
        },
    }
    response = user_session.post(f"{get_frontend_url()}/api/v2/graphql", json=json)
    response.raise_for_status()
    res_data = response.json()
    assert res_data["data"]["createIngestionSource"]
    ingestion_source_urn = res_data["data"]["createIngestionSource"]

    json = {
        "query": """mutation updateIngestionSource($urn: String!, $input: UpdateIngestionSourceInput!) {\n
            updateIngestionSource(urn: $urn, input: $input)\n}""",
        "variables": {
            "urn": ingestion_source_urn,
            "input": {
                "type": "snowflake",
                "name": "test updated",
                "config": {
                    "recipe": """{\"source\":{\"type\":\"snowflake\",\"config\":{
                                 \"account_id\":null,
                                 \"include_table_lineage\":true,
                                 \"include_view_lineage\":true,
                                 \"include_tables\":true,
                                 \"include_views\":true,
                                 \"profiling\":{\"enabled\":true,\"profile_table_level_only\":true},
                                 \"stateful_ingestion\":{\"enabled\":true}}}}""",
                    "executorId": "default",
                    "debugMode": False,
                    "extraArgs": [],
                },
            },
        },
    }
    response = user_session.post(f"{get_frontend_url()}/api/v2/graphql", json=json)
    response.raise_for_status()
    res_data = response.json()
    assert res_data["data"]["updateIngestionSource"]
    wait_for_writes_to_sync(consumer_group="datahub-usage-event-consumer-job-client")

    wait_for_audit_event_types_for_entity(
        user_session,
        ingestion_source_urn,
        {"CreateIngestionSourceEvent", "UpdateIngestionSourceEvent"},
        ["CreateIngestionSourceEvent", "UpdateIngestionSourceEvent"],
        ["urn:li:corpuser:datahub", "urn:li:corpuser:admin"],
    )
    user_session.cookies.clear()


def test_user_events():
    user_session = login_as(admin_user, admin_pass)
    wait_for_writes_to_sync(consumer_group="datahub-usage-event-consumer-job-client")

    # TODO: sign-up link actor should be the admin who created the invite, not __datahub_system.
    event_types = ["CreateUserEvent", "UpdateUserEvent"]
    actor_urns = ["urn:li:corpuser:__datahub_system"]
    aspect_names = [
        "corpUserKey",
        "corpUserInfo",
        "corpUserStatus",
        "corpUserCredentials",
    ]

    wait_for_audit_event_types_for_entity(
        user_session,
        AUDIT_SUITE_USER_URN,
        {"CreateUserEvent", "UpdateUserEvent"},
        event_types,
        actor_urns,
        aspect_names,
        search_size=20,
    )

    res_data = searchForAuditEvents(
        user_session, 20, event_types, actor_urns, aspect_names
    )
    entity_events = audit_events_for_entity(
        res_data.get("usageEvents", []), AUDIT_SUITE_USER_URN
    )
    logger.info({"auditSuiteUserEvents": entity_events})

    assert len(entity_events) == 4
    assert {event["aspectName"] for event in entity_events} == set(aspect_names)
    found_types = {event["eventType"] for event in entity_events}
    assert "CreateUserEvent" in found_types
    assert "UpdateUserEvent" in found_types
    user_session.cookies.clear()


def test_policy_create_delete():
    user_session = login_as(admin_user, admin_pass)
    json = {
        "query": """mutation createPolicy($input: PolicyUpdateInput!) {\n
            createPolicy(input: $input) }""",
        "variables": {
            "input": {
                "type": "METADATA",
                "name": "Test Metadata Policy",
                "description": "My New Metadata Policy",
                "state": "ACTIVE",
                "resources": {"type": "dataset", "allResources": True},
                "privileges": ["EDIT_ENTITY_TAGS"],
                "actors": {
                    "users": ["urn:li:corpuser:datahub", "urn:li:corpuser:admin"],
                    "resourceOwners": False,
                    "allUsers": False,
                    "allGroups": False,
                },
            }
        },
    }

    response = user_session.post(f"{get_frontend_url()}/api/v2/graphql", json=json)
    response.raise_for_status()
    res_data = response.json()

    assert res_data
    assert res_data["data"]
    assert res_data["data"]["createPolicy"]

    wait_for_writes_to_sync(consumer_group="datahub-usage-event-consumer-job-client")

    new_urn = res_data["data"]["createPolicy"]

    update_json = {
        "query": """mutation deletePolicy($urn: String!) {\n
            deletePolicy(urn: $urn) }""",
        "variables": {
            "urn": new_urn,
        },
    }

    response = user_session.post(
        f"{get_frontend_url()}/api/v2/graphql", json=update_json
    )
    response.raise_for_status()
    res_data = response.json()

    # Check updated was submitted successfully
    assert res_data
    assert res_data["data"]
    assert res_data["data"]["deletePolicy"]
    assert res_data["data"]["deletePolicy"] == new_urn

    wait_for_writes_to_sync(consumer_group="datahub-usage-event-consumer-job-client")
    wait_for_audit_event_types_for_entity(
        user_session,
        new_urn,
        {"CreatePolicyEvent", "DeletePolicyEvent"},
        ["CreatePolicyEvent", "DeletePolicyEvent"],
        ["urn:li:corpuser:datahub", "urn:li:corpuser:admin"],
    )
    user_session.cookies.clear()


def generateAccessToken_v2(session, actorUrn):
    # Create new token
    json = {
        "query": """mutation createAccessToken($input: CreateAccessTokenInput!) {
            createAccessToken(input: $input) {
              accessToken
              metadata {
                id
                actorUrn
                ownerUrn
                name
                description
              }
            }
        }""",
        "variables": {
            "input": {
                "type": "PERSONAL",
                "actorUrn": actorUrn,
                "duration": "ONE_HOUR",
                "name": AUDIT_SUITE_TOKEN_NAME,
            }
        },
    }

    response = session.post(f"{get_frontend_url()}/api/v2/graphql", json=json)
    response.raise_for_status()
    return response.json()


def listAccessTokens(session, filters):
    if filters is None:
        filters = []
    # Get count of existing tokens
    input = {"start": 0, "count": 20}

    if filters:
        input["filters"] = filters

    json = {
        "query": """query listAccessTokens($input: ListAccessTokenInput!) {
            listAccessTokens(input: $input) {
              start
              count
              total
              tokens {
                urn
                id
                actorUrn
                ownerUrn
              }
            }
        }""",
        "variables": {"input": input},
    }

    response = session.post(f"{get_frontend_url()}/api/v2/graphql", json=json)
    response.raise_for_status()
    return response.json()


def revokeAccessToken(session, tokenId):
    # Revoke token
    json = {
        "query": """mutation revokeAccessToken($tokenId: String!) {
            revokeAccessToken(tokenId: $tokenId)
        }""",
        "variables": {"tokenId": tokenId},
    }

    response = session.post(f"{get_frontend_url()}/api/v2/graphql", json=json)
    response.raise_for_status()

    return response.json()


def searchForAuditEvents(
    session, size, event_types: List, user_urns: List, aspect_names: List
):
    json = {
        "eventTypes": event_types,
        "actorUrns": user_urns,
        "aspectTypes": aspect_names,
    }
    response = session.post(
        f"{get_frontend_url()}/openapi/v1/events/audit/search?size={size}", json=json
    )
    response.raise_for_status()

    return response.json()


def audit_events_for_entity(events: List[Dict], entity_urn: str) -> List[Dict]:
    return [event for event in events if event.get("entityUrn") == entity_urn]


def assert_audit_event_types_for_entity(
    events: List[Dict], entity_urn: str, expected_types: Set[str]
) -> None:
    entity_events = audit_events_for_entity(events, entity_urn)
    found_types = {event["eventType"] for event in entity_events}
    assert expected_types <= found_types, entity_events


def wait_for_audit_event_types_for_entity(
    session,
    entity_urn: str,
    expected_types: Set[str],
    event_types: List[str],
    actor_urns: List[str],
    aspect_names: List | None = None,
    *,
    search_size: int = 10,
    timeout_sec: int = 60,
) -> None:
    """Poll audit search until each expected event type is indexed for entity_urn.

    Audit search sorts by timestamp DESC then event type ASC, so list order is not
    stable across Kafka/pgQueue/CDC profiles or when timestamps tie. Filtering by
    entity URN checks the contract we care about: every mutation emitted an event.

    Entity creates write key + info aspects, so the same event type (e.g.
    UpdatePolicyEvent) may appear more than once per entity. We assert on the set of
    event types present, not the total event count.
    """
    deadline = time.time() + timeout_sec
    last_entity_events: List[Dict] = []
    while time.time() < deadline:
        res_data = searchForAuditEvents(
            session, search_size, event_types, actor_urns, aspect_names or []
        )
        last_entity_events = audit_events_for_entity(
            res_data.get("usageEvents", []), entity_urn
        )
        found_types = {event["eventType"] for event in last_entity_events}
        if expected_types <= found_types:
            logger.info(
                "Audit events ready for %s: %s",
                entity_urn,
                sorted(found_types),
            )
            return
        time.sleep(2)

    assert_audit_event_types_for_entity(last_entity_events, entity_urn, expected_types)
