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
)
from tests.utils import (
    TestSessionWrapper,
    get_admin_credentials,
    get_frontend_url,
    login_as,
    wait_for_writes_to_sync,
)

logger = logging.getLogger(__name__)

pytestmark = pytest.mark.no_cypress_suite1

# Disable telemetry
os.environ["DATAHUB_TELEMETRY_ENABLED"] = "false"

DEFAULT_AUDIT_USER_PASSWORD = "user"

# Valid emails for auth.native.signUp.enforceValidEmail (Play EmailValidator).
TOKEN_AUDIT_USER_EMAIL = "audit.events.token@smoke.datahub.test"
TOKEN_AUDIT_USER_URN = f"urn:li:corpuser:{TOKEN_AUDIT_USER_EMAIL}"
TOKEN_AUDIT_TOKEN_NAME = "audit-token-test-token"

LOGIN_AUDIT_USER_EMAIL = "audit.events.login@smoke.datahub.test"
LOGIN_AUDIT_USER_URN = f"urn:li:corpuser:{LOGIN_AUDIT_USER_EMAIL}"

SIGNUP_AUDIT_USER_EMAIL = "audit.events.signup@smoke.datahub.test"
SIGNUP_AUDIT_USER_URN = f"urn:li:corpuser:{SIGNUP_AUDIT_USER_EMAIL}"

FAILED_LOGIN_USER_EMAIL = "audit.events.failed-login@smoke.datahub.test"
FAILED_LOGIN_USER_URN = f"urn:li:corpuser:{FAILED_LOGIN_USER_EMAIL}"

AUDIT_EVENT_SEARCH_SIZE = 25


def audit_action_started_ms() -> int:
    return int(time.time() * 1000)


def _provision_audit_user(
    email: str, password: str = DEFAULT_AUDIT_USER_PASSWORD
) -> TestSessionWrapper:
    from tests.privileges.utils import create_user

    admin_user, admin_pass = get_admin_credentials()
    return TestSessionWrapper(
        create_user(login_as(admin_user, admin_pass), email, password)
    )


def _teardown_audit_user(admin_session: TestSessionWrapper, user_urn: str) -> None:
    res_data = removeUser(admin_session, user_urn)
    assert res_data
    assert "error" not in res_data
    wait_for_writes_to_sync(auth_session=admin_session)
    admin_session.destroy()


@pytest.fixture()
def token_audit_user(auth_session):
    token_filter = [token_name_filter(TOKEN_AUDIT_TOKEN_NAME)]
    wait_for_no_tokens_matching(auth_session, token_filter)
    admin_session = _provision_audit_user(TOKEN_AUDIT_USER_EMAIL)
    try:
        yield admin_session
    finally:
        revoke_tokens_matching(auth_session, token_filter)
        _teardown_audit_user(admin_session, TOKEN_AUDIT_USER_URN)


@pytest.fixture()
def login_audit_user():
    admin_session = _provision_audit_user(LOGIN_AUDIT_USER_EMAIL)
    try:
        yield admin_session
    finally:
        _teardown_audit_user(admin_session, LOGIN_AUDIT_USER_URN)


@pytest.fixture()
def signup_audit_user():
    provision_started_ms = audit_action_started_ms()
    admin_session = _provision_audit_user(SIGNUP_AUDIT_USER_EMAIL)
    try:
        wait_for_writes_to_sync(
            consumer_group="datahub-usage-event-consumer-job-client",
            auth_session=admin_session,
        )
        yield admin_session, provision_started_ms
    finally:
        _teardown_audit_user(admin_session, SIGNUP_AUDIT_USER_URN)


@pytest.fixture()
def failed_login_audit_user():
    """Dedicated user so failed-login audit events are not mixed with suite logins."""
    admin_session = _provision_audit_user(
        FAILED_LOGIN_USER_EMAIL, DEFAULT_AUDIT_USER_PASSWORD
    )
    try:
        yield admin_session
    finally:
        _teardown_audit_user(admin_session, FAILED_LOGIN_USER_URN)


def test_audit_token_events(auth_session, token_audit_user):
    user_session = login_as(TOKEN_AUDIT_USER_EMAIL, DEFAULT_AUDIT_USER_PASSWORD)

    create_started_ms = audit_action_started_ms()
    res_data = generateAccessToken_v2(
        user_session, TOKEN_AUDIT_USER_URN, TOKEN_AUDIT_TOKEN_NAME
    )
    assert_graphql_mutation_succeeded(res_data)
    assert res_data["data"]["createAccessToken"]
    assert res_data["data"]["createAccessToken"]["accessToken"]
    assert (
        res_data["data"]["createAccessToken"]["metadata"]["actorUrn"]
        == TOKEN_AUDIT_USER_URN
    )
    user_token_id = res_data["data"]["createAccessToken"]["metadata"]["id"]
    token_entity_urn = f"urn:li:dataHubAccessToken:{user_token_id}"
    wait_for_writes_to_sync(
        consumer_group="datahub-usage-event-consumer-job-client",
        auth_session=auth_session,
    )

    revoke_started_ms = audit_action_started_ms()
    res_data = revokeAccessToken(user_session, user_token_id)
    assert res_data
    assert res_data["data"]
    assert res_data["data"]["revokeAccessToken"]
    assert res_data["data"]["revokeAccessToken"] is True
    wait_for_writes_to_sync(
        consumer_group="datahub-usage-event-consumer-job-client",
        auth_session=auth_session,
    )

    create_event = wait_for_audit_event(
        user_session,
        actor_urn=TOKEN_AUDIT_USER_URN,
        event_types=["CreateAccessTokenEvent", "RevokeAccessTokenEvent"],
        event_type="CreateAccessTokenEvent",
        entity_urn=token_entity_urn,
        after_timestamp_ms=create_started_ms,
    )
    revoke_event = wait_for_audit_event(
        user_session,
        actor_urn=TOKEN_AUDIT_USER_URN,
        event_types=["CreateAccessTokenEvent", "RevokeAccessTokenEvent"],
        event_type="RevokeAccessTokenEvent",
        entity_urn=token_entity_urn,
        after_timestamp_ms=revoke_started_ms,
    )
    logger.info(
        {"createAccessTokenEvent": create_event, "revokeAccessTokenEvent": revoke_event}
    )
    assert revoke_event["timestamp"] > create_event["timestamp"]


def test_login_events(auth_session, login_audit_user):
    login_started_ms = audit_action_started_ms()
    user_session = login_as(LOGIN_AUDIT_USER_EMAIL, DEFAULT_AUDIT_USER_PASSWORD)
    wait_for_writes_to_sync(
        consumer_group="datahub-usage-event-consumer-job-client",
        auth_session=auth_session,
    )

    login_event = wait_for_audit_event(
        user_session,
        actor_urn=LOGIN_AUDIT_USER_URN,
        event_types=["LogInEvent"],
        event_type="LogInEvent",
        login_source="PASSWORD_LOGIN",
        after_timestamp_ms=login_started_ms,
    )
    logger.info({"passwordLoginEvent": login_event})
    user_session.cookies.clear()


def test_failed_login_events(failed_login_audit_user):
    admin_session = failed_login_audit_user
    failed_login_started_ms = audit_action_started_ms()
    try:
        login_as(FAILED_LOGIN_USER_EMAIL, "NOTMYPASSWORD")
    except Exception:
        pass

    wait_for_writes_to_sync(
        consumer_group="datahub-usage-event-consumer-job-client",
        auth_session=admin_session,
    )

    failed_login_event = wait_for_audit_event(
        admin_session,
        actor_urn=FAILED_LOGIN_USER_URN,
        event_types=["FailedLogInEvent"],
        event_type="FailedLogInEvent",
        login_source="PASSWORD_LOGIN",
        after_timestamp_ms=failed_login_started_ms,
    )
    logger.info({"failedLoginEvent": failed_login_event})


def test_policy_events(auth_session):
    user_session = auth_session
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

    wait_for_writes_to_sync(
        consumer_group="datahub-usage-event-consumer-job-client",
        auth_session=auth_session,
    )

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

    wait_for_writes_to_sync(
        consumer_group="datahub-usage-event-consumer-job-client",
        auth_session=auth_session,
    )
    wait_for_audit_event_types_for_entity(
        user_session,
        new_urn,
        {"CreatePolicyEvent", "UpdatePolicyEvent"},
        ["CreatePolicyEvent", "UpdatePolicyEvent"],
        ["urn:li:corpuser:datahub", "urn:li:corpuser:admin"],
    )


def test_ingestion_source_events(auth_session):
    user_session = auth_session
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
    wait_for_writes_to_sync(
        consumer_group="datahub-usage-event-consumer-job-client",
        auth_session=auth_session,
    )

    wait_for_audit_event_types_for_entity(
        user_session,
        ingestion_source_urn,
        {"CreateIngestionSourceEvent", "UpdateIngestionSourceEvent"},
        ["CreateIngestionSourceEvent", "UpdateIngestionSourceEvent"],
        ["urn:li:corpuser:datahub", "urn:li:corpuser:admin"],
    )


def test_user_events(auth_session, signup_audit_user):
    user_session = auth_session
    _, provision_started_ms = signup_audit_user

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
        SIGNUP_AUDIT_USER_URN,
        {"CreateUserEvent", "UpdateUserEvent"},
        event_types,
        actor_urns,
        aspect_names,
        after_timestamp_ms=provision_started_ms,
    )

    res_data = searchForAuditEvents(
        user_session,
        AUDIT_EVENT_SEARCH_SIZE,
        event_types,
        actor_urns,
        aspect_names,
    )
    entity_events = audit_events_for_entity(
        res_data.get("usageEvents", []),
        SIGNUP_AUDIT_USER_URN,
        after_timestamp_ms=provision_started_ms,
    )
    logger.info({"signupAuditUserEvents": entity_events})

    assert len(entity_events) >= 4
    assert {event["aspectName"] for event in entity_events} == set(aspect_names)
    found_types = {event["eventType"] for event in entity_events}
    assert "CreateUserEvent" in found_types
    assert "UpdateUserEvent" in found_types


def test_policy_create_delete(auth_session):
    user_session = auth_session
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

    wait_for_writes_to_sync(
        consumer_group="datahub-usage-event-consumer-job-client",
        auth_session=auth_session,
    )

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

    wait_for_writes_to_sync(
        consumer_group="datahub-usage-event-consumer-job-client",
        auth_session=auth_session,
    )
    wait_for_audit_event_types_for_entity(
        user_session,
        new_urn,
        {"CreatePolicyEvent", "DeletePolicyEvent"},
        ["CreatePolicyEvent", "DeletePolicyEvent"],
        ["urn:li:corpuser:datahub", "urn:li:corpuser:admin"],
    )


def generateAccessToken_v2(session, actorUrn, token_name: str):
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
                "name": token_name,
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


def filter_audit_events(
    events: List[Dict],
    *,
    actor_urn: str | None = None,
    event_type: str | None = None,
    entity_urn: str | None = None,
    login_source: str | None = None,
    after_timestamp_ms: int | None = None,
) -> List[Dict]:
    matched: List[Dict] = []
    for event in events:
        if actor_urn is not None and event.get("actorUrn") != actor_urn:
            continue
        if event_type is not None and event.get("eventType") != event_type:
            continue
        if entity_urn is not None and event.get("entityUrn") != entity_urn:
            continue
        if login_source is not None and event.get("loginSource") != login_source:
            continue
        if (
            after_timestamp_ms is not None
            and event.get("timestamp", 0) <= after_timestamp_ms
        ):
            continue
        matched.append(event)
    return matched


def wait_for_audit_event(
    session,
    *,
    actor_urn: str,
    event_types: List[str],
    event_type: str,
    entity_urn: str | None = None,
    login_source: str | None = None,
    after_timestamp_ms: int | None = None,
    aspect_names: List | None = None,
    search_size: int = AUDIT_EVENT_SEARCH_SIZE,
    timeout_sec: int = 60,
) -> Dict:
    """Poll audit search for a specific actor/event after an action timestamp.

    Audit search returns a recent page sorted by timestamp DESC. Under xdist the
    newest event for an actor may still be from fixture setup, and indexing can lag
    behind consumer lag reaching zero. Filter by actor URN plus
    ``timestamp > after_timestamp_ms`` to target events from the current test action.
    """
    deadline = time.time() + timeout_sec
    last_events: List[Dict] = []
    last_page: List[Dict] = []
    while time.time() < deadline:
        res_data = searchForAuditEvents(
            session, search_size, event_types, [actor_urn], aspect_names or []
        )
        last_page = res_data.get("usageEvents", [])
        last_events = filter_audit_events(
            last_page,
            actor_urn=actor_urn,
            event_type=event_type,
            entity_urn=entity_urn,
            login_source=login_source,
            after_timestamp_ms=after_timestamp_ms,
        )
        if last_events:
            return last_events[0]
        time.sleep(2)

    raise AssertionError(
        f"Timed out waiting for {event_type} actor={actor_urn} "
        f"entity={entity_urn} login_source={login_source} "
        f"after_timestamp_ms={after_timestamp_ms}; last page: {last_page}"
    )


def audit_events_for_entity(
    events: List[Dict],
    entity_urn: str,
    *,
    after_timestamp_ms: int | None = None,
) -> List[Dict]:
    return filter_audit_events(
        events,
        entity_urn=entity_urn,
        after_timestamp_ms=after_timestamp_ms,
    )


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
    after_timestamp_ms: int | None = None,
    search_size: int = AUDIT_EVENT_SEARCH_SIZE,
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
            res_data.get("usageEvents", []),
            entity_urn,
            after_timestamp_ms=after_timestamp_ms,
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
