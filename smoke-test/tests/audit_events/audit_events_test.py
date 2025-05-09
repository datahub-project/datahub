import os
import time
from typing import List

import pytest

from tests.tokens.token_utils import listUsers, removeUser
from tests.utils import (
    get_admin_credentials,
    get_frontend_url,
    login_as,
    wait_for_writes_to_sync,
)

pytestmark = pytest.mark.no_cypress_suite1

# Disable telemetry
os.environ["DATAHUB_TELEMETRY_ENABLED"] = "false"

(admin_user, admin_pass) = get_admin_credentials()


@pytest.fixture()
def auth_exclude_filter():
    return {
        "field": "name",
        "condition": "EQUAL",
        "negated": True,
        "values": ["Test Session Token"],
    }


@pytest.fixture(scope="class", autouse=True)
def custom_user_setup():
    admin_session = login_as(admin_user, admin_pass)
    try:
        """Fixture to execute setup before and tear down after all tests are run"""
        res_data = removeUser(admin_session, "urn:li:corpuser:user")
        assert res_data
        assert "error" not in res_data

        # Test getting the invite token
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
        assert "error" not in invite_token

        # Pass the invite token when creating the user
        sign_up_json = {
            "fullName": "Test User",
            "email": "user",
            "password": "user",
            "title": "Data Engineer",
            "inviteToken": invite_token,
        }

        sign_up_response = admin_session.post(
            f"{get_frontend_url()}/signUp", json=sign_up_json
        )
        sign_up_response.raise_for_status()
        assert sign_up_response
        assert "error" not in sign_up_response
        # Sleep for eventual consistency
        wait_for_writes_to_sync(
            consumer_group="datahub-usage-event-consumer-job-client"
        )

        # signUp will override the session cookie to the new user to be signed up.
        admin_session.cookies.clear()
        admin_session = login_as(admin_user, admin_pass)

        # Make user created user is there.
        res_data = listUsers(admin_session)
        assert res_data["data"]
        assert res_data["data"]["listUsers"]
        assert {"username": "user"} in res_data["data"]["listUsers"]["users"]
        admin_session.cookies.clear()

        yield

    finally:
        # Delete created user
        admin_session = login_as(admin_user, admin_pass)
        res_data = removeUser(admin_session, "urn:li:corpuser:user")
        assert res_data
        assert res_data["data"]
        assert res_data["data"]["removeUser"] is True
        # Sleep for eventual consistency
        wait_for_writes_to_sync(
            consumer_group="datahub-usage-event-consumer-job-client"
        )

        # Make user created user is not there.
        res_data = listUsers(admin_session)
        assert res_data["data"]
        assert res_data["data"]["listUsers"]
        assert {"username": "user"} not in res_data["data"]["listUsers"]["users"]


@pytest.fixture(autouse=True)
def access_token_setup(auth_session, auth_exclude_filter):
    """Fixture to execute asserts before and after a test is run"""
    admin_session = login_as(admin_user, admin_pass)

    res_data = listAccessTokens(admin_session, filters=[auth_exclude_filter])
    assert res_data
    assert res_data["data"]
    assert res_data["data"]["listAccessTokens"]["total"] == 0
    assert not res_data["data"]["listAccessTokens"]["tokens"]

    yield

    # Clean up
    res_data = listAccessTokens(admin_session, filters=[auth_exclude_filter])
    for metadata in res_data["data"]["listAccessTokens"]["tokens"]:
        revokeAccessToken(admin_session, metadata["id"])


def test_audit_token_events(auth_exclude_filter):
    user_session = login_as("user", "user")

    # Normal user should be able to generate token for himself.
    res_data = generateAccessToken_v2(user_session, "urn:li:corpuser:user")
    assert res_data
    assert res_data["data"]
    assert res_data["data"]["createAccessToken"]
    assert res_data["data"]["createAccessToken"]["accessToken"]
    assert (
        res_data["data"]["createAccessToken"]["metadata"]["actorUrn"]
        == "urn:li:corpuser:user"
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
        ["urn:li:corpuser:user"],
        [],
    )
    print(res_data)
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


def test_login_events(auth_exclude_filter):
    user_session = login_as("user", "user")
    time.sleep(10)

    # Audit events for create & revoke should show
    res_data = searchForAuditEvents(
        user_session,
        1,
        [
            "LogInEvent",
        ],
        ["urn:li:corpuser:user"],
        [],
    )
    print(res_data)
    assert res_data
    assert res_data["usageEvents"]
    assert len(res_data["usageEvents"]) == 1
    assert res_data["usageEvents"][0]["eventType"] == "LogInEvent"
    assert res_data["usageEvents"][0]["loginSource"] == "PASSWORD_LOGIN"
    user_session.cookies.clear()


def test_failed_login_events(auth_exclude_filter):
    try:
        user_session = login_as("user", "NOTMYPASSWORD")
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
        ["urn:li:corpuser:user"],
        [],
    )
    print(res_data)
    assert res_data
    assert res_data["usageEvents"]
    assert len(res_data["usageEvents"]) == 1
    assert res_data["usageEvents"][0]["eventType"] == "FailedLogInEvent"
    assert res_data["usageEvents"][0]["loginSource"] == "PASSWORD_LOGIN"
    user_session.cookies.clear()


def test_policy_events(auth_exclude_filter):
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
    res_data = searchForAuditEvents(
        user_session,
        3,
        ["CreatePolicyEvent", "UpdatePolicyEvent"],
        ["urn:li:corpuser:datahub", "urn:li:corpuser:admin"],
        [],
    )
    print(res_data)
    assert res_data
    assert res_data["usageEvents"]
    assert len(res_data["usageEvents"]) == 3 or len(res_data["usageEvents"]) == 2
    assert res_data["usageEvents"][0]["eventType"] == "UpdatePolicyEvent"
    assert res_data["usageEvents"][0]["entityUrn"] == new_urn
    assert (
        res_data["usageEvents"][1]["eventType"] == "CreatePolicyEvent"
        or res_data["usageEvents"][1]["eventType"] == "UpdatePolicyEvent"
    )
    assert res_data["usageEvents"][1]["entityUrn"] == new_urn
    if len(res_data["usageEvents"]) == 3:
        assert (
            res_data["usageEvents"][2]["eventType"] == "CreatePolicyEvent"
            or res_data["usageEvents"][2]["eventType"] == "UpdatePolicyEvent"
        )
        assert res_data["usageEvents"][2]["entityUrn"] == new_urn
    user_session.cookies.clear()


def test_ingestion_source_events(auth_exclude_filter):
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

    res_data = searchForAuditEvents(
        user_session,
        2,
        ["CreateIngestionSourceEvent", "UpdateIngestionSourceEvent"],
        ["urn:li:corpuser:datahub", "urn:li:corpuser:admin"],
        [],
    )
    print(res_data)
    assert res_data
    assert res_data["usageEvents"]
    assert len(res_data["usageEvents"]) == 2
    assert res_data["usageEvents"][0]["eventType"] == "UpdateIngestionSourceEvent"
    assert res_data["usageEvents"][0]["entityUrn"] == ingestion_source_urn
    assert res_data["usageEvents"][1]["eventType"] == "CreateIngestionSourceEvent"
    assert res_data["usageEvents"][1]["entityUrn"] == ingestion_source_urn
    user_session.cookies.clear()


def test_user_events(auth_exclude_filter):
    user_session = login_as(admin_user, admin_pass)
    wait_for_writes_to_sync(consumer_group="datahub-usage-event-consumer-job-client")

    # TODO: This feels wrong, sign up link should have the user who created the sign up link as the actor, but this
    #       requires a fairly significant refactor that's not in scope right now
    res_data = searchForAuditEvents(
        user_session,
        4,
        ["CreateUserEvent", "UpdateUserEvent"],
        ["urn:li:corpuser:__datahub_system"],
        ["corpUserKey", "corpUserInfo", "corpUserStatus", "corpUserCredentials"],
    )
    print(res_data)
    assert len(res_data["usageEvents"]) == 4
    assert res_data["usageEvents"][0]["eventType"] == "UpdateUserEvent"
    assert res_data["usageEvents"][0]["entityUrn"] == "urn:li:corpuser:user"
    # Credentials and settings are in random order due to async
    assert res_data["usageEvents"][0]["aspectName"] == "corpUserCredentials"

    assert res_data["usageEvents"][1]["eventType"] == "UpdateUserEvent"
    assert res_data["usageEvents"][1]["entityUrn"] == "urn:li:corpuser:user"
    assert res_data["usageEvents"][1]["aspectName"] == "corpUserStatus"

    # These get created at the same time
    assert (
        res_data["usageEvents"][2]["eventType"] == "UpdateUserEvent"
        or res_data["usageEvents"][2]["eventType"] == "CreateUserEvent"
    )
    assert res_data["usageEvents"][2]["entityUrn"] == "urn:li:corpuser:user"
    assert (
        res_data["usageEvents"][2]["aspectName"] == "corpUserInfo"
        or res_data["usageEvents"][2]["aspectName"] == "corpUserKey"
    )

    assert (
        res_data["usageEvents"][3]["eventType"] == "UpdateUserEvent"
        or res_data["usageEvents"][3]["eventType"] == "CreateUserEvent"
    )
    assert res_data["usageEvents"][3]["entityUrn"] == "urn:li:corpuser:user"
    assert (
        res_data["usageEvents"][3]["aspectName"] == "corpUserInfo"
        or res_data["usageEvents"][3]["aspectName"] == "corpUserKey"
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
                "name": "my token",
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
