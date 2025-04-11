import os

import pytest

from tests.utils import (
    get_admin_credentials,
    get_frontend_url,
    login_as,
    wait_for_writes_to_sync,
)

from .token_utils import listUsers, removeUser

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
    """Fixture to execute setup before and tear down after all tests are run"""
    admin_session = login_as(admin_user, admin_pass)

    res_data = removeUser(admin_session, "urn:li:corpuser:user")
    assert res_data
    assert "error" not in res_data

    # Test getting the invite token
    get_invite_token_json = {
        "query": """query getInviteToken($input: GetInviteTokenInput!) {
            getInviteToken(input: $input){
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
    invite_token = get_invite_token_res_data["data"]["getInviteToken"]["inviteToken"]
    assert invite_token is not None
    assert "error" not in invite_token

    # Pass the invite token when creating the user
    sign_up_json = {
        "fullName": "Test User",
        "email": "user",
        "password": "user",
        "title": "Date Engineer",
        "inviteToken": invite_token,
    }

    sign_up_response = admin_session.post(
        f"{get_frontend_url()}/signUp", json=sign_up_json
    )
    sign_up_response.raise_for_status()
    assert sign_up_response
    assert "error" not in sign_up_response
    # Sleep for eventual consistency
    wait_for_writes_to_sync()

    # signUp will override the session cookie to the new user to be signed up.
    admin_session.cookies.clear()
    admin_session = login_as(admin_user, admin_pass)

    # Make user created user is there.
    res_data = listUsers(admin_session)
    assert res_data["data"]
    assert res_data["data"]["listUsers"]
    assert {"username": "user"} in res_data["data"]["listUsers"]["users"]

    yield

    # Delete created user
    res_data = removeUser(admin_session, "urn:li:corpuser:user")
    assert res_data
    assert res_data["data"]
    assert res_data["data"]["removeUser"] is True
    # Sleep for eventual consistency
    wait_for_writes_to_sync()

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


def test_admin_can_create_list_and_revoke_tokens(auth_exclude_filter):
    admin_session = login_as(admin_user, admin_pass)
    admin_user_urn = f"urn:li:corpuser:{admin_user}"

    # Using a super account, there should be no tokens
    res_data = listAccessTokens(admin_session, filters=[auth_exclude_filter])
    assert res_data
    assert res_data["data"]
    assert res_data["data"]["listAccessTokens"]["total"] is not None
    assert len(res_data["data"]["listAccessTokens"]["tokens"]) == 0

    # Using a super account, generate a token for itself.
    res_data = generateAccessToken_v2(admin_session, admin_user_urn)
    assert res_data
    assert res_data["data"]
    assert res_data["data"]["createAccessToken"]
    assert res_data["data"]["createAccessToken"]["accessToken"]
    assert (
        res_data["data"]["createAccessToken"]["metadata"]["actorUrn"] == admin_user_urn
    )
    access_token = res_data["data"]["createAccessToken"]["accessToken"]
    admin_tokenId = res_data["data"]["createAccessToken"]["metadata"]["id"]
    # Sleep for eventual consistency
    wait_for_writes_to_sync()

    res_data = getAccessTokenMetadata(admin_session, access_token)
    assert res_data
    assert res_data["data"]
    assert res_data["data"]["getAccessTokenMetadata"]
    assert res_data["data"]["getAccessTokenMetadata"]["ownerUrn"] == admin_user_urn
    assert res_data["data"]["getAccessTokenMetadata"]["actorUrn"] == admin_user_urn

    # Using a super account, list the previously created token.
    res_data = listAccessTokens(admin_session, filters=[auth_exclude_filter])
    assert res_data
    assert res_data["data"]
    assert res_data["data"]["listAccessTokens"]["total"] is not None
    assert len(res_data["data"]["listAccessTokens"]["tokens"]) == 1
    assert (
        res_data["data"]["listAccessTokens"]["tokens"][0]["actorUrn"] == admin_user_urn
    )
    assert (
        res_data["data"]["listAccessTokens"]["tokens"][0]["ownerUrn"] == admin_user_urn
    )

    # Check that the super account can revoke tokens that it created
    res_data = revokeAccessToken(admin_session, admin_tokenId)
    assert res_data
    assert res_data["data"]
    assert res_data["data"]["revokeAccessToken"]
    assert res_data["data"]["revokeAccessToken"] is True

    # Using a super account, there should be no tokens
    res_data = listAccessTokens(admin_session, filters=[auth_exclude_filter])
    assert res_data
    assert res_data["data"]
    assert res_data["data"]["listAccessTokens"]["total"] is not None
    assert len(res_data["data"]["listAccessTokens"]["tokens"]) == 0


def test_admin_can_create_and_revoke_tokens_for_other_user(auth_exclude_filter):
    admin_session = login_as(admin_user, admin_pass)

    # Using a super account, there should be no tokens
    res_data = listAccessTokens(admin_session, filters=[auth_exclude_filter])
    assert res_data
    assert res_data["data"]
    assert res_data["data"]["listAccessTokens"]["total"] is not None
    assert len(res_data["data"]["listAccessTokens"]["tokens"]) == 0

    # Using a super account, generate a token for another user.
    res_data = generateAccessToken_v2(admin_session, "urn:li:corpuser:user")
    assert res_data
    assert res_data["data"]
    assert res_data["data"]["createAccessToken"]
    assert res_data["data"]["createAccessToken"]["accessToken"]
    assert (
        res_data["data"]["createAccessToken"]["metadata"]["actorUrn"]
        == "urn:li:corpuser:user"
    )
    user_tokenId = res_data["data"]["createAccessToken"]["metadata"]["id"]
    # Sleep for eventual consistency
    wait_for_writes_to_sync()

    # Using a super account, list the previously created tokens.
    res_data = listAccessTokens(admin_session, filters=[auth_exclude_filter])
    assert res_data
    assert res_data["data"]
    assert res_data["data"]["listAccessTokens"]["total"] is not None
    assert len(res_data["data"]["listAccessTokens"]["tokens"]) == 1
    assert (
        res_data["data"]["listAccessTokens"]["tokens"][0]["actorUrn"]
        == "urn:li:corpuser:user"
    )
    assert (
        res_data["data"]["listAccessTokens"]["tokens"][0]["ownerUrn"]
        == f"urn:li:corpuser:{admin_user}"
    )

    # Check that the super account can revoke tokens that it created for another user
    res_data = revokeAccessToken(admin_session, user_tokenId)
    assert res_data
    assert res_data["data"]
    assert res_data["data"]["revokeAccessToken"]
    assert res_data["data"]["revokeAccessToken"] is True

    # Using a super account, there should be no tokens
    res_data = listAccessTokens(admin_session, filters=[auth_exclude_filter])
    assert res_data
    assert res_data["data"]
    assert res_data["data"]["listAccessTokens"]["total"] is not None
    assert len(res_data["data"]["listAccessTokens"]["tokens"]) == 0


def test_non_admin_can_create_list_revoke_tokens(auth_exclude_filter):
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
    user_tokenId = res_data["data"]["createAccessToken"]["metadata"]["id"]
    # Sleep for eventual consistency
    wait_for_writes_to_sync()

    # User should be able to list his own token
    res_data = listAccessTokens(
        user_session,
        [
            {"field": "ownerUrn", "values": ["urn:li:corpuser:user"]},
            auth_exclude_filter,
        ],
    )
    assert res_data
    assert res_data["data"]
    assert res_data["data"]["listAccessTokens"]["total"] is not None
    assert len(res_data["data"]["listAccessTokens"]["tokens"]) == 1
    assert (
        res_data["data"]["listAccessTokens"]["tokens"][0]["actorUrn"]
        == "urn:li:corpuser:user"
    )
    assert (
        res_data["data"]["listAccessTokens"]["tokens"][0]["ownerUrn"]
        == "urn:li:corpuser:user"
    )
    assert res_data["data"]["listAccessTokens"]["tokens"][0]["id"] == user_tokenId

    # User should be able to revoke his own token
    res_data = revokeAccessToken(user_session, user_tokenId)
    assert res_data
    assert res_data["data"]
    assert res_data["data"]["revokeAccessToken"]
    assert res_data["data"]["revokeAccessToken"] is True

    # Using a normal account, check that all its tokens where removed.
    res_data = listAccessTokens(
        user_session,
        [
            {"field": "ownerUrn", "values": ["urn:li:corpuser:user"]},
            auth_exclude_filter,
        ],
    )
    assert res_data
    assert res_data["data"]
    assert res_data["data"]["listAccessTokens"]["total"] is not None
    assert len(res_data["data"]["listAccessTokens"]["tokens"]) == 0


def test_admin_can_manage_tokens_generated_by_other_user(auth_exclude_filter):
    admin_session = login_as(admin_user, admin_pass)

    # Using a super account, there should be no tokens
    res_data = listAccessTokens(admin_session, filters=[auth_exclude_filter])
    assert res_data
    assert res_data["data"]
    assert res_data["data"]["listAccessTokens"]["total"] is not None
    assert len(res_data["data"]["listAccessTokens"]["tokens"]) == 0

    admin_session.cookies.clear()
    user_session = login_as("user", "user")
    res_data = generateAccessToken_v2(user_session, "urn:li:corpuser:user")
    assert res_data
    assert res_data["data"]
    assert res_data["data"]["createAccessToken"]
    assert res_data["data"]["createAccessToken"]["accessToken"]
    assert (
        res_data["data"]["createAccessToken"]["metadata"]["actorUrn"]
        == "urn:li:corpuser:user"
    )
    assert (
        res_data["data"]["createAccessToken"]["metadata"]["ownerUrn"]
        == "urn:li:corpuser:user"
    )
    user_tokenId = res_data["data"]["createAccessToken"]["metadata"]["id"]
    # Sleep for eventual consistency
    wait_for_writes_to_sync()

    # Admin should be able to list other tokens
    user_session.cookies.clear()
    admin_session = login_as(admin_user, admin_pass)
    res_data = listAccessTokens(
        admin_session,
        [
            {"field": "ownerUrn", "values": ["urn:li:corpuser:user"]},
            auth_exclude_filter,
        ],
    )
    assert res_data
    assert res_data["data"]
    assert res_data["data"]["listAccessTokens"]["total"] is not None
    assert len(res_data["data"]["listAccessTokens"]["tokens"]) == 1
    assert (
        res_data["data"]["listAccessTokens"]["tokens"][0]["actorUrn"]
        == "urn:li:corpuser:user"
    )
    assert (
        res_data["data"]["listAccessTokens"]["tokens"][0]["ownerUrn"]
        == "urn:li:corpuser:user"
    )
    assert res_data["data"]["listAccessTokens"]["tokens"][0]["id"] == user_tokenId

    # Admin can delete token created by someone else.
    admin_session.cookies.clear()
    admin_session = login_as(admin_user, admin_pass)
    res_data = revokeAccessToken(admin_session, user_tokenId)
    assert res_data
    assert res_data["data"]
    assert res_data["data"]["revokeAccessToken"]
    assert res_data["data"]["revokeAccessToken"] is True

    # Using a normal account, check that all its tokens where removed.
    user_session.cookies.clear()
    user_session = login_as("user", "user")
    res_data = listAccessTokens(
        user_session,
        [
            {"field": "ownerUrn", "values": ["urn:li:corpuser:user"]},
            auth_exclude_filter,
        ],
    )
    assert res_data
    assert res_data["data"]
    assert res_data["data"]["listAccessTokens"]["total"] is not None
    assert len(res_data["data"]["listAccessTokens"]["tokens"]) == 0

    # Using the super account, check that all tokens where removed.
    admin_session = login_as(admin_user, admin_pass)
    res_data = listAccessTokens(
        admin_session,
        [
            {"field": "ownerUrn", "values": ["urn:li:corpuser:user"]},
            auth_exclude_filter,
        ],
    )
    assert res_data
    assert res_data["data"]
    assert res_data["data"]["listAccessTokens"]["total"] is not None
    assert len(res_data["data"]["listAccessTokens"]["tokens"]) == 0


def test_non_admin_can_not_generate_tokens_for_others():
    user_session = login_as("user", "user")
    # Normal user should not be able to generate token for another user
    res_data = generateAccessToken_v2(user_session, f"urn:li:corpuser:{admin_user}")
    assert res_data
    assert res_data["errors"]
    assert (
        res_data["errors"][0]["message"]
        == "Unauthorized to perform this action. Please contact your DataHub administrator."
    )


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


def getAccessTokenMetadata(session, token):
    json = {
        "query": """
        query getAccessTokenMetadata($token: String!) {
            getAccessTokenMetadata(token: $token) {
                id
                ownerUrn
                actorUrn
            }
        }""",
        "variables": {"token": token},
    }
    response = session.post(f"{get_frontend_url()}/api/v2/graphql", json=json)
    response.raise_for_status()

    return response.json()
