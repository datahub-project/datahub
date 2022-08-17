import os
import pytest
import requests
from time import sleep

from tests.utils import get_frontend_url, wait_for_healthcheck_util, get_admin_credentials


# Disable telemetry
os.environ["DATAHUB_TELEMETRY_ENABLED"] = "false"

(admin_user, admin_pass) = get_admin_credentials()


@pytest.fixture(scope="session")
def wait_for_healthchecks():
    wait_for_healthcheck_util()
    yield

@pytest.mark.dependency()
def test_healthchecks(wait_for_healthchecks):
    # Call to wait_for_healthchecks fixture will do the actual functionality.
    pass

@pytest.mark.dependency(depends=["test_healthchecks"])
@pytest.fixture(scope='class', autouse=True)
def custom_user_setup():
  """Fixture to execute setup before and tear down after all tests are run"""
  admin_session = loginAs(admin_user, admin_pass)

  res_data = removeUser(admin_session, "urn:li:corpuser:user")
  assert res_data
  assert "error" not in res_data

  # Test getting the invite token
  get_invite_token_json = {
      "query": """query getNativeUserInviteToken {\n
          getNativeUserInviteToken{\n
            inviteToken\n
          }\n
      }"""
  }

  get_invite_token_response = admin_session.post(f"{get_frontend_url()}/api/v2/graphql", json=get_invite_token_json)
  get_invite_token_response.raise_for_status()
  get_invite_token_res_data = get_invite_token_response.json()

  assert get_invite_token_res_data
  assert get_invite_token_res_data["data"]
  invite_token = get_invite_token_res_data["data"]["getNativeUserInviteToken"]["inviteToken"]
  assert invite_token is not None
  assert "error" not in invite_token

  # Pass the invite token when creating the user
  sign_up_json = {
    "fullName": "Test User",
    "email": "user",
    "password": "user",
    "title": "Date Engineer",
    "inviteToken": invite_token
  }

  sign_up_response = admin_session.post(f"{get_frontend_url()}/signUp", json=sign_up_json)
  sign_up_response.raise_for_status()
  assert sign_up_response
  assert "error" not in sign_up_response
  # Sleep for eventual consistency
  sleep(3)

  # signUp will override the session cookie to the new user to be signed up.
  admin_session.cookies.clear()
  admin_session = loginAs(admin_user, admin_pass)

  # Make user created user is there.
  res_data = listUsers(admin_session)
  assert res_data["data"]
  assert res_data["data"]["listUsers"]
  assert {'username': 'user'} in res_data["data"]["listUsers"]["users"]

  yield

  # Delete created user
  res_data = removeUser(admin_session, "urn:li:corpuser:user")
  assert res_data
  assert res_data['data']
  assert res_data['data']['removeUser'] == True
  # Sleep for eventual consistency
  sleep(3)

  # Make user created user is not there.
  res_data = listUsers(admin_session)
  assert res_data["data"]
  assert res_data["data"]["listUsers"]
  assert {'username': 'user'} not in res_data["data"]["listUsers"]["users"]

@pytest.mark.dependency(depends=["test_healthchecks"])
@pytest.fixture(autouse=True)
def access_token_setup():
    """Fixture to execute asserts before and after a test is run"""
    admin_session = loginAs(admin_user, admin_pass)

    res_data = listAccessTokens(admin_session)
    assert res_data
    assert res_data["data"]
    assert res_data["data"]["listAccessTokens"]["total"] == 0
    assert not res_data["data"]["listAccessTokens"]["tokens"]

    yield

    # Clean up
    res_data = listAccessTokens(admin_session)
    for metadata in res_data["data"]["listAccessTokens"]["tokens"]:
        revokeAccessToken(admin_session, metadata["id"])

    # Sleep for eventual consistency
    sleep(3)

@pytest.mark.dependency(depends=["test_healthchecks"])
def test_admin_can_create_list_and_revoke_tokens(wait_for_healthchecks):
    admin_session = loginAs(admin_user, admin_pass)

    # Using a super account, there should be no tokens
    res_data = listAccessTokens(admin_session)
    assert res_data
    assert res_data["data"]
    assert res_data["data"]["listAccessTokens"]["total"] is not None
    assert len(res_data["data"]["listAccessTokens"]["tokens"]) == 0

    # Using a super account, generate a token for itself.
    res_data = generateAccessToken_v2(admin_session, f"urn:li:corpuser:{admin_user}")
    assert res_data
    assert res_data["data"]
    assert res_data["data"]["createAccessToken"]
    assert res_data["data"]["createAccessToken"]["accessToken"]
    assert res_data["data"]["createAccessToken"]["metadata"]["actorUrn"] == f"urn:li:corpuser:{admin_user}"
    admin_tokenId = res_data["data"]["createAccessToken"]["metadata"]["id"]
    # Sleep for eventual consistency
    sleep(3)

    # Using a super account, list the previously created token.
    res_data = listAccessTokens(admin_session)
    assert res_data
    assert res_data["data"]
    assert res_data["data"]["listAccessTokens"]["total"] is not None
    assert len(res_data["data"]["listAccessTokens"]["tokens"]) == 1
    assert res_data["data"]["listAccessTokens"]["tokens"][0]["actorUrn"] == f"urn:li:corpuser:{admin_user}"
    assert res_data["data"]["listAccessTokens"]["tokens"][0]["ownerUrn"] == f"urn:li:corpuser:{admin_user}"

    # Check that the super account can revoke tokens that it created
    res_data = revokeAccessToken(admin_session, admin_tokenId)
    assert res_data
    assert res_data["data"]
    assert res_data["data"]["revokeAccessToken"]
    assert res_data["data"]["revokeAccessToken"] == True
    # Sleep for eventual consistency
    sleep(3)

    # Using a super account, there should be no tokens
    res_data = listAccessTokens(admin_session)
    assert res_data
    assert res_data["data"]
    assert res_data["data"]["listAccessTokens"]["total"] is not None
    assert len(res_data["data"]["listAccessTokens"]["tokens"]) == 0

@pytest.mark.dependency(depends=["test_healthchecks"])
def test_admin_can_create_and_revoke_tokens_for_other_user(wait_for_healthchecks):
    admin_session = loginAs(admin_user, admin_pass)

    # Using a super account, there should be no tokens
    res_data = listAccessTokens(admin_session)
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
    assert res_data["data"]["createAccessToken"]["metadata"]["actorUrn"] == "urn:li:corpuser:user"
    user_tokenId = res_data["data"]["createAccessToken"]["metadata"]["id"]
    # Sleep for eventual consistency
    sleep(3)

    # Using a super account, list the previously created tokens.
    res_data = listAccessTokens(admin_session)
    assert res_data
    assert res_data["data"]
    assert res_data["data"]["listAccessTokens"]["total"] is not None
    assert len(res_data["data"]["listAccessTokens"]["tokens"]) == 1
    assert res_data["data"]["listAccessTokens"]["tokens"][0]["actorUrn"] == "urn:li:corpuser:user"
    assert res_data["data"]["listAccessTokens"]["tokens"][0]["ownerUrn"] == f"urn:li:corpuser:{admin_user}"

    # Check that the super account can revoke tokens that it created for another user
    res_data = revokeAccessToken(admin_session, user_tokenId)
    assert res_data
    assert res_data["data"]
    assert res_data["data"]["revokeAccessToken"]
    assert res_data["data"]["revokeAccessToken"] == True
    # Sleep for eventual consistency
    sleep(3)

    # Using a super account, there should be no tokens
    res_data = listAccessTokens(admin_session)
    assert res_data
    assert res_data["data"]
    assert res_data["data"]["listAccessTokens"]["total"] is not None
    assert len(res_data["data"]["listAccessTokens"]["tokens"]) == 0

@pytest.mark.dependency(depends=["test_healthchecks"])
def test_non_admin_can_create_list_revoke_tokens(wait_for_healthchecks):
    user_session = loginAs("user", "user")

    # Normal user should be able to generate token for himself.
    res_data = generateAccessToken_v2(user_session, "urn:li:corpuser:user")
    assert res_data
    assert res_data["data"]
    assert res_data["data"]["createAccessToken"]
    assert res_data["data"]["createAccessToken"]["accessToken"]
    assert res_data["data"]["createAccessToken"]["metadata"]["actorUrn"] == "urn:li:corpuser:user"
    user_tokenId = res_data["data"]["createAccessToken"]["metadata"]["id"]
    # Sleep for eventual consistency
    sleep(3)

    # User should be able to list his own token
    res_data = listAccessTokens(user_session, [{"field": "ownerUrn","value": "urn:li:corpuser:user"}])
    assert res_data
    assert res_data["data"]
    assert res_data["data"]["listAccessTokens"]["total"] is not None
    assert len(res_data["data"]["listAccessTokens"]["tokens"]) == 1
    assert res_data["data"]["listAccessTokens"]["tokens"][0]["actorUrn"] == "urn:li:corpuser:user"
    assert res_data["data"]["listAccessTokens"]["tokens"][0]["ownerUrn"] == "urn:li:corpuser:user"
    assert res_data["data"]["listAccessTokens"]["tokens"][0]["id"] == user_tokenId

    # User should be able to revoke his own token
    res_data = revokeAccessToken(user_session, user_tokenId)
    assert res_data
    assert res_data["data"]
    assert res_data["data"]["revokeAccessToken"]
    assert res_data["data"]["revokeAccessToken"] == True
    # Sleep for eventual consistency
    sleep(3)

    # Using a normal account, check that all its tokens where removed.
    res_data = listAccessTokens(user_session, [{"field": "ownerUrn","value": "urn:li:corpuser:user"}])
    assert res_data
    assert res_data["data"]
    assert res_data["data"]["listAccessTokens"]["total"] is not None
    assert len(res_data["data"]["listAccessTokens"]["tokens"]) == 0

@pytest.mark.dependency(depends=["test_healthchecks"])
def test_admin_can_manage_tokens_generated_by_other_user(wait_for_healthchecks):
    admin_session = loginAs(admin_user, admin_pass)

    # Using a super account, there should be no tokens
    res_data = listAccessTokens(admin_session)
    assert res_data
    assert res_data["data"]
    assert res_data["data"]["listAccessTokens"]["total"] is not None
    assert len(res_data["data"]["listAccessTokens"]["tokens"]) == 0

    admin_session.cookies.clear()
    user_session = loginAs("user", "user")
    res_data = generateAccessToken_v2(user_session, "urn:li:corpuser:user")
    assert res_data
    assert res_data["data"]
    assert res_data["data"]["createAccessToken"]
    assert res_data["data"]["createAccessToken"]["accessToken"]
    assert res_data["data"]["createAccessToken"]["metadata"]["actorUrn"] == "urn:li:corpuser:user"
    assert res_data["data"]["createAccessToken"]["metadata"]["ownerUrn"] == "urn:li:corpuser:user"
    user_tokenId = res_data["data"]["createAccessToken"]["metadata"]["id"]
    # Sleep for eventual consistency
    sleep(3)

    # Admin should be able to list other tokens
    user_session.cookies.clear()
    admin_session = loginAs(admin_user, admin_pass)
    res_data = listAccessTokens(admin_session, [{"field": "ownerUrn","value": "urn:li:corpuser:user"}])
    assert res_data
    assert res_data["data"]
    assert res_data["data"]["listAccessTokens"]["total"] is not None
    assert len(res_data["data"]["listAccessTokens"]["tokens"]) == 1
    assert res_data["data"]["listAccessTokens"]["tokens"][0]["actorUrn"] == "urn:li:corpuser:user"
    assert res_data["data"]["listAccessTokens"]["tokens"][0]["ownerUrn"] == "urn:li:corpuser:user"
    assert res_data["data"]["listAccessTokens"]["tokens"][0]["id"] == user_tokenId

    # Admin can delete token created by someone else.
    admin_session.cookies.clear()
    admin_session = loginAs(admin_user, admin_pass)
    res_data = revokeAccessToken(admin_session, user_tokenId)
    assert res_data
    assert res_data["data"]
    assert res_data["data"]["revokeAccessToken"]
    assert res_data["data"]["revokeAccessToken"] == True
    # Sleep for eventual consistency
    sleep(3)

    # Using a normal account, check that all its tokens where removed.
    user_session.cookies.clear()
    user_session = loginAs("user", "user")
    res_data = listAccessTokens(user_session, [{"field": "ownerUrn","value": "urn:li:corpuser:user"}])
    assert res_data
    assert res_data["data"]
    assert res_data["data"]["listAccessTokens"]["total"] is not None
    assert len(res_data["data"]["listAccessTokens"]["tokens"]) == 0

    # Using the super account, check that all tokens where removed.
    admin_session = loginAs(admin_user, admin_pass)
    res_data = listAccessTokens(admin_session, [{"field": "ownerUrn","value": "urn:li:corpuser:user"}])
    assert res_data
    assert res_data["data"]
    assert res_data["data"]["listAccessTokens"]["total"] is not None
    assert len(res_data["data"]["listAccessTokens"]["tokens"]) == 0

@pytest.mark.dependency(depends=["test_healthchecks"])
def test_non_admin_can_not_generate_tokens_for_others(wait_for_healthchecks):
    user_session = loginAs("user", "user")
    # Normal user should not be able to generate token for another user
    res_data = generateAccessToken_v2(user_session, f"urn:li:corpuser:{admin_user}")
    assert res_data
    assert res_data["errors"]
    assert res_data["errors"][0]["message"] == "Unauthorized to perform this action. Please contact your DataHub administrator."

def generateAccessToken_v2(session, actorUrn):
    # Create new token
    json = {
        "query": """mutation createAccessToken($input: CreateAccessTokenInput!) {\n
            createAccessToken(input: $input) {\n
              accessToken\n
              metadata {\n
                id\n
                actorUrn\n
                ownerUrn\n
                name\n
                description\n
              }
            }\n
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


def listAccessTokens(session, filters=[]):
    # Get count of existing tokens
    input = {
        "start": "0",
        "count": "20",
    }

    if filters:
        input["filters"] = filters

    json = {
        "query": """query listAccessTokens($input: ListAccessTokenInput!) {\n
            listAccessTokens(input: $input) {\n
              start\n
              count\n
              total\n
              tokens {\n
                urn\n
                id\n
                actorUrn\n
                ownerUrn\n
              }\n
            }\n
        }""",
        "variables": {"input": input},
    }

    response = session.post(f"{get_frontend_url()}/api/v2/graphql", json=json)
    response.raise_for_status()
    return response.json()


def revokeAccessToken(session, tokenId):
    # Revoke token
    json = {
        "query": """mutation revokeAccessToken($tokenId: String!) {\n
            revokeAccessToken(tokenId: $tokenId)
        }""",
        "variables": {"tokenId": tokenId},
    }

    response = session.post(f"{get_frontend_url()}/api/v2/graphql", json=json)
    response.raise_for_status()

    return response.json()


def loginAs(username, password):
    session = requests.Session()

    headers = {
        "Content-Type": "application/json",
    }
    data = '{"username":"' + username + '", "password":"' + password + '"}'
    response = session.post(f"{get_frontend_url()}/logIn", headers=headers, data=data)
    response.raise_for_status()

    return session


def removeUser(session, urn):
    # Remove user
    json = {
        "query": """mutation removeUser($urn: String!) {\n
            removeUser(urn: $urn)
        }""",
        "variables": {
          "urn": urn
        }
    }

    response = session.post(
        f"{get_frontend_url()}/api/v2/graphql", json=json
    )

    response.raise_for_status()
    return response.json()


def listUsers(session):
    input = {
      "start": "0",
      "count": "20",
    }

    # list users
    json = {
        "query": """query listUsers($input: ListUsersInput!) {\n
            listUsers(input: $input) {\n
              start\n
              count\n
              total\n
              users {\n
                username\n
              }
            }
        }""",
        "variables": {
          "input": input
        }
    }

    response = session.post(
        f"{get_frontend_url()}/api/v2/graphql", json=json
    )

    response.raise_for_status()
    return response.json()
