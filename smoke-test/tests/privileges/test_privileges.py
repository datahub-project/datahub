import pytest
import tenacity

from tests.utils import (get_frontend_session, wait_for_writes_to_sync, wait_for_healthcheck_util,
                        get_frontend_url, get_admin_credentials,get_sleep_info)
from tests.privileges.utils import *

pytestmark = pytest.mark.no_cypress_suite1

sleep_sec, sleep_times = get_sleep_info()

@pytest.fixture(scope="session")
def wait_for_healthchecks():
    wait_for_healthcheck_util()
    yield


@pytest.mark.dependency()
def test_healthchecks(wait_for_healthchecks):
    # Call to wait_for_healthchecks fixture will do the actual functionality.
    pass


@pytest.fixture(scope="session")
def admin_session(wait_for_healthchecks):
    yield get_frontend_session()


@pytest.mark.dependency(depends=["test_healthchecks"])
@pytest.fixture(scope="module", autouse=True)
def privileges_and_test_user_setup(admin_session):
    """Fixture to execute setup before and tear down after all tests are run"""
    # Disable 'All users' privileges
    set_base_platform_privileges_policy_status("INACTIVE", admin_session)
    set_view_dataset_sensitive_info_policy_status("INACTIVE", admin_session)
    set_view_entity_profile_privileges_policy_status("INACTIVE", admin_session)
    # Sleep for eventual consistency
    wait_for_writes_to_sync()

    # Create a new user   
    admin_session = create_user(admin_session, "user", "user")

    yield

    # Remove test user
    remove_user(admin_session, "urn:li:corpuser:user")

    # Restore All users privileges
    set_base_platform_privileges_policy_status("ACTIVE", admin_session)
    set_view_dataset_sensitive_info_policy_status("ACTIVE", admin_session)
    set_view_entity_profile_privileges_policy_status("ACTIVE", admin_session)

    # Sleep for eventual consistency
    wait_for_writes_to_sync()


@tenacity.retry(
    stop=tenacity.stop_after_attempt(sleep_times), wait=tenacity.wait_fixed(sleep_sec)
)
def _ensure_cant_perform_action(session, json,assertion_key):
    action_response = session.post(
        f"{get_frontend_url()}/api/v2/graphql", json=json)
    action_response.raise_for_status()
    action_data = action_response.json()

    assert action_data["errors"][0]["extensions"]["code"] == 403
    assert action_data["errors"][0]["extensions"]["type"] == "UNAUTHORIZED"
    assert action_data["data"][assertion_key] == None


@tenacity.retry(
    stop=tenacity.stop_after_attempt(10), wait=tenacity.wait_fixed(sleep_sec)
)
def _ensure_can_create_secret(session, json, urn):
    create_secret_success = session.post(
        f"{get_frontend_url()}/api/v2/graphql", json=json)
    create_secret_success.raise_for_status()
    secret_data = create_secret_success.json()

    assert secret_data
    assert secret_data["data"]
    assert secret_data["data"]["createSecret"]
    assert secret_data["data"]["createSecret"] == urn
    

@tenacity.retry(
    stop=tenacity.stop_after_attempt(10), wait=tenacity.wait_fixed(sleep_sec)
)
def _ensure_can_create_ingestion_source(session, json):
    create_ingestion_success = session.post(
        f"{get_frontend_url()}/api/v2/graphql", json=json)
    create_ingestion_success.raise_for_status()
    ingestion_data = create_ingestion_success.json()

    assert ingestion_data
    assert ingestion_data["data"]
    assert ingestion_data["data"]["createIngestionSource"]
    assert ingestion_data["data"]["createIngestionSource"] is not None

    return ingestion_data["data"]["createIngestionSource"]
    

@tenacity.retry(
    stop=tenacity.stop_after_attempt(10), wait=tenacity.wait_fixed(sleep_sec)
)
def _ensure_can_create_access_token(session, json):
    create_access_token_success = session.post(
        f"{get_frontend_url()}/api/v2/graphql", json=json)
    create_access_token_success.raise_for_status()
    ingestion_data = create_access_token_success.json()

    assert ingestion_data
    assert ingestion_data["data"]
    assert ingestion_data["data"]["createAccessToken"]
    assert ingestion_data["data"]["createAccessToken"]["accessToken"] is not None
    assert ingestion_data["data"]["createAccessToken"]["__typename"] == "AccessToken"


@tenacity.retry(
    stop=tenacity.stop_after_attempt(10), wait=tenacity.wait_fixed(sleep_sec)
)
def _ensure_can_create_user_policy(session, json):
    response = session.post(f"{get_frontend_url()}/api/v2/graphql", json=json)
    response.raise_for_status()
    res_data = response.json()

    assert res_data
    assert res_data["data"]
    assert res_data["data"]["createPolicy"] is not None

    return res_data["data"]["createPolicy"] 


@pytest.mark.dependency(depends=["test_healthchecks"])
def test_privilege_to_create_and_manage_secrets():

    (admin_user, admin_pass) = get_admin_credentials()
    admin_session = login_as(admin_user, admin_pass)
    user_session = login_as("user", "user")
    secret_urn = "urn:li:dataHubSecret:TestSecretName"

    # Verify new user can't create secrets
    create_secret = { 
        "query": """mutation createSecret($input: CreateSecretInput!) {\n
            createSecret(input: $input)\n}""",
        "variables": {
            "input":{
                "name":"TestSecretName",
                "value":"Test Secret Value",
                "description":"Test Secret Description"
                }
        },
    }
    _ensure_cant_perform_action(user_session, create_secret,"createSecret")


    # Assign privileges to the new user to manage secrets
    policy_urn = create_user_policy("urn:li:corpuser:user", ["MANAGE_SECRETS"], admin_session)

    # Verify new user can create and manage secrets
    # Create a secret
    _ensure_can_create_secret(user_session, create_secret, secret_urn)


    # Remove a secret
    remove_secret = { 
        "query": """mutation deleteSecret($urn: String!) {\n
            deleteSecret(urn: $urn)\n}""",
        "variables": {
            "urn": secret_urn
        },
    }

    remove_secret_response = user_session.post(f"{get_frontend_url()}/api/v2/graphql", json=remove_secret)
    remove_secret_response.raise_for_status()
    secret_data = remove_secret_response.json()

    assert secret_data
    assert secret_data["data"]
    assert secret_data["data"]["deleteSecret"]
    assert secret_data["data"]["deleteSecret"] == secret_urn


    # Remove the policy
    remove_policy(policy_urn, admin_session)

    # Ensure user can't create secret after policy is removed
    _ensure_cant_perform_action(user_session, create_secret,"createSecret")


@pytest.mark.dependency(depends=["test_healthchecks"])
def test_privilege_to_create_and_manage_ingestion_source():

    (admin_user, admin_pass) = get_admin_credentials()
    admin_session = login_as(admin_user, admin_pass)
    user_session = login_as("user", "user")

    # Verify new user can't create ingestion source
    create_ingestion_source = { 
        "query": """mutation createIngestionSource($input: UpdateIngestionSourceInput!) {\n
            createIngestionSource(input: $input)\n}""",
        "variables": {"input":{"type":"snowflake","name":"test","config":
            {"recipe":
                """{\"source\":{\"type\":\"snowflake\",\"config\":{
                    \"account_id\":null,
                    \"include_table_lineage\":true,
                    \"include_view_lineage\":true,
                    \"include_tables\":true,
                    \"include_views\":true,
                    \"profiling\":{\"enabled\":true,\"profile_table_level_only\":true},
                    \"stateful_ingestion\":{\"enabled\":true}}}}""",
                "executorId":"default","debugMode":False,"extraArgs":[]}}},
    }

    _ensure_cant_perform_action(user_session, create_ingestion_source, "createIngestionSource")


     # Assign privileges to the new user to manage ingestion source
    policy_urn = create_user_policy("urn:li:corpuser:user", ["MANAGE_INGESTION"], admin_session)
   
    # Verify new user can create and manage ingestion source(edit, delete)
    ingestion_source_urn = _ensure_can_create_ingestion_source(user_session, create_ingestion_source)

    # Edit ingestion source
    update_ingestion_source = { 
        "query": """mutation updateIngestionSource($urn: String!, $input: UpdateIngestionSourceInput!) {\n
            updateIngestionSource(urn: $urn, input: $input)\n}""",
        "variables": {"urn":ingestion_source_urn,
              "input":{"type":"snowflake","name":"test updated",
                       "config":{"recipe":"""{\"source\":{\"type\":\"snowflake\",\"config\":{
                                 \"account_id\":null,
                                 \"include_table_lineage\":true,
                                 \"include_view_lineage\":true,
                                 \"include_tables\":true,
                                 \"include_views\":true,
                                 \"profiling\":{\"enabled\":true,\"profile_table_level_only\":true},
                                 \"stateful_ingestion\":{\"enabled\":true}}}}""",
                        "executorId":"default","debugMode":False,"extraArgs":[]}}}
    }

    update_ingestion_success = user_session.post(
        f"{get_frontend_url()}/api/v2/graphql", json=update_ingestion_source)
    update_ingestion_success.raise_for_status()
    ingestion_data = update_ingestion_success.json()

    assert ingestion_data
    assert ingestion_data["data"]
    assert ingestion_data["data"]["updateIngestionSource"]
    assert ingestion_data["data"]["updateIngestionSource"] == ingestion_source_urn


    # Delete ingestion source
    remove_ingestion_source = { 
        "query": """mutation deleteIngestionSource($urn: String!) {\n
            deleteIngestionSource(urn: $urn)\n}""",
        "variables": {
            "urn": ingestion_source_urn
        },
    }

    remove_ingestion_response = user_session.post(f"{get_frontend_url()}/api/v2/graphql", json=remove_ingestion_source)
    remove_ingestion_response.raise_for_status()
    ingestion_data = remove_ingestion_response.json()

    assert ingestion_data
    assert ingestion_data["data"]
    assert ingestion_data["data"]["deleteIngestionSource"]
    assert ingestion_data["data"]["deleteIngestionSource"] == ingestion_source_urn

    # Remove the policy
    remove_policy(policy_urn, admin_session)

    # Ensure that user can't create ingestion source after policy is removed
    _ensure_cant_perform_action(user_session, create_ingestion_source, "createIngestionSource")


@pytest.mark.dependency(depends=["test_healthchecks"])
def test_privilege_to_create_and_manage_access_tokens():

    (admin_user, admin_pass) = get_admin_credentials()
    admin_session = login_as(admin_user, admin_pass)
    user_session = login_as("user", "user")
  

    # Verify new user can't create access token
    create_access_token = { 
        "query": """mutation createAccessToken($input: CreateAccessTokenInput!) {\n 
          createAccessToken(input: $input) {\n    accessToken\n    __typename\n  }\n}\n""",
        "variables": {"input":{"actorUrn":"urn:li:corpuser:user",
                               "type":"PERSONAL",
                               "duration":"ONE_MONTH",
                               "name":"test",
                               "description":"test"}}
    }

    _ensure_cant_perform_action(user_session, create_access_token,"createAccessToken")


    # Assign privileges to the new user to create and manage access tokens
    policy_urn = create_user_policy("urn:li:corpuser:user", ["MANAGE_ACCESS_TOKENS"], admin_session)
   

    # Verify new user can create and manage access token(create, revoke)
    # Create a access token
    _ensure_can_create_access_token(user_session, create_access_token)


    # List access tokens first to get token id
    list_access_tokens = { 
        "query": """query listAccessTokens($input: ListAccessTokenInput!) {\n 
          listAccessTokens(input: $input) {\n
                start\n count\n total\n tokens {\n urn\n type\n
                id\n name\n description\n actorUrn\n ownerUrn\n  
                createdAt\n expiresAt\n __typename\n }\n __typename\n  }\n}\n""",
        "variables": {
            "input":{
                "start":0,"count":10,"filters":[{
                    "field":"ownerUrn",
                    "values":["urn:li:corpuser:user"]}]}
        }
    }

    list_tokens_response = user_session.post(f"{get_frontend_url()}/api/v2/graphql", json=list_access_tokens)
    list_tokens_response.raise_for_status()
    list_tokens_data = list_tokens_response.json()

    assert list_tokens_data
    assert list_tokens_data["data"]
    assert list_tokens_data["data"]["listAccessTokens"]["tokens"][0]["id"] is not None
    
    access_token_id = list_tokens_data["data"]["listAccessTokens"]["tokens"][0]["id"]


    # Revoke access token
    revoke_access_token = { 
        "query": "mutation revokeAccessToken($tokenId: String!) {\n  revokeAccessToken(tokenId: $tokenId)\n}\n",
        "variables": {
            "tokenId": access_token_id
        },
    }

    revoke_token_response = user_session.post(f"{get_frontend_url()}/api/v2/graphql", json=revoke_access_token)
    revoke_token_response.raise_for_status()
    revoke_token_data = revoke_token_response.json()

    assert revoke_token_data
    assert revoke_token_data["data"]
    assert revoke_token_data["data"]["revokeAccessToken"]
    assert revoke_token_data["data"]["revokeAccessToken"] is True


    # Remove the policy
    remove_policy(policy_urn, admin_session)


    # Ensure that user can't create access token after policy is removed
    _ensure_cant_perform_action(user_session, create_access_token,"createAccessToken")


@pytest.mark.dependency(depends=["test_healthchecks"])
def test_privilege_to_create_and_manage_policies():

    (admin_user, admin_pass) = get_admin_credentials()
    admin_session = login_as(admin_user, admin_pass)
    user_session = login_as("user", "user")
  

    # Verify new user can't create a policy
    create_policy = {
        "query": """mutation createPolicy($input: PolicyUpdateInput!) {\n
            createPolicy(input: $input) }""",
        "variables": {
            "input": {
                "type": "PLATFORM",
                "name": "Policy Name",
                "description": "Policy Description",
                "state": "ACTIVE",
                "resources": {"filter":{"criteria":[]}},
                "privileges": ["MANAGE_POLICIES"],
                "actors": {
                    "users": [],
                    "resourceOwners": False,
                    "allUsers": True,
                    "allGroups": False,
                },
            }
        },
    }

    _ensure_cant_perform_action(user_session, create_policy,"createPolicy")


    # Assign privileges to the new user to create and manage policies
    admin_policy_urn = create_user_policy("urn:li:corpuser:user", ["MANAGE_POLICIES"], admin_session)
   

    # Verify new user can create and manage policy(create, edit, delete)
    # Create a policy
    user_policy_urn = _ensure_can_create_user_policy(user_session, create_policy)

    # Edit a policy
    edit_policy = { 
        "query": """mutation updatePolicy($urn: String!, $input: PolicyUpdateInput!) {\n
            updatePolicy(urn: $urn, input: $input) }""",
        "variables": {
            "urn": user_policy_urn,
            "input": {
                "type": "PLATFORM",
                "state": "INACTIVE",
                "name": "Policy Name test",
                "description": "Policy Description updated",
                "privileges": ["MANAGE_POLICIES"],
                "actors": {
                    "users": [],
                    "groups": None,
                    "resourceOwners": False,
                    "allUsers": True,
                    "allGroups": False,
                    "resourceOwnersTypes": None,
                },
            },
        },
    }
    edit_policy_response = user_session.post(f"{get_frontend_url()}/api/v2/graphql", json=edit_policy)
    edit_policy_response.raise_for_status()
    res_data = edit_policy_response.json()

    assert res_data
    assert res_data["data"]
    assert res_data["data"]["updatePolicy"] == user_policy_urn

    # Delete a policy
    remove_user_policy = { 
        "query": "mutation deletePolicy($urn: String!) {\n  deletePolicy(urn: $urn)\n}\n",
        "variables":{"urn":user_policy_urn}
    }

    remove_policy_response = user_session.post(f"{get_frontend_url()}/api/v2/graphql", json=remove_user_policy)
    remove_policy_response.raise_for_status()
    res_data = remove_policy_response.json()

    assert res_data
    assert res_data["data"]
    assert res_data["data"]["deletePolicy"] == user_policy_urn


    # Remove the user privilege by admin
    remove_policy(admin_policy_urn, admin_session)


    # Ensure that user can't create a policy after privilege is removed by admin
    _ensure_cant_perform_action(user_session, create_policy,"createPolicy")