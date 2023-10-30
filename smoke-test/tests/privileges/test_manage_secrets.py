import pytest
import tenacity

from tests.utils import (get_frontend_session, wait_for_writes_to_sync, wait_for_healthcheck_util,
                        get_frontend_url, get_admin_credentials,get_sleep_info)
from tests.privileges.utils import (set_base_platform_privileges_policy_status, set_view_dataset_sensitive_info_policy_status, 
                                    set_view_entity_profile_privileges_policy_status, create_user, remove_user,login_as)

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
    stop=tenacity.stop_after_attempt(10), wait=tenacity.wait_fixed(sleep_sec)
)
def _ensure_can_create_secret(session, json, url):
    create_secret_success = session.post(
        f"{get_frontend_url()}/api/v2/graphql", json=json)
    create_secret_success.raise_for_status()
    secret_data = create_secret_success.json()

    assert secret_data
    assert secret_data["data"]
    assert secret_data["data"]["createSecret"]
    assert secret_data["data"]["createSecret"] == url
    

@tenacity.retry(
    stop=tenacity.stop_after_attempt(sleep_times), wait=tenacity.wait_fixed(sleep_sec)
)
def _ensure_cant_create_secret(session, json):
    create_secret_response = session.post(
        f"{get_frontend_url()}/api/v2/graphql", json=json)
    create_secret_response.raise_for_status()
    create_secret_data = create_secret_response.json()

    assert create_secret_data["errors"][0]["extensions"]["code"] == 403
    assert create_secret_data["errors"][0]["extensions"]["type"] == "UNAUTHORIZED"
    assert create_secret_data["data"]["createSecret"] == None


@pytest.mark.dependency(depends=["test_healthchecks"])
def test_add_and_verify_privileges_to_manage_secrets():

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
    _ensure_cant_create_secret(user_session, create_secret)


    # Assign privileges to the new user to manage secrets
    manage_secrets = {
        "query": """mutation createPolicy($input: PolicyUpdateInput!) {\n
            createPolicy(input: $input) }""",
        "variables": {
            "input": {
                "type": "PLATFORM",
                "name": "Manage Secrets",
                "description": "Manage Secrets Policy",
                "state": "ACTIVE",
                "resources": {"filter":{"criteria":[]}},
                "privileges": ["MANAGE_SECRETS"],
                "actors": {
                    "users": ["urn:li:corpuser:user"],
                    "resourceOwners": False,
                    "allUsers": False,
                    "allGroups": False,
                },
            }
        },
    }

    response = admin_session.post(f"{get_frontend_url()}/api/v2/graphql", json=manage_secrets)
    response.raise_for_status()
    res_data = response.json()

    assert res_data
    assert res_data["data"]
    assert res_data["data"]["createPolicy"]
    policy_urn = res_data["data"]["createPolicy"]
   

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
    remove_policy = {
        "query": """mutation deletePolicy($urn: String!) {\n
            deletePolicy(urn: $urn) }""",
        "variables": {"urn": policy_urn},
    }

    response = admin_session.post(f"{get_frontend_url()}/api/v2/graphql", json=remove_policy)
    response.raise_for_status()
    res_data = response.json()

    assert res_data
    assert res_data["data"]
    assert res_data["data"]["deletePolicy"]
    assert res_data["data"]["deletePolicy"] == policy_urn


    # Ensure user can't create secret after policy is removed
    _ensure_cant_create_secret(user_session, create_secret)