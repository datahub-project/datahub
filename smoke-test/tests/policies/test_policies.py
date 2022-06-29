import time
import pytest
import requests
from tests.utils import FRONTEND_ENDPOINT
from datahub.cli.docker import check_local_docker_containers

TEST_POLICY_NAME = "Updated Platform Policy"

@pytest.fixture(scope="session")
def wait_for_healthchecks():
    # Simply assert that everything is healthy, but don't wait.
    assert not check_local_docker_containers()
    yield


@pytest.mark.dependency()
def test_healthchecks(wait_for_healthchecks):
    # Call to wait_for_healthchecks fixture will do the actual functionality.
    pass


@pytest.fixture(scope="session")
def frontend_session(wait_for_healthchecks):
    session = requests.Session()

    headers = {
        "Content-Type": "application/json",
    }
    data = '{"username":"datahub", "password":"datahub"}'
    response = session.post(f"{FRONTEND_ENDPOINT}/logIn", headers=headers, data=data)
    response.raise_for_status()

    yield session

@pytest.mark.dependency(depends=["test_healthchecks"])
@pytest.fixture(scope='class', autouse=True)
def frontend_list_policies(frontend_session):
    """Fixture to execute setup before and tear down after all tests are run"""
    res_data = listPolicies(frontend_session)

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

    res_data = listPolicies(frontend_session)

    assert res_data
    assert res_data["data"]
    assert res_data["data"]["listPolicies"]

    # Verify that policy that was created is no longer in the list
    result = filter(
      lambda x: x["name"] == TEST_POLICY_NAME,
      res_data["data"]["listPolicies"]["policies"],
    )
    assert len(list(result)) == 0


@pytest.mark.dependency(depends=["test_healthchecks"])
def test_frontend_update_policy(frontend_session):

    json = {
        "query": """mutation updatePolicy($urn: String!, $input: PolicyUpdateInput!) {\n
            updatePolicy(urn: $urn, input: $input) }""",
        "variables": {
            "urn": "urn:li:dataHubPolicy:7",
            "input": {
                "type": "PLATFORM",
                "state": "INACTIVE",
                "name": "All Users - All Platform Privileges",
                "description": "My Metadaata Policy",
                "privileges": ["MANAGE_POLICIES"],
                "actors": {
                    "users": ["urn:li:corpuser:datahub"],
                    "resourceOwners": False,
                    "allUsers": False,
                    "allGroups": False,
                },
            },
        },
    }

    response = frontend_session.post(f"{FRONTEND_ENDPOINT}/api/v2/graphql", json=json)
    response.raise_for_status()
    res_data = response.json()

    assert res_data
    assert res_data["data"]
    assert res_data["data"]["updatePolicy"]
    assert res_data["data"]["updatePolicy"] == "urn:li:dataHubPolicy:7"

@pytest.mark.dependency(depends=["test_healthchecks"])
def test_frontend_create_delete_policy(frontend_session):

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
                    "users": ["urn:li:corpuser:datahub"],
                    "resourceOwners": False,
                    "allUsers": False,
                    "allGroups": False,
                },
            }
        },
    }

    response = frontend_session.post(f"{FRONTEND_ENDPOINT}/api/v2/graphql", json=json)
    response.raise_for_status()
    res_data = response.json()

    assert res_data
    assert res_data["data"]
    assert res_data["data"]["createPolicy"]

    new_urn = res_data["data"]["createPolicy"]

    # Sleep for eventual consistency
    time.sleep(3)

    res_data = listPolicies(frontend_session)

    assert res_data
    assert res_data["data"]
    assert res_data["data"]["listPolicies"]

    # Verify that the URN appears in the list
    result = filter(
        lambda x: x["urn"] == new_urn, res_data["data"]["listPolicies"]["policies"]
    )
    assert len(list(result)) == 1

    # Now test that policy can be deleted
    json = {
        "query": """mutation deletePolicy($urn: String!) {\n
            deletePolicy(urn: $urn) }""",
        "variables": {"urn": new_urn},
    }

    response = frontend_session.post(f"{FRONTEND_ENDPOINT}/api/v2/graphql", json=json)
    response.raise_for_status()
    res_data = response.json()

    res_data = listPolicies(frontend_session)

    assert res_data
    assert res_data["data"]
    assert res_data["data"]["listPolicies"]

    # Verify that the URN is no longer in the list
    result = filter(
        lambda x: x["urn"] == new_urn,
        res_data["data"]["listPolicies"]["policies"],
    )
    assert len(list(result)) == 0

def listPolicies(session):
    json = {
        "query": """query listPolicies($input: ListPoliciesInput!) {\n
            listPolicies(input: $input) {\n
                start\n
                count\n
                total\n
                policies {\n
                    urn\n
                    type\n
                    name\n
                    description\n
                    state\n
                    resources {\n
                      type\n
                      allResources\n
                      resources\n
                    }\n
                    privileges\n
                    actors {\n
                      users\n
                      groups\n
                      allUsers\n
                      allGroups\n
                      resourceOwners\n
                    }\n
                    editable\n
                }\n
            }\n
        }""",
        "variables": {
            "input": {
                "start": "0",
                "count": "20",
            }
        },
    }
    response = session.post(f"{FRONTEND_ENDPOINT}/api/v2/graphql", json=json)
    response.raise_for_status()

    return response.json()
