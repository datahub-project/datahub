import logging
from typing import Any, Dict

import pytest

from tests.utils import execute_graphql, get_root_urn, with_test_retry

logger = logging.getLogger(__name__)
TEST_POLICY_NAME = "Updated Platform Policy"


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
