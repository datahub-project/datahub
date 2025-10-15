import time
from typing import Any, Dict, List

import pytest

from conftest import _ingest_cleanup_data_impl
from tests.utils import execute_graphql, with_test_retry

TEST_URNS: List[str] = []


@pytest.fixture(scope="module", autouse=True)
def ingest_cleanup_data(auth_session, graph_client):
    yield from _ingest_cleanup_data_impl(
        auth_session,
        graph_client,
        "tests/tests/data.json",
        "tests",
        to_delete_urns=TEST_URNS,
    )


test_name = "test name"
test_category = "test category"
test_description = "test description"
test_description = "test description"


def create_test(auth_session, test_id="test id"):
    test_id = f"{test_id}_{int(time.time())}"
    TEST_URNS.extend([f"urn:li:test:{test_id}"])

    # Create new Test
    create_test_query = """mutation createTest($input: CreateTestInput!) {
            createTest(input: $input)
        }"""
    create_test_variables: Dict[str, Any] = {
        "input": {
            "id": test_id,
            "name": test_name,
            "category": test_category,
            "description": test_description,
            "definition": {"json": "{}"},
        }
    }

    res_data = execute_graphql(auth_session, create_test_query, create_test_variables)

    assert res_data["data"]["createTest"] is not None

    return res_data["data"]["createTest"]


@pytest.mark.dependency()
def test_get_test_results(auth_session):
    urn = (
        "urn:li:dataset:(urn:li:dataPlatform:kafka,test-tests-sample,PROD)"  # Test urn
    )
    query = """query getDataset($urn: String!) {
            dataset(urn: $urn) {
                urn
                testResults {
                    failing {
                      test {
                        urn
                      }
                      type
                    }
                    passing {
                      test {
                        urn
                      }
                      type
                    }
                }
            }
        }"""
    variables: Dict[str, Any] = {"urn": urn}

    res_data = execute_graphql(auth_session, query, variables)

    assert res_data["data"]["dataset"]
    assert res_data["data"]["dataset"]["urn"] == urn
    assert res_data["data"]["dataset"]["testResults"] == {
        "failing": [{"test": {"urn": "urn:li:test:test-1"}, "type": "FAILURE"}],
        "passing": [{"test": {"urn": "urn:li:test:test-2"}, "type": "SUCCESS"}],
    }


@pytest.mark.dependency(depends=["test_get_test_results"])
def test_create_test(auth_session):
    test_urn = create_test(auth_session)

    # Get the test
    get_test_query = """query test($urn: String!) {
            test(urn: $urn) {
              urn
              name
              category
              description
              definition {
                json
              }
            }
        }"""
    get_test_variables: Dict[str, Any] = {"urn": test_urn}

    res_data = execute_graphql(auth_session, get_test_query, get_test_variables)

    assert res_data["data"]["test"] == {
        "urn": test_urn,
        "name": test_name,
        "category": test_category,
        "description": test_description,
        "definition": {
            "json": "{}",
        },
    }

    # Ensure that soft-deleted tests
    res_data = execute_graphql(auth_session, get_test_query, get_test_variables)

    assert res_data["data"]["test"] is not None


@pytest.mark.dependency(depends=["test_create_test"])
def test_update_test(auth_session):
    test_urn = create_test(auth_session)
    test_name = "new name"
    test_category = "new category"
    test_description = "new description"

    # Update Test
    update_test_query = """mutation updateTest($urn: String!, $input: UpdateTestInput!) {
            updateTest(urn: $urn, input: $input)
        }"""
    update_test_variables: Dict[str, Any] = {
        "urn": test_urn,
        "input": {
            "name": test_name,
            "category": test_category,
            "description": test_description,
            "definition": {"json": "{}"},
        },
    }

    res_data = execute_graphql(auth_session, update_test_query, update_test_variables)

    assert res_data["data"]["updateTest"] is not None

    # Get the test
    get_test_query = """query test($urn: String!) {
            test(urn: $urn) {
              urn
              name
              category
              description
              definition {
                json
              }
            }
        }"""
    get_test_variables: Dict[str, Any] = {"urn": test_urn}

    res_data = execute_graphql(auth_session, get_test_query, get_test_variables)

    assert res_data["data"]["test"] == {
        "urn": test_urn,
        "name": test_name,
        "category": test_category,
        "description": test_description,
        "definition": {
            "json": "{}",
        },
    }


@with_test_retry()
def test_list_tests_retries(auth_session):
    list_tests_query = """query listTests($input: ListTestsInput!) {
          listTests(input: $input) {
            start
            count
            total
            tests {
              urn
            }
          }
      }"""
    list_tests_variables: Dict[str, Any] = {"input": {"start": 0, "count": 20}}

    res_data = execute_graphql(auth_session, list_tests_query, list_tests_variables)

    assert res_data["data"]["listTests"]["total"] >= 2
    assert len(res_data["data"]["listTests"]["tests"]) >= 2


@pytest.mark.dependency(depends=["test_update_test"])
def test_list_tests(auth_session):
    test_list_tests_retries(auth_session)
