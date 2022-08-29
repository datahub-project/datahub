import pytest
import tenacity
from tests.utils import delete_urns_from_file, get_frontend_url, ingest_file_via_rest, wait_for_healthcheck_util, get_sleep_info

sleep_sec, sleep_times = get_sleep_info()

@pytest.fixture(scope="module", autouse=True)
def ingest_cleanup_data(request):
    print("ingesting test data")
    ingest_file_via_rest("tests/tests/data.json")
    yield
    print("removing test data")
    delete_urns_from_file("tests/tests/data.json")


@pytest.fixture(scope="session")
def wait_for_healthchecks():
    wait_for_healthcheck_util()
    yield

@pytest.mark.dependency()
def test_healthchecks(wait_for_healthchecks):
    # Call to wait_for_healthchecks fixture will do the actual functionality.
    pass


test_id = "test id"
test_name = "test name"
test_category = "test category"
test_description = "test description"
test_description = "test description"


def create_test(frontend_session):

    # Create new Test
    create_test_json = {
        "query": """mutation createTest($input: CreateTestInput!) {\n
            createTest(input: $input)
        }""",
        "variables": {
            "input": {
                "id": test_id,
                "name": test_name,
                "category": test_category,
                "description": test_description,
                "definition": {"json": "{}"},
            }
        },
    }

    response = frontend_session.post(
        f"{get_frontend_url()}/api/v2/graphql", json=create_test_json
    )
    response.raise_for_status()
    res_data = response.json()

    assert res_data
    assert res_data["data"]
    assert res_data["data"]["createTest"] is not None
    assert "errors" not in res_data

    return res_data["data"]["createTest"]


def delete_test(frontend_session, test_urn):
    delete_test_json = {
        "query": """mutation deleteTest($urn: String!) {\n
            deleteTest(urn: $urn)
        }""",
        "variables": {"urn": test_urn},
    }

    response = frontend_session.post(
        f"{get_frontend_url()}/api/v2/graphql", json=delete_test_json
    )
    response.raise_for_status()


@pytest.mark.dependency(depends=["test_healthchecks"])
def test_create_test(frontend_session, wait_for_healthchecks):

    test_urn = create_test(frontend_session)

    # Get the test
    get_test_json = {
        "query": """query test($urn: String!) {\n
            test(urn: $urn) { \n
              urn\n
              name\n
              category\n
              description\n
              definition {\n
                json\n
              }\n
            }
        }""",
        "variables": {"urn": test_urn},
    }
    response = frontend_session.post(
        f"{get_frontend_url()}/api/v2/graphql", json=get_test_json
    )
    response.raise_for_status()
    res_data = response.json()

    assert res_data
    assert res_data["data"]
    assert res_data["data"]["test"] == {
        "urn": test_urn,
        "name": test_name,
        "category": test_category,
        "description": test_description,
        "definition": {
            "json": "{}",
        },
    }
    assert "errors" not in res_data

    # Delete test
    delete_test(frontend_session, test_urn)

    # Ensure the test no longer exists
    response = frontend_session.post(
        f"{get_frontend_url()}/api/v2/graphql", json=get_test_json
    )
    response.raise_for_status()
    res_data = response.json()

    assert res_data["data"]["test"] is None
    assert "errors" not in res_data


@pytest.mark.dependency(depends=["test_healthchecks", "test_create_test"])
def test_update_test(frontend_session, wait_for_healthchecks):
    test_urn = create_test(frontend_session)
    test_name = "new name"
    test_category = "new category"
    test_description = "new description"
    test_description = "new description"

    # Update Test
    update_test_json = {
        "query": """mutation updateTest($urn: String!, $input: UpdateTestInput!) {\n
            updateTest(urn: $urn, input: $input)
        }""",
        "variables": {
            "urn": test_urn,
            "input": {
                "name": test_name,
                "category": test_category,
                "description": test_description,
                "definition": {"json": "{}"},
            },
        },
    }

    response = frontend_session.post(
        f"{get_frontend_url()}/api/v2/graphql", json=update_test_json
    )
    response.raise_for_status()
    res_data = response.json()

    assert res_data
    assert res_data["data"]
    assert res_data["data"]["updateTest"] is not None
    assert "errors" not in res_data

    # Get the test
    get_test_json = {
        "query": """query test($urn: String!) {\n
            test(urn: $urn) { \n
              urn\n
              name\n
              category\n
              description\n
              definition {\n
                json\n
              }\n
            }
        }""",
        "variables": {"urn": test_urn},
    }
    response = frontend_session.post(
        f"{get_frontend_url()}/api/v2/graphql", json=get_test_json
    )
    response.raise_for_status()
    res_data = response.json()

    assert res_data
    assert res_data["data"]
    assert res_data["data"]["test"] == {
        "urn": test_urn,
        "name": test_name,
        "category": test_category,
        "description": test_description,
        "definition": {
            "json": "{}",
        },
    }
    assert "errors" not in res_data

    delete_test(frontend_session, test_urn)


@tenacity.retry(
    stop=tenacity.stop_after_attempt(sleep_times), wait=tenacity.wait_fixed(sleep_sec)
)
def test_list_tests_retries(frontend_session):
    list_tests_json = {
        "query": """query listTests($input: ListTestsInput!) {\n
          listTests(input: $input) {\n
            start\n
            count\n
            total\n
            tests {\n
              urn\n
            }\n
          }\n
      }""",
        "variables": {"input": {"start": "0", "count": "20"}},
    }

    response = frontend_session.post(
        f"{get_frontend_url()}/api/v2/graphql", json=list_tests_json
    )
    response.raise_for_status()
    res_data = response.json()

    assert res_data
    assert res_data["data"]
    assert res_data["data"]["listTests"]["total"] >= 2
    assert len(res_data["data"]["listTests"]["tests"]) >= 2
    assert "errors" not in res_data


@pytest.mark.dependency(depends=["test_healthchecks", "test_update_test"])
def test_list_tests(frontend_session, wait_for_healthchecks):
    test_list_tests_retries(frontend_session)


@pytest.mark.dependency(depends=["test_healthchecks"])
def test_get_test_results(frontend_session, wait_for_healthchecks):
    urn = (
        "urn:li:dataset:(urn:li:dataPlatform:kafka,test-tests-sample,PROD)"  # Test urn
    )
    json = {
        "query": """query getDataset($urn: String!) {\n
            dataset(urn: $urn) {\n
                urn\n
                testResults {\n
                    failing {\n
                      test {\n
                        urn\n
                      }\n
                      type
                    }\n
                    passing {\n
                      test {\n
                        urn\n
                      }\n
                      type
                    }\n
                }\n
            }\n
        }""",
        "variables": {"urn": urn},
    }
    response = frontend_session.post(f"{get_frontend_url()}/api/v2/graphql", json=json)
    response.raise_for_status()
    res_data = response.json()

    assert res_data
    assert res_data["data"]
    assert res_data["data"]["dataset"]
    assert res_data["data"]["dataset"]["urn"] == urn
    assert res_data["data"]["dataset"]["testResults"] == {
        "failing": [{"test": {"urn": "urn:li:test:test-1"}, "type": "FAILURE"}],
        "passing": [{"test": {"urn": "urn:li:test:test-2"}, "type": "SUCCESS"}],
    }
