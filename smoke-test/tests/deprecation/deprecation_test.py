import pytest
import time
from tests.utils import FRONTEND_ENDPOINT
from tests.utils import GMS_ENDPOINT
from tests.utils import ingest_file_via_rest
from tests.utils import delete_urns_from_file

@pytest.fixture(scope="module", autouse=True)
def ingest_cleanup_data(request):
    print("ingesting deprecation test data")
    ingest_file_via_rest("tests/deprecation/data.json")
    yield
    print("removing deprecation test data")
    delete_urns_from_file("tests/deprecation/data.json")

@pytest.mark.dependency()
def test_healthchecks(wait_for_healthchecks):
    # Call to wait_for_healthchecks fixture will do the actual functionality.
    pass

@pytest.mark.dependency(depends=["test_healthchecks"])
def test_update_deprecation_all_fields(frontend_session):
    dataset_urn = "urn:li:dataset:(urn:li:dataPlatform:kafka,test-tags-terms-sample-kafka,PROD)"

    dataset_json = {
        "query": """query getDataset($urn: String!) {\n
            dataset(urn: $urn) {\n
                deprecation {\n
                    deprecated\n
                    decommissionTime\n
                    note\n
                    actor\n
                }\n
            }\n
        }""",
        "variables": {
            "urn": dataset_urn
        }
    }

    # Fetch tags
    response = frontend_session.post(
        f"{FRONTEND_ENDPOINT}/api/v2/graphql", json=dataset_json
    )
    response.raise_for_status()
    res_data = response.json()

    assert res_data
    assert res_data["data"]
    assert res_data["data"]["dataset"]
    assert res_data["data"]["dataset"]["deprecation"] is None

    update_deprecation_json = {
        "query": """mutation updateDeprecation($input: UpdateDeprecationInput!) {\n
            updateDeprecation(input: $input)
        }""",
        "variables": {
            "input": {
              "urn": dataset_urn,
              "deprecated": True,
              "note": "My test note",
              "decommissionTime": 0
            }
        }
    }

    response = frontend_session.post(
        f"{FRONTEND_ENDPOINT}/api/v2/graphql", json=update_deprecation_json
    )
    response.raise_for_status()
    res_data = response.json()

    assert res_data
    assert res_data["data"]
    assert res_data["data"]["updateDeprecation"] is True

    # Refetch the dataset
    response = frontend_session.post(
        f"{FRONTEND_ENDPOINT}/api/v2/graphql", json=dataset_json
    )
    response.raise_for_status()
    res_data = response.json()

    assert res_data
    assert res_data["data"]
    assert res_data["data"]["dataset"]
    assert res_data["data"]["dataset"]["deprecation"] == {
      'deprecated': True,
      'decommissionTime': 0,
      'note': 'My test note',
      'actor': 'urn:li:corpuser:datahub'
    }

@pytest.mark.dependency(depends=["test_healthchecks", "test_update_deprecation_all_fields"])
def test_update_deprecation_partial_fields(frontend_session, ingest_cleanup_data):
    dataset_urn = "urn:li:dataset:(urn:li:dataPlatform:kafka,test-tags-terms-sample-kafka,PROD)"

    update_deprecation_json = {
        "query": """mutation updateDeprecation($input: UpdateDeprecationInput!) {\n
            updateDeprecation(input: $input)
        }""",
        "variables": {
            "input": {
              "urn": dataset_urn,
              "deprecated": False
            }
        }
    }

    response = frontend_session.post(
        f"{FRONTEND_ENDPOINT}/api/v2/graphql", json=update_deprecation_json
    )
    response.raise_for_status()
    res_data = response.json()

    assert res_data
    assert res_data["data"]
    assert res_data["data"]["updateDeprecation"] is True

    # Refetch the dataset
    dataset_json = {
        "query": """query getDataset($urn: String!) {\n
            dataset(urn: $urn) {\n
                deprecation {\n
                    deprecated\n
                    decommissionTime\n
                    note\n
                    actor\n
                }\n
            }\n
        }""",
        "variables": {
            "urn": dataset_urn
        }
    }

    response = frontend_session.post(
        f"{FRONTEND_ENDPOINT}/api/v2/graphql", json=dataset_json
    )
    response.raise_for_status()
    res_data = response.json()

    assert res_data
    assert res_data["data"]
    assert res_data["data"]["dataset"]
    assert res_data["data"]["dataset"]["deprecation"] == {
      'deprecated': False,
      'note': '',
      'actor': 'urn:li:corpuser:datahub',
      'decommissionTime': None
    }