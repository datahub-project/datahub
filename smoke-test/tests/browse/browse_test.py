import time

import pytest
import requests_wrapper as requests
from tests.utils import delete_urns_from_file, get_frontend_url, ingest_file_via_rest


TEST_DATASET_1_URN = "urn:li:dataset:(urn:li:dataPlatform:kafka,test-browse-1,PROD)"
TEST_DATASET_2_URN = "urn:li:dataset:(urn:li:dataPlatform:kafka,test-browse-2,PROD)"
TEST_DATASET_3_URN = "urn:li:dataset:(urn:li:dataPlatform:kafka,test-browse-3,PROD)"


@pytest.fixture(scope="module", autouse=False)
def ingest_cleanup_data(request):
    print("ingesting browse test data")
    ingest_file_via_rest("tests/browse/data.json")

    yield
    print("removing browse test data")
    delete_urns_from_file("tests/browse/data.json")


@pytest.mark.dependency()
def test_healthchecks(wait_for_healthchecks):
    # Call to wait_for_healthchecks fixture will do the actual functionality.
    pass


@pytest.mark.dependency(depends=["test_healthchecks"])
def test_get_browse_paths(frontend_session, ingest_cleanup_data):

    # Iterate through each browse path, starting with the root

    get_browse_paths_query = """query browse($input: BrowseInput!) {\n
                                 browse(input: $input) {\n
                                   total\n
                                   entities {\n
                                     urn\n
                                   }\n
                                   groups {\n
                                     name\n
                                     count\n
                                   }\n
                                   metadata {\n
                                     path\n
                                     totalNumEntities\n
                                   }\n
                                 }\n
                               }"""

    # /prod -- There should be one entity
    get_browse_paths_json = {
        "query": get_browse_paths_query,
        "variables": {"input": { "type": "DATASET", "path": ["prod"], "start": 0, "count": 100 } },
    }

    response = frontend_session.post(
        f"{get_frontend_url()}/api/v2/graphql", json=get_browse_paths_json
    )
    response.raise_for_status()
    res_data = response.json()

    assert res_data
    assert res_data["data"]
    assert res_data["data"]["browse"] is not None
    assert "errors" not in res_data

    browse = res_data["data"]["browse"]
    print(browse)
    assert browse["entities"] == [{ "urn": TEST_DATASET_3_URN }]

    # /prod/kafka1
    get_browse_paths_json = {
        "query": get_browse_paths_query,
        "variables": {"input": { "type": "DATASET", "path": ["prod", "kafka1"], "start": 0, "count": 10 } },
    }

    response = frontend_session.post(
        f"{get_frontend_url()}/api/v2/graphql", json=get_browse_paths_json
    )
    response.raise_for_status()
    res_data = response.json()

    assert res_data
    assert res_data["data"]
    assert res_data["data"]["browse"] is not None
    assert "errors" not in res_data

    browse = res_data["data"]["browse"]
    assert browse == {
      "total": 3,
      "entities": [{ "urn": TEST_DATASET_1_URN }, { "urn": TEST_DATASET_2_URN }, { "urn": TEST_DATASET_3_URN }],
      "groups": [],
      "metadata": { "path": ["prod", "kafka1"], "totalNumEntities": 0 }
    }

    # /prod/kafka2
    get_browse_paths_json = {
        "query": get_browse_paths_query,
        "variables": {"input": { "type": "DATASET", "path": ["prod", "kafka2"], "start": 0, "count": 10 } },
    }

    response = frontend_session.post(
        f"{get_frontend_url()}/api/v2/graphql", json=get_browse_paths_json
    )
    response.raise_for_status()
    res_data = response.json()

    assert res_data
    assert res_data["data"]
    assert res_data["data"]["browse"] is not None
    assert "errors" not in res_data

    browse = res_data["data"]["browse"]
    assert browse == {
      "total": 2,
      "entities": [{ "urn": TEST_DATASET_1_URN }, { "urn": TEST_DATASET_2_URN }],
      "groups": [],
      "metadata": { "path": ["prod", "kafka2"], "totalNumEntities": 0 }
    }


