import logging
from typing import Any, Dict

import pytest

from conftest import _ingest_cleanup_data_impl
from tests.utils import execute_graphql

logger = logging.getLogger(__name__)

TEST_DATASET_1_URN = "urn:li:dataset:(urn:li:dataPlatform:kafka,test-browse-1,PROD)"
TEST_DATASET_2_URN = "urn:li:dataset:(urn:li:dataPlatform:kafka,test-browse-2,PROD)"
TEST_DATASET_3_URN = "urn:li:dataset:(urn:li:dataPlatform:kafka,test-browse-3,PROD)"


@pytest.fixture(scope="module", autouse=False)
def ingest_cleanup_data(auth_session, graph_client):
    yield from _ingest_cleanup_data_impl(
        auth_session, graph_client, "tests/browse/data.json", "browse"
    )


def test_get_browse_paths(auth_session, ingest_cleanup_data):
    # Iterate through each browse path, starting with the root

    query = """query browse($input: BrowseInput!) {\n
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
    variables: Dict[str, Any] = {
        "input": {"type": "DATASET", "path": ["prod"], "start": 0, "count": 100}
    }

    res_data = execute_graphql(auth_session, query, variables)

    browse = res_data["data"]["browse"]
    logger.info(browse)
    assert browse["entities"] == [{"urn": TEST_DATASET_3_URN}]

    # /prod/kafka1
    variables = {
        "input": {
            "type": "DATASET",
            "path": ["prod", "kafka1"],
            "start": 0,
            "count": 10,
        }
    }

    res_data = execute_graphql(auth_session, query, variables)

    browse = res_data["data"]["browse"]
    assert browse == {
        "total": 3,
        "entities": [
            {"urn": TEST_DATASET_1_URN},
            {"urn": TEST_DATASET_2_URN},
            {"urn": TEST_DATASET_3_URN},
        ],
        "groups": [],
        "metadata": {"path": ["prod", "kafka1"], "totalNumEntities": 0},
    }

    # /prod/kafka2
    variables = {
        "input": {
            "type": "DATASET",
            "path": ["prod", "kafka2"],
            "start": 0,
            "count": 10,
        }
    }

    res_data = execute_graphql(auth_session, query, variables)

    browse = res_data["data"]["browse"]
    assert browse == {
        "total": 2,
        "entities": [{"urn": TEST_DATASET_1_URN}, {"urn": TEST_DATASET_2_URN}],
        "groups": [],
        "metadata": {"path": ["prod", "kafka2"], "totalNumEntities": 0},
    }
