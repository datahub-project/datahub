import json
import time
# import urllib
# from typing import Any, Dict, Optional, cast

import pytest
import requests
import os
from jsoncomparison import Compare, NO_DIFF

# from datahub.ingestion.run.pipeline import Pipeline
# from datahub.ingestion.source.sql.mysql import MySQLConfig, MySQLSource
# from datahub.ingestion.source.sql.sql_common import BaseSQLAlchemyCheckpointState
# from datahub.ingestion.source.state.checkpoint import Checkpoint
# from tests.utils import ingest_file_via_rest

GMS_ENDPOINT = "http://localhost:8080"
GOLDEN_FILES_PATH = "./spark-smoke-test/golden_json/"
golden_files = os.listdir(GOLDEN_FILES_PATH)

print(golden_files)
[file_name.strip(".json") for file_name in golden_files]
restli_default_headers = {
    "X-RestLi-Protocol-Version": "2.0.0",
}
kafka_post_ingestion_wait_sec = 60

JSONDIFF_CONFIG = {
    'output': {
        'console': False,
        'file': {
            'allow_nan': True,
            'ensure_ascii': True,
            'indent': 4,
            'name': None,
            'skipkeys': True,
        },
    },
    'types': {
        'float': {
            'allow_round': 2,
        },
        'list': {
            'check_length': False,
        },
    },
}
json_compare = Compare(JSONDIFF_CONFIG)


@pytest.fixture(scope="session")
def wait_for_healthchecks():
    # os.system('docker run --network datahub_network spark-submit')
    yield


@pytest.mark.dependency()
def test_healthchecks(wait_for_healthchecks):
    # Call to wait_for_healthchecks fixture will do the actual functionality.
    pass


def sort_aspects(input):
    print(input)
    item_id = list(input["value"].keys())[0]
    input["value"][item_id]["aspects"] = sorted(
        input["value"][item_id]["aspects"], key=lambda x: list(x.keys())[0]
    )


@pytest.mark.dependency(depends=["test_healthchecks"])
@pytest.mark.parametrize("json_file", golden_files, )
def test_ingestion_via_rest(json_file):
    print(json_file)
    # Opening JSON file
    f = open(os.path.join(GOLDEN_FILES_PATH, json_file))
    golden_data = json.load(f)
    for urn, value in golden_data.items():
        url = GMS_ENDPOINT + "/entities/" + urn
        print(url)
        response = requests.get(url)
        response.raise_for_status()

        data = sort_aspects(response.json())
        value = sort_aspects(value)
        diff = json_compare.check(value, data)
        print(urn)
        if diff != NO_DIFF:
            print("Expected: {} Actual: {}".format(value, data))
        assert diff == NO_DIFF
