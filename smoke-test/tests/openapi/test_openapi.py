import logging
import requests_wrapper as requests
import glob
import json

import pytest
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph

from tests.utils import (
    get_gms_url,
)

logger = logging.getLogger(__name__)


@pytest.mark.dependency()
def test_healthchecks(wait_for_healthchecks):
    # Call to wait_for_healthchecks fixture will do the actual functionality.
    pass


def list_files():
    return glob.glob("tests/openapi/**/*.json")


def load_tests():
    for test_fixture in list_files():
        with open(test_fixture) as f:
            yield (test_fixture, json.load(f))


def execute_request(request):
    session = requests.Session()
    method = request.pop("method")
    url = get_gms_url() + request.pop("url")
    return getattr(session, method)(url, **request)


@pytest.mark.dependency(depends=["test_healthchecks"])
def test_openapi():
    for test_fixture, test_data in load_tests():
        try:
            for req_resp in test_data:
                actual_resp = execute_request(req_resp["request"])
                try:
                    assert actual_resp.status_code == req_resp["response"]["status_code"]
                    assert actual_resp == req_resp["response"]["body"]
                except Exception as e:
                    logger.error(f"Error executing test fixture: {test_fixture}")
                    logger.error(f"Response body: {actual_resp.content}")
                    raise e
        except Exception as e:
            logger.error(f"Error executing test fixture: {test_fixture}")
            raise e


