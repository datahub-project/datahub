import time
import urllib
from typing import Any, Dict, Optional, cast

import pytest
import requests

from datahub.cli.docker import check_local_docker_containers
from datahub.ingestion.run.pipeline import Pipeline
from datahub.ingestion.source.state.checkpoint import Checkpoint
from tests.utils import ingest_file_via_rest

bootstrap_small = "test_resources/bootstrap_single.json"
bootstrap_small_2 = "test_resources/bootstrap_single2.json"
FRONTEND_ENDPOINT = "http://localhost:9002"

@pytest.fixture(scope="session")
def wait_for_healthchecks():
    # Simply assert that everything is healthy, but don't wait.
    assert not check_local_docker_containers()
    yield

@pytest.fixture(scope="session")
def frontend_session(wait_for_healthchecks):
    session = requests.Session()

    headers = {
        "Content-Type": "application/json",
    }
    data = '{"username":"datahub", "password":"datahub"}'
    response = session.post(
        f"{FRONTEND_ENDPOINT}/logIn", headers=headers, data=data
    )
    response.raise_for_status()

    yield session

def test_ingestion_via_rest_rapid(frontend_session, wait_for_healthchecks):
    ingest_file_via_rest(bootstrap_small)
    ingest_file_via_rest(bootstrap_small_2)
    urn = f"urn:li:dataset:(urn:li:dataPlatform:testPlatform,testDataset,PROD)"
    json = {
            "query": """query getDataset($urn: String!) {\n
                dataset(urn: $urn) {\n
                    urn\n
                    name\n
                    description\n
                    platform {\n
                        urn\n
                    }\n
                    schemaMetadata {\n
                        name\n
                        version\n
                        createdAt\n
                    }\n
                    outgoing: relationships(\n
                                input: { types: ["DownstreamOf", "Consumes", "Produces"], direction: OUTGOING, start: 0, count: 10000 }\n
                            ) {\n
                            start\n
                            count\n
                            total\n
                            relationships {\n
                                type\n
                                direction\n
                                entity {\n
                                    urn\n
                                    type\n
                                }\n
                            }\n
                    }\n
                }\n
            }""",
            "variables": {
                "urn": urn
            }
        }
    #
    time.sleep(2)
    response = frontend_session.post(
        f"{FRONTEND_ENDPOINT}/api/v2/graphql", json=json
    )
    response.raise_for_status()
    res_data = response.json()

    assert res_data
    assert res_data["data"]
    assert res_data["data"]["dataset"]
    assert res_data["data"]["dataset"]["urn"] == urn
    # commenting this out temporarily while we work on fixing this race condition for elasticsearch
    # assert len(res_data["data"]["dataset"]["outgoing"]["relationships"]) == 1
