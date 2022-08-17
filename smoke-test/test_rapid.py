import pytest
import requests
import tenacity

from tests.utils import (
    get_frontend_url,
    ingest_file_via_rest,
    wait_for_healthcheck_util,
    get_sleep_info,
    get_frontend_session,
)

sleep_sec, sleep_times = get_sleep_info()

bootstrap_small = "test_resources/bootstrap_single.json"
bootstrap_small_2 = "test_resources/bootstrap_single2.json"


@pytest.fixture(scope="session")
def wait_for_healthchecks():
    wait_for_healthcheck_util()
    yield


@pytest.fixture(scope="session")
def frontend_session(wait_for_healthchecks):
    yield get_frontend_session()


@tenacity.retry(
    stop=tenacity.stop_after_attempt(sleep_times), wait=tenacity.wait_fixed(sleep_sec)
)
def _ensure_dataset_present_correctly(frontend_session):
    urn = "urn:li:dataset:(urn:li:dataPlatform:testPlatform,testDataset,PROD)"
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
        "variables": {"urn": urn},
    }
    response = frontend_session.post(f"{get_frontend_url()}/api/v2/graphql", json=json)
    response.raise_for_status()
    res_data = response.json()

    assert res_data
    assert res_data["data"]
    assert res_data["data"]["dataset"]
    assert res_data["data"]["dataset"]["urn"] == urn


def test_ingestion_via_rest_rapid(frontend_session, wait_for_healthchecks):
    ingest_file_via_rest(bootstrap_small)
    ingest_file_via_rest(bootstrap_small_2)
    _ensure_dataset_present_correctly(frontend_session)
