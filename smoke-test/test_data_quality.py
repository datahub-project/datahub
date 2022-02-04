import json
import urllib
import time
import pytest
import requests
from datahub.cli.docker import check_local_docker_containers
from tests.utils import ingest_file_via_rest

bootstrap_sample_data = "test_resources/bootstrap_data_quality.json"
GMS_ENDPOINT = "http://localhost:8080"

restli_default_headers = {
    "X-RestLi-Protocol-Version": "2.0.0",
}


@pytest.fixture(scope="session")
def wait_for_healthchecks():
    # Simply assert that everything is healthy, but don't wait.
    assert not check_local_docker_containers()
    yield


@pytest.mark.dependency()
def test_healthchecks(wait_for_healthchecks):
    # Call to wait_for_healthchecks fixture will do the actual functionality.
    pass


@pytest.mark.dependency(depends=["test_healthchecks"])
def test_run_ingestion(wait_for_healthchecks):
    ingest_file_via_rest(bootstrap_sample_data)


@pytest.mark.dependency(depends=["test_healthchecks", "test_run_ingestion"])
def test_gms_get_latest_assertions_results_by_partition():
    urn = "urn:li:dataset:(urn:li:dataPlatform:postgres,fooTable,PROD)"

    # sleep for elasticsearch indices to be updated
    time.sleep(5)

    # Query
    # Given the dataset
    # show me latest assertion results grouped-by date, partition, assertionId
    query = json.dumps(
        {
            "entityName": "dataset",
            "aspectName": "assertionResult",
            "filter": {
                "or": [
                    {
                        "and": [
                            {
                                "field": "urn",
                                "value": urn,
                                "condition": "EQUAL",
                            }
                        ]
                    }
                ]
            },
            "metrics": [
                {"fieldPath": "batchAssertionResult", "aggregationType": "LATEST"}
            ],
            "buckets": [
                {"key": "asserteeUrn", "type": "STRING_GROUPING_BUCKET"},
                {"key": "partitionSpec.partition", "type": "STRING_GROUPING_BUCKET"},
                {
                    "key": "timestampMillis",
                    "type": "DATE_GROUPING_BUCKET",
                    "timeWindowSize": {"multiple": 1, "unit": "DAY"},
                },
                {"key": "assertionUrn", "type": "STRING_GROUPING_BUCKET"},
            ],
        }
    )
    response = requests.post(
        f"{GMS_ENDPOINT}/analytics?action=getTimeseriesStats",
        data=query,
        headers=restli_default_headers,
    )

    response.raise_for_status()
    data = response.json()

    assert data["value"]
    assert data["value"]["table"]
    assert sorted(data["value"]["table"]["columnNames"]) == [
        "asserteeUrn",
        "assertionUrn",
        "latest_batchAssertionResult",
        "partitionSpec.partition",
        "timestampMillis",
    ]
    assert len(data["value"]["table"]["rows"]) == 6
    assert (
        data["value"]["table"]["rows"][0][
            data["value"]["table"]["columnNames"].index("asserteeUrn")
        ]
        == urn
    )


@pytest.mark.dependency(depends=["test_healthchecks", "test_run_ingestion"])
def test_gms_get_assertions_on_dataset():
    """lists all assertion urns including those which may not have executed"""
    urn = "urn:li:dataset:(urn:li:dataPlatform:postgres,fooTable,PROD)"
    response = requests.get(
        f"{GMS_ENDPOINT}/relationships?direction=INCOMING&urn={urllib.parse.quote(urn)}&types=Asserts"
    )

    response.raise_for_status()
    data = response.json()
    assert len(data["relationships"]) == 1


@pytest.mark.dependency(depends=["test_healthchecks", "test_run_ingestion"])
def test_gms_get_assertions_on_dataset_field():
    """lists all assertion urns including those which may not have executed"""
    urn = "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:postgres,fooTable,PROD), col1)"
    response = requests.get(
        f"{GMS_ENDPOINT}/relationships?direction=INCOMING&urn={urllib.parse.quote(urn)}&types=Asserts"
    )

    response.raise_for_status()
    data = response.json()
    assert len(data["relationships"]) == 1


@pytest.mark.dependency(depends=["test_healthchecks", "test_run_ingestion"])
def test_gms_get_assertion_info():
    assertion_urn = "urn:li:assertion:2d3b06a6e77e1f24adc9860a05ea089b"
    response = requests.get(
        f"{GMS_ENDPOINT}/aspects/{urllib.parse.quote(assertion_urn)}\
            ?aspect=assertionInfo&version=0",
        headers=restli_default_headers,
    )

    response.raise_for_status()
    data = response.json()

    assert data["aspect"]
    assert data["aspect"]["com.linkedin.assertion.AssertionInfo"]
    assert data["aspect"]["com.linkedin.assertion.AssertionInfo"]["assertionType"]
