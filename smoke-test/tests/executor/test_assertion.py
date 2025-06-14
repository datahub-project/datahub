import os
import time

import pytest
import requests
from acryl.executor.request.execution_request import ExecutionRequest

from datahub.metadata.schema_classes import AssertionRunSummaryClass
from tests.utils import delete_urns_from_file, ingest_file_via_rest


@pytest.fixture(scope="module", autouse=False)
def ingest_cleanup_data(auth_session, graph_client, request):
    print("ingesting test data")
    ingest_file_via_rest(auth_session, "tests/executor/assertions-data.json")
    yield
    print("removing test data")
    delete_urns_from_file(graph_client, "tests/executor/assertions-data.json")


def wait_for_assertion_success(graph_client, timeout=30) -> bool:
    start_time = time.time()
    while True:
        res = get_assertion_result(
            graph_client, "urn:li:assertion:d7a1f48e-c17d-4ca8-b525-14d071d7f59a"
        )
        if res:
            return True

        if time.time() > (start_time + timeout):
            return False
        time.sleep(1.0)


def get_assertion_result(graph_client, urn) -> bool:
    result = graph_client.get_aspect(urn, AssertionRunSummaryClass)
    if result is not None and result.lastPassedAtMillis is not None:
        now = int(time.time() * 1000)
        return (now - int(result.lastPassedAtMillis)) < 120000
    return False


def validate_assertion(graph, executor_id) -> None:
    result = False
    for _ in range(3):
        report_operation(graph)
        time.sleep(2.0)
        submit_assertion_request(executor_id)
        result = wait_for_assertion_success(graph)
        if result:
            break
    assert result


def report_operation(graph_client) -> None:
    query = """
        mutation reportOperation {
            reportOperation(
                input: {
                    urn: "urn:li:dataset:(urn:li:dataPlatform:hdfs,SampleHdfsDataset,PROD)"
                    operationType: INSERT
                    sourceType: DATA_PROCESS
                }
            )
        }
    """
    graph_client.execute_graphql(query)


def submit_assertion_request(executor_id: str = "default") -> int:
    execution_request = ExecutionRequest(
        urn="urn:li:monitor:(urn:li:dataset:(urn:li:dataPlatform:hdfs,SampleHdfsDataset,PROD),0bd7215b-d7d7-4408-9f72-88d62f53b630)",
        executor_id=executor_id,
        exec_id="urn:li:monitor:(urn:li:dataset:(urn:li:dataPlatform:hdfs,SampleHdfsDataset,PROD),0bd7215b-d7d7-4408-9f72-88d62f53b630)_scheduled_training",
        name="RUN_ASSERTION",
        args={
            "urn": "urn:li:monitor:(urn:li:dataset:(urn:li:dataPlatform:hdfs,SampleHdfsDataset,PROD),0bd7215b-d7d7-4408-9f72-88d62f53b630)",
            "assertion_spec": {
                "assertion": {
                    "urn": "urn:li:assertion:d7a1f48e-c17d-4ca8-b525-14d071d7f59a",
                    "connectionUrn": "urn:li:dataPlatform:hdfs",
                    "type": "FRESHNESS",
                    "entity": {
                        "urn": "urn:li:dataset:(urn:li:dataPlatform:hdfs,SampleHdfsDataset,PROD)",
                        "platformUrn": "urn:li:dataPlatform:hdfs",
                        "platformInstance": None,
                        "subTypes": [],
                        "table_name": None,
                        "qualifiedName": None,
                        "exists": True,
                    },
                    "sourceType": "NATIVE",
                    "freshnessAssertion": {
                        "type": "DATASET_CHANGE",
                        "schedule": {
                            "type": "FIXED_INTERVAL",
                            "cron": None,
                            "fixedInterval": {"unit": "HOUR", "multiple": 1},
                            "exclusions": None,
                        },
                        "filter": None,
                    },
                    "volumeAssertion": None,
                    "sqlAssertion": None,
                    "fieldAssertion": None,
                    "schemaAssertion": None,
                    "raw_info_aspect": {
                        "aspectName": "assertionInfo",
                        "payload": '{"lastUpdated":{"actor":"urn:li:corpuser:admin","time":1752256444362},"source":{"type":"NATIVE","created":{"actor":"urn:li:corpuser:admin","time":1752240867854}},"type":"FRESHNESS","freshnessAssertion":{"type":"DATASET_CHANGE","schedule":{"type":"FIXED_INTERVAL","fixedInterval":{"multiple":1,"unit":"HOUR"}},"entity":"urn:li:dataset:(urn:li:dataPlatform:hdfs,SampleHdfsDataset,PROD)"}}',
                    },
                },
                "schedule": {"cron": "0 */6 * * *", "timezone": "America/New_York"},
                "parameters": {
                    "type": "DATASET_FRESHNESS",
                    "datasetFreshnessParameters": {
                        "sourceType": "DATAHUB_OPERATION",
                        "field": None,
                        "auditLog": None,
                        "dataHubOperation": None,
                    },
                    "datasetVolumeParameters": None,
                    "datasetFieldParameters": None,
                    "datasetSchemaParameters": None,
                },
            },
            "context": {
                "dry_run": False,
                "online_smart_assertions": False,
                "monitor_urn": "urn:li:monitor:(urn:li:dataset:(urn:li:dataPlatform:hdfs,SampleHdfsDataset,PROD),0bd7215b-d7d7-4408-9f72-88d62f53b630)",
            },
        },
    )
    result = requests.post(
        "http://localhost:9004/scheduler/execute_async", data=execution_request.json()
    )
    return result.status_code


@pytest.mark.remote_executor
def test_assertion(auth_session, graph_client, ingest_cleanup_data):
    # Test embedded mode
    validate_assertion(graph_client, "default")

    # Test remote mode
    executor_id = os.environ.get("DATAHUB_SMOKETEST_EXECUTOR_ID", "remote-ci")
    validate_assertion(graph_client, executor_id)
