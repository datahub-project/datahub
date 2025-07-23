import logging
import time

import pytest

logger = logging.getLogger(__name__)


def get_test_connection_result(graph_client, exec_request) -> bool:
    logger.info(f"TestConnectionRequest:{exec_request} is pending...")

    query = """
      query getIngestionExecutionRequest($urn: String!) {
        executionRequest(urn: $urn) {
          result {
            status
            structuredReport {
              type
              serializedValue
            }
          }
        }
      }
      """
    res = graph_client.execute_graphql(query, variables={"urn": exec_request})

    result = res.get("executionRequest", {}).get("result", {})
    if result is None:
        return False

    status = result.get("status", "")
    report = result.get("structuredReport", {})

    if report is None:
        return False

    report_type = report.get("type", "")
    success = status == "SUCCESS" and report_type == "TEST_CONNECTION"
    if success:
        logger.info(
            f"TestConnectionRequest succeeded; id = {exec_request}; status = {status};"
        )
    return success


def submit_test_connection_request(graph_client) -> str:
    query = """
      mutation createTestConnectionRequest($input: CreateTestConnectionRequestInput!) {
        createTestConnectionRequest(input: $input)
      }
    """

    input = {
        "version": None,
        "recipe": """\
        {\
          "source": {\
            "type": "bigquery",\
            "config": {\
              "credential": {\
                "private_key": "smoke-test-key",\
                "private_key_id": "smoke-test-key-id",\
                "client_email": "dh-bigquery-smoke-test@acryl-staging.iam.gserviceaccount.com",\
                "client_id": "1234567890",\
                "project_id": "acryl-test"\
              }\
            }\
          }\
        }""",
    }
    res = graph_client.execute_graphql(query, variables={"input": input})
    exec_request = res.get("createTestConnectionRequest")

    logger.info(f"Submitted TestConnectionRequest; id = {exec_request}")
    return exec_request


def wait_for_test_connection_success(graph_client, exec_request, timeout=30) -> bool:
    start_time = time.time()
    while True:
        res = get_test_connection_result(graph_client, exec_request)
        if res:
            return True
        if time.time() > (start_time + timeout):
            return False
        time.sleep(1.0)


@pytest.mark.remote_executor
def test_test_connection(auth_session, graph_client):
    # Remote Executor support a special type of task to verify datasource connectivity without
    # executing actual ingestion. This test validates that code path without executing actual
    # connectivity test.
    result = False
    for _ in range(3):
        exec_request = submit_test_connection_request(graph_client)
        result = wait_for_test_connection_success(graph_client, exec_request)
        if result:
            break
    assert result
