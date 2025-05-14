import time
from typing import Any

import pytest

from datahub.emitter.mce_builder import make_dataset_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.schema_classes import StatusClass
from tests.consistency_utils import wait_for_writes_to_sync
from tests.utils import delete_urn

restli_default_headers = {
    "X-RestLi-Protocol-Version": "2.0.0",
}

TEST_DATASET_URN = make_dataset_urn(platform="postgres", name="foo_custom")


@pytest.fixture(scope="module")
def test_data(graph_client):
    mcpw = MetadataChangeProposalWrapper(
        entityUrn=TEST_DATASET_URN, aspect=StatusClass(removed=False)
    )
    graph_client.emit(mcpw)
    yield
    delete_urn(graph_client, TEST_DATASET_URN)


def test_create_update_delete_dataset_custom_assertion(
    test_data: Any, graph_client: DataHubGraph
) -> None:
    # Create custom assertion
    resp = graph_client.upsert_custom_assertion(
        urn=None,
        entity_urn=TEST_DATASET_URN,
        type="My custom category",
        description="Description of custom assertion",
        platform_name="customDQPlatform",
    )

    assert resp.get("urn")
    assertion_urn = resp["urn"]

    # Update custom assertion
    resp = graph_client.upsert_custom_assertion(
        urn=assertion_urn,
        entity_urn=TEST_DATASET_URN,
        type="My custom category",
        description="Updated Description of custom assertion",
        platform_name="customDQPlatform",
        external_url="http://some_url",
    )

    wait_for_writes_to_sync()

    # Report custom assertion result for success
    result_reported = graph_client.report_assertion_result(
        urn=assertion_urn,
        timestamp_millis=0,
        type="SUCCESS",
        external_url="http://some_url/run/1",
    )
    assert result_reported

    # Report custom assertion result for error
    result_reported = graph_client.report_assertion_result(
        urn=assertion_urn,
        timestamp_millis=round(time.time() * 1000),
        type="ERROR",
        external_url="http://some_url/run/2",
        error_type="SOURCE_QUERY_FAILED",
        error_message="Source query failed with error Permission Denied.",
    )
    assert result_reported

    # Report custom assertion result for failure
    result_reported = graph_client.report_assertion_result(
        urn=assertion_urn,
        timestamp_millis=round(time.time() * 1000),
        type="FAILURE",
        external_url="http://some_url/run/3",
    )
    assert result_reported

    wait_for_writes_to_sync()

    graphql_query_retrive_assertion = """
        query dataset($datasetUrn: String!) {
            dataset(urn: $datasetUrn) {
                assertions(start: 0, count: 1000) {
                    start
                    count
                    total
                    assertions {
                        urn
                        # Fetch the last run of each associated assertion. 
                        runEvents(status: COMPLETE, limit: 3) {
                            total
                            failed
                            succeeded
                            runEvents {
                                timestampMillis
                                status
                                result {
                                    type
                                    externalUrl
                                    nativeResults {
                                        key
                                        value
                                    }
                                }
                            }
                        }
                        info {
                            type
                            description
                            externalUrl
                            lastUpdated {
                                time
                                actor
                            }
                            customAssertion {
                                type
                                entityUrn
                                field {
                                    path
                                }
                                logic
                            }
                            source {
                                type
                                created {
                                    time
                                    actor
                                }
                            }
                        }
                    }
                }
            }
        }
    """

    dataset_assertions = graph_client.execute_graphql(
        query=graphql_query_retrive_assertion,
        variables={"datasetUrn": TEST_DATASET_URN},
    )

    assertions = dataset_assertions["dataset"]["assertions"]["assertions"]
    assert assertions
    assert assertions[0]["urn"] == assertion_urn
    assert assertions[0]["info"]
    assert assertions[0]["info"]["type"] == "CUSTOM"
    assert assertions[0]["info"]["externalUrl"] == "http://some_url"
    assert (
        assertions[0]["info"]["description"]
        == "Updated Description of custom assertion"
    )
    assert assertions[0]["info"]["customAssertion"]
    assert assertions[0]["info"]["customAssertion"]["type"] == "My custom category"

    assert assertions[0]["runEvents"]
    assert assertions[0]["runEvents"]["total"] == 3
    assert assertions[0]["runEvents"]["succeeded"] == 1
    assert assertions[0]["runEvents"]["failed"] == 1
    assert assertions[0]["runEvents"]["runEvents"][0]["result"]["externalUrl"]

    graph_client.delete_entity(assertion_urn, True)

    dataset_assertions = graph_client.execute_graphql(
        query=graphql_query_retrive_assertion,
        variables={"datasetUrn": TEST_DATASET_URN},
    )
    assertions = dataset_assertions["dataset"]["assertions"]["assertions"]
    assert not assertions
