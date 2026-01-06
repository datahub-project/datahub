"""Helper functions and constants for SDK assertion tests."""

from typing import Optional

from datahub.ingestion.graph.client import DataHubGraph
from tests.consistency_utils import wait_for_writes_to_sync

# GraphQL query to retrieve assertions for a dataset
GRAPHQL_GET_ASSERTIONS = """
    query dataset($datasetUrn: String!) {
        dataset(urn: $datasetUrn) {
            assertions(start: 0, count: 1000) {
                total
                assertions {
                    urn
                    info {
                        type
                        description
                        externalUrl
                        freshnessAssertion {
                            entityUrn
                            type
                            schedule {
                                type
                                cron {
                                    cron
                                    timezone
                                }
                                fixedInterval {
                                    unit
                                    multiple
                                }
                            }
                        }
                        volumeAssertion {
                            entityUrn
                            type
                            rowCountTotal {
                                operator
                            }
                        }
                        sqlAssertion {
                            entityUrn
                            statement
                            operator
                        }
                        schemaAssertion {
                            entityUrn
                            compatibility
                            fields {
                                path
                                type
                            }
                        }
                        fieldAssertion {
                            entityUrn
                            type
                            fieldValuesAssertion {
                                field {
                                    path
                                }
                                operator
                            }
                            fieldMetricAssertion {
                                field {
                                    path
                                }
                                metric
                                operator
                            }
                        }
                    }
                }
            }
        }
    }
"""


def get_assertion_by_urn(
    graph_client: DataHubGraph, dataset_urn: str, assertion_urn: str
) -> Optional[dict]:
    """Helper to fetch a specific assertion from a dataset."""
    result = graph_client.execute_graphql(
        query=GRAPHQL_GET_ASSERTIONS,
        variables={"datasetUrn": dataset_urn},
    )
    assertions = result["dataset"]["assertions"]["assertions"]
    for assertion in assertions:
        if assertion["urn"] == assertion_urn:
            return assertion
    return None


def cleanup_assertion(graph_client: DataHubGraph, assertion_urn: str) -> None:
    """Helper to clean up an assertion after test."""
    graph_client.delete_entity(assertion_urn, hard=True)
    wait_for_writes_to_sync()
