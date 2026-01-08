"""Helper functions and constants for SDK assertion tests."""

import uuid
from typing import Any, Optional

from datahub.ingestion.graph.client import DataHubGraph
from tests.consistency_utils import wait_for_writes_to_sync


def wait_for_assertion_sync() -> None:
    """Faster wait suitable for single-entity assertion operations.

    Uses a 30-second timeout instead of the default 120 seconds,
    which is sufficient for assertion create/update/delete operations.
    """
    wait_for_writes_to_sync(max_timeout_in_sec=30)


def generate_unique_test_id() -> str:
    """Generate a unique identifier for test isolation.

    Returns a short UUID string suitable for use in display names and URNs.
    """
    return str(uuid.uuid4())[:8]


def get_nested_value(data: dict, path: str, default: Any = None) -> Any:
    """Get a value from a nested dictionary using dot notation.

    Args:
        data: The dictionary to search
        path: Dot-separated path (e.g., "info.source.type")
        default: Default value if path not found

    Returns:
        The value at the path, or default if not found

    Examples:
        >>> get_nested_value({"info": {"source": {"type": "NATIVE"}}}, "info.source.type")
        "NATIVE"
        >>> get_nested_value({"info": {}}, "info.source.type", None)
        None
    """
    keys = path.split(".")
    current = data
    for key in keys:
        if not isinstance(current, dict) or key not in current:
            return default
        current = current[key]
    return current


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
                        source {
                            type
                        }
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
                                parameters {
                                    value {
                                        value
                                        type
                                    }
                                    minValue {
                                        value
                                        type
                                    }
                                    maxValue {
                                        value
                                        type
                                    }
                                }
                            }
                            rowCountChange {
                                type
                                operator
                                parameters {
                                    value {
                                        value
                                        type
                                    }
                                }
                            }
                        }
                        sqlAssertion {
                            entityUrn
                            statement
                            operator
                            parameters {
                                value {
                                    value
                                    type
                                }
                                minValue {
                                    value
                                    type
                                }
                                maxValue {
                                    value
                                    type
                                }
                            }
                        }
                        schemaAssertion {
                            entityUrn
                            compatibility
                            fields {
                                path
                                type
                                nativeType
                            }
                        }
                        fieldAssertion {
                            entityUrn
                            type
                            fieldValuesAssertion {
                                field {
                                    path
                                    type
                                    nativeType
                                }
                                operator
                                transform {
                                    type
                                }
                                parameters {
                                    value {
                                        value
                                        type
                                    }
                                    minValue {
                                        value
                                        type
                                    }
                                    maxValue {
                                        value
                                        type
                                    }
                                }
                                failThreshold {
                                    type
                                    value
                                }
                                excludeNulls
                            }
                            fieldMetricAssertion {
                                field {
                                    path
                                    type
                                    nativeType
                                }
                                metric
                                operator
                                parameters {
                                    value {
                                        value
                                        type
                                    }
                                    minValue {
                                        value
                                        type
                                    }
                                    maxValue {
                                        value
                                        type
                                    }
                                }
                            }
                        }
                    }
                    tags {
                        tags {
                            tag {
                                urn
                                properties {
                                    name
                                }
                            }
                        }
                    }
                    actions {
                        onSuccess {
                            type
                        }
                        onFailure {
                            type
                        }
                    }
                    monitor {
                        urn
                        info {
                            status {
                                mode
                            }
                            assertionMonitor {
                                assertions {
                                    schedule {
                                        cron
                                        timezone
                                    }
                                    parameters {
                                        type
                                        datasetFreshnessParameters {
                                            sourceType
                                            field {
                                                path
                                                type
                                                nativeType
                                                kind
                                            }
                                        }
                                        datasetVolumeParameters {
                                            sourceType
                                        }
                                        datasetFieldParameters {
                                            sourceType
                                            changedRowsField {
                                                path
                                                type
                                                nativeType
                                            }
                                        }
                                    }
                                }
                                settings {
                                    inferenceSettings {
                                        sensitivity {
                                            level
                                        }
                                        trainingDataLookbackWindowDays
                                        exclusionWindows {
                                            type
                                            displayName
                                        }
                                    }
                                }
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
    """Helper to clean up an assertion after test.

    Uses a shorter 10-second timeout since cleanup doesn't need
    to verify the deletion is visible in search indexes.
    """
    graph_client.delete_entity(assertion_urn, hard=True)
    wait_for_writes_to_sync(max_timeout_in_sec=10)
