# type: ignore

from unittest.mock import Mock

from datahub.ingestion.graph.client import DataHubGraph

from datahub_monitors.constants import LIST_MONITORS_BATCH_SIZE
from datahub_monitors.fetcher.fetcher import MonitorFetcher
from datahub_monitors.graphql.query import GRAPHQL_LIST_MONITORS_QUERY
from datahub_monitors.types import (
    AssertionEvaluationParametersType,
    AssertionType,
    DatasetFreshnessSourceType,
    FreshnessAssertionScheduleType,
    FreshnessAssertionType,
    MonitorType,
)


def test_fetch_assertions() -> None:
    # Create a mock DataHubGraph object
    graph = Mock(spec=DataHubGraph)

    # Configure the mock object to return a specific result when its execute_graphql method is called
    graph.execute_graphql.return_value = {
        "searchAcrossEntities": {
            "searchResults": [
                {
                    "entity": {
                        "urn": "urn:li:monitor:test",
                        "type": "MONITOR",
                        "info": {
                            "type": "ASSERTION",
                            "assertionMonitor": {
                                "assertions": [
                                    {
                                        "assertion": {
                                            "urn": "urn:li:assertion:test",
                                            "info": {
                                                "type": "FRESHNESS",
                                                "freshnessAssertion": {
                                                    "type": "DATASET_CHANGE",
                                                    "schedule": {
                                                        "type": "CRON",
                                                        "cron": {
                                                            "cron": "0 * * * *",
                                                            "timezone": "America/Los_Angeles",
                                                        },
                                                    },
                                                },
                                                "source": {"type": "NATIVE"},
                                            },
                                            "relationships": {
                                                "relationships": [
                                                    {
                                                        "entity": {
                                                            "urn": "urn:li:dataset:test",
                                                            "type": "DATASET",
                                                            "platform": {
                                                                "urn": "urn:li:dataPlatform:snowflake"
                                                            },
                                                        }
                                                    }
                                                ]
                                            },
                                        },
                                        "schedule": {
                                            "cron": "0 * * * *",
                                            "timezone": "America/Los_Angeles",
                                        },
                                        "parameters": {
                                            "type": "DATASET_FRESHNESS",
                                            "datasetFreshnessParameters": {
                                                "sourceType": "INFORMATION_SCHEMA"
                                            },
                                        },
                                    }
                                ]
                            },
                        },
                    }
                }
            ]
        },
        "error": None,
    }

    graph.execute_graphql.side_effect = (
        lambda *args, **kwargs: graph.execute_graphql.return_value
    )

    # Create the MonitorFetcher with the mock graph
    fetcher = MonitorFetcher(graph)

    # Call the fetch_monitors method
    monitors = fetcher.fetch_monitors()

    # Verify that the fetch_assertions method returns the expected result
    assert len(monitors) == 1
    assert monitors[0].type == MonitorType.ASSERTION
    assert (
        monitors[0].assertion_monitor.assertions[0].assertion.type
        == AssertionType.FRESHNESS
    )
    assert (
        monitors[0].assertion_monitor.assertions[0].assertion.freshness_assertion.type
        == FreshnessAssertionType.DATASET_CHANGE
    )
    assert (
        monitors[0]
        .assertion_monitor.assertions[0]
        .assertion.freshness_assertion.schedule.type
        == FreshnessAssertionScheduleType.CRON
    )
    assert (
        monitors[0]
        .assertion_monitor.assertions[0]
        .assertion.freshness_assertion.schedule.cron.cron
        == "0 * * * *"
    )
    assert (
        monitors[0]
        .assertion_monitor.assertions[0]
        .assertion.freshness_assertion.schedule.cron.timezone
        == "America/Los_Angeles"
    )
    assert monitors[0].assertion_monitor.assertions[0].schedule.cron == "0 * * * *"
    assert (
        monitors[0].assertion_monitor.assertions[0].schedule.timezone
        == "America/Los_Angeles"
    )
    assert (
        monitors[0].assertion_monitor.assertions[0].parameters.type
        == AssertionEvaluationParametersType.DATASET_FRESHNESS
    )
    assert (
        monitors[0]
        .assertion_monitor.assertions[0]
        .parameters.dataset_freshness_parameters.source_type
        == DatasetFreshnessSourceType.INFORMATION_SCHEMA
    )

    # Verify that the execute_graphql method was called
    graph.execute_graphql.assert_called_with(
        GRAPHQL_LIST_MONITORS_QUERY,
        variables={
            "input": {
                "types": ["MONITOR"],
                "start": 0,
                "count": LIST_MONITORS_BATCH_SIZE,
                "query": "*",
                "searchFlags": {"skipCache": True},
            }
        },
    )
