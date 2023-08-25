# type: ignore

from unittest.mock import Mock

from datahub.ingestion.graph.client import DataHubGraph

from datahub_monitors.constants import LIST_MONITORS_BATCH_SIZE
from datahub_monitors.fetcher.fetcher import MonitorFetcher
from datahub_monitors.fetcher.types import MonitorFetcherConfig, MonitorFetcherMode
from datahub_monitors.graphql.query import GRAPHQL_LIST_MONITORS_QUERY
from datahub_monitors.types import (
    AssertionEvaluationParametersType,
    AssertionStdOperator,
    AssertionStdParameterType,
    AssertionType,
    DatasetFreshnessSourceType,
    DatasetVolumeSourceType,
    FreshnessAssertionScheduleType,
    FreshnessAssertionType,
    MonitorType,
    VolumeAssertionType,
)


def test_fetch_freshness_assertion() -> None:
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
                                            "urn": "urn:li:assertion:test1",
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
                                                            "properties": {
                                                                "name": "test_table",
                                                                "qualifiedName": "test_db.public.test_table",
                                                            },
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
                            "status": {"mode": "ACTIVE"},
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
    fetcher = MonitorFetcher(
        graph=graph,
        config=MonitorFetcherConfig(mode=MonitorFetcherMode.DEFAULT, executor_ids=None),
    )

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
                "orFilters": [
                    {
                        "and": [
                            {
                                "field": "executorId",
                                "condition": "EXISTS",
                                "negated": True,
                            }
                        ]
                    }
                ],
                "searchFlags": {"skipCache": True},
            }
        },
    )


def test_fetch_volume_assertion() -> None:
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
                                            "urn": "urn:li:assertion:test2",
                                            "info": {
                                                "type": "VOLUME",
                                                "volumeAssertion": {
                                                    "type": "ROW_COUNT_TOTAL",
                                                    "rowCountTotal": {
                                                        "operator": "EQUAL_TO",
                                                        "parameters": {
                                                            "value": {
                                                                "value": "1000",
                                                                "type": "NUMBER",
                                                            }
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
                                                            "properties": {
                                                                "name": "test_table",
                                                                "qualifiedName": "test_db.public.test_table",
                                                            },
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
                                            "type": "DATASET_VOLUME",
                                            "datasetVolumeParameters": {
                                                "sourceType": "INFORMATION_SCHEMA"
                                            },
                                        },
                                    }
                                ]
                            },
                            "status": {"mode": "ACTIVE"},
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
    fetcher = MonitorFetcher(
        graph=graph,
        config=MonitorFetcherConfig(mode=MonitorFetcherMode.DEFAULT, executor_ids=None),
    )

    # Call the fetch_monitors method
    monitors = fetcher.fetch_monitors()

    # Verify that the fetch_assertions method returns the expected result
    assert len(monitors) == 1
    assert monitors[0].type == MonitorType.ASSERTION
    assert (
        monitors[0].assertion_monitor.assertions[0].assertion.type
        == AssertionType.VOLUME
    )
    assert (
        monitors[0].assertion_monitor.assertions[0].assertion.volume_assertion.type
        == VolumeAssertionType.ROW_COUNT_TOTAL
    )
    assert (
        monitors[0]
        .assertion_monitor.assertions[0]
        .assertion.volume_assertion.row_count_total.operator
        == AssertionStdOperator.EQUAL_TO
    )
    assert (
        monitors[0]
        .assertion_monitor.assertions[0]
        .assertion.volume_assertion.row_count_total.parameters.value.value
        == "1000"
    )
    assert (
        monitors[0]
        .assertion_monitor.assertions[0]
        .assertion.volume_assertion.row_count_total.parameters.value.type
        == AssertionStdParameterType.NUMBER
    )

    assert (
        monitors[0]
        .assertion_monitor.assertions[0]
        .parameters.dataset_volume_parameters.source_type
        == DatasetVolumeSourceType.INFORMATION_SCHEMA
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
                "orFilters": [
                    {
                        "and": [
                            {
                                "field": "executorId",
                                "condition": "EXISTS",
                                "negated": True,
                            }
                        ]
                    }
                ],
            }
        },
    )
