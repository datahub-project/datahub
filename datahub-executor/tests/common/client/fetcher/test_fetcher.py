# type: ignore

from unittest.mock import Mock

from datahub.ingestion.graph.client import DataHubGraph

from datahub_executor.common.client.fetcher.monitors.fetcher import MonitorFetcher
from datahub_executor.common.client.fetcher.monitors.graphql.query import (
    GRAPHQL_LIST_MONITORS_QUERY,
)
from datahub_executor.common.constants import (
    DATAHUB_EXECUTOR_EMBEDDED_POOL_ID,
    LIST_MONITORS_BATCH_SIZE,
)
from datahub_executor.common.types import (
    AssertionEvaluationParametersType,
    AssertionSourceType,
    AssertionStdOperator,
    AssertionStdParameterType,
    AssertionType,
    DatasetFieldSourceType,
    DatasetFreshnessSourceType,
    DatasetSchemaSourceType,
    DatasetVolumeSourceType,
    FetcherConfig,
    FetcherMode,
    FieldAssertionType,
    FreshnessAssertionScheduleType,
    FreshnessAssertionType,
    MonitorType,
    SchemaAssertionCompatibility,
    SchemaFieldDataType,
    SQLAssertionType,
    VolumeAssertionType,
)


def test_fetch_freshness_assertion() -> None:
    # Create a mock DataHubGraph object
    graph = Mock(spec=DataHubGraph)

    # Configure the mock object to return a specific result when its execute_graphql method is called
    graph.execute_graphql.return_value = {
        "searchAcrossEntities": {
            "start": 0,
            "total": 1,
            "count": 10,
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
                                                            "exists": True,
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
            ],
        },
        "error": None,
    }

    graph.execute_graphql.side_effect = (
        lambda *args, **kwargs: graph.execute_graphql.return_value
    )

    # Create the MonitorFetcher with the mock graph
    fetcher = MonitorFetcher(
        graph=graph,
        config=FetcherConfig(
            id="fetcher-id",
            mode=FetcherMode.DEFAULT,
            executor_ids=None,
            refresh_interval=1,
        ),
    )

    # Call the _fetch_monitors method
    monitors = fetcher._fetch_monitors()

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

    assert (
        monitors[0].assertion_monitor.assertions[0].assertion.source_type
        == AssertionSourceType.NATIVE
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
                                "field": "mode",
                                "values": ["ACTIVE", "PASSIVE"],
                                "condition": "EQUAL",
                            },
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
            "start": 0,
            "total": 1,
            "count": 10,
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
                                                            "exists": True,
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
            ],
        },
        "error": None,
    }

    graph.execute_graphql.side_effect = (
        lambda *args, **kwargs: graph.execute_graphql.return_value
    )

    # Create the MonitorFetcher with the mock graph
    fetcher = MonitorFetcher(
        graph=graph,
        config=FetcherConfig(
            id="fetcher-id",
            mode=FetcherMode.DEFAULT,
            executor_ids=None,
            refresh_interval=1,
        ),
    )

    # Call the _fetch_monitors method
    monitors = fetcher._fetch_monitors()

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

    assert (
        monitors[0].assertion_monitor.assertions[0].assertion.source_type
        == AssertionSourceType.NATIVE
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
                                "field": "mode",
                                "values": ["ACTIVE", "PASSIVE"],
                                "condition": "EQUAL",
                            },
                        ]
                    }
                ],
            }
        },
    )


def test_fetch_inferred_volume_assertion() -> None:
    # Create a mock DataHubGraph object
    graph = Mock(spec=DataHubGraph)

    # Configure the mock object to return a specific result when its execute_graphql method is called
    graph.execute_graphql.return_value = {
        "searchAcrossEntities": {
            "start": 0,
            "total": 1,
            "count": 10,
            "searchResults": [
                {
                    "entity": {
                        "urn": "urn:li:monitor:(urn:li:dataset:(urn:li:dataPlatform:snowflake,database_1.schema_1.message,PROD),__system__volume)",
                        "info": {
                            "type": "ASSERTION",
                            "status": {"mode": "ACTIVE"},
                            "executorId": DATAHUB_EXECUTOR_EMBEDDED_POOL_ID,
                            "assertionMonitor": {
                                "assertions": [
                                    {
                                        "assertion": {
                                            "urn": "urn:li:assertion:dXJuOmxpOmRhdGFzZXQ6KHVybjpsaTpkYXRhUGxhdGZvcm06c25vd2ZsYWtlLGRhdGFodWJfY29tbXVuaXR5LmRhdGFodWJfc2xhY2subWVzc2FnZSxQUk9EKQ==-__system__volume",
                                            "type": "ASSERTION",
                                            "platform": {
                                                "urn": "urn:li:dataPlatform:unknown",
                                                "properties": {
                                                    "displayName": "N/A",
                                                    "logoUrl": None,
                                                },
                                                "info": None,
                                            },
                                            "info": {
                                                "type": "VOLUME",
                                                "datasetAssertion": None,
                                                "freshnessAssertion": None,
                                                "volumeAssertion": {
                                                    "type": "ROW_COUNT_TOTAL",
                                                    "rowCountTotal": {
                                                        "operator": "BETWEEN",
                                                        "parameters": {
                                                            "value": None,
                                                            "minValue": {
                                                                "value": "1000.0",
                                                                "type": "NUMBER",
                                                            },
                                                            "maxValue": {
                                                                "value": "2000.0",
                                                                "type": "NUMBER",
                                                            },
                                                        },
                                                    },
                                                    "rowCountChange": None,
                                                    "incrementingSegmentRowCountTotal": None,
                                                    "incrementingSegmentRowCountChange": None,
                                                    "filter": None,
                                                },
                                                "sqlAssertion": None,
                                                "fieldAssertion": None,
                                                "schemaAssertion": None,
                                                "source": {"type": "INFERRED"},
                                            },
                                            "inferenceDetails": None,
                                            "rawInfoAspect": [
                                                {
                                                    "aspectName": "assertionInfo",
                                                    "payload": '{"volumeAssertion":{"type":"ROW_COUNT_TOTAL","entity":"urn:li:dataset:(urn:li:dataPlatform:snowflake,database_1.schema_1.message,PROD)","rowCountTotal":{"parameters":{"maxValue":{"type":"NUMBER","value":"2000.0"},"minValue":{"type":"NUMBER","value":"1000.0"}},"operator":"BETWEEN"}},"customProperties":{},"source":{"type":"INFERRED"},"type":"VOLUME"}',
                                                }
                                            ],
                                            "relationships": {
                                                "start": 0,
                                                "count": 1,
                                                "total": 1,
                                                "relationships": [
                                                    {
                                                        "entity": {
                                                            "urn": "urn:li:dataset:(urn:li:dataPlatform:snowflake,database_1.schema_1.message,PROD)",
                                                            "properties": {
                                                                "name": "MESSAGE",
                                                                "qualifiedName": "DATAHUB_COMMUNITY.DATAHUB_SLACK.MESSAGE",
                                                            },
                                                            "platform": {
                                                                "urn": "urn:li:dataPlatform:snowflake",
                                                                "properties": {
                                                                    "displayName": "Snowflake"
                                                                },
                                                            },
                                                            "subTypes": {
                                                                "typeNames": ["Table"]
                                                            },
                                                            "exists": True,
                                                        }
                                                    }
                                                ],
                                            },
                                        },
                                        "schedule": {
                                            "cron": "* * * * *",
                                            "timezone": "UTC",
                                        },
                                        "parameters": {
                                            "type": "DATASET_VOLUME",
                                            "datasetFreshnessParameters": None,
                                            "datasetVolumeParameters": {
                                                "sourceType": "DATAHUB_DATASET_PROFILE"
                                            },
                                            "datasetFieldParameters": None,
                                            "datasetSchemaParameters": None,
                                        },
                                        "rawParameters": '{"type":"DATASET_VOLUME","datasetVolumeParameters":{"sourceType":"DATAHUB_DATASET_PROFILE"}}',
                                        "context": {
                                            "embeddedAssertions": [
                                                {
                                                    "assertion": {
                                                        "type": "VOLUME",
                                                        "datasetAssertion": None,
                                                        "freshnessAssertion": None,
                                                        "volumeAssertion": {
                                                            "type": "ROW_COUNT_TOTAL",
                                                            "rowCountTotal": {
                                                                "operator": "BETWEEN",
                                                                "parameters": {
                                                                    "value": None,
                                                                    "minValue": {
                                                                        "value": "1000.0",
                                                                        "type": "NUMBER",
                                                                    },
                                                                    "maxValue": {
                                                                        "value": "2000.0",
                                                                        "type": "NUMBER",
                                                                    },
                                                                },
                                                            },
                                                            "rowCountChange": None,
                                                            "incrementingSegmentRowCountTotal": None,
                                                            "incrementingSegmentRowCountChange": None,
                                                            "filter": None,
                                                        },
                                                        "sqlAssertion": None,
                                                        "fieldAssertion": None,
                                                        "schemaAssertion": None,
                                                        "source": {"type": "INFERRED"},
                                                    },
                                                    "rawAssertion": '{"volumeAssertion":{"type":"ROW_COUNT_TOTAL","entity":"urn:li:dataset:(urn:li:dataPlatform:snowflake,database_1.schema_1.message,PROD)","rowCountTotal":{"parameters":{"maxValue":{"type":"NUMBER","value":"2000.0"},"minValue":{"type":"NUMBER","value":"1000.0"}},"operator":"BETWEEN"}},"customProperties":{},"source":{"type":"INFERRED"},"type":"VOLUME"}',
                                                    "evaluationTimeWindow": {
                                                        "startTimeMillis": 1715731200000,
                                                        "endTimeMillis": 1715817600000,
                                                    },
                                                },
                                            ]
                                        },
                                    }
                                ]
                            },
                        },
                    }
                }
            ],
        },
        "error": None,
    }

    graph.execute_graphql.side_effect = (
        lambda *args, **kwargs: graph.execute_graphql.return_value
    )

    # Create the MonitorFetcher with the mock graph
    fetcher = MonitorFetcher(
        graph=graph,
        config=FetcherConfig(
            id="fetcher-id",
            mode=FetcherMode.DEFAULT,
            executor_ids=None,
            refresh_interval=1,
        ),
    )

    # Call the _fetch_monitors method
    monitors = fetcher._fetch_monitors()

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
        == AssertionStdOperator.BETWEEN
    )
    assert (
        monitors[0]
        .assertion_monitor.assertions[0]
        .assertion.volume_assertion.row_count_total.parameters.min_value.value
        == "1000.0"
    )
    assert (
        monitors[0]
        .assertion_monitor.assertions[0]
        .assertion.volume_assertion.row_count_total.parameters.max_value.value
        == "2000.0"
    )
    assert (
        monitors[0]
        .assertion_monitor.assertions[0]
        .assertion.volume_assertion.row_count_total.parameters.min_value.type
        == AssertionStdParameterType.NUMBER
    )
    assert (
        monitors[0]
        .assertion_monitor.assertions[0]
        .assertion.volume_assertion.row_count_total.parameters.max_value.type
        == AssertionStdParameterType.NUMBER
    )

    assert (
        monitors[0].assertion_monitor.assertions[0].assertion.source_type
        == AssertionSourceType.INFERRED
    )

    assert (
        monitors[0]
        .assertion_monitor.assertions[0]
        .parameters.dataset_volume_parameters.source_type
        == DatasetVolumeSourceType.DATAHUB_DATASET_PROFILE
    )

    assert monitors[0].assertion_monitor.assertions[0].assertion.raw_info_aspect

    # Verify that embedded assertion is parsed correctly

    assert (
        monitors[0]
        .assertion_monitor.assertions[0]
        .context.embedded_assertions[0]
        .assertion.type
        == AssertionType.VOLUME
    )
    assert (
        monitors[0]
        .assertion_monitor.assertions[0]
        .context.embedded_assertions[0]
        .assertion.volume_assertion.type
        == VolumeAssertionType.ROW_COUNT_TOTAL
    )
    assert (
        monitors[0]
        .assertion_monitor.assertions[0]
        .context.embedded_assertions[0]
        .assertion.volume_assertion.row_count_total.operator
        == AssertionStdOperator.BETWEEN
    )
    assert (
        monitors[0]
        .assertion_monitor.assertions[0]
        .context.embedded_assertions[0]
        .assertion.volume_assertion.row_count_total.parameters.min_value.value
        == "1000.0"
    )
    assert (
        monitors[0]
        .assertion_monitor.assertions[0]
        .context.embedded_assertions[0]
        .assertion.volume_assertion.row_count_total.parameters.max_value.value
        == "2000.0"
    )
    assert (
        monitors[0]
        .assertion_monitor.assertions[0]
        .context.embedded_assertions[0]
        .assertion.volume_assertion.row_count_total.parameters.min_value.type
        == AssertionStdParameterType.NUMBER
    )
    assert (
        monitors[0]
        .assertion_monitor.assertions[0]
        .context.embedded_assertions[0]
        .assertion.volume_assertion.row_count_total.parameters.max_value.type
        == AssertionStdParameterType.NUMBER
    )

    assert (
        monitors[0]
        .assertion_monitor.assertions[0]
        .context.embedded_assertions[0]
        .assertion.source_type
        == AssertionSourceType.INFERRED
    )

    assert (
        monitors[0]
        .assertion_monitor.assertions[0]
        .context.embedded_assertions[0]
        .raw_assertion
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
                                "field": "mode",
                                "values": ["ACTIVE", "PASSIVE"],
                                "condition": "EQUAL",
                            },
                        ]
                    }
                ],
            }
        },
    )


def test_fetch_sql_assertion() -> None:
    # Create a mock DataHubGraph object
    graph = Mock(spec=DataHubGraph)

    # Configure the mock object to return a specific result when its execute_graphql method is called
    graph.execute_graphql.return_value = {
        "searchAcrossEntities": {
            "start": 0,
            "total": 1,
            "count": 10,
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
                                            "urn": "urn:li:assertion:test3",
                                            "info": {
                                                "type": "SQL",
                                                "sqlAssertion": {
                                                    "type": "METRIC",
                                                    "statement": "SELECT AVG(total) FROM purchases;",
                                                    "changeType": "ABSOLUTE",
                                                    "operator": "GREATER_THAN",
                                                    "parameters": {
                                                        "value": {
                                                            "value": "1000",
                                                            "type": "NUMBER",
                                                        }
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
                                                            "exists": True,
                                                        }
                                                    }
                                                ]
                                            },
                                        },
                                        "schedule": {
                                            "cron": "0 * * * *",
                                            "timezone": "America/Los_Angeles",
                                        },
                                        "parameters": {"type": "DATASET_SQL"},
                                    }
                                ]
                            },
                            "status": {"mode": "ACTIVE"},
                        },
                    }
                }
            ],
        },
        "error": None,
    }

    graph.execute_graphql.side_effect = (
        lambda *args, **kwargs: graph.execute_graphql.return_value
    )

    # Create the MonitorFetcher with the mock graph
    fetcher = MonitorFetcher(
        graph=graph,
        config=FetcherConfig(
            id="fetcher-id",
            mode=FetcherMode.DEFAULT,
            executor_ids=None,
            refresh_interval=1,
        ),
    )

    # Call the _fetch_monitors method
    monitors = fetcher._fetch_monitors()

    # Verify that the fetch_assertions method returns the expected result
    assert len(monitors) == 1
    assert monitors[0].type == MonitorType.ASSERTION
    assert (
        monitors[0].assertion_monitor.assertions[0].assertion.type == AssertionType.SQL
    )
    assert (
        monitors[0].assertion_monitor.assertions[0].assertion.sql_assertion.type
        == SQLAssertionType.METRIC
    )
    assert (
        monitors[0].assertion_monitor.assertions[0].assertion.sql_assertion.statement
        == "SELECT AVG(total) FROM purchases;"
    )
    assert (
        monitors[0].assertion_monitor.assertions[0].assertion.sql_assertion.operator
        == AssertionStdOperator.GREATER_THAN
    )
    assert (
        monitors[0]
        .assertion_monitor.assertions[0]
        .assertion.sql_assertion.parameters.value.value
        == "1000"
    )
    assert (
        monitors[0]
        .assertion_monitor.assertions[0]
        .assertion.sql_assertion.parameters.value.type
        == AssertionStdParameterType.NUMBER
    )

    assert (
        monitors[0].assertion_monitor.assertions[0].assertion.source_type
        == AssertionSourceType.NATIVE
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
                                "field": "mode",
                                "values": ["ACTIVE", "PASSIVE"],
                                "condition": "EQUAL",
                            },
                        ]
                    }
                ],
            }
        },
    )


def test_fetch_field_assertion() -> None:
    # Create a mock DataHubGraph object
    graph = Mock(spec=DataHubGraph)

    # Configure the mock object to return a specific result when its execute_graphql method is called
    graph.execute_graphql.return_value = {
        "searchAcrossEntities": {
            "start": 0,
            "total": 1,
            "count": 10,
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
                                                "type": "FIELD",
                                                "fieldAssertion": {
                                                    "type": "FIELD_VALUES",
                                                    "fieldValuesAssertion": {
                                                        "field": {
                                                            "path": "id",
                                                            "type": "STRING",
                                                            "native_type": "STRING",
                                                        },
                                                        "transform": {"type": "LENGTH"},
                                                        "operator": "EQUAL_TO",
                                                        "parameters": {
                                                            "value": {
                                                                "value": "10",
                                                                "type": "NUMBER",
                                                            }
                                                        },
                                                        "failThreshold": {
                                                            "type": "COUNT",
                                                            "value": "1",
                                                        },
                                                        "excludeNulls": "false",
                                                    },
                                                },
                                                "filter": {
                                                    "type": "SQL",
                                                    "sql": "where foo='bar'",
                                                },
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
                                                            "exists": True,
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
                                            "type": "DATASET_FIELD",
                                            "datasetFieldParameters": {
                                                "sourceType": "ALL_ROWS_QUERY",
                                                "changedRowsField": {
                                                    "kind": "LAST_MODIFIED",
                                                    "path": "timestamp_field",
                                                    "type": "TIMESTAMP",
                                                },
                                            },
                                        },
                                    }
                                ]
                            },
                            "status": {"mode": "ACTIVE"},
                        },
                    }
                }
            ],
        },
        "error": None,
    }

    graph.execute_graphql.side_effect = (
        lambda *args, **kwargs: graph.execute_graphql.return_value
    )

    # Create the MonitorFetcher with the mock graph
    fetcher = MonitorFetcher(
        graph=graph,
        config=FetcherConfig(
            id="fetcher-id",
            mode=FetcherMode.DEFAULT,
            executor_ids=None,
            refresh_interval=1,
        ),
    )

    # Call the _fetch_monitors method
    monitors = fetcher._fetch_monitors()

    # Verify that the fetch_assertions method returns the expected result
    assert len(monitors) == 1
    assert monitors[0].type == MonitorType.ASSERTION
    assert (
        monitors[0].assertion_monitor.assertions[0].assertion.type
        == AssertionType.FIELD
    )
    assert (
        monitors[0].assertion_monitor.assertions[0].assertion.field_assertion.type
        == FieldAssertionType.FIELD_VALUES
    )
    assert (
        monitors[0]
        .assertion_monitor.assertions[0]
        .assertion.field_assertion.field_values_assertion.operator
        == AssertionStdOperator.EQUAL_TO
    )
    assert (
        monitors[0]
        .assertion_monitor.assertions[0]
        .assertion.field_assertion.field_values_assertion.parameters.value.type
        == AssertionStdParameterType.NUMBER
    )

    assert (
        monitors[0]
        .assertion_monitor.assertions[0]
        .parameters.dataset_field_parameters.source_type
        == DatasetFieldSourceType.ALL_ROWS_QUERY
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
                                "field": "mode",
                                "values": ["ACTIVE", "PASSIVE"],
                                "condition": "EQUAL",
                            },
                        ]
                    }
                ],
            }
        },
    )


def test_fetch_schema_assertion() -> None:
    # Create a mock DataHubGraph object
    graph = Mock(spec=DataHubGraph)

    # Configure the mock object to return a specific result when its execute_graphql method is called
    graph.execute_graphql.return_value = {
        "searchAcrossEntities": {
            "start": 0,
            "total": 1,
            "count": 10,
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
                                                "type": "DATA_SCHEMA",
                                                "schemaAssertion": {
                                                    "compatibility": "EXACT_MATCH",
                                                    "fields": [
                                                        {
                                                            "path": "path1",
                                                            "type": "STRING",
                                                            "nativeType": "varchar(64)",
                                                        },
                                                        {
                                                            "path": "path2",
                                                            "type": "NUMBER",
                                                            "nativeType": "int(64)",
                                                        },
                                                    ],
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
                                                            "exists": True,
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
                                            "type": "DATASET_SCHEMA",
                                            "datasetSchemaParameters": {
                                                "sourceType": "DATAHUB_SCHEMA"
                                            },
                                        },
                                    }
                                ]
                            },
                            "status": {"mode": "ACTIVE"},
                        },
                    }
                }
            ],
        },
        "error": None,
    }

    graph.execute_graphql.side_effect = (
        lambda *args, **kwargs: graph.execute_graphql.return_value
    )

    # Create the MonitorFetcher with the mock graph
    fetcher = MonitorFetcher(
        graph=graph,
        config=FetcherConfig(
            id="fetcher-id",
            mode=FetcherMode.DEFAULT,
            executor_ids=None,
            refresh_interval=1,
        ),
    )

    # Call the _fetch_monitors method
    monitors = fetcher._fetch_monitors()

    # Verify that the fetch_assertions method returns the expected result
    assert len(monitors) == 1
    assert monitors[0].type == MonitorType.ASSERTION
    assert (
        monitors[0].assertion_monitor.assertions[0].assertion.type
        == AssertionType.DATA_SCHEMA
    )
    assert (
        monitors[0]
        .assertion_monitor.assertions[0]
        .assertion.schema_assertion.compatibility
        == SchemaAssertionCompatibility.EXACT_MATCH
    )
    assert (
        len(
            monitors[0]
            .assertion_monitor.assertions[0]
            .assertion.schema_assertion.fields
        )
        == 2
    )
    # Field 1
    assert (
        monitors[0]
        .assertion_monitor.assertions[0]
        .assertion.schema_assertion.fields[0]
        .path
        == "path1"
    )
    assert (
        monitors[0]
        .assertion_monitor.assertions[0]
        .assertion.schema_assertion.fields[0]
        .type
        == SchemaFieldDataType.STRING
    )
    assert (
        monitors[0]
        .assertion_monitor.assertions[0]
        .assertion.schema_assertion.fields[0]
        .nativeType
        == "varchar(64)"
    )
    # Field 2
    assert (
        monitors[0]
        .assertion_monitor.assertions[0]
        .assertion.schema_assertion.fields[1]
        .path
        == "path2"
    )
    assert (
        monitors[0]
        .assertion_monitor.assertions[0]
        .assertion.schema_assertion.fields[1]
        .type
        == SchemaFieldDataType.NUMBER
    )
    assert (
        monitors[0]
        .assertion_monitor.assertions[0]
        .assertion.schema_assertion.fields[1]
        .nativeType
        == "int(64)"
    )
    assert (
        monitors[0]
        .assertion_monitor.assertions[0]
        .parameters.dataset_schema_parameters.source_type
        == DatasetSchemaSourceType.DATAHUB_SCHEMA
    )

    assert (
        monitors[0].assertion_monitor.assertions[0].assertion.source_type
        == AssertionSourceType.NATIVE
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
                                "field": "mode",
                                "values": ["ACTIVE", "PASSIVE"],
                                "condition": "EQUAL",
                            },
                        ]
                    }
                ],
            }
        },
    )
