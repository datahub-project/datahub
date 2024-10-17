import pytest
from datahub.emitter.mce_builder import make_dataset_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.schema_classes import StatusClass

from tests.consistency_utils import wait_for_writes_to_sync
from tests.utils import delete_urn, get_sleep_info

restli_default_headers = {
    "X-RestLi-Protocol-Version": "2.0.0",
}
sleep_sec, sleep_times = get_sleep_info()

TEST_DATASET_URN = make_dataset_urn(platform="postgres", name="foo")


@pytest.fixture(scope="session", autouse=True)
def ingest_cleanup_data(graph_client):
    mcpw = MetadataChangeProposalWrapper(
        entityUrn=TEST_DATASET_URN, aspect=StatusClass(removed=False)
    )
    graph_client.emit(mcpw)
    yield
    delete_urn(graph_client, TEST_DATASET_URN)


def test_create_delete_freshness_assertion(auth_session):
    json = {
        "query": """mutation createFreshnessAssertion($input: CreateFreshnessAssertionInput!) {\n
            createFreshnessAssertion(input: $input) {\n
                urn\n
            }\n
        }""",
        "variables": {
            "input": {
                "entityUrn": TEST_DATASET_URN,
                "type": "DATASET_CHANGE",
                "schedule": {
                    "type": "CRON",
                    "cron": {"cron": "* * * * *", "timezone": "America / Los Angeles"},
                },
                "actions": {
                    "onSuccess": [{"type": "RESOLVE_INCIDENT"}],
                    "onFailure": [{"type": "RAISE_INCIDENT"}],
                },
            }
        },
    }

    response = auth_session.post(
        f"{auth_session.frontend_url()}/api/v2/graphql", json=json
    )
    response.raise_for_status()
    res_data = response.json()

    assert res_data
    assert res_data["data"]
    assert res_data["data"]["createFreshnessAssertion"]

    assertion_urn = res_data["data"]["createFreshnessAssertion"]["urn"]

    # Delete the assertion
    json = {
        "query": """mutation deleteAssertion($urn: String!) {\n
            deleteAssertion(urn: $urn)
        },""",
        "variables": {"urn": assertion_urn},
    }

    response = auth_session.post(
        f"{auth_session.frontend_url()}/api/v2/graphql", json=json
    )
    response.raise_for_status()
    res_data = response.json()

    assert res_data
    assert res_data["data"]
    assert res_data["data"]["deleteAssertion"] is True


def test_create_update_delete_dataset_freshness_assertion_monitor(auth_session):
    # Create the assertion
    json = {
        "query": """mutation upsertDatasetFreshnessAssertionMonitor($input: UpsertDatasetFreshnessAssertionMonitorInput!) {\n
            upsertDatasetFreshnessAssertionMonitor(input: $input) {\n
                urn\n
            }\n
        }""",
        "variables": {
            "input": {
                "entityUrn": TEST_DATASET_URN,
                "schedule": {
                    "type": "CRON",
                    "cron": {
                        "cron": "0 */8 * * *",
                        "timezone": "America / Los Angeles",
                    },
                },
                "actions": {
                    "onSuccess": [{"type": "RESOLVE_INCIDENT"}],
                    "onFailure": [{"type": "RAISE_INCIDENT"}],
                },
                "evaluationSchedule": {
                    "timezone": "America/Los_Angeles",
                    "cron": "0 */8 * * *",
                },
                "evaluationParameters": {"sourceType": "INFORMATION_SCHEMA"},
                "mode": "ACTIVE",
            }
        },
    }

    response = auth_session.post(
        f"{auth_session.frontend_url()}/api/v2/graphql", json=json
    )
    response.raise_for_status()
    res_data = response.json()

    assert res_data
    assert res_data["data"]
    assert res_data["data"]["upsertDatasetFreshnessAssertionMonitor"]

    assertion_urn = res_data["data"]["upsertDatasetFreshnessAssertionMonitor"]["urn"]

    wait_for_writes_to_sync()

    # update the assertion
    json = {
        "query": """mutation upsertDatasetFreshnessAssertionMonitor($assertionUrn:String, $input: UpsertDatasetFreshnessAssertionMonitorInput!) {\n
            upsertDatasetFreshnessAssertionMonitor(assertionUrn: $assertionUrn, input: $input) {\n
                urn\n
                monitor: relationships(
                    input: {types: "Evaluates", direction: INCOMING, start: 0, count: 1}
                ) {
                  relationships {
                      entity {
                      urn
                    }
                  }
                }
            }\n
        }""",
        "variables": {
            "assertionUrn": assertion_urn,
            "input": {
                "entityUrn": TEST_DATASET_URN,
                "schedule": {
                    "type": "CRON",
                    "cron": {
                        "cron": "0 */6 * * *",
                        "timezone": "America / Los Angeles",
                    },
                },
                "actions": {
                    "onSuccess": [{"type": "RESOLVE_INCIDENT"}],
                    "onFailure": [{"type": "RAISE_INCIDENT"}],
                },
                "evaluationSchedule": {
                    "timezone": "America/Los_Angeles",
                    "cron": "0 */6 * * *",
                },
                "evaluationParameters": {"sourceType": "INFORMATION_SCHEMA"},
                "mode": "ACTIVE",
            },
        },
    }

    response = auth_session.post(
        f"{auth_session.frontend_url()}/api/v2/graphql", json=json
    )
    response.raise_for_status()
    res_data = response.json()

    assert res_data
    assert res_data["data"]
    assert res_data["data"]["upsertDatasetFreshnessAssertionMonitor"]

    assert (
        res_data["data"]["upsertDatasetFreshnessAssertionMonitor"]["urn"]
        == assertion_urn
    )
    monitor_urn = res_data["data"]["upsertDatasetFreshnessAssertionMonitor"]["monitor"][
        "relationships"
    ][0]["entity"]["urn"]

    # Delete the assertion
    json = {
        "query": """mutation deleteAssertion($urn: String!) {\n
            deleteAssertion(urn: $urn)
        },""",
        "variables": {"urn": assertion_urn},
    }

    response = auth_session.post(
        f"{auth_session.frontend_url()}/api/v2/graphql", json=json
    )
    response.raise_for_status()
    res_data = response.json()

    assert res_data
    assert res_data["data"]
    assert res_data["data"]["deleteAssertion"] is True

    # Delete the monitor
    json = {
        "query": """mutation deleteMonitor($urn: String!) {\n
            deleteMonitor(urn: $urn)
        },""",
        "variables": {"urn": monitor_urn},
    }

    response = auth_session.post(
        f"{auth_session.frontend_url()}/api/v2/graphql", json=json
    )
    response.raise_for_status()
    res_data = response.json()

    assert res_data
    assert res_data["data"]
    assert res_data["data"]["deleteMonitor"] is True


def test_create_update_delete_dataset_volume_assertion_monitor(auth_session):
    # Create the assertion
    json = {
        "query": """mutation upsertDatasetVolumeAssertionMonitor($input: UpsertDatasetVolumeAssertionMonitorInput!) {\n
            upsertDatasetVolumeAssertionMonitor(input: $input) {\n
                urn\n
            }\n
        }""",
        "variables": {
            "input": {
                "entityUrn": TEST_DATASET_URN,
                "type": "ROW_COUNT_CHANGE",
                "rowCountChange": {
                    "type": "ABSOLUTE",
                    "operator": "LESS_THAN_OR_EQUAL_TO",
                    "parameters": {"value": {"value": "10", "type": "NUMBER"}},
                },
                "actions": {
                    "onSuccess": [{"type": "RESOLVE_INCIDENT"}],
                    "onFailure": [{"type": "RAISE_INCIDENT"}],
                },
                "evaluationSchedule": {
                    "timezone": "America/Los_Angeles",
                    "cron": "0 */8 * * *",
                },
                "evaluationParameters": {"sourceType": "INFORMATION_SCHEMA"},
                "mode": "ACTIVE",
            }
        },
    }

    response = auth_session.post(
        f"{auth_session.frontend_url()}/api/v2/graphql", json=json
    )
    response.raise_for_status()
    res_data = response.json()

    assert res_data
    assert res_data["data"]
    assert res_data["data"]["upsertDatasetVolumeAssertionMonitor"]

    assertion_urn = res_data["data"]["upsertDatasetVolumeAssertionMonitor"]["urn"]

    wait_for_writes_to_sync()

    # update the assertion
    json = {
        "query": """mutation upsertDatasetVolumeAssertionMonitor($assertionUrn:String, $input: UpsertDatasetVolumeAssertionMonitorInput!) {\n
            upsertDatasetVolumeAssertionMonitor(assertionUrn: $assertionUrn, input: $input) {\n
                urn\n
                monitor: relationships(
                    input: {types: "Evaluates", direction: INCOMING, start: 0, count: 1}
                ) {
                  relationships {
                      entity {
                      urn
                    }
                  }
                }
            }\n
        }""",
        "variables": {
            "assertionUrn": assertion_urn,
            "input": {
                "entityUrn": TEST_DATASET_URN,
                "type": "ROW_COUNT_TOTAL",
                "rowCountTotal": {
                    "operator": "LESS_THAN_OR_EQUAL_TO",
                    "parameters": {"value": {"value": "1000", "type": "NUMBER"}},
                },
                "actions": {
                    "onSuccess": [{"type": "RESOLVE_INCIDENT"}],
                    "onFailure": [{"type": "RAISE_INCIDENT"}],
                },
                "evaluationSchedule": {
                    "timezone": "America/Los_Angeles",
                    "cron": "0 */8 * * *",
                },
                "evaluationParameters": {"sourceType": "INFORMATION_SCHEMA"},
                "mode": "ACTIVE",
            },
        },
    }

    response = auth_session.post(
        f"{auth_session.frontend_url()}/api/v2/graphql", json=json
    )
    response.raise_for_status()
    res_data = response.json()

    assert res_data
    assert res_data["data"]
    assert res_data["data"]["upsertDatasetVolumeAssertionMonitor"]

    assert (
        res_data["data"]["upsertDatasetVolumeAssertionMonitor"]["urn"] == assertion_urn
    )
    monitor_urn = res_data["data"]["upsertDatasetVolumeAssertionMonitor"]["monitor"][
        "relationships"
    ][0]["entity"]["urn"]

    # Delete the assertion
    json = {
        "query": """mutation deleteAssertion($urn: String!) {\n
            deleteAssertion(urn: $urn)
        },""",
        "variables": {"urn": assertion_urn},
    }

    response = auth_session.post(
        f"{auth_session.frontend_url()}/api/v2/graphql", json=json
    )
    response.raise_for_status()
    res_data = response.json()

    assert res_data
    assert res_data["data"]
    assert res_data["data"]["deleteAssertion"] is True

    # Delete the monitor
    json = {
        "query": """mutation deleteMonitor($urn: String!) {\n
            deleteMonitor(urn: $urn)
        },""",
        "variables": {"urn": monitor_urn},
    }

    response = auth_session.post(
        f"{auth_session.frontend_url()}/api/v2/graphql", json=json
    )
    response.raise_for_status()
    res_data = response.json()

    assert res_data
    assert res_data["data"]
    assert res_data["data"]["deleteMonitor"] is True


def test_create_update_delete_dataset_sql_assertion_monitor(auth_session):
    # Create the assertion
    json = {
        "query": """mutation upsertDatasetSqlAssertionMonitor($input: UpsertDatasetSqlAssertionMonitorInput!) {\n
            upsertDatasetSqlAssertionMonitor(input: $input) {\n
                urn\n
            }\n
        }""",
        "variables": {
            "input": {
                "entityUrn": TEST_DATASET_URN,
                "description": "Custom sql assertion",
                "type": "METRIC",
                "statement": "select x from table",
                "operator": "LESS_THAN_OR_EQUAL_TO",
                "parameters": {"value": {"value": "1000", "type": "NUMBER"}},
                "actions": {
                    "onSuccess": [{"type": "RESOLVE_INCIDENT"}],
                    "onFailure": [{"type": "RAISE_INCIDENT"}],
                },
                "evaluationSchedule": {
                    "timezone": "America/Los_Angeles",
                    "cron": "0 */8 * * *",
                },
                "mode": "ACTIVE",
            }
        },
    }

    response = auth_session.post(
        f"{auth_session.frontend_url()}/api/v2/graphql", json=json
    )
    response.raise_for_status()
    res_data = response.json()

    assert res_data
    assert res_data["data"]
    assert res_data["data"]["upsertDatasetSqlAssertionMonitor"]

    assertion_urn = res_data["data"]["upsertDatasetSqlAssertionMonitor"]["urn"]

    wait_for_writes_to_sync()

    # update the assertion
    json = {
        "query": """mutation upsertDatasetSqlAssertionMonitor($assertionUrn:String, $input: UpsertDatasetSqlAssertionMonitorInput!) {\n
            upsertDatasetSqlAssertionMonitor(assertionUrn: $assertionUrn, input: $input) {\n
                urn\n
                monitor: relationships(
                    input: {types: "Evaluates", direction: INCOMING, start: 0, count: 1}
                ) {
                  relationships {
                      entity {
                      urn
                    }
                  }
                }
            }\n
        }""",
        "variables": {
            "assertionUrn": assertion_urn,
            "input": {
                "entityUrn": TEST_DATASET_URN,
                "description": "Custom sql assertion",
                "type": "METRIC_CHANGE",
                "changeType": "ABSOLUTE",
                "statement": "select x from table",
                "operator": "LESS_THAN_OR_EQUAL_TO",
                "parameters": {"value": {"value": "1000", "type": "NUMBER"}},
                "actions": {
                    "onSuccess": [{"type": "RESOLVE_INCIDENT"}],
                    "onFailure": [{"type": "RAISE_INCIDENT"}],
                },
                "evaluationSchedule": {
                    "timezone": "America/Los_Angeles",
                    "cron": "0 */8 * * *",
                },
                "mode": "ACTIVE",
            },
        },
    }

    response = auth_session.post(
        f"{auth_session.frontend_url()}/api/v2/graphql", json=json
    )
    response.raise_for_status()
    res_data = response.json()

    assert res_data
    assert res_data["data"]
    assert res_data["data"]["upsertDatasetSqlAssertionMonitor"]

    assert res_data["data"]["upsertDatasetSqlAssertionMonitor"]["urn"] == assertion_urn
    monitor_urn = res_data["data"]["upsertDatasetSqlAssertionMonitor"]["monitor"][
        "relationships"
    ][0]["entity"]["urn"]

    # Delete the assertion
    json = {
        "query": """mutation deleteAssertion($urn: String!) {\n
            deleteAssertion(urn: $urn)
        },""",
        "variables": {"urn": assertion_urn},
    }

    response = auth_session.post(
        f"{auth_session.frontend_url()}/api/v2/graphql", json=json
    )
    response.raise_for_status()
    res_data = response.json()

    assert res_data
    assert res_data["data"]
    assert res_data["data"]["deleteAssertion"] is True

    # Delete the monitor
    json = {
        "query": """mutation deleteMonitor($urn: String!) {\n
            deleteMonitor(urn: $urn)
        },""",
        "variables": {"urn": monitor_urn},
    }

    response = auth_session.post(
        f"{auth_session.frontend_url()}/api/v2/graphql", json=json
    )
    response.raise_for_status()
    res_data = response.json()

    assert res_data
    assert res_data["data"]
    assert res_data["data"]["deleteMonitor"] is True


def test_create_update_delete_dataset_field_assertion_monitor(auth_session):
    # Create the assertion
    json = {
        "query": """mutation upsertDatasetFieldAssertionMonitor($input: UpsertDatasetFieldAssertionMonitorInput!) {\n
            upsertDatasetFieldAssertionMonitor(input: $input) {\n
                urn\n
            }\n
        }""",
        "variables": {
            "input": {
                "entityUrn": TEST_DATASET_URN,
                "type": "FIELD_VALUES",
                "fieldValuesAssertion": {
                    "field": {"path": "x", "type": "NUMBER", "nativeType": "NUMBER"},
                    "operator": "LESS_THAN_OR_EQUAL_TO",
                    "parameters": {"value": {"value": "10", "type": "NUMBER"}},
                    "excludeNulls": True,
                    "failThreshold": {"type": "COUNT", "value": "0"},
                },
                "actions": {
                    "onSuccess": [{"type": "RESOLVE_INCIDENT"}],
                    "onFailure": [{"type": "RAISE_INCIDENT"}],
                },
                "evaluationSchedule": {
                    "timezone": "America/Los_Angeles",
                    "cron": "0 */8 * * *",
                },
                "evaluationParameters": {"sourceType": "ALL_ROWS_QUERY"},
                "mode": "ACTIVE",
            }
        },
    }

    response = auth_session.post(
        f"{auth_session.frontend_url()}/api/v2/graphql", json=json
    )
    response.raise_for_status()
    res_data = response.json()

    assert res_data
    assert res_data["data"]
    assert res_data["data"]["upsertDatasetFieldAssertionMonitor"]

    assertion_urn = res_data["data"]["upsertDatasetFieldAssertionMonitor"]["urn"]

    wait_for_writes_to_sync()

    # update the assertion
    json = {
        "query": """mutation upsertDatasetFieldAssertionMonitor($assertionUrn:String, $input: UpsertDatasetFieldAssertionMonitorInput!) {\n
            upsertDatasetFieldAssertionMonitor(assertionUrn: $assertionUrn, input: $input) {\n
                urn\n
                monitor: relationships(
                    input: {types: "Evaluates", direction: INCOMING, start: 0, count: 1}
                ) {
                  relationships {
                      entity {
                      urn
                    }
                  }
                }
            }\n
        }""",
        "variables": {
            "assertionUrn": assertion_urn,
            "input": {
                "entityUrn": TEST_DATASET_URN,
                "type": "FIELD_METRIC",
                "fieldMetricAssertion": {
                    "metric": "MAX",
                    "field": {"path": "x", "type": "NUMBER", "nativeType": "NUMBER"},
                    "operator": "LESS_THAN_OR_EQUAL_TO",
                    "parameters": {"value": {"value": "10", "type": "NUMBER"}},
                },
                "actions": {
                    "onSuccess": [{"type": "RESOLVE_INCIDENT"}],
                    "onFailure": [{"type": "RAISE_INCIDENT"}],
                },
                "evaluationSchedule": {
                    "timezone": "America/Los_Angeles",
                    "cron": "0 */8 * * *",
                },
                "evaluationParameters": {"sourceType": "CHANGED_ROWS_QUERY"},
                "mode": "ACTIVE",
            },
        },
    }

    response = auth_session.post(
        f"{auth_session.frontend_url()}/api/v2/graphql", json=json
    )
    response.raise_for_status()
    res_data = response.json()

    assert res_data
    assert res_data["data"]
    assert res_data["data"]["upsertDatasetFieldAssertionMonitor"]

    assert (
        res_data["data"]["upsertDatasetFieldAssertionMonitor"]["urn"] == assertion_urn
    )
    monitor_urn = res_data["data"]["upsertDatasetFieldAssertionMonitor"]["monitor"][
        "relationships"
    ][0]["entity"]["urn"]

    # Delete the assertion
    json = {
        "query": """mutation deleteAssertion($urn: String!) {\n
            deleteAssertion(urn: $urn)
        },""",
        "variables": {"urn": assertion_urn},
    }

    response = auth_session.post(
        f"{auth_session.frontend_url()}/api/v2/graphql", json=json
    )
    response.raise_for_status()
    res_data = response.json()

    assert res_data
    assert res_data["data"]
    assert res_data["data"]["deleteAssertion"] is True

    # Delete the monitor
    json = {
        "query": """mutation deleteMonitor($urn: String!) {\n
            deleteMonitor(urn: $urn)
        },""",
        "variables": {"urn": monitor_urn},
    }

    response = auth_session.post(
        f"{auth_session.frontend_url()}/api/v2/graphql", json=json
    )
    response.raise_for_status()
    res_data = response.json()

    assert res_data
    assert res_data["data"]
    assert res_data["data"]["deleteMonitor"] is True
