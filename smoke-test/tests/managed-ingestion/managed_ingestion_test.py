import json
import time

import pytest
import tenacity

from tests.utils import get_frontend_url, get_sleep_info, wait_for_healthcheck_util

sleep_sec, sleep_times = get_sleep_info()


@pytest.fixture(scope="session")
def wait_for_healthchecks():
    wait_for_healthcheck_util()
    yield


@pytest.mark.dependency()
def test_healthchecks(wait_for_healthchecks):
    # Call to wait_for_healthchecks fixture will do the actual functionality.
    pass


def _get_ingestionSources(frontend_session):
    json_q = {
        "query": """query listIngestionSources($input: ListIngestionSourcesInput!) {\n
            listIngestionSources(input: $input) {\n
              start\n
              count\n
              total\n
              ingestionSources {\n
                urn\n
              }\n
            }\n
        }""",
        "variables": {"input": {"start": "0", "count": "20"}},
    }

    response = frontend_session.post(
        f"{get_frontend_url()}/api/v2/graphql", json=json_q
    )
    response.raise_for_status()
    res_data = response.json()

    assert res_data
    assert res_data["data"]
    assert res_data["data"]["listIngestionSources"]["total"] is not None
    assert "errors" not in res_data
    return res_data


@tenacity.retry(
    stop=tenacity.stop_after_attempt(sleep_times), wait=tenacity.wait_fixed(sleep_sec)
)
def _ensure_ingestion_source_count(frontend_session, expected_count):
    res_data = _get_ingestionSources(frontend_session)
    after_count = res_data["data"]["listIngestionSources"]["total"]
    assert after_count == expected_count
    return after_count


@tenacity.retry(
    stop=tenacity.stop_after_attempt(sleep_times), wait=tenacity.wait_fixed(sleep_sec)
)
def _ensure_secret_increased(frontend_session, before_count):
    json_q = {
        "query": """query listSecrets($input: ListSecretsInput!) {\n
            listSecrets(input: $input) {\n
              start\n
              count\n
              total\n
              secrets {\n
                urn\n
                name\n
              }\n
            }\n
        }""",
        "variables": {"input": {"start": "0", "count": "20"}},
    }

    response = frontend_session.post(
        f"{get_frontend_url()}/api/v2/graphql", json=json_q
    )
    response.raise_for_status()
    res_data = response.json()

    assert res_data
    assert res_data["data"]
    assert res_data["data"]["listSecrets"]["total"] is not None
    assert "errors" not in res_data

    # Assert that there are more secrets now.
    after_count = res_data["data"]["listSecrets"]["total"]
    assert after_count == before_count + 1


@tenacity.retry(
    stop=tenacity.stop_after_attempt(sleep_times), wait=tenacity.wait_fixed(sleep_sec)
)
def _ensure_secret_not_present(frontend_session):
    # Get the secret value back
    json_q = {
        "query": """query getSecretValues($input: GetSecretValuesInput!) {\n
            getSecretValues(input: $input) {\n
              name\n
              value\n
            }\n
        }""",
        "variables": {"input": {"secrets": ["SMOKE_TEST"]}},
    }

    response = frontend_session.post(
        f"{get_frontend_url()}/api/v2/graphql", json=json_q
    )
    response.raise_for_status()
    res_data = response.json()

    assert res_data
    assert res_data["data"]
    assert res_data["data"]["getSecretValues"] is not None
    assert "errors" not in res_data

    secret_values = res_data["data"]["getSecretValues"]
    secret_value_arr = [x for x in secret_values if x["name"] == "SMOKE_TEST"]
    assert len(secret_value_arr) == 0


@tenacity.retry(
    stop=tenacity.stop_after_attempt(sleep_times), wait=tenacity.wait_fixed(sleep_sec)
)
def _ensure_ingestion_source_present(
    frontend_session, ingestion_source_urn, num_execs=None
):
    json_q = {
        "query": """query ingestionSource($urn: String!) {\n
            ingestionSource(urn: $urn) {\n
              executions(start: 0, count: 1) {\n
                  start\n
                  count\n
                  total\n
                  executionRequests {\n
                      urn\n
                  }\n
              }\n
            }\n
        }""",
        "variables": {"urn": ingestion_source_urn},
    }

    response = frontend_session.post(
        f"{get_frontend_url()}/api/v2/graphql", json=json_q
    )
    response.raise_for_status()
    res_data = response.json()

    assert res_data
    assert res_data["data"]
    assert res_data["data"]["ingestionSource"] is not None
    assert "errors" not in res_data

    if num_execs is not None:
        ingestion_source = res_data["data"]["ingestionSource"]
        assert ingestion_source["executions"]["total"] == num_execs

    return res_data


@tenacity.retry(
    stop=tenacity.stop_after_attempt(sleep_times), wait=tenacity.wait_fixed(sleep_sec)
)
def _ensure_execution_request_present(frontend_session, execution_request_urn):
    json_q = {
        "query": """query executionRequest($urn: String!) {\n
            executionRequest(urn: $urn) {\n
              urn\n
              input {\n
                task\n
                arguments {\n
                  key\n
                  value\n
                }\n
              }\n
              result {\n
                  status\n
                  startTimeMs\n
                  durationMs\n
              }\n
            }\n
        }""",
        "variables": {"urn": execution_request_urn},
    }

    response = frontend_session.post(
        f"{get_frontend_url()}/api/v2/graphql", json=json_q
    )
    response.raise_for_status()
    res_data = response.json()

    assert res_data
    assert res_data["data"]
    assert res_data["data"]["executionRequest"] is not None
    assert "errors" not in res_data
    return res_data


@pytest.mark.dependency(depends=["test_healthchecks"])
def test_create_list_get_remove_secret(frontend_session):

    # Get count of existing secrets
    json_q = {
        "query": """query listSecrets($input: ListSecretsInput!) {\n
            listSecrets(input: $input) {\n
              start\n
              count\n
              total\n
              secrets {\n
                urn\n
                name\n
              }\n
            }\n
        }""",
        "variables": {"input": {"start": "0", "count": "20"}},
    }

    response = frontend_session.post(
        f"{get_frontend_url()}/api/v2/graphql", json=json_q
    )
    response.raise_for_status()
    res_data = response.json()

    assert res_data
    assert res_data["data"]
    assert res_data["data"]["listSecrets"]["total"] is not None
    assert "errors" not in res_data

    before_count = res_data["data"]["listSecrets"]["total"]

    # Create new secret
    json_q = {
        "query": """mutation createSecret($input: CreateSecretInput!) {\n
            createSecret(input: $input)
        }""",
        "variables": {"input": {"name": "SMOKE_TEST", "value": "mytestvalue"}},
    }

    response = frontend_session.post(
        f"{get_frontend_url()}/api/v2/graphql", json=json_q
    )
    response.raise_for_status()
    res_data = response.json()

    assert res_data
    assert res_data["data"]
    assert res_data["data"]["createSecret"] is not None
    assert "errors" not in res_data

    secret_urn = res_data["data"]["createSecret"]

    # Get new count of secrets
    _ensure_secret_increased(frontend_session, before_count)

    # Get the secret value back
    json_q = {
        "query": """query getSecretValues($input: GetSecretValuesInput!) {\n
            getSecretValues(input: $input) {\n
              name\n
              value\n
            }\n
        }""",
        "variables": {"input": {"secrets": ["SMOKE_TEST"]}},
    }

    response = frontend_session.post(
        f"{get_frontend_url()}/api/v2/graphql", json=json_q
    )
    response.raise_for_status()
    res_data = response.json()

    print(res_data)
    assert res_data
    assert res_data["data"]
    assert res_data["data"]["getSecretValues"] is not None
    assert "errors" not in res_data

    secret_values = res_data["data"]["getSecretValues"]
    secret_value = [x for x in secret_values if x["name"] == "SMOKE_TEST"][0]
    assert secret_value["value"] == "mytestvalue"

    # Now cleanup and remove the secret
    json_q = {
        "query": """mutation deleteSecret($urn: String!) {\n
            deleteSecret(urn: $urn)
        }""",
        "variables": {"urn": secret_urn},
    }

    response = frontend_session.post(
        f"{get_frontend_url()}/api/v2/graphql", json=json_q
    )
    response.raise_for_status()
    res_data = response.json()

    assert res_data
    assert res_data["data"]
    assert res_data["data"]["deleteSecret"] is not None
    assert "errors" not in res_data

    # Re-fetch the secret values and see that they are not there.
    _ensure_secret_not_present(frontend_session)


@pytest.mark.dependency(depends=["test_healthchecks"])
def test_create_list_get_remove_ingestion_source(frontend_session):

    # Get count of existing ingestion sources
    res_data = _get_ingestionSources(frontend_session)

    before_count = res_data["data"]["listIngestionSources"]["total"]

    # Create new ingestion source
    json_q = {
        "query": """mutation createIngestionSource($input: UpdateIngestionSourceInput!) {\n
            createIngestionSource(input: $input)
        }""",
        "variables": {
            "input": {
                "name": "My Test Ingestion Source",
                "type": "mysql",
                "description": "My ingestion source description",
                "schedule": {"interval": "*/5 * * * *", "timezone": "UTC"},
                "config": {
                    "recipe": '{"source":{"type":"mysql","config":{"include_tables":true,"database":null,"password":"${MYSQL_PASSWORD}","profiling":{"enabled":false},"host_port":null,"include_views":true,"username":"${MYSQL_USERNAME}"}},"pipeline_name":"urn:li:dataHubIngestionSource:f38bd060-4ea8-459c-8f24-a773286a2927"}',
                    "version": "0.8.18",
                    "executorId": "mytestexecutor",
                },
            }
        },
    }

    response = frontend_session.post(
        f"{get_frontend_url()}/api/v2/graphql", json=json_q
    )
    response.raise_for_status()
    res_data = response.json()

    assert res_data
    assert res_data["data"]
    assert res_data["data"]["createIngestionSource"] is not None
    assert "errors" not in res_data

    ingestion_source_urn = res_data["data"]["createIngestionSource"]

    # Assert that there are more ingestion sources now.
    after_count = _ensure_ingestion_source_count(frontend_session, before_count + 1)

    # Get the ingestion source back
    json_q = {
        "query": """query ingestionSource($urn: String!) {\n
            ingestionSource(urn: $urn) {\n
              urn\n
              type\n
              name\n
              schedule {\n
                timezone\n
                interval\n
              }\n
              config {\n
                recipe\n
                executorId\n
                version\n
              }\n
            }\n
        }""",
        "variables": {"urn": ingestion_source_urn},
    }

    response = frontend_session.post(
        f"{get_frontend_url()}/api/v2/graphql", json=json_q
    )
    response.raise_for_status()
    res_data = response.json()

    assert res_data
    assert res_data["data"]
    assert res_data["data"]["ingestionSource"] is not None
    assert "errors" not in res_data

    ingestion_source = res_data["data"]["ingestionSource"]
    assert ingestion_source["urn"] == ingestion_source_urn
    assert ingestion_source["type"] == "mysql"
    assert ingestion_source["name"] == "My Test Ingestion Source"
    assert ingestion_source["schedule"]["interval"] == "*/5 * * * *"
    assert ingestion_source["schedule"]["timezone"] == "UTC"
    assert (
        ingestion_source["config"]["recipe"]
        == '{"source":{"type":"mysql","config":{"include_tables":true,"database":null,"password":"${MYSQL_PASSWORD}","profiling":{"enabled":false},"host_port":null,"include_views":true,"username":"${MYSQL_USERNAME}"}},"pipeline_name":"urn:li:dataHubIngestionSource:f38bd060-4ea8-459c-8f24-a773286a2927"}'
    )
    assert ingestion_source["config"]["executorId"] == "mytestexecutor"
    assert ingestion_source["config"]["version"] == "0.8.18"

    # Now cleanup and remove the ingestion source
    json_q = {
        "query": """mutation deleteIngestionSource($urn: String!) {\n
            deleteIngestionSource(urn: $urn)
        }""",
        "variables": {"urn": ingestion_source_urn},
    }

    response = frontend_session.post(
        f"{get_frontend_url()}/api/v2/graphql", json=json_q
    )
    response.raise_for_status()
    res_data = response.json()

    assert res_data
    print(res_data)
    assert res_data["data"]
    assert res_data["data"]["deleteIngestionSource"] is not None
    assert "errors" not in res_data

    # Ensure the ingestion source has been removed.
    _ensure_ingestion_source_count(frontend_session, after_count - 1)


@pytest.mark.dependency(
    depends=[
        "test_healthchecks",
        "test_create_list_get_remove_ingestion_source",
    ]
)
def test_create_list_get_ingestion_execution_request(frontend_session):
    # Create new ingestion source
    json_q = {
        "query": """mutation createIngestionSource($input: UpdateIngestionSourceInput!) {\n
            createIngestionSource(input: $input)
        }""",
        "variables": {
            "input": {
                "name": "My Test Ingestion Source",
                "type": "mysql",
                "description": "My ingestion source description",
                "schedule": {"interval": "*/5 * * * *", "timezone": "UTC"},
                "config": {
                    "recipe": '{"source":{"type":"mysql","config":{"include_tables":true,"database":null,"password":"${MYSQL_PASSWORD}","profiling":{"enabled":false},"host_port":null,"include_views":true,"username":"${MYSQL_USERNAME}"}},"pipeline_name":"urn:li:dataHubIngestionSource:f38bd060-4ea8-459c-8f24-a773286a2927"}',
                    "version": "0.8.18",
                    "executorId": "mytestexecutor",
                },
            }
        },
    }

    response = frontend_session.post(
        f"{get_frontend_url()}/api/v2/graphql", json=json_q
    )
    response.raise_for_status()
    res_data = response.json()

    assert res_data
    assert res_data["data"]
    assert res_data["data"]["createIngestionSource"] is not None
    assert "errors" not in res_data

    ingestion_source_urn = res_data["data"]["createIngestionSource"]

    # Create a request to execute the ingestion source
    json_q = {
        "query": """mutation createIngestionExecutionRequest($input: CreateIngestionExecutionRequestInput!) {\n
            createIngestionExecutionRequest(input: $input)
        }""",
        "variables": {"input": {"ingestionSourceUrn": ingestion_source_urn}},
    }

    response = frontend_session.post(
        f"{get_frontend_url()}/api/v2/graphql", json=json_q
    )
    response.raise_for_status()
    res_data = response.json()

    assert res_data
    assert res_data["data"]
    assert (
        res_data["data"]["createIngestionExecutionRequest"] is not None
    ), f"res_data was {res_data}"
    assert "errors" not in res_data

    execution_request_urn = res_data["data"]["createIngestionExecutionRequest"]

    res_data = _ensure_ingestion_source_present(
        frontend_session, ingestion_source_urn, 1
    )

    ingestion_source = res_data["data"]["ingestionSource"]

    assert (
        ingestion_source["executions"]["executionRequests"][0]["urn"]
        == execution_request_urn
    )

    # Get the ingestion request back via direct lookup
    res_data = _ensure_execution_request_present(
        frontend_session, execution_request_urn
    )

    execution_request = res_data["data"]["executionRequest"]
    assert execution_request["urn"] == execution_request_urn

    # Verify input
    assert execution_request["input"]["task"] == "RUN_INGEST"
    assert len(execution_request["input"]["arguments"]) == 3
    assert execution_request["input"]["arguments"][0]["key"] == "recipe"
    assert (
        json.loads(execution_request["input"]["arguments"][0]["value"])["source"]
        == json.loads(
            '{"source":{"type":"mysql","config":{"include_tables":true,"database":null,"password":"${MYSQL_PASSWORD}","profiling":{"enabled":false},"host_port":null,"include_views":true,"username":"${MYSQL_USERNAME}"}},"pipeline_name":"urn:li:dataHubIngestionSource:f38bd060-4ea8-459c-8f24-a773286a2927"}'
        )["source"]
    )
    assert execution_request["input"]["arguments"][1]["key"] == "version"
    assert execution_request["input"]["arguments"][1]["value"] == "0.8.18"
    assert execution_request["input"]["arguments"][2] == {
        "key": "debug_mode",
        "value": "false",
    }

    # Verify no result
    assert execution_request["result"] is None

    # Now cleanup and remove the ingestion source
    json_q = {
        "query": """mutation deleteIngestionSource($urn: String!) {\n
            deleteIngestionSource(urn: $urn)
        }""",
        "variables": {"urn": ingestion_source_urn},
    }

    response = frontend_session.post(
        f"{get_frontend_url()}/api/v2/graphql", json=json_q
    )
    response.raise_for_status()
    res_data = response.json()

    assert res_data
    assert res_data["data"]
    assert res_data["data"]["deleteIngestionSource"] is not None
    assert "errors" not in res_data
