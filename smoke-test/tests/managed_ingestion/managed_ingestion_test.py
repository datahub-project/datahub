import json

import pytest
import tenacity

from tests.utils import execute_graphql_mutation, execute_graphql_query, get_sleep_info

sleep_sec, sleep_times = get_sleep_info()


def _get_ingestionSources(auth_session):
    list_ingestion_sources_query = """query listIngestionSources($input: ListIngestionSourcesInput!) {
        listIngestionSources(input: $input) {
          start
          count
          total
          ingestionSources {
            urn
          }
        }
    }"""

    res_data = execute_graphql_query(
        auth_session,
        list_ingestion_sources_query,
        variables={"input": {"start": 0, "count": 20}},
        expected_data_key="listIngestionSources",
    )
    return res_data


@tenacity.retry(
    stop=tenacity.stop_after_attempt(sleep_times), wait=tenacity.wait_fixed(sleep_sec)
)
def _ensure_ingestion_source_count(auth_session, expected_count):
    res_data = _get_ingestionSources(auth_session)
    after_count = res_data["data"]["listIngestionSources"]["total"]
    assert after_count == expected_count
    return after_count


@tenacity.retry(
    stop=tenacity.stop_after_attempt(sleep_times), wait=tenacity.wait_fixed(sleep_sec)
)
def _ensure_secret_increased(auth_session, before_count):
    list_secrets_query = """query listSecrets($input: ListSecretsInput!) {
        listSecrets(input: $input) {
          start
          count
          total
          secrets {
            urn
            name
          }
        }
    }"""

    res_data = execute_graphql_query(
        auth_session,
        list_secrets_query,
        variables={"input": {"start": 0, "count": 20}},
        expected_data_key="listSecrets",
    )

    # Assert that there are more secrets now.
    after_count = res_data["data"]["listSecrets"]["total"]
    assert after_count == before_count + 1


@tenacity.retry(
    stop=tenacity.stop_after_attempt(sleep_times), wait=tenacity.wait_fixed(sleep_sec)
)
def _ensure_secret_not_present(auth_session):
    # Get the secret value back
    get_secret_values_query = """query getSecretValues($input: GetSecretValuesInput!) {
        getSecretValues(input: $input) {
          name
          value
        }
    }"""

    res_data = execute_graphql_query(
        auth_session,
        get_secret_values_query,
        variables={"input": {"secrets": ["SMOKE_TEST"]}},
        expected_data_key="getSecretValues",
    )

    secret_values = res_data["data"]["getSecretValues"]
    secret_value_arr = [x for x in secret_values if x["name"] == "SMOKE_TEST"]
    assert len(secret_value_arr) == 0


@tenacity.retry(
    stop=tenacity.stop_after_attempt(sleep_times), wait=tenacity.wait_fixed(sleep_sec)
)
def _ensure_ingestion_source_present(
    auth_session, ingestion_source_urn, num_execs=None
):
    ingestion_source_query = """query ingestionSource($urn: String!) {
            ingestionSource(urn: $urn) {
              executions(start: 0, count: 1) {
                  start
                  count
                  total
                  executionRequests {
                      urn
                  }
              }
            }
        }"""

    res_data = execute_graphql_query(
        auth_session,
        ingestion_source_query,
        variables={"urn": ingestion_source_urn},
        expected_data_key="ingestionSource",
    )
    print(res_data)

    if num_execs is not None:
        ingestion_source = res_data["data"]["ingestionSource"]
        assert ingestion_source["executions"]["total"] >= num_execs

    return res_data


@tenacity.retry(
    stop=tenacity.stop_after_attempt(sleep_times), wait=tenacity.wait_fixed(sleep_sec)
)
def _ensure_execution_request_present(auth_session, execution_request_urn):
    execution_request_query = """query executionRequest($urn: String!) {
            executionRequest(urn: $urn) {
              urn
              input {
                task
                arguments {
                  key
                  value
                }
              }
              result {
                  status
                  startTimeMs
                  durationMs
              }
            }
        }"""

    res_data = execute_graphql_query(
        auth_session,
        execution_request_query,
        variables={"urn": execution_request_urn},
        expected_data_key="executionRequest",
    )
    return res_data


def test_create_list_get_remove_secret(auth_session):
    # Get count of existing secrets
    list_secrets_query = """query listSecrets($input: ListSecretsInput!) {
        listSecrets(input: $input) {
          start
          count
          total
          secrets {
            urn
            name
          }
        }
    }"""

    res_data = execute_graphql_query(
        auth_session,
        list_secrets_query,
        variables={"input": {"start": 0, "count": 20}},
        expected_data_key="listSecrets",
    )

    before_count = res_data["data"]["listSecrets"]["total"]

    # Create new secret
    create_secret_mutation = """mutation createSecret($input: CreateSecretInput!) {
        createSecret(input: $input)
    }"""

    res_data = execute_graphql_mutation(
        auth_session,
        create_secret_mutation,
        {"input": {"name": "SMOKE_TEST", "value": "mytestvalue"}},
        "createSecret",
    )

    secret_urn = res_data["data"]["createSecret"]

    # Get new count of secrets
    _ensure_secret_increased(auth_session, before_count)

    # Update existing secret
    update_secret_mutation = """mutation updateSecret($input: UpdateSecretInput!) {
            updateSecret(input: $input)
        }"""

    res_data = execute_graphql_mutation(
        auth_session,
        update_secret_mutation,
        {
            "input": {
                "urn": secret_urn,
                "name": "SMOKE_TEST",
                "value": "mytestvalue.updated",
            }
        },
        "updateSecret",
    )

    secret_urn = res_data["data"]["updateSecret"]

    # Get the secret value back
    get_secret_values_query = """query getSecretValues($input: GetSecretValuesInput!) {
            getSecretValues(input: $input) {
              name
              value
            }
        }"""

    res_data = execute_graphql_query(
        auth_session,
        get_secret_values_query,
        variables={"input": {"secrets": ["SMOKE_TEST"]}},
        expected_data_key="getSecretValues",
    )
    print(res_data)

    secret_values = res_data["data"]["getSecretValues"]
    secret_value = [x for x in secret_values if x["name"] == "SMOKE_TEST"][0]
    assert secret_value["value"] == "mytestvalue.updated"

    # Now cleanup and remove the secret
    delete_secret_mutation = """mutation deleteSecret($urn: String!) {
            deleteSecret(urn: $urn)
        }"""

    res_data = execute_graphql_mutation(
        auth_session, delete_secret_mutation, {"urn": secret_urn}, "deleteSecret"
    )

    # Re-fetch the secret values and see that they are not there.
    _ensure_secret_not_present(auth_session)


@pytest.mark.dependency()
def test_create_list_get_remove_ingestion_source(auth_session):
    # Get count of existing ingestion sources
    res_data = _get_ingestionSources(auth_session)

    before_count = res_data["data"]["listIngestionSources"]["total"]

    # Create new ingestion source
    create_ingestion_source_mutation = """mutation createIngestionSource($input: UpdateIngestionSourceInput!) {
            createIngestionSource(input: $input)
        }"""

    create_ingestion_source_variables = {
        "input": {
            "name": "My Test Ingestion Source",
            "type": "mysql",
            "description": "My ingestion source description",
            "schedule": {"interval": "*/60 * * * *", "timezone": "UTC"},
            "config": {
                "recipe": '{"source":{"type":"mysql","config":{"include_tables":true,"database":null,"password":"${MYSQL_PASSWORD}","profiling":{"enabled":false},"host_port":null,"include_views":true,"username":"${MYSQL_USERNAME}"}},"pipeline_name":"urn:li:dataHubIngestionSource:f38bd060-4ea8-459c-8f24-a773286a2927"}',
                "version": "0.8.18",
                "executorId": "mytestexecutor",
            },
        }
    }

    res_data = execute_graphql_mutation(
        auth_session,
        create_ingestion_source_mutation,
        create_ingestion_source_variables,
        "createIngestionSource",
    )

    ingestion_source_urn = res_data["data"]["createIngestionSource"]

    # Assert that there are more ingestion sources now.
    after_count = _ensure_ingestion_source_count(auth_session, before_count + 1)

    # Get the ingestion source back
    get_ingestion_source_query = """query ingestionSource($urn: String!) {
            ingestionSource(urn: $urn) {
              urn
              type
              name
              schedule {
                timezone
                interval
              }
              config {
                recipe
                executorId
                version
              }
            }
        }"""

    res_data = execute_graphql_query(
        auth_session,
        get_ingestion_source_query,
        variables={"urn": ingestion_source_urn},
        expected_data_key="ingestionSource",
    )

    ingestion_source = res_data["data"]["ingestionSource"]
    assert ingestion_source["urn"] == ingestion_source_urn
    assert ingestion_source["type"] == "mysql"
    assert ingestion_source["name"] == "My Test Ingestion Source"
    assert ingestion_source["schedule"]["interval"] == "*/60 * * * *"
    assert ingestion_source["schedule"]["timezone"] == "UTC"
    assert (
        ingestion_source["config"]["recipe"]
        == '{"source":{"type":"mysql","config":{"include_tables":true,"database":null,"password":"${MYSQL_PASSWORD}","profiling":{"enabled":false},"host_port":null,"include_views":true,"username":"${MYSQL_USERNAME}"}},"pipeline_name":"urn:li:dataHubIngestionSource:f38bd060-4ea8-459c-8f24-a773286a2927"}'
    )
    assert ingestion_source["config"]["executorId"] == "mytestexecutor"
    assert ingestion_source["config"]["version"] == "0.8.18"

    # Now cleanup and remove the ingestion source
    delete_ingestion_source_mutation = """mutation deleteIngestionSource($urn: String!) {
            deleteIngestionSource(urn: $urn)
        }"""

    res_data = execute_graphql_mutation(
        auth_session,
        delete_ingestion_source_mutation,
        {"urn": ingestion_source_urn},
        "deleteIngestionSource",
    )
    print(res_data)

    # Ensure the ingestion source has been removed.
    _ensure_ingestion_source_count(auth_session, after_count - 1)


@pytest.mark.dependency(
    depends=[
        "test_create_list_get_remove_ingestion_source",
    ]
)
def test_create_list_get_ingestion_execution_request(auth_session):
    # Create new ingestion source
    create_ingestion_source_mutation = """mutation createIngestionSource($input: UpdateIngestionSourceInput!) {
            createIngestionSource(input: $input)
        }"""

    create_ingestion_source_variables = {
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
    }

    res_data = execute_graphql_mutation(
        auth_session,
        create_ingestion_source_mutation,
        create_ingestion_source_variables,
        "createIngestionSource",
    )

    ingestion_source_urn = res_data["data"]["createIngestionSource"]

    # Create a request to execute the ingestion source
    create_execution_request_mutation = """mutation createIngestionExecutionRequest($input: CreateIngestionExecutionRequestInput!) {
            createIngestionExecutionRequest(input: $input)
        }"""

    res_data = execute_graphql_mutation(
        auth_session,
        create_execution_request_mutation,
        {"input": {"ingestionSourceUrn": ingestion_source_urn}},
        "createIngestionExecutionRequest",
    )

    assert res_data["data"]["createIngestionExecutionRequest"] is not None, (
        f"res_data was {res_data}"
    )

    execution_request_urn = res_data["data"]["createIngestionExecutionRequest"]

    res_data = _ensure_ingestion_source_present(auth_session, ingestion_source_urn, 1)

    ingestion_source = res_data["data"]["ingestionSource"]

    assert (
        ingestion_source["executions"]["executionRequests"][0]["urn"]
        == execution_request_urn
    )

    # Get the ingestion request back via direct lookup
    res_data = _ensure_execution_request_present(auth_session, execution_request_urn)

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
    delete_ingestion_source_mutation = """mutation deleteIngestionSource($urn: String!) {
            deleteIngestionSource(urn: $urn)
        }"""

    res_data = execute_graphql_mutation(
        auth_session,
        delete_ingestion_source_mutation,
        {"urn": ingestion_source_urn},
        "deleteIngestionSource",
    )
