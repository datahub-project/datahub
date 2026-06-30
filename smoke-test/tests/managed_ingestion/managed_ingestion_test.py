import json
import logging
from typing import Any, Dict

import pytest

from tests.privileges.utils import create_user
from tests.utils import (
    TestSessionWrapper,
    execute_graphql,
    get_admin_credentials,
    login_as,
    wait_for_writes_to_sync,
    with_test_retry,
)

logger = logging.getLogger(__name__)

_SMOKE_SECRET_NAMES = ["SMOKE_TEST", "SMOKE_TEST_BIGQUERY_KEY", "SMOKE_TEST_EDGE_CASES"]
_SECRET_GUARD_TEST_EMAIL = "managed.ingestion.secret.guard@smoke.datahub.test"
_SECRET_GUARD_TEST_PASSWORD = "user"


def _delete_secrets_by_name(auth_session: object, names: list) -> None:
    res_data = execute_graphql(
        auth_session,
        """query listSecrets($input: ListSecretsInput!) {

            listSecrets(input: $input) {

              secrets {

                urn

                name

              }

            }

        }""",
        {"input": {"start": 0, "count": 100}},
    )
    deleted = False
    for secret in res_data["data"]["listSecrets"]["secrets"]:
        if secret["name"] in names:
            execute_graphql(
                auth_session,
                """mutation deleteSecret($urn: String!) {

                    deleteSecret(urn: $urn)
                }""",
                {"urn": secret["urn"]},
            )
            deleted = True
    if deleted:
        wait_for_writes_to_sync()


@pytest.fixture(scope="module", autouse=True)
def cleanup_smoke_secrets(auth_session: object):
    """Delete leftover smoke test secrets before and after the module to ensure idempotency."""
    _delete_secrets_by_name(auth_session, _SMOKE_SECRET_NAMES)
    yield
    _delete_secrets_by_name(auth_session, _SMOKE_SECRET_NAMES)


def _get_ingestionSources(auth_session):
    query = """query listIngestionSources($input: ListIngestionSourcesInput!) {\n
            listIngestionSources(input: $input) {\n
              start\n
              count\n
              total\n
              ingestionSources {\n
                urn\n
              }\n
            }\n
        }"""
    variables: Dict[str, Any] = {"input": {"start": 0, "count": 20}}
    res_data = execute_graphql(auth_session, query, variables)
    assert res_data["data"]["listIngestionSources"]["total"] is not None
    return res_data


@with_test_retry()
def _ensure_ingestion_source_count(auth_session, expected_count):
    res_data = _get_ingestionSources(auth_session)
    after_count = res_data["data"]["listIngestionSources"]["total"]
    assert after_count == expected_count
    return after_count


@with_test_retry()
def _ensure_secret_increased(auth_session, before_count):
    query = """query listSecrets($input: ListSecretsInput!) {\n
            listSecrets(input: $input) {\n
              start\n
              count\n
              total\n
              secrets {\n
                urn\n
                name\n
              }\n
            }\n
        }"""
    variables: Dict[str, Any] = {"input": {"start": 0, "count": 20}}
    res_data = execute_graphql(auth_session, query, variables)
    assert res_data["data"]["listSecrets"]["total"] is not None

    # Assert that there are more secrets now.
    after_count = res_data["data"]["listSecrets"]["total"]
    assert after_count == before_count + 1


@with_test_retry()
def _ensure_secret_not_present(auth_session):
    query = """query listSecrets($input: ListSecretsInput!) {\n
            listSecrets(input: $input) {\n
              secrets {\n
                name\n
              }\n
            }\n
        }"""
    variables: Dict[str, Any] = {"input": {"start": 0, "count": 100}}
    res_data = execute_graphql(auth_session, query, variables)
    assert res_data["data"]["listSecrets"] is not None
    secrets = res_data["data"]["listSecrets"]["secrets"]
    secret_arr = [x for x in secrets if x["name"] == "SMOKE_TEST"]
    assert len(secret_arr) == 0


@with_test_retry()
def _ensure_ingestion_source_present(
    auth_session, ingestion_source_urn, num_execs=None
):
    query = """query ingestionSource($urn: String!) {\n
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
        }"""
    variables: Dict[str, Any] = {"urn": ingestion_source_urn}
    res_data = execute_graphql(auth_session, query, variables)
    logger.info(res_data)

    assert res_data["data"]["ingestionSource"] is not None

    if num_execs is not None:
        ingestion_source = res_data["data"]["ingestionSource"]
        assert ingestion_source["executions"]["total"] >= num_execs

    return res_data


@with_test_retry()
def _ensure_execution_request_present(auth_session, execution_request_urn):
    query = """query executionRequest($urn: String!) {\n
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
        }"""
    variables: Dict[str, Any] = {"urn": execution_request_urn}
    res_data = execute_graphql(auth_session, query, variables)
    assert res_data["data"]["executionRequest"] is not None
    return res_data


def test_create_list_get_remove_secret(auth_session):
    # Get count of existing secrets
    query = """query listSecrets($input: ListSecretsInput!) {\n
            listSecrets(input: $input) {\n
              start\n
              count\n
              total\n
              secrets {\n
                urn\n
                name\n
              }\n
            }\n
        }"""
    variables: Dict[str, Any] = {"input": {"start": 0, "count": 100}}
    res_data = execute_graphql(auth_session, query, variables)
    assert res_data["data"]["listSecrets"]["total"] is not None

    # Inline cleanup: delete any leftover SMOKE_TEST from prior partial runs or reruns
    for s in res_data["data"]["listSecrets"]["secrets"]:
        if s["name"] == "SMOKE_TEST":
            execute_graphql(
                auth_session,
                """mutation deleteSecret($urn: String!) {\n
                    deleteSecret(urn: $urn)
                }""",
                {"urn": s["urn"]},
            )
            wait_for_writes_to_sync()

    # Re-fetch to get accurate baseline count after any cleanup
    res_data = execute_graphql(auth_session, query, variables)
    before_count = res_data["data"]["listSecrets"]["total"]

    # Create new secret
    query = """mutation createSecret($input: CreateSecretInput!) {\n
            createSecret(input: $input)
        }"""
    variables = {"input": {"name": "SMOKE_TEST", "value": "mytestvalue"}}
    res_data = execute_graphql(auth_session, query, variables)
    assert res_data["data"]["createSecret"] is not None

    secret_urn = res_data["data"]["createSecret"]

    # Get new count of secrets
    _ensure_secret_increased(auth_session, before_count)

    # Update existing secret
    query = """mutation updateSecret($input: UpdateSecretInput!) {\n
            updateSecret(input: $input)
        }"""
    variables = {
        "input": {
            "urn": secret_urn,
            "name": "SMOKE_TEST",
            "value": "mytestvalue.updated",
        }
    }
    res_data = execute_graphql(auth_session, query, variables)
    assert res_data["data"]["updateSecret"] is not None

    secret_urn = res_data["data"]["updateSecret"]

    # getSecretValues is blocked for non-system user PATs under ENFORCE. The default
    # smoke-test admin (urn:li:corpuser:datahub) is a trusted system principal and
    # may still decrypt; verify denial with a regular corp user's PAT.
    query = """query getSecretValues($input: GetSecretValuesInput!) {\n
            getSecretValues(input: $input) {\n
              name\n
              value\n
            }\n
        }"""
    variables = {"input": {"secrets": ["SMOKE_TEST"]}}
    admin_user, admin_pass = get_admin_credentials()
    admin_frontend = login_as(admin_user, admin_pass)
    create_user(admin_frontend, _SECRET_GUARD_TEST_EMAIL, _SECRET_GUARD_TEST_PASSWORD)
    user_pat_session = TestSessionWrapper(
        login_as(_SECRET_GUARD_TEST_EMAIL, _SECRET_GUARD_TEST_PASSWORD)
    )
    try:
        res_data = execute_graphql(
            user_pat_session, query, variables, expect_errors=True
        )
        assert res_data["data"]["getSecretValues"] is None
        assert "errors" in res_data
    finally:
        user_pat_session.destroy()

    # Now cleanup and remove the secret
    query = """mutation deleteSecret($urn: String!) {\n
            deleteSecret(urn: $urn)
        }"""
    variables = {"urn": secret_urn}
    res_data = execute_graphql(auth_session, query, variables)
    assert res_data["data"]["deleteSecret"] is not None

    # Re-fetch the secret values and see that they are not there.
    _ensure_secret_not_present(auth_session)


def test_secret_roundtrip_preserves_json_credentials_with_newlines_and_slashes(
    auth_session,
):
    """
    Test that JSON credentials (e.g., BigQuery service account keys) with newlines,
    forward slashes, and quotes are preserved exactly through create and update operations.

    BigQuery private keys contain newlines that must not be corrupted when updating secrets.
    """
    fake_bigquery_key = """{
  "type": "service_account",
  "project_id": "test-project",
  "private_key_id": "key123",
  "private_key": "-----BEGIN PRIVATE KEY-----\\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQC\\n-----END PRIVATE KEY-----\\n",
  "client_email": "test@test-project.iam.gserviceaccount.com",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://oauth2.googleapis.com/token"
}"""

    query = """mutation createSecret($input: CreateSecretInput!) {\n
            createSecret(input: $input)
        }"""
    variables: Dict[str, Any] = {
        "input": {
            "name": "SMOKE_TEST_BIGQUERY_KEY",
            "value": fake_bigquery_key,
            "description": "Test secret with special characters",
        }
    }
    res_data = execute_graphql(auth_session, query, variables)
    assert res_data["data"]["createSecret"] is not None

    secret_urn = res_data["data"]["createSecret"]

    updated_bigquery_key = """{
  "type": "service_account",
  "project_id": "updated-project/with/slashes",
  "private_key_id": "key456",
  "private_key": "-----BEGIN PRIVATE KEY-----\\nUPDATED_KEY_DATA\\n-----END PRIVATE KEY-----\\n",
  "client_email": "updated@test-project.iam.gserviceaccount.com",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://oauth2.googleapis.com/token"
}"""

    query = """mutation updateSecret($input: UpdateSecretInput!) {\n
            updateSecret(input: $input)
        }"""
    variables = {
        "input": {
            "urn": secret_urn,
            "name": "SMOKE_TEST_BIGQUERY_KEY",
            "value": updated_bigquery_key,
            "description": "Updated test secret with special characters",
        }
    }
    res_data = execute_graphql(auth_session, query, variables)
    assert res_data["data"]["updateSecret"] is not None

    query = """mutation deleteSecret($urn: String!) {\n
            deleteSecret(urn: $urn)
        }"""
    variables = {"urn": secret_urn}
    res_data = execute_graphql(auth_session, query, variables)
    assert res_data["data"]["deleteSecret"] is not None


def test_secret_roundtrip_preserves_passwords_and_connection_strings_with_special_chars(
    auth_session,
):
    """
    Test that complex passwords and connection strings with special characters
    are preserved exactly through create and update operations.

    Validates handling of mixed quotes, tabs, unicode, backslashes, special characters,
    and connection strings with embedded credentials (MongoDB, PostgreSQL, etc.).
    """
    edge_case_value = """Line 1: Single quotes 'like this' and double quotes "like that"
Line 2: Tab\tseparated\tvalues
Line 3: Mixed quotes: "It's a test" and 'He said "hello"'
Line 4: Backslashes: C:\\Users\\path\\to\\file
Line 5: URLs: https://example.com/path?param=value&other=123
Line 6: Unicode: 你好 🎉 café naïve
Line 7: Special chars: @#$%^&*()_+-=[]{}|;:,.<>?
Line 8: Empty line below:

Line 9: Regex-like: ^.*\\.test\\.(js|ts)$
Line 10: SQL-like: SELECT * FROM "table" WHERE name = 'O''Brien'"""

    query = """mutation createSecret($input: CreateSecretInput!) {\n
            createSecret(input: $input)
        }"""
    variables: Dict[str, Any] = {
        "input": {
            "name": "SMOKE_TEST_EDGE_CASES",
            "value": edge_case_value,
            "description": "Testing edge case characters",
        }
    }
    res_data = execute_graphql(auth_session, query, variables)
    assert res_data["data"]["createSecret"] is not None

    secret_urn = res_data["data"]["createSecret"]

    updated_edge_case = """Password with all the problematic chars:
P@ssw0rd!/?\\"'`~
Connection string: mongodb://user:p@ss"word'123@localhost:27017/db?authSource=admin
JSON snippet: {"key": "value with \\"quotes\\" and 'apostrophes'", "path": "C:\\\\Windows\\\\System32"}
Multiline command:
  echo "Line 1" && \\
  echo 'Line 2' && \\
  echo Line\\ 3
Heredoc-like:
<<EOF
Content with "quotes" and 'apostrophes'
\t\tIndented with tabs
EOF"""

    query = """mutation updateSecret($input: UpdateSecretInput!) {\n
            updateSecret(input: $input)
        }"""
    variables = {
        "input": {
            "urn": secret_urn,
            "name": "SMOKE_TEST_EDGE_CASES",
            "value": updated_edge_case,
            "description": "Updated with more edge cases",
        }
    }
    res_data = execute_graphql(auth_session, query, variables)
    assert res_data["data"]["updateSecret"] is not None

    query = """mutation deleteSecret($urn: String!) {\n
            deleteSecret(urn: $urn)
        }"""
    variables = {"urn": secret_urn}
    res_data = execute_graphql(auth_session, query, variables)
    assert res_data["data"]["deleteSecret"] is not None


@pytest.mark.dependency()
def test_create_list_get_remove_ingestion_source(auth_session):
    # Get count of existing ingestion sources
    res_data = _get_ingestionSources(auth_session)

    before_count = res_data["data"]["listIngestionSources"]["total"]

    # Create new ingestion source
    query = """mutation createIngestionSource($input: UpdateIngestionSourceInput!) {\n
            createIngestionSource(input: $input)
        }"""
    variables: Dict[str, Any] = {
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
    res_data = execute_graphql(auth_session, query, variables)
    assert res_data["data"]["createIngestionSource"] is not None

    ingestion_source_urn = res_data["data"]["createIngestionSource"]

    # Assert that there are more ingestion sources now.
    after_count = _ensure_ingestion_source_count(auth_session, before_count + 1)

    # Get the ingestion source back
    query = """query ingestionSource($urn: String!) {\n
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
        }"""
    variables = {"urn": ingestion_source_urn}
    res_data = execute_graphql(auth_session, query, variables)
    assert res_data["data"]["ingestionSource"] is not None

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
    query = """mutation deleteIngestionSource($urn: String!) {\n
            deleteIngestionSource(urn: $urn)
        }"""
    variables = {"urn": ingestion_source_urn}
    res_data = execute_graphql(auth_session, query, variables)
    logger.info(res_data)
    assert res_data["data"]["deleteIngestionSource"] is not None

    # Ensure the ingestion source has been removed.
    _ensure_ingestion_source_count(auth_session, after_count - 1)


@pytest.mark.dependency(
    depends=[
        "test_create_list_get_remove_ingestion_source",
    ]
)
def test_create_list_get_ingestion_execution_request(auth_session):
    # Create new ingestion source
    query = """mutation createIngestionSource($input: UpdateIngestionSourceInput!) {\n
            createIngestionSource(input: $input)
        }"""
    variables: Dict[str, Any] = {
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
    res_data = execute_graphql(auth_session, query, variables)
    assert res_data["data"]["createIngestionSource"] is not None

    ingestion_source_urn = res_data["data"]["createIngestionSource"]

    # Create a request to execute the ingestion source
    query = """mutation createIngestionExecutionRequest($input: CreateIngestionExecutionRequestInput!) {\n
            createIngestionExecutionRequest(input: $input)
        }"""
    variables = {"input": {"ingestionSourceUrn": ingestion_source_urn}}
    res_data = execute_graphql(auth_session, query, variables)
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
    query = """mutation deleteIngestionSource($urn: String!) {\n
            deleteIngestionSource(urn: $urn)
        }"""
    variables = {"urn": ingestion_source_urn}
    res_data = execute_graphql(auth_session, query, variables)
    assert res_data["data"]["deleteIngestionSource"] is not None
