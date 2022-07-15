import json
import os

from freezegun import freeze_time

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.usage.bigquery_usage import (
    BQ_AUDIT_V1,
    BigQueryTableRef,
    BigQueryUsageConfig,
    BigQueryUsageSource,
)
from datahub.ingestion.source_config.bigquery import (
    _BIGQUERY_DEFAULT_SHARDED_TABLE_REGEX,
)

FROZEN_TIME = "2021-07-20 00:00:00"


def test_bigquery_uri_with_credential():

    expected_credential_json = {
        "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
        "client_email": "test@acryl.io",
        "client_id": "test_client-id",
        "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/test@acryl.io",
        "private_key": "random_private_key",
        "private_key_id": "test-private-key",
        "project_id": "test-project",
        "token_uri": "https://oauth2.googleapis.com/token",
        "type": "service_account",
    }

    config = BigQueryUsageConfig.parse_obj(
        {
            "project_id": "test-project",
            "credential": {
                "project_id": "test-project",
                "private_key_id": "test-private-key",
                "private_key": "random_private_key",
                "client_email": "test@acryl.io",
                "client_id": "test_client-id",
            },
        }
    )

    try:
        assert config._credentials_path

        with open(config._credentials_path) as jsonFile:
            json_credential = json.load(jsonFile)
            jsonFile.close()

        credential = json.dumps(json_credential, sort_keys=True)
        expected_credential = json.dumps(expected_credential_json, sort_keys=True)
        assert expected_credential == credential

    except AssertionError as e:
        if config._credentials_path:
            os.unlink(str(config._credentials_path))
        raise e


@freeze_time(FROZEN_TIME)
def test_bigquery_filters_with_allow_filter():
    config = {
        "project_id": "test-project",
        "credential": {
            "project_id": "test-project",
            "private_key_id": "test-private-key",
            "private_key": "random_private_key",
            "client_email": "test@acryl.io",
            "client_id": "test_client-id",
        },
        "table_pattern": {"allow": ["test-regex", "test-regex-1"], "deny": []},
    }
    expected_filter: str = """protoPayload.serviceName="bigquery.googleapis.com"
AND
(
    (
        protoPayload.methodName="jobservice.jobcompleted"
        AND
        protoPayload.serviceData.jobCompletedEvent.eventName="query_job_completed"
        AND
        protoPayload.serviceData.jobCompletedEvent.job.jobStatus.state="DONE"
        AND
        NOT protoPayload.serviceData.jobCompletedEvent.job.jobStatus.error.code:*
    )
    OR
    (
        protoPayload.metadata.tableDataRead:*
    )
)
AND (
    
protoPayload.serviceData.jobCompletedEvent.job.jobStatistics.referencedTables.tableId =~ "test-regex|test-regex-1"

    
AND
protoPayload.serviceData.jobCompletedEvent.job.jobStatistics.referencedTables.tableId !~ "__TABLES_SUMMARY__|INFORMATION_SCHEMA"

    OR
    protoPayload.metadata.tableDataRead.reason = "JOB"
)
AND
timestamp >= "2021-07-18T23:45:00Z"
AND
timestamp < "2021-07-21T00:15:00Z\""""  # noqa: W293

    source = BigQueryUsageSource.create(config, PipelineContext(run_id="bq-usage-test"))

    # source: BigQueryUsageSource = BigQueryUsageSource(
    #    config=config, ctx=PipelineContext(run_id="test")
    # )
    filter: str = source._generate_filter(BQ_AUDIT_V1)
    assert filter == expected_filter


@freeze_time(FROZEN_TIME)
def test_bigquery_filters_with_deny_filter():
    config = {
        "project_id": "test-project",
        "credential": {
            "project_id": "test-project",
            "private_key_id": "test-private-key",
            "private_key": "random_private_key",
            "client_email": "test@acryl.io",
            "client_id": "test_client-id",
        },
        "table_pattern": {
            "allow": ["test-regex", "test-regex-1"],
            "deny": ["excluded_table_regex", "excluded-regex-2"],
        },
    }
    expected_filter: str = """protoPayload.serviceName="bigquery.googleapis.com"
AND
(
    (
        protoPayload.methodName="jobservice.jobcompleted"
        AND
        protoPayload.serviceData.jobCompletedEvent.eventName="query_job_completed"
        AND
        protoPayload.serviceData.jobCompletedEvent.job.jobStatus.state="DONE"
        AND
        NOT protoPayload.serviceData.jobCompletedEvent.job.jobStatus.error.code:*
    )
    OR
    (
        protoPayload.metadata.tableDataRead:*
    )
)
AND (
    
protoPayload.serviceData.jobCompletedEvent.job.jobStatistics.referencedTables.tableId =~ "test-regex|test-regex-1"

    
AND
protoPayload.serviceData.jobCompletedEvent.job.jobStatistics.referencedTables.tableId !~ "__TABLES_SUMMARY__|INFORMATION_SCHEMA|excluded_table_regex|excluded-regex-2"

    OR
    protoPayload.metadata.tableDataRead.reason = "JOB"
)
AND
timestamp >= "2021-07-18T23:45:00Z"
AND
timestamp < "2021-07-21T00:15:00Z\""""  # noqa: W293
    source = BigQueryUsageSource.create(config, PipelineContext(run_id="bq-usage-test"))
    filter: str = source._generate_filter(BQ_AUDIT_V1)
    assert filter == expected_filter


def test_bigquery_ref_extra_removal():
    table_ref = BigQueryTableRef("project-1234", "dataset-4567", "foo_*")
    new_table_ref = table_ref.remove_extras(_BIGQUERY_DEFAULT_SHARDED_TABLE_REGEX)
    assert new_table_ref.table == "foo"
    assert new_table_ref.project == table_ref.project
    assert new_table_ref.dataset == table_ref.dataset

    table_ref = BigQueryTableRef("project-1234", "dataset-4567", "foo_2022")
    new_table_ref = table_ref.remove_extras(_BIGQUERY_DEFAULT_SHARDED_TABLE_REGEX)
    assert new_table_ref.table == "foo"
    assert new_table_ref.project == table_ref.project
    assert new_table_ref.dataset == table_ref.dataset

    table_ref = BigQueryTableRef("project-1234", "dataset-4567", "foo_20222110")
    new_table_ref = table_ref.remove_extras(_BIGQUERY_DEFAULT_SHARDED_TABLE_REGEX)
    assert new_table_ref.table == "foo"
    assert new_table_ref.project == table_ref.project
    assert new_table_ref.dataset == table_ref.dataset

    table_ref = BigQueryTableRef("project-1234", "dataset-4567", "foo")
    new_table_ref = table_ref.remove_extras(_BIGQUERY_DEFAULT_SHARDED_TABLE_REGEX)
    assert new_table_ref.table == "foo"
    assert new_table_ref.project == table_ref.project
    assert new_table_ref.dataset == table_ref.dataset
