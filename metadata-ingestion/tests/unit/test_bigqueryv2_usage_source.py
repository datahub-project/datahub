import json
import os

from freezegun import freeze_time

from datahub.ingestion.source.bigquery_v2.bigquery_audit import (
    BQ_AUDIT_V2,
    BigqueryTableIdentifier,
    BigQueryTableRef,
)
from datahub.ingestion.source.bigquery_v2.bigquery_config import BigQueryV2Config
from datahub.ingestion.source.bigquery_v2.bigquery_report import BigQueryV2Report
from datahub.ingestion.source.bigquery_v2.usage import BigQueryUsageExtractor

FROZEN_TIME = "2021-07-20 00:00:00"


def test_bigqueryv2_uri_with_credential():

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

    config = BigQueryV2Config.parse_obj(
        {
            "project_id": "test-project",
            "stateful_ingestion": {"enabled": False},
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
def test_bigqueryv2_filters_with_allow_filter():
    config = BigQueryV2Config.parse_obj(
        {
            "project_id": "test-project",
            "stateful_ingestion": {"enabled": False},
            "credential": {
                "project_id": "test-project",
                "private_key_id": "test-private-key",
                "private_key": "random_private_key",
                "client_email": "test@acryl.io",
                "client_id": "test_client-id",
            },
            "table_pattern": {"allow": ["test-regex", "test-regex-1"], "deny": []},
        }
    )
    expected_filter: str = """resource.type=(\"bigquery_project\" OR \"bigquery_dataset\")
AND
(
    (
        protoPayload.methodName=
            (
                \"google.cloud.bigquery.v2.JobService.Query\"
                OR
                \"google.cloud.bigquery.v2.JobService.InsertJob\"
            )
        AND
        protoPayload.metadata.jobChange.job.jobStatus.jobState=\"DONE\"
        AND NOT protoPayload.metadata.jobChange.job.jobStatus.errorResult:*
        AND protoPayload.metadata.jobChange.job.jobStats.queryStats.referencedTables:*
        AND NOT protoPayload.metadata.jobChange.job.jobStats.queryStats.referencedTables =~ "projects/.*/datasets/.*/tables/__TABLES__|__TABLES_SUMMARY__|INFORMATION_SCHEMA.*"
         AND (
            protoPayload.metadata.jobChange.job.jobStats.queryStats.referencedTables =~ \"projects/.*/datasets/.*/tables/test-regex|test-regex-1\"
            
         OR
            protoPayload.metadata.tableDataRead.reason = \"JOB\"
        )
    )
    OR
    (
        protoPayload.metadata.tableDataRead:*
    )
)
AND
timestamp >= \"2021-07-18T23:45:00Z\"
AND
timestamp < \"2021-07-20T00:15:00Z\""""  # noqa: W293

    source = BigQueryUsageExtractor(config, BigQueryV2Report())
    filter: str = source._generate_filter(BQ_AUDIT_V2)
    assert filter == expected_filter


@freeze_time(FROZEN_TIME)
def test_bigqueryv2_filters_with_deny_filter():
    config = BigQueryV2Config.parse_obj(
        {
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
    )
    expected_filter: str = """resource.type=(\"bigquery_project\" OR \"bigquery_dataset\")
AND
(
    (
        protoPayload.methodName=
            (
                \"google.cloud.bigquery.v2.JobService.Query\"
                OR
                \"google.cloud.bigquery.v2.JobService.InsertJob\"
            )
        AND
        protoPayload.metadata.jobChange.job.jobStatus.jobState=\"DONE\"
        AND NOT protoPayload.metadata.jobChange.job.jobStatus.errorResult:*
        AND protoPayload.metadata.jobChange.job.jobStats.queryStats.referencedTables:*
        AND NOT protoPayload.metadata.jobChange.job.jobStats.queryStats.referencedTables =~ "projects/.*/datasets/.*/tables/__TABLES__|__TABLES_SUMMARY__|INFORMATION_SCHEMA.*"
         AND (
            protoPayload.metadata.jobChange.job.jobStats.queryStats.referencedTables =~ \"projects/.*/datasets/.*/tables/test-regex|test-regex-1\"
            AND
            NOT (
                protoPayload.metadata.jobChange.job.jobStats.queryStats.referencedTables =~ \"projects/.*/datasets/.*/tables/excluded_table_regex|excluded-regex-2\"
            )
         OR
            protoPayload.metadata.tableDataRead.reason = \"JOB\"
        )
    )
    OR
    (
        protoPayload.metadata.tableDataRead:*
    )
)
AND
timestamp >= \"2021-07-18T23:45:00Z\"
AND
timestamp < \"2021-07-20T00:15:00Z\""""  # noqa: W293
    source = BigQueryUsageExtractor(config, BigQueryV2Report())
    filter: str = source._generate_filter(BQ_AUDIT_V2)
    assert filter == expected_filter


def test_bigquery_table_sanitasitation():
    table_ref = BigQueryTableRef(
        BigqueryTableIdentifier("project-1234", "dataset-4567", "foo_*")
    )
    new_table_ref = BigqueryTableIdentifier.from_string_name(
        table_ref.table_identifier.get_table_name()
    )
    assert new_table_ref.table == "foo"
    assert new_table_ref.project_id == "project-1234"
    assert new_table_ref.dataset == "dataset-4567"

    table_ref = BigQueryTableRef(
        BigqueryTableIdentifier("project-1234", "dataset-4567", "foo_2022")
    )
    new_table_ref = BigqueryTableIdentifier.from_string_name(
        table_ref.table_identifier.get_table_name()
    )
    assert new_table_ref.table == "foo_2022"
    assert new_table_ref.project_id == "project-1234"
    assert new_table_ref.dataset == "dataset-4567"

    table_ref = BigQueryTableRef(
        BigqueryTableIdentifier("project-1234", "dataset-4567", "foo_20222110")
    )
    new_table_ref = BigqueryTableIdentifier.from_string_name(
        table_ref.table_identifier.get_table_name()
    )
    assert new_table_ref.table == "foo"
    assert new_table_ref.project_id == "project-1234"
    assert new_table_ref.dataset == "dataset-4567"

    table_ref = BigQueryTableRef(
        BigqueryTableIdentifier("project-1234", "dataset-4567", "foo")
    )
    new_table_ref = BigqueryTableIdentifier.from_string_name(
        table_ref.table_identifier.get_table_name()
    )
    assert new_table_ref.table == "foo"
    assert new_table_ref.project_id == "project-1234"
    assert new_table_ref.dataset == "dataset-4567"

    table_ref = BigQueryTableRef(
        BigqueryTableIdentifier("project-1234", "dataset-4567", "foo_2016*")
    )
    new_table_ref = BigqueryTableIdentifier.from_string_name(
        table_ref.table_identifier.get_table_name()
    )
    assert new_table_ref.table == "foo"
    assert new_table_ref.project_id == "project-1234"
    assert new_table_ref.dataset == "dataset-4567"
