from unittest.mock import patch

import time_machine

from datahub.ingestion.source.bigquery_v2.bigquery_audit import (
    BigqueryTableIdentifier,
    BigQueryTableRef,
)
from datahub.ingestion.source.bigquery_v2.bigquery_config import BigQueryV2Config
from datahub.ingestion.source.bigquery_v2.bigquery_report import BigQueryV2Report
from datahub.ingestion.source.bigquery_v2.common import BigQueryIdentifierBuilder
from datahub.ingestion.source.bigquery_v2.usage import BigQueryUsageExtractor
from datahub.sql_parsing.schema_resolver import SchemaResolver

FROZEN_TIME = "2021-07-20 00:00:00+00:00"


@patch(
    "datahub.ingestion.source.bigquery_v2.bigquery_connection.service_account.Credentials.from_service_account_info"
)
@time_machine.travel(FROZEN_TIME, tick=False)
def test_bigqueryv2_filters(mock_from_sa_info):
    config = BigQueryV2Config.model_validate(
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
timestamp >= \"2021-07-18T23:45:00Z\"
AND
timestamp < \"2021-07-20T00:15:00Z\"
AND protoPayload.serviceName="bigquery.googleapis.com"
AND
(
    (
        protoPayload.methodName=
            (
                "google.cloud.bigquery.v2.JobService.Query"
                OR
                "google.cloud.bigquery.v2.JobService.InsertJob"
            )
        AND protoPayload.metadata.jobChange.job.jobStatus.jobState=\"DONE\"
        AND NOT protoPayload.metadata.jobChange.job.jobStatus.errorResult:*
        AND protoPayload.metadata.jobChange.job.jobConfig.queryConfig:*
        AND
        (
            (
                protoPayload.metadata.jobChange.job.jobStats.queryStats.referencedTables:*
                AND NOT protoPayload.metadata.jobChange.job.jobStats.queryStats.referencedTables =~ "projects/.*/datasets/.*/tables/__TABLES__|__TABLES_SUMMARY__|INFORMATION_SCHEMA.*"
            )
            OR
            (
                protoPayload.metadata.jobChange.job.jobConfig.queryConfig.destinationTable:*
            )
        )
    )
    OR
    protoPayload.metadata.tableDataRead.reason = "JOB"
)"""

    corrected_start_time = config.start_time - config.max_query_duration
    corrected_end_time = config.end_time + config.max_query_duration
    report = BigQueryV2Report()
    filter: str = BigQueryUsageExtractor(
        config,
        report,
        schema_resolver=SchemaResolver(platform="bigquery"),
        identifiers=BigQueryIdentifierBuilder(config, report),
    )._generate_filter(corrected_start_time, corrected_end_time)
    assert filter == expected_filter


def test_bigquery_table_sanitasitation():
    table_ref = BigQueryTableRef(
        BigqueryTableIdentifier("project-1234", "dataset-4567", "foo_*")
    )

    assert (
        table_ref.table_identifier.raw_table_name() == "project-1234.dataset-4567.foo_*"
    )
    assert table_ref.table_identifier.table == "foo_*"
    assert table_ref.table_identifier.project_id == "project-1234"
    assert table_ref.table_identifier.dataset == "dataset-4567"
    assert table_ref.table_identifier.is_sharded_table()
    assert table_ref.table_identifier.get_table_display_name() == "foo"

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
        BigqueryTableIdentifier("project-1234", "dataset-4567", "foo_20221210")
    )
    new_table_identifier = table_ref.table_identifier
    assert new_table_identifier.table == "foo_20221210"
    assert new_table_identifier.is_sharded_table()
    assert new_table_identifier.get_table_display_name() == "foo"
    assert new_table_identifier.project_id == "project-1234"
    assert new_table_identifier.dataset == "dataset-4567"

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
    table_identifier = table_ref.table_identifier
    assert table_identifier.is_sharded_table()
    assert table_identifier.project_id == "project-1234"
    assert table_identifier.dataset == "dataset-4567"
    assert table_identifier.table == "foo_2016*"
    assert table_identifier.get_table_display_name() == "foo"
