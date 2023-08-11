import logging
import textwrap
from datetime import datetime
from typing import Callable, Iterable, List, Optional

from google.cloud import bigquery
from google.cloud.logging_v2.client import Client as GCPLoggingClient
from ratelimiter import RateLimiter

from datahub.ingestion.source.bigquery_v2.bigquery_audit import (
    AuditLogEntry,
    BigQueryAuditMetadata,
)
from datahub.ingestion.source.bigquery_v2.bigquery_report import (
    BigQueryAuditLogApiPerfReport,
)
from datahub.ingestion.source.bigquery_v2.common import (
    BQ_DATE_SHARD_FORMAT,
    BQ_DATETIME_FORMAT,
)

logger: logging.Logger = logging.getLogger(__name__)


# Api interfaces are separated based on functionality they provide
# rather than the underlying bigquery client that is used to
# provide the functionality.
class BigQueryAuditLogApi:
    def __init__(
        self,
        report: BigQueryAuditLogApiPerfReport,
        rate_limit: bool,
        requests_per_min: int,
    ) -> None:
        self.report = report
        self.rate_limit = rate_limit
        self.requests_per_min = requests_per_min

    def get_exported_bigquery_audit_metadata(
        self,
        bigquery_client: bigquery.Client,
        bigquery_audit_metadata_query_template: Callable[
            [str, bool, Optional[int]], str
        ],
        bigquery_audit_metadata_datasets: Optional[List[str]],
        use_date_sharded_audit_log_tables: bool,
        start_time: datetime,
        end_time: datetime,
        limit: Optional[int] = None,
    ) -> Iterable[BigQueryAuditMetadata]:
        if bigquery_audit_metadata_datasets is None:
            return

        audit_start_time = start_time.strftime(BQ_DATETIME_FORMAT)
        audit_start_date = start_time.strftime(BQ_DATE_SHARD_FORMAT)

        audit_end_time = end_time.strftime(BQ_DATETIME_FORMAT)
        audit_end_date = end_time.strftime(BQ_DATE_SHARD_FORMAT)

        with self.report.get_exported_log_entries as current_timer:
            for dataset in bigquery_audit_metadata_datasets:
                logger.info(
                    f"Start loading log entries from BigQueryAuditMetadata in {dataset}"
                )

                query = bigquery_audit_metadata_query_template(
                    dataset,
                    use_date_sharded_audit_log_tables,
                    limit,
                ).format(
                    start_time=audit_start_time,
                    end_time=audit_end_time,
                    start_date=audit_start_date,
                    end_date=audit_end_date,
                )

                query_job = bigquery_client.query(query)
                logger.info(
                    f"Finished loading log entries from BigQueryAuditMetadata in {dataset}"
                )

                if self.rate_limit:
                    with RateLimiter(max_calls=self.requests_per_min, period=60):
                        for entry in query_job:
                            with current_timer.pause_timer():
                                yield entry
                else:
                    for entry in query_job:
                        with current_timer.pause_timer():
                            yield entry

    def get_bigquery_log_entries_via_gcp_logging(
        self,
        client: GCPLoggingClient,
        filter: str,
        log_page_size: int,
        limit: Optional[int] = None,
    ) -> Iterable[AuditLogEntry]:
        logger.debug(filter)

        list_entries: Iterable[AuditLogEntry]
        rate_limiter: Optional[RateLimiter] = None
        if self.rate_limit:
            # client.list_entries is a generator, does api calls to GCP Logging when it runs out of entries and needs to fetch more from GCP Logging
            # to properly ratelimit we multiply the page size by the number of requests per minute
            rate_limiter = RateLimiter(
                max_calls=self.requests_per_min * log_page_size,
                period=60,
            )

        with self.report.list_log_entries as current_timer:
            list_entries = client.list_entries(
                filter_=filter,
                page_size=log_page_size,
                max_results=limit,
            )

            for i, entry in enumerate(list_entries):
                if i == 0:
                    logger.info(
                        f"Starting log load from GCP Logging for {client.project}"
                    )

                if i % 1000 == 0:
                    logger.info(
                        f"Loaded {i} log entries from GCP Log for {client.project}"
                    )

                with current_timer.pause_timer():
                    if rate_limiter:
                        with rate_limiter:
                            yield entry
                    else:
                        yield entry

            logger.info(
                f"Finished loading log entries from GCP Log for {client.project}"
            )


def bigquery_audit_metadata_query_template_usage(
    dataset: str,
    use_date_sharded_tables: bool,
    limit: Optional[int] = None,
) -> str:
    """
    Receives a dataset (with project specified) and returns a query template that is used to query exported
    v2 AuditLogs containing protoPayloads of type BigQueryAuditMetadata.
    :param dataset: the dataset to query against in the form of $PROJECT.$DATASET
    :param use_date_sharded_tables: whether to read from date sharded audit log tables or time partitioned audit log
           tables
    :param limit: maximum number of events to query for
    :return: a query template, when supplied start_time and end_time, can be used to query audit logs from BigQuery
    """

    limit_text = f"limit {limit}" if limit else ""

    shard_condition = ""
    if use_date_sharded_tables:
        from_table = f"`{dataset}.cloudaudit_googleapis_com_data_access_*`"
        shard_condition = (
            """ AND _TABLE_SUFFIX BETWEEN "{start_date}" AND "{end_date}" """
        )
    else:
        from_table = f"`{dataset}.cloudaudit_googleapis_com_data_access`"

    # Deduplicates insertId via QUALIFY, see:
    # https://cloud.google.com/logging/docs/reference/v2/rest/v2/LogEntry, insertId field
    query = f"""
        SELECT
            timestamp,
            logName,
            insertId,
            protopayload_auditlog AS protoPayload,
            protopayload_auditlog.metadataJson AS metadata
        FROM
            {from_table}
        WHERE (
            timestamp >= "{{start_time}}"
            AND timestamp < "{{end_time}}"
        )
        {shard_condition}
        AND protopayload_auditlog.serviceName="bigquery.googleapis.com"
        AND
        (
            (
                protopayload_auditlog.methodName IN
                    (
                        "google.cloud.bigquery.v2.JobService.Query",
                        "google.cloud.bigquery.v2.JobService.InsertJob"
                    )
                AND JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson, "$.jobChange.job.jobStatus.jobState") = "DONE"
                AND JSON_EXTRACT(protopayload_auditlog.metadataJson, "$.jobChange.job.jobStatus.errorResults") IS NULL
                AND JSON_EXTRACT(protopayload_auditlog.metadataJson, "$.jobChange.job.jobConfig.queryConfig") IS NOT NULL
                AND (
                        JSON_EXTRACT_ARRAY(protopayload_auditlog.metadataJson,
                                                            "$.jobChange.job.jobStats.queryStats.referencedTables") IS NOT NULL
                    OR
                        JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson, "$.jobChange.job.jobConfig.queryConfig.destinationTable") IS NOT NULL
                    )
            )
            OR
                JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson, "$.tableDataRead.reason") = "JOB"
        )
        QUALIFY ROW_NUMBER() OVER (PARTITION BY insertId, timestamp, logName) = 1
        {limit_text};
    """

    return textwrap.dedent(query)


def bigquery_audit_metadata_query_template_lineage(
    dataset: str, use_date_sharded_tables: bool, limit: Optional[int] = None
) -> str:
    """
    Receives a dataset (with project specified) and returns a query template that is used to query exported
    AuditLogs containing protoPayloads of type BigQueryAuditMetadata.
    Include only those that:
    - have been completed (jobStatus.jobState = "DONE")
    - do not contain errors (jobStatus.errorResults is none)
    :param dataset: the dataset to query against in the form of $PROJECT.$DATASET
    :param use_date_sharded_tables: whether to read from date sharded audit log tables or time partitioned audit log
           tables
    :param limit: set a limit for the maximum event to return. It is used for connection testing currently
    :return: a query template, when supplied start_time and end_time, can be used to query audit logs from BigQuery
    """
    limit_text = f"limit {limit}" if limit else ""

    shard_condition = ""
    if use_date_sharded_tables:
        from_table = f"`{dataset}.cloudaudit_googleapis_com_data_access_*`"
        shard_condition = (
            """ AND _TABLE_SUFFIX BETWEEN "{start_date}" AND "{end_date}" """
        )
    else:
        from_table = f"`{dataset}.cloudaudit_googleapis_com_data_access`"

    query = f"""
            SELECT
                timestamp,
                logName,
                insertId,
                protopayload_auditlog AS protoPayload,
                protopayload_auditlog.metadataJson AS metadata
            FROM
                {from_table}
            WHERE (
                timestamp >= "{{start_time}}"
                AND timestamp < "{{end_time}}"
            )
            {shard_condition}
            AND protopayload_auditlog.serviceName="bigquery.googleapis.com"
            AND JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson, "$.jobChange.job.jobStatus.jobState") = "DONE"
            AND JSON_EXTRACT(protopayload_auditlog.metadataJson, "$.jobChange.job.jobStatus.errorResults") IS NULL
            AND JSON_EXTRACT(protopayload_auditlog.metadataJson, "$.jobChange.job.jobConfig.queryConfig") IS NOT NULL
            {limit_text};
        """

    return textwrap.dedent(query)


BQ_FILTER_RULE_TEMPLATE_V2_USAGE = """
resource.type=("bigquery_project" OR "bigquery_dataset")
AND
timestamp >= "{start_time}"
AND
timestamp < "{end_time}"
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
        AND protoPayload.metadata.jobChange.job.jobStatus.jobState="DONE"
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
)
""".strip(
    "\t \n"
)

BQ_FILTER_RULE_TEMPLATE_V2_LINEAGE = """
resource.type=("bigquery_project")
AND
(
    protoPayload.methodName=
        (
            "google.cloud.bigquery.v2.JobService.Query"
            OR
            "google.cloud.bigquery.v2.JobService.InsertJob"
        )
    AND
    protoPayload.metadata.jobChange.job.jobStatus.jobState="DONE"
    AND NOT protoPayload.metadata.jobChange.job.jobStatus.errorResult:*
    AND (
        protoPayload.metadata.jobChange.job.jobStats.queryStats.referencedTables:*
        OR
        protoPayload.metadata.jobChange.job.jobStats.queryStats.referencedViews:*
    )
    AND (
        protoPayload.metadata.jobChange.job.jobStats.queryStats.referencedTables !~ "projects/.*/datasets/_.*/tables/anon.*"
        AND
        protoPayload.metadata.jobChange.job.jobStats.queryStats.referencedTables !~ "projects/.*/datasets/.*/tables/INFORMATION_SCHEMA.*"
        AND
        protoPayload.metadata.jobChange.job.jobStats.queryStats.referencedTables !~ "projects/.*/datasets/.*/tables/__TABLES__"
        AND
        protoPayload.metadata.jobChange.job.jobConfig.queryConfig.destinationTable !~ "projects/.*/datasets/_.*/tables/anon.*"
    )

)
AND
timestamp >= "{start_time}"
AND
timestamp < "{end_time}"
""".strip()
