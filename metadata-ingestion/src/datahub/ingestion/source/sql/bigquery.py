import atexit
import collections
import datetime
import functools
import logging
import os
import re
import textwrap
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple, Union
from unittest.mock import patch

# This import verifies that the dependencies are available.
import sqlalchemy_bigquery
from dateutil.relativedelta import relativedelta
from google.cloud.bigquery import Client as BigQueryClient
from google.cloud.logging_v2.client import Client as GCPLoggingClient
from ratelimiter import RateLimiter
from sqlalchemy import create_engine, inspect
from sqlalchemy.engine.reflection import Inspector

from datahub.emitter import mce_builder
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import (
    BigQueryDatasetKey,
    PlatformKey,
    ProjectIdKey,
    gen_containers,
)
from datahub.ingestion.api.decorators import (
    SourceCapability,
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.sql.sql_common import (
    SQLAlchemyConfig,
    SQLAlchemySource,
    SqlWorkUnit,
    make_sqlalchemy_type,
    register_custom_type,
)
from datahub.ingestion.source.usage.bigquery_usage import (
    BQ_DATE_SHARD_FORMAT,
    BQ_DATETIME_FORMAT,
    AuditLogEntry,
    BigQueryAuditMetadata,
    BigQueryTableRef,
    QueryEvent,
)
from datahub.ingestion.source_config.sql.bigquery import BigQueryConfig
from datahub.ingestion.source_report.sql.bigquery import BigQueryReport
from datahub.metadata.com.linkedin.pegasus2avro.metadata.key import DatasetKey
from datahub.metadata.com.linkedin.pegasus2avro.metadata.snapshot import DatasetSnapshot
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from datahub.metadata.com.linkedin.pegasus2avro.schema import RecordTypeClass
from datahub.metadata.schema_classes import (
    ChangeTypeClass,
    DatasetLineageTypeClass,
    UpstreamClass,
    UpstreamLineageClass,
)
from datahub.utilities.bigquery_sql_parser import BigQuerySQLParser
from datahub.utilities.mapping import Constants

logger = logging.getLogger(__name__)

BQ_FILTER_RULE_TEMPLATE = """
protoPayload.serviceName="bigquery.googleapis.com"
AND
(
    (
        protoPayload.methodName="jobservice.jobcompleted"
        AND
        protoPayload.serviceData.jobCompletedEvent.eventName="query_job_completed"
        AND
        protoPayload.serviceData.jobCompletedEvent.job.jobStatus.state="DONE"
        AND NOT
        protoPayload.serviceData.jobCompletedEvent.job.jobStatus.error.code:*
        AND
        (
            protoPayload.serviceData.jobCompletedEvent.job.jobStatistics.referencedTables:*
            OR
            protoPayload.serviceData.jobCompletedEvent.job.jobStatistics.referencedViews:*
        )
    )
)
AND
timestamp >= "{start_time}"
AND
timestamp < "{end_time}"
""".strip()

BQ_FILTER_RULE_TEMPLATE_V2 = """
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
)
AND
timestamp >= "{start_time}"
AND
timestamp < "{end_time}"
""".strip()

BQ_GET_LATEST_PARTITION_TEMPLATE = """
SELECT
    c.table_catalog,
    c.table_schema,
    c.table_name,
    c.column_name,
    c.data_type,
    max(p.partition_id) as partition_id
FROM
    `{project_id}.{schema}.INFORMATION_SCHEMA.COLUMNS` as c
join `{project_id}.{schema}.INFORMATION_SCHEMA.PARTITIONS` as p
on
    c.table_catalog = p.table_catalog
    and c.table_schema = p.table_schema
    and c.table_name = p.table_name
where
    is_partitioning_column = 'YES'
    -- Filter out special partitions (https://cloud.google.com/bigquery/docs/partitioned-tables#date_timestamp_partitioned_tables)
    and p.partition_id not in ('__NULL__', '__UNPARTITIONED__', '__STREAMING_UNPARTITIONED__')
    and STORAGE_TIER='ACTIVE'
    and p.table_name= '{table}'
group by
    c.table_catalog,
    c.table_schema,
    c.table_name,
    c.column_name,
    c.data_type
order by
    c.table_catalog,
    c.table_schema,
    c.table_name,
    c.column_name
""".strip()

SHARDED_TABLE_REGEX = r"^(.+)[_](\d{4}|\d{6}|\d{8}|\d{10})$"

BQ_GET_LATEST_SHARD = """
SELECT SUBSTR(MAX(table_id), LENGTH('{table}_') + 1) as max_shard
FROM `{project_id}.{schema}.__TABLES_SUMMARY__`
WHERE table_id LIKE '{table}%'
""".strip()


# The existing implementation of this method can be found here:
# https://github.com/googleapis/python-bigquery-sqlalchemy/blob/main/sqlalchemy_bigquery/base.py#L1018-L1025.
# The existing implementation does not use the schema parameter and hence
# does not properly resolve the view definitions. As such, we must monkey
# patch the implementation.


def get_view_definition(self, connection, view_name, schema=None, **kw):
    view = self._get_table(connection, view_name, schema)
    return view.view_query


sqlalchemy_bigquery.BigQueryDialect.get_view_definition = get_view_definition


def bigquery_audit_metadata_query_template(
    dataset: str, use_date_sharded_tables: bool
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
    :return: a query template, when supplied start_time and end_time, can be used to query audit logs from BigQuery
    """
    query: str
    if use_date_sharded_tables:
        query = (
            f"""
        SELECT
            timestamp,
            logName,
            insertId,
            protopayload_auditlog AS protoPayload,
            protopayload_auditlog.metadataJson AS metadata
        FROM
            `{dataset}.cloudaudit_googleapis_com_data_access_*`
        """
            + """
        WHERE
            _TABLE_SUFFIX BETWEEN "{start_date}" AND "{end_date}" AND
        """
        )
    else:
        query = f"""
        SELECT
            timestamp,
            logName,
            insertId,
            protopayload_auditlog AS protoPayload,
            protopayload_auditlog.metadataJson AS metadata
        FROM
            `{dataset}.cloudaudit_googleapis_com_data_access`
        WHERE
        """

    audit_log_filter = """    timestamp >= "{start_time}"
    AND timestamp < "{end_time}"
    AND protopayload_auditlog.serviceName="bigquery.googleapis.com"
    AND JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson, "$.jobChange.job.jobStatus.jobState") = "DONE"
    AND JSON_EXTRACT(protopayload_auditlog.metadataJson, "$.jobChange.job.jobStatus.errorResults") IS NULL
    AND JSON_EXTRACT(protopayload_auditlog.metadataJson, "$.jobChange.job.jobConfig.queryConfig") IS NOT NULL;
    """

    query = textwrap.dedent(query) + audit_log_filter

    return textwrap.dedent(query)


def get_partition_range_from_partition_id(
    partition_id: str, partition_datetime: Optional[datetime.datetime]
) -> Tuple[datetime.datetime, datetime.datetime]:
    duration: relativedelta
    # if yearly partitioned,
    if len(partition_id) == 4:
        duration = relativedelta(years=1)
        if not partition_datetime:
            partition_datetime = datetime.datetime.strptime(partition_id, "%Y")
        partition_datetime = partition_datetime.replace(month=1, day=1)
    # elif monthly partitioned,
    elif len(partition_id) == 6:
        duration = relativedelta(months=1)
        if not partition_datetime:
            partition_datetime = datetime.datetime.strptime(partition_id, "%Y%m")
        partition_datetime = partition_datetime.replace(day=1)
    # elif daily partitioned,
    elif len(partition_id) == 8:
        duration = relativedelta(days=1)
        if not partition_datetime:
            partition_datetime = datetime.datetime.strptime(partition_id, "%Y%m%d")
    # elif hourly partitioned,
    elif len(partition_id) == 10:
        duration = relativedelta(hours=1)
        if not partition_datetime:
            partition_datetime = datetime.datetime.strptime(partition_id, "%Y%m%d%H")
    else:
        raise ValueError(
            f"check your partition_id {partition_id}. It must be yearly/monthly/daily/hourly."
        )
    upper_bound_partition_datetime = partition_datetime + duration
    return partition_datetime, upper_bound_partition_datetime


# Handle the GEOGRAPHY type. We will temporarily patch the _type_map
# in the get_workunits method of the source.
GEOGRAPHY = make_sqlalchemy_type("GEOGRAPHY")
register_custom_type(GEOGRAPHY)
assert sqlalchemy_bigquery._types._type_map
# STRUCT is a custom sqlalchemy data type defined by the sqlalchemy_bigquery library
# https://github.com/googleapis/python-bigquery-sqlalchemy/blob/934e25f705fd9f226e438d075c7e00e495cce04e/sqlalchemy_bigquery/_types.py#L47
register_custom_type(sqlalchemy_bigquery.STRUCT, output=RecordTypeClass)


@dataclass
class BigQueryPartitionColumn:
    table_catalog: str
    table_schema: str
    table_name: str
    column_name: str
    data_type: str
    partition_id: str


# We can't use close as it is not called if the ingestion is not successful
def cleanup(config: BigQueryConfig) -> None:
    if config._credentials_path is not None:
        logger.debug(
            f"Deleting temporary credential file at {config._credentials_path}"
        )
        os.unlink(config._credentials_path)


@config_class(BigQueryConfig)
@platform_name("BigQuery")
@support_status(SupportStatus.CERTIFIED)
@capability(
    SourceCapability.PLATFORM_INSTANCE,
    "BigQuery doesn't need platform instances because project ids in BigQuery are globally unique.",
    supported=False,
)
@capability(SourceCapability.DOMAINS, "Supported via the `domain` config field")
@capability(SourceCapability.DATA_PROFILING, "Optionally enabled via configuration")
@capability(SourceCapability.DESCRIPTIONS, "Enabled by default")
@capability(SourceCapability.LINEAGE_COARSE, "Enabled by default")
@capability(
    SourceCapability.USAGE_STATS,
    "Not provided by this module, use `bigquery-usage` for that.",
    supported=False,
)
@capability(SourceCapability.DELETION_DETECTION, "Enabled via stateful ingestion")
class BigQuerySource(SQLAlchemySource):
    """
    This plugin extracts the following:
    - Metadata for databases, schemas, and tables
    - Column types associated with each table
    - Table, row, and column statistics via optional SQL profiling
    - Table level lineage.
    """

    def __init__(self, config, ctx):
        super().__init__(config, ctx, "bigquery")
        self.config: BigQueryConfig = config
        self.ctx = ctx
        self.report: BigQueryReport = BigQueryReport()
        self.lineage_metadata: Optional[Dict[str, Set[str]]] = None
        self.maximum_shard_ids: Dict[str, str] = dict()
        self.partition_info: Dict[str, str] = dict()
        atexit.register(cleanup, config)

    def get_db_name(self, inspector: Inspector = None) -> str:
        if self.config.project_id:
            return self.config.project_id
        else:
            return self._get_project_id(inspector)

    def _compute_big_query_lineage(self) -> None:
        if not self.config.include_table_lineage:
            return

        lineage_client_project_id = self._get_lineage_client_project_id()
        if self.config.use_exported_bigquery_audit_metadata:
            self._compute_bigquery_lineage_via_exported_bigquery_audit_metadata(
                lineage_client_project_id
            )
        else:
            self._compute_bigquery_lineage_via_gcp_logging(lineage_client_project_id)

        if self.lineage_metadata is None:
            self.lineage_metadata = {}

        self.report.lineage_metadata_entries = len(self.lineage_metadata)
        logger.info(
            f"Built lineage map containing {len(self.lineage_metadata)} entries."
        )
        logger.debug(f"lineage metadata is {self.lineage_metadata}")

    def _compute_bigquery_lineage_via_gcp_logging(
        self, lineage_client_project_id: Optional[str]
    ) -> None:
        logger.info("Populating lineage info via GCP audit logs")
        try:
            _clients: List[GCPLoggingClient] = self._make_bigquery_client(
                lineage_client_project_id
            )
            template: str = BQ_FILTER_RULE_TEMPLATE

            if self.config.use_v2_audit_metadata:
                template = BQ_FILTER_RULE_TEMPLATE_V2

            log_entries: Iterable[AuditLogEntry] = self._get_bigquery_log_entries(
                _clients, template
            )
            parsed_entries: Iterable[QueryEvent] = self._parse_bigquery_log_entries(
                log_entries
            )
            self.lineage_metadata = self._create_lineage_map(parsed_entries)
        except Exception as e:
            self.error(
                logger,
                "lineage-gcp-logs",
                f"Error was {e}",
            )

    def _compute_bigquery_lineage_via_exported_bigquery_audit_metadata(
        self, lineage_client_project_id: Optional[str]
    ) -> None:
        logger.info("Populating lineage info via exported GCP audit logs")
        try:
            _client: BigQueryClient = BigQueryClient(project=lineage_client_project_id)
            exported_bigquery_audit_metadata: Iterable[
                BigQueryAuditMetadata
            ] = self._get_exported_bigquery_audit_metadata(_client)
            parsed_entries: Iterable[
                QueryEvent
            ] = self._parse_exported_bigquery_audit_metadata(
                exported_bigquery_audit_metadata
            )
            self.lineage_metadata = self._create_lineage_map(parsed_entries)
        except Exception as e:
            self.error(
                logger,
                "lineage-exported-gcp-audit-logs",
                f"Error: {e}",
            )

    def _make_bigquery_client(
        self, lineage_client_project_id: Optional[str]
    ) -> List[GCPLoggingClient]:
        # See https://github.com/googleapis/google-cloud-python/issues/2674 for
        # why we disable gRPC here.
        client_options = self.config.extra_client_options.copy()
        client_options["_use_grpc"] = False
        if lineage_client_project_id is not None:
            return [
                GCPLoggingClient(**client_options, project=lineage_client_project_id)
            ]
        else:
            return [GCPLoggingClient(**client_options)]

    def _get_lineage_client_project_id(self) -> Optional[str]:
        project_id: Optional[str] = (
            self.config.lineage_client_project_id
            if self.config.lineage_client_project_id
            else self.config.project_id
        )
        return project_id

    def _get_bigquery_log_entries(
        self,
        clients: List[GCPLoggingClient],
        template: str,
    ) -> Union[Iterable[AuditLogEntry], Iterable[BigQueryAuditMetadata]]:
        self.report.num_total_log_entries = 0
        # Add a buffer to start and end time to account for delays in logging events.
        start_time = (self.config.start_time - self.config.max_query_duration).strftime(
            BQ_DATETIME_FORMAT
        )
        self.report.log_entry_start_time = start_time

        end_time = (self.config.end_time + self.config.max_query_duration).strftime(
            BQ_DATETIME_FORMAT
        )
        self.report.log_entry_end_time = end_time

        filter = template.format(
            start_time=start_time,
            end_time=end_time,
        )

        logger.info(
            f"Start loading log entries from BigQuery start_time={start_time} and end_time={end_time}"
        )
        for client in clients:
            if self.config.rate_limit:
                with RateLimiter(max_calls=self.config.requests_per_min, period=60):
                    entries = client.list_entries(
                        filter_=filter, page_size=self.config.log_page_size
                    )
            else:
                entries = client.list_entries(
                    filter_=filter, page_size=self.config.log_page_size
                )
            for entry in entries:
                self.report.num_total_log_entries += 1
                yield entry

        logger.info(
            f"Finished loading {self.report.num_total_log_entries} log entries from BigQuery so far"
        )

    def _get_exported_bigquery_audit_metadata(
        self, bigquery_client: BigQueryClient
    ) -> Iterable[BigQueryAuditMetadata]:
        if self.config.bigquery_audit_metadata_datasets is None:
            self.error(
                logger, "audit-metadata", "bigquery_audit_metadata_datasets not set"
            )
            self.report.bigquery_audit_metadata_datasets_missing = True
            return

        start_time: str = (
            self.config.start_time - self.config.max_query_duration
        ).strftime(BQ_DATETIME_FORMAT)
        self.report.audit_start_time = start_time

        end_time: str = (
            self.config.end_time + self.config.max_query_duration
        ).strftime(BQ_DATETIME_FORMAT)
        self.report.audit_end_time = end_time

        for dataset in self.config.bigquery_audit_metadata_datasets:
            logger.info(
                f"Start loading log entries from BigQueryAuditMetadata in {dataset}"
            )

            query: str
            if self.config.use_date_sharded_audit_log_tables:
                start_date: str = (
                    self.config.start_time - self.config.max_query_duration
                ).strftime(BQ_DATE_SHARD_FORMAT)
                end_date: str = (
                    self.config.end_time + self.config.max_query_duration
                ).strftime(BQ_DATE_SHARD_FORMAT)

                query = bigquery_audit_metadata_query_template(
                    dataset, self.config.use_date_sharded_audit_log_tables
                ).format(
                    start_time=start_time,
                    end_time=end_time,
                    start_date=start_date,
                    end_date=end_date,
                )
            else:
                query = bigquery_audit_metadata_query_template(
                    dataset, self.config.use_date_sharded_audit_log_tables
                ).format(start_time=start_time, end_time=end_time)
            query_job = bigquery_client.query(query)

            logger.info(
                f"Finished loading log entries from BigQueryAuditMetadata in {dataset}"
            )

            if self.config.rate_limit:
                with RateLimiter(max_calls=self.config.requests_per_min, period=60):
                    yield from query_job
            else:
                yield from query_job

    # Currently we only parse JobCompleted events but in future we would want to parse other
    # events to also create field level lineage.
    def _parse_bigquery_log_entries(
        self,
        entries: Union[Iterable[AuditLogEntry], Iterable[BigQueryAuditMetadata]],
    ) -> Iterable[QueryEvent]:
        self.report.num_parsed_log_entires = 0
        for entry in entries:
            event: Optional[QueryEvent] = None

            missing_entry = QueryEvent.get_missing_key_entry(entry=entry)
            if missing_entry is None:
                event = QueryEvent.from_entry(entry)

            missing_entry_v2 = QueryEvent.get_missing_key_entry_v2(entry=entry)
            if event is None and missing_entry_v2 is None:
                event = QueryEvent.from_entry_v2(entry)

            if event is None:
                self.error(
                    logger,
                    f"{entry.log_name}-{entry.insert_id}",
                    f"Unable to parse log missing {missing_entry}, missing v2 {missing_entry_v2} for {entry}",
                )
            else:
                self.report.num_parsed_log_entires += 1
                yield event

        logger.info(
            "Parsing BigQuery log entries: "
            f"number of log entries successfully parsed={self.report.num_parsed_log_entires}"
        )

    def _parse_exported_bigquery_audit_metadata(
        self, audit_metadata_rows: Iterable[BigQueryAuditMetadata]
    ) -> Iterable[QueryEvent]:
        self.report.num_total_audit_entries = 0
        self.report.num_parsed_audit_entires = 0
        for audit_metadata in audit_metadata_rows:
            self.report.num_total_audit_entries += 1
            event: Optional[QueryEvent] = None

            missing_exported_audit = (
                QueryEvent.get_missing_key_exported_bigquery_audit_metadata(
                    audit_metadata
                )
            )

            if missing_exported_audit is None:
                event = QueryEvent.from_exported_bigquery_audit_metadata(audit_metadata)

            if event is None:
                self.error(
                    logger,
                    f"{audit_metadata['logName']}-{audit_metadata['insertId']}",
                    f"Unable to parse audit metadata missing {missing_exported_audit} for {audit_metadata}",
                )
            else:
                self.report.num_parsed_audit_entires += 1
                yield event

    def _create_lineage_map(self, entries: Iterable[QueryEvent]) -> Dict[str, Set[str]]:
        lineage_map: Dict[str, Set[str]] = collections.defaultdict(set)
        self.report.num_total_lineage_entries = 0
        self.report.num_skipped_lineage_entries_missing_data = 0
        self.report.num_skipped_lineage_entries_not_allowed = 0
        self.report.num_skipped_lineage_entries_other = 0
        for e in entries:
            self.report.num_total_lineage_entries += 1
            if e.destinationTable is None or not (
                e.referencedTables or e.referencedViews
            ):
                self.report.num_skipped_lineage_entries_missing_data += 1
                continue
            # Skip if schema/table pattern don't allow the destination table
            destination_table_str = str(e.destinationTable.remove_extras())
            destination_table_str_parts = destination_table_str.split("/")
            if not self.config.schema_pattern.allowed(
                destination_table_str_parts[3]
            ) or not self.config.table_pattern.allowed(destination_table_str_parts[-1]):
                self.report.num_skipped_lineage_entries_not_allowed += 1
                continue
            has_table = False
            for ref_table in e.referencedTables:
                ref_table_str = str(ref_table.remove_extras())
                if ref_table_str != destination_table_str:
                    lineage_map[destination_table_str].add(ref_table_str)
                    has_table = True
            has_view = False
            for ref_view in e.referencedViews:
                ref_view_str = str(ref_view.remove_extras())
                if ref_view_str != destination_table_str:
                    lineage_map[destination_table_str].add(ref_view_str)
                    has_view = True
            if has_table and has_view:
                # If there is a view being referenced then bigquery sends both the view as well as underlying table
                # in the references. There is no distinction between direct/base objects accessed. So doing sql parsing
                # to ensure we only use direct objects accessed for lineage
                parser = BigQuerySQLParser(e.query)
                referenced_objs = set(
                    map(lambda x: x.split(".")[-1], parser.get_tables())
                )
                curr_lineage_str = lineage_map[destination_table_str]
                new_lineage_str = set()
                for lineage_str in curr_lineage_str:
                    name = lineage_str.split("/")[-1]
                    if name in referenced_objs:
                        new_lineage_str.add(lineage_str)
                lineage_map[destination_table_str] = new_lineage_str
            if not (has_table or has_view):
                self.report.num_skipped_lineage_entries_other += 1
        return lineage_map

    def get_latest_partition(
        self, schema: str, table: str
    ) -> Optional[BigQueryPartitionColumn]:
        url = self.config.get_sql_alchemy_url()
        engine = create_engine(url, **self.config.options)
        with engine.connect() as con:
            inspector = inspect(con)
            sql = BQ_GET_LATEST_PARTITION_TEMPLATE.format(
                project_id=self.get_db_name(inspector), schema=schema, table=table
            )
            result = con.execute(sql)
            # Bigquery only supports one partition column
            # https://stackoverflow.com/questions/62886213/adding-multiple-partitioned-columns-to-bigquery-table-from-sql-query
            row = result.fetchone()
            if row:
                return BigQueryPartitionColumn(**row)
            return None

    def get_shard_from_table(self, table: str) -> Tuple[str, Optional[str]]:
        match = re.search(SHARDED_TABLE_REGEX, table, re.IGNORECASE)
        if match:
            table_name = match.group(1)
            shard = match.group(2)
            return table_name, shard
        return table, None

    def is_latest_shard(self, project_id: str, schema: str, table: str) -> bool:
        # Getting latest shard from table names
        # https://cloud.google.com/bigquery/docs/partitioned-tables#dt_partition_shard
        table_name, shard = self.get_shard_from_table(table)
        if shard:
            logger.debug(f"{table_name} is sharded and shard id is: {shard}")
            url = self.config.get_sql_alchemy_url()
            engine = create_engine(url, **self.config.options)
            if f"{project_id}.{schema}.{table_name}" not in self.maximum_shard_ids:
                with engine.connect() as con:
                    sql = BQ_GET_LATEST_SHARD.format(
                        project_id=project_id,
                        schema=schema,
                        table=table_name,
                    )

                    result = con.execute(sql)
                    for row in result:
                        max_shard = row["max_shard"]
                        self.maximum_shard_ids[
                            f"{project_id}.{schema}.{table_name}"
                        ] = max_shard

                    logger.debug(f"Max shard for table {table_name} is {max_shard}")

            return (
                self.maximum_shard_ids[f"{project_id}.{schema}.{table_name}"] == shard
            )
        else:
            return True

    def add_information_for_schema(self, inspector: Inspector, schema: str) -> None:
        url = self.config.get_sql_alchemy_url()
        engine = create_engine(url, **self.config.options)
        project_id = self.get_db_name(inspector)
        with engine.connect() as con:
            inspector = inspect(con)
            sql = f"""
                select table_name, column_name
                from `{project_id}.{schema}.INFORMATION_SCHEMA.COLUMNS`
                where is_partitioning_column = 'YES';
            """
            result = con.execute(sql)
            for row in result.fetchall():
                table = row[0]
                partition_column = row[1]
                self.partition_info[f"{project_id}.{schema}.{table}"] = partition_column
        self.report.partition_info = self.partition_info

    def get_extra_tags(
        self, inspector: Inspector, schema: str, table: str
    ) -> Dict[str, List[str]]:
        extra_tags: Dict[str, List[str]] = {}
        project_id = self.get_db_name(inspector)

        partition_lookup_key = f"{project_id}.{schema}.{table}"
        if partition_lookup_key in self.partition_info:
            extra_tags[self.partition_info[partition_lookup_key]] = [
                Constants.TAG_PARTITION_KEY
            ]
        return extra_tags

    def generate_partition_profiler_query(
        self, schema: str, table: str, partition_datetime: Optional[datetime.datetime]
    ) -> Tuple[Optional[str], Optional[str]]:
        """
        Method returns partition id if table is partitioned or sharded and generate custom partition query for
        partitioned table.
        See more about partitioned tables at https://cloud.google.com/bigquery/docs/partitioned-tables
        """

        partition = self.get_latest_partition(schema, table)
        if partition:
            partition_where_clause: str
            logger.debug(f"{table} is partitioned and partition column is {partition}")
            (
                partition_datetime,
                upper_bound_partition_datetime,
            ) = get_partition_range_from_partition_id(
                partition.partition_id, partition_datetime
            )
            if partition.data_type in ("TIMESTAMP", "DATETIME"):
                partition_where_clause = "{column_name} BETWEEN '{partition_id}' AND '{upper_bound_partition_id}'".format(
                    column_name=partition.column_name,
                    partition_id=partition_datetime,
                    upper_bound_partition_id=upper_bound_partition_datetime,
                )
            elif partition.data_type == "DATE":
                partition_where_clause = "{column_name} = '{partition_id}'".format(
                    column_name=partition.column_name,
                    partition_id=partition_datetime.date(),
                )
            else:
                logger.warning(f"Not supported partition type {partition.data_type}")
                return None, None

            custom_sql = """
SELECT
    *
FROM
    `{table_catalog}.{table_schema}.{table_name}`
WHERE
    {partition_where_clause}
            """.format(
                table_catalog=partition.table_catalog,
                table_schema=partition.table_schema,
                table_name=partition.table_name,
                partition_where_clause=partition_where_clause,
            )

            return (partition.partition_id, custom_sql)
        else:
            # For sharded table we want to get the partition id but not needed to generate custom query
            table, shard = self.get_shard_from_table(table)
            if shard:
                return shard, None
        return None, None

    def is_dataset_eligible_for_profiling(
        self, dataset_name: str, sql_config: SQLAlchemyConfig
    ) -> bool:
        """
        Method overrides default profiling filter which checks profiling eligibility based on allow-deny pattern.
        This one also don't profile those sharded tables which are not the latest.
        """
        if not super().is_dataset_eligible_for_profiling(dataset_name, sql_config):
            return False

        (project_id, schema, table) = dataset_name.split(".")
        if not self.is_latest_shard(project_id=project_id, table=table, schema=schema):
            logger.debug(
                f"{dataset_name} is sharded but not the latest shard, skipping..."
            )
            return False
        return True

    @classmethod
    def create(cls, config_dict, ctx):
        config = BigQueryConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def add_config_to_report(self):
        self.report.start_time = self.config.start_time
        self.report.end_time = self.config.end_time
        self.report.include_table_lineage = self.config.include_table_lineage
        self.report.use_date_sharded_audit_log_tables = (
            self.config.use_date_sharded_audit_log_tables
        )
        self.report.log_page_size = self.config.log_page_size
        self.report.use_exported_bigquery_audit_metadata = (
            self.config.use_exported_bigquery_audit_metadata
        )
        self.report.use_v2_audit_metadata = self.config.use_v2_audit_metadata

    # Overriding the get_workunits method to first compute the workunits using the base SQLAlchemySource
    # and then computing lineage information only for those datasets that were ingested. This helps us to
    # maintain a clear separation between SQLAlchemySource and the BigQuerySource. Also, this way we honor
    # that flags like schema and table patterns for lineage computation as well.
    def get_workunits(self) -> Iterable[Union[MetadataWorkUnit, SqlWorkUnit]]:
        # only compute the lineage if the object is none. This is is safety check in case if in future refactoring we
        # end up computing lineage multiple times.
        self.add_config_to_report()
        if self.lineage_metadata is None:
            self._compute_big_query_lineage()
        with patch.dict(
            "sqlalchemy_bigquery._types._type_map",
            {"GEOGRAPHY": GEOGRAPHY},
            clear=False,
        ):
            for wu in super().get_workunits():
                yield wu
                if (
                    isinstance(wu, SqlWorkUnit)
                    and isinstance(wu.metadata, MetadataChangeEvent)
                    and isinstance(wu.metadata.proposedSnapshot, DatasetSnapshot)
                ):
                    lineage_mcp = self.get_lineage_mcp(wu.metadata.proposedSnapshot.urn)
                    if lineage_mcp is not None:
                        lineage_wu = MetadataWorkUnit(
                            id=f"{self.platform}-{lineage_mcp.entityUrn}-{lineage_mcp.aspectName}",
                            mcp=lineage_mcp,
                        )
                        yield lineage_wu
                        self.report.report_workunit(lineage_wu)

    def get_upstream_tables(
        self, bq_table: str, tables_seen: List[str] = []
    ) -> Set[BigQueryTableRef]:
        upstreams: Set[BigQueryTableRef] = set()
        assert self.lineage_metadata
        for ref_table in self.lineage_metadata[str(bq_table)]:
            upstream_table = BigQueryTableRef.from_string_name(ref_table)
            if upstream_table.is_temporary_table(self.config.temp_table_dataset_prefix):
                # making sure we don't process a table twice and not get into a recursive loop
                if ref_table in tables_seen:
                    logger.debug(
                        f"Skipping table {ref_table} because it was seen already"
                    )
                    continue
                tables_seen.append(ref_table)
                if ref_table in self.lineage_metadata:
                    upstreams = upstreams.union(
                        self.get_upstream_tables(ref_table, tables_seen=tables_seen)
                    )
            else:
                upstreams.add(upstream_table)
        return upstreams

    def get_lineage_mcp(
        self, dataset_urn: str
    ) -> Optional[MetadataChangeProposalWrapper]:
        if self.lineage_metadata is None:
            logger.debug("No lineage metadata so skipping getting mcp")
            return None
        dataset_key: Optional[DatasetKey] = mce_builder.dataset_urn_to_key(dataset_urn)
        if dataset_key is None:
            logger.debug(f"No dataset_key for {dataset_urn} so skipping getting mcp")
            return None
        project_id, dataset_name, tablename = dataset_key.name.split(".")
        bq_table = BigQueryTableRef(project_id, dataset_name, tablename)
        if str(bq_table) in self.lineage_metadata:
            upstream_list: List[UpstreamClass] = []
            # Sorting the list of upstream lineage events in order to avoid creating multiple aspects in backend
            # even if the lineage is same but the order is different.
            for upstream_table in sorted(
                self.get_upstream_tables(str(bq_table), tables_seen=[])
            ):
                upstream_table_class = UpstreamClass(
                    mce_builder.make_dataset_urn_with_platform_instance(
                        self.platform,
                        "{project}.{database}.{table}".format(
                            project=upstream_table.project,
                            database=upstream_table.dataset,
                            table=upstream_table.table,
                        ),
                        self.config.platform_instance,
                        self.config.env,
                    ),
                    DatasetLineageTypeClass.TRANSFORMED,
                )
                if self.config.upstream_lineage_in_report:
                    current_lineage_map: Set = self.report.upstream_lineage.get(
                        str(bq_table), set()
                    )
                    current_lineage_map.add(str(upstream_table))
                    self.report.upstream_lineage[str(bq_table)] = current_lineage_map
                upstream_list.append(upstream_table_class)

            if upstream_list:
                upstream_lineage = UpstreamLineageClass(upstreams=upstream_list)
                mcp = MetadataChangeProposalWrapper(
                    entityType="dataset",
                    changeType=ChangeTypeClass.UPSERT,
                    entityUrn=dataset_urn,
                    aspectName="upstreamLineage",
                    aspect=upstream_lineage,
                )
                return mcp
        return None

    def prepare_profiler_args(
        self,
        schema: str,
        table: str,
        partition: Optional[str],
        custom_sql: Optional[str] = None,
    ) -> dict:
        return dict(
            schema=self.config.project_id,
            table=f"{schema}.{table}",
            partition=partition,
            custom_sql=custom_sql,
        )

    @staticmethod
    @functools.lru_cache()
    def _get_project_id(inspector: Inspector) -> str:
        with inspector.bind.connect() as connection:
            project_id = connection.connection._client.project
            return project_id

    def normalise_dataset_name(self, dataset_name: str) -> str:
        (project_id, schema, table) = dataset_name.split(".")

        trimmed_table_name = (
            BigQueryTableRef.from_spec_obj(
                {"projectId": project_id, "datasetId": schema, "tableId": table}
            )
            .remove_extras()
            .table
        )
        return f"{project_id}.{schema}.{trimmed_table_name}"

    def get_identifier(
        self,
        *,
        schema: str,
        entity: str,
        inspector: Inspector,
        **kwargs: Any,
    ) -> str:
        assert inspector
        project_id = self._get_project_id(inspector)
        table_name = BigQueryTableRef.from_spec_obj(
            {"projectId": project_id, "datasetId": schema, "tableId": entity}
        ).table
        return f"{project_id}.{schema}.{table_name}"

    def standardize_schema_table_names(
        self, schema: str, entity: str
    ) -> Tuple[str, str]:
        # The get_table_names() method of the BigQuery driver returns table names
        # formatted as "<schema>.<table>" as the table name. Since later calls
        # pass both schema and table, schema essentially is passed in twice. As
        # such, one of the schema names is incorrectly interpreted as the
        # project ID. By removing the schema from the table name, we avoid this
        # issue.
        segments = entity.split(".")
        if len(segments) != 2:
            raise ValueError(f"expected table to contain schema name already {entity}")
        if segments[0] != schema:
            raise ValueError(f"schema {schema} does not match table {entity}")
        return segments[0], segments[1]

    def gen_schema_key(self, db_name: str, schema: str) -> PlatformKey:
        return BigQueryDatasetKey(
            project_id=db_name,
            dataset_id=schema,
            platform=self.platform,
            instance=self.config.platform_instance
            if self.config.platform_instance is not None
            else self.config.env,
        )

    def gen_database_key(self, database: str) -> PlatformKey:
        return ProjectIdKey(
            project_id=database,
            platform=self.platform,
            instance=self.config.platform_instance
            if self.config.platform_instance is not None
            else self.config.env,
        )

    def gen_database_containers(self, database: str) -> Iterable[MetadataWorkUnit]:
        domain_urn = self._gen_domain_urn(database)

        database_container_key = self.gen_database_key(database)

        container_workunits = gen_containers(
            container_key=database_container_key,
            name=database,
            sub_types=["Project"],
            domain_urn=domain_urn,
        )

        for wu in container_workunits:
            self.report.report_workunit(wu)
            yield wu

    def gen_schema_containers(
        self, schema: str, db_name: str
    ) -> Iterable[MetadataWorkUnit]:
        schema_container_key = self.gen_schema_key(db_name, schema)

        database_container_key = self.gen_database_key(database=db_name)

        container_workunits = gen_containers(
            schema_container_key,
            schema,
            ["Dataset"],
            database_container_key,
        )

        for wu in container_workunits:
            self.report.report_workunit(wu)
            yield wu
