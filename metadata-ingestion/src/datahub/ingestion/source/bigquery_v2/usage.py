import logging
import textwrap
import time
import traceback
import uuid
from dataclasses import dataclass
from datetime import datetime
from types import TracebackType
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    MutableMapping,
    Optional,
    Set,
    Tuple,
    Type,
    Union,
)

from google.cloud.bigquery import Client as BigQueryClient
from google.cloud.logging_v2.client import Client as GCPLoggingClient
from ratelimiter import RateLimiter

from datahub.configuration.time_window_config import get_time_bucket
from datahub.emitter.mce_builder import make_user_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.bigquery_v2.bigquery_audit import (
    BQ_AUDIT_V2,
    AuditEvent,
    AuditLogEntry,
    BigQueryAuditMetadata,
    BigQueryTableRef,
    QueryEvent,
    ReadEvent,
)
from datahub.ingestion.source.bigquery_v2.bigquery_config import BigQueryV2Config
from datahub.ingestion.source.bigquery_v2.bigquery_report import BigQueryV2Report
from datahub.ingestion.source.bigquery_v2.common import (
    BQ_DATE_SHARD_FORMAT,
    BQ_DATETIME_FORMAT,
    _make_gcp_logging_client,
)
from datahub.ingestion.source.usage.usage_common import (
    GenericAggregatedDataset,
    make_usage_workunit,
)
from datahub.metadata.schema_classes import OperationClass, OperationTypeClass
from datahub.utilities.file_backed_collections import ConnectionWrapper, FileBackedDict
from datahub.utilities.perf_timer import PerfTimer

logger: logging.Logger = logging.getLogger(__name__)

AggregatedDataset = GenericAggregatedDataset[BigQueryTableRef]

OPERATION_STATEMENT_TYPES = {
    "INSERT": OperationTypeClass.INSERT,
    "UPDATE": OperationTypeClass.UPDATE,
    "DELETE": OperationTypeClass.DELETE,
    "MERGE": OperationTypeClass.UPDATE,
    "CREATE": OperationTypeClass.CREATE,
    "CREATE_TABLE_AS_SELECT": OperationTypeClass.CREATE,
    "CREATE_SCHEMA": OperationTypeClass.CREATE,
    "DROP_TABLE": OperationTypeClass.DROP,
}

READ_STATEMENT_TYPES: List[str] = ["SELECT"]
AggregatedInfo = MutableMapping[datetime, Dict[BigQueryTableRef, AggregatedDataset]]


@dataclass(frozen=True, order=True)
class OperationalDataMeta:
    statement_type: str
    last_updated_timestamp: int
    actor_email: str
    custom_type: Optional[str] = None


def bigquery_audit_metadata_query_template(
    dataset: str,
    use_date_sharded_tables: bool,
    table_allow_filter: Optional[str] = None,
    limit: Optional[int] = None,
) -> str:
    """
    Receives a dataset (with project specified) and returns a query template that is used to query exported
    v2 AuditLogs containing protoPayloads of type BigQueryAuditMetadata.
    :param dataset: the dataset to query against in the form of $PROJECT.$DATASET
    :param use_date_sharded_tables: whether to read from date sharded audit log tables or time partitioned audit log
           tables
    :param table_allow_filter: regex used to filter on log events that contain the wanted datasets
    :return: a query template, when supplied start_time and end_time, can be used to query audit logs from BigQuery
    """
    allow_filter = f"""
      AND EXISTS (SELECT *
              from UNNEST(JSON_EXTRACT_ARRAY(protopayload_auditlog.metadataJson,
                                             "$.jobChange.job.jobStats.queryStats.referencedTables")) AS x
              where REGEXP_CONTAINS(x, r'(projects/.*/datasets/.*/tables/{table_allow_filter if table_allow_filter else ".*"})'))
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
            _TABLE_SUFFIX BETWEEN "{start_time}" AND "{end_time}"
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
        WHERE 1=1
        """
    audit_log_filter_timestamps = """AND (timestamp >= "{start_time}"
        AND timestamp < "{end_time}"
    );
    """
    audit_log_filter_query_complete = f"""
    AND (
            (
                protopayload_auditlog.serviceName="bigquery.googleapis.com"
                AND JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson, "$.jobChange.job.jobStatus.jobState") = "DONE"
                AND JSON_EXTRACT(protopayload_auditlog.metadataJson, "$.jobChange.job.jobConfig.queryConfig") IS NOT NULL
                {allow_filter}
            )
            OR
            JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson, "$.tableDataRead.reason") = "JOB"
    )
    """

    limit_text = f"limit {limit}" if limit else ""
    query = (
        textwrap.dedent(query)
        + audit_log_filter_query_complete
        + audit_log_filter_timestamps
        + limit_text
    )

    return textwrap.dedent(query)


class BigQueryUsageState:
    read_events: FileBackedDict[ReadEvent]
    query_events: FileBackedDict[QueryEvent]
    column_accesses: FileBackedDict[Tuple[str, str]]

    def __init__(self, config: BigQueryV2Config):
        self.conn = ConnectionWrapper()
        self.read_events = FileBackedDict[ReadEvent](
            connection=self.conn,
            tablename="read_events",
            extra_columns={
                "resource": lambda e: str(e.resource),
                "name": lambda e: e.jobName,
                "timestamp": lambda e: get_time_bucket(
                    e.timestamp, config.bucket_duration
                ),
                "user": lambda e: e.actor_email,
            },
        )
        # Keyed by job_name
        self.query_events = FileBackedDict[QueryEvent](
            connection=self.conn,
            tablename="query_events",
            extra_columns={
                "query": lambda e: e.query,
                "is_read": lambda e: int(e.statementType in READ_STATEMENT_TYPES),
            },
        )
        # Created just to store column accesses in sqlite for JOIN
        self.column_accesses = FileBackedDict[Tuple[str, str]](
            connection=self.conn,
            tablename="column_accesses",
            extra_columns={"read_event": lambda p: p[0], "field": lambda p: p[1]},
        )

    def __enter__(self) -> "BigQueryUsageState":
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        self.read_events.close()
        self.query_events.close()
        self.column_accesses.close()
        self.conn.close()

    def standalone_events(self) -> Iterable[AuditEvent]:
        for read_event in self.read_events.values():
            query_event = (
                self.query_events.get(read_event.jobName)
                if read_event.jobName
                else None
            )
            yield AuditEvent(read_event=read_event, query_event=query_event)
        for _, query_event in self.query_events.filtered_items("NOT is_read"):
            yield AuditEvent(query_event=query_event)

    def statistics_buckets(self) -> Iterable[Tuple[str, str]]:
        query = f"""
        SELECT DISTINCT
          timestamp,
          resource
        FROM {self.read_events.tablename}
        ORDER BY timestamp
        """
        return self.read_events.sql_query_iterator(query)  # type: ignore

    def query_count(self) -> int:
        query = f"""
        SELECT
          COUNT(q.query) count
        FROM
          {self.read_events.tablename} r
          LEFT JOIN {self.query_events.tablename} q ON r.name = q.key
        """
        return self.read_events.sql_query(query)[0][0]

    def query_freq(
        self, timestamp: str, resource: str, top_n: int
    ) -> List[Tuple[str, int]]:
        query = f"""
        SELECT
          q.query,
          COUNT(r.key) count
        FROM
          {self.read_events.tablename} r
          LEFT JOIN {self.query_events.tablename} q ON r.name = q.key
        WHERE r.timestamp = ? AND r.resource = ?
        GROUP BY q.query
        ORDER BY count DESC
        LIMIT {top_n}
        """
        return self.read_events.sql_query(  # type: ignore
            query, params=(timestamp, resource), refs=[self.query_events]
        )

    def user_frequencies(self, timestamp: str, resource: str) -> List[Tuple[str, int]]:
        query = f"""
        SELECT
          r.user,
          COUNT(r.key) count
        FROM
          {self.read_events.tablename} r
        WHERE r.timestamp = ? AND r.resource = ?
        GROUP BY r.user
        """
        return self.read_events.sql_query(query, params=(timestamp, resource))  # type: ignore

    def column_frequencies(
        self, timestamp: str, resource: str
    ) -> List[Tuple[str, int]]:
        query = f"""
        SELECT
          c.field,
          COUNT(r.key) count
        FROM
          {self.read_events.tablename} r
          RIGHT JOIN {self.column_accesses.tablename} c ON r.key = c.read_event
        WHERE r.timestamp = ? AND r.resource = ?
        GROUP BY c.field
        """
        return self.read_events.sql_query(query, params=(timestamp, resource))  # type: ignore


class BigQueryUsageExtractor:
    """
    This plugin extracts the following:
    * Statistics on queries issued and tables and columns accessed (excludes views)
    * Aggregation of these statistics into buckets, by day or hour granularity

    :::note
    1. Depending on the compliance policies setup for the bigquery instance, sometimes logging.read permission is not sufficient. In that case, use either admin or private log viewer permission.
    :::
    """

    def __init__(self, config: BigQueryV2Config, report: BigQueryV2Report):
        self.config: BigQueryV2Config = config
        self.report: BigQueryV2Report = report

    def _is_table_allowed(self, table_ref: Optional[BigQueryTableRef]) -> bool:
        return (
            table_ref is not None
            and self.config.dataset_pattern.allowed(table_ref.table_identifier.dataset)
            and self.config.table_pattern.allowed(table_ref.table_identifier.table)
        )

    def run(
        self, projects: Iterable[str], table_refs: Set[str]
    ) -> Iterable[MetadataWorkUnit]:
        try:
            with BigQueryUsageState(self.config) as usage_state:
                yield from self._run(projects, table_refs, usage_state)
        except Exception:
            logger.error(f"Error processing usage", exc_info=True)

    def _run(
        self,
        projects: Iterable[str],
        table_refs: Set[str],
        usage_state: BigQueryUsageState,
    ) -> Iterable[MetadataWorkUnit]:
        num_aggregated = 0
        for project in projects:
            for audit_event in self.get_usage_events(project):
                try:
                    num_aggregated += self.store_usage_event(
                        audit_event, usage_state, table_refs
                    )
                except Exception:
                    logger.warning(
                        f"Unable to store usage event {audit_event}", exc_info=True
                    )
        logger.info(f"Total number of events aggregated = {num_aggregated}.")

        for timestamp, resource in usage_state.statistics_buckets():
            try:
                resource_ref = BigQueryTableRef.from_string_name(resource)
                query_freq = (
                    usage_state.query_freq(
                        timestamp, resource, self.config.usage.top_n_queries
                    )
                    if self.config.usage.include_top_n_queries
                    else None
                )
                yield make_usage_workunit(
                    bucket_start_time=datetime.fromisoformat(timestamp),
                    resource=resource_ref,
                    query_count=usage_state.query_count(),
                    query_freq=query_freq,
                    user_freq=usage_state.user_frequencies(timestamp, resource),
                    column_freq=usage_state.column_frequencies(timestamp, resource),
                    bucket_duration=self.config.bucket_duration,
                    urn_builder=lambda resource: resource.to_urn(self.config.env),
                    top_n_queries=self.config.usage.top_n_queries,
                    format_sql_queries=self.config.usage.format_sql_queries,
                )
                self.report.num_usage_workunits_emitted += 1
            except Exception:
                logger.warning(
                    f"Unable to generate usage workunit for bucket {timestamp}, {resource}",
                    exc_info=True,
                )

        if self.config.usage.include_operational_stats:
            for audit_event in usage_state.standalone_events():
                try:
                    operational_wu = self._create_operation_workunit(audit_event)
                    if operational_wu:
                        yield operational_wu
                        self.report.num_operational_stats_workunits_emitted += 1
                except Exception:
                    logger.warning(
                        f"Unable to generate operation workunit for event {audit_event}",
                        exc_info=True,
                    )

    def get_usage_events(self, project_id: str) -> Iterable[AuditEvent]:
        with PerfTimer() as timer:
            try:
                yield from self._get_parsed_bigquery_log_events(project_id)
            except Exception as e:
                self.report.usage_failed_extraction.append(project_id)
                self.report.report_failure("usage-extraction", str(e))
                trace = traceback.format_exc()
                logger.error(
                    f"Error getting usage events for project {project_id} due to error {e}, trace: {trace}"
                )

            self.report.usage_extraction_sec[project_id] = round(
                timer.elapsed_seconds(), 2
            )

    def store_usage_event(
        self,
        event: AuditEvent,
        usage_state: BigQueryUsageState,
        table_refs: Set[str],
    ) -> bool:
        """Stores a usage event in `usage_state` and returns if an event was successfully processed."""
        if event.read_event and (
            self.config.start_time <= event.read_event.timestamp < self.config.end_time
        ):
            resource = event.read_event.resource
            if str(resource) not in table_refs:
                logger.debug(f"Skipping non existing {resource} from usage")
                return False
            elif resource.is_temporary_table([self.config.temp_table_dataset_prefix]):
                logger.debug(f"Dropping temporary table {resource}")
                self.report.report_dropped(str(resource))
                return False

            # Use uuid keys to store all entries -- no overwriting
            key = str(uuid.uuid4())
            usage_state.read_events[key] = event.read_event
            for field in event.read_event.fieldsRead:
                usage_state.column_accesses[str(uuid.uuid4())] = (
                    key,
                    field,
                )
            return True
        elif event.query_event and event.query_event.job_name:
            usage_state.query_events[event.query_event.job_name] = event.query_event
            return True
        return False

    def _get_exported_bigquery_audit_metadata(
        self,
        bigquery_client: BigQueryClient,
        allow_filter: str,
    ) -> Iterable[BigQueryAuditMetadata]:
        if self.config.bigquery_audit_metadata_datasets is None:
            return

        start_time: str = (
            self.config.start_time - self.config.max_query_duration
        ).strftime(
            BQ_DATE_SHARD_FORMAT
            if self.config.use_date_sharded_audit_log_tables
            else BQ_DATETIME_FORMAT
        )
        self.report.audit_start_time = start_time

        end_time: str = (
            self.config.end_time + self.config.max_query_duration
        ).strftime(
            BQ_DATE_SHARD_FORMAT
            if self.config.use_date_sharded_audit_log_tables
            else BQ_DATETIME_FORMAT
        )
        self.report.audit_end_time = end_time

        for dataset in self.config.bigquery_audit_metadata_datasets:
            logger.info(
                f"Start loading log entries from BigQueryAuditMetadata in {dataset}"
            )

            query = bigquery_audit_metadata_query_template(
                dataset, self.config.use_date_sharded_audit_log_tables, allow_filter
            ).format(
                start_time=start_time,
                end_time=end_time,
            )

            query_job = bigquery_client.query(query)
            logger.info(
                f"Finished loading log entries from BigQueryAuditMetadata in {dataset}"
            )
            if self.config.rate_limit:
                with RateLimiter(max_calls=self.config.requests_per_min, period=60):
                    yield from query_job
            else:
                yield from query_job

    def _get_bigquery_log_entries_via_gcp_logging(
        self, client: GCPLoggingClient, limit: Optional[int] = None
    ) -> Iterable[Union[AuditLogEntry, BigQueryAuditMetadata]]:
        filter = self._generate_filter(BQ_AUDIT_V2)
        logger.debug(filter)

        try:
            list_entries: Iterable[Union[AuditLogEntry, BigQueryAuditMetadata]]
            rate_limiter: Optional[RateLimiter] = None
            if self.config.rate_limit:
                # client.list_entries is a generator, does api calls to GCP Logging when it runs out of entries and needs to fetch more from GCP Logging
                # to properly ratelimit we multiply the page size by the number of requests per minute
                rate_limiter = RateLimiter(
                    max_calls=self.config.requests_per_min * self.config.log_page_size,
                    period=60,
                )

            list_entries = client.list_entries(
                filter_=filter,
                page_size=self.config.log_page_size,
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
                self.report.total_query_log_entries += 1

                if rate_limiter:
                    with rate_limiter:
                        yield entry
                else:
                    yield entry

            logger.info(
                f"Finished loading {self.report.total_query_log_entries} log entries from GCP Logging for {client.project}"
            )

        except Exception as e:
            logger.warning(
                f"Encountered exception retrieving AuditLogEntires for project {client.project} - {e}"
            )
            self.report.report_failure(
                "usage-extraction",
                f"{client.project} - unable to retrive log entrires {e}",
            )

    def _generate_filter(self, audit_templates: Dict[str, str]) -> str:
        # We adjust the filter values a bit, since we need to make sure that the join
        # between query events and read events is complete. For example, this helps us
        # handle the case where the read happens within our time range but the query
        # completion event is delayed and happens after the configured end time.
        # Can safely access the first index of the allow list as it by default contains ".*"
        use_allow_filter = self.config.table_pattern and (
            len(self.config.table_pattern.allow) > 1
            or self.config.table_pattern.allow[0] != ".*"
        )
        use_deny_filter = self.config.table_pattern and self.config.table_pattern.deny
        allow_regex = (
            audit_templates["BQ_FILTER_REGEX_ALLOW_TEMPLATE"].format(
                table_allow_pattern=self.config.get_table_pattern(
                    self.config.table_pattern.allow
                )
            )
            if use_allow_filter
            else ""
        )
        deny_regex = (
            audit_templates["BQ_FILTER_REGEX_DENY_TEMPLATE"].format(
                table_deny_pattern=self.config.get_table_pattern(
                    self.config.table_pattern.deny
                ),
                logical_operator="AND" if use_allow_filter else "",
            )
            if use_deny_filter
            else ("" if use_allow_filter else "FALSE")
        )

        logger.debug(
            f"use_allow_filter={use_allow_filter}, use_deny_filter={use_deny_filter}, "
            f"allow_regex={allow_regex}, deny_regex={deny_regex}"
        )
        start_time = (self.config.start_time - self.config.max_query_duration).strftime(
            BQ_DATETIME_FORMAT
        )
        self.report.log_entry_start_time = start_time
        end_time = (self.config.end_time + self.config.max_query_duration).strftime(
            BQ_DATETIME_FORMAT
        )
        self.report.log_entry_end_time = end_time
        filter = audit_templates["BQ_FILTER_RULE_TEMPLATE"].format(
            start_time=start_time,
            end_time=end_time,
            allow_regex=allow_regex,
            deny_regex=deny_regex,
        )
        return filter

    @staticmethod
    def _get_destination_table(event: AuditEvent) -> Optional[BigQueryTableRef]:
        if (
            not event.read_event
            and event.query_event
            and event.query_event.destinationTable
        ):
            return event.query_event.destinationTable.get_sanitized_table_ref()
        elif event.read_event:
            return event.read_event.resource.get_sanitized_table_ref()
        else:
            # TODO: CREATE_SCHEMA operation ends up here, maybe we should capture that as well
            # but it is tricky as we only get the query so it can't be tied to anything
            # - SCRIPT statement type ends up here as well
            logger.debug(f"Unable to find destination table in event {event}")
            return None

    def _extract_operational_meta(
        self, event: AuditEvent
    ) -> Optional[OperationalDataMeta]:
        # If we don't have Query object that means this is a queryless read operation or a read operation which was not executed as JOB
        # https://cloud.google.com/bigquery/docs/reference/auditlogs/rest/Shared.Types/BigQueryAuditMetadata.TableDataRead.Reason/
        if not event.query_event and event.read_event:
            return OperationalDataMeta(
                statement_type=OperationTypeClass.CUSTOM,
                custom_type="CUSTOM_READ",
                last_updated_timestamp=int(
                    event.read_event.timestamp.timestamp() * 1000
                ),
                actor_email=event.read_event.actor_email,
            )
        elif event.query_event:
            custom_type = None
            # If AuditEvent only have queryEvent that means it is the target of the Insert Operation
            if (
                event.query_event.statementType in OPERATION_STATEMENT_TYPES
                and not event.read_event
            ):
                statement_type = OPERATION_STATEMENT_TYPES[
                    event.query_event.statementType
                ]
            # We don't have SELECT in OPERATION_STATEMENT_TYPES , so those queries will end up here
            # and this part should capture those operation types as well which we don't have in our mapping
            else:
                statement_type = OperationTypeClass.CUSTOM
                custom_type = event.query_event.statementType

            self.report.operation_types_stat[event.query_event.statementType] = (
                self.report.operation_types_stat.get(event.query_event.statementType, 0)
                + 1
            )
            return OperationalDataMeta(
                statement_type=statement_type,
                custom_type=custom_type,
                last_updated_timestamp=int(
                    event.query_event.timestamp.timestamp() * 1000
                ),
                actor_email=event.query_event.actor_email,
            )
        else:
            return None

    def _create_operation_workunit(
        self, event: AuditEvent
    ) -> Optional[MetadataWorkUnit]:
        if not event.read_event and not event.query_event:
            return None

        destination_table = self._get_destination_table(event)
        if destination_table is None:
            return None

        if not self._is_table_allowed(destination_table):
            return None

        operational_meta = self._extract_operational_meta(event)
        if not operational_meta:
            return None

        if not self.config.usage.include_read_operational_stats and (
            operational_meta.statement_type not in OPERATION_STATEMENT_TYPES.values()
        ):
            return None

        reported_time: int = int(time.time() * 1000)
        affected_datasets = []
        if event.query_event and event.query_event.referencedTables:
            for table in event.query_event.referencedTables:
                try:
                    affected_datasets.append(
                        table.get_sanitized_table_ref().to_urn(self.config.env)
                    )
                except Exception as e:
                    self.report.report_warning(
                        str(table),
                        f"Failed to clean up table, {e}",
                    )

        operation_aspect = OperationClass(
            timestampMillis=reported_time,
            lastUpdatedTimestamp=operational_meta.last_updated_timestamp,
            actor=make_user_urn(operational_meta.actor_email.split("@")[0]),
            operationType=operational_meta.statement_type,
            customOperationType=operational_meta.custom_type,
            affectedDatasets=affected_datasets,
        )

        if self.config.usage.include_read_operational_stats:
            operation_aspect.customProperties = (
                self._create_operational_custom_properties(event)
            )
            if event.query_event and event.query_event.numAffectedRows:
                operation_aspect.numAffectedRows = event.query_event.numAffectedRows

        return MetadataChangeProposalWrapper(
            entityUrn=destination_table.to_urn(env=self.config.env),
            aspect=operation_aspect,
        ).as_workunit()

    def _create_operational_custom_properties(
        self, event: AuditEvent
    ) -> Dict[str, str]:
        custom_properties: Dict[str, str] = {}
        # This only needs for backward compatibility reason. To make sure we generate the same operational metadata than before
        if self.config.usage.include_read_operational_stats:
            if event.query_event:
                if event.query_event.end_time and event.query_event.start_time:
                    custom_properties["millisecondsTaken"] = str(
                        int(event.query_event.end_time.timestamp() * 1000)
                        - int(event.query_event.start_time.timestamp() * 1000)
                    )

                if event.query_event.job_name:
                    custom_properties["sessionId"] = event.query_event.job_name

                custom_properties["text"] = event.query_event.query

                if event.query_event.billed_bytes:
                    custom_properties["bytesProcessed"] = str(
                        event.query_event.billed_bytes
                    )

                if event.query_event.default_dataset:
                    custom_properties[
                        "defaultDatabase"
                    ] = event.query_event.default_dataset
            if event.read_event:
                if event.read_event.readReason:
                    custom_properties["readReason"] = event.read_event.readReason

                if event.read_event.fieldsRead:
                    custom_properties["fieldsRead"] = ",".join(
                        event.read_event.fieldsRead
                    )

        return custom_properties

    def _parse_bigquery_log_entry(
        self, entry: Union[AuditLogEntry, BigQueryAuditMetadata]
    ) -> Optional[AuditEvent]:
        event: Optional[Union[ReadEvent, QueryEvent]] = None

        missing_read_entry = ReadEvent.get_missing_key_entry(entry)
        if missing_read_entry is None:
            event = ReadEvent.from_entry(entry, self.config.debug_include_full_payloads)
            if not self._is_table_allowed(event.resource):
                self.report.num_filtered_read_events += 1
                return None

            if event.readReason:
                self.report.read_reasons_stat[event.readReason] = (
                    self.report.read_reasons_stat.get(event.readReason, 0) + 1
                )
            self.report.num_read_events += 1

        missing_query_entry = QueryEvent.get_missing_key_entry(entry)
        if event is None and missing_query_entry is None:
            event = QueryEvent.from_entry(entry)
            self.report.num_query_events += 1

        missing_query_entry_v2 = QueryEvent.get_missing_key_entry_v2(entry)

        if event is None and missing_query_entry_v2 is None:
            event = QueryEvent.from_entry_v2(
                entry, self.config.debug_include_full_payloads
            )
            self.report.num_query_events += 1

        if event is None:
            reason = f"Unable to parse {type(entry)} missing read {missing_read_entry}, missing query {missing_query_entry} missing v2 {missing_query_entry_v2} for {entry}"
            self.report.report_warning("usage-extraction", reason)
            logger.warning(reason)
            return None

        return AuditEvent.create(event)

    def _parse_exported_bigquery_audit_metadata(
        self, audit_metadata: BigQueryAuditMetadata
    ) -> Optional[AuditEvent]:
        event: Optional[Union[QueryEvent, ReadEvent]] = None

        missing_read_event = ReadEvent.get_missing_key_exported_bigquery_audit_metadata(
            audit_metadata
        )
        if missing_read_event is None:
            event = ReadEvent.from_exported_bigquery_audit_metadata(
                audit_metadata, self.config.debug_include_full_payloads
            )
            if not self._is_table_allowed(event.resource):
                self.report.num_filtered_read_events += 1
                return None
            if event.readReason:
                self.report.read_reasons_stat[event.readReason] = (
                    self.report.read_reasons_stat.get(event.readReason, 0) + 1
                )
            self.report.num_read_events += 1

        missing_query_event = (
            QueryEvent.get_missing_key_exported_bigquery_audit_metadata(audit_metadata)
        )
        if event is None and missing_query_event is None:
            event = QueryEvent.from_exported_bigquery_audit_metadata(
                audit_metadata, self.config.debug_include_full_payloads
            )
            self.report.num_query_events += 1

        if event is None:
            reason = f"{audit_metadata['logName']}-{audit_metadata['insertId']} Unable to parse audit metadata missing QueryEvent keys:{str(missing_query_event)} ReadEvent keys: {str(missing_read_event)} for {audit_metadata}"
            self.report.report_warning("usage-extraction", reason)
            logger.warning(reason)
            return None

        return AuditEvent.create(event)

    def _get_parsed_bigquery_log_events(
        self, project_id: str, limit: Optional[int] = None
    ) -> Iterable[AuditEvent]:
        parse_fn: Callable[[Any], Optional[AuditEvent]]
        if self.config.use_exported_bigquery_audit_metadata:
            _client: BigQueryClient = BigQueryClient(project=project_id)
            entries = self._get_exported_bigquery_audit_metadata(
                bigquery_client=_client,
                allow_filter=self.config.get_table_pattern(
                    self.config.table_pattern.allow
                ),
            )
            parse_fn = self._parse_exported_bigquery_audit_metadata
        else:
            logging_client: GCPLoggingClient = _make_gcp_logging_client(
                project_id, self.config.extra_client_options
            )
            entries = self._get_bigquery_log_entries_via_gcp_logging(
                logging_client, limit=limit
            )
            parse_fn = self._parse_bigquery_log_entry

        for entry in entries:
            try:
                event = parse_fn(entry)
                if event:
                    yield event
            except Exception as e:
                reason = f"Unable to parse log event `{entry}`: {e}"
                self.report.report_warning("usage-extraction", reason)
                logger.warning(reason)

    def test_capability(self, project_id: str) -> None:
        for entry in self._get_parsed_bigquery_log_events(project_id, limit=1):
            logger.debug(f"Connection test got one {entry}")
            return
