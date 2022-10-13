import collections
import logging
import textwrap
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Iterable, List, MutableMapping, Optional, Set, Union, cast

import cachetools
from google.cloud.bigquery import Client as BigQueryClient
from google.cloud.logging_v2.client import Client as GCPLoggingClient
from more_itertools import partition
from ratelimiter import RateLimiter

from datahub.configuration.time_window_config import get_time_bucket
from datahub.emitter.mce_builder import make_user_urn
from datahub.emitter.mcp_builder import wrap_aspect_as_workunit
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
from datahub.ingestion.source.usage.usage_common import GenericAggregatedDataset
from datahub.metadata.schema_classes import OperationClass, OperationTypeClass
from datahub.utilities.delayed_iter import delayed_iter
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


@dataclass(frozen=True, order=True)
class OperationalDataMeta:
    statement_type: str
    last_updated_timestamp: int
    actor_email: str
    custom_type: Optional[str] = None


def bigquery_audit_metadata_query_template(
    dataset: str,
    use_date_sharded_tables: bool,
    table_allow_filter: str = None,
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


class BigQueryUsageExtractor:
    """
    This plugin extracts the following:
    * Statistics on queries issued and tables and columns accessed (excludes views)
    * Aggregation of these statistics into buckets, by day or hour granularity

    :::note
    1. This source only does usage statistics. To get the tables, views, and schemas in your BigQuery project, use the `bigquery` plugin.
    2. Depending on the compliance policies setup for the bigquery instance, sometimes logging.read permission is not sufficient. In that case, use either admin or private log viewer permission.
    :::
    """

    aggregated_info: Dict[
        datetime, Dict[BigQueryTableRef, AggregatedDataset]
    ] = collections.defaultdict(dict)

    def __init__(self, config: BigQueryV2Config, report: BigQueryV2Report):
        self.config: BigQueryV2Config = config
        self.report: BigQueryV2Report = report

    def add_config_to_report(self):
        self.report.query_log_delay = self.config.usage.query_log_delay

    def _is_table_allowed(self, table_ref: Optional[BigQueryTableRef]) -> bool:
        return (
            table_ref is not None
            and self.config.dataset_pattern.allowed(table_ref.table_identifier.dataset)
            and self.config.table_pattern.allowed(table_ref.table_identifier.table)
        )

    def generate_usage_for_project(self, project_id: str) -> Iterable[MetadataWorkUnit]:
        parsed_bigquery_log_events: Iterable[
            Union[ReadEvent, QueryEvent, MetadataWorkUnit]
        ]
        with PerfTimer() as timer:
            try:
                bigquery_log_entries = self._get_parsed_bigquery_log_events(project_id)
                if self.config.use_exported_bigquery_audit_metadata:
                    parsed_bigquery_log_events = (
                        self._parse_exported_bigquery_audit_metadata(
                            bigquery_log_entries
                        )
                    )
                else:
                    parsed_bigquery_log_events = self._parse_bigquery_log_entries(
                        bigquery_log_entries
                    )

                parsed_events_uncasted: Iterable[
                    Union[ReadEvent, QueryEvent, MetadataWorkUnit]
                ]
                last_updated_work_units_uncasted: Iterable[
                    Union[ReadEvent, QueryEvent, MetadataWorkUnit]
                ]
                parsed_events_uncasted, last_updated_work_units_uncasted = partition(
                    lambda x: isinstance(x, MetadataWorkUnit),
                    parsed_bigquery_log_events,
                )
                parsed_events: Iterable[Union[ReadEvent, QueryEvent]] = cast(
                    Iterable[Union[ReadEvent, QueryEvent]], parsed_events_uncasted
                )

                hydrated_read_events = self._join_events_by_job_id(parsed_events)
                # storing it all in one big object.

                # TODO: handle partitioned tables

                # TODO: perhaps we need to continuously prune this, rather than
                num_aggregated: int = 0
                self.report.num_operational_stats_workunits_emitted = 0
                for event in hydrated_read_events:
                    if self.config.usage.include_operational_stats:
                        operational_wu = self._create_operation_aspect_work_unit(event)
                        if operational_wu:
                            self.report.report_workunit(operational_wu)
                            yield operational_wu
                            self.report.num_operational_stats_workunits_emitted += 1
                    if event.read_event:
                        self.aggregated_info = self._aggregate_enriched_read_events(
                            self.aggregated_info, event
                        )
                        num_aggregated += 1
                logger.info(f"Total number of events aggregated = {num_aggregated}.")
                bucket_level_stats: str = "\n\t" + "\n\t".join(
                    [
                        f'bucket:{db.strftime("%m-%d-%Y:%H:%M:%S")}, size={len(ads)}'
                        for db, ads in self.aggregated_info.items()
                    ]
                )
                logger.debug(
                    f"Number of buckets created = {len(self.aggregated_info)}. Per-bucket details:{bucket_level_stats}"
                )

                self.report.usage_extraction_sec[project_id] = round(
                    timer.elapsed_seconds(), 2
                )
            except Exception as e:
                self.report.usage_failed_extraction.append(project_id)
                logger.error(
                    f"Error getting usage for project {project_id} due to error {e}"
                )

    def _get_bigquery_log_entries_via_exported_bigquery_audit_metadata(
        self, client: BigQueryClient
    ) -> Iterable[BigQueryAuditMetadata]:
        try:
            list_entries: Iterable[
                BigQueryAuditMetadata
            ] = self._get_exported_bigquery_audit_metadata(
                client, self.config.get_table_pattern(self.config.table_pattern.allow)
            )
            i: int = 0
            for i, entry in enumerate(list_entries):
                if i == 0:
                    logger.info(
                        f"Starting log load from BigQuery for project {client.project}"
                    )
                yield entry

            logger.info(
                f"Finished loading {i} log entries from BigQuery for project {client.project}"
            )

        except Exception as e:
            logger.warning(
                f"Encountered exception retrieving AuditLogEntries for project {client.project}",
                e,
            )
            self.report.report_failure(
                f"{client.project}", f"unable to retrieve log entries {e}"
            )

    def _get_exported_bigquery_audit_metadata(
        self,
        bigquery_client: BigQueryClient,
        allow_filter: str,
        limit: Optional[int] = None,
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
        self.report.total_query_log_entries = 0

        filter = self._generate_filter(BQ_AUDIT_V2)
        logger.debug(filter)

        try:
            list_entries: Iterable[Union[AuditLogEntry, BigQueryAuditMetadata]]
            if self.config.rate_limit:
                with RateLimiter(max_calls=self.config.requests_per_min, period=60):
                    list_entries = client.list_entries(
                        filter_=filter, page_size=self.config.log_page_size
                    )
            else:
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
                self.report.total_query_log_entries += 1
                yield entry

            logger.info(
                f"Finished loading {self.report.total_query_log_entries} log entries from GCP Logging for {client.project}"
            )

        except Exception as e:
            logger.warning(
                f"Encountered exception retrieving AuditLogEntires for project {client.project}",
                e,
            )
            self.report.report_failure(
                f"{client.project}", f"unable to retrive log entrires {e}"
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
            logger.warning(f"Unable to find destination table in event {event}")
            return None

    def _extract_operational_meta(
        self, event: AuditEvent
    ) -> Optional[OperationalDataMeta]:
        # If we don't have Query object that means this is a queryless read operation or a read operation which was not executed as JOB
        # https://cloud.google.com/bigquery/docs/reference/auditlogs/rest/Shared.Types/BigQueryAuditMetadata.TableDataRead.Reason/
        operation_meta: OperationalDataMeta
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

    def _create_operation_aspect_work_unit(
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

        return wrap_aspect_as_workunit(
            "dataset",
            destination_table.to_urn(
                env=self.config.env,
            ),
            "operation",
            operation_aspect,
        )

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

    def _parse_bigquery_log_entries(
        self, entries: Iterable[Union[AuditLogEntry, BigQueryAuditMetadata]]
    ) -> Iterable[Union[ReadEvent, QueryEvent]]:
        self.report.num_read_events = 0
        self.report.num_query_events = 0
        self.report.num_filtered_read_events = 0
        self.report.num_filtered_query_events = 0
        for entry in entries:
            event: Optional[Union[ReadEvent, QueryEvent]] = None

            missing_read_entry = ReadEvent.get_missing_key_entry(entry)
            if missing_read_entry is None:
                event = ReadEvent.from_entry(
                    entry, self.config.debug_include_full_payloads
                )
                if not self._is_table_allowed(event.resource):
                    self.report.num_filtered_read_events += 1
                    continue

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
                self.error(
                    logger,
                    f"{entry.log_name}-{entry.insert_id}",
                    f"Unable to parse {type(entry)} missing read {missing_query_entry}, missing query {missing_query_entry} missing v2 {missing_query_entry_v2} for {entry}",
                )
            else:
                yield event

        logger.info(
            f"Parsed {self.report.num_read_events} ReadEvents and {self.report.num_query_events} QueryEvents"
        )

    def _parse_exported_bigquery_audit_metadata(
        self, audit_metadata_rows: Iterable[BigQueryAuditMetadata]
    ) -> Iterable[Union[ReadEvent, QueryEvent, MetadataWorkUnit]]:
        for audit_metadata in audit_metadata_rows:
            event: Optional[Union[QueryEvent, ReadEvent]] = None
            missing_query_event_exported_audit = (
                QueryEvent.get_missing_key_exported_bigquery_audit_metadata(
                    audit_metadata
                )
            )
            if missing_query_event_exported_audit is None:
                event = QueryEvent.from_exported_bigquery_audit_metadata(
                    audit_metadata, self.config.debug_include_full_payloads
                )

            missing_read_event_exported_audit = (
                ReadEvent.get_missing_key_exported_bigquery_audit_metadata(
                    audit_metadata
                )
            )
            if missing_read_event_exported_audit is None:
                event = ReadEvent.from_exported_bigquery_audit_metadata(
                    audit_metadata, self.config.debug_include_full_payloads
                )

            if event is not None:
                yield event
            else:
                self.error(
                    logger,
                    f"{audit_metadata['logName']}-{audit_metadata['insertId']}",
                    f"Unable to parse audit metadata missing "
                    f"QueryEvent keys:{str(missing_query_event_exported_audit)},"
                    f" ReadEvent keys: {str(missing_read_event_exported_audit)} for {audit_metadata}",
                )

    def error(self, log: logging.Logger, key: str, reason: str) -> Any:
        self.report.report_failure(key, reason)
        log.error(f"{key} => {reason}")

    def _join_events_by_job_id(
        self, events: Iterable[Union[ReadEvent, QueryEvent]]
    ) -> Iterable[AuditEvent]:
        # If caching eviction is enabled, we only store the most recently used query events,
        # which are used when resolving job information within the read events.
        query_jobs: MutableMapping[str, QueryEvent]
        if self.config.usage.query_log_delay:
            query_jobs = cachetools.LRUCache(
                maxsize=5 * self.config.usage.query_log_delay
            )
        else:
            query_jobs = {}

        def event_processor(
            events: Iterable[Union[ReadEvent, QueryEvent]]
        ) -> Iterable[AuditEvent]:
            for event in events:
                if isinstance(event, QueryEvent):
                    if event.job_name:
                        query_jobs[event.job_name] = event
                        # For Insert operations we yield the query event as it is possible
                        # there won't be any read event.
                        if event.statementType not in READ_STATEMENT_TYPES:
                            yield AuditEvent(query_event=event)
                        # If destination table exists we yield the query event as it is insert operation
                else:
                    yield AuditEvent(read_event=event)

        # TRICKY: To account for the possibility that the query event arrives after
        # the read event in the audit logs, we wait for at least `query_log_delay`
        # additional events to be processed before attempting to resolve BigQuery
        # job information from the logs. If `query_log_delay` is None, it gets treated
        # as an unlimited delay, which prioritizes correctness at the expense of memory usage.
        original_read_events = event_processor(events)
        delayed_read_events = delayed_iter(
            original_read_events, self.config.usage.query_log_delay
        )

        num_joined: int = 0
        for event in delayed_read_events:
            # If event_processor yields a query event which is an insert operation
            # then we should just yield it.
            if event.query_event and not event.read_event:
                yield event
                continue
            if (
                event.read_event is None
                or event.read_event.timestamp < self.config.start_time
                or event.read_event.timestamp >= self.config.end_time
                or not self._is_table_allowed(event.read_event.resource)
            ):
                continue

            # There are some read event which does not have jobName because it was read in a different way
            # Like https://cloud.google.com/logging/docs/reference/audit/bigquery/rest/Shared.Types/AuditData#tabledatalistrequest
            # There are various reason to read a table
            # https://cloud.google.com/bigquery/docs/reference/auditlogs/rest/Shared.Types/BigQueryAuditMetadata.TableDataRead.Reason
            if event.read_event.jobName:
                if event.read_event.jobName in query_jobs:
                    # Join the query log event into the table read log event.
                    num_joined += 1
                    event.query_event = query_jobs[event.read_event.jobName]
                else:
                    self.report.report_warning(
                        str(event.read_event.resource),
                        f"Failed to match table read event {event.read_event.jobName} with reason {event.read_event.readReason} with job at {event.read_event.timestamp}; try increasing `query_log_delay` or `max_query_duration`",
                    )
            yield event
        logger.info(f"Number of read events joined with query events: {num_joined}")

    def _aggregate_enriched_read_events(
        self,
        datasets: Dict[datetime, Dict[BigQueryTableRef, AggregatedDataset]],
        event: AuditEvent,
    ) -> Dict[datetime, Dict[BigQueryTableRef, AggregatedDataset]]:
        if not event.read_event:
            return datasets

        floored_ts = get_time_bucket(
            event.read_event.timestamp, self.config.bucket_duration
        )
        resource: Optional[BigQueryTableRef] = None
        try:
            resource = event.read_event.resource.get_sanitized_table_ref()
        except Exception as e:
            self.report.report_warning(
                str(event.read_event.resource), f"Failed to clean up resource, {e}"
            )
            logger.warning(
                f"Failed to process event {str(event.read_event.resource)}", e
            )
            return datasets

        if resource.is_temporary_table([self.config.temp_table_dataset_prefix]):
            logger.debug(f"Dropping temporary table {resource}")
            self.report.report_dropped(str(resource))
            return datasets

        agg_bucket = datasets[floored_ts].setdefault(
            resource,
            AggregatedDataset(
                bucket_start_time=floored_ts,
                resource=resource,
                user_email_pattern=self.config.usage.user_email_pattern,
            ),
        )

        agg_bucket.add_read_entry(
            event.read_event.actor_email,
            event.query_event.query if event.query_event else None,
            event.read_event.fieldsRead,
        )

        return datasets

    def get_workunits(self):
        self.report.num_usage_workunits_emitted = 0
        for time_bucket in self.aggregated_info.values():
            for aggregate in time_bucket.values():
                wu = self._make_usage_stat(aggregate)
                self.report.report_workunit(wu)
                yield wu
                self.report.num_usage_workunits_emitted += 1

    def _make_usage_stat(self, agg: AggregatedDataset) -> MetadataWorkUnit:
        return agg.make_usage_workunit(
            self.config.bucket_duration,
            lambda resource: resource.to_urn(self.config.env),
            self.config.usage.top_n_queries,
            self.config.usage.format_sql_queries,
            self.config.usage.include_top_n_queries,
        )

    def _get_parsed_bigquery_log_events(
        self, project_id: str, limit: Optional[int] = None
    ) -> Iterable[Union[ReadEvent, QueryEvent, MetadataWorkUnit]]:
        if self.config.use_exported_bigquery_audit_metadata:
            _client: BigQueryClient = BigQueryClient(project=project_id)
            return self._get_exported_bigquery_audit_metadata(
                bigquery_client=_client,
                allow_filter=self.config.get_table_pattern(
                    self.config.table_pattern.allow
                ),
                limit=limit,
            )
        else:
            logging_client: GCPLoggingClient = _make_gcp_logging_client(
                project_id, self.config.extra_client_options
            )
            return self._get_bigquery_log_entries_via_gcp_logging(
                logging_client, limit=limit
            )

    def test_capability(self, project_id: str) -> None:
        lineage_metadata: Dict[str, Set[str]]
        for entry in self._get_parsed_bigquery_log_events(project_id, limit=1):
            logger.debug(f"Connection test got one {entry}")
            return
