import collections
import heapq
import json
import logging
import os
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Iterable, List, MutableMapping, Optional, Union, cast

import cachetools
from google.cloud.logging_v2.client import Client as GCPLoggingClient
from more_itertools import partition

import datahub.emitter.mce_builder as builder
from datahub.configuration.time_window_config import get_time_bucket
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.source import Source
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.usage.usage_common import GenericAggregatedDataset
from datahub.ingestion.source_config.usage.bigquery_usage import BigQueryUsageConfig
from datahub.ingestion.source_report.usage.bigquery_usage import (
    BigQueryUsageSourceReport,
)
from datahub.metadata.schema_classes import (
    ChangeTypeClass,
    OperationClass,
    OperationTypeClass,
)
from datahub.utilities.delayed_iter import delayed_iter
from datahub.utilities.parsing_util import (
    get_first_missing_key,
    get_first_missing_key_any,
)

logger = logging.getLogger(__name__)

# ProtobufEntry is generated dynamically using a namedtuple, so mypy
# can't really deal with it. As such, we short circuit mypy's typing
# but keep the code relatively clear by retaining dummy types.
#
# from google.cloud.logging_v2 import ProtobufEntry
# AuditLogEntry = ProtobufEntry
AuditLogEntry = Any

# BigQueryAuditMetadata is the v2 format in which audit logs are exported to BigQuery
BigQueryAuditMetadata = Any

DEBUG_INCLUDE_FULL_PAYLOADS = False

# Handle yearly, monthly, daily, or hourly partitioning.
# See https://cloud.google.com/bigquery/docs/partitioned-tables.
# This REGEX handles both Partitioned Tables ($ separator) and Sharded Tables (_ separator)
PARTITIONED_TABLE_REGEX = re.compile(
    r"^(.+)[\$_](\d{4}|\d{6}|\d{8}|\d{10}|__PARTITIONS_SUMMARY__)$"
)

# Handle table snapshots
# See https://cloud.google.com/bigquery/docs/table-snapshots-intro.
SNAPSHOT_TABLE_REGEX = re.compile(r"^(.+)@(\d{13})$")

BQ_DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
BQ_DATE_SHARD_FORMAT = "%Y%m%d"
BQ_AUDIT_V1 = {
    "BQ_FILTER_REGEX_ALLOW_TEMPLATE": """
protoPayload.serviceData.jobCompletedEvent.job.jobStatistics.referencedTables.tableId =~ "{allow_pattern}"
""",
    "BQ_FILTER_REGEX_DENY_TEMPLATE": """
{logical_operator}
protoPayload.serviceData.jobCompletedEvent.job.jobStatistics.referencedTables.tableId !~ "{deny_pattern}"
""",
    "BQ_FILTER_RULE_TEMPLATE": """
protoPayload.serviceName="bigquery.googleapis.com"
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
    {allow_regex}
    {deny_regex}
    OR
    protoPayload.metadata.tableDataRead.reason = "JOB"
)
AND
timestamp >= "{start_time}"
AND
timestamp < "{end_time}"
""".strip(),
}

BQ_AUDIT_V2 = {
    "BQ_FILTER_REGEX_ALLOW_TEMPLATE": """
protoPayload.metadata.jobChange.job.jobStats.queryStats.referencedTables =~ "projects/.*/datasets/.*/tables/{allow_pattern}"
""",
    "BQ_FILTER_REGEX_DENY_TEMPLATE": """
{logical_operator}
protoPayload.metadata.jobChange.job.jobStats.queryStats.referencedTables !~ "projects/.*/datasets/.*/tables/{deny_pattern}"
""",
    "BQ_FILTER_RULE_TEMPLATE": """
resource.type=("bigquery_project" OR "bigquery_dataset")
AND
(
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
        AND protoPayload.metadata.jobChange.job.jobStats.queryStats.referencedTables:*
         AND (
            {allow_regex}
            {deny_regex}
                OR
            protoPayload.metadata.tableDataRead.reason = "JOB"
        )
    )
    OR
    (
        protoPayload.metadata.tableDataRead:*
    )
)
AND
timestamp >= "{start_time}"
AND
timestamp < "{end_time}"
""".strip(),
}


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


@dataclass(frozen=True, order=True)
class BigQueryTableRef:
    project: str
    dataset: str
    table: str

    @classmethod
    def from_spec_obj(cls, spec: dict) -> "BigQueryTableRef":
        return cls(spec["projectId"], spec["datasetId"], spec["tableId"])

    @classmethod
    def from_string_name(cls, ref: str) -> "BigQueryTableRef":
        parts = ref.split("/")
        if parts[0] != "projects" or parts[2] != "datasets" or parts[4] != "tables":
            raise ValueError(f"invalid BigQuery table reference: {ref}")
        return cls(parts[1], parts[3], parts[5])

    def is_temporary_table(self) -> bool:
        # Temporary tables will have a dataset that begins with an underscore.
        return self.dataset.startswith("_")

    def remove_extras(self) -> "BigQueryTableRef":
        # Handle partitioned and sharded tables.
        matches = PARTITIONED_TABLE_REGEX.match(self.table)
        if matches:
            table_name = matches.group(1)
            logger.debug(
                f"Found partitioned table {self.table}. Using {table_name} as the table name."
            )
            return BigQueryTableRef(self.project, self.dataset, table_name)

        # Handle table snapshots.
        matches = SNAPSHOT_TABLE_REGEX.match(self.table)
        if matches:
            table_name = matches.group(1)
            logger.debug(
                f"Found table snapshot {self.table}. Using {table_name} as the table name."
            )
            return BigQueryTableRef(self.project, self.dataset, table_name)

        # Handle exceptions
        invalid_chars_in_table_name: List[str] = [
            c for c in {"$", "@"} if c in self.table
        ]
        if invalid_chars_in_table_name:
            raise ValueError(
                f"Cannot handle {self} - poorly formatted table name, contains {invalid_chars_in_table_name}"
            )

        return self

    def __str__(self) -> str:
        return f"projects/{self.project}/datasets/{self.dataset}/tables/{self.table}"


AggregatedDataset = GenericAggregatedDataset[BigQueryTableRef]


def _table_ref_to_urn(ref: BigQueryTableRef, env: str) -> str:
    return builder.make_dataset_urn(
        "bigquery", f"{ref.project}.{ref.dataset}.{ref.table}", env
    )


def _job_name_ref(project: str, jobId: str) -> Optional[str]:
    if project and jobId:
        return f"projects/{project}/jobs/{jobId}"
    else:
        return None


@dataclass
class ReadEvent:
    """
    A container class for data from a TableDataRead event.
    See https://cloud.google.com/bigquery/docs/reference/auditlogs/rest/Shared.Types/BigQueryAuditMetadata#BigQueryAuditMetadata.TableDataRead.
    """

    timestamp: datetime
    actor_email: str

    resource: BigQueryTableRef
    fieldsRead: List[str]
    readReason: Optional[str]
    jobName: Optional[str]

    payload: Any

    # We really should use composition here since the query isn't actually
    # part of the read event, but this solution is just simpler.
    query: Optional[str] = None  # populated via join

    @classmethod
    def get_missing_key_entry(cls, entry: AuditLogEntry) -> Optional[str]:
        return (
            get_first_missing_key(
                inp_dict=entry.payload, keys=["metadata", "tableDataRead"]
            )
            or get_first_missing_key(
                inp_dict=entry.payload, keys=["authenticationInfo", "principalEmail"]
            )
            or get_first_missing_key(inp_dict=entry.payload, keys=["resourceName"])
        )

    @classmethod
    def from_entry(cls, entry: AuditLogEntry) -> "ReadEvent":
        user = entry.payload["authenticationInfo"]["principalEmail"]
        resourceName = entry.payload["resourceName"]
        readInfo = entry.payload["metadata"]["tableDataRead"]

        fields = readInfo.get("fields", [])

        # https://cloud.google.com/bigquery/docs/reference/auditlogs/rest/Shared.Types/BigQueryAuditMetadata.TableDataRead.Reason
        readReason = readInfo.get("reason")
        jobName = None
        if readReason == "JOB":
            jobName = readInfo.get("jobName")

        readEvent = ReadEvent(
            actor_email=user,
            timestamp=entry.timestamp,
            resource=BigQueryTableRef.from_string_name(resourceName),
            fieldsRead=fields,
            readReason=readReason,
            jobName=jobName,
            payload=entry.payload if DEBUG_INCLUDE_FULL_PAYLOADS else None,
        )
        if readReason == "JOB" and not jobName:
            logger.debug(
                "jobName from read events is absent when readReason is JOB. "
                "Auditlog entry - {logEntry}".format(logEntry=entry)
            )
        return readEvent


@dataclass
class QueryEvent:
    """
    A container class for a query job completion event.
    See https://cloud.google.com/bigquery/docs/reference/auditlogs/rest/Shared.Types/AuditData#JobCompletedEvent.
    """

    timestamp: datetime
    actor_email: str

    query: str
    statementType: Optional[str]
    destinationTable: Optional[BigQueryTableRef]
    referencedTables: Optional[List[BigQueryTableRef]]
    jobName: Optional[str]

    payload: Any

    @staticmethod
    def get_missing_key_entry(entry: AuditLogEntry) -> Optional[str]:
        return get_first_missing_key(
            inp_dict=entry.payload, keys=["serviceData", "jobCompletedEvent", "job"]
        )

    @staticmethod
    def get_missing_key_entry_v2(entry: AuditLogEntry) -> Optional[str]:
        return get_first_missing_key(
            inp_dict=entry.payload, keys=["metadata", "jobChange", "job"]
        )

    @classmethod
    def from_entry(cls, entry: AuditLogEntry) -> "QueryEvent":
        user = entry.payload["authenticationInfo"]["principalEmail"]

        job = entry.payload["serviceData"]["jobCompletedEvent"]["job"]
        jobName = _job_name_ref(
            job.get("jobName", {}).get("projectId"), job.get("jobName", {}).get("jobId")
        )
        rawQuery = job["jobConfiguration"]["query"]["query"]

        rawDestTable = job["jobConfiguration"]["query"]["destinationTable"]
        destinationTable = None
        if rawDestTable:
            destinationTable = BigQueryTableRef.from_spec_obj(rawDestTable)

        try:
            statementType = job["jobConfiguration"]["query"]["statementType"]
        except KeyError:
            statementType = None

        rawRefTables = job["jobStatistics"].get("referencedTables")
        referencedTables = None
        if rawRefTables:
            referencedTables = [
                BigQueryTableRef.from_spec_obj(spec) for spec in rawRefTables
            ]

        queryEvent = QueryEvent(
            timestamp=entry.timestamp,
            actor_email=user,
            query=rawQuery,
            statementType=statementType,
            destinationTable=destinationTable,
            referencedTables=referencedTables,
            jobName=jobName,
            payload=entry.payload if DEBUG_INCLUDE_FULL_PAYLOADS else None,
        )
        if not jobName:
            logger.debug(
                "jobName from query events is absent. "
                "Auditlog entry - {logEntry}".format(logEntry=entry)
            )

        return queryEvent

    @staticmethod
    def get_missing_key_exported_bigquery_audit_metadata(
        row: BigQueryAuditMetadata,
    ) -> Optional[str]:
        return get_first_missing_key_any(row, ["timestamp", "protoPayload", "metadata"])

    @classmethod
    def from_exported_bigquery_audit_metadata(
        cls, row: BigQueryAuditMetadata
    ) -> "QueryEvent":
        timestamp = row["timestamp"]
        payload = row["protoPayload"]
        metadata = json.loads(row["metadata"])

        user = payload["authenticationInfo"]["principalEmail"]

        job = metadata["jobChange"]["job"]

        job_name = job.get("jobName")
        raw_query = job["jobConfig"]["queryConfig"]["query"]

        raw_dest_table = job["jobConfig"]["queryConfig"].get("destinationTable")
        destination_table = None
        if raw_dest_table:
            destination_table = BigQueryTableRef.from_string_name(raw_dest_table)

        raw_ref_tables = job["jobStats"]["queryStats"].get("referencedTables")
        referenced_tables = None
        if raw_ref_tables:
            referenced_tables = [
                BigQueryTableRef.from_string_name(spec) for spec in raw_ref_tables
            ]

        try:
            statementType = job["jobConfiguration"]["query"]["statementType"]
        except KeyError:
            statementType = None

        query_event = QueryEvent(
            timestamp=timestamp,
            actor_email=user,
            query=raw_query,
            statementType=statementType,
            destinationTable=destination_table,
            referencedTables=referenced_tables,
            jobName=job_name,
            payload=payload if DEBUG_INCLUDE_FULL_PAYLOADS else None,
        )
        if not job_name:
            logger.debug(
                "jobName from query events is absent. "
                "BigQueryAuditMetadata entry - {logEntry}".format(logEntry=row)
            )

        return query_event

    @classmethod
    def from_entry_v2(cls, row: BigQueryAuditMetadata) -> "QueryEvent":
        timestamp = row.timestamp
        payload = row.payload
        metadata = payload["metadata"]

        user = payload["authenticationInfo"]["principalEmail"]

        job = metadata["jobChange"]["job"]

        job_name = job.get("jobName")
        raw_query = job["jobConfig"]["queryConfig"]["query"]

        raw_dest_table = job["jobConfig"]["queryConfig"].get("destinationTable")
        destination_table = None
        if raw_dest_table:
            destination_table = BigQueryTableRef.from_string_name(raw_dest_table)

        raw_ref_tables = job["jobStats"]["queryStats"].get("referencedTables")
        referenced_tables = None
        if raw_ref_tables:
            referenced_tables = [
                BigQueryTableRef.from_string_name(spec) for spec in raw_ref_tables
            ]

        try:
            statementType = job["jobConfig"]["queryConfig"]["statementType"]
        except KeyError:
            statementType = None

        query_event = QueryEvent(
            timestamp=timestamp,
            actor_email=user,
            query=raw_query,
            statementType=statementType,
            destinationTable=destination_table,
            referencedTables=referenced_tables,
            jobName=job_name,
            payload=payload if DEBUG_INCLUDE_FULL_PAYLOADS else None,
        )
        if not job_name:
            logger.debug(
                "jobName from query events is absent. "
                "BigQueryAuditMetadata entry - {logEntry}".format(logEntry=row)
            )

        return query_event


class BigQueryUsageSource(Source):
    def __init__(self, config: BigQueryUsageConfig, ctx: PipelineContext):
        super().__init__(ctx)
        self.config: BigQueryUsageConfig = config
        self.report: BigQueryUsageSourceReport = BigQueryUsageSourceReport()

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "BigQueryUsageSource":
        config = BigQueryUsageConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def add_config_to_report(self):
        self.report.start_time = self.config.start_time
        self.report.end_time = self.config.end_time
        self.report.use_v2_audit_metadata = self.config.use_v2_audit_metadata
        self.report.query_log_delay = self.config.query_log_delay
        self.report.log_page_size = self.config.log_page_size
        self.report.allow_pattern = self.config.get_allow_pattern_string()
        self.report.deny_pattern = self.config.get_deny_pattern_string()

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        clients = self._make_bigquery_clients()
        bigquery_log_entries = self._get_bigquery_log_entries(clients)
        parsed_bigquery_log_events = self._parse_bigquery_log_entries(
            bigquery_log_entries
        )
        parsed_events_uncasted: Iterable[Union[ReadEvent, QueryEvent, MetadataWorkUnit]]
        last_updated_work_units_uncasted: Iterable[
            Union[ReadEvent, QueryEvent, MetadataWorkUnit]
        ]
        parsed_events_uncasted, last_updated_work_units_uncasted = partition(
            lambda x: isinstance(x, MetadataWorkUnit), parsed_bigquery_log_events
        )
        parsed_events: Iterable[Union[ReadEvent, QueryEvent]] = cast(
            Iterable[Union[ReadEvent, QueryEvent]], parsed_events_uncasted
        )
        last_updated_work_units: Iterable[MetadataWorkUnit] = cast(
            Iterable[MetadataWorkUnit], last_updated_work_units_uncasted
        )
        if self.config.include_operational_stats:
            for wu in last_updated_work_units:
                self.report.report_workunit(wu)
                yield wu
        hydrated_read_events = self._join_events_by_job_id(parsed_events)
        aggregated_info = self._aggregate_enriched_read_events(hydrated_read_events)

        for time_bucket in aggregated_info.values():
            for aggregate in time_bucket.values():
                wu = self._make_usage_stat(aggregate)
                self.report.report_workunit(wu)
                yield wu

    def _make_bigquery_clients(self) -> List[GCPLoggingClient]:
        # See https://github.com/googleapis/google-cloud-python/issues/2674 for
        # why we disable gRPC here.
        client_options = self.config.extra_client_options.copy()
        client_options["_use_grpc"] = False
        if self.config.projects is None:
            return [
                GCPLoggingClient(**client_options),
            ]
        else:
            return [
                GCPLoggingClient(**client_options, project=project_id)
                for project_id in self.config.projects
            ]

    def _get_bigquery_log_entries(
        self, clients: List[GCPLoggingClient]
    ) -> Iterable[Union[AuditLogEntry, BigQueryAuditMetadata]]:
        self.report.total_log_entries = 0
        audit_templates: Dict[str, str] = BQ_AUDIT_V1
        if self.config.use_v2_audit_metadata:
            audit_templates = BQ_AUDIT_V2

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
                allow_pattern=self.config.get_allow_pattern_string()
            )
            if use_allow_filter
            else ""
        )
        deny_regex = (
            audit_templates["BQ_FILTER_REGEX_DENY_TEMPLATE"].format(
                deny_pattern=self.config.get_deny_pattern_string(),
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
        logger.debug(filter)

        def get_entry_timestamp(
            entry: Union[AuditLogEntry, BigQueryAuditMetadata]
        ) -> datetime:
            return entry.timestamp

        list_entry_generators_across_clients: List[
            Iterable[Union[AuditLogEntry, BigQueryAuditMetadata]]
        ] = list()
        for client in clients:
            try:
                list_entries: Iterable[
                    Union[AuditLogEntry, BigQueryAuditMetadata]
                ] = client.list_entries(
                    filter_=filter, page_size=self.config.log_page_size
                )
                list_entry_generators_across_clients.append(list_entries)
            except Exception as e:
                logger.warning(
                    f"Encountered exception retrieving AuditLogEntires for project {client.project}",
                    e,
                )
                self.report.report_failure(
                    f"{client.project}", f"unable to retrive log entrires {e}"
                )

        i: int = 0
        entry: Union[AuditLogEntry, BigQueryAuditMetadata]
        for i, entry in enumerate(
            heapq.merge(
                *list_entry_generators_across_clients,
                key=get_entry_timestamp,
            )
        ):
            if i == 0:
                logger.info("Starting log load from BigQuery")
            self.report.total_log_entries += 1
            yield entry
        logger.info(f"Finished loading {i} log entries from BigQuery")

    def _create_operation_aspect_work_unit(
        self, event: QueryEvent
    ) -> Optional[MetadataWorkUnit]:
        if event.statementType in OPERATION_STATEMENT_TYPES and event.destinationTable:
            destination_table: BigQueryTableRef
            try:
                destination_table = event.destinationTable.remove_extras()
            except Exception as e:
                self.report.report_warning(
                    str(event.destinationTable),
                    f"Failed to clean up destination table, {e}",
                )
                return None
            last_updated_timestamp: int = int(event.timestamp.timestamp() * 1000)
            affected_datasets = []
            if event.referencedTables:
                for table in event.referencedTables:
                    try:
                        affected_datasets.append(
                            _table_ref_to_urn(
                                table.remove_extras(),
                                self.config.env,
                            )
                        )
                    except Exception as e:
                        self.report.report_warning(
                            str(table),
                            f"Failed to clean up table, {e}",
                        )
            operation_aspect = OperationClass(
                timestampMillis=last_updated_timestamp,
                lastUpdatedTimestamp=last_updated_timestamp,
                actor=builder.make_user_urn(event.actor_email.split("@")[0]),
                operationType=OPERATION_STATEMENT_TYPES[event.statementType],
                affectedDatasets=affected_datasets,
            )
            mcp = MetadataChangeProposalWrapper(
                entityType="dataset",
                aspectName="operation",
                changeType=ChangeTypeClass.UPSERT,
                entityUrn=_table_ref_to_urn(
                    destination_table,
                    env=self.config.env,
                ),
                aspect=operation_aspect,
            )
            return MetadataWorkUnit(
                id=f"{event.timestamp.isoformat()}-operation-aspect-{destination_table}",
                mcp=mcp,
            )
        return None

    def _parse_bigquery_log_entries(
        self, entries: Iterable[Union[AuditLogEntry, BigQueryAuditMetadata]]
    ) -> Iterable[Union[ReadEvent, QueryEvent, MetadataWorkUnit]]:
        self.report.num_read_events = 0
        self.report.num_query_events = 0
        for entry in entries:
            event: Optional[Union[ReadEvent, QueryEvent]] = None

            missing_read_entry = ReadEvent.get_missing_key_entry(entry)
            if missing_read_entry is None:
                event = ReadEvent.from_entry(entry)
                self.report.num_read_events += 1

            missing_query_entry = QueryEvent.get_missing_key_entry(entry)
            if event is None and missing_query_entry is None:
                event = QueryEvent.from_entry(entry)
                self.report.num_query_events += 1
                wu = self._create_operation_aspect_work_unit(event)
                if wu:
                    yield wu

            missing_query_entry_v2 = QueryEvent.get_missing_key_entry_v2(entry)

            if event is None and missing_query_entry_v2 is None:
                event = QueryEvent.from_entry_v2(entry)
                self.report.num_query_events += 1
                wu = self._create_operation_aspect_work_unit(event)
                if wu:
                    yield wu

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

    def error(self, log: logging.Logger, key: str, reason: str) -> Any:
        self.report.report_failure(key, reason)
        log.error(f"{key} => {reason}")

    def _join_events_by_job_id(
        self, events: Iterable[Union[ReadEvent, QueryEvent]]
    ) -> Iterable[ReadEvent]:
        # If caching eviction is enabled, we only store the most recently used query events,
        # which are used when resolving job information within the read events.
        query_jobs: MutableMapping[str, QueryEvent]
        if self.config.query_log_delay:
            query_jobs = cachetools.LRUCache(maxsize=5 * self.config.query_log_delay)
        else:
            query_jobs = {}

        def event_processor(
            events: Iterable[Union[ReadEvent, QueryEvent]]
        ) -> Iterable[ReadEvent]:
            for event in events:
                if isinstance(event, QueryEvent):
                    if event.jobName:
                        query_jobs[event.jobName] = event
                else:
                    yield event

        # TRICKY: To account for the possibility that the query event arrives after
        # the read event in the audit logs, we wait for at least `query_log_delay`
        # additional events to be processed before attempting to resolve BigQuery
        # job information from the logs. If `query_log_delay` is None, it gets treated
        # as an unlimited delay, which prioritizes correctness at the expense of memory usage.
        original_read_events = event_processor(events)
        delayed_read_events = delayed_iter(
            original_read_events, self.config.query_log_delay
        )

        num_joined: int = 0
        for event in delayed_read_events:
            if (
                event.timestamp < self.config.start_time
                or event.timestamp >= self.config.end_time
            ):
                continue

            if event.jobName:
                if event.jobName in query_jobs:
                    # Join the query log event into the table read log event.
                    num_joined += 1
                    event.query = query_jobs[event.jobName].query

                    # TODO also join into the query itself for column references
                else:
                    self.report.report_warning(
                        str(event.resource),
                        "failed to match table read event with job; try increasing `query_log_delay` or `max_query_duration`",
                    )
            yield event

        logger.info(f"Number of read events joined with query events: {num_joined}")

    def _aggregate_enriched_read_events(
        self, events: Iterable[ReadEvent]
    ) -> Dict[datetime, Dict[BigQueryTableRef, AggregatedDataset]]:
        # TODO: handle partitioned tables

        # TODO: perhaps we need to continuously prune this, rather than
        # storing it all in one big object.
        datasets: Dict[
            datetime, Dict[BigQueryTableRef, AggregatedDataset]
        ] = collections.defaultdict(dict)

        num_aggregated: int = 0
        for event in events:
            floored_ts = get_time_bucket(event.timestamp, self.config.bucket_duration)
            resource: Optional[BigQueryTableRef] = None
            try:
                resource = event.resource.remove_extras()
            except Exception as e:
                self.report.report_warning(
                    str(event.resource), f"Failed to clean up resource, {e}"
                )
                logger.warning(f"Failed to process event {str(event.resource)}", e)
                continue

            if resource.is_temporary_table():
                logger.debug(f"Dropping temporary table {resource}")
                self.report.report_dropped(str(resource))
                continue

            agg_bucket = datasets[floored_ts].setdefault(
                resource,
                AggregatedDataset(
                    bucket_start_time=floored_ts,
                    resource=resource,
                    user_email_pattern=self.config.user_email_pattern,
                ),
            )
            agg_bucket.add_read_entry(event.actor_email, event.query, event.fieldsRead)
            num_aggregated += 1
        logger.info(f"Total number of events aggregated = {num_aggregated}.")
        bucket_level_stats: str = "\n\t" + "\n\t".join(
            [
                f'bucket:{db.strftime("%m-%d-%Y:%H:%M:%S")}, size={len(ads)}'
                for db, ads in datasets.items()
            ]
        )
        logger.debug(
            f"Number of buckets created = {len(datasets)}. Per-bucket details:{bucket_level_stats}"
        )

        return datasets

    def _make_usage_stat(self, agg: AggregatedDataset) -> MetadataWorkUnit:
        return agg.make_usage_workunit(
            self.config.bucket_duration,
            lambda resource: _table_ref_to_urn(resource, self.config.env),
            self.config.top_n_queries,
            self.config.format_sql_queries,
        )

    def get_report(self) -> BigQueryUsageSourceReport:
        return self.report

    # We can't use close as it is not called if the ingestion is not successful
    def __del__(self):
        if self.config._credentials_path is not None:
            logger.debug(
                f"Deleting temporary credential file at {self.config._credentials_path}"
            )
            os.unlink(self.config._credentials_path)
