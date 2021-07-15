import collections
import dataclasses
import logging
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Counter, Dict, Iterable, List, Optional, Union

import cachetools
import pydantic
from google.cloud.logging_v2.client import Client as GCPLoggingClient

import datahub.emitter.mce_builder as builder
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.source import Source, SourceReport
from datahub.ingestion.api.workunit import UsageStatsWorkUnit
from datahub.ingestion.source.usage_common import (
    BaseUsageConfig,
    GenericAggregatedDataset,
    get_time_bucket,
)
from datahub.utilities.delayed_iter import delayed_iter

logger = logging.getLogger(__name__)

# ProtobufEntry is generated dynamically using a namedtuple, so mypy
# can't really deal with it. As such, we short circuit mypy's typing
# but keep the code relatively clear by retaining dummy types.
#
# from google.cloud.logging_v2 import ProtobufEntry
# AuditLogEntry = ProtobufEntry
AuditLogEntry = Any

DEBUG_INCLUDE_FULL_PAYLOADS = False
GCP_LOGGING_PAGE_SIZE = 1000

# Handle yearly, monthly, daily, or hourly partitioning.
# See https://cloud.google.com/bigquery/docs/partitioned-tables.
PARTITIONED_TABLE_REGEX = re.compile(r"^(.+)_(\d{4}|\d{6}|\d{8}|\d{10})$")

BQ_DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
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
        AND
        NOT protoPayload.serviceData.jobCompletedEvent.job.jobStatus.error.code:*
    )
    OR
    (
        protoPayload.metadata.tableDataRead:*
    )
)
AND
timestamp >= "{start_time}"
AND
timestamp <= "{end_time}"
""".strip()


@dataclass(frozen=True, order=True)
class BigQueryTableRef:
    project: str
    dataset: str
    table: str

    @classmethod
    def from_spec_obj(cls, spec: dict) -> "BigQueryTableRef":
        return BigQueryTableRef(spec["projectId"], spec["datasetId"], spec["tableId"])

    @classmethod
    def from_string_name(cls, ref: str) -> "BigQueryTableRef":
        parts = ref.split("/")
        if parts[0] != "projects" or parts[2] != "datasets" or parts[4] != "tables":
            raise ValueError(f"invalid BigQuery table reference: {ref}")
        return BigQueryTableRef(parts[1], parts[3], parts[5])

    def is_anonymous(self) -> bool:
        # Temporary tables will have a dataset that begins with an underscore.
        return self.dataset.startswith("_")

    def remove_extras(self) -> "BigQueryTableRef":
        if "$" in self.table or "@" in self.table:
            raise ValueError(f"cannot handle {self} - poorly formatted table name")

        # Handle partitioned and sharded tables.
        matches = PARTITIONED_TABLE_REGEX.match(self.table)
        if matches:
            return BigQueryTableRef(self.project, self.dataset, matches.group(1))

        return self

    def __str__(self) -> str:
        return f"projects/{self.project}/datasets/{self.dataset}/tables/{self.table}"


AggregatedDataset = GenericAggregatedDataset[BigQueryTableRef]


def _table_ref_to_urn(ref: BigQueryTableRef, env: str) -> str:
    return builder.make_dataset_urn(
        "bigquery", f"{ref.project}.{ref.dataset}.{ref.table}", env
    )


def _job_name_ref(project: str, jobId: str) -> str:
    return f"projects/{project}/jobs/{jobId}"


@dataclass
class ReadEvent:
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
    def can_parse_entry(cls, entry: AuditLogEntry) -> bool:
        try:
            entry.payload["metadata"]["tableDataRead"]
            return True
        except (KeyError, TypeError):
            return False

    @classmethod
    def from_entry(cls, entry: AuditLogEntry) -> "ReadEvent":
        user = entry.payload["authenticationInfo"]["principalEmail"]
        resourceName = entry.payload["resourceName"]
        readInfo = entry.payload["metadata"]["tableDataRead"]

        fields = readInfo.get("fields", [])
        readReason = readInfo.get("reason")
        jobName = None
        if readReason == "JOB":
            jobName = readInfo["jobName"]

        readEvent = ReadEvent(
            actor_email=user,
            timestamp=entry.timestamp,
            resource=BigQueryTableRef.from_string_name(resourceName),
            fieldsRead=fields,
            readReason=readReason,
            jobName=jobName,
            payload=entry.payload if DEBUG_INCLUDE_FULL_PAYLOADS else None,
        )
        return readEvent


@dataclass
class QueryEvent:
    timestamp: datetime
    actor_email: str

    query: str
    destinationTable: Optional[BigQueryTableRef]
    referencedTables: Optional[List[BigQueryTableRef]]
    jobName: str

    payload: Any

    @classmethod
    def can_parse_entry(cls, entry: AuditLogEntry) -> bool:
        try:
            entry.payload["serviceData"]["jobCompletedEvent"]["job"]
            return True
        except (KeyError, TypeError):
            return False

    @classmethod
    def from_entry(cls, entry: AuditLogEntry) -> "QueryEvent":
        user = entry.payload["authenticationInfo"]["principalEmail"]

        job = entry.payload["serviceData"]["jobCompletedEvent"]["job"]
        jobName = _job_name_ref(job["jobName"]["projectId"], job["jobName"]["jobId"])
        rawQuery = job["jobConfiguration"]["query"]["query"]

        rawDestTable = job["jobConfiguration"]["query"]["destinationTable"]
        destinationTable = None
        if rawDestTable:
            destinationTable = BigQueryTableRef.from_spec_obj(rawDestTable)

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
            destinationTable=destinationTable,
            referencedTables=referencedTables,
            jobName=jobName,
            payload=entry.payload if DEBUG_INCLUDE_FULL_PAYLOADS else None,
        )
        return queryEvent


class BigQueryUsageConfig(BaseUsageConfig):
    project_id: Optional[str] = None
    extra_client_options: dict = {}
    env: str = builder.DEFAULT_ENV

    query_log_delay: pydantic.PositiveInt = 100


@dataclass
class BigQueryUsageSourceReport(SourceReport):
    dropped_table: Counter[str] = dataclasses.field(default_factory=collections.Counter)

    def report_dropped(self, key: str) -> None:
        self.dropped_table[key] += 1


class BigQueryUsageSource(Source):
    config: BigQueryUsageConfig
    report: BigQueryUsageSourceReport

    client: GCPLoggingClient

    def __init__(self, config: BigQueryUsageConfig, ctx: PipelineContext):
        super().__init__(ctx)
        self.config = config
        self.report = BigQueryUsageSourceReport()

        client_options = self.config.extra_client_options.copy()
        if self.config.project_id is not None:
            client_options["project"] = self.config.project_id

        # See https://github.com/googleapis/google-cloud-python/issues/2674 for
        # why we disable gRPC here.
        self.client = GCPLoggingClient(**client_options, _use_grpc=False)

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "BigQueryUsageSource":
        config = BigQueryUsageConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def get_workunits(self) -> Iterable[UsageStatsWorkUnit]:
        bigquery_log_entries = self._get_bigquery_log_entries()
        parsed_events = self._parse_bigquery_log_entries(bigquery_log_entries)
        hydrated_read_events = self._join_events_by_job_id(parsed_events)
        aggregated_info = self._aggregate_enriched_read_events(hydrated_read_events)

        for time_bucket in aggregated_info.values():
            for aggregate in time_bucket.values():
                wu = self._make_usage_stat(aggregate)
                self.report.report_workunit(wu)
                yield wu

    def _get_bigquery_log_entries(self) -> Iterable[AuditLogEntry]:
        filter = BQ_FILTER_RULE_TEMPLATE.format(
            start_time=self.config.start_time.strftime(BQ_DATETIME_FORMAT),
            end_time=self.config.end_time.strftime(BQ_DATETIME_FORMAT),
        )

        entry: AuditLogEntry
        for i, entry in enumerate(
            self.client.list_entries(filter_=filter, page_size=GCP_LOGGING_PAGE_SIZE)
        ):
            if i == 0:
                logger.debug("starting log load from BigQuery")
            yield entry
        logger.debug("finished loading log entries from BigQuery")

    def _parse_bigquery_log_entries(
        self, entries: Iterable[AuditLogEntry]
    ) -> Iterable[Union[ReadEvent, QueryEvent]]:
        for entry in entries:
            event: Union[None, ReadEvent, QueryEvent] = None
            if ReadEvent.can_parse_entry(entry):
                event = ReadEvent.from_entry(entry)
            elif QueryEvent.can_parse_entry(entry):
                event = QueryEvent.from_entry(entry)
            else:
                self.report.report_failure(
                    f"{entry.log_name}-{entry.insert_id}",
                    f"unable to parse log entry: {entry!r}",
                )
            if event:
                yield event

    def _join_events_by_job_id(
        self, events: Iterable[Union[ReadEvent, QueryEvent]]
    ) -> Iterable[ReadEvent]:
        # We only store the most recently used query events, which are used when
        # resolving job information within the read events.
        query_jobs: cachetools.LRUCache[str, QueryEvent] = cachetools.LRUCache(
            maxsize=2 * self.config.query_log_delay
        )

        def event_processor(
            events: Iterable[Union[ReadEvent, QueryEvent]]
        ) -> Iterable[ReadEvent]:
            for event in events:
                if isinstance(event, QueryEvent):
                    query_jobs[event.jobName] = event
                else:
                    yield event

        # TRICKY: To account for the possibility that the query event arrives after
        # the read event in the audit logs, we wait for at least `query_log_delay`
        # additional events to be processed before attempting to resolve BigQuery
        # job information from the logs.
        original_read_events = event_processor(events)
        delayed_read_events = delayed_iter(
            original_read_events, self.config.query_log_delay
        )

        for event in delayed_read_events:
            if event.jobName:
                if event.jobName in query_jobs:
                    # Join the query log event into the table read log event.
                    event.query = query_jobs[event.jobName].query

                    # TODO also join into the query itself for column references
                else:
                    self.report.report_warning(
                        "<general>",
                        "failed to match table read event with job; try increasing `query_log_delay`",
                    )

            yield event

    def _aggregate_enriched_read_events(
        self, events: Iterable[ReadEvent]
    ) -> Dict[datetime, Dict[BigQueryTableRef, AggregatedDataset]]:
        # TODO: handle partitioned tables

        # TODO: perhaps we need to continuously prune this, rather than
        # storing it all in one big object.
        datasets: Dict[
            datetime, Dict[BigQueryTableRef, AggregatedDataset]
        ] = collections.defaultdict(dict)

        for event in events:
            floored_ts = get_time_bucket(event.timestamp, self.config.bucket_duration)
            resource = event.resource.remove_extras()

            if resource.is_anonymous():
                self.report.report_dropped(str(resource))
                continue

            agg_bucket = datasets[floored_ts].setdefault(
                resource,
                AggregatedDataset(bucket_start_time=floored_ts, resource=resource),
            )
            agg_bucket.add_read_entry(event.actor_email, event.query, event.fieldsRead)

        return datasets

    def _make_usage_stat(self, agg: AggregatedDataset) -> UsageStatsWorkUnit:
        return agg.make_usage_workunit(
            self.config.bucket_duration,
            lambda resource: _table_ref_to_urn(resource, self.config.env),
            self.config.top_n_queries,
        )

    def get_report(self) -> SourceReport:
        return self.report
