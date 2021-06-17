import collections
import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pprint import pprint
from typing import Any, Counter, Dict, Iterable, List, Optional, Union

import cachetools
from google.cloud.logging_v2.client import Client

from datahub.utilities.delayed_iter import delayed_iter

# ProtobufEntry is generated dynamically using a namedtuple, so mypy
# can't really deal with it. As such, we short circuit mypy's typing
# but keep the code relatively clear by retaining dummy types.
#
# from google.cloud.logging_v2 import ProtobufEntry
# AuditLogEntry = ProtobufEntry
AuditLogEntry = Any

BQ_DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
DEBUG_INCLUDE_FULL_PAYLOADS = False
GCP_LOGGING_PAGE_SIZE = 1000

# Handle yearly, monthly, daily, or hourly partitioning.
# See https://cloud.google.com/bigquery/docs/partitioned-tables.
PARTITIONED_TABLE_REGEX = re.compile(r"^(.+)_(\d{4}|\d{6}|\d{8}|\d{10})$")

time_start = datetime.now(tz=timezone.utc) - timedelta(days=21)
filter_rule = f"""
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
timestamp >= "{time_start.strftime(BQ_DATETIME_FORMAT)}"
""".strip()


def get_time_bucket(original: datetime) -> datetime:
    # This rounds/floors the timestamp to the closest day.
    return original.replace(hour=0, minute=0, second=0, microsecond=0)


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


def _job_name_ref(project: str, jobId: str) -> str:
    return f"projects/{project}/jobs/{jobId}"


@dataclass
class ReadEvent:
    timestamp: datetime
    actor_email: str

    resource: BigQueryTableRef
    fieldsRead: List[str]
    readReason: str
    jobName: Optional[str]

    payload: Any

    # FIXME: we really should use composition here, but this is just simpler.
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

        fields = readInfo["fields"]
        readReason = readInfo["reason"]
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
        # if job['jobConfiguration']['query']['statementType'] != "SCRIPT" and not referencedTables:
        #     breakpoint()

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


@dataclass
class AggregatedDataset:
    bucket_start_time: datetime
    resource: BigQueryTableRef

    queryFreq: Counter[str] = field(default_factory=collections.Counter)
    userCounts: Counter[str] = field(default_factory=collections.Counter)
    # TODO add column usage counters


class BigQueryUsageSource:
    def __init__(self, project: str, client_options: dict = {}) -> None:
        client_options = client_options.copy()
        if project is not None:
            client_options["project"] = project

        # See https://github.com/googleapis/google-cloud-python/issues/2674 for
        # why we disable gRPC here.
        self.client = Client(**client_options, _use_grpc=False)

        self.query_log_delay = 100

    def get_workunits(self) -> Iterable[None]:
        bigquery_log_entries = self._get_bigquery_log_entries()
        parsed_events = self._parse_bigquery_log_entries(bigquery_log_entries)
        hydrated_read_events = self._join_events_by_job_id(parsed_events)

        aggregated_info = self._aggregate_enriched_read_events(hydrated_read_events)
        pprint(aggregated_info)

        yield from []

    def _get_bigquery_log_entries(self) -> Iterable[AuditLogEntry]:
        entry: AuditLogEntry
        for entry in self.client.list_entries(
            filter_=filter_rule, page_size=GCP_LOGGING_PAGE_SIZE
        ):
            yield entry

    def _parse_bigquery_log_entries(
        self, entries: Iterable[AuditLogEntry]
    ) -> Iterable[Union[ReadEvent, QueryEvent]]:
        for entry in entries:
            event: Union[ReadEvent, QueryEvent]
            if ReadEvent.can_parse_entry(entry):
                event = ReadEvent.from_entry(entry)
            elif QueryEvent.can_parse_entry(entry):
                event = QueryEvent.from_entry(entry)
            else:
                # TODO: add better error handling
                print("Unable to parse:", entry)
                exit(1)
            yield event

    def _join_events_by_job_id(
        self, events: Iterable[Union[ReadEvent, QueryEvent]]
    ) -> Iterable[ReadEvent]:
        # We only store the most recently used query events, which are used when
        # resolving job information within the read events.
        query_jobs = cachetools.LRUCache[str, QueryEvent](
            maxsize=2 * self.query_log_delay
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
        delayed_read_events = delayed_iter(original_read_events, self.query_log_delay)

        for event in delayed_read_events:
            if event.jobName:
                if event.jobName in query_jobs:
                    # Join the query log event into the table read log event.
                    event.query = query_jobs[event.jobName].query

                    # TODO also join into the query itself for column references
                else:
                    # TODO raise a warning and suggest increasing the buffer size option
                    breakpoint()

            yield event

        # pprint(query_jobs)

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
            floored_ts = get_time_bucket(event.timestamp)
            resource = event.resource.remove_extras()

            if resource.is_anonymous():
                # TODO report dropped (ideally just once)
                breakpoint()
                continue

            agg_bucket = datasets[floored_ts].setdefault(
                resource,
                AggregatedDataset(bucket_start_time=floored_ts, resource=resource),
            )

            agg_bucket.userCounts[event.actor_email] += 1
            if event.query:
                agg_bucket.queryFreq[event.query] += 1

        return datasets


if __name__ == "__main__":
    # TODO: remove this bit
    source = BigQueryUsageSource(project="harshal-playground-306419")
    events = list(source.get_workunits())
    # pprint(events)
    pprint(f"Processed {len(events)} entries")
    breakpoint()
    exit(0)
