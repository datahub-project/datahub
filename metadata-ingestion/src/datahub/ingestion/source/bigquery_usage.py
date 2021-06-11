import collections
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
    # This floors the timestamp to the closest day.
    return original.replace(hour=0, minute=0, second=0, microsecond=0)


def _table_name_ref(project: str, dataset: str, table: str) -> str:
    return f"projects/{project}/datasets/{dataset}/tables/{table}"


def _table_resource_from_obj(spec: dict) -> str:
    return _table_name_ref(spec["projectId"], spec["datasetId"], spec["tableId"])


def _job_name_ref(project: str, jobId: str) -> str:
    return f"projects/{project}/jobs/{jobId}"


@dataclass
class ReadEvent:
    timestamp: datetime
    actor_email: str

    resource: str
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
            resource=resourceName,
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
    destinationTable: Optional[str]
    referencedTables: Optional[List[str]]
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
            destinationTable = _table_resource_from_obj(rawDestTable)

        rawRefTables = job["jobStatistics"].get("referencedTables")
        referencedTables = None
        if rawRefTables:
            referencedTables = [_table_resource_from_obj(spec) for spec in rawRefTables]
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
    resource: str

    queryFreq: Counter[str] = field(default_factory=collections.Counter)
    userCounts: Counter[str] = field(default_factory=collections.Counter)


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
        query_jobs = cachetools.LRUCache(maxsize=2 * self.query_log_delay)

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
    ) -> Dict[datetime, Dict[str, AggregatedDataset]]:
        # TODO: dataset will begin with underscore if it's temporary
        # TODO: handle partitioned tables

        # TODO: perhaps we need to continuously prune this, rather than
        # storing it all in one big object.
        datasets: Dict[
            datetime, Dict[str, AggregatedDataset]
        ] = collections.defaultdict(dict)

        for event in events:
            floored_ts = get_time_bucket(event.timestamp)
            agg_bucket = datasets[floored_ts].setdefault(
                event.resource,
                AggregatedDataset(
                    bucket_start_time=floored_ts, resource=event.resource
                ),
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
