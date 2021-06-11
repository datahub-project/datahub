import dataclasses
from datetime import datetime, timedelta, timezone
from pprint import pprint
from typing import Any, Iterable, List, Optional, Union

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
INCLUDE_FULL_PAYLOADS = False
GCP_LOGGING_PAGE_SIZE = 1000

time_start = datetime.now(tz=timezone.utc) - timedelta(days=14)
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


@dataclasses.dataclass
class ReadEvent:
    timestamp: datetime
    actor_email: str

    resource: str
    fieldsRead: List[str]
    readReason: str
    jobName: Optional[str]

    payload: Any

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
            payload=entry.payload if INCLUDE_FULL_PAYLOADS else None,
        )
        return readEvent


@dataclasses.dataclass
class QueryEvent:
    timestamp: datetime
    actor_email: str

    query: str
    destinationTable: str
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
        rawQuery = job["jobConfiguration"]["query"]["query"]
        destinationTable = job["jobConfiguration"]["query"]["destinationTable"]
        # TODO: dataset will begin with underscore if it's temporary

        jobName = (
            f"projects/{job['jobName']['projectId']}/jobs/{job['jobName']['jobId']}"
        )

        referencedTables = job["jobStatistics"].get("referencedTables")
        # if job['jobConfiguration']['query']['statementType'] != "SCRIPT" and not referencedTables:
        #     breakpoint()

        queryEvent = QueryEvent(
            timestamp=entry.timestamp,
            actor_email=user,
            query=rawQuery,
            destinationTable=destinationTable,
            referencedTables=referencedTables,
            jobName=jobName,
            payload=entry.payload if INCLUDE_FULL_PAYLOADS else None,
        )
        return queryEvent


class BigQueryUsageSource:
    def __init__(self, project: str, client_options: dict = {}) -> None:
        client_options = client_options.copy()
        if project is not None:
            client_options["project"] = project

        # See https://github.com/googleapis/google-cloud-python/issues/2674 for
        # why we disable gRPC here.
        self.client = Client(**client_options, _use_grpc=False)

        self.query_buffer_size = 100

    def get_workunits(self) -> Iterable[ReadEvent]:
        bigquery_log_entries = self._get_bigquery_log_entries()
        parsed_events = self._parse_bigquery_log_entries(bigquery_log_entries)
        hydrated_read_events = self._join_events_by_job_id(parsed_events)

        # TODO: now we need to do the aggregations
        yield from hydrated_read_events

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
        # TODO: replace with an LRU cache
        # use cachetools
        query_jobs = {}

        def event_processor(
            events: Iterable[Union[ReadEvent, QueryEvent]]
        ) -> Iterable[ReadEvent]:
            for event in events:
                if isinstance(event, QueryEvent):
                    query_jobs[event.jobName] = event
                else:
                    yield event

        original_read_events = event_processor(events)
        delayed_read_events = delayed_iter(original_read_events, self.query_buffer_size)

        for event in delayed_read_events:
            if event.jobName:
                # TODO update this comment
                # Anecdotally, the query completion event seems to come before the table read events.
                # As such, we can simply keep a cache of the recent query events and match them to the
                # table read events as we receive the read events.
                if event.jobName in query_jobs:
                    # Join the query log event into the table read log event.
                    event.query = query_jobs[event.jobName].query

                    # TODO also join into the query itself
                else:
                    # TODO raise a warning and suggest increasing the buffer size option
                    breakpoint()

            yield event

        print(query_jobs)


if __name__ == "__main__":
    # TODO: remove this bit
    source = BigQueryUsageSource(project="harshal-playground-306419")
    events = list(source.get_workunits())
    # pprint(events)
    pprint(f"Processed {len(events)} entries")
    breakpoint()
    exit(0)
