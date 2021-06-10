import dataclasses
from datetime import datetime, timedelta, timezone
from pprint import pprint
from typing import Any, Dict, List, Optional, Union

from google.cloud.logging_v2.client import Client

# from google.cloud.logging_v2 import ProtobufEntry
# ProtobufEntry is generated dynamically using a namedtuple, so mypy
# can't really deal with it. As such, we short circuit mypy's typing
# but keep the code relatively clear by retaining dummy types.
AuditLogEntry = Any

BQ_DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
INCLUDE_FULL_PAYLOADS = False

# See https://github.com/googleapis/google-cloud-python/issues/2674
client = Client(project="harshal-playground-306419", _use_grpc=False)
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


entry: AuditLogEntry
events: List[Union[ReadEvent, QueryEvent]] = []
queries: Dict[str, QueryEvent] = {}
for i, entry in enumerate(
    client.list_entries(filter_=filter_rule, page_size=GCP_LOGGING_PAGE_SIZE)
):
    event: Union[ReadEvent, QueryEvent]
    if ReadEvent.can_parse_entry(entry):
        event = ReadEvent.from_entry(entry)
        if event.jobName:
            if event.jobName not in queries:
                # Anecdotally, the query completion event seems to come before the table read events.
                # As such, we can simply keep a cache of the recent query events and match them to the
                # table read events as we receive the read events.
                breakpoint()
            else:
                # Join the query log event into the table read log event.
                event.query = queries[event.jobName].query
    elif QueryEvent.can_parse_entry(entry):
        event = QueryEvent.from_entry(entry)
        queries[event.jobName] = event
    else:
        print("Unable to parse:", entry)
        exit(1)
    pprint(event)
    events.append(event)

pprint(events)
print(f"Processed {i+1} entries")
breakpoint()
exit(0)
