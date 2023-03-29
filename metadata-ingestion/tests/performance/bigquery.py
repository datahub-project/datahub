import dataclasses
import random
import uuid
from collections import defaultdict
from typing import Dict, Iterable, List

from datahub.ingestion.source.bigquery_v2.bigquery_audit import (
    AuditEvent,
    BigqueryTableIdentifier,
    BigQueryTableRef,
    QueryEvent,
    ReadEvent,
)
from datahub.ingestion.source.bigquery_v2.bigquery_config import BigQueryV2Config
from tests.performance.data_model import Query, Table

# https://cloud.google.com/bigquery/docs/reference/auditlogs/rest/Shared.Types/BigQueryAuditMetadata.TableDataRead.Reason
READ_REASONS = [
    "REASON_UNSPECIFIED",
    "JOB",
    "TABLEDATA_LIST_REQUEST",
    "GET_QUERY_RESULTS_REQUEST",
    "QUERY_REQUEST",
    "CREATE_READ_SESSION",
    "MATERIALIZED_VIEW_REFRESH",
]


def generate_events(
    queries: Iterable[Query],
    projects: List[str],
    table_to_project: Dict[str, str],
    config: BigQueryV2Config,
) -> Iterable[AuditEvent]:
    for query in queries:
        project = (  # Most queries are run in the project of the tables they access
            table_to_project[
                query.object_modified.name
                if query.object_modified
                else query.fields_accessed[0].table.name
            ]
            if random.random() >= 0.1
            else random.choice(projects)
        )
        job_name = str(uuid.uuid4())
        yield AuditEvent.create(
            QueryEvent(
                job_name=job_name,
                timestamp=query.timestamp,
                actor_email=query.actor,
                query=query.text,
                statementType=query.type,
                project_id=project,
                destinationTable=ref_from_table(query.object_modified, table_to_project)
                if query.object_modified
                else None,
                referencedTables=[
                    ref_from_table(field.table, table_to_project)
                    for field in query.fields_accessed
                    if not field.table.is_view()
                ],
                referencedViews=[
                    ref_from_table(field.table, table_to_project)
                    for field in query.fields_accessed
                    if field.table.is_view()
                ],
                payload=dataclasses.asdict(query)
                if config.debug_include_full_payloads
                else None,
            )
        )
        table_accesses = defaultdict(list)
        for field in query.fields_accessed:
            table_accesses[ref_from_table(field.table, table_to_project)].append(
                field.column
            )

        for ref, columns in table_accesses.items():
            yield AuditEvent.create(
                ReadEvent(
                    jobName=job_name,
                    timestamp=query.timestamp,
                    actor_email=query.actor,
                    resource=ref,
                    fieldsRead=columns,
                    readReason=random.choice(READ_REASONS),
                    payload=dataclasses.asdict(query)
                    if config.debug_include_full_payloads
                    else None,
                )
            )


def ref_from_table(table: Table, table_to_project: Dict[str, str]) -> BigQueryTableRef:
    return BigQueryTableRef(
        BigqueryTableIdentifier(
            table_to_project[table.name], table.container.name, table.name
        )
    )
