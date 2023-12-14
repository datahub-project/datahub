import dataclasses
import random
import uuid
from collections import defaultdict
from typing import Dict, Iterable, List, Set

from typing_extensions import get_args

from datahub.ingestion.source.bigquery_v2.bigquery_audit import (
    AuditEvent,
    BigqueryTableIdentifier,
    BigQueryTableRef,
    QueryEvent,
    ReadEvent,
)
from datahub.ingestion.source.bigquery_v2.bigquery_config import BigQueryV2Config
from datahub.ingestion.source.bigquery_v2.usage import OPERATION_STATEMENT_TYPES
from tests.performance.data_model import Query, StatementType, Table

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


# Converts StatementType to possible BigQuery Operation Types
OPERATION_TYPE_MAP: Dict[str, List[str]] = defaultdict(list)
for bq_type, operation_type in OPERATION_STATEMENT_TYPES.items():
    OPERATION_TYPE_MAP[operation_type].append(bq_type)
for typ in get_args(StatementType):
    OPERATION_TYPE_MAP.setdefault(typ, [typ])


def generate_events(
    queries: Iterable[Query],
    projects: List[str],
    table_to_project: Dict[str, str],
    config: BigQueryV2Config,
    proabability_of_project_mismatch: float = 0.1,
) -> Iterable[AuditEvent]:
    for query in queries:
        project = (  # Most queries are run in the project of the tables they access
            table_to_project[
                query.object_modified.name
                if query.object_modified
                else query.fields_accessed[0].table.name
            ]
            if random.random() >= proabability_of_project_mismatch
            else random.choice(projects)
        )
        job_name = str(uuid.uuid4())
        referencedViews = list(
            dict.fromkeys(
                ref_from_table(field.table, table_to_project)
                for field in query.fields_accessed
                if field.table.is_view()
            )
        )

        yield AuditEvent.create(
            QueryEvent(
                job_name=job_name,
                timestamp=query.timestamp,
                actor_email=query.actor,
                query=query.text,
                statementType=random.choice(OPERATION_TYPE_MAP[query.type]),
                project_id=project,
                destinationTable=ref_from_table(query.object_modified, table_to_project)
                if query.object_modified
                else None,
                referencedTables=list(
                    dict.fromkeys(  # Preserve order
                        ref_from_table(field.table, table_to_project)
                        for field in query.fields_accessed
                        if not field.table.is_view()
                    )
                )
                + list(
                    dict.fromkeys(  # Preserve order
                        ref_from_table(parent, table_to_project)
                        for field in query.fields_accessed
                        if field.table.is_view()
                        for parent in field.table.upstreams
                    )
                ),
                referencedViews=referencedViews,
                payload=dataclasses.asdict(query)
                if config.debug_include_full_payloads
                else None,
                query_on_view=True if referencedViews else False,
            )
        )
        table_accesses: Dict[BigQueryTableRef, Set[str]] = defaultdict(set)
        for field in query.fields_accessed:
            if not field.table.is_view():
                table_accesses[ref_from_table(field.table, table_to_project)].add(
                    field.column
                )
            else:
                # assuming that same fields are accessed in parent tables
                for parent in field.table.upstreams:
                    table_accesses[ref_from_table(parent, table_to_project)].add(
                        field.column
                    )

        for ref, columns in table_accesses.items():
            yield AuditEvent.create(
                ReadEvent(
                    jobName=job_name,
                    timestamp=query.timestamp,
                    actor_email=query.actor,
                    resource=ref,
                    fieldsRead=list(columns),
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
