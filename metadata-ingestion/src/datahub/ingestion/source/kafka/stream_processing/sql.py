from typing import Dict, List, Optional, Set

import sqlglot
from sqlglot import exp

from datahub.emitter.mce_builder import make_schema_field_urn
from datahub.ingestion.source.kafka.kafka_report import KafkaSourceReport
from datahub.ingestion.source.kafka.stream_processing.constants import (
    FLINK_SQL_DIALECT,
)
from datahub.ingestion.source.kafka.stream_processing.models import FlinkJob
from datahub.metadata.schema_classes import (
    FineGrainedLineageClass,
    FineGrainedLineageDownstreamTypeClass,
    FineGrainedLineageUpstreamTypeClass,
)
from datahub.metadata.urns import DatasetUrn
from datahub.sql_parsing.schema_resolver import SchemaResolver
from datahub.sql_parsing.sqlglot_lineage import sqlglot_lineage


def column_lineage_fine_grained(
    job: FlinkJob,
    topic_urns: Dict[str, str],
    schema_resolver: SchemaResolver,
    platform_instance: Optional[str],
    report: KafkaSourceReport,
) -> List[FineGrainedLineageClass]:
    """Column-level lineage for a job, on the job's real topic and field names.

    `topic_urns` maps each of the job's topics to its dataset URN. The SQL parser
    lowercases table names, and column names too unless it found a schema, so each
    table is mapped back to the job's topic by case-insensitive name and each column
    to a name whose case is known: a field of the topic's schema in DataHub, the
    spelling the query uses, or the upstream field a column passes through unchanged.
    Edges with any name that cannot be placed are dropped.
    """
    if not job.parse_query:
        return []
    try:
        result = sqlglot_lineage(
            job.parse_query,
            schema_resolver=schema_resolver,
            override_dialect=FLINK_SQL_DIALECT,
        )
        error = result.debug_info.error
    except Exception as e:
        return _skipped(report, job.name, e)
    if error is not None:
        return _skipped(report, job.name, error)
    if not result.column_lineage:
        return []

    by_lower_name = _unambiguous_lower_names(topic_urns)
    real_tables = {
        urn: by_lower_name.get(_topic_name(urn, platform_instance).lower())
        for urn in list(result.in_tables) + list(result.out_tables)
    }
    written = _written_spellings(job.parse_query)
    schema_fields: Dict[str, Dict[str, str]] = {}

    def real_column(real_table: str, column: str) -> Optional[str]:
        if real_table not in schema_fields:
            schema = schema_resolver.resolve_urn(real_table)[1]
            schema_fields[real_table] = _unambiguous_lower_names(
                {field: field for field in schema or {}}
            )
        if schema_fields[real_table]:
            return schema_fields[real_table].get(column.lower())
        return written.get(column.lower())

    fine_grained: List[FineGrainedLineageClass] = []
    for column_lineage in result.column_lineage:
        upstreams: List[str] = []
        upstream_names: Set[str] = set()
        for upstream in column_lineage.upstreams:
            table = real_tables.get(upstream.table)
            column = real_column(table, upstream.column) if table else None
            if table is None or column is None:
                break
            upstreams.append(make_schema_field_urn(table, column))
            upstream_names.add(column)
        else:
            downstream_ref = column_lineage.downstream
            table = (
                real_tables.get(downstream_ref.table) if downstream_ref.table else None
            )
            column = real_column(table, downstream_ref.column) if table else None
            if column is None and len(upstream_names) == 1:
                # A pass-through column (e.g. from SELECT *) keeps its upstream name.
                only = next(iter(upstream_names))
                column = only if only.lower() == downstream_ref.column.lower() else None
            if table is None or column is None or not upstreams:
                continue
            fine_grained.append(
                FineGrainedLineageClass(
                    upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                    downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
                    upstreams=upstreams,
                    downstreams=[make_schema_field_urn(table, column)],
                )
            )
    return fine_grained


def _skipped(
    report: KafkaSourceReport, job_name: str, error: Exception
) -> List[FineGrainedLineageClass]:
    report.warning(
        title="Flink column-level lineage skipped",
        message="Could not derive column-level lineage from a Flink statement; its "
        "topic-level lineage is still emitted",
        context=f"statement={job_name}",
        exc=error,
    )
    return []


def _unambiguous_lower_names(names: Dict[str, str]) -> Dict[str, str]:
    # Two names differing only by case cannot be told apart once lowercased.
    seen: Dict[str, str] = {}
    ambiguous: Set[str] = set()
    for name, value in names.items():
        key = name.lower()
        if key in seen and seen[key] != value:
            ambiguous.add(key)
        seen[key] = value
    return {key: urn for key, urn in seen.items() if key not in ambiguous}


def _topic_name(dataset_urn: str, platform_instance: Optional[str]) -> str:
    name = DatasetUrn.from_string(dataset_urn).name
    prefix = f"{platform_instance}." if platform_instance else ""
    if prefix and name.lower().startswith(prefix.lower()):
        return name[len(prefix) :]
    return name


def _written_spellings(query: str) -> Dict[str, str]:
    # Lowercase name -> the one spelling the query uses for it, over column references
    # and output aliases only, so a table or alias named like a column does not count.
    parsed = sqlglot.parse_one(query, read=FLINK_SQL_DIALECT)
    names = [column.name for column in parsed.find_all(exp.Column)]
    names += [alias.alias for alias in parsed.find_all(exp.Alias)]
    return _unambiguous_lower_names({name: name for name in names if name})
