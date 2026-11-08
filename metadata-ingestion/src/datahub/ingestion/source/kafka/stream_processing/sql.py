from typing import List, Optional

from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.source.kafka.kafka_report import KafkaSourceReport
from datahub.metadata.schema_classes import (
    FineGrainedLineageClass,
    FineGrainedLineageDownstreamTypeClass,
    FineGrainedLineageUpstreamTypeClass,
)
from datahub.sql_parsing.sqlglot_lineage import create_lineage_sql_parsed_result


def column_lineage_fine_grained(
    query: str,
    platform: str,
    platform_instance: Optional[str],
    env: str,
    graph: Optional[DataHubGraph],
    dialect: Optional[str],
    report: KafkaSourceReport,
    job_name: str,
) -> List[FineGrainedLineageClass]:
    # Schema-aware when the graph is available (so `SELECT *` and column types
    # resolve); otherwise only explicitly projected columns get lineage. A parse
    # failure costs the column lineage only; the job keeps its topic-level edges.
    if not query:
        return []
    try:
        result = create_lineage_sql_parsed_result(
            query=query,
            default_db=None,
            platform=platform,
            platform_instance=platform_instance,
            env=env,
            graph=graph,
            schema_aware=graph is not None,
            override_dialect=dialect,
        )
        error = result.debug_info.error
    except Exception as e:
        result = None
        error = e
    if error is not None:
        report.warning(
            title="Flink column-level lineage skipped",
            message="Could not derive column-level lineage from a Flink statement; its "
            "topic-level lineage is still emitted",
            context=f"statement={job_name}",
            exc=error,
        )
    if result is None or not result.column_lineage:
        return []

    if not result.column_lineage:
        return []

    fine_grained: List[FineGrainedLineageClass] = []
    for column_lineage in result.column_lineage:
        downstream_urn = column_lineage.downstream_schema_field_urn()
        if not downstream_urn:
            continue
        upstreams = column_lineage.upstream_schema_field_urns()
        if not upstreams:
            continue
        fine_grained.append(
            FineGrainedLineageClass(
                upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
                upstreams=upstreams,
                downstreams=[downstream_urn],
            )
        )
    return fine_grained
