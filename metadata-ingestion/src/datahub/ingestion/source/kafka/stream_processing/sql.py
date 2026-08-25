import logging
from typing import List, Optional

from datahub.emitter.mce_builder import make_schema_field_urn
from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.schema_classes import (
    FineGrainedLineageClass,
    FineGrainedLineageDownstreamTypeClass,
    FineGrainedLineageUpstreamTypeClass,
)
from datahub.sql_parsing.sqlglot_lineage import create_lineage_sql_parsed_result

logger = logging.getLogger(__name__)


def column_lineage_fine_grained(
    query: str,
    platform: str,
    platform_instance: Optional[str],
    env: str,
    graph: Optional[DataHubGraph],
    dialect: Optional[str],
) -> List[FineGrainedLineageClass]:
    # Best-effort: schema-aware when the graph is available (so `SELECT *` and column
    # types resolve), otherwise falls back to schema-unaware parsing, which still yields
    # column lineage for explicitly-projected columns. A parse miss yields no CLL and the
    # caller still emits table-level lineage.
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
    except Exception as e:
        logger.debug(f"Column-level lineage parse failed: {e}")
        return []

    if not result.column_lineage:
        return []

    fine_grained: List[FineGrainedLineageClass] = []
    for column_lineage in result.column_lineage:
        downstream_urn = column_lineage.downstream_schema_field_urn()
        if not downstream_urn:
            continue
        upstreams = [
            make_schema_field_urn(str(upstream.table), upstream.column)
            for upstream in column_lineage.upstreams
        ]
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
