from typing import List, Optional

from datahub.emitter.mce_builder import make_schema_field_urn
from datahub.metadata.schema_classes import (
    DatasetLineageTypeClass,
    FineGrainedLineageClass,
    FineGrainedLineageDownstreamTypeClass,
    FineGrainedLineageUpstreamTypeClass,
    UpstreamClass,
    UpstreamLineageClass,
)
from datahub.sql_parsing.schema_resolver import SchemaResolverInterface
from datahub.sql_parsing.sqlglot_lineage import sqlglot_lineage

# sqlglot has no 'informix' dialect (get_dialect("informix") raises); Informix's
# normalized sysviews.viewtext parses correctly under the postgres dialect.
_DIALECT = "postgres"


def build_view_upstream_lineage(
    view_urn: str,
    view_sql: str,
    schema_resolver: SchemaResolverInterface,
    database: str,
    owner: str,
) -> Optional[UpstreamLineageClass]:
    result = sqlglot_lineage(
        view_sql,
        schema_resolver=schema_resolver,
        default_db=database,
        default_schema=owner,
        override_dialect=_DIALECT,
    )
    if result.debug_info.error:
        return None
    if not result.in_tables:
        return None

    upstreams = [
        UpstreamClass(dataset=urn, type=DatasetLineageTypeClass.VIEW)
        for urn in result.in_tables
    ]
    fine_grained: List[FineGrainedLineageClass] = []
    for cl in result.column_lineage or []:
        up_fields = [make_schema_field_urn(u.table, u.column) for u in cl.upstreams]
        if not up_fields:
            continue
        fine_grained.append(
            FineGrainedLineageClass(
                upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                upstreams=up_fields,
                downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
                downstreams=[make_schema_field_urn(view_urn, cl.downstream.column)],
            )
        )
    return UpstreamLineageClass(
        upstreams=upstreams, fineGrainedLineages=fine_grained or None
    )
