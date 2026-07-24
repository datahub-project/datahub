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
    view_columns: Optional[List[str]] = None,
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
    col_lineages = result.column_lineage or []
    # Informix normalizes views to `create view V (c1..cN) as select p1..pN`, moving any
    # column aliases into the outer column list. sqlglot keys the downstream column by the
    # inner projection name (p_i), not the view's declared column (c_i), so an aliased
    # column like `c.id AS customer_id` surfaces downstream as `id`. Remap positionally to
    # the view's declared columns (colno order == projection order) when counts align.
    remap_cols = (
        view_columns
        if view_columns is not None and len(view_columns) == len(col_lineages)
        else None
    )
    fine_grained: List[FineGrainedLineageClass] = []
    for idx, cl in enumerate(col_lineages):
        up_fields = [make_schema_field_urn(u.table, u.column) for u in cl.upstreams]
        if not up_fields:
            continue
        down_col = remap_cols[idx] if remap_cols is not None else cl.downstream.column
        fine_grained.append(
            FineGrainedLineageClass(
                upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                upstreams=up_fields,
                downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
                downstreams=[make_schema_field_urn(view_urn, down_col)],
            )
        )
    return UpstreamLineageClass(
        upstreams=upstreams, fineGrainedLineages=fine_grained or None
    )
