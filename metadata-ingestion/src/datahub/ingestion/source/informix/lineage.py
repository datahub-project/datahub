from typing import List, Optional

from datahub.emitter.mce_builder import make_schema_field_urn
from datahub.ingestion.source.informix.report import InformixSourceReport
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

# get_dialect_str("informix") maps to postgres centrally; keep an explicit
# override so connector view lineage does not depend on SchemaResolver.platform
# alone. Informix normalizes sysviews.viewtext to a qualified, aliased,
# comma-join form that postgres parses for the common case. Views that retain
# Informix-specific syntax (MATCHES / NOT MATCHES, FIRST / SKIP, native OUTER
# joins, DATETIME ... YEAR TO DAY) still fail to parse and fall through to the
# per-view failure path; see the connector docs.
_DIALECT = "postgres"


def build_view_upstream_lineage(
    view_urn: str,
    view_sql: str,
    schema_resolver: SchemaResolverInterface,
    database: str,
    owner: str,
    report: InformixSourceReport,
    view_columns: Optional[List[str]] = None,
) -> Optional[UpstreamLineageClass]:
    result = sqlglot_lineage(
        view_sql,
        schema_resolver=schema_resolver,
        default_db=database,
        default_schema=owner,
        override_dialect=_DIALECT,
    )
    # A table-level parse error means the view's sources couldn't be resolved at
    # all; re-raise so the caller records a warning + view_lineage_failures rather
    # than silently emitting nothing. A column-only error still leaves usable
    # coarse (table) lineage, so fall through and emit what did parse.
    if result.debug_info.table_error:
        raise result.debug_info.table_error
    if result.debug_info.column_error:
        report.view_column_lineage_failures += 1
        report.warning(
            title="View column lineage unavailable",
            message="The view's sources resolved but its column lineage did not "
            "parse, so only table-level lineage is emitted.",
            context=view_urn,
            exc=result.debug_info.column_error,
        )
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
    remap_cols = view_columns
    if view_columns is not None and len(view_columns) != len(col_lineages):
        # Without the remap the downstream schemaField URN carries the inner
        # projection name, which may not be a column the view actually exposes.
        # Count it so a systematically-misparsed catalog is visible in the report
        # rather than showing up as silently wrong column lineage.
        remap_cols = None
        report.view_column_remap_mismatches += 1
        report.warning(
            title="View column lineage may reference undeclared columns",
            message="Parsed projection count does not match the view's declared "
            "column count, so column lineage falls back to the inner projection "
            "names.",
            context=f"{view_urn} declared={len(view_columns)} "
            f"parsed={len(col_lineages)}",
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
