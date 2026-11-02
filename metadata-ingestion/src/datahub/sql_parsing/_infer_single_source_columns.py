"""Restore sqlglot's pre-30.22 single-source column inference.

Up to 30.21, ``qualify_columns`` ran with ``infer_schema=True`` whenever the schema
was empty: an unqualified column was attributed to the one source in its scope whose
columns are unknown, e.g. ``SELECT a FROM t`` became ``SELECT t.a AS a FROM t AS t``.
The same inference let it read ``s.f`` as field ``f`` of a struct column ``s`` when
no source is named ``s``, giving ``SELECT t.s.f AS f FROM t AS t``. 30.22 removed
that option, leaving both unresolved. DataHub routinely parses SQL without any schema
info, and without the inference column lineage through CTEs, subqueries and joins
loses its upstream table.

30.22 moved a similar fallback into ``sqlglot.lineage``, but it counts every source
visible to a scope rather than the ones it selects from. A CTE can see the CTEs
defined before it, so in any query with two or more CTEs the fallback finds several
candidates and gives up.

This runs after ``qualify`` and applies the removed rule from
``Resolver.get_table`` to whatever columns qualification left unresolved. Delete it
if upstream brings the inference back.
"""

import logging
from typing import List, Optional

from sqlglot import exp
from sqlglot.errors import OptimizeError
from sqlglot.optimizer.resolver import Resolver
from sqlglot.optimizer.scope import Scope, find_all_in_scope, traverse_scope
from sqlglot.schema import Schema

logger = logging.getLogger(__name__)


def infer_single_source_columns(expression: exp.Expr, schema: Schema) -> exp.Expr:
    # 30.21 only inferred when the whole schema was empty, never for a partial one.
    if not schema.empty:
        return expression

    for scope in traverse_scope(expression):
        candidates = _candidate_columns(scope)
        unqualified = [
            column
            for column in candidates
            if not column.table or _names_enclosing_source(scope, column)
        ]
        # Compared by identity: sqlglot's == walks the whole node.
        unqualified_ids = {id(column) for column in unqualified}
        struct_paths = [
            column
            for column in candidates
            if id(column) not in unqualified_ids
            and _is_unresolved_struct_path(scope, column)
        ]
        table_columns = scope.table_columns
        if not unqualified and not struct_paths and not table_columns:
            continue

        source_name = sole_source_without_schema(scope, schema)
        if source_name is None:
            continue

        for column in unqualified:
            if may_name_projection_alias(scope, column):
                continue
            column.set("table", source_identifier(scope, source_name))

        for column in struct_paths:
            # Mirrors qualify_columns' _convert_columns_to_dots: `s.f` becomes `t.s.f`.
            root, *fields = column.parts
            column.replace(
                exp.Dot.build(
                    [
                        exp.column(root, table=source_identifier(scope, source_name)),
                        *fields,
                    ]
                )
            )

        # Since 30.22, Postgres-like dialects read an unqualified column named like a
        # selected table as that table's whole row. 30.21 attributed it to the sole
        # source first, as a column.
        for table_column in table_columns:
            table_column.replace(
                exp.column(
                    table_column.this, table=source_identifier(scope, source_name)
                )
            )

        # A cached column list would still show what we just qualified as unqualified,
        # in this scope and in the parent that lists it as an outer reference.
        scope.clear_cache()
        if scope.parent:
            scope.parent.clear_cache()

    return expression


def _candidate_columns(scope: Scope) -> List[exp.Column]:
    columns = list(scope.columns)
    # Scope.columns leaves out QUALIFY, but 30.21 qualified it too.
    qualify = scope.expression.args.get("qualify")
    if qualify is not None:
        seen = {id(column) for column in columns}
        columns.extend(
            column
            for column in find_all_in_scope(qualify, exp.Column)
            if id(column) not in seen
        )
    return columns


def may_name_projection_alias(scope: Scope, column: exp.Column) -> bool:
    # The scope's own ORDER BY and QUALIFY may name a projection alias rather than a
    # source column. An ORDER BY inside a window, WITHIN GROUP or aggregate can't.
    if not isinstance(scope.expression, exp.Select):
        return False
    clause = column.find_ancestor(exp.Order, exp.Qualify)
    return (
        clause is not None
        and clause.parent is scope.expression
        and column.name in scope.expression.named_selects
    )


def _names_enclosing_source(scope: Scope, column: exp.Column) -> bool:
    # qualify gives a LATERAL subquery's unqualified columns the lateral's own alias,
    # e.g. `y` in `LATERAL (SELECT y FROM u) AS l` becomes `l.y`. The alias names the
    # subquery's output, so inside it the column is as good as unqualified.
    table = column.table
    if not table or table in scope.sources or len(column.parts) != 2:
        return False
    node = scope.expression.parent
    while isinstance(node, (exp.Subquery, exp.Lateral)):
        if node.alias:
            return node.alias == table
        node = node.parent
    return False


def _is_unresolved_struct_path(scope: Scope, column: exp.Column) -> bool:
    # Same guard as _convert_columns_to_dots: a correlated subquery's reference to an
    # outer source is a column of that source, not a struct path. Columns read from a
    # PIVOT carry the pivot's alias, which lineage() resolves through the pivot.
    table = column.table
    return bool(
        table
        and not isinstance(column.this, exp.Star)
        and table not in scope.selected_sources
        and table not in {pivot.alias for pivot in scope.pivots}
        and (
            not scope.parent
            or table not in scope.parent.sources
            or not scope.is_correlated_subquery
        )
    )


def sole_source_without_schema(scope: Scope, schema: Schema) -> Optional[str]:
    """Name the one source of ``scope`` without schema info, if there is exactly one."""
    resolver = Resolver(scope, schema)
    without_schema: List[str] = []
    for name in [*scope.selected_sources, *scope.lateral_sources]:
        try:
            columns = resolver.get_source_columns(name)
        except OptimizeError as e:
            # Unknown source columns make the attribution ambiguous, so infer nothing.
            logger.debug("Not inferring a source for %s: %s", scope.expression, e)
            return None
        if not columns or "*" in columns:
            without_schema.append(name)
    return without_schema[0] if len(without_schema) == 1 else None


def source_identifier(scope: Scope, source_name: str) -> exp.Identifier:
    """Build the identifier that qualifies a column with ``source_name``."""
    # Mirrors the tail of Resolver.get_table: reuse the source's own alias identifier
    # so its quoting carries over.
    if source_name not in scope.selected_sources:
        return exp.to_identifier(source_name)

    node: Optional[exp.Expr] = scope.selected_sources[source_name][0]
    if isinstance(node, exp.Query):
        while node and node.alias != source_name and node.parent:
            node = node.parent

    node_alias = node.args.get("alias") if node else None
    if node_alias and isinstance(node_alias.this, exp.Identifier):
        return node_alias.this.copy()
    return exp.to_identifier(source_name)
