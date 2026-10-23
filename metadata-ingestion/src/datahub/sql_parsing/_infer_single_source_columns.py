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
        unqualified = [column for column in candidates if not column.table]
        struct_paths = [
            column for column in candidates if _is_unresolved_struct_path(scope, column)
        ]
        if not unqualified and not struct_paths:
            continue

        source_name = sole_source_without_schema(scope, schema)
        if source_name is None:
            continue

        # ORDER BY and QUALIFY may name a projection alias rather than a source column.
        projection_aliases = (
            set(scope.expression.named_selects)
            if isinstance(scope.expression, exp.Select)
            else set()
        )
        for column in unqualified:
            if column.name in projection_aliases and column.find_ancestor(
                exp.Order, exp.Qualify
            ):
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
        if struct_paths:
            scope.clear_cache()

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


def _is_unresolved_struct_path(scope: Scope, column: exp.Column) -> bool:
    # Same guard as _convert_columns_to_dots: a correlated subquery's reference to an
    # outer source is a column of that source, not a struct path.
    table = column.table
    return bool(
        table
        and not isinstance(column.this, exp.Star)
        and table not in scope.selected_sources
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
