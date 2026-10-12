"""Restore sqlglot's pre-30.22 single-source column inference.

Up to 30.21, ``qualify_columns`` ran with ``infer_schema=True`` whenever the schema
was empty: an unqualified column was attributed to the one source in its scope whose
columns are unknown, e.g. ``SELECT a FROM t`` became ``SELECT t.a AS a FROM t AS t``.
30.22 removed that option, leaving the column unqualified. DataHub routinely parses
SQL without any schema info, and without the inference column lineage through CTEs,
subqueries and joins loses its upstream table.

30.22 moved a similar fallback into ``sqlglot.lineage``, but it counts every source
visible to a scope rather than the ones it selects from. A CTE can see the CTEs
defined before it, so in any query with two or more CTEs the fallback finds several
candidates and gives up.

This runs after ``qualify`` and applies the removed rule from
``Resolver.get_table`` to whatever columns qualification left unqualified. Delete it
if upstream brings the inference back.
"""

from typing import List, Optional

from sqlglot import exp
from sqlglot.errors import OptimizeError
from sqlglot.optimizer.resolver import Resolver
from sqlglot.optimizer.scope import Scope, find_all_in_scope, traverse_scope
from sqlglot.schema import Schema


def infer_single_source_columns(expression: exp.Expr, schema: Schema) -> exp.Expr:
    # 30.21 only inferred when the whole schema was empty, never for a partial one.
    if not schema.empty:
        return expression

    for scope in traverse_scope(expression):
        unqualified = [
            column for column in _candidate_columns(scope) if not column.table
        ]
        if not unqualified:
            continue

        source_name = _sole_source_without_schema(scope, schema)
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
            column.set("table", _source_identifier(scope, source_name))

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


def _sole_source_without_schema(scope: Scope, schema: Schema) -> Optional[str]:
    resolver = Resolver(scope, schema)
    without_schema: List[str] = []
    for name in [*scope.selected_sources, *scope.lateral_sources]:
        try:
            columns = resolver.get_source_columns(name)
        except OptimizeError:
            return None
        if not columns or "*" in columns:
            without_schema.append(name)
    return without_schema[0] if len(without_schema) == 1 else None


def _source_identifier(scope: Scope, source_name: str) -> exp.Identifier:
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
