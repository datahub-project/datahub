"""Vendored copy of sqlglot 30.20.0's ``pushdown_projections`` optimizer rule.

sqlglot 30.21.0 replaced this rule with a reachability-based algorithm that raises
``OptimizeError`` whenever a projection star cannot be expanded. DataHub routinely
parses SQL without schema info for every table, so ``SELECT *`` over an unknown
table is common. The old algorithm handled that case by narrowing the star to the
columns the outer query actually reads, e.g. ``SELECT x.a FROM (SELECT * FROM t) AS x``
became ``SELECT x.a FROM (SELECT t.a AS a FROM t) AS x``, which is what lets us
trace column lineage without a schema.

``sqlglot_lineage`` runs the upstream rule first and only falls back to this copy
when it raises. Upstream raises before mutating the tree, so the fallback always
sees an untouched expression.

Changes from upstream: this docstring, the ``Resolver`` import (moved to
``sqlglot.optimizer.resolver`` in 30.21.0), and a ``noqa: C901`` on
``pushdown_projections``. ``_remove_unused_selections`` stays untyped as upstream
has it; setup.cfg exempts this module from ``disallow_untyped_defs``. Keep the rest identical so it can be diffed against
upstream. Delete this module once upstream can skip unexpandable stars.

sqlglot is MIT licensed: https://github.com/tobymao/sqlglot/blob/main/LICENSE
"""

from __future__ import annotations

import typing as t
from collections import defaultdict

from sqlglot import alias, exp
from sqlglot.errors import OptimizeError
from sqlglot.helper import seq_get
from sqlglot.optimizer.helpers import projection_has_aggregate
from sqlglot.optimizer.journal import Journal, record, revert
from sqlglot.optimizer.resolver import Resolver
from sqlglot.optimizer.scope import (
    Scope,
    find_all_in_scope,
    find_in_scope,
    traverse_scope,
)
from sqlglot.schema import ensure_schema

if t.TYPE_CHECKING:
    from sqlglot._typing import E
    from sqlglot.dialects.dialect import DialectType
    from sqlglot.schema import Schema

# Sentinel value that means an outer query selecting ALL columns
SELECT_ALL = object()

SET_RETURNING_FUNCTIONS = exp.SET_RETURNING_FUNCTIONS

# GROUP BY constructs whose children are grouping items; a one-column set, e.g. ((1)), is a Paren
GROUPING_CONSTRUCTS = (exp.Cube, exp.GroupingSets, exp.Paren, exp.Rollup, exp.Tuple)


class PruningFrame(t.NamedTuple):
    # visited scopes in the subtree rooted at a set operation
    scopes: set[Scope]

    # source scopes those visited scopes depend on
    sources: set[Scope]

    # whether the remaining branches in the subtree must keep every column
    disabled: bool

    # journal index where the subtree's mutations start, used to revert them
    start: int


def _output_column_refs(expression: exp.Expr, scoped: bool) -> set[str]:
    refs: set[str] = set()

    for arg in ("order", "sort", "distribute", "cluster"):
        node = expression.args.get(arg)
        if node:
            columns = (
                find_all_in_scope(node, exp.Column)
                if scoped
                else node.find_all(exp.Column)
            )
            refs.update(c.name for c in columns if not c.table)

    return refs


def _is_self_referencing_cte(scope: Scope) -> bool:
    cte = scope.expression.parent
    return (
        isinstance(cte, exp.CTE)
        and isinstance(cte.parent, exp.With)
        and cte.parent.recursive
        and any(
            not table.db and table.name == cte.alias
            for table in scope.expression.find_all(exp.Table)
        )
    )


# Selection to use if selection list is empty
def default_selection(is_agg: bool) -> exp.Alias:
    return alias(exp.Max(this=exp.Literal.number(1)) if is_agg else "1", "_").assert_is(
        exp.Alias
    )


def pushdown_projections(  # noqa: C901
    expression: E,
    schema: dict[str, object] | Schema | None = None,
    dialect: DialectType = None,
    journal: Journal | None = None,
) -> E:
    """
    Rewrite sqlglot AST to remove unused columns projections.

    Example:
        >>> import sqlglot
        >>> sql = "SELECT y.a AS a FROM (SELECT x.a AS a, x.b AS b FROM x) AS y"
        >>> expression = sqlglot.parse_one(sql)
        >>> pushdown_projections(expression).sql()
        'SELECT y.a AS a FROM (SELECT x.a AS a FROM x) AS y'

    Args:
        expression: the expression to optimize, mutated in place.
        schema: the database schema, used to expand `*` projections.
        dialect: the dialect of the expression.
        journal: if given, records every mutation so that `revert(journal)` undoes this rule.

    Returns:
        The optimized expression.
    """
    schema = ensure_schema(schema, dialect=dialect)
    source_column_alias_count: dict[Scope, int] = {}

    # Map of Scope to all columns being selected by outer queries.
    referenced_columns: defaultdict[Scope, set[str | object]] = defaultdict(set)

    # Pruning needs to be avoided for some scope subtrees rooted at set operations, e.g.:
    #
    #   SELECT t.a FROM ((SELECT DISTINCT a, b FROM x) UNION ALL SELECT b, c FROM y) AS t
    #
    # If we trimmed the right branch of the union operation to just b, we would get
    # an "unequal number of projections" error, since the left branch can't be pruned.
    #
    # The following state helps us revert these pruning decisions after the fact.
    pruning_stack: list[PruningFrame] = []
    pruning_journal: Journal = journal if journal is not None else []

    # We build the scope tree (which is traversed in DFS postorder), then iterate
    # over the result in reverse order. This should ensure that the set of selected
    # columns for a particular scope are completely build by the time we get to it.
    for scope in reversed(traverse_scope(expression)):
        scope_expression = scope.expression
        parent_selections = referenced_columns.get(scope, {SELECT_ALL})
        alias_count = max(
            source_column_alias_count.get(scope, 0), len(scope.outer_columns)
        )
        widened = False

        # Do not optimize this set operation if it's using the BigQuery-specific kind / side
        # syntax (e.g INNER UNION ALL BY NAME), which changes the semantics of the operation
        unsupported_set_operation = isinstance(scope_expression, exp.SetOperation) and (
            scope_expression.kind or scope_expression.side
        )

        while pruning_stack and scope.parent not in pruning_stack[-1].scopes:
            nested_sources = pruning_stack.pop().sources
            if pruning_stack:
                # The enclosing frame's journal start precedes the nested frame's, so rolling it back also
                # reverts the nested pruning; it must then relax the nested sources as well (e.g., CTEs)
                pruning_stack[-1].sources.update(nested_sources)
            elif journal is None:
                pruning_journal.clear()

        if pruning_stack:
            if (
                isinstance(scope_expression, exp.SetOperation)
                and not scope.is_set_operation
            ):
                # This set operation is a source of a branch, e.g. the inner union in
                # SELECT s.a FROM (... UNION ...) AS s UNION ALL SELECT b FROM z, so its branches can
                # widen without affecting the width of the outer union's branches. A separate frame
                # keeps such a rollback from undoing the outer union's pruning
                pruning_stack.append(
                    PruningFrame({scope}, set(), False, len(pruning_journal))
                )
            else:
                pruning_stack[-1].scopes.add(scope)

        if pruning_stack and pruning_stack[-1].disabled and scope.is_set_operation:
            parent_selections = {SELECT_ALL}

        # SELECT DISTINCT, UNION DISTINCT, INTERSECT, and EXCEPT consume the entire row, so we
        # can't remove any columns, otherwise we risk changing the query's semantics. Also, we
        # conservatively skip pruning on recursive CTEs that read their own output for now.
        order = scope_expression.args.get("order")
        if (
            scope_expression.args.get("distinct")
            or isinstance(scope_expression, (exp.Intersect, exp.Except))
            or _is_self_referencing_cte(scope)
            or unsupported_set_operation
            or (order and any(ordered.this.is_int for ordered in order.expressions))
        ):
            widened = SELECT_ALL not in parent_selections
            parent_selections = {SELECT_ALL}

        if (
            isinstance(scope_expression, exp.SetOperation)
            and not unsupported_set_operation
        ):
            if not pruning_stack and SELECT_ALL not in parent_selections:
                pruning_stack.append(
                    PruningFrame({scope}, set(), False, len(pruning_journal))
                )

            left, right = scope.set_operation_scopes
            le = left.expression
            re = right.expression

            if not (isinstance(le, exp.Selectable) and isinstance(re, exp.Selectable)):
                continue

            by_name = scope_expression.args.get("by_name")

            if alias_count and by_name:
                # The aliases name the merged output, which doesn't map onto operand positions
                widened = SELECT_ALL not in parent_selections
                parent_selections = {SELECT_ALL}

            if not by_name and len(le.selects) != len(re.selects):
                scope_sql = scope_expression.sql(dialect=dialect)
                raise OptimizeError(
                    f"Invalid set operation due to column mismatch: {scope_sql}."
                )

            # Columns referenced by ORDER BY and friends need to be kept too
            if SELECT_ALL not in parent_selections:
                output_refs = _output_column_refs(scope_expression, scoped=True)
                if output_refs:
                    widened = not output_refs.issubset(parent_selections)
                    parent_selections = parent_selections | output_refs

            referenced_columns[left] = parent_selections

            if re.is_star:
                referenced_columns[right] = parent_selections
            elif not le.is_star:
                if by_name:
                    referenced_columns[right] = parent_selections
                elif SELECT_ALL not in parent_selections:
                    # This being unset means looking up `right` later yields SELECT_ALL (default)
                    referenced_columns[right] = {
                        re.selects[i].alias_or_name
                        for i, select in enumerate(le.selects)
                        if select.alias_or_name in parent_selections
                    }

        if (
            isinstance(scope_expression, exp.Select)
            and SELECT_ALL not in parent_selections
        ):
            widened = _remove_unused_selections(
                scope,
                parent_selections,
                schema,
                alias_count,
                pruning_journal if pruning_stack else journal,
            )

        if widened and pruning_stack and scope.is_set_operation:
            frame = pruning_stack[-1]

            # Scope subtrees are fully visited before traversing sibling subtrees, so reverting the
            # journaled edits here is safe, because they're all related to the frame's set op. scope
            revert(pruning_journal, frame.start)
            for pruned_source in frame.sources:
                referenced_columns[pruned_source].add(SELECT_ALL)

            pruning_stack[-1] = frame._replace(disabled=True)
            scope.clear_cache()

        if isinstance(scope_expression, exp.Select):
            if scope.scans_all_subscope_columns:
                continue

            # Group columns by source name
            selects: dict[str, set[object]] = defaultdict(set)
            for col in scope.columns:
                selects[col.table].add(col.name)
            for table_column in scope.table_columns:
                selects[table_column.name].add(SELECT_ALL)

            # Push the selected columns down to the next scope
            for name, (node, source) in scope.selected_sources.items():
                if isinstance(source, Scope) and isinstance(
                    source.expression, exp.Selectable
                ):
                    if pruning_stack:
                        pruning_stack[-1].sources.add(source)

                    select = seq_get(source.expression.selects, 0)

                    if scope.pivots or isinstance(select, exp.QueryTransform):
                        columns: set[object] = {SELECT_ALL}
                    else:
                        unqualified = selects.get("", set())
                        columns = (
                            {SELECT_ALL}
                            if name in unqualified
                            else (selects.get(name) or set())
                        )

                    referenced_columns[source].update(columns)

                    column_aliases = node.alias_column_names
                    if column_aliases:
                        source_column_alias_count[source] = max(
                            source_column_alias_count.get(source, 0),
                            len(column_aliases),
                        )

    return expression


def _remove_unused_selections(
    scope, parent_selections, schema, alias_count, journal=None
):
    expression = scope.expression
    windows = expression.args.get("windows")
    output_refs = _output_column_refs(expression, scoped=False)

    # Resolve GROUP BY ordinals before pruning
    ordinal_refs = _group_by_ordinal_refs(expression)
    group_ordinal_selection_ids = {id(selection) for _, selection in ordinal_refs}

    # GROUP BY ALL with no explicit keys implicitly groups by every non-aggregate
    # projection, so those projections are grouping keys and can't be pruned
    implicit_group_by_all = _is_implicit_group_by_all(expression)

    new_selections = []
    removed = False
    widened = False
    star = False
    is_agg = False

    for selection in expression.selects:
        name = selection.alias_or_name
        referenced = name in parent_selections
        is_agg_selection = (
            implicit_group_by_all or not is_agg
        ) and projection_has_aggregate(selection, windows)

        if (
            referenced
            or name in output_refs
            or alias_count > 0
            or id(selection) in group_ordinal_selection_ids
            or (implicit_group_by_all and not is_agg_selection)
        ):
            new_selections.append(selection)
            alias_count -= 1
            widened = widened or not referenced
        # keep projections containing these functions
        elif find_in_scope(selection, *SET_RETURNING_FUNCTIONS):
            new_selections.append(selection)
            widened = True
        else:
            if selection.is_star:
                star = True
            removed = True

        if not is_agg and is_agg_selection:
            is_agg = True

    if star:
        resolver = Resolver(scope, schema)
        names = {s.alias_or_name for s in new_selections}

        for name in sorted(parent_selections):
            if name not in names:
                new_selections.append(
                    alias(
                        exp.column(name, table=resolver.get_table(name)),
                        name,
                        copy=False,
                    )
                )

    # If there are no remaining selections, just select a single constant
    if not new_selections:
        new_selections.append(default_selection(is_agg))

    if journal is not None and removed:
        record(journal, expression, "expressions")

    expression.select(*new_selections, append=False, copy=False)

    # Rewrite GROUP BY ordinals to their positions in the pruned SELECT list
    if ordinal_refs:
        new_pos = {id(selection): i + 1 for i, selection in enumerate(new_selections)}
        for node, old_selection in ordinal_refs:
            pos = new_pos.get(id(old_selection))
            if pos is not None and int(node.this) != pos:
                if journal is not None:
                    record(journal, node, "this")
                node.set("this", str(pos))

    if removed:
        scope.clear_cache()

    # Count duplicate names too; an unreferenced SELECT still needs one output.
    return (widened and bool(parent_selections)) or len(new_selections) > (
        len(parent_selections) or 1
    )


def _is_implicit_group_by_all(select: exp.Select) -> bool:
    """Bare GROUP BY ALL infers its keys from the SELECT list, unlike ALL as a
    grouping-sets modifier (e.g. GROUP BY ALL CUBE (...) or GROUP BY ALL a, b)."""
    group = select.args.get("group")
    if not group or not group.args.get("all"):
        return False

    return not (
        group.expressions
        or group.args.get("cube")
        or group.args.get("rollup")
        or group.args.get("grouping_sets")
    )


def _group_by_ordinal_refs(
    select: exp.Select,
) -> list[tuple[exp.Literal, exp.Expr]]:
    """Map each GROUP BY integer ordinal to its pre-prune projection, including the ordinals
    nested in a grouping construct such as GROUPING SETS / CUBE / ROLLUP."""
    group = select.args.get("group")
    if not group:
        return []

    selects = select.selects
    n = len(selects)
    refs: list[tuple[exp.Literal, exp.Expr]] = []

    def collect(nodes: t.Iterable[exp.Expr]) -> None:
        for node in nodes:
            if isinstance(node, GROUPING_CONSTRUCTS):
                collect(node.iter_expressions())
            elif node.is_int and isinstance(node, exp.Literal):
                pos = int(node.this)
                if 1 <= pos <= n:
                    refs.append((node, selects[pos - 1]))

    collect(group.iter_expressions())

    return refs
