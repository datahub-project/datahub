"""Runtime patches for sqlglot to add features and mitigate bugs.

Prior to sqlglot v29, we used the `patchy` library to apply source-level patches
(text diffs) to sqlglot functions. However, sqlglot[c] uses a C-based tokenizer which
breaks patchy's mechanism - patchy uses introspection and weak references that fail
when C extensions are present, causing "TypeError: cannot create weak reference to
'method_descriptor' object" even though the code we're patching is pure Python.

Since our patches modify behavior in the middle of functions (not just at entry/exit),
we cannot use simple wrapper functions. Full function replacement is the only practical
approach in Python when source-level patching tools don't work and you need to modify
internal function behavior.

This module replaces entire functions with modified versions where 95% of the code is
unchanged from sqlglot v29.0.1. Lines modified from the original are marked with
"##! PATCH" at the end. This is a standard Python pattern for monkey-patching when
source-level tools fail.
"""

import dataclasses
import itertools
import logging
from copy import deepcopy
from typing import Any, List, Tuple

import sqlglot
import sqlglot.expression_core
import sqlglot.expressions
import sqlglot.lineage
import sqlglot.optimizer.scope
import sqlglot.optimizer.unnest_subqueries

logger = logging.getLogger(__name__)

# Store original functions
_original_deepcopy = sqlglot.expression_core.ExpressionCore.__deepcopy__
_original_traverse = sqlglot.optimizer.scope.Scope.traverse


def _patched_deepcopy(
    self: sqlglot.expression_core.ExpressionCore, memo: Any
) -> sqlglot.expression_core.ExpressionCore:
    """Patched __deepcopy__ with cooperative timeout support.

    PATCH: Adds cooperative timeout checks to prevent hangs on deeply nested expressions.
    This is a full replacement of sqlglot v29's ExpressionCore.__deepcopy__.
    Only modification: Added 3 lines at the start to check for timeouts.
    """
    import datahub.utilities.cooperative_timeout  ##! PATCH

    datahub.utilities.cooperative_timeout.cooperate()  ##! PATCH

    # Original implementation from sqlglot v29 (unchanged below this point)
    root = self.__class__()
    stack: List[
        Tuple[
            sqlglot.expression_core.ExpressionCore,
            sqlglot.expression_core.ExpressionCore,
        ]
    ] = [(self, root)]

    while stack:
        node, copy = stack.pop()

        if node.comments is not None:
            copy.comments = deepcopy(node.comments)
        if node._type is not None:
            copy._type = deepcopy(node._type)
        if node._meta is not None:
            copy._meta = deepcopy(node._meta)
        if node._hash is not None:
            copy._hash = node._hash

        for k, vs in node.args.items():
            if isinstance(vs, sqlglot.expression_core.ExpressionCore):
                stack.append((vs, vs.__class__()))
                copy.set(k, stack[-1][-1])
            elif type(vs) is list:
                copy.args[k] = []

                for v in vs:
                    if isinstance(v, sqlglot.expression_core.ExpressionCore):
                        stack.append((v, v.__class__()))
                        copy.append(k, stack[-1][-1])
                    else:
                        copy.append(k, v)
            else:
                copy.args[k] = vs

    return root


def _patched_traverse(
    self: sqlglot.optimizer.scope.Scope,
) -> List[sqlglot.optimizer.scope.Scope]:
    """Patched traverse with circular scope dependency detection.

    PATCH: Detects circular dependencies in scope traversal to prevent infinite loops.
    This is a full replacement of sqlglot v29's Scope.traverse.
    Modification: Added seen_scopes tracking and circular dependency check.
    """
    from sqlglot.errors import OptimizeError

    stack = [self]
    seen_scopes = set()  ##! PATCH
    result = []

    while stack:
        scope = stack.pop()

        # Scopes aren't hashable, so we use id(scope) instead.
        if id(scope) in seen_scopes:  ##! PATCH
            raise OptimizeError(
                f"Scope {scope} has a circular scope dependency"
            )  ##! PATCH
        seen_scopes.add(id(scope))  ##! PATCH

        result.append(scope)
        stack.extend(
            itertools.chain(
                scope.union_scopes or (),
                scope.subquery_scopes,
                scope.derived_table_scopes,
                scope.cte_scopes,
            )
        )

    return result


def _patched_decorrelate(  # noqa: C901
    select: Any,
    parent_select: Any,
    external_columns: Any,
    next_alias_name: Any,
) -> None:
    """Patched decorrelate with null pointer fixes (v29 signature).

    PATCH: Full replacement of sqlglot v29's decorrelate function.
    Modifications: Added null checks before _replace() calls at lines ~281 and ~287
    to prevent AttributeError when parent_predicate is None.

    95% of this function is unchanged from sqlglot v29.0.1.
    """
    from sqlglot import exp

    where = select.args.get("where")

    if not where or where.find(exp.Or) or select.find(exp.Limit, exp.Offset):
        return

    table_alias = next_alias_name()
    keys = []

    # for all external columns in the where statement, find the relevant predicate
    # keys to convert it into a join
    for column in external_columns:
        if column.find_ancestor(exp.Where) is not where:
            return

        predicate = column.find_ancestor(exp.Predicate)

        if not predicate or predicate.find_ancestor(exp.Where) is not where:
            return

        if isinstance(predicate, exp.Binary):
            key = (
                predicate.right
                if any(node is column for node in predicate.left.walk())
                else predicate.left
            )
        else:
            return

        keys.append((key, column, predicate))

    if not any(isinstance(predicate, exp.EQ) for *_, predicate in keys):
        return

    is_subquery_projection = any(
        node is select.parent
        for node in map(lambda s: s.unalias(), parent_select.selects)
        if isinstance(node, exp.Subquery)
    )

    value = select.selects[0]
    key_aliases = {}
    group_by = []

    for key, _, predicate in keys:
        # if we filter on the value of the subquery, it needs to be unique
        if key == value.this:
            key_aliases[key] = value.alias
            group_by.append(key)
        else:
            if key not in key_aliases:
                key_aliases[key] = next_alias_name()
            # all predicates that are equalities must also be in the unique
            # so that we don't do a many to many join
            if isinstance(predicate, exp.EQ) and key not in group_by:
                group_by.append(key)

    parent_predicate = select.find_ancestor(exp.Predicate)

    # if the value of the subquery is not an agg or a key, we need to collect it into an array
    # so that it can be grouped. For subquery projections, we use a MAX aggregation instead.
    agg_func = exp.Max if is_subquery_projection else exp.ArrayAgg
    if not value.find(exp.AggFunc) and value.this not in group_by:
        select.select(
            exp.alias_(agg_func(this=value.this), value.alias, quoted=False),
            append=False,
            copy=False,
        )

    # exists queries should not have any selects as it only checks if there are any rows
    # all selects will be added by the optimizer and only used for join keys
    if isinstance(parent_predicate, exp.Exists):
        select.set("expressions", [])

    for key, alias in key_aliases.items():
        if key in group_by:
            # add all keys to the projections of the subquery
            # so that we can use it as a join key
            if isinstance(parent_predicate, exp.Exists) or key != value.this:
                select.select(f"{key} AS {alias}", copy=False)
        else:
            select.select(
                exp.alias_(agg_func(this=key.copy()), alias, quoted=False), copy=False
            )

    alias = exp.column(value.alias, table_alias)
    other = _other_operand(parent_predicate)
    op_type = type(parent_predicate.parent) if parent_predicate else None

    if isinstance(parent_predicate, exp.Exists):
        alias = exp.column(list(key_aliases.values())[0], table_alias)
        parent_predicate = _replace(parent_predicate, f"NOT {alias} IS NULL")
    elif isinstance(parent_predicate, exp.All):
        assert issubclass(op_type, exp.Binary)  # type: ignore[arg-type]
        predicate = op_type(this=other, expression=exp.column("_x"))  # type: ignore[misc]
        parent_predicate = _replace(
            parent_predicate.parent, f"ARRAY_ALL({alias}, _x -> {predicate})"
        )
    elif isinstance(parent_predicate, exp.Any):
        assert issubclass(op_type, exp.Binary)  # type: ignore[arg-type]
        if value.this in group_by:
            predicate = op_type(this=other, expression=alias)  # type: ignore[misc]
            parent_predicate = _replace(parent_predicate.parent, predicate)
        else:
            predicate = op_type(this=other, expression=exp.column("_x"))  # type: ignore[misc]
            parent_predicate = _replace(
                parent_predicate, f"ARRAY_ANY({alias}, _x -> {predicate})"
            )
    elif isinstance(parent_predicate, exp.In):
        if value.this in group_by:
            parent_predicate = _replace(parent_predicate, f"{other} = {alias}")
        else:
            parent_predicate = _replace(
                parent_predicate,
                f"ARRAY_ANY({alias}, _x -> _x = {parent_predicate.this})",
            )
    else:
        if is_subquery_projection and select.parent.alias:
            alias = exp.alias_(alias, select.parent.alias)

        # COUNT always returns 0 on empty datasets, so we need take that into consideration here
        # by transforming all counts into 0 and using that as the coalesced value
        if value.find(exp.Count):

            def remove_aggs(node: Any) -> Any:
                if isinstance(node, exp.Count):
                    return exp.Literal.number(0)
                elif isinstance(node, exp.AggFunc):
                    return exp.null()
                return node

            alias = exp.Coalesce(
                this=alias, expressions=[value.this.transform(remove_aggs)]
            )

        select.parent.replace(alias)

    for key, column, predicate in keys:
        predicate.replace(exp.true())
        nested = exp.column(key_aliases[key], table_alias)

        if is_subquery_projection:
            key.replace(nested)
            if not isinstance(predicate, exp.EQ):
                parent_select.where(predicate, copy=False)
            continue

        if key in group_by:
            key.replace(nested)
        elif isinstance(predicate, exp.EQ):
            if parent_predicate:  ##! PATCH
                parent_predicate = _replace(
                    parent_predicate,
                    f"({parent_predicate} AND ARRAY_CONTAINS({nested}, {column}))",
                )
        else:
            key.replace(exp.to_identifier("_x"))
            if parent_predicate:  ##! PATCH
                parent_predicate = _replace(
                    parent_predicate,
                    f"({parent_predicate} AND ARRAY_ANY({nested}, _x -> {predicate}))",
                )

    parent_select.join(
        select.group_by(*group_by, copy=False),
        on=[predicate for *_, predicate in keys if isinstance(predicate, exp.EQ)],
        join_type="LEFT",
        join_alias=table_alias,
        copy=False,
    )


def _other_operand(expression: Any) -> Any:
    """Helper function for decorrelate (from sqlglot v29)."""
    from sqlglot import exp

    if isinstance(expression, exp.In):
        return expression.this

    if isinstance(expression, (exp.Any, exp.All)):
        return _other_operand(expression.parent)

    if isinstance(expression, exp.Binary):
        return (
            expression.right
            if isinstance(expression.left, (exp.Subquery, exp.Any, exp.Exists, exp.All))
            else expression.left
        )

    return None


def _replace(expression: Any, condition: Any) -> Any:
    """Helper function for decorrelate (from sqlglot v29)."""
    from sqlglot import exp

    return expression.replace(exp.condition(condition))


def _extract_subfield(column: sqlglot.exp.Column) -> str:
    """Extract subfield path from a Column expression by walking up Dot parents.

    SUBFIELD EXTRACTION FOR STRUCT FIELD ACCESS
    ============================================

    When a query accesses nested struct fields (e.g., widget.asset.id in BigQuery),
    sqlglot's lineage only tracks the base column (widget), losing the nested path.

    This function walks up the parent chain of Dot expressions to reconstruct
    the full subfield path.

    Example: given widget.asset.id where the column is 'id':
    - column.parent is Dot for 'asset.id'
    - column.parent.parent is Dot for 'widget.asset.id'
    - Result: 'asset.id'

    This subfield is stored in Node.subfield and later used by DataHub to track
    fine-grained column lineage for struct fields.
    """
    subfields = []
    field: sqlglot.exp.Expression = column
    while isinstance(field.parent, sqlglot.exp.Dot):
        field = field.parent
        subfields.append(field.name)
    return ".".join(subfields)


def _patched_to_node(  # noqa: C901
    column: str | int,
    scope: sqlglot.optimizer.scope.Scope,
    dialect: sqlglot.Dialect,
    scope_name: str | None = None,
    upstream: sqlglot.lineage.Node | None = None,
    source_name: str | None = None,
    reference_node_name: str | None = None,
    trim_selects: bool = True,
) -> sqlglot.lineage.Node:
    """Patched to_node with subfield extraction for struct field access.

    PATCH: Full replacement of sqlglot v29's lineage.to_node function.
    Modifications:
      1. Changed source_columns from set to list (line ~260) for consistent ordering
      2. Added subfield extraction when creating leaf nodes (line ~400)

    95% of this function is unchanged from sqlglot v29.0.1.
    """
    import typing as t

    import sqlglot.expressions as exp
    from sqlglot.optimizer import find_all_in_scope
    from sqlglot.optimizer.scope import ScopeType

    # Find the specific select clause that is the source of the column we want.
    # This can either be a specific, named select or a generic `*` clause.
    select = (
        scope.expression.selects[column]
        if isinstance(column, int)
        else next(
            (
                select
                for select in scope.expression.selects
                if select.alias_or_name == column
            ),
            exp.Star() if scope.expression.is_star else scope.expression,
        )
    )

    if isinstance(scope.expression, exp.Subquery):
        for source in scope.subquery_scopes:
            return _patched_to_node(
                column,
                scope=source,
                dialect=dialect,
                upstream=upstream,
                source_name=source_name,
                reference_node_name=reference_node_name,
                trim_selects=trim_selects,
            )
    if isinstance(scope.expression, exp.SetOperation):
        name = type(scope.expression).__name__.upper()
        upstream = upstream or sqlglot.lineage.Node(
            name=name, source=scope.expression, expression=select
        )

        index = (
            column
            if isinstance(column, int)
            else next(
                (
                    i
                    for i, select in enumerate(scope.expression.selects)
                    if select.alias_or_name == column or select.is_star
                ),
                -1,  # mypy will not allow a None here, but a negative index should never be returned
            )
        )

        if index == -1:
            raise ValueError(f"Could not find {column} in {scope.expression}")

        for s in scope.union_scopes:
            _patched_to_node(
                index,
                scope=s,
                dialect=dialect,
                upstream=upstream,
                source_name=source_name,
                reference_node_name=reference_node_name,
                trim_selects=trim_selects,
            )

        return upstream

    if trim_selects and isinstance(scope.expression, exp.Select):
        # For better ergonomics in our node labels, replace the full select with
        # a version that has only the column we care about.
        #   "x", SELECT x, y FROM foo
        #     => "x", SELECT x FROM foo
        source = t.cast(exp.Expression, scope.expression.select(select, append=False))
    else:
        source = scope.expression

    # Create the node for this step in the lineage chain, and attach it to the previous one.
    node = sqlglot.lineage.Node(
        name=f"{scope_name}.{column}" if scope_name else str(column),
        source=source,
        expression=select,
        source_name=source_name or "",
        reference_node_name=reference_node_name or "",
    )

    if upstream:
        upstream.downstream.append(node)

    subquery_scopes = {
        id(subquery_scope.expression): subquery_scope
        for subquery_scope in scope.subquery_scopes
    }

    for subquery in find_all_in_scope(select, exp.UNWRAPPED_QUERIES):
        subquery_scope = subquery_scopes.get(id(subquery))
        if not subquery_scope:
            logger.warning(f"Unknown subquery scope: {subquery.sql(dialect=dialect)}")
            continue

        for name in subquery.named_selects:
            _patched_to_node(
                name,
                scope=subquery_scope,
                dialect=dialect,
                upstream=node,
                trim_selects=trim_selects,
            )

    # if the select is a star add all scope sources as downstreams
    if isinstance(select, exp.Star):
        for source in scope.sources.values():
            if isinstance(source, sqlglot.optimizer.scope.Scope):
                source = source.expression
            node.downstream.append(
                sqlglot.lineage.Node(
                    name=select.sql(comments=False), source=source, expression=source
                )
            )

    # Find all columns that went into creating this one to list their lineage nodes.
    source_columns = list(find_all_in_scope(select, exp.Column))  ##! PATCH (was: set)

    # If the source is a UDTF find columns used in the UDTF to generate the table
    source_expr = scope.expression
    if isinstance(source_expr, exp.UDTF):
        source_columns += list(source_expr.find_all(exp.Column))
        derived_tables = [
            source.expression.parent
            for source in scope.sources.values()
            if isinstance(source, sqlglot.optimizer.scope.Scope)
            and source.is_derived_table
        ]
    else:
        derived_tables = scope.derived_tables

    source_names = {
        dt.alias: dt.comments[0].split()[1]
        for dt in derived_tables
        if dt.comments and dt.comments[0].startswith("source: ")
    }

    pivots = scope.pivots
    pivot = pivots[0] if len(pivots) == 1 and not pivots[0].unpivot else None
    if pivot:
        # For each aggregation function, the pivot creates a new column for each field in category
        # combined with the aggfunc. So the columns parsed have this order: cat_a_value_sum, cat_a,
        # b_value_sum, b. Because of this step wise manner the aggfunc 'sum(value) as value_sum'
        # belongs to the column indices 0, 2, and the aggfunc 'max(price)' without an alias belongs
        # to the column indices 1, 3. Here, only the columns used in the aggregations are of interest
        # in the lineage, so lookup the pivot column name by index and map that with the columns used
        # in the aggregation.
        #
        # Example: PIVOT (SUM(value) AS value_sum, MAX(price)) FOR category IN ('a' AS cat_a, 'b')
        pivot_columns = pivot.args["columns"]
        pivot_aggs_count = len(pivot.expressions)

        pivot_column_mapping = {}
        for i, agg in enumerate(pivot.expressions):
            agg_cols = list(agg.find_all(exp.Column))
            for col_index in range(i, len(pivot_columns), pivot_aggs_count):
                pivot_column_mapping[pivot_columns[col_index].name] = agg_cols

    for c in source_columns:
        table = c.table
        source = scope.sources.get(table)

        if isinstance(source, sqlglot.optimizer.scope.Scope):
            reference_node_name_inner = None
            if (
                source.scope_type == ScopeType.DERIVED_TABLE
                and table not in source_names
            ):
                reference_node_name_inner = table
            elif source.scope_type == ScopeType.CTE:
                selected_node, _ = scope.selected_sources.get(table, (None, None))
                reference_node_name_inner = (
                    selected_node.name if selected_node else None
                )

            # The table itself came from a more specific scope. Recurse into that one using the unaliased column name.
            _patched_to_node(
                c.name,
                scope=source,
                dialect=dialect,
                scope_name=table,
                upstream=node,
                source_name=source_names.get(table) or source_name,
                reference_node_name=reference_node_name_inner,
                trim_selects=trim_selects,
            )
        elif pivot and pivot.alias_or_name == c.table:
            downstream_columns = []

            column_name = c.name
            if any(column_name == pivot_column.name for pivot_column in pivot_columns):
                downstream_columns.extend(pivot_column_mapping[column_name])
            else:
                # The column is not in the pivot, so it must be an implicit column of the
                # pivoted source -- adapt column to be from the implicit pivoted source.
                downstream_columns.append(
                    exp.column(c.this, table=pivot.parent.alias_or_name)
                )

            for downstream_column in downstream_columns:
                table = downstream_column.table
                source = scope.sources.get(table)
                if isinstance(source, sqlglot.optimizer.scope.Scope):
                    _patched_to_node(
                        downstream_column.name,
                        scope=source,
                        scope_name=table,
                        dialect=dialect,
                        upstream=node,
                        source_name=source_names.get(table) or source_name,
                        reference_node_name=reference_node_name,
                        trim_selects=trim_selects,
                    )
                else:
                    source = source or exp.Placeholder()
                    node.downstream.append(
                        sqlglot.lineage.Node(
                            name=downstream_column.sql(comments=False),
                            source=source,
                            expression=source,
                        )
                    )
        else:
            # The source is not a scope and the column is not in any pivot - we've reached the end
            # of the line. At this point, if a source is not found it means this column's lineage
            # is unknown. This can happen if the definition of a source used in a query is not
            # passed into the `sources` map.
            source = source or exp.Placeholder()

            subfield = _extract_subfield(c)  ##! PATCH

            node.downstream.append(
                sqlglot.lineage.Node(  # type: ignore[call-arg]
                    name=c.sql(comments=False),
                    source=source,
                    expression=source,
                    subfield=subfield,  ##! PATCH
                )
            )

    return node


def _patched_lineage(
    column: str | sqlglot.exp.Column,
    sql: str | sqlglot.exp.Expression,
    schema: Any = None,
    sources: Any = None,
    dialect: Any = None,
    scope: sqlglot.optimizer.scope.Scope | None = None,
    trim_selects: bool = True,
    copy: bool = True,
    **kwargs: Any,
) -> sqlglot.lineage.Node:
    """Patched lineage function that disables column normalization.

    This is the v29 implementation with column normalization disabled.
    """
    from sqlglot import maybe_parse
    from sqlglot.errors import SqlglotError
    from sqlglot.optimizer import build_scope, qualify

    expression = maybe_parse(sql, copy=copy, dialect=dialect)

    # Original: column = normalize_identifiers.normalize_identifiers(column, dialect=dialect).name  ##! PATCH
    assert isinstance(column, str)  ##! PATCH

    if sources:
        expression = sqlglot.exp.expand(
            expression,
            {
                k: sqlglot.exp.Query(maybe_parse(v, copy=copy, dialect=dialect))  # type: ignore
                for k, v in sources.items()
            },
            dialect=dialect,
            copy=copy,
        )

    if not scope:
        expression = qualify.qualify(
            expression,
            dialect=dialect,
            schema=schema,
            **{"validate_qualify_columns": False, "identify": False, **kwargs},  # type: ignore
        )

        scope = build_scope(expression)

    if not scope:
        raise SqlglotError("Cannot build lineage, sql must be SELECT")

    if not any(select.alias_or_name == column for select in scope.expression.selects):
        raise SqlglotError(f"Cannot find column '{column}' in query.")

    return _patched_to_node(column, scope, dialect, trim_selects=trim_selects)


def _patch_lineage() -> None:
    """Add the 'subfield' attribute to sqlglot.lineage.Node and patch lineage functions.

    Note: The sqlglot v29 lineage API has changed significantly.
    This patch adds the subfield attribute via inheritance and patches lineage/to_node to populate it.
    """

    # Add the "subfield" attribute to sqlglot.lineage.Node using dataclass inheritance
    @dataclasses.dataclass(frozen=True)
    class Node(sqlglot.lineage.Node):
        subfield: str = ""

    sqlglot.lineage.Node = Node  # type: ignore

    # Patch main lineage function to disable column normalization
    sqlglot.lineage.lineage = _patched_lineage  # type: ignore

    # Patch to_node to extract and populate subfield information
    sqlglot.lineage.to_node = _patched_to_node  # type: ignore


def apply_patches() -> None:
    """Apply all runtime patches to sqlglot."""
    logger.debug("Applying runtime patches to sqlglot for [c] tokenizer compatibility")

    # Patch 1: Add cooperative timeout to deepcopy
    sqlglot.expression_core.ExpressionCore.__deepcopy__ = _patched_deepcopy  # type: ignore

    # Patch 2: Add circular scope dependency detection
    sqlglot.optimizer.scope.Scope.traverse = _patched_traverse  # type: ignore

    # Patch 3: Fix null pointer issues in decorrelate
    sqlglot.optimizer.unnest_subqueries.decorrelate = _patched_decorrelate  # type: ignore

    # Patch 4: Add subfield support to lineage
    _patch_lineage()

    logger.debug("Runtime patches applied successfully")


# Apply patches on module import
apply_patches()

SQLGLOT_PATCHED = True
