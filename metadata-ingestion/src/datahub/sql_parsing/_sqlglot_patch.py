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
unchanged from sqlglot v29.0.1. Our modifications are marked with "# PATCH" comments.
This is a standard Python pattern for monkey-patching when source-level tools fail.
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
_original_decorrelate = sqlglot.optimizer.unnest_subqueries.decorrelate


def _patched_deepcopy(
    self: sqlglot.expression_core.ExpressionCore, memo: Any
) -> sqlglot.expression_core.ExpressionCore:
    """Patched __deepcopy__ with cooperative timeout support.

    PATCH: Adds cooperative timeout checks to prevent hangs on deeply nested expressions.
    This is a full replacement of sqlglot v29's ExpressionCore.__deepcopy__.
    Only modification: Added 3 lines at the start to check for timeouts.
    """
    # PATCH START: Add cooperative timeout check
    import datahub.utilities.cooperative_timeout

    datahub.utilities.cooperative_timeout.cooperate()
    # PATCH END

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
    # PATCH START: Track seen scopes to detect circular dependencies
    seen_scopes = set()
    # PATCH END
    result = []

    while stack:
        scope = stack.pop()

        # PATCH START: Check for circular dependencies
        # Scopes aren't hashable, so we use id(scope) instead.
        if id(scope) in seen_scopes:
            raise OptimizeError(f"Scope {scope} has a circular scope dependency")
        seen_scopes.add(id(scope))
        # PATCH END

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


def _patched_decorrelate(
    select: sqlglot.expressions.Select,
    parent_select: sqlglot.expressions.Select,
    external_columns: Any,
    next_alias_name: Any,
) -> None:
    """Patched decorrelate with null pointer fixes (v29 signature).

    PATCH: Wraps the original to catch AttributeErrors from null pointer issues.
    This is a wrapper (not full replacement) that adds error handling around
    sqlglot's decorrelate function. The signature changed in v29:
      v28: decorrelate(select, parent_select, parent_scope, outer_scope, where=False)
      v29: decorrelate(select, parent_select, external_columns, next_alias_name)
    """
    try:
        _original_decorrelate(select, parent_select, external_columns, next_alias_name)
    except AttributeError as e:
        # If we hit the null pointer issue, log and skip
        logger.debug(f"Skipping decorrelate due to null pointer: {e}")


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
    # PATCH: Changed from set to list for consistent ordering during subfield extraction
    source_columns = list(find_all_in_scope(select, exp.Column))

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

            # PATCH START: Extract subfield information for struct field access
            # Original code: node.downstream.append(Node(name=c.sql(), source=source, expression=source))
            # Modified to: extract subfield and pass it to Node constructor
            subfield = _extract_subfield(c)

            node.downstream.append(
                sqlglot.lineage.Node(  # type: ignore[call-arg]
                    name=c.sql(comments=False),
                    source=source,
                    expression=source,
                    subfield=subfield,  # Our patched Node has this field
                )
            )
            # PATCH END

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

    # PATCH: Disable column normalization - it was causing issues
    # Original: column = normalize_identifiers.normalize_identifiers(column, dialect=dialect).name
    assert isinstance(column, str)

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
