"""
M-Query resolver: walks a powerquery-parser NodeIdMap to find DataAccessFunctionDetail
entries (recognized data-source function calls with their navigation chain).
"""

import logging
from typing import Dict, FrozenSet, List, NamedTuple, Optional, Set, Tuple

from datahub.ingestion.source.powerbi.m_query.ast_utils import (
    NodeIdMap,
    get_record_field_values,
    resolve_identifier,
)
from datahub.ingestion.source.powerbi.m_query.data_classes import (
    DataAccessFunctionDetail,
    FunctionName,
    IdentifierAccessor,
)

logger = logging.getLogger(__name__)

_RECOGNIZED_FUNCTIONS: FrozenSet[str] = frozenset(f.value for f in FunctionName)


def resolve_to_data_access_functions(
    node_map: NodeIdMap,
    parameters: Optional[Dict[str, str]] = None,
) -> List[DataAccessFunctionDetail]:
    """
    Entry point: walk the NodeIdMap and return all DataAccessFunctionDetail entries
    for recognized data-access function calls in the expression.
    """
    parameters = parameters or {}
    let_nodes = [
        (k, v) for k, v in node_map.items() if v.get("kind") == "LetExpression"
    ]
    if not let_nodes:
        logger.debug("No LetExpression found in node map")
        return []

    # Use the outermost let (smallest id = parsed first / outermost scope)
    root_let_id, root_let = min(let_nodes, key=lambda kv: kv[0])

    # LetExpression.expression is embedded -- not an ID
    output_node = root_let.get("expression")
    if output_node is None:
        logger.debug(
            "LetExpression (id=%d) has no output expression — cannot resolve lineage",
            root_let_id,
        )
        return []

    results: List[DataAccessFunctionDetail] = []
    seen: Set[Tuple[int, str]] = set()

    _walk(
        node_map=node_map,
        node=output_node,
        scopes=(_Scope(scope_id=root_let_id, let_node=root_let),),
        accessor_chain=None,
        results=results,
        seen=seen,
        parameters=parameters,
    )
    return results


def resolve_to_table_references(
    node_map: NodeIdMap,
    parameters: Optional[Dict[str, str]] = None,
    parent_by_id: Optional[Dict[int, int]] = None,
) -> List[str]:
    """
    Find identifier names in the expression that do not resolve to a local `let`
    variable — i.e. references to another table in the same PowerBI dataset.

    Covers bare identifiers (``DimDate``), quoted identifiers
    (``#"tbl_PayrollHistory"``), and identifiers inside wrapper functions
    (``Table.Combine({tblA, tblB})``). The returned names are candidates: the
    caller validates them against the dataset's actual table names before
    emitting lineage, which is what guards against false positives.
    """
    if not node_map:
        return []

    let_nodes = [
        (k, v) for k, v in node_map.items() if v.get("kind") == "LetExpression"
    ]
    if let_nodes:
        # Use the outermost let (smallest id = parsed first / outermost scope)
        root_let_id, root_let = min(let_nodes, key=lambda kv: kv[0])
        root_node = root_let.get("expression")
        root_scopes: Tuple[_Scope, ...] = (
            _Scope(scope_id=root_let_id, let_node=root_let),
        )
    else:
        # No let scope. Whether a call's arguments may be sibling tables is
        # decided per callee while walking (see _walk_invoke), not by scanning the
        # whole expression — an unknown call in one argument must not suppress
        # references in the others.
        # The root is the one node with no parent. Falling back to the lowest id
        # is only an approximation — for `TblA & TblB` it picks the left operand
        # and the right one is never walked.
        root_id = _root_node_id(node_map, parent_by_id)
        root_node = node_map[root_id]
        root_scopes = (_Scope(scope_id=root_id),)

    if root_node is None:
        return []

    unresolved: Set[str] = set()
    _walk(
        node_map=node_map,
        node=root_node,
        scopes=root_scopes,
        accessor_chain=None,
        results=[],
        seen=set(),
        parameters={},
        unresolved=unresolved,
    )
    # Names bound by the expression are resolved through the scope chain during
    # the walk, so only external query parameters need excluding here.
    excluded = {name.casefold() for name in (parameters or {})}
    return sorted(name for name in unresolved if name.casefold() not in excluded)


class _Scope(NamedTuple):
    """One lexical scope: either a `let` (with its variables) or a function's parameters."""

    scope_id: int
    let_node: Optional[dict] = None
    param_names: FrozenSet[str] = frozenset()


def _function_param_names(node: dict) -> FrozenSet[str]:
    """Parameter names bound by a FunctionExpression, e.g. `(Country) => ...`."""
    params = node.get("parameters", {})
    content = params.get("content", {}) if isinstance(params, dict) else {}
    names = set()
    for elem in content.get("elements", []) if isinstance(content, dict) else []:
        inner = _unwrap_csv(elem)
        if not isinstance(inner, dict) or inner.get("kind") != "Parameter":
            continue
        name_node = inner.get("name", {})
        if isinstance(name_node, dict):
            literal = _strip_quoted_identifier(name_node.get("literal", ""))
            if literal:
                names.add(literal.casefold())
    return frozenset(names)


def _root_node_id(node_map: NodeIdMap, parent_by_id: Optional[Dict[int, int]]) -> int:
    """The node that has no parent, or the lowest id when parents are unavailable."""
    if parent_by_id:
        roots = [node_id for node_id in node_map if node_id not in parent_by_id]
        if len(roots) == 1:
            return roots[0]
    return min(node_map.keys())


def _strip_quoted_identifier(literal: str) -> str:
    """Turn a quoted identifier (``#"name"``) into its bare name."""
    if literal.startswith('#"') and literal.endswith('"'):
        return literal[2:-1]
    return literal


def _walk(
    node_map: NodeIdMap,
    node: Optional[dict],
    scopes: Tuple[_Scope, ...],
    accessor_chain: Optional[IdentifierAccessor],
    results: List[DataAccessFunctionDetail],
    seen: Set[Tuple[int, str]],
    parameters: Optional[Dict[str, str]] = None,
    unresolved: Optional[Set[str]] = None,
) -> None:
    if node is None:
        return

    kind = node.get("kind", "")

    # -- IdentifierExpression (wraps Identifier) --
    if kind == "IdentifierExpression":
        identifier = node.get("identifier", {})
        raw_name = identifier.get("literal", "")
        name = _strip_quoted_identifier(raw_name)
        _walk_identifier_name(
            node_map,
            name,
            scopes,
            accessor_chain,
            results,
            seen,
            parameters,
            unresolved,
            was_quoted=raw_name != name,
        )
        return

    # -- Identifier --
    if kind == "Identifier":
        raw_name = node.get("literal", "")
        name = _strip_quoted_identifier(raw_name)
        _walk_identifier_name(
            node_map,
            name,
            scopes,
            accessor_chain,
            results,
            seen,
            parameters,
            unresolved,
            was_quoted=raw_name != name,
        )
        return

    # -- LetExpression (nested let scope) --
    if kind == "LetExpression":
        inner_let_id = node.get("id", -1)
        inner_output = node.get("expression")  # embedded node
        _walk(
            node_map,
            inner_output,
            (_Scope(scope_id=inner_let_id, let_node=node),) + scopes,
            accessor_chain,
            results,
            seen,
            parameters,
            unresolved,
        )
        return

    # -- RecursivePrimaryExpression --
    # Covers both function calls (head + InvokeExpression) and
    # accessor chains (head + ItemAccessExpression + FieldSelector)
    if kind == "RecursivePrimaryExpression":
        _walk_recursive_primary(
            node_map,
            node,
            scopes,
            accessor_chain,
            results,
            seen,
            parameters,
            unresolved,
        )
        return

    # -- ListExpression (Table.Combine sources) --
    if kind == "ListExpression":
        content = node.get("content", {})
        if isinstance(content, dict) and content.get("kind") == "ArrayWrapper":
            for elem in content.get("elements", []):
                inner = _unwrap_csv(elem)
                # Use a copy of seen for each list element so sibling paths
                # sharing common ancestors don't trigger false circular refs
                _walk(
                    node_map,
                    inner,
                    scopes,
                    accessor_chain,
                    results,
                    _fork_seen(seen, unresolved),
                    parameters,
                    unresolved,
                )
        return

    # -- FunctionExpression (each / anonymous function body) --
    if kind == "FunctionExpression":
        body = node.get("expression")
        if body is not None:
            _walk(
                node_map,
                body,
                (
                    _Scope(
                        scope_id=node.get("id", -1),
                        param_names=_function_param_names(node),
                    ),
                )
                + scopes,
                accessor_chain,
                results,
                seen,
                parameters,
                unresolved,
            )
        return

    # -- ParenthesizedExpression — transparent, unwrap and continue --
    if kind == "ParenthesizedExpression":
        _walk(
            node_map,
            node.get("content"),
            scopes,
            accessor_chain,
            results,
            seen,
            parameters,
            unresolved,
        )
        return

    # -- EachExpression (`each <body>`) — the body is an ordinary expression --
    if kind == "EachExpression":
        _walk(
            node_map,
            node.get("paired"),
            scopes,
            accessor_chain,
            results,
            seen,
            parameters,
            unresolved,
        )
        return

    # -- Binary expressions (e.g. `TblA & TblB`, `a ?? b`) — walk both operands --
    if kind in ("ArithmeticExpression", "NullCoalescingExpression"):
        for side in ("left", "right"):
            _walk(
                node_map,
                node.get(side),
                scopes,
                accessor_chain,
                results,
                _fork_seen(seen, unresolved),
                parameters,
                unresolved,
            )
        return

    # -- IfExpression (conditional data source selection, e.g. dev/prod switching) --
    if kind == "IfExpression":
        _walk(
            node_map,
            node.get("trueExpression"),
            scopes,
            accessor_chain,
            results,
            seen,
            parameters,
            unresolved,
        )
        _walk(
            node_map,
            node.get("falseExpression"),
            scopes,
            accessor_chain,
            results,
            _fork_seen(seen, unresolved),
            parameters,
            unresolved,
        )
        return

    logger.debug("Unhandled node kind '%s', returning empty for this branch", kind)


def _walk_recursive_primary(
    node_map: NodeIdMap,
    node: dict,
    scopes: Tuple[_Scope, ...],
    accessor_chain: Optional[IdentifierAccessor],
    results: List[DataAccessFunctionDetail],
    seen: Set[Tuple[int, str]],
    parameters: Optional[Dict[str, str]] = None,
    unresolved: Optional[Set[str]] = None,
) -> None:
    head = node.get("head")  # embedded IdentifierExpression
    rec_exprs = node.get("recursiveExpressions", {})
    elements = rec_exprs.get("elements", []) if isinstance(rec_exprs, dict) else []

    if not elements:
        _walk(
            node_map,
            head,
            scopes,
            accessor_chain,
            results,
            seen,
            parameters,
            unresolved,
        )
        return

    first = elements[0]

    # Function call: Snowflake.Databases(...), Table.RenameColumns(...), etc.
    if first.get("kind") == "InvokeExpression":
        _walk_invoke(
            node_map,
            head,
            first,
            scopes,
            accessor_chain,
            results,
            seen,
            parameters,
            unresolved,
        )
        return

    # Accessor chain step: Source{[Name="mydb", Kind="Database"]}[Data]
    if first.get("kind") == "ItemAccessExpression":
        content = first.get("content", {})  # RecordExpression
        kv: Dict[str, str] = {}
        if isinstance(content, dict):
            kv = get_record_field_values(node_map, content, parameters=parameters)

        new_accessor = IdentifierAccessor(
            identifier=kv.get("Name", ""),
            items=kv,
            next=accessor_chain,
        )
        _walk(
            node_map,
            head,
            scopes,
            new_accessor,
            results,
            seen,
            parameters,
            unresolved,
        )
        return

    # FieldSelector or other -- just walk the head
    _walk(
        node_map,
        head,
        scopes,
        accessor_chain,
        results,
        seen,
        parameters,
        unresolved,
    )


def _walk_invoke(
    node_map: NodeIdMap,
    head: Optional[dict],
    invoke_node: dict,
    scopes: Tuple[_Scope, ...],
    accessor_chain: Optional[IdentifierAccessor],
    results: List[DataAccessFunctionDetail],
    seen: Set[Tuple[int, str]],
    parameters: Optional[Dict[str, str]] = None,
    unresolved: Optional[Set[str]] = None,
) -> None:
    callee = None
    if isinstance(head, dict) and head.get("kind") == "IdentifierExpression":
        callee = head.get("identifier", {}).get("literal")

    if callee and callee in _RECOGNIZED_FUNCTIONS:
        results.append(
            DataAccessFunctionDetail(
                arg_list=invoke_node,
                data_access_function_name=callee,
                identifier_accessor=accessor_chain,
                node_map=node_map,
                parameters=parameters or {},
            )
        )
        return

    # Unrecognized wrapper (Table.RenameColumns, Table.NestedJoin, ...) — descend
    # into its arguments to reach the source underneath.
    #
    # When collecting *table references* only, skip bare callees: M library
    # functions are always namespaced, so a bare callee is an unknown or
    # unsupported source whose arguments are parameters rather than sibling
    # tables. Data-access resolution must still descend, or a pipeline that wraps
    # an already-bound warehouse source in such a call loses its lineage.
    if callee and (unresolved is None or "." in callee):
        content = invoke_node.get("content", {})
        if isinstance(content, dict) and content.get("kind") == "ArrayWrapper":
            for elem in content.get("elements", []):
                inner = _unwrap_csv(elem)
                # Use a fresh copy of `seen` per argument so sibling arguments
                # that share a common ancestor don't trigger false circular-ref
                # warnings (same rationale as the ListExpression branch).
                _walk(
                    node_map,
                    inner,
                    scopes,
                    accessor_chain,
                    results,
                    _fork_seen(seen, unresolved),
                    parameters,
                    unresolved,
                )
                # Data-access resolution follows only the first argument (the
                # pipeline chain). Table-reference collection walks every
                # argument, since joins (Table.NestedJoin / Table.Join) name
                # sibling tables in later arguments too.
                if unresolved is None:
                    return


def _fork_seen(
    seen: Set[Tuple[int, str]], unresolved: Optional[Set[str]]
) -> Set[Tuple[int, str]]:
    """Per-branch visited set for the data-access walk; shared for collection.

    The data-access walk builds a path-dependent accessor chain, so each branch
    needs its own visited set. Table-reference collection has no path-dependent
    state, so it shares one set — memoizing subtrees that are reachable from
    several arguments. Copying there instead made a merge chain (each step
    joining two earlier steps) re-walk shared subtrees exponentially.
    """
    return seen if unresolved is not None else seen.copy()


def _unwrap_csv(elem: object) -> Optional[dict]:
    """Unwrap a Csv wrapper node, returning the inner node."""
    if isinstance(elem, dict) and elem.get("kind") == "Csv":
        return elem.get("node")
    if isinstance(elem, dict):
        return elem
    return None


def _walk_identifier_name(
    node_map: NodeIdMap,
    name: str,
    scopes: Tuple[_Scope, ...],
    accessor_chain: Optional[IdentifierAccessor],
    results: List[DataAccessFunctionDetail],
    seen: Set[Tuple[int, str]],
    parameters: Optional[Dict[str, str]] = None,
    unresolved: Optional[Set[str]] = None,
    was_quoted: bool = False,
) -> None:
    """Resolve a name against the scope chain (innermost outward) and keep walking."""
    if not name:
        return

    # Walk outward: M scoping is lexical, so a name not bound by the innermost
    # `let` may still be bound by an enclosing one. Resolving per scope chain is
    # what lets a nested `let` shadow a name without suppressing a genuine
    # sibling reference of the same name in an outer scope.
    for depth, scope in enumerate(scopes):
        if name.casefold() in scope.param_names:
            # Bound as a function parameter: a value, never a sibling table.
            return

        if scope.let_node is None:
            continue

        # Circular reference guard, per (scope, name). During table-reference
        # collection the set is shared across branches (see _fork_seen), so a
        # repeat visit means "already resolved" rather than a cycle — skip quietly.
        guard_key = (scope.scope_id, name)
        if guard_key in seen:
            if unresolved is None:
                logger.warning(
                    "Circular reference detected for variable '%s', stopping", name
                )
            return

        resolved = resolve_identifier(node_map, scope.let_node, name)
        if resolved is None:
            continue
        seen.add(guard_key)
        # The bound value belongs to the scope that binds it, so continue from
        # there outward rather than from the (deeper) reference site.
        _walk(
            node_map,
            resolved,
            scopes[depth:],
            accessor_chain,
            results,
            seen,
            parameters,
            unresolved,
        )
        return

    # Bound by nothing in the chain. When collecting table references this is a
    # candidate sibling table — but skip *unquoted* dotted identifiers, which are
    # M library/enum references (QuoteStyle.Csv, JoinKind.LeftOuter) and never
    # table names. A quoted #"My.Table" is a real, dotted table name.
    if unresolved is not None and (was_quoted or "." not in name):
        unresolved.add(name)
    return
