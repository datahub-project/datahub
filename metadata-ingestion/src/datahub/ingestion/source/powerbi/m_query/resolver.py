"""
M-Query resolver: walks a powerquery-parser NodeIdMap to find DataAccessFunctionDetail
entries (recognized data-source function calls with their navigation chain).
"""

import logging
from typing import Dict, FrozenSet, List, Optional, Set, Tuple

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
        current_let=root_let,
        current_let_id=root_let_id,
        accessor_chain=None,
        results=results,
        seen=seen,
        parameters=parameters,
    )
    return results


def resolve_to_table_references(node_map: NodeIdMap) -> List[str]:
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
        current_let: dict = root_let
        current_let_id = root_let_id
    else:
        # No let scope. Only a plain identifier expression (e.g. `DimDate`) is a
        # bare sibling reference. Anything with a function call
        # (RecursivePrimaryExpression / InvokeExpression) is a data-source or
        # transformation expression — e.g. an unsupported source like
        # `LOAD_DATA(Source)` — not table-to-table lineage, so skip it.
        kinds = {node.get("kind") for node in node_map.values()}
        if kinds & {"RecursivePrimaryExpression", "InvokeExpression"}:
            return []
        root_id = min(node_map.keys())
        root_node = node_map[root_id]
        current_let = {}
        current_let_id = root_id

    if root_node is None:
        return []

    unresolved: Set[str] = set()
    _walk(
        node_map=node_map,
        node=root_node,
        current_let=current_let,
        current_let_id=current_let_id,
        accessor_chain=None,
        results=[],
        seen=set(),
        parameters={},
        unresolved=unresolved,
    )
    return sorted(unresolved)


def _walk(
    node_map: NodeIdMap,
    node: Optional[dict],
    current_let: dict,
    current_let_id: int,
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
        name = identifier.get("literal", "")
        # Strip quoted identifier prefix/suffix (#"name" → name)
        if name.startswith('#"') and name.endswith('"'):
            name = name[2:-1]
        _walk_identifier_name(
            node_map,
            name,
            current_let,
            current_let_id,
            accessor_chain,
            results,
            seen,
            parameters,
            unresolved,
        )
        return

    # -- Identifier --
    if kind == "Identifier":
        name = node.get("literal", "")
        if name.startswith('#"') and name.endswith('"'):
            name = name[2:-1]
        _walk_identifier_name(
            node_map,
            name,
            current_let,
            current_let_id,
            accessor_chain,
            results,
            seen,
            parameters,
            unresolved,
        )
        return

    # -- LetExpression (nested let scope) --
    if kind == "LetExpression":
        inner_let_id = node.get("id", -1)
        inner_output = node.get("expression")  # embedded node
        _walk(
            node_map,
            inner_output,
            node,
            inner_let_id,
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
            current_let,
            current_let_id,
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
                    current_let,
                    current_let_id,
                    accessor_chain,
                    results,
                    seen.copy(),
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
                current_let,
                current_let_id,
                accessor_chain,
                results,
                seen,
                parameters,
                unresolved,
            )
        return

    # -- IfExpression (conditional data source selection, e.g. dev/prod switching) --
    if kind == "IfExpression":
        _walk(
            node_map,
            node.get("trueExpression"),
            current_let,
            current_let_id,
            accessor_chain,
            results,
            seen,
            parameters,
            unresolved,
        )
        _walk(
            node_map,
            node.get("falseExpression"),
            current_let,
            current_let_id,
            accessor_chain,
            results,
            seen.copy(),
            parameters,
            unresolved,
        )
        return

    logger.debug("Unhandled node kind '%s', returning empty for this branch", kind)


def _walk_recursive_primary(
    node_map: NodeIdMap,
    node: dict,
    current_let: dict,
    current_let_id: int,
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
            current_let,
            current_let_id,
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
            current_let,
            current_let_id,
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
            current_let,
            current_let_id,
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
        current_let,
        current_let_id,
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
    current_let: dict,
    current_let_id: int,
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

    # Unrecognized wrapper (Table.RenameColumns, Table.NestedJoin, etc.).
    if callee:
        content = invoke_node.get("content", {})
        if isinstance(content, dict) and content.get("kind") == "ArrayWrapper":
            for elem in content.get("elements", []):
                inner = _unwrap_csv(elem)
                _walk(
                    node_map,
                    inner,
                    current_let,
                    current_let_id,
                    accessor_chain,
                    results,
                    seen,
                    parameters,
                    unresolved,
                )
                # Data-access resolution follows only the first argument (the
                # pipeline chain). Table-reference collection walks every
                # argument, since joins (Table.NestedJoin / Table.Join) name
                # sibling tables in later arguments too.
                if unresolved is None:
                    return


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
    current_let: dict,
    current_let_id: int,
    accessor_chain: Optional[IdentifierAccessor],
    results: List[DataAccessFunctionDetail],
    seen: Set[Tuple[int, str]],
    parameters: Optional[Dict[str, str]] = None,
    unresolved: Optional[Set[str]] = None,
) -> None:
    """Resolve a variable name in the current let scope and continue walking."""
    if not name:
        return
    # Circular reference guard: (let_id, variable_name) pair
    guard_key = (current_let_id, name)
    if guard_key in seen:
        logger.warning("Circular reference detected for variable '%s', stopping", name)
        return
    seen.add(guard_key)

    resolved = resolve_identifier(node_map, current_let, name)
    if resolved is None:
        # Not a local `let` variable. When collecting table references, this is
        # a candidate reference to a sibling table in the same dataset.
        if unresolved is not None:
            unresolved.add(name)
        return
    _walk(
        node_map,
        resolved,
        current_let,
        current_let_id,
        accessor_chain,
        results,
        seen,
        parameters,
        unresolved,
    )
