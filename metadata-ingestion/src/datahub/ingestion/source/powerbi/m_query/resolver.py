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


def _walk(
    node_map: NodeIdMap,
    node: Optional[dict],
    current_let: dict,
    current_let_id: int,
    accessor_chain: Optional[IdentifierAccessor],
    results: List[DataAccessFunctionDetail],
    seen: Set[Tuple[int, str]],
    parameters: Optional[Dict[str, str]] = None,
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

    # Unrecognized wrapper (Table.RenameColumns, Table.AddColumn, etc.)
    # Recurse into first argument
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
                )
                return  # only first arg


def _unwrap_csv(elem: object) -> Optional[dict]:
    """Unwrap a Csv wrapper node, returning the inner node."""
    if isinstance(elem, dict) and elem.get("kind") == "Csv":
        return elem.get("node")
    if isinstance(elem, dict):
        return elem
    return None


def _resolve_identifier_for_table_refs(
    node_map: NodeIdMap,
    name: str,
    current_let: Optional[dict],
    current_let_id: Optional[int],
    sibling_names_lower: FrozenSet[str],
    sibling_name_map: Dict[str, str],
    results: List[str],
    seen: Set[Tuple[Optional[int], str]],
) -> None:
    """Resolve a single identifier name and check if it maps to a sibling table."""
    guard_key = (current_let_id, name.lower())
    if guard_key in seen:
        return
    seen.add(guard_key)

    resolved = resolve_identifier(node_map, current_let, name) if current_let else None
    if resolved is not None:
        _walk_for_table_refs(
            node_map,
            resolved,
            current_let,
            current_let_id,
            sibling_names_lower,
            sibling_name_map,
            results,
            seen,
        )
    elif name.lower() in sibling_names_lower:
        results.append(sibling_name_map[name.lower()])


def _walk_invoke_for_table_refs(
    node_map: NodeIdMap,
    invoke_node: dict,
    current_let: Optional[dict],
    current_let_id: Optional[int],
    sibling_names_lower: FrozenSet[str],
    sibling_name_map: Dict[str, str],
    results: List[str],
    seen: Set[Tuple[Optional[int], str]],
) -> None:
    """Walk arguments of an InvokeExpression for sibling table references."""
    content = invoke_node.get("content", {})
    if isinstance(content, dict) and content.get("kind") == "ArrayWrapper":
        for arg in content.get("elements", []):
            inner = _unwrap_csv(arg)
            _walk_for_table_refs(
                node_map,
                inner,
                current_let,
                current_let_id,
                sibling_names_lower,
                sibling_name_map,
                results,
                seen,
            )


def _walk_for_table_refs(
    node_map: NodeIdMap,
    node: Optional[dict],
    current_let: Optional[dict],
    current_let_id: Optional[int],
    sibling_names_lower: FrozenSet[str],
    sibling_name_map: Dict[str, str],
    results: List[str],
    seen: Set[Tuple[Optional[int], str]],
) -> None:
    """Walk AST collecting identifiers unresolved in let scope that match sibling names."""
    if isinstance(node, int):
        node = node_map.get(node)
    if node is None:
        return

    kind: str = node.get("kind", "")

    if kind == "LetExpression":
        _walk_for_table_refs(
            node_map,
            node.get("expression"),
            node,
            id(node),
            sibling_names_lower,
            sibling_name_map,
            results,
            seen,
        )
        return

    if kind == "IdentifierExpression":
        identifier = node.get("identifier", {})
        raw_name = identifier.get("literal", "") if isinstance(identifier, dict) else ""
        name = raw_name.strip()
        if name.startswith('#"') and name.endswith('"'):
            name = name[2:-1]
        _resolve_identifier_for_table_refs(
            node_map,
            name,
            current_let,
            current_let_id,
            sibling_names_lower,
            sibling_name_map,
            results,
            seen,
        )
        return

    if kind == "Identifier":
        raw_name = node.get("literal", "")
        name = raw_name.strip()
        if name.startswith('#"') and name.endswith('"'):
            name = name[2:-1]
        _resolve_identifier_for_table_refs(
            node_map,
            name,
            current_let,
            current_let_id,
            sibling_names_lower,
            sibling_name_map,
            results,
            seen,
        )
        return

    if kind == "ListExpression":
        content = node.get("content", {})
        if isinstance(content, dict) and content.get("kind") == "ArrayWrapper":
            for elem in content.get("elements", []):
                inner = _unwrap_csv(elem)
                _walk_for_table_refs(
                    node_map,
                    inner,
                    current_let,
                    current_let_id,
                    sibling_names_lower,
                    sibling_name_map,
                    results,
                    seen.copy(),
                )
        return

    if kind == "RecursivePrimaryExpression":
        _walk_for_table_refs(
            node_map,
            node.get("head"),
            current_let,
            current_let_id,
            sibling_names_lower,
            sibling_name_map,
            results,
            seen,
        )
        rec_exprs = node.get("recursiveExpressions", {})
        elements = rec_exprs.get("elements", []) if isinstance(rec_exprs, dict) else []
        for recursive_expr in elements:
            if isinstance(recursive_expr, int):
                recursive_expr = node_map.get(recursive_expr)
            if recursive_expr is None:
                continue
            if recursive_expr.get("kind") == "InvokeExpression":
                _walk_invoke_for_table_refs(
                    node_map,
                    recursive_expr,
                    current_let,
                    current_let_id,
                    sibling_names_lower,
                    sibling_name_map,
                    results,
                    seen,
                )
        return

    if kind == "FunctionExpression":
        _walk_for_table_refs(
            node_map,
            node.get("expression"),
            current_let,
            current_let_id,
            sibling_names_lower,
            sibling_name_map,
            results,
            seen,
        )
        return


def resolve_to_table_references(
    node_map: NodeIdMap,
    sibling_names: FrozenSet[str],
) -> List[str]:
    """Return original-cased sibling names that appear as unresolved identifiers in node_map."""
    if not sibling_names:
        return []

    sibling_names_lower: FrozenSet[str] = frozenset(n.lower() for n in sibling_names)
    sibling_name_map: Dict[str, str] = {n.lower(): n for n in sibling_names}

    let_node_id: Optional[int] = None
    let_node: Optional[dict] = None
    root_node_id: Optional[int] = None
    root_node: Optional[dict] = None

    for node_id, node in node_map.items():
        kind = node.get("kind")
        if kind == "LetExpression":
            if let_node_id is None or node_id < let_node_id:
                let_node_id = node_id
                let_node = node
        if root_node_id is None or node_id < root_node_id:
            root_node_id = node_id
            root_node = node

    results: List[str] = []

    if let_node is not None:
        _walk_for_table_refs(
            node_map,
            let_node.get("expression"),
            let_node,
            let_node_id,
            sibling_names_lower,
            sibling_name_map,
            results,
            set(),
        )
    elif root_node is not None:
        _walk_for_table_refs(
            node_map,
            root_node,
            None,
            None,
            sibling_names_lower,
            sibling_name_map,
            results,
            set(),
        )

    return results


def _walk_identifier_name(
    node_map: NodeIdMap,
    name: str,
    current_let: dict,
    current_let_id: int,
    accessor_chain: Optional[IdentifierAccessor],
    results: List[DataAccessFunctionDetail],
    seen: Set[Tuple[int, str]],
    parameters: Optional[Dict[str, str]] = None,
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
    _walk(
        node_map,
        resolved,
        current_let,
        current_let_id,
        accessor_chain,
        results,
        seen,
        parameters,
    )
