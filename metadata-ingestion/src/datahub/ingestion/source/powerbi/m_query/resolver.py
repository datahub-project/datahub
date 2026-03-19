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
) -> List[DataAccessFunctionDetail]:
    """
    Entry point: walk the NodeIdMap and return all DataAccessFunctionDetail entries
    for recognized data-access function calls in the expression.
    """
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
) -> None:
    if node is None:
        return

    kind = node.get("kind", "")

    # -- IdentifierExpression (wraps Identifier) --
    if kind == "IdentifierExpression":
        identifier = node.get("identifier", {})
        name = identifier.get("literal", "")
        # Strip quoted identifier prefix/suffix (#"name" → name)
        if identifier.get("isQuoted") and name.startswith('#"') and name.endswith('"'):
            name = name[2:-1]
        _walk_identifier_name(
            node_map,
            name,
            current_let,
            current_let_id,
            accessor_chain,
            results,
            seen,
        )
        return

    # -- Identifier --
    if kind == "Identifier":
        name = node.get("literal", "")
        if node.get("isQuoted") and name.startswith('#"'):
            name = name[2:-1]
        _walk_identifier_name(
            node_map,
            name,
            current_let,
            current_let_id,
            accessor_chain,
            results,
            seen,
        )
        return

    # -- LetExpression (nested let scope) --
    if kind == "LetExpression":
        inner_let_id = node.get("id", -1)
        inner_output = node.get("expression")  # embedded node
        _walk(node_map, inner_output, node, inner_let_id, accessor_chain, results, seen)
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
        )
        return

    # -- ListExpression (Table.Combine sources) --
    if kind == "ListExpression":
        content = node.get("content", {})
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
        )
        return

    # Accessor chain step: Source{[Name="mydb", Kind="Database"]}[Data]
    if first.get("kind") == "ItemAccessExpression":
        content = first.get("content", {})  # RecordExpression
        kv: Dict[str, str] = {}
        if isinstance(content, dict):
            kv = get_record_field_values(node_map, content)

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
                )
                return  # only first arg


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
        node_map, resolved, current_let, current_let_id, accessor_chain, results, seen
    )
