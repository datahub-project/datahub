"""
M-Query resolver: walks a powerquery-parser NodeIdMap to find DataAccessFunctionDetail
entries (recognized data-source function calls with their navigation chain).
"""

import logging
from typing import Dict, FrozenSet, Optional, Set, Tuple

from datahub.ingestion.source.powerbi.m_query.ast_utils import (
    NodeIdMap,
    get_record_field_values,
    resolve_identifier,
    resolve_parameter_value,
)
from datahub.ingestion.source.powerbi.m_query.data_classes import (
    DataAccessFunctionDetail,
    DataAccessResolution,
    FunctionName,
    IdentifierAccessor,
)

logger = logging.getLogger(__name__)

_RECOGNIZED_FUNCTIONS: FrozenSet[str] = frozenset(f.value for f in FunctionName)

# Where the walk ends on a plain value rather than on a shape it cannot model.
# An unrecognized source recurses into its first argument, so its URL or path
# literal would otherwise dominate the unhandled-kind tally.
_VALUE_LEAF_NODE_KINDS: FrozenSet[str] = frozenset({"LiteralExpression"})


def resolve_to_data_access_functions(
    node_map: NodeIdMap,
    parameters: Optional[Dict[str, str]] = None,
) -> DataAccessResolution:
    """
    Entry point: walk the NodeIdMap and return the recognized data-access function
    calls in the expression, along with any NodeKinds the walk could not follow.
    """
    parameters = parameters or {}
    resolution = DataAccessResolution(functions=[])

    let_nodes = [
        (k, v) for k, v in node_map.items() if v.get("kind") == "LetExpression"
    ]
    for _, let_node in let_nodes:
        resolution.let_bound_names.update(_let_variable_names(let_node))
    if not let_nodes:
        logger.debug("No LetExpression found in node map")
        return resolution

    # Use the outermost let (smallest id = parsed first / outermost scope)
    root_let_id, root_let = min(let_nodes, key=lambda kv: kv[0])

    # LetExpression.expression is embedded -- not an ID
    output_node = root_let.get("expression")
    if output_node is None:
        logger.debug(
            "LetExpression (id=%d) has no output expression — cannot resolve lineage",
            root_let_id,
        )
        return resolution

    seen: Set[Tuple[int, str]] = set()

    _walk(
        node_map=node_map,
        node=output_node,
        current_let=root_let,
        current_let_id=root_let_id,
        accessor_chain=None,
        resolution=resolution,
        seen=seen,
        parameters=parameters,
    )
    return resolution


def _walk(
    node_map: NodeIdMap,
    node: Optional[dict],
    current_let: dict,
    current_let_id: int,
    accessor_chain: Optional[IdentifierAccessor],
    resolution: DataAccessResolution,
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
            resolution,
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
            resolution,
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
            resolution,
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
            resolution,
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
                    resolution,
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
                resolution,
                seen,
                parameters,
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
            resolution,
            seen,
            parameters,
        )
        _walk(
            node_map,
            node.get("falseExpression"),
            current_let,
            current_let_id,
            accessor_chain,
            resolution,
            seen.copy(),
            parameters,
        )
        return

    if kind not in _VALUE_LEAF_NODE_KINDS:
        resolution.unhandled_node_kinds.add(kind)
    logger.debug("Unhandled node kind '%s', returning empty for this branch", kind)


def _walk_recursive_primary(
    node_map: NodeIdMap,
    node: dict,
    current_let: dict,
    current_let_id: int,
    accessor_chain: Optional[IdentifierAccessor],
    resolution: DataAccessResolution,
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
            resolution,
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
            resolution,
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
            resolution,
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
        resolution,
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
    resolution: DataAccessResolution,
    seen: Set[Tuple[int, str]],
    parameters: Optional[Dict[str, str]] = None,
) -> None:
    callee = None
    if isinstance(head, dict) and head.get("kind") == "IdentifierExpression":
        callee = head.get("identifier", {}).get("literal")

    if callee and callee in _RECOGNIZED_FUNCTIONS:
        resolution.functions.append(
            DataAccessFunctionDetail(
                arg_list=invoke_node,
                data_access_function_name=callee,
                identifier_accessor=accessor_chain,
                node_map=node_map,
                parameters=parameters or {},
            )
        )
        # The handler still needs a server and database out of these arguments,
        # so an argument naming something the expression never defines loses the
        # lineage even though the function itself was recognized. Only identifier
        # arguments are examined -- walking the whole argument list would file
        # option records as unmodelled shapes.
        _record_unresolved_arguments(
            node_map, invoke_node, current_let, resolution, parameters
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
                    resolution,
                    seen,
                    parameters,
                )
                return  # only first arg


def _record_unresolved_arguments(
    node_map: NodeIdMap,
    invoke_node: dict,
    current_let: dict,
    resolution: DataAccessResolution,
    parameters: Optional[Dict[str, str]],
) -> None:
    """Note identifier arguments of a recognized call that resolve to nothing."""
    content = invoke_node.get("content", {})
    if not isinstance(content, dict) or content.get("kind") != "ArrayWrapper":
        return

    for elem in content.get("elements", []):
        inner = _unwrap_csv(elem)
        if not isinstance(inner, dict) or inner.get("kind") != "IdentifierExpression":
            continue
        name = inner.get("identifier", {}).get("literal", "")
        if name.startswith('#"') and name.endswith('"'):
            name = name[2:-1]
        if not name:
            continue
        if resolve_identifier(node_map, current_let, name) is None:
            _record_unresolved(resolution, name, parameters)


def _let_variable_names(let_node: dict) -> Set[str]:
    """Names bound by one LetExpression, with any #"..." quoting stripped."""
    names: Set[str] = set()
    var_list = let_node.get("variableList", {})
    if not isinstance(var_list, dict):
        return names

    for elem in var_list.get("elements", []):
        inner = _unwrap_csv(elem)
        if not isinstance(inner, dict):
            continue
        key = inner.get("key", {})
        if not isinstance(key, dict):
            continue
        literal = key.get("literal", "")
        if literal.startswith('#"') and literal.endswith('"'):
            literal = literal[2:-1]
        if literal:
            names.add(literal)

    return names


def _record_unresolved(
    resolution: DataAccessResolution,
    name: str,
    parameters: Optional[Dict[str, str]],
) -> None:
    """Note a name the walk could not resolve to anything.

    Skips dataset parameters, which are values rather than tables, and names the
    expression binds in another let -- those are in scope for the model even when
    this walk cannot reach them, so reporting them would point at a query that
    does not exist.
    """
    if name in resolution.let_bound_names:
        return
    if resolve_parameter_value(parameters, name) is not None:
        return
    resolution.unresolved_identifiers.add(name)


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
    resolution: DataAccessResolution,
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
    if resolved is None:
        _record_unresolved(resolution, name, parameters)

    _walk(
        node_map,
        resolved,
        current_let,
        current_let_id,
        accessor_chain,
        resolution,
        seen,
        parameters,
    )
