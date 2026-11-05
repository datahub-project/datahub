"""
M-Query resolver: walks a powerquery-parser NodeIdMap to find DataAccessFunctionDetail
entries (recognized data-source function calls with their navigation chain).
"""

import logging
from typing import Dict, FrozenSet, List, Optional, Set, Tuple

from datahub.ingestion.source.powerbi.m_query.ast_utils import (
    NodeIdMap,
    get_literal_value,
    get_record_field_values,
    resolve_identifier,
)
from datahub.ingestion.source.powerbi.m_query.data_classes import (
    ColumnRename,
    ColumnRenameStep,
    DataAccessFunctionDetail,
    FunctionName,
    IdentifierAccessor,
)

logger = logging.getLogger(__name__)

_RECOGNIZED_FUNCTIONS: FrozenSet[str] = frozenset(f.value for f in FunctionName)
_TABLE_RENAME_COLUMNS = "Table.RenameColumns"

# The let scopes enclosing the node being walked, outermost first, each as
# (let id, let node). A nested let is pushed onto the end rather than replacing
# what is already there, so its body can still reach the steps its parents bind.
Scopes = Tuple[Tuple[int, dict], ...]


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
        scopes=((root_let_id, root_let),),
        accessor_chain=None,
        results=results,
        seen=seen,
        parameters=parameters,
    )
    return results


def _walk(
    node_map: NodeIdMap,
    node: Optional[dict],
    scopes: Scopes,
    accessor_chain: Optional[IdentifierAccessor],
    results: List[DataAccessFunctionDetail],
    seen: Set[Tuple[int, str]],
    parameters: Optional[Dict[str, str]] = None,
    column_rename_steps: Optional[List[ColumnRenameStep]] = None,
) -> None:
    if node is None:
        return
    column_rename_steps = column_rename_steps or []

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
            scopes,
            accessor_chain,
            results,
            seen,
            parameters,
            column_rename_steps,
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
            scopes,
            accessor_chain,
            results,
            seen,
            parameters,
            column_rename_steps,
        )
        return

    # -- LetExpression (nested let scope) --
    if kind == "LetExpression":
        inner_let_id = node.get("id", -1)
        inner_output = node.get("expression")  # embedded node
        _walk(
            node_map,
            inner_output,
            scopes + ((inner_let_id, node),),
            accessor_chain,
            results,
            seen,
            parameters,
            column_rename_steps,
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
            column_rename_steps,
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
                    seen.copy(),
                    parameters,
                    column_rename_steps,
                )
        return

    # -- FunctionExpression (each / anonymous function body) --
    if kind == "FunctionExpression":
        body = node.get("expression")
        if body is not None:
            _walk(
                node_map,
                body,
                scopes,
                accessor_chain,
                results,
                seen,
                parameters,
                column_rename_steps,
            )
        return

    # -- ParenthesizedExpression --
    # Parentheses around an expression carry no semantics, so the navigation
    # chain continues through them, e.g. `then (Source{[Name=..]}[Data])`.
    if kind == "ParenthesizedExpression":
        _walk(
            node_map,
            node.get("content"),
            scopes,
            accessor_chain,
            results,
            seen,
            parameters,
            column_rename_steps,
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
            column_rename_steps,
        )
        _walk(
            node_map,
            node.get("falseExpression"),
            scopes,
            accessor_chain,
            results,
            seen.copy(),
            parameters,
            column_rename_steps,
        )
        return

    logger.debug("Unhandled node kind '%s', returning empty for this branch", kind)


def _walk_recursive_primary(
    node_map: NodeIdMap,
    node: dict,
    scopes: Scopes,
    accessor_chain: Optional[IdentifierAccessor],
    results: List[DataAccessFunctionDetail],
    seen: Set[Tuple[int, str]],
    parameters: Optional[Dict[str, str]] = None,
    column_rename_steps: Optional[List[ColumnRenameStep]] = None,
) -> None:
    column_rename_steps = column_rename_steps or []
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
            column_rename_steps,
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
            column_rename_steps,
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
            column_rename_steps,
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
        column_rename_steps,
    )


def _walk_invoke(
    node_map: NodeIdMap,
    head: Optional[dict],
    invoke_node: dict,
    scopes: Scopes,
    accessor_chain: Optional[IdentifierAccessor],
    results: List[DataAccessFunctionDetail],
    seen: Set[Tuple[int, str]],
    parameters: Optional[Dict[str, str]] = None,
    column_rename_steps: Optional[List[ColumnRenameStep]] = None,
) -> None:
    column_rename_steps = column_rename_steps or []
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
                column_rename_steps=column_rename_steps,
            )
        )
        return

    # Unrecognized wrapper (Table.AddColumn, Table.SelectRows, etc.).
    # Table.RenameColumns is recorded, then we still follow its table argument.
    next_steps = column_rename_steps
    if callee == _TABLE_RENAME_COLUMNS:
        step = _rename_step(invoke_node)
        if step:
            next_steps = [*column_rename_steps, step]

    if callee:
        content = invoke_node.get("content", {})
        if isinstance(content, dict) and content.get("kind") == "ArrayWrapper":
            for elem in content.get("elements", []):
                inner = _unwrap_csv(elem)
                _walk(
                    node_map,
                    inner,
                    scopes,
                    accessor_chain,
                    results,
                    seen,
                    parameters,
                    next_steps,
                )
                return  # only first arg


def _unwrap_csv(elem: object) -> Optional[dict]:
    """Unwrap a Csv wrapper node, returning the inner node."""
    if isinstance(elem, dict) and elem.get("kind") == "Csv":
        return elem.get("node")
    if isinstance(elem, dict):
        return elem
    return None


def _list_items(node: object) -> List[dict]:
    """Elements of a ListExpression, or [] for any other node."""
    list_node = _unwrap_csv(node)
    if not isinstance(list_node, dict) or list_node.get("kind") != "ListExpression":
        return []
    content = list_node.get("content", {})
    if not isinstance(content, dict):
        return []
    items = (_unwrap_csv(elem) for elem in content.get("elements", []))
    return [item for item in items if isinstance(item, dict)]


def _text_values(nodes: List[dict]) -> List[str]:
    values = (get_literal_value(node) for node in nodes)
    return [value for value in values if value is not None]


def _rename_step(invoke_node: dict) -> Optional[ColumnRenameStep]:
    """Pairs from Table.RenameColumns' second argument, or None if absent.

    The argument is either a list of {old, new} lists or a single {old, new}
    pair. A trailing MissingField argument is ignored. Pairs that are not text
    literals are skipped.
    """
    content = invoke_node.get("content", {})
    if not isinstance(content, dict) or content.get("kind") != "ArrayWrapper":
        return None
    elements = content.get("elements", [])
    if len(elements) < 2:
        return None

    items = _list_items(elements[1])
    single_pair = _text_values(items)
    pairs = (
        [single_pair]
        if len(items) == 2 and len(single_pair) == 2
        else [_text_values(_list_items(item)) for item in items]
    )
    renames_in_step = [
        ColumnRename(source_name=names[0], renamed_name=names[1])
        for names in pairs
        if len(names) >= 2
    ]
    if not renames_in_step:
        return None
    return ColumnRenameStep(renames=renames_in_step)


def _resolve_in_scopes(
    node_map: NodeIdMap, scopes: Scopes, name: str
) -> Optional[Tuple[int, dict, Scopes]]:
    """Look a name up in the innermost scope that binds it.

    Returns the binding scope's id, the value assigned to the name, and the
    scopes that value is itself evaluated in. That last part matters: a step
    bound by an outer let cannot see names introduced by a nested one, so the
    chain is truncated at the scope where the name was found.

    Known gap, covered by an xfail test: a plain reference in M is *exclusive*,
    so a binding is not in scope for its own value and `a = a` reads an
    enclosing `a`. Preferring the innermost binding unconditionally resolves it
    to itself instead, which the circular-reference guard then stops.
    """
    for index in range(len(scopes) - 1, -1, -1):
        let_id, let_node = scopes[index]
        resolved = resolve_identifier(node_map, let_node, name)
        if resolved is not None:
            return let_id, resolved, scopes[: index + 1]
    return None


def _walk_identifier_name(
    node_map: NodeIdMap,
    name: str,
    scopes: Scopes,
    accessor_chain: Optional[IdentifierAccessor],
    results: List[DataAccessFunctionDetail],
    seen: Set[Tuple[int, str]],
    parameters: Optional[Dict[str, str]] = None,
    column_rename_steps: Optional[List[ColumnRenameStep]] = None,
) -> None:
    """Resolve a variable name against the enclosing scopes and keep walking."""
    if not name:
        return

    found = _resolve_in_scopes(node_map, scopes, name)
    if found is None:
        logger.debug("No enclosing let binds '%s', stopping this branch", name)
        return
    binding_let_id, resolved, binding_scopes = found

    # Circular reference guard: (binding let id, variable name) pair. Casefolded
    # to match how the name was resolved -- M treats `a` and `A` as one variable,
    # so a cycle spelled inconsistently has to count as the same visit.
    guard_key = (binding_let_id, name.casefold())
    if guard_key in seen:
        logger.warning("Circular reference detected for variable '%s', stopping", name)
        return
    seen.add(guard_key)

    _walk(
        node_map,
        resolved,
        binding_scopes,
        accessor_chain,
        results,
        seen,
        parameters,
        column_rename_steps,
    )
