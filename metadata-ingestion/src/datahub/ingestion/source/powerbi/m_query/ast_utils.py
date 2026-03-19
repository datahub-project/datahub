"""
Utilities for navigating a powerquery-parser NodeIdMap.

A NodeIdMap is dict[int, dict] — the deserialized output of the TypeScript
bridge. Each node dict has at minimum:
    "kind": str   — NodeKind enum value (e.g. "LetExpression")
    "id": int     — unique node identifier

Child nodes are embedded directly as nested dicts, not as integer ID references.
The nodeIdMap provides a flat index to find any node by ID.
"""

from typing import Dict, Optional

NodeIdMap = dict[int, dict]


def find_nodes_by_kind(node_map: NodeIdMap, kind: str) -> list[dict]:
    """Return all nodes of the given NodeKind at any depth in the map."""
    return [node for node in node_map.values() if node.get("kind") == kind]


def get_literal_value(node: dict) -> Optional[str]:
    """
    Return the string content of a Text LiteralExpression with quotes stripped.
    Returns None for non-Text literals or non-LiteralExpression nodes.
    """
    if node.get("kind") != "LiteralExpression":
        return None
    if node.get("literalKind") != "Text":
        return None
    literal = node.get("literal", "")
    if not (literal.startswith('"') and literal.endswith('"')):
        return None
    return literal[1:-1]


def get_invoke_callee_name(node_map: NodeIdMap, invoke_node: dict) -> Optional[str]:
    """
    Resolve the callee name of an InvokeExpression.

    In the powerquery-parser AST, a call like `Snowflake.Databases(...)` is
    represented as a RecursivePrimaryExpression where:
      - head: IdentifierExpression → identifier.literal = "Snowflake.Databases"
      - recursiveExpressions: ArrayWrapper → elements = [InvokeExpression]

    Returns None if the node is not an InvokeExpression or the callee cannot be resolved.
    """
    if invoke_node.get("kind") != "InvokeExpression":
        return None

    invoke_id = invoke_node.get("id")
    if invoke_id is None:
        return None

    # Find the parent RecursivePrimaryExpression that contains this InvokeExpression
    for node in node_map.values():
        if node.get("kind") != "RecursivePrimaryExpression":
            continue
        rec_exprs = node.get("recursiveExpressions", {})
        if not isinstance(rec_exprs, dict) or rec_exprs.get("kind") != "ArrayWrapper":
            continue
        for elem in rec_exprs.get("elements", []):
            if isinstance(elem, dict) and elem.get("id") == invoke_id:
                head = node.get("head", {})
                if head.get("kind") == "IdentifierExpression":
                    identifier = head.get("identifier", {})
                    return identifier.get("literal")

    return None


def resolve_identifier(
    node_map: NodeIdMap,
    let_node: dict,
    name: str,
) -> Optional[dict]:
    """
    Look up a variable name in the given LetExpression's variable list.
    Returns the value node assigned to `name`, or None if not found.

    Structure: LetExpression.variableList (ArrayWrapper) → elements (Csv[]) → node (IdentifierPairedExpression)
    """
    if let_node.get("kind") != "LetExpression":
        return None

    var_list = let_node.get("variableList", {})
    if not isinstance(var_list, dict) or var_list.get("kind") != "ArrayWrapper":
        return None

    for elem in var_list.get("elements", []):
        # elements are Csv nodes wrapping IdentifierPairedExpression
        if isinstance(elem, dict) and elem.get("kind") == "Csv":
            inner = elem.get("node", {})
        else:
            inner = elem

        if not isinstance(inner, dict):
            continue
        if inner.get("kind") not in (
            "IdentifierPairedExpression",
            "GeneralizedIdentifierPairedExpression",
        ):
            continue

        key_node = inner.get("key", {})
        if not isinstance(key_node, dict):
            continue
        key_literal = key_node.get("literal", "")
        # Normalize: strip #"..." quoting and compare case-insensitively
        # (M-Query variable names are case-insensitive)
        key_bare = key_literal
        if key_bare.startswith('#"') and key_bare.endswith('"'):
            key_bare = key_bare[2:-1]
        name_bare = name
        if name_bare.startswith('#"') and name_bare.endswith('"'):
            name_bare = name_bare[2:-1]
        if key_bare.lower() == name_bare.lower():
            return inner.get("value")

    return None


def get_record_field_values(
    node_map: NodeIdMap,
    record_node: dict,
    parameters: Optional[Dict[str, str]] = None,
) -> dict[str, str]:
    """
    Extract key-value pairs from a RecordExpression where values are Text literals.
    Keys: GeneralizedIdentifier literals.
    Values: Text LiteralExpression values (quotes stripped).
    Non-string values are omitted unless resolvable via parameters.

    Structure: RecordExpression.content (ArrayWrapper) → elements (Csv[]) → node (GeneralizedIdentifierPairedExpression)
    """
    parameters = parameters or {}
    result: dict[str, str] = {}
    if record_node.get("kind") != "RecordExpression":
        return result

    content = record_node.get("content", {})
    if not isinstance(content, dict) or content.get("kind") != "ArrayWrapper":
        return result

    for elem in content.get("elements", []):
        if isinstance(elem, dict) and elem.get("kind") == "Csv":
            inner = elem.get("node", {})
        else:
            inner = elem

        if not isinstance(inner, dict):
            continue
        if inner.get("kind") not in (
            "GeneralizedIdentifierPairedExpression",
            "IdentifierPairedExpression",
        ):
            continue

        key_node = inner.get("key", {})
        value_node = inner.get("value", {})
        if not isinstance(key_node, dict) or not isinstance(value_node, dict):
            continue

        key = key_node.get("literal", "")
        value = get_literal_value(value_node)
        if (
            value is None
            and parameters
            and value_node.get("kind") == "IdentifierExpression"
        ):
            # Resolve identifier references using parameters
            ident = value_node.get("identifier", {})
            ref_name = ident.get("literal", "")
            if ref_name.startswith('#"') and ref_name.endswith('"'):
                ref_name = ref_name[2:-1]
            if ref_name in parameters:
                value = parameters[ref_name]
        if value is not None:
            result[key] = value

    return result
