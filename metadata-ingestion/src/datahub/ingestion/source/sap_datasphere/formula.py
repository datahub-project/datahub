from typing import Dict, List, Optional

from datahub.ingestion.source.sap_datasphere.constants import (
    CSN_ARGS,
    CSN_AS,
    CSN_COLUMNS,
    CSN_FUNC,
    CSN_KEY_ELEMENTS,
    CSN_KEY_QUERY,
    CSN_KEY_VALUE,
    CSN_LIST,
    CSN_REF,
    CSN_SELECT,
    CSN_SET,
    CSN_VAL,
    CSN_XPR,
    PROJECTION_ALIAS,
)

# A projected column is calculated (vs. a plain projection/rename) when it carries
# one of these expression keys rather than a bare ``ref``.
_CALCULATION_KEYS = frozenset({CSN_XPR, CSN_FUNC, CSN_VAL})

_SQL_NULL = "NULL"


def _render_ref(segments: List[object]) -> str:
    parts = [str(seg) for seg in segments if isinstance(seg, (str, int))]
    # ``$projection.<col>`` points at a sibling output column; drop the internal alias.
    if len(parts) >= 2 and parts[0] == PROJECTION_ALIAS:
        parts = parts[1:]
    return ".".join(parts)


def _render_literal(value: object) -> str:
    if value is None:
        return _SQL_NULL
    if isinstance(value, str):
        # Double embedded single quotes to keep the SQL-like quoting valid.
        return "'" + value.replace("'", "''") + "'"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    return str(value)


def render_cqn_expression(node: object) -> str:
    """Serialize a CQN expression node into a readable SQL-like formula string.

    Unrecognized shapes render to an empty string so the caller can skip them.
    """
    if isinstance(node, str):
        # A bare string inside an ``xpr`` is an operator/keyword (``+``, ``case``, ...).
        return node
    if isinstance(node, bool):
        return _render_literal(node)
    if isinstance(node, (int, float)):
        return _render_literal(node)
    if not isinstance(node, dict):
        return ""
    ref = node.get(CSN_REF)
    if isinstance(ref, list):
        return _render_ref(ref)
    if CSN_VAL in node:
        return _render_literal(node[CSN_VAL])
    func = node.get(CSN_FUNC)
    if isinstance(func, str):
        args = node.get(CSN_ARGS)
        if not isinstance(args, list):
            return f"{func}()"
        rendered_args = _render_operands(args)
        if rendered_args is None:
            return ""
        return f"{func}({', '.join(rendered_args)})"
    xpr = node.get(CSN_XPR)
    if isinstance(xpr, list):
        return _render_xpr(xpr)
    items = node.get(CSN_LIST)
    if isinstance(items, list):
        rendered_items = _render_operands(items)
        if rendered_items is None:
            return ""
        return "(" + ", ".join(rendered_items) + ")"
    return ""


def _render_operands(items: List[object]) -> Optional[List[str]]:
    # None if any operand is unrenderable: a partial ``COALESCE(A, )`` misleads.
    rendered: List[str] = []
    for item in items:
        text = render_cqn_expression(item)
        if not text:
            return None
        rendered.append(text)
    return rendered


def _render_xpr(items: List[object]) -> str:
    rendered: List[str] = []
    for item in items:
        text = render_cqn_expression(item)
        if not text:
            # Reject rather than emit a dangling operator (``A +``).
            return ""
        # Parenthesize a nested infix expression to keep precedence unambiguous.
        if isinstance(item, dict) and isinstance(item.get(CSN_XPR), list):
            text = f"({text})"
        rendered.append(text)
    return " ".join(rendered)


def _is_calculated_column(col: Dict[str, object]) -> bool:
    return any(key in col for key in _CALCULATION_KEYS)


def _is_meaningful_formula(formula: str) -> bool:
    # Skip empty renders and bare-``NULL`` placeholder columns.
    return bool(formula) and formula != _SQL_NULL


def _output_name(col: Dict[str, object]) -> Optional[str]:
    alias = col.get(CSN_AS)
    if isinstance(alias, str) and alias:
        return alias
    ref = col.get(CSN_REF)
    if isinstance(ref, list) and ref and isinstance(ref[-1], str):
        return ref[-1]
    return None


def _iter_selects(query: object) -> List[dict]:
    # Flatten a ``SET`` (UNION/INTERSECT/EXCEPT) body so branch columns are reached.
    if not isinstance(query, dict):
        return []
    selects: List[dict] = []
    select = query.get(CSN_SELECT)
    if isinstance(select, dict):
        selects.append(select)
    set_node = query.get(CSN_SET)
    if isinstance(set_node, dict):
        args = set_node.get(CSN_ARGS)
        if isinstance(args, list):
            for arg in args:
                selects.extend(_iter_selects(arg))
    return selects


def _select_output_names(select: dict) -> List[Optional[str]]:
    columns = select.get(CSN_COLUMNS)
    if not isinstance(columns, list):
        return []
    return [_output_name(c) if isinstance(c, dict) else None for c in columns]


def _formulas_from_query_columns(csn_def: dict, out: Dict[str, str]) -> None:
    selects = _iter_selects(csn_def.get(CSN_KEY_QUERY))
    if not selects:
        return
    # A UNION's output columns take their names from the first branch, by position;
    # later branches may rename or omit aliases, so map every branch by index.
    output_names = _select_output_names(selects[0])
    for select in selects:
        columns = select.get(CSN_COLUMNS)
        if not isinstance(columns, list):
            continue
        for index, col in enumerate(columns):
            if not isinstance(col, dict) or not _is_calculated_column(col):
                continue
            name = output_names[index] if index < len(output_names) else None
            if name is None:
                name = _output_name(col)
            if name is None:
                continue
            formula = render_cqn_expression(col)
            if _is_meaningful_formula(formula):
                out.setdefault(name, formula)


def _formulas_from_elements(csn_def: dict, out: Dict[str, str]) -> None:
    elements = csn_def.get(CSN_KEY_ELEMENTS)
    if not isinstance(elements, dict):
        return
    for name, element in elements.items():
        if not isinstance(element, dict):
            continue
        value = element.get(CSN_KEY_VALUE)
        if not isinstance(value, dict):
            continue
        formula = render_cqn_expression(value)
        if _is_meaningful_formula(formula):
            out.setdefault(name, formula)


def extract_calculated_column_formulas(csn_def: dict) -> Dict[str, str]:
    """Map each calculated output column (by name) to its rendered formula.

    Reads the ``query`` projection columns and any calculated element carrying an
    inline ``value`` expression; plain projections/renames are omitted.
    """
    formulas: Dict[str, str] = {}
    _formulas_from_query_columns(csn_def, formulas)
    _formulas_from_elements(csn_def, formulas)
    return formulas


def make_description_with_formula(
    description: Optional[str], formula: Optional[str]
) -> Optional[str]:
    """Combine a column label with its calculation: label first, then a
    ``formula: <expr>`` line."""
    parts: List[str] = []
    if description:
        parts.append(description)
    if formula:
        parts.append(f"formula: {formula}")
    if not parts:
        return None
    return "\n\n".join(parts)
