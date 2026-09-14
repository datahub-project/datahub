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
    CSN_VAL,
    CSN_XPR,
    PROJECTION_ALIAS,
)

# A projected column is *calculated* (as opposed to a plain projection/rename of
# a source column) when it carries one of these expression keys rather than a
# bare ``ref``. Mirrors the calculated-field handling in the Tableau connector,
# where the field's ``formula`` is surfaced on the column description.
_CALCULATION_KEYS = frozenset({CSN_XPR, CSN_FUNC, CSN_VAL})


def _render_ref(segments: List[object]) -> str:
    parts = [str(seg) for seg in segments if isinstance(seg, (str, int))]
    # A ``$projection.<col>`` ref points at a sibling output column; the alias is
    # an internal CDS detail, so show just the column for a readable formula.
    if len(parts) >= 2 and parts[0] == PROJECTION_ALIAS:
        parts = parts[1:]
    return ".".join(parts)


def _render_literal(value: object) -> str:
    if isinstance(value, str):
        return f"'{value}'"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    return str(value)


def render_cqn_expression(node: object) -> str:
    """Serialize a CQN expression node into a readable SQL-like formula string.

    Handles the node shapes SAP Datasphere emits in ``query.SELECT.columns`` and
    in a calculated element's inline ``value``: column refs, literals, function
    calls, infix expressions, and IN-lists. Unrecognized shapes render to an
    empty string so the caller can skip them rather than surface noise.
    """
    if isinstance(node, str):
        # A bare string inside an ``xpr`` list is an operator or SQL keyword
        # (``+``, ``>``, ``case``, ``then``, ``end``, ...).
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
        rendered_args = (
            ", ".join(render_cqn_expression(arg) for arg in args)
            if isinstance(args, list)
            else ""
        )
        return f"{func}({rendered_args})"
    xpr = node.get(CSN_XPR)
    if isinstance(xpr, list):
        return _render_xpr(xpr)
    items = node.get(CSN_LIST)
    if isinstance(items, list):
        return "(" + ", ".join(render_cqn_expression(item) for item in items) + ")"
    return ""


def _render_xpr(items: List[object]) -> str:
    rendered: List[str] = []
    for item in items:
        text = render_cqn_expression(item)
        if not text:
            continue
        # Parenthesize a nested infix expression so operator precedence stays
        # visually unambiguous once flattened into one line.
        if isinstance(item, dict) and isinstance(item.get(CSN_XPR), list):
            text = f"({text})"
        rendered.append(text)
    return " ".join(rendered)


def _is_calculated_column(col: Dict[str, object]) -> bool:
    return any(key in col for key in _CALCULATION_KEYS)


def _output_name(col: Dict[str, object]) -> Optional[str]:
    alias = col.get(CSN_AS)
    if isinstance(alias, str) and alias:
        return alias
    ref = col.get(CSN_REF)
    if isinstance(ref, list) and ref and isinstance(ref[-1], str):
        return ref[-1]
    return None


def _formulas_from_query_columns(csn_def: dict, out: Dict[str, str]) -> None:
    query = csn_def.get(CSN_KEY_QUERY)
    if not isinstance(query, dict):
        return
    select = query.get(CSN_SELECT)
    if not isinstance(select, dict):
        return
    columns = select.get(CSN_COLUMNS)
    if not isinstance(columns, list):
        return
    for col in columns:
        if not isinstance(col, dict) or not _is_calculated_column(col):
            continue
        name = _output_name(col)
        if name is None:
            continue
        formula = render_cqn_expression(col)
        if formula:
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
        if formula:
            out.setdefault(name, formula)


def extract_calculated_column_formulas(csn_def: dict) -> Dict[str, str]:
    """Map each calculated output column (by name) to its rendered formula.

    Reads the top-level ``query.SELECT.columns`` projection (graphical views and
    analytic models) and any calculated element carrying an inline ``value``
    expression. Plain column projections/renames are omitted — they carry no
    calculation to describe.
    """
    formulas: Dict[str, str] = {}
    _formulas_from_query_columns(csn_def, formulas)
    _formulas_from_elements(csn_def, formulas)
    return formulas


def make_description_with_formula(
    description: Optional[str], formula: Optional[str]
) -> Optional[str]:
    """Combine an existing column label with its calculation, mirroring the
    Tableau connector's ``make_description_from_params``: the label first, then a
    ``formula: <expr>`` line."""
    parts: List[str] = []
    if description:
        parts.append(description)
    if formula:
        parts.append(f"formula: {formula}")
    if not parts:
        return None
    return "\n\n".join(parts)
