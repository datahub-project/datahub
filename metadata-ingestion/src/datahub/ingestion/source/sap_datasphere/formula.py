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

# A projected column is *calculated* (as opposed to a plain projection/rename of
# a source column) when it carries one of these expression keys rather than a
# bare ``ref``. Mirrors the calculated-field handling in the Tableau connector,
# where the field's ``formula`` is surfaced on the column description.
_CALCULATION_KEYS = frozenset({CSN_XPR, CSN_FUNC, CSN_VAL})

# SQL literal for a CQN null ({"val": null}). A column whose whole formula is
# just this is a placeholder measure with no real calculation, so it is skipped
# rather than surfaced as a noisy ``formula: NULL``.
_SQL_NULL = "NULL"


def _render_ref(segments: List[object]) -> str:
    parts = [str(seg) for seg in segments if isinstance(seg, (str, int))]
    # A ``$projection.<col>`` ref points at a sibling output column; the alias is
    # an internal CDS detail, so show just the column for a readable formula.
    if len(parts) >= 2 and parts[0] == PROJECTION_ALIAS:
        parts = parts[1:]
    return ".".join(parts)


def _render_literal(value: object) -> str:
    if value is None:
        return _SQL_NULL
    if isinstance(value, str):
        # Double any embedded single quote so an apostrophe (``O'Reilly``) yields
        # valid SQL-like quoting rather than a broken ``'O'Reilly'``.
        return "'" + value.replace("'", "''") + "'"
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
    # Reject the whole expression if any operand is unrenderable: a partial render
    # such as ``COALESCE(A, )`` is misleading metadata, worse than none.
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
            # A dangling operator (``A +``) is worse than omitting the formula.
            return ""
        # Parenthesize a nested infix expression so operator precedence stays
        # visually unambiguous once flattened into one line.
        if isinstance(item, dict) and isinstance(item.get(CSN_XPR), list):
            text = f"({text})"
        rendered.append(text)
    return " ".join(rendered)


def _is_calculated_column(col: Dict[str, object]) -> bool:
    return any(key in col for key in _CALCULATION_KEYS)


def _is_meaningful_formula(formula: str) -> bool:
    # Skip an empty render (unrecognized shape) and a bare NULL placeholder — a
    # column that is only ``NULL`` carries no calculation worth describing.
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
    # A view body is either a single ``SELECT`` or a ``SET`` (UNION/INTERSECT/
    # EXCEPT) whose ``args`` are themselves query bodies. Flatten both so a
    # calculated column in any branch is reached, mirroring lineage extraction.
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


def _formulas_from_query_columns(csn_def: dict, out: Dict[str, str]) -> None:
    for select in _iter_selects(csn_def.get(CSN_KEY_QUERY)):
        columns = select.get(CSN_COLUMNS)
        if not isinstance(columns, list):
            continue
        for col in columns:
            if not isinstance(col, dict) or not _is_calculated_column(col):
                continue
            name = _output_name(col)
            if name is None:
                continue
            formula = render_cqn_expression(col)
            if _is_meaningful_formula(formula):
                # First branch wins; UNION branches align by output column name.
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
