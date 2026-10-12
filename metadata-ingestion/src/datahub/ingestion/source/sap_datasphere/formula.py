from typing import Dict, List, Mapping, Optional

from datahub.ingestion.source.sap_datasphere.constants import (
    CSN_ARGS,
    CSN_AS,
    CSN_CASE,
    CSN_CAST,
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
_SQL_TRUE = "TRUE"
_SQL_FALSE = "FALSE"
# ``IN`` is an infix membership test rendered as ``<expr> IN <list>``, not a call.
_SQL_IN = "IN"

# xpr operands that are themselves compound expressions and must be parenthesized
# to keep infix precedence unambiguous.
_NESTED_EXPR_KEYS = (CSN_XPR, CSN_CASE, CSN_CAST)


def _needs_parens(node: object) -> bool:
    # A compound operand (xpr/case/cast token stream) is wrapped so infix
    # precedence stays unambiguous when it sits beside an operator.
    return isinstance(node, dict) and any(
        isinstance(node.get(key), list) for key in _NESTED_EXPR_KEYS
    )


def _render_ref(segments: List[object]) -> str:
    parts = [str(seg) for seg in segments if isinstance(seg, (str, int))]
    # ``$projection.<col>`` points at a sibling output column; drop the internal alias.
    if len(parts) >= 2 and parts[0] == PROJECTION_ALIAS:
        parts = parts[1:]
    return ".".join(parts)


def _render_literal(value: object) -> Optional[str]:
    if value is None:
        return _SQL_NULL
    # bool is an int subclass, so it must be tested before int/float.
    if isinstance(value, bool):
        return _SQL_TRUE if value else _SQL_FALSE
    if isinstance(value, str):
        # Double embedded single quotes to keep the SQL-like quoting valid.
        return "'" + value.replace("'", "''") + "'"
    if isinstance(value, (int, float)):
        return str(value)
    # A nested dict/list under ``val`` is malformed — don't fake a SQL literal.
    return None


def render_cqn_expression(node: object) -> Optional[str]:
    """Serialize a CQN expression node into a readable SQL-like formula string.

    Returns ``None`` for a node that can't be fully rendered, so a partial or
    misleading formula is never surfaced (distinct from a rendered value).
    """
    if isinstance(node, str):
        # A bare string inside an ``xpr`` is an operator/keyword (``+``, ``case``, ...).
        return node or None
    # bool is an int subclass; _render_literal formats it.
    if isinstance(node, (int, float)):
        return _render_literal(node)
    if not isinstance(node, dict):
        return None
    ref = node.get(CSN_REF)
    if isinstance(ref, list):
        return _render_ref(ref) or None
    if CSN_VAL in node:
        return _render_literal(node[CSN_VAL])
    func = node.get(CSN_FUNC)
    if isinstance(func, str) and func:
        args = node.get(CSN_ARGS)
        if not isinstance(args, list):
            return f"{func}()"
        rendered_args = _render_operands(args)
        if rendered_args is None:
            return None
        # ``IN`` is an infix membership test (``C IN (1, 2)``), not a call. In CDS
        # CSN it is normally an ``xpr`` operator, so this rarely fires — but a
        # ``func``-shaped IN must not render as ``IN(C, (1, 2))``.
        if func.upper() == _SQL_IN and len(rendered_args) == 2:
            infix_args = [
                f"({text})" if _needs_parens(arg) else text
                for arg, text in zip(args, rendered_args, strict=True)
            ]
            return f"{infix_args[0]} {_SQL_IN} {infix_args[1]}"
        return f"{func}({', '.join(rendered_args)})"
    # All three keys carry a flat token stream. ``case``/``cast`` can arrive as
    # first-class keys (the shape lineage.py walks), not only as bare tokens
    # inside an ``xpr``.
    for key in (CSN_XPR, CSN_CASE, CSN_CAST):
        tokens = node.get(key)
        if isinstance(tokens, list):
            return _render_xpr(tokens)
    items = node.get(CSN_LIST)
    if isinstance(items, list):
        # An empty list is degenerate — unrenderable, like an empty xpr.
        rendered_items = _render_operands(items) if items else None
        if rendered_items is None:
            return None
        return "(" + ", ".join(rendered_items) + ")"
    return None


def _render_operands(items: List[object]) -> Optional[List[str]]:
    # None if any operand is unrenderable: a partial ``COALESCE(A, )`` misleads.
    rendered: List[str] = []
    for item in items:
        text = render_cqn_expression(item)
        if text is None:
            return None
        rendered.append(text)
    return rendered


def _render_xpr(items: List[object]) -> Optional[str]:
    rendered: List[str] = []
    for item in items:
        text = render_cqn_expression(item)
        if text is None:
            # Reject rather than emit a dangling operator (``A +``).
            return None
        # Parenthesize a nested compound operand to keep precedence unambiguous.
        if _needs_parens(item):
            text = f"({text})"
        rendered.append(text)
    return " ".join(rendered) if rendered else None


def _is_calculated_column(col: Dict[str, object]) -> bool:
    if any(key in col for key in _CALCULATION_KEYS):
        return True
    # ``case``/``cast`` only count as a calculation when they carry a renderable
    # token stream (a list). A dict value is type-cast metadata beside a plain
    # ``ref`` (e.g. ``{"ref": [...], "cast": {"type": "cds.Decimal"}}``) — not a
    # calculation, just a typed projection.
    return isinstance(col.get(CSN_CASE), list) or isinstance(col.get(CSN_CAST), list)


def _output_name(col: Dict[str, object]) -> Optional[str]:
    alias = col.get(CSN_AS)
    if isinstance(alias, str) and alias:
        return alias
    ref = col.get(CSN_REF)
    if isinstance(ref, list) and ref and isinstance(ref[-1], str):
        return ref[-1]
    return None


def _iter_selects(query: object) -> List[Dict[str, object]]:
    # Flatten a ``SET`` (UNION/INTERSECT/EXCEPT) body so branch columns are reached.
    if not isinstance(query, dict):
        return []
    selects: List[Dict[str, object]] = []
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


def _select_output_names(select: Dict[str, object]) -> List[Optional[str]]:
    columns = select.get(CSN_COLUMNS)
    if not isinstance(columns, list):
        return []
    return [_output_name(c) if isinstance(c, dict) else None for c in columns]


def _record_formula(out: Dict[str, str], name: str, node: object) -> None:
    formula = render_cqn_expression(node)
    # Skip bare-``NULL`` placeholder columns; unrenderable nodes are already None.
    # First occurrence wins (setdefault): a UNION's output name comes from its
    # first branch, so branch 0's formula is the authoritative one.
    if formula and formula != _SQL_NULL:
        out.setdefault(name, formula)


def _formulas_from_query_columns(
    csn_def: Mapping[str, object], out: Dict[str, str]
) -> None:
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
            if name is not None:
                _record_formula(out, name, col)


def _formulas_from_elements(csn_def: Mapping[str, object], out: Dict[str, str]) -> None:
    elements = csn_def.get(CSN_KEY_ELEMENTS)
    if not isinstance(elements, dict):
        return
    for name, element in elements.items():
        if not isinstance(element, dict):
            continue
        value = element.get(CSN_KEY_VALUE)
        if isinstance(value, dict):
            _record_formula(out, name, value)


def extract_calculated_column_formulas(csn_def: Mapping[str, object]) -> Dict[str, str]:
    """Map each calculated output column (by name) to its rendered formula.

    Reads the ``query`` projection columns and any calculated element carrying an
    inline ``value`` expression; plain projections/renames are omitted. When a
    name occurs more than once (across UNION branches or between the query and
    elements maps), the first occurrence wins.
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
