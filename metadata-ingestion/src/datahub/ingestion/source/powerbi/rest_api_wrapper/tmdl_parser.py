"""Minimal TMDL reader for DirectLake column bindings.

The admin scan does not return a DirectLake column's ``sourceColumn``, so a
column renamed in the semantic model cannot be matched to its Delta column.
The semantic model's TMDL definition (Fabric ``getDefinition``) does carry the
binding::

    table Customers
        column CustomerID
            sourceColumn: CustomerID
        column 'Customer Name'
            sourceColumn: CustomerName

Only what column lineage needs is read: per table, the ``sourceColumn`` of each
data column. Calculated columns (``column X = <DAX>``), calculated-table
columns, measures, partitions, hierarchies and everything else are ignored.

TMDL reference:
https://learn.microsoft.com/en-us/analysis-services/tmdl/tmdl-overview
"""

import base64
import logging
import re
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Tuple

logger = logging.getLogger(__name__)

# Semantic model definition parts holding one table each.
TMDL_TABLE_PART_PREFIX = "definition/tables/"
TMDL_FILE_SUFFIX = ".tmdl"

# Column ``type`` values with no physical upstream column.
_NON_DATA_COLUMN_TYPES = {"calculated", "calculatedtablecolumn", "rownumber"}

_DECLARATION_RE = re.compile(r"^(table|column)\s+(.+)$")
_PROPERTY_RE = re.compile(r"^(sourceColumn|type)\s*:\s*(.*)$")
_FENCE = "```"


class TmdlParseError(ValueError):
    """A definition part could not be decoded or read as TMDL."""


@dataclass
class _ColumnState:
    name: str
    indent: int
    is_calculated: bool
    source_column: Optional[str] = None
    column_type: Optional[str] = None


def _measure_indent(line: str) -> int:
    expanded = line.expandtabs(4)
    return len(expanded) - len(expanded.lstrip())


def parse_tmdl_name(text: str) -> Tuple[str, str]:
    """Split a TMDL object name from the rest of a declaration.

    Names with spaces or special characters are single-quoted, with an embedded
    quote doubled (``'Customer''s Name'``). Returns ``(name, remainder)``.
    """
    text = text.strip()
    if text.startswith("'"):
        chars: List[str] = []
        i = 1
        while i < len(text):
            ch = text[i]
            if ch == "'":
                if i + 1 < len(text) and text[i + 1] == "'":
                    chars.append("'")
                    i += 2
                    continue
                return "".join(chars), text[i + 1 :].strip()
            chars.append(ch)
            i += 1
        raise TmdlParseError(f"Unterminated quoted name: {text!r}")

    match = re.match(r"[^\s=]+", text)
    if not match:
        raise TmdlParseError(f"Missing object name: {text!r}")
    return match.group(0), text[match.end() :].strip()


def _parse_property_value(value: str) -> str:
    value = value.strip()
    for quote in ('"', "'"):
        if len(value) >= 2 and value[0] == quote and value[-1] == quote:
            return value[1:-1].replace(quote * 2, quote)
    return value


def parse_tmdl_table(text: str) -> Tuple[Optional[str], Dict[str, str]]:
    """Read one table's TMDL file.

    Returns the table name (None when the file declares no table) and a map of
    data-column name to its ``sourceColumn``. Columns without a
    ``sourceColumn`` are omitted.
    """
    table_name: Optional[str] = None
    table_indent = -1
    # Indent of the table's direct children (columns, measures, partitions...).
    # Deeper lines belong to a child, e.g. a multi-line DAX or M expression.
    child_indent: Optional[int] = None
    column: Optional[_ColumnState] = None
    columns: Dict[str, str] = {}
    in_fence = False

    def close_column() -> None:
        nonlocal column
        if (
            column is not None
            and not column.is_calculated
            and column.source_column
            and (column.column_type or "data").lower() not in _NON_DATA_COLUMN_TYPES
        ):
            columns.setdefault(column.name, column.source_column)
        column = None

    for raw_line in text.lstrip("\ufeff").splitlines():
        stripped = raw_line.strip()
        if in_fence:
            if stripped.startswith(_FENCE):
                in_fence = False
            continue
        if not stripped or stripped.startswith("//"):
            continue
        # A fenced (```) multi-line expression opens at the end of a line.
        opens_fence = stripped.endswith(_FENCE) and stripped != _FENCE
        if stripped == _FENCE:
            in_fence = True
            continue

        indent = _measure_indent(raw_line)

        if column is not None and indent <= column.indent:
            close_column()

        if column is not None:
            prop = _PROPERTY_RE.match(stripped)
            if prop and not opens_fence:
                key, value = prop.group(1), _parse_property_value(prop.group(2))
                if key == "sourceColumn" and column.source_column is None:
                    column.source_column = value or None
                elif key == "type":
                    column.column_type = value
            if opens_fence:
                in_fence = True
            continue

        if table_name is not None and child_indent is None and indent > table_indent:
            child_indent = indent

        declaration = _DECLARATION_RE.match(stripped)
        if declaration:
            keyword, rest = declaration.group(1), declaration.group(2)
            if keyword == "table" and table_name is None:
                table_name, _ = parse_tmdl_name(rest)
                table_indent = indent
            elif keyword == "column" and indent == child_indent:
                name, remainder = parse_tmdl_name(rest)
                column = _ColumnState(
                    name=name,
                    indent=indent,
                    is_calculated=remainder.startswith("="),
                )
        if opens_fence:
            in_fence = True

    close_column()
    return table_name, columns


def decode_definition_part(part: dict) -> str:
    payload_type = part.get("payloadType")
    if payload_type != "InlineBase64":
        raise TmdlParseError(f"Unsupported payloadType {payload_type!r}")
    try:
        return base64.b64decode(part.get("payload") or "", validate=True).decode(
            "utf-8-sig"
        )
    except (ValueError, UnicodeDecodeError) as e:
        raise TmdlParseError(f"Cannot decode payload: {e}") from e


def is_table_part(part: dict) -> bool:
    path = part.get("path") or ""
    return path.startswith(TMDL_TABLE_PART_PREFIX) and path.endswith(TMDL_FILE_SUFFIX)


def iter_table_parts(parts: Iterable[dict]) -> Iterable[dict]:
    return (part for part in parts if is_table_part(part))
