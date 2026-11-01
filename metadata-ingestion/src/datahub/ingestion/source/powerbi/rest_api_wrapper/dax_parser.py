"""Extract model references from DAX without evaluating it.

The connector only needs to know which model tables, columns and measures a
DAX expression reads, so a small tokenizer is enough: comments and string
literals are blanked out first (a column name inside ``"..."`` or after
``//`` is not a reference), then ``'Table'[Field]``, ``Table[Field]`` and
bare ``[Field]`` references plus the table arguments of the common
table-valued functions are collected. Whether a referenced table or field
exists in the model is the caller's decision.
"""

import re
from typing import List, Optional, Set

from datahub.ingestion.source.powerbi.rest_api_wrapper.data_classes import (
    DaxReferences,
    MeasureReference,
)

# `'Table'[Field]`, `Table[Field]` or a bare `[Field]`. A quoted table name
# escapes an embedded quote as `''`.
DAX_FIELD_REFERENCE_RE = re.compile(
    r"(?:'(?P<quoted>(?:[^']|'')+)'|(?P<bare>[A-Za-z_]\w*))?\[(?P<field>[^\]]+)\]"
)
# Every quoted table name, e.g. `COUNTROWS('Claims')`; the ones directly
# followed by `[` are field references and already handled above. Quoted
# names are consumed left to right so a closing quote is never paired with
# the opening quote of the next reference.
DAX_QUOTED_TABLE_RE = re.compile(r"'((?:[^']|'')+)'")

# Functions whose first argument is a table. An unquoted first argument is a
# bare table name only when it is not followed by `[` (then it is a column
# reference, handled above) and the caller still has to check it is a model
# table rather than a VAR holding a table expression.
TABLE_ARGUMENT_FUNCTIONS = (
    "RELATED",
    "RELATEDTABLE",
    "LOOKUPVALUE",
    "SUMMARIZE",
    "SUMMARIZECOLUMNS",
    "CALCULATETABLE",
    "FILTER",
    "VALUES",
    "DISTINCT",
    "ALL",
    "ALLEXCEPT",
    "ALLNOBLANKROW",
    "ALLSELECTED",
    "COUNTROWS",
    "ADDCOLUMNS",
    "SELECTCOLUMNS",
)
DAX_TABLE_ARGUMENT_RE = re.compile(
    r"\b(?:"
    + "|".join(TABLE_ARGUMENT_FUNCTIONS)
    + r")\s*\(\s*(?P<bare>[A-Za-z_]\w*)\s*(?=[,)])",
    re.IGNORECASE,
)

ROW_CONTEXT_VARIABLE_PREFIX = "@"


def _unescape_table(name: str) -> str:
    return name.replace("''", "'")


def strip_comments_and_strings(expression: str) -> str:
    """Blank out `//`, `--` and `/* */` comments and `"..."` string literals
    (a doubled quote escapes a quote inside a string). Everything else is
    returned unchanged, so a reference regex never matches inside a literal."""
    out: List[str] = []
    i = 0
    length = len(expression)
    while i < length:
        ch = expression[i]
        nxt = expression[i + 1] if i + 1 < length else ""
        if ch == '"':
            i += 1
            while i < length:
                if expression[i] == '"':
                    if i + 1 < length and expression[i + 1] == '"':
                        i += 2
                        continue
                    break
                i += 1
            out.append(" ")
            i += 1
        elif ch == "/" and nxt == "*":
            end = expression.find("*/", i + 2)
            i = length if end == -1 else end + 2
            out.append(" ")
        elif (ch == "/" and nxt == "/") or (ch == "-" and nxt == "-"):
            end = expression.find("\n", i)
            i = length if end == -1 else end
        else:
            out.append(ch)
            i += 1
    return "".join(out)


def extract_dax_references(expression: Optional[str]) -> DaxReferences:
    """Model fields and tables a DAX expression reads, each distinct and in
    order of first appearance. `[@Alias]` row-context variables are not
    fields. A field reference also contributes its table."""
    references = DaxReferences()
    if not expression:
        return references
    text = strip_comments_and_strings(expression)

    seen_fields: Set[MeasureReference] = set()
    seen_tables: Set[str] = set()

    def add_table(name: str) -> None:
        if name not in seen_tables:
            seen_tables.add(name)
            references.tables.append(name)

    for match in DAX_FIELD_REFERENCE_RE.finditer(text):
        field_name = match.group("field").strip()
        if not field_name or field_name.startswith(ROW_CONTEXT_VARIABLE_PREFIX):
            continue
        table: Optional[str] = None
        if match.group("quoted") is not None:
            table = _unescape_table(match.group("quoted"))
        elif match.group("bare") is not None:
            table = match.group("bare")
        reference = MeasureReference(table=table, field=field_name)
        if reference not in seen_fields:
            seen_fields.add(reference)
            references.fields.append(reference)
        if table is not None:
            add_table(table)

    for match in DAX_QUOTED_TABLE_RE.finditer(text):
        if text[match.end() : match.end() + 1] == "[":
            continue
        add_table(_unescape_table(match.group(1)))

    for match in DAX_TABLE_ARGUMENT_RE.finditer(text):
        add_table(match.group("bare"))

    return references
