import re
from typing import List, Optional, Set

from datahub.ingestion.source.powerbi.rest_api_wrapper.data_classes import (
    DaxFieldReference,
    DaxReferences,
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
    "SUMX",
    "AVERAGEX",
    "MAXX",
    "MINX",
    "COUNTX",
    "RANKX",
    "TOPN",
    "GENERATE",
    "UNION",
    "EXCEPT",
    "INTERSECT",
    "CROSSJOIN",
    "TREATAS",
)
# Table functions that evaluate their other arguments in the row context of
# the table they are given: inside them a bare `[Column]` is a column of that
# table, not of the table that owns the expression.
ROW_CONTEXT_ITERATOR_FUNCTIONS = (
    "SUMX",
    "AVERAGEX",
    "MAXX",
    "MINX",
    "COUNTX",
    "RANKX",
    "TOPN",
    "GENERATE",
    "RELATEDTABLE",
    "CALCULATETABLE",
    "FILTER",
    "ADDCOLUMNS",
    "SELECTCOLUMNS",
    "SUMMARIZE",
)
DAX_TABLE_ARGUMENT_RE = re.compile(
    r"\b(?:"
    + "|".join(TABLE_ARGUMENT_FUNCTIONS)
    + r")\s*\(\s*(?P<bare>[A-Za-z_]\w*)\s*(?=[,)])",
    re.IGNORECASE,
)
DAX_ROW_CONTEXT_ITERATOR_RE = re.compile(
    r"\b(?:" + "|".join(ROW_CONTEXT_ITERATOR_FUNCTIONS) + r")\s*\(",
    re.IGNORECASE,
)

ROW_CONTEXT_VARIABLE_PREFIX = "@"
STRING_QUOTE = '"'
IDENTIFIER_QUOTE = "'"
BLOCK_COMMENT_START = "/*"
BLOCK_COMMENT_END = "*/"
LINE_COMMENT_MARKERS = ("//", "--")


def _unescape_table(name: str) -> str:
    return name.replace("''", "'")


def _end_of_quoted(expression: str, start: int, quote: str) -> int:
    """Index just past the closing quote of the quoted run that opens at
    `start`; a doubled quote inside the run escapes a quote. An unterminated
    run extends to the end of the expression."""
    i = start + 1
    length = len(expression)
    while i < length:
        if expression[i] == quote:
            if expression[i + 1 : i + 2] == quote:
                i += 2
                continue
            return i + 1
        i += 1
    return length


def strip_comments_and_strings(expression: str) -> str:
    """Blank out `//`, `--` and `/* */` comments and `"..."` string literals
    so a reference regex never matches inside them. Single-quoted table
    identifiers are kept verbatim, including any `"` or comment marker they
    contain. Everything else is returned unchanged."""
    out: List[str] = []
    i = 0
    length = len(expression)
    while i < length:
        ch = expression[i]
        pair = expression[i : i + 2]
        if ch == IDENTIFIER_QUOTE:
            end = _end_of_quoted(expression, i, IDENTIFIER_QUOTE)
            out.append(expression[i:end])
            i = end
        elif ch == STRING_QUOTE:
            i = _end_of_quoted(expression, i, STRING_QUOTE)
            out.append(" ")
        elif pair == BLOCK_COMMENT_START:
            end = expression.find(BLOCK_COMMENT_END, i + 2)
            i = length if end == -1 else end + len(BLOCK_COMMENT_END)
            out.append(" ")
        elif pair in LINE_COMMENT_MARKERS:
            end = expression.find("\n", i)
            i = length if end == -1 else end
        else:
            out.append(ch)
            i += 1
    return "".join(out)


def extract_dax_references(expression: Optional[str]) -> DaxReferences:
    """Model fields and tables a DAX expression reads, each distinct and in
    order of first appearance, plus whether the expression iterates a table.

    Comments and string literals are blanked out first, so a field name
    inside `"..."` or after `//` is not a reference. `'Table'[Field]`,
    `Table[Field]` and bare `[Field]` references are collected together with
    the table arguments of the common table functions; `[@Alias]` row-context
    variables are not fields. A field reference also contributes its table.
    Whether a referenced table or field exists in the model is the caller's
    decision."""
    references = DaxReferences()
    if not expression:
        return references
    text = strip_comments_and_strings(expression)

    seen_fields: Set[DaxFieldReference] = set()
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
        reference = DaxFieldReference(table=table, field=field_name)
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

    references.has_row_context_iterators = (
        DAX_ROW_CONTEXT_ITERATOR_RE.search(text) is not None
    )
    return references
