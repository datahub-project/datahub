"""Parameter types for list arguments that clients may send as a string.

MCP clients and LLMs frequently serialize a list-typed argument as a *string*: either a
JSON-encoded array (``'["a", "b"]'``) or a single bare value (``'a'``). Pydantic rejects
that against a plain ``List[str]`` before the tool body runs.

Widening the annotation to ``List[str] | str`` would accept it, but publishes
``anyOf: [array, string]``. Strict function-calling APIs reject a parameter with no
single declared type: Google Gemini (acryldata/mcp-server-datahub#98), and several
OpenAI-compatible endpoints, which reject the *whole* tool list with a 422
(acryldata/mcp-server-datahub#173). So:

- ``StrList`` (the default): coerce the string to a list in a ``BeforeValidator``. The
  schema stays a plain array and the tool body always receives a list.
- ``StrListOrStr``: only for tools whose body must see the original string (e.g.
  ``get_entities`` returns a single dict for a bare URN). The runtime type stays the
  union and ``WithJsonSchema`` publishes a plain array.
"""

from typing import Annotated, List, Optional

from pydantic import BeforeValidator, WithJsonSchema
from pydantic_core import from_json


def coerce_str_list(
    value: Optional[List[str] | str],
) -> Optional[List[str]]:
    """Normalize a list argument that an MCP client may have sent as a string.

    Parsing is delegated to pydantic's own strict JSON parser: a string that parses to a
    JSON array is used as-is, anything else is treated as a single value. Item
    validation stays with the outer ``List[str]``, so this only decides list-vs-scalar.

    Malformed JSON is deliberately not repaired (unlike ``get_entities``, which uses
    ``repair_json``): most of these parameters feed write tools, where guessing at a
    broken array is riskier than treating the string as one literal value.

    ``""`` and ``"null"`` mean "no value", not a one-item list: they become ``None``, so
    an optional parameter is treated as omitted and a required one is still rejected.
    Otherwise ``column_paths="null"`` would tag a column literally named ``null``.
    """
    if not isinstance(value, str):
        return value
    stripped = value.strip()
    if not stripped:
        return None
    try:
        parsed = from_json(stripped)
    except ValueError:
        return [stripped]
    if parsed is None:
        return None
    return parsed if isinstance(parsed, list) else [stripped]


StrList = Annotated[List[str], BeforeValidator(coerce_str_list)]
"""A list of strings; a bare or JSON-stringified string is coerced to a list."""

# The optional variants put the validator *outside* the Optional, so a coerced ``None``
# (from ``""`` or ``"null"``) is accepted. ``Optional[StrList]`` would check for None
# first and then reject the coerced None as "not a valid list".
OptionalStrList = Annotated[Optional[List[str]], BeforeValidator(coerce_str_list)]
"""``StrList`` that may be omitted; ``""`` and ``"null"`` count as omitted."""

OptionalNullableItemStrList = Annotated[
    Optional[List[Optional[str]]], BeforeValidator(coerce_str_list)
]
"""Like ``OptionalStrList``, but items may be null (e.g. ``column_paths``)."""

StrListOrStr = Annotated[
    List[str] | str,
    WithJsonSchema({"type": "array", "items": {"type": "string"}}),
]
"""A list of strings in the schema; a single string reaches the tool body unchanged."""


def coerce_keyword_list(
    value: Optional[List[str] | str],
) -> Optional[List[str]]:
    """Like ``coerce_str_list``, and also split comma-joined keywords.

    Only for keyword arguments: a URN can contain commas.
    """
    values = coerce_str_list(value)
    if not isinstance(values, list):
        return values
    keywords: List[str] = []
    for item in values:
        if isinstance(item, str):
            keywords.extend(piece.strip() for piece in item.split(",") if piece.strip())
        else:
            # BeforeValidator sees unvalidated items; leave them for pydantic to reject.
            keywords.append(item)
    return keywords
