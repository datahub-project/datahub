"""Helpers for decoding Presto/Athena view definitions.

Presto, Trino and Athena register catalog views whose stored "view text" is a
base64-encoded JSON document wrapped in a marker comment, e.g.::

    /* Presto View: <base64-of-json> */

The decoded JSON carries ``originalSql`` (the view's SQL), ``columns`` and other
fields. Several connectors (Hive Metastore, Glue) encounter this same encoding, so
the decode logic lives here to avoid duplication.
"""

import base64
import json
import re
from typing import Any, Dict, List, Optional, Tuple, TypedDict, cast

# Markers wrapping the base64-encoded payload of a Presto/Athena view.
PRESTO_VIEW_PREFIX = "/* Presto View: "
PRESTO_VIEW_SUFFIX = " */"

# Type keywords are matched case-insensitively; the type string itself is not
# lowercased up front, so quoted row field names keep their case.
_TIME_ZONE_SUFFIX = re.compile(r"\s+with\s+time\s+zone$", re.IGNORECASE)
_TYPE_PRECISION = re.compile(r"^(timestamp|time)\s*\(\s*\d+\s*\)$", re.IGNORECASE)
_PARAMETERIZED_TYPE = re.compile(
    r"^(array|map|row)\s*\((.*)\)$", re.DOTALL | re.IGNORECASE
)
# A named row field: a double-quoted (with `""` escaping a quote) or bare
# identifier followed by its type.
_ROW_FIELD = re.compile(r'^(?:"((?:[^"]|"")+)"|(\w+))\s+(.+)$', re.DOTALL)
# A scalar type name with optional integer parameters, e.g. `varchar(10)` or
# `decimal(10,2)`.
_SCALAR_TYPE = re.compile(
    r"^([a-z]+)(?:\s*\(\s*\d+\s*(?:,\s*\d+\s*)?\))?$", re.IGNORECASE
)
# Trino types spelled with several words. An anonymous row field of one of these
# types (e.g. `interval day to second`) would otherwise match _ROW_FIELD, taking
# the first word as a field name.
_MULTI_WORD_TYPE = re.compile(
    r"^(?:interval\s+(?:year|month|day|hour|minute|second)"
    r"|(?:timestamp|time)\s+with"
    r"|double\s+precision)\b",
    re.IGNORECASE,
)

# Trino scalar types whose name differs from the Hive equivalent.
_TRINO_TO_HIVE_SCALAR_TYPES: Dict[str, str] = {
    "real": "float",
    "integer": "int",
    "varbinary": "binary",
}
# Hive scalar types (by base name, before any "(...)") that DataHub's Hive schema
# parser understands. Any other Trino type (time, interval, json, uuid, ipaddress,
# hyperloglog, ...) or malformed type string falls back to string, so the column
# keeps a usable type instead of becoming untyped.
_HIVE_SCALAR_TYPES = frozenset(
    {
        "boolean",
        "tinyint",
        "smallint",
        "int",
        "bigint",
        "float",
        "double",
        "decimal",
        "string",
        "varchar",
        "char",
        "binary",
        "date",
        "timestamp",
    }
)
FALLBACK_HIVE_TYPE = "string"
_MAX_TYPE_NESTING_DEPTH = 64
# Real Presto view payloads nest only a few levels (object -> columns -> column).
_MAX_JSON_NESTING_DEPTH = 64
# Far above any real column type (a row with thousands of fields), while still
# bounding the work spent on corrupt input. Whitespace runs are collapsed before
# matching, which keeps the type regexes linear.
_MAX_TYPE_LENGTH = 1_000_000


class PrestoViewDefinition(TypedDict, total=False):
    """Decoded payload of a Presto/Athena view.

    ``total=False`` because the decoder does not guarantee these keys are present
    (a malformed payload may omit them); only the documented subset that consumers
    rely on is declared.
    """

    originalSql: str
    # Raw, unvalidated JSON from the catalog; use get_presto_view_columns for
    # validated access.
    columns: List[Dict[str, Any]]


class PrestoViewColumn(TypedDict):
    """A validated output column of a Presto/Athena view."""

    name: str
    type: str  # Trino type string, e.g. "timestamp(3) with time zone"


def is_presto_view(view_text: str) -> bool:
    """Return True if ``view_text`` is a Presto/Athena base64-encoded view."""
    return view_text.startswith(PRESTO_VIEW_PREFIX)


def _json_nesting_exceeds(text: str, limit: int) -> bool:
    """Return True if JSON ``text`` nests arrays/objects deeper than ``limit``.

    Brackets inside string literals are ignored. The text is not validated;
    malformed JSON is left for ``json.loads`` to reject.
    """
    depth = 0
    in_string = False
    escaped = False
    for char in text:
        if in_string:
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == '"':
                in_string = False
        elif char == '"':
            in_string = True
        elif char in "[{":
            depth += 1
            if depth > limit:
                return True
        elif char in "]}":
            depth -= 1
    return False


def decode_presto_view(view_text: str) -> PrestoViewDefinition:
    """Decode a Presto/Athena view envelope into its JSON payload.

    Expects text of the form ``/* Presto View: <base64> */`` and returns the parsed
    JSON object (typically with ``originalSql`` and ``columns`` keys).

    Raises ``ValueError`` on malformed input (invalid base64 or text encoding, non-JSON,
    nesting deeper than ``_MAX_JSON_NESTING_DEPTH``, or a non-object payload).
    Callers that want best-effort behaviour should catch it.
    """
    encoded = view_text.split(PRESTO_VIEW_PREFIX, 1)[-1].rsplit(PRESTO_VIEW_SUFFIX, 1)[
        0
    ]
    raw = base64.b64decode(encoded)
    # Same encoding detection as json.loads(bytes), so UTF-16/32 and BOM-prefixed
    # payloads keep decoding.
    text = raw.decode(json.detect_encoding(raw), "surrogatepass")
    # json.loads can overflow the C stack and segfault the whole process on
    # deeply nested input (seen on Python 3.12), which no except clause catches.
    # The payload is untrusted catalog data, so reject deep nesting up front.
    if _json_nesting_exceeds(text, _MAX_JSON_NESTING_DEPTH):
        raise ValueError("Presto view payload is nested too deeply")
    payload = json.loads(text)
    if not isinstance(payload, dict):
        raise ValueError("Presto view payload is not a JSON object")
    return cast(PrestoViewDefinition, payload)


def get_presto_view_columns(
    definition: PrestoViewDefinition,
) -> Optional[List[PrestoViewColumn]]:
    """Return the view's output columns, or ``None`` when ``columns`` is missing,
    empty, not a list, or has any entry without a non-empty string ``name`` and
    ``type``.

    All-or-nothing on purpose: a partial column list would present an incomplete
    schema as if it were the whole view.
    """
    raw_columns = definition.get("columns")
    if not isinstance(raw_columns, list) or not raw_columns:
        return None
    columns: List[PrestoViewColumn] = []
    for raw_column in raw_columns:
        if not isinstance(raw_column, dict):
            return None
        name = raw_column.get("name")
        column_type = raw_column.get("type")
        if not (
            isinstance(name, str)
            and name
            and isinstance(column_type, str)
            and column_type
        ):
            return None
        columns.append(PrestoViewColumn(name=name, type=column_type))
    return columns


def _split_top_level(type_args: str) -> List[str]:
    """Split ``type_args`` on commas outside parentheses and double quotes.

    Quoted row field names may contain commas or parentheses (``"a,b" bigint``),
    which must not be treated as separators. An escaped quote (``""``) toggles
    the quote state twice, so it leaves the name quoted.
    """
    parts: List[str] = []
    depth = 0
    in_quotes = False
    current: List[str] = []
    for char in type_args:
        if char == '"':
            in_quotes = not in_quotes
        elif not in_quotes and char == "(":
            depth += 1
        elif not in_quotes and char == ")":
            depth -= 1
        if char == "," and depth == 0 and not in_quotes:
            parts.append("".join(current).strip())
            current = []
        else:
            current.append(char)
    parts.append("".join(current).strip())
    return parts


def trino_type_to_hive_type(trino_type: str) -> str:
    """Convert a Trino type string (as stored in a Presto view's ``columns``) to
    the closest Hive type string.

    Presto view payloads describe columns with Trino types (``row(a bigint)``,
    ``timestamp(3) with time zone``), while DataHub's Glue/Hive schema handling
    parses Hive types (``struct<a:bigint>``, ``timestamp``). Types Hive cannot
    represent are mapped to their nearest Hive type (dropping time zone and
    precision) or to ``string``, so the column is kept rather than lost.

    Raises ``ValueError`` when the type is longer than ``_MAX_TYPE_LENGTH`` or
    nested deeper than ``_MAX_TYPE_NESTING_DEPTH``, so callers can report the
    column instead of silently typing it as ``string``.
    """
    if len(trino_type) > _MAX_TYPE_LENGTH:
        raise ValueError("Trino type is too long to convert")
    # Collapse whitespace runs once here; otherwise the time-zone regex re-scans
    # long inner runs at every nesting level.
    return _trino_type_to_hive_type(" ".join(trino_type.split()), depth=0)


def _trino_type_to_hive_type(trino_type: str, depth: int) -> str:
    # The payload is untrusted catalog data; cap nesting so a corrupt or crafted
    # type cannot exhaust the stack and abort the ingestion run.
    if depth > _MAX_TYPE_NESTING_DEPTH:
        raise ValueError("Trino type is nested too deeply to convert")

    normalized = _TIME_ZONE_SUFFIX.sub("", trino_type.strip())
    precision_match = _TYPE_PRECISION.match(normalized)
    if precision_match:
        normalized = precision_match.group(1)

    parameterized = _PARAMETERIZED_TYPE.match(normalized)
    if parameterized:
        kind, type_args = parameterized.group(1).lower(), parameterized.group(2)
        args = _split_top_level(type_args)
        if kind == "array" and len(args) == 1:
            return f"array<{_trino_type_to_hive_type(args[0], depth + 1)}>"
        if kind == "map" and len(args) == 2:
            key_type = _trino_type_to_hive_type(args[0], depth + 1)
            value_type = _trino_type_to_hive_type(args[1], depth + 1)
            return f"map<{key_type},{value_type}>"
        if kind == "row":
            return _row_type_to_hive_struct(args, depth)
        # Wrong number of array/map arguments: not a type we can translate.
        return FALLBACK_HIVE_TYPE

    scalar_type = normalized.lower()
    if not _SCALAR_TYPE.match(scalar_type):
        return FALLBACK_HIVE_TYPE
    hive_type = _TRINO_TO_HIVE_SCALAR_TYPES.get(scalar_type, scalar_type)
    if hive_type.split("(", 1)[0].strip() not in _HIVE_SCALAR_TYPES:
        return FALLBACK_HIVE_TYPE
    return hive_type


def _row_type_to_hive_struct(args: List[str], depth: int) -> str:
    named_fields: List[Tuple[Optional[str], str]] = []
    for arg in args:
        row_field = _ROW_FIELD.match(arg)
        if row_field and not _MULTI_WORD_TYPE.match(arg):
            # Trino folds unquoted identifiers to lowercase; quoted names keep
            # their case.
            field_name = (
                row_field.group(1).replace('""', '"')
                if row_field.group(1)
                else row_field.group(2).lower()
            )
            named_fields.append((field_name, row_field.group(3)))
        else:
            named_fields.append((None, arg))

    # Hive struct field names are case-insensitive.
    used_names = {name.lower() for name, _ in named_fields if name is not None}
    struct_fields: List[str] = []
    for index, (field_name, field_trino_type) in enumerate(named_fields):
        if field_name is None:
            # Anonymous row fields (e.g. from `ROW(1, 'a')`) have no name, but
            # Hive structs require one; use a positional name that no sibling
            # already has.
            field_name = f"field{index}"
            while field_name.lower() in used_names:
                field_name = f"_{field_name}"
            used_names.add(field_name.lower())
        field_type = _trino_type_to_hive_type(field_trino_type, depth + 1)
        struct_fields.append(f"{field_name}:{field_type}")
    return f"struct<{','.join(struct_fields)}>"
