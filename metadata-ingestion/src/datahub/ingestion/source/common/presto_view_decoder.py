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
from typing import Any, Dict, List, Optional, TypedDict, cast

# Markers wrapping the base64-encoded payload of a Presto/Athena view.
PRESTO_VIEW_PREFIX = "/* Presto View: "
PRESTO_VIEW_SUFFIX = " */"

_TIME_ZONE_SUFFIX = re.compile(r"\s+with\s+time\s+zone$")
_TYPE_PRECISION = re.compile(r"^(timestamp|time)\s*\(\s*\d+\s*\)$")
_PARAMETERIZED_TYPE = re.compile(r"^(array|map|row)\s*\((.*)\)$", re.DOTALL)
# A named row field: a double-quoted or bare identifier followed by its type.
_ROW_FIELD = re.compile(r'^(?:"([^"]+)"|(\w+))\s+(.+)$', re.DOTALL)

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
_FALLBACK_HIVE_TYPE = "string"


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


def decode_presto_view(view_text: str) -> PrestoViewDefinition:
    """Decode a Presto/Athena view envelope into its JSON payload.

    Expects text of the form ``/* Presto View: <base64> */`` and returns the parsed
    JSON object (typically with ``originalSql`` and ``columns`` keys).

    Raises on malformed input (invalid base64, non-JSON, or a non-object payload).
    Callers that want best-effort behaviour should catch the exception.
    """
    encoded = view_text.split(PRESTO_VIEW_PREFIX, 1)[-1].rsplit(PRESTO_VIEW_SUFFIX, 1)[
        0
    ]
    payload = json.loads(base64.b64decode(encoded))
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
    """Split ``type_args`` on commas that are not nested inside parentheses."""
    parts: List[str] = []
    depth = 0
    current: List[str] = []
    for char in type_args:
        if char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
        if char == "," and depth == 0:
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
    """
    normalized = _TIME_ZONE_SUFFIX.sub("", trino_type.strip().lower())
    precision_match = _TYPE_PRECISION.match(normalized)
    if precision_match:
        normalized = precision_match.group(1)

    parameterized = _PARAMETERIZED_TYPE.match(normalized)
    if parameterized:
        kind, type_args = parameterized.group(1), parameterized.group(2)
        args = _split_top_level(type_args)
        if kind == "array" and len(args) == 1:
            return f"array<{trino_type_to_hive_type(args[0])}>"
        if kind == "map" and len(args) == 2:
            key_type = trino_type_to_hive_type(args[0])
            value_type = trino_type_to_hive_type(args[1])
            return f"map<{key_type},{value_type}>"
        if kind == "row":
            struct_fields: List[str] = []
            for arg in args:
                row_field = _ROW_FIELD.match(arg)
                if not row_field:
                    # Anonymous row fields have no name; Hive structs require one.
                    return _FALLBACK_HIVE_TYPE
                field_name = row_field.group(1) or row_field.group(2)
                field_type = trino_type_to_hive_type(row_field.group(3))
                struct_fields.append(f"{field_name}:{field_type}")
            return f"struct<{','.join(struct_fields)}>"
        # Wrong number of array/map arguments: not a type we can translate.
        return _FALLBACK_HIVE_TYPE

    hive_type = _TRINO_TO_HIVE_SCALAR_TYPES.get(normalized, normalized)
    if hive_type.split("(", 1)[0].strip() not in _HIVE_SCALAR_TYPES:
        return _FALLBACK_HIVE_TYPE
    return hive_type
