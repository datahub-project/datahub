"""Utility helpers for the BigID DataHub connector."""

from __future__ import annotations

import re
from datetime import datetime
from typing import Optional
from urllib.parse import quote

from datahub.metadata.schema_classes import (
    BooleanTypeClass,
    BytesTypeClass,
    NumberTypeClass,
    SchemaFieldDataTypeClass,
    StringTypeClass,
    TimeTypeClass,
)


_NUMERIC_TYPES = ("int", "bigint", "smallint", "tinyint", "number", "numeric", "decimal", "float", "double", "real")


def _map_field_type(field_type: str) -> SchemaFieldDataTypeClass:
    ft = (field_type or "").lower()
    if any(t in ft for t in ("varchar", "char", "text", "string", "nvarchar", "clob")):
        return SchemaFieldDataTypeClass(type=StringTypeClass())
    if any(t in ft for t in _NUMERIC_TYPES):
        return SchemaFieldDataTypeClass(type=NumberTypeClass())
    if any(t in ft for t in ("bool", "boolean", "bit")):
        return SchemaFieldDataTypeClass(type=BooleanTypeClass())
    if any(t in ft for t in ("date", "time", "timestamp", "datetime")):
        return SchemaFieldDataTypeClass(type=TimeTypeClass())
    if any(t in ft for t in ("bytes", "binary", "blob", "varbinary")):
        return SchemaFieldDataTypeClass(type=BytesTypeClass())
    return SchemaFieldDataTypeClass(type=StringTypeClass())


def _slugify(name: str) -> str:
    """Convert a free-text name to a URL-safe slug."""
    slug = name.lower().strip()
    slug = re.sub(r"[^a-z0-9]+", "_", slug)
    return slug.strip("_")


# DataHub dataset URN format: urn:li:dataset:(urn:li:dataPlatform:{p},{name},{env})
# The name component must not contain the delimiter characters ( ) ,
_URN_RESERVED = "(),:"
_URN_SAFE_CHARS = "".join(
    c for c in
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-._~!$&'*+;=@/?"
    if c not in _URN_RESERVED
)


def _encode_urn_name(name: str) -> str:
    """Percent-encode characters that are reserved in DataHub URN delimiters."""
    return quote(name, safe=_URN_SAFE_CHARS)


def _parse_iso_to_ms(ts: str) -> Optional[int]:
    """Parse an ISO-8601 timestamp string to milliseconds since epoch."""
    if not ts:
        return None
    try:
        ts_clean = ts.replace("Z", "+00:00")
        dt = datetime.fromisoformat(ts_clean)
        return int(dt.timestamp() * 1000)
    except ValueError:
        return None
