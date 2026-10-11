"""Kafka-compatible pgQueue message headers (JSONB in PostgreSQL)."""

from __future__ import annotations

import base64
import json
from typing import Any, List, Sequence, Tuple

HeaderList = List[Tuple[str, bytes]]

KEY_FIELD = "key"
VALUE_B64_FIELD = "v"


def headers_to_json(headers: Sequence[Tuple[str, bytes]]) -> str | None:
    """Serialize headers for JSONB column; empty sequence stores SQL NULL."""
    if len(headers) == 0:
        return None
    arr = []
    for k, v in headers:
        arr.append(
            {
                KEY_FIELD: k,
                VALUE_B64_FIELD: base64.standard_b64encode(v).decode("ascii"),
            }
        )
    return json.dumps(arr, separators=(",", ":"))


def headers_from_db(raw: Any) -> HeaderList:
    """Deserialize JSONB (string, dict/list, or bytes) to ordered ``(key, value)`` tuples."""
    if raw is None:
        return []
    if isinstance(raw, memoryview):
        raw = raw.tobytes()
    if isinstance(raw, (bytes, bytearray)):
        raw = raw.decode("utf-8")
    if isinstance(raw, str):
        if not raw.strip():
            return []
        parsed = json.loads(raw)
    elif isinstance(raw, list):
        parsed = raw
    else:
        return []

    out: HeaderList = []
    for item in parsed:
        if not isinstance(item, dict):
            continue
        key = str(item.get(KEY_FIELD, ""))
        vb64 = str(item.get(VALUE_B64_FIELD, ""))
        out.append((key, base64.standard_b64decode(vb64.encode("ascii"))))
    return out
