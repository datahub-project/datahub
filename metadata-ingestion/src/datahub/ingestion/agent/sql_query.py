from typing import Any, Dict, List, Sequence

_JSON_SAFE_TYPES = (str, int, float, bool)


def _json_safe(value: object) -> object:
    # Catalog reads return dates, decimals, UUIDs and (on some drivers) bytes.
    # Coercing here keeps every caller free of a custom JSON encoder.
    if value is None or isinstance(value, _JSON_SAFE_TYPES):
        return value
    if isinstance(value, (bytes, bytearray)):
        return bytes(value).decode("utf-8", errors="replace")
    return str(value)


def sql_result(
    columns: Sequence[str], rows: Sequence[Sequence[Any]], limit: int
) -> Dict[str, object]:
    """Shape one catalog result set for probe output.

    Rows are positional with a separate column list, so a wide result does not
    repeat every column name on every row -- probe output is read by an agent
    with a finite context window.

    Callers fetch one row beyond `limit` so truncation is observed rather than
    inferred from a full page.
    """
    kept: List[List[object]] = [
        [_json_safe(value) for value in row] for row in rows[:limit]
    ]
    return {
        "columns": list(columns),
        "rows": kept,
        "row_count": len(kept),
        "truncated": len(rows) > limit,
    }
