import json
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

from datahub.ingestion.source.external_dq.contract import EPOCH, LogicalType


def coerce_value(value: Any, logical_type: LogicalType) -> Any:  # noqa: C901
    """Convert a driver value to the canonical Python value for a logical type.

    Readers should return TIMESTAMP columns as epoch millis; a naive datetime is
    assumed to be UTC.
    """
    if value is None:
        return None
    if logical_type is LogicalType.STRING:
        return value if isinstance(value, str) else str(value)
    if logical_type is LogicalType.BOOLEAN:
        if isinstance(value, bool):
            return value
        if isinstance(value, int) and value in (0, 1):
            return bool(value)
        if isinstance(value, str) and value.strip().lower() in ("true", "false"):
            return value.strip().lower() == "true"
        raise ValueError(f"not a boolean: {value!r}")
    if logical_type is LogicalType.INT64:
        if isinstance(value, bool):
            raise ValueError(f"not an integer: {value!r}")
        if isinstance(value, int):
            return value
        if isinstance(value, (float, Decimal)) and value == int(value):
            return int(value)
        if isinstance(value, str):
            return int(value.strip())
        raise ValueError(f"not an integer: {value!r}")
    if logical_type is LogicalType.FLOAT64:
        if isinstance(value, bool):
            raise ValueError(f"not a number: {value!r}")
        if isinstance(value, (int, float, Decimal, str)):
            return float(value)
        raise ValueError(f"not a number: {value!r}")
    if logical_type is LogicalType.TIMESTAMP:
        if isinstance(value, datetime):
            if value.tzinfo is None:
                return value.replace(tzinfo=timezone.utc)
            return value.astimezone(timezone.utc)
        if isinstance(value, int) and not isinstance(value, bool):
            return EPOCH + timedelta(milliseconds=value)
        raise ValueError(f"not a timestamp: {value!r}")
    if logical_type is LogicalType.ARRAY_STRING:
        if isinstance(value, str):
            # Platforms without a native array type encode it as a JSON string.
            value = json.loads(value)
        elif hasattr(value, "tolist"):
            # databricks-sql / pyarrow can hand back numpy arrays.
            value = value.tolist()
        if not isinstance(value, (list, tuple)):
            raise ValueError(f"not an array: {value!r}")
        return [str(item) for item in value if item is not None]
    raise ValueError(f"unsupported logical type {logical_type}")
