import json
import math
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Mapping, Tuple

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
        if isinstance(value, (float, Decimal)):
            # int(inf)/int(Decimal('NaN')) raise OverflowError/InvalidOperation
            # instead of ValueError, which would escape the row-skip in _parse.
            if not math.isfinite(float(value)):
                raise ValueError(f"not an integer: {value!r}")
            if value == int(value):
                return int(value)
        if isinstance(value, str):
            return int(value.strip())
        raise ValueError(f"not an integer: {value!r}")
    if logical_type is LogicalType.FLOAT64:
        if isinstance(value, bool):
            raise ValueError(f"not a number: {value!r}")
        if isinstance(value, (int, float, Decimal, str)):
            number = float(value)
            # NaN/inf serialize to bare JSON tokens the sink rejects; failing here
            # skips the row instead of checkpointing a result that never landed.
            if not math.isfinite(number):
                raise ValueError(f"not a finite number: {value!r}")
            return number
        raise ValueError(f"not a number: {value!r}")
    if logical_type is LogicalType.TIMESTAMP:
        if isinstance(value, datetime):
            if value.tzinfo is None:
                return value.replace(tzinfo=timezone.utc)
            return value.astimezone(timezone.utc)
        if isinstance(value, int) and not isinstance(value, bool):
            try:
                return EPOCH + timedelta(milliseconds=value)
            except OverflowError as e:
                raise ValueError(f"timestamp out of range: {value!r}") from e
        raise ValueError(f"not a timestamp: {value!r}")
    if logical_type is LogicalType.ARRAY_STRING:
        if isinstance(value, str):
            # Platforms without a native array type encode it as a JSON string.
            value = json.loads(value)
        if not isinstance(value, (list, tuple)):
            raise ValueError(f"not an array: {value!r}")
        for item in value:
            if item is None:
                raise ValueError("array elements must not be null")
        return [str(item) for item in value]
    raise ValueError(f"unsupported logical type {logical_type}")


@dataclass(frozen=True)
class TypeProfile:
    """Which physical column types a platform may use for each logical type.

    Widening is allowed (e.g. INT for INT64); lossy types are not listed.
    Patterns are full-matched against the lower-cased type with whitespace removed.
    """

    name: str
    accepted: Mapping[LogicalType, Tuple[str, ...]]

    def accepts(self, logical_type: LogicalType, physical_type: str) -> bool:
        normalized = re.sub(r"\s+", "", physical_type.lower())
        return any(
            re.fullmatch(pattern, normalized)
            for pattern in self.accepted.get(logical_type, ())
        )


_DATABRICKS_INTS: Tuple[str, ...] = ("bigint", "int", "smallint", "tinyint")

DATABRICKS_TYPE_PROFILE = TypeProfile(
    name="databricks",
    accepted={
        LogicalType.STRING: ("string", r"varchar\(\d+\)", r"char\(\d+\)"),
        LogicalType.BOOLEAN: ("boolean",),
        LogicalType.INT64: _DATABRICKS_INTS,
        LogicalType.FLOAT64: ("double", "float", r"decimal(\(\d+,\d+\))?")
        + _DATABRICKS_INTS,
        # timestamp_ntz is deliberately absent: without a time zone, executed_at
        # cannot be compared to a UTC watermark.
        LogicalType.TIMESTAMP: ("timestamp",),
        LogicalType.ARRAY_STRING: ("array<string>",),
    },
)
