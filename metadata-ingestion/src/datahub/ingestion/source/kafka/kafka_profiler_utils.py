import math
import statistics
from dataclasses import dataclass
from typing import Any, List, Optional

# Per-value gate: drop values beyond int64 max so squaring (stdev) and summation
# stay within float64 range.
VALUE_OVERFLOW_THRESHOLD = 9.223372036854775e18

# Aggregation headroom for callers that pass raw floats. float64 overflows near
# 1.8e308, so beyond this we switch to overflow-safe math.
AGGREGATION_OVERFLOW_THRESHOLD = 1e200


@dataclass
class NumericStats:
    min: Optional[float] = None
    max: Optional[float] = None
    mean: Optional[float] = None
    median: Optional[float] = None
    stdev: Optional[float] = None


def calculate_standard_deviation(values: List[float]) -> Optional[float]:
    if len(values) < 2:
        return None

    try:
        mean = sum(values) / len(values)
        variance = sum((x - mean) ** 2 for x in values) / (len(values) - 1)

        if variance >= 0:
            return math.sqrt(variance)
        return None
    except (OverflowError, ValueError):
        return None


def _overflow_safe_median(numeric_values: List[float]) -> float:
    # a / 2 + b / 2 avoids the inf that (a + b) / 2 hits for near-float-max inputs.
    sorted_values = sorted(numeric_values)
    n = len(sorted_values)
    mid = n // 2
    if n % 2 == 1:
        return sorted_values[mid]
    return sorted_values[mid - 1] / 2 + sorted_values[mid] / 2


def calculate_numeric_stats(numeric_values: List[float]) -> NumericStats:
    stats = NumericStats()

    if not numeric_values:
        return stats

    stats.min = min(numeric_values)
    stats.max = max(numeric_values)

    has_overflow_risk = any(
        abs(x) > AGGREGATION_OVERFLOW_THRESHOLD for x in numeric_values
    )

    # statistics.median is correct for even-length inputs but overflows near
    # float max, so fall back to the overflow-safe variant when at risk.
    stats.median = (
        _overflow_safe_median(numeric_values)
        if has_overflow_risk
        else statistics.median(numeric_values)
    )

    if len(numeric_values) == 1:
        # A lone value has no summation risk, so compute it even when at overflow risk.
        stats.mean = numeric_values[0]
        stats.stdev = 0.0
    elif not has_overflow_risk:
        stats.mean = sum(numeric_values) / len(numeric_values)
        stats.stdev = calculate_standard_deviation(numeric_values)

    return stats


def filter_numeric_values(
    values: List[Any], exclude_special: bool = True
) -> List[float]:
    numeric_values: List[float] = []

    for value in values:
        if value is None:
            continue

        try:
            float_val = float(value)

            if exclude_special and (math.isnan(float_val) or math.isinf(float_val)):
                continue

            numeric_values.append(float_val)

        except (ValueError, TypeError):
            continue

    return numeric_values
