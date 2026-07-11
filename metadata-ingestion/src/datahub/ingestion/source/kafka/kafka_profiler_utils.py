import math
import statistics
from dataclasses import dataclass
from typing import Any, List, Optional

# Per-value gate applied before aggregation: values beyond int64 max are dropped
# so that squaring (for stdev) and summation stay comfortably within float64 range.
VALUE_OVERFLOW_THRESHOLD = 9.223372036854775e18

# Headroom used by the median/mean utility when it is called directly with
# arbitrary floats (not pre-filtered by VALUE_OVERFLOW_THRESHOLD). float64
# overflows near 1.8e308, so beyond this we switch to overflow-safe math.
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
    # statistics.median averages the two middle values as (a + b) / 2, which
    # overflows to inf for near-float-max inputs. a / 2 + b / 2 is mathematically
    # equivalent and cannot overflow since each half is finite.
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

    # statistics.median averages the two middle values for even-length inputs
    # (e.g. [1, 2, 3, 4] -> 2.5), unlike sorted[len // 2] which returns 3. Fall
    # back to an overflow-safe average when values are near float max.
    stats.median = (
        _overflow_safe_median(numeric_values)
        if has_overflow_risk
        else statistics.median(numeric_values)
    )

    if len(numeric_values) == 1:
        # For a single value, mean equals the value (no summation risk).
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
