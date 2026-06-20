import math
from typing import Any, Dict, List, Optional


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


def is_special_numeric_value(value: Any) -> bool:
    try:
        float_val = float(value)
        return math.isnan(float_val) or math.isinf(float_val)
    except (ValueError, TypeError):
        return False


def calculate_numeric_stats(numeric_values: List[float]) -> Dict[str, Optional[float]]:
    stats: Dict[str, Optional[float]] = {
        "min": None,
        "max": None,
        "mean": None,
        "median": None,
        "stdev": None,
    }

    if not numeric_values:
        return stats

    stats["min"] = min(numeric_values)
    stats["max"] = max(numeric_values)

    sorted_values = sorted(numeric_values)
    stats["median"] = sorted_values[len(sorted_values) // 2]

    # For single values, mean equals the value (no summation risk)
    if len(numeric_values) == 1:
        stats["mean"] = numeric_values[0]
    elif not any(abs(x) > 1e200 for x in numeric_values):
        # Only calculate mean for multiple values if no overflow risk
        mean_val = sum(numeric_values) / len(numeric_values)
        stats["mean"] = mean_val

    if len(numeric_values) == 1:
        stats["stdev"] = 0.0
    elif len(numeric_values) > 1 and not any(abs(x) > 1e200 for x in numeric_values):
        stats["stdev"] = calculate_standard_deviation(numeric_values)

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
