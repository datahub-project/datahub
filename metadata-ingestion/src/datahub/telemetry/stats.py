import math
from typing import Any, Dict, List, TypeVar, Union

from typing_extensions import Protocol


class SupportsLT(Protocol):
    def __lt__(self, __other: Any) -> Any:
        ...


_SupportsComparisonT = TypeVar("_SupportsComparisonT", bound=SupportsLT)


def calculate_percentiles(
    data: List[_SupportsComparisonT], percentiles: List[int]
) -> Dict[int, _SupportsComparisonT]:
    size = len(data)

    if size == 0:
        return {}

    data_sorted = sorted(data)

    percentile_indices = [int(math.ceil(size * p / 100)) - 1 for p in percentiles]
    percentile_indices = [
        min(i, size - 1) for i in percentile_indices
    ]  # in case of rounding errors

    return {p: data_sorted[i] for p, i in zip(percentiles, percentile_indices)}


def discretize(statistic: Union[float, int]) -> int:
    """Convert to nearest power of 2 to discretize"""
    if statistic == 0:
        return 0
    else:
        return 2 ** int(math.log2(statistic))
