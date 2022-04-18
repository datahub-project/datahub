import math
from typing import Any, Dict, List, TypeVar

from typing_extensions import Protocol


class SupportsLT(Protocol):
    def __lt__(self, __other: Any) -> Any:
        ...


SupportsComparisonT = TypeVar("SupportsComparisonT", bound=SupportsLT)  # noqa: Y001


def calculate_percentiles(
    data: List[SupportsComparisonT], percentiles: List[int]
) -> Dict[int, SupportsComparisonT]:
    size = len(data)

    if size == 0:
        return {}

    data_sorted = sorted(data)

    percentile_indices = [int(math.ceil(size * p / 100)) - 1 for p in percentiles]
    percentile_indices = [
        min(i, size - 1) for i in percentile_indices
    ]  # in case of rounding errors

    values = {p: data_sorted[i] for p, i in zip(percentiles, percentile_indices)}

    return values
