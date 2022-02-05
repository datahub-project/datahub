import math
from typing import Any, List

def calculate_percentiles(data: List[Any], percentiles: List[int]):
    size = len(data)
    
    if size == 0:
        return {p: None for p in percentiles}
    
    data_sorted = sorted(data)
    
    percentile_indices = [int(math.ceil(size * p / 100)) - 1 for p in percentiles]
    percentile_indices = [min(i, size - 1) for i in percentile_indices] # in case of rounding errors
    
    values = {p: data_sorted[i] for p,i in zip(percentiles, percentile_indices)}
    
    return values
    
calculate_percentiles([1], [50,75,90,95,99])