from enum import Enum
from typing import Optional


class Cardinality(Enum):
    NONE = 0
    ONE = 1
    TWO = 2
    VERY_FEW = 3
    FEW = 4
    MANY = 5
    VERY_MANY = 6
    UNIQUE = 7


def convert_to_cardinality(
    unique_count: Optional[int], pct_unique: Optional[float]
) -> Optional[Cardinality]:
    """
    Resolve the cardinality of a column based on the unique count and the percentage of unique values.

    Logic adopted from Great Expectations.
    See https://github.com/great-expectations/great_expectations/blob/develop/great_expectations/profile/base.py

    Args:
        unique_count: raw number of unique values
        pct_unique: raw proportion of unique values

    Returns:
        Optional[Cardinality]: resolved cardinality
    """

    if unique_count is None:
        return Cardinality.NONE

    if pct_unique == 1.0:
        cardinality = Cardinality.UNIQUE
    elif unique_count == 1:
        cardinality = Cardinality.ONE
    elif unique_count == 2:
        cardinality = Cardinality.TWO
    elif 0 < unique_count < 20:
        cardinality = Cardinality.VERY_FEW
    elif 0 < unique_count < 60:
        cardinality = Cardinality.FEW
    elif unique_count is None or unique_count == 0 or pct_unique is None:
        cardinality = Cardinality.NONE
    elif pct_unique > 0.1:
        cardinality = Cardinality.VERY_MANY
    else:
        cardinality = Cardinality.MANY
    return cardinality
