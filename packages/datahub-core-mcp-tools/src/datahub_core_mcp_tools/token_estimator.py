"""Fast token estimates for keeping tool responses inside a budget.

A character heuristic rather than a tokenizer: it is fast, needs no model-specific
dependency, and is accurate enough for budgets that already keep a safety margin.
"""

from loguru import logger
from pydantic import JsonValue

_MAX_DEPTH = 100


def _count_chars(item: object, depth: int = 0) -> int:
    if depth > _MAX_DEPTH:
        logger.error(f"Structure deeper than {_MAX_DEPTH} levels; not counted further")
        return 0
    if item is None:
        return 4
    if isinstance(item, bool):
        return 5
    if isinstance(item, str):
        # Quotes, plus about 10% for escape characters.
        return len(item) + 6 + int(len(item) * 0.1)
    if isinstance(item, (int, float)):
        return 6
    if isinstance(item, list):
        return sum(_count_chars(element, depth + 1) for element in item) + len(item)
    if isinstance(item, dict):
        # Each key costs its quotes, colon, space and comma, which often tokenize
        # separately, so they are weighted heavier than plain text.
        return sum(
            len(str(key)) + 9 + _count_chars(value, depth + 1)
            for key, value in item.items()
        ) + len(item)
    return 10


def estimate_dict_tokens(obj: JsonValue) -> int:
    """About how many tokens ``obj`` takes once serialized as JSON."""
    return int(1.3 * _count_chars(obj) / 4)
