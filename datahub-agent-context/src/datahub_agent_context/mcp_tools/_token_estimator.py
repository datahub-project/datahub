"""Token count estimation utilities for MCP responses."""

import logging
from typing import Union

logger = logging.getLogger(__name__)


class TokenCountEstimator:
    """Fast token estimation for MCP response budget management.

    Uses character-based heuristics instead of actual tokenization for performance.
    Accuracy is sufficient given the 90% budget buffer used in practice.
    """

    @staticmethod
    def estimate_dict_tokens(
        obj: Union[dict, list, str, int, float, bool, None],
    ) -> int:
        """Fast approximation of token count for dict/list structures.

        Recursively walks structure counting characters. Much faster than json.dumps + estimate_tokens.

        IMPORTANT: Assumes no circular references in the structure.
        Protected against infinite recursion with MAX_DEPTH=100.

        Args:
            obj: Dict, list, or primitive value (must not contain circular references)

        Returns:
            Approximate token count
        """
        MAX_DEPTH = 100

        def _count_chars(item, depth: int = 0) -> int:
            if depth > MAX_DEPTH:
                logger.error(
                    f"Max depth {MAX_DEPTH} exceeded in structure, stopping recursion"
                )
                return 0

            if item is None:
                return 4  # "null"
            elif isinstance(item, bool):
                return 5  # "true" or "false"
            elif isinstance(item, str):
                # Account for:
                # - Quotes around string values: "value" → +6
                # - Escape characters (\n, \", \\, etc.) → +10% of length
                # Structural chars weighted heavier as they often tokenize separately
                base_length = len(item)
                escape_overhead = int(base_length * 0.1)
                return base_length + 6 + escape_overhead
            elif isinstance(item, (int, float)):
                return 6  # Average number length
            elif isinstance(item, list):
                return sum(_count_chars(elem, depth + 1) for elem in item) + len(item)
            elif isinstance(item, dict):
                total = 0
                for key, value in item.items():
                    # Account for: "key": value, → 2 quotes + colon + space + comma
                    # Structural chars weighted heavier (often separate tokens)
                    total += len(str(key)) + 9
                    total += _count_chars(value, depth + 1)
                return total + len(item)  # Additional padding for structure
            else:
                return 10  # Fallback for other types

        chars = _count_chars(obj, depth=0)
        # Use same formula as estimate_tokens for consistency
        return int(1.3 * chars / 4)
