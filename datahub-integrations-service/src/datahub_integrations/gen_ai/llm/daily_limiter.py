"""Daily token usage limiter for LLM calls.

Default limit: 20M tokens (~$100/day at $5/1M tokens)
"""

from __future__ import annotations

import os
import threading
from datetime import date, datetime, timezone

from loguru import logger

from datahub_integrations.gen_ai.llm.exceptions import LlmDailyLimitExceededException

# Default: 20M tokens (~$100/day at $5/1M tokens)
DEFAULT_DAILY_TOKEN_LIMIT = 20_000_000


class DailyTokenLimiter:
    """Tracks and enforces daily token usage limits.

    Resets at midnight UTC.
    """

    def __init__(self, limit: int | None = None):
        # Default: 20M tokens (~$100/day at $5/1M tokens)
        self._limit = (
            limit
            if limit is not None
            else int(os.getenv("GENAI_DAILY_TOKEN_LIMIT", DEFAULT_DAILY_TOKEN_LIMIT))
        )
        self._tokens_used_today: int = 0
        self._current_date: date | None = None
        self._lock = threading.Lock()

    def _maybe_reset(self) -> None:
        """Reset counter if date has changed (UTC)."""
        today = datetime.now(timezone.utc).date()
        if self._current_date != today:
            if self._current_date is not None:
                logger.info(
                    "Daily token limit reset",
                    extra={
                        "previous_date": str(self._current_date),
                        "tokens_used_yesterday": self._tokens_used_today,
                    },
                )
            self._tokens_used_today = 0
            self._current_date = today

    def check(self) -> None:
        """Check if limit is already exceeded. Raises LlmDailyLimitExceededException if so.

        Call this BEFORE making an LLM call to fail fast.
        """
        with self._lock:
            self._maybe_reset()
            if self._limit > 0 and self._tokens_used_today >= self._limit:
                raise LlmDailyLimitExceededException(
                    f"Daily token limit ({self._limit:,}) exceeded. "
                    f"Used today: {self._tokens_used_today:,}"
                )

    def record_usage(self, tokens: int) -> None:
        """Record token usage after a successful LLM call."""
        with self._lock:
            self._maybe_reset()
            self._tokens_used_today += tokens
            logger.debug(
                "Daily token usage updated",
                extra={
                    "tokens_added": tokens,
                    "tokens_used_today": self._tokens_used_today,
                    "daily_limit": self._limit,
                },
            )


# Global instance
_global_limiter: DailyTokenLimiter | None = None


def get_daily_token_limiter() -> DailyTokenLimiter:
    """Get the global DailyTokenLimiter instance."""
    global _global_limiter
    if _global_limiter is None:
        _global_limiter = DailyTokenLimiter()
    return _global_limiter
