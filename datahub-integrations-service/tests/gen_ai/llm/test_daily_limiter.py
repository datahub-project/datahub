"""Tests for DailyTokenLimiter.

Tests the daily token usage limit functionality including:
- Basic initialization and configuration
- Check and record usage methods
- Daily reset logic
- Exception handling when limit exceeded
"""

from datetime import date, datetime, timezone
from unittest.mock import patch

import pytest

from datahub_integrations.gen_ai.llm.daily_limiter import (
    DEFAULT_DAILY_TOKEN_LIMIT,
    DailyTokenLimiter,
    get_daily_token_limiter,
)
from datahub_integrations.gen_ai.llm.exceptions import LlmDailyLimitExceededException


class TestDailyTokenLimiter:
    """Tests for DailyTokenLimiter class."""

    def test_initialization_with_default_limit(self) -> None:
        """Test that limiter initializes with default 20M token limit."""
        limiter = DailyTokenLimiter()
        assert limiter._limit == DEFAULT_DAILY_TOKEN_LIMIT
        assert limiter._limit == 20_000_000

    def test_initialization_with_custom_limit(self) -> None:
        """Test that limiter accepts custom limit."""
        limiter = DailyTokenLimiter(limit=1_000_000)
        assert limiter._limit == 1_000_000

    def test_initialization_with_zero_limit_disables_limiting(self) -> None:
        """Test that limit=0 effectively disables the limiter."""
        limiter = DailyTokenLimiter(limit=0)
        assert limiter._limit == 0
        # Should not raise even with high usage
        limiter.record_usage(100_000_000)
        limiter.check()  # Should not raise

    @patch.dict("os.environ", {"GENAI_DAILY_TOKEN_LIMIT": "5000000"})
    def test_initialization_from_env_var(self) -> None:
        """Test that limiter reads limit from environment variable."""
        limiter = DailyTokenLimiter()
        assert limiter._limit == 5_000_000

    def test_check_passes_when_under_limit(self) -> None:
        """Test that check() passes when usage is under limit."""
        limiter = DailyTokenLimiter(limit=1000)
        limiter.record_usage(500)
        limiter.check()  # Should not raise

    def test_check_passes_when_exactly_under_limit(self) -> None:
        """Test that check() passes when usage is exactly one below limit."""
        limiter = DailyTokenLimiter(limit=1000)
        limiter.record_usage(999)
        limiter.check()  # Should not raise - we're at 999, limit is 1000

    def test_check_raises_when_at_limit(self) -> None:
        """Test that check() raises when usage equals limit."""
        limiter = DailyTokenLimiter(limit=1000)
        limiter.record_usage(1000)

        with pytest.raises(LlmDailyLimitExceededException) as exc_info:
            limiter.check()

        assert "1,000" in str(exc_info.value)  # Limit formatted with commas
        assert "exceeded" in str(exc_info.value).lower()

    def test_check_raises_when_over_limit(self) -> None:
        """Test that check() raises when usage exceeds limit."""
        limiter = DailyTokenLimiter(limit=1000)
        limiter.record_usage(1500)

        with pytest.raises(LlmDailyLimitExceededException) as exc_info:
            limiter.check()

        assert "1,000" in str(exc_info.value)  # Limit
        assert "1,500" in str(exc_info.value)  # Usage

    def test_record_usage_accumulates(self) -> None:
        """Test that record_usage() accumulates token counts."""
        limiter = DailyTokenLimiter(limit=10000)
        limiter.record_usage(100)
        limiter.record_usage(200)
        limiter.record_usage(300)

        assert limiter._tokens_used_today == 600

    def test_daily_reset_on_date_change(self) -> None:
        """Test that counters reset when date changes."""
        limiter = DailyTokenLimiter(limit=1000)

        # Simulate usage on one day
        with patch(
            "datahub_integrations.gen_ai.llm.daily_limiter.datetime"
        ) as mock_datetime:
            mock_datetime.now.return_value = datetime(
                2025, 1, 15, 12, 0, 0, tzinfo=timezone.utc
            )
            limiter.record_usage(800)
            assert limiter._tokens_used_today == 800

        # Simulate next day
        with patch(
            "datahub_integrations.gen_ai.llm.daily_limiter.datetime"
        ) as mock_datetime:
            mock_datetime.now.return_value = datetime(
                2025, 1, 16, 12, 0, 0, tzinfo=timezone.utc
            )
            limiter.check()  # This should trigger reset
            assert limiter._tokens_used_today == 0
            assert limiter._current_date == date(2025, 1, 16)

    def test_check_then_record_workflow(self) -> None:
        """Test typical workflow: check() before call, record_usage() after."""
        limiter = DailyTokenLimiter(limit=1000)

        # First call
        limiter.check()  # Should pass
        limiter.record_usage(300)

        # Second call
        limiter.check()  # Should pass
        limiter.record_usage(400)

        # Third call
        limiter.check()  # Should pass (700 < 1000)
        limiter.record_usage(400)  # Now at 1100

        # Fourth call should fail check
        with pytest.raises(LlmDailyLimitExceededException):
            limiter.check()

    def test_exception_message_format(self) -> None:
        """Test that exception message is informative."""
        limiter = DailyTokenLimiter(limit=20_000_000)
        limiter.record_usage(25_000_000)

        with pytest.raises(LlmDailyLimitExceededException) as exc_info:
            limiter.check()

        error_msg = str(exc_info.value)
        assert "20,000,000" in error_msg  # Limit with commas
        assert "25,000,000" in error_msg  # Usage with commas
        assert "exceeded" in error_msg.lower()


class TestGetDailyTokenLimiter:
    """Tests for get_daily_token_limiter() factory function."""

    def test_returns_singleton_instance(self) -> None:
        """Test that get_daily_token_limiter returns same instance."""
        # Reset global for test isolation
        import datahub_integrations.gen_ai.llm.daily_limiter as module

        module._global_limiter = None

        limiter1 = get_daily_token_limiter()
        limiter2 = get_daily_token_limiter()

        assert limiter1 is limiter2

    def test_creates_limiter_with_default_config(self) -> None:
        """Test that factory creates limiter with default configuration."""
        # Reset global for test isolation
        import datahub_integrations.gen_ai.llm.daily_limiter as module

        module._global_limiter = None

        limiter = get_daily_token_limiter()
        assert limiter._limit == DEFAULT_DAILY_TOKEN_LIMIT
