"""Tests for LLM utility functions."""

import pytest

from datahub_integrations.gen_ai.llm.utils import is_verbose_llm_logging_enabled


class TestIsVerboseLlmLoggingEnabled:
    """Tests for is_verbose_llm_logging_enabled function."""

    def test_enabled_with_true(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Test that 'true' enables verbose logging."""
        monkeypatch.setenv("LLM_VERBOSE_LOGGING", "true")
        assert is_verbose_llm_logging_enabled() is True

    def test_enabled_with_true_uppercase(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Test that 'TRUE' enables verbose logging (case-insensitive)."""
        monkeypatch.setenv("LLM_VERBOSE_LOGGING", "TRUE")
        assert is_verbose_llm_logging_enabled() is True

    def test_enabled_with_1(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Test that '1' enables verbose logging."""
        monkeypatch.setenv("LLM_VERBOSE_LOGGING", "1")
        assert is_verbose_llm_logging_enabled() is True

    def test_enabled_with_yes(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Test that 'yes' enables verbose logging."""
        monkeypatch.setenv("LLM_VERBOSE_LOGGING", "yes")
        assert is_verbose_llm_logging_enabled() is True

    def test_enabled_with_yes_uppercase(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Test that 'YES' enables verbose logging (case-insensitive)."""
        monkeypatch.setenv("LLM_VERBOSE_LOGGING", "YES")
        assert is_verbose_llm_logging_enabled() is True

    def test_disabled_with_false(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Test that 'false' disables verbose logging."""
        monkeypatch.setenv("LLM_VERBOSE_LOGGING", "false")
        assert is_verbose_llm_logging_enabled() is False

    def test_disabled_with_0(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Test that '0' disables verbose logging."""
        monkeypatch.setenv("LLM_VERBOSE_LOGGING", "0")
        assert is_verbose_llm_logging_enabled() is False

    def test_disabled_with_no(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Test that 'no' disables verbose logging."""
        monkeypatch.setenv("LLM_VERBOSE_LOGGING", "no")
        assert is_verbose_llm_logging_enabled() is False

    def test_disabled_with_invalid_value(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Test that invalid values disable verbose logging."""
        monkeypatch.setenv("LLM_VERBOSE_LOGGING", "invalid")
        assert is_verbose_llm_logging_enabled() is False

    def test_disabled_when_not_set(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Test that verbose logging is disabled when environment variable is not set."""
        monkeypatch.delenv("LLM_VERBOSE_LOGGING", raising=False)
        assert is_verbose_llm_logging_enabled() is False

    def test_disabled_with_empty_string(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Test that empty string disables verbose logging."""
        monkeypatch.setenv("LLM_VERBOSE_LOGGING", "")
        assert is_verbose_llm_logging_enabled() is False
