"""Unit tests for chat_session_formatter.py"""

from unittest.mock import patch

import pytest

from datahub_integrations.chat.chat_session_formatter import format_message
from datahub_integrations.chat.types import ChatType


class TestFormatMessage:
    """Test the format_message function."""

    def test_format_message_datahub_ui_within_limit(self):
        """Test DataHub UI message within limit."""
        text = "This is a short message"
        result = format_message(text, ChatType.DATAHUB_UI)
        assert result == text

    def test_format_message_datahub_ui_exceeds_limit(self):
        """Test DataHub UI message that exceeds limit."""
        long_text = "x" * 10001  # Exceeds 10000 limit
        result = format_message(long_text, ChatType.DATAHUB_UI)
        assert len(result) == 10000
        assert result.endswith("...")

    def test_format_message_slack_within_limit(self):
        """Test Slack message within limit."""
        text = "This is a short message"
        result = format_message(text, ChatType.SLACK)
        assert result == text

    def test_format_message_slack_exceeds_limit(self):
        """Test Slack message that exceeds limit."""
        long_text = "x" * 3000  # Exceeds 2900 limit
        result = format_message(long_text, ChatType.SLACK)
        assert len(result) == 2900
        assert result.endswith("...")

    def test_format_message_teams_within_limit(self):
        """Test Teams message within limit."""
        text = "This is a short message"
        result = format_message(text, ChatType.TEAMS)
        assert result == text

    def test_format_message_teams_exceeds_limit(self):
        """Test Teams message that exceeds limit."""
        long_text = "x" * 5000  # Exceeds 4000 limit
        result = format_message(long_text, ChatType.TEAMS)
        assert len(result) == 4000
        assert result.endswith("...")

    def test_format_message_default_within_limit(self):
        """Test default message within limit."""
        text = "This is a short message"
        result = format_message(text, ChatType.DEFAULT)
        assert result == text

    def test_format_message_default_exceeds_limit(self):
        """Test default message that exceeds limit."""
        long_text = "x" * 3000  # Exceeds 2900 limit
        result = format_message(long_text, ChatType.DEFAULT)
        assert len(result) == 2900
        assert result.endswith("...")

    def test_format_message_unknown_type_falls_back_to_default(self):
        """Test that unknown chat type falls back to default limit."""

        # Create a mock chat type that's not in the limits dict
        class UnknownChatType:
            value = "unknown"

        long_text = "x" * 3000  # Exceeds 2900 default limit
        result = format_message(long_text, UnknownChatType())
        assert len(result) == 2900
        assert result.endswith("...")

    @patch("datahub_integrations.chat.chat_session_formatter.logger")
    def test_format_message_logs_warning_on_truncation(self, mock_logger):
        """Test that truncation logs a warning."""
        long_text = "x" * 3000
        format_message(long_text, ChatType.SLACK)
        mock_logger.warning.assert_called_once()
        assert "Message truncated" in mock_logger.warning.call_args[0][0]
        assert "slack" in mock_logger.warning.call_args[0][0]


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
