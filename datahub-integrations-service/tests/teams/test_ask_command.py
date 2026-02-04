"""Tests for Teams ask command handling."""

from unittest.mock import Mock, patch

import pytest

from datahub_integrations.gen_ai.llm.exceptions import LlmDailyLimitExceededException
from datahub_integrations.teams.command.ask import handle_ask_command_teams


@pytest.mark.asyncio
async def test_handle_ask_command_daily_limit_exceeded() -> None:
    """Test that LlmDailyLimitExceededException returns a user-friendly message."""
    mock_graph = Mock()

    # Mock asyncio.get_event_loop().run_in_executor to raise the exception
    async def mock_run_in_executor(executor, func):
        raise LlmDailyLimitExceededException(
            "Daily token limit (20,000,000) exceeded. Used today: 20,123,456"
        )

    mock_loop = Mock()
    mock_loop.run_in_executor = mock_run_in_executor

    with patch("asyncio.get_event_loop", return_value=mock_loop):
        result = await handle_ask_command_teams(
            graph=mock_graph,
            question="What tables contain customer data?",
            user_urn="urn:li:corpuser:test",
        )

        # Should return a user-friendly message
        assert result["type"] == "message"
        assert (
            result["text"]
            == "AI features have reached their daily usage limit. Please try again later."
        )


@pytest.mark.asyncio
async def test_handle_ask_command_generic_error_shows_details() -> None:
    """Test that generic errors still show the error details (for debugging)."""
    mock_graph = Mock()

    # Mock asyncio.get_event_loop().run_in_executor to raise a generic exception
    async def mock_run_in_executor(executor, func):
        raise RuntimeError("Something went wrong")

    mock_loop = Mock()
    mock_loop.run_in_executor = mock_run_in_executor

    with patch("asyncio.get_event_loop", return_value=mock_loop):
        result = await handle_ask_command_teams(
            graph=mock_graph,
            question="What tables contain customer data?",
            user_urn="urn:li:corpuser:test",
        )

        # Generic errors should still include the error message for debugging
        assert result["type"] == "message"
        assert "Something went wrong" in result["text"]
        assert "Please try again or contact support" in result["text"]


@pytest.mark.asyncio
async def test_handle_ask_command_empty_question() -> None:
    """Test that empty questions return a helpful message."""
    mock_graph = Mock()

    result = await handle_ask_command_teams(
        graph=mock_graph,
        question="   ",  # Empty/whitespace only
        user_urn="urn:li:corpuser:test",
    )

    assert result["type"] == "message"
    assert "Please provide a question" in result["text"]
