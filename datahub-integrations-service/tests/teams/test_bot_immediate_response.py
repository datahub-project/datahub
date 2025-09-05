"""
Test immediate response handling for Bot Framework Teams integration.
"""

from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from botbuilder.schema import Activity, ChannelAccount

from datahub_integrations.teams.bot import DataHubTeamsBot


class TestImmediateResponse:
    """Test immediate response to button clicks to prevent Teams timeout."""

    @pytest.fixture
    def bot(self) -> DataHubTeamsBot:
        """Create a DataHubTeamsBot instance for testing."""
        return DataHubTeamsBot()

    @pytest.fixture
    def turn_context(self) -> MagicMock:
        """Create a mock TurnContext with proper activity mock."""
        context = MagicMock()
        context.send_activity = AsyncMock()

        # Mock the activity that send_activity returns
        mock_activity = MagicMock()
        mock_activity.id = "thinking_message_123"
        context.send_activity.return_value = mock_activity

        context.update_activity = AsyncMock()
        return context

    @pytest.mark.asyncio
    async def test_immediate_acknowledgment_sent(
        self, bot: DataHubTeamsBot, turn_context: MagicMock
    ) -> None:
        """Test that immediate acknowledgment is sent to prevent Teams timeout."""
        # Mock button click activity
        turn_context.activity = Activity(
            type="message",
            text="",
            value={
                "action": "followup_question",
                "question": "What datasets are available?",
            },
            from_property=ChannelAccount(id="user123", name="Test User"),
            conversation=MagicMock(id="conv123"),
            id="activity123",
        )

        # Call the action submission handler
        await bot._handle_action_submission(turn_context)

        # Verify immediate response was sent
        turn_context.send_activity.assert_called_once()
        immediate_response = turn_context.send_activity.call_args[0][0]
        assert "🤔 Let me think about that..." in immediate_response.text

    @pytest.mark.asyncio
    async def test_async_task_created_for_processing(
        self, bot: DataHubTeamsBot, turn_context: MagicMock
    ) -> None:
        """Test that async task is created for background processing."""
        turn_context.activity = Activity(
            type="message",
            text="",
            value={
                "action": "followup_question",
                "question": "Show me the data lineage",
            },
            from_property=ChannelAccount(id="user123", name="Test User"),
        )

        # Mock asyncio.create_task to verify it's called
        with patch("asyncio.create_task") as mock_create_task:
            await bot._handle_action_submission(turn_context)

            # Verify async task was created for background processing
            mock_create_task.assert_called_once()
            task_args = mock_create_task.call_args[0][0]
            # Should be a coroutine for _process_followup_question_async
            assert hasattr(task_args, "__await__")

    @pytest.mark.asyncio
    async def test_handler_returns_immediately_after_acknowledgment(
        self, bot: DataHubTeamsBot, turn_context: MagicMock
    ) -> None:
        """Test that the handler returns immediately after sending acknowledgment."""
        turn_context.activity = Activity(
            type="message",
            text="",
            value={"action": "followup_question", "question": "Quick test question"},
        )

        # Mock create_task to ensure it doesn't block
        with patch("asyncio.create_task") as mock_create_task:
            # This should return quickly without waiting for AI processing
            await bot._handle_action_submission(turn_context)

            # Verify immediate response was sent
            assert turn_context.send_activity.called

            # Verify background task was created
            assert mock_create_task.called

    @pytest.mark.asyncio
    async def test_error_handling_still_sends_immediate_response(
        self, bot: DataHubTeamsBot, turn_context: MagicMock
    ) -> None:
        """Test that errors in setup still result in immediate response."""
        turn_context.activity = Activity(
            type="message",
            text="",
            value={"action": "followup_question", "question": "Test question"},
        )

        # Mock send_activity to raise exception after first call
        call_count = 0

        def side_effect(*args: Any, **kwargs: Any) -> MagicMock:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                # First call (thinking message) succeeds
                mock_activity = MagicMock()
                mock_activity.id = "thinking_123"
                return mock_activity
            else:
                # Subsequent calls fail
                raise Exception("Teams API error")

        turn_context.send_activity.side_effect = side_effect

        # Should not raise exception and should still send immediate response
        await bot._handle_action_submission(turn_context)

        # Verify at least one call was made (the immediate acknowledgment)
        assert turn_context.send_activity.call_count >= 1

    @pytest.mark.asyncio
    async def test_unknown_action_type_handles_gracefully(
        self, bot: DataHubTeamsBot, turn_context: MagicMock
    ) -> None:
        """Test that unknown action types don't cause errors."""
        turn_context.activity = Activity(
            type="message", value={"action": "unknown_action_type", "data": "some data"}
        )

        # Should handle gracefully without sending any response
        await bot._handle_action_submission(turn_context)

        # No immediate response should be sent for unknown actions
        turn_context.send_activity.assert_not_called()

    @pytest.mark.asyncio
    async def test_missing_question_handles_gracefully(
        self, bot: DataHubTeamsBot, turn_context: MagicMock
    ) -> None:
        """Test handling of followup_question action with missing question."""
        turn_context.activity = Activity(
            type="message",
            value={
                "action": "followup_question"
                # Missing "question" field
            },
        )

        # Should handle gracefully without processing
        await bot._handle_action_submission(turn_context)

        # No response should be sent if question is missing
        turn_context.send_activity.assert_not_called()
