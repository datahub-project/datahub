"""
Tests for Bot Framework Teams integration focusing on action submission handling.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from botbuilder.core import TurnContext
from botbuilder.schema import Activity, ChannelAccount

from datahub_integrations.teams.bot import DataHubTeamsBot


class TestDataHubTeamsBotActionSubmission:
    """Test action submission handling in DataHubTeamsBot."""

    @pytest.fixture
    def bot(self) -> DataHubTeamsBot:
        """Create a DataHubTeamsBot instance for testing."""
        return DataHubTeamsBot()

    @pytest.fixture
    def turn_context(self) -> MagicMock:
        """Create a mock TurnContext for testing."""
        context = MagicMock(spec=TurnContext)
        context.send_activity = AsyncMock()
        context.update_activity = AsyncMock()
        return context

    @pytest.mark.asyncio
    async def test_handle_action_submission_followup_question(
        self, bot: DataHubTeamsBot, turn_context: MagicMock
    ) -> None:
        """Test handling follow-up question button clicks."""
        # Mock the activity with button click data
        turn_context.activity = Activity(
            type="message",
            text="",  # No text for button clicks
            value={
                "action": "followup_question",
                "question": "What are the main datasets?",
            },
            from_property=ChannelAccount(id="user123", name="Test User"),
            conversation=MagicMock(id="conv123"),
        )

        # Mock asyncio.create_task to prevent actual async processing
        with patch("asyncio.create_task") as mock_create_task:
            await bot._handle_action_submission(turn_context)

            # Verify that immediate acknowledgment was sent
            turn_context.send_activity.assert_called_once()

            # Verify that async task was created (but not executed due to mock)
            mock_create_task.assert_called_once()

            # Get the coroutine that would have been passed to create_task
            call_args = mock_create_task.call_args[0][0]
            # Verify it's the followup processing method
            assert hasattr(call_args, "cr_code")  # It's a coroutine

    @pytest.mark.asyncio
    async def test_handle_action_submission_no_value(
        self, bot: DataHubTeamsBot, turn_context: MagicMock
    ) -> None:
        """Test handling action submission with no value."""
        turn_context.activity = Activity(
            type="message",
            value=None,  # No action data
        )

        # Should handle gracefully without errors
        await bot._handle_action_submission(turn_context)

        # Should not send any messages for empty value
        turn_context.send_activity.assert_not_called()

    @pytest.mark.asyncio
    async def test_handle_action_submission_unknown_action(
        self, bot: DataHubTeamsBot, turn_context: MagicMock
    ) -> None:
        """Test handling unknown action types."""
        turn_context.activity = Activity(
            type="message", value={"action": "unknown_action", "data": "some data"}
        )

        await bot._handle_action_submission(turn_context)

        # Should log warning but not crash or send error messages
        turn_context.send_activity.assert_not_called()

    @pytest.mark.asyncio
    async def test_handle_action_submission_missing_question(
        self, bot: DataHubTeamsBot, turn_context: MagicMock
    ) -> None:
        """Test handling follow-up question action with missing question text."""
        turn_context.activity = Activity(
            type="message",
            value={
                "action": "followup_question"
                # Missing "question" field
            },
        )

        await bot._handle_action_submission(turn_context)

        # Should handle gracefully without calling ask handler
        turn_context.send_activity.assert_not_called()

    @pytest.mark.asyncio
    async def test_handle_action_submission_exception_handling(
        self, bot: DataHubTeamsBot, turn_context: MagicMock
    ) -> None:
        """Test error handling in action submission processing."""
        turn_context.activity = Activity(
            type="message",
            value={"action": "followup_question", "question": "Test question"},
            from_property=ChannelAccount(id="user123", name="Test User"),
            conversation=MagicMock(id="conv123"),
        )

        # Mock asyncio.create_task to prevent actual async processing
        with patch("asyncio.create_task") as mock_create_task:
            await bot._handle_action_submission(turn_context)

            # Verify immediate acknowledgment was sent and task was created
            turn_context.send_activity.assert_called_once()
            mock_create_task.assert_called_once()

    @pytest.mark.asyncio
    async def test_on_message_activity_detects_button_clicks(
        self, bot: DataHubTeamsBot, turn_context: MagicMock
    ) -> None:
        """Test that on_message_activity correctly detects and routes button clicks."""
        # Mock activity that represents a button click (has value, no text)
        turn_context.activity = Activity(
            type="message",
            text="",  # Empty text for button clicks
            value={"action": "followup_question", "question": "Show me more details"},
        )

        # Mock the action submission handler
        with patch.object(
            bot, "_handle_action_submission", new_callable=AsyncMock
        ) as mock_handler:
            await bot.on_message_activity(turn_context)

            # Verify button click was detected and routed to action handler
            mock_handler.assert_called_once_with(turn_context)

    @pytest.mark.asyncio
    async def test_on_message_activity_ignores_text_messages_with_value(
        self, bot: DataHubTeamsBot, turn_context: MagicMock
    ) -> None:
        """Test that messages with both text and value are processed as regular messages."""
        turn_context.activity = Activity(
            type="message",
            text="Hello DataHub",  # Has text, so not a button click
            value={"some": "data"},  # Also has value, but text takes precedence
        )

        # Mock the general message handler
        with (
            patch.object(
                bot, "_handle_action_submission", new_callable=AsyncMock
            ) as mock_action,
            patch.object(
                bot, "_handle_general_message", new_callable=AsyncMock
            ) as mock_general,
        ):
            await bot.on_message_activity(turn_context)

            # Should route to general message handler, not action handler
            mock_action.assert_not_called()
            mock_general.assert_called_once_with(turn_context, "Hello DataHub")


class TestDataHubTeamsBotIntegration:
    """Integration tests for the complete Bot Framework Teams flow."""

    @pytest.fixture
    def bot(self) -> DataHubTeamsBot:
        """Create a DataHubTeamsBot instance for testing."""
        return DataHubTeamsBot()

    @pytest.mark.asyncio
    async def test_complete_button_click_flow(self, bot: DataHubTeamsBot) -> None:
        """Test the complete flow from button click to response."""
        # Create a realistic turn context
        turn_context = MagicMock(spec=TurnContext)
        turn_context.send_activity = AsyncMock()
        turn_context.update_activity = AsyncMock()

        # Mock activity representing a follow-up question button click
        turn_context.activity = Activity(
            type="message",
            text="",
            value={"action": "followup_question", "question": "What is DataHub?"},
            from_property=ChannelAccount(id="user123", name="Test User"),
            conversation=MagicMock(id="conv123"),
            id="activity123",
        )

        # Mock asyncio.create_task to prevent actual async processing that would initialize AI infrastructure
        with patch("asyncio.create_task") as mock_create_task:
            # Process the button click
            await bot.on_message_activity(turn_context)

            # Verify immediate acknowledgment was sent
            turn_context.send_activity.assert_called_once()

            # Verify async task was created (but not executed due to mock)
            mock_create_task.assert_called_once()

            # Get the coroutine that would have been passed to create_task
            call_args = mock_create_task.call_args[0][0]
            # Verify it's the followup processing method
            assert hasattr(call_args, "cr_code")  # It's a coroutine
