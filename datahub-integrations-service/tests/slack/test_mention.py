from unittest.mock import Mock, patch

from datahub_integrations.chat.chat_session import (
    ChatMaxToolCallsExceededError,
    ChatSessionMaxTokensExceededError,
)
from datahub_integrations.slack.command.mention import (
    SlackMentionEvent,
    _build_progress_message,
    handle_app_mention,
)


def test_build_progress_message_basic_functionality() -> None:
    """Test basic progress message building with a few steps."""
    steps = ["Loading files", "Processing data", "Finalizing"]
    text, blocks = _build_progress_message(steps)

    assert text == ":hourglass_flowing_sand: _*Finalizing*_"
    assert len(blocks) == 1
    assert blocks[0]["type"] == "context"
    assert len(blocks[0]["elements"]) == 3

    # Check that previous steps show checkmarks and current shows hourglass
    assert ":white_check_mark:" in blocks[0]["elements"][0]["text"]
    assert ":white_check_mark:" in blocks[0]["elements"][1]["text"]
    assert ":hourglass_flowing_sand:" in blocks[0]["elements"][2]["text"]


def test_build_progress_message_limits_to_ten_elements() -> None:
    """Test that more than 10 steps are limited to last 9 previous + current."""
    steps = [f"Step {i}" for i in range(1, 16)]  # 15 steps total
    text, blocks = _build_progress_message(steps)

    assert text == ":hourglass_flowing_sand: _*Step 15*_"
    assert len(blocks[0]["elements"]) == 10

    # Should show steps 6-14 as previous (last 9), and step 15 as current
    assert "Step 6" in blocks[0]["elements"][0]["text"]
    assert "Step 14" in blocks[0]["elements"][8]["text"]
    assert "Step 15" in blocks[0]["elements"][9]["text"]


def test_handle_app_mention_chat_max_tool_calls_error() -> None:
    mock_event = SlackMentionEvent(
        channel_id="C123",
        message_ts="1234.5678",
        original_thread_ts="1234.5678",
        user_id="U123",
        message_text="test question",
    )

    with (
        patch(
            "datahub_integrations.slack.command.mention._generate_mention_response"
        ) as mock_generate,
        patch("datahub_integrations.slack.command.mention.track_saas_event"),
        patch("datahub_integrations.slack.command.mention.fetch_thread_history"),
    ):
        mock_app = Mock()
        mock_generate.side_effect = ChatMaxToolCallsExceededError(
            "The model returned the following errors: input length and `max_tokens` exceed context limit"
        )
        mock_app.client.chat_postMessage.return_value = {"ts": "1234.9999"}
        mock_app.client.users_info.return_value = {"user": {"name": "test_user"}}

        handle_app_mention(mock_app, mock_event)

        mock_app.client.chat_update.assert_called_once()

        assert (
            mock_app.client.chat_update.call_args[1]["text"]
            == ":x: Uh, oh ! Looks like your question is too complex. Please try again with a simpler question."
        )


def test_handle_app_mention_chat_session_max_tokens_error() -> None:
    mock_event = SlackMentionEvent(
        channel_id="C123",
        message_ts="1234.5678",
        original_thread_ts="1234.5678",
        user_id="U123",
        message_text="test question",
    )

    with (
        patch(
            "datahub_integrations.slack.command.mention._generate_mention_response"
        ) as mock_generate,
        patch("datahub_integrations.slack.command.mention.track_saas_event"),
        patch("datahub_integrations.slack.command.mention.fetch_thread_history"),
    ):
        mock_app = Mock()
        mock_generate.side_effect = ChatSessionMaxTokensExceededError(
            "he model returned the following errors: Input is too long for requested model."
        )
        mock_app.client.chat_postMessage.return_value = {"ts": "1234.9999"}
        mock_app.client.users_info.return_value = {"user": {"name": "test_user"}}

        handle_app_mention(mock_app, mock_event)

        mock_app.client.chat_update.assert_called_once()

        assert (
            mock_app.client.chat_update.call_args[1]["text"]
            == ":x: Uh, oh ! Looks like I fetched too much information here. Please try asking your question in a new thread."
        )
