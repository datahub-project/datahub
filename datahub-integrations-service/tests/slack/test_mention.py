from typing import Any
from unittest.mock import MagicMock, Mock, patch

import pytest

from datahub_integrations.chat.agent import (
    AgentMaxLLMTurnsExceededError,
    AgentMaxTokensExceededError,
)
from datahub_integrations.gen_ai.llm.exceptions import LlmDailyLimitExceededException
from datahub_integrations.slack.command.mention import (
    FeedbackPayload,
    SlackMentionEvent,
    _build_connect_account_message,
    _build_progress_message,
    handle_app_mention,
    handle_feedback,
)
from datahub_integrations.slack.utils.datahub_user import UserResolutionResult


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


def test_build_progress_message_plan_execution() -> None:
    """Test that plan messages are displayed in full without previous steps."""
    # Simulate previous regular steps
    steps = [
        "Analyzing query",
        "Searching for entities",
    ]

    # Add a plan message (multi-line with plan indicators)
    plan_message = """**Plan: Find affected Looker dashboards**

✓ Find orders dataset
✓ Get downstream lineage
▶ Filter for Looker assets
> _Examining 12 downstream entities_
• Fetch dashboard details"""

    steps.append(plan_message)

    text, blocks = _build_progress_message(steps)

    # Plain text should show the full plan message
    assert text == plan_message

    # Should use section block (not context) for better multi-line formatting
    assert len(blocks) == 1
    assert blocks[0]["type"] == "section"
    assert blocks[0]["text"]["type"] == "mrkdwn"

    # Full plan message should be in the block
    assert "**Plan: Find affected Looker dashboards**" in blocks[0]["text"]["text"]
    assert "✓ Find orders dataset" in blocks[0]["text"]["text"]
    assert "▶ Filter for Looker assets" in blocks[0]["text"]["text"]
    assert "• Fetch dashboard details" in blocks[0]["text"]["text"]

    # Previous non-plan steps should NOT be included
    assert "Analyzing query" not in blocks[0]["text"]["text"]
    assert "Searching for entities" not in blocks[0]["text"]["text"]


def test_build_progress_message_plan_with_checkmarks() -> None:
    """Test that plan messages with checkmarks are detected correctly."""
    steps = ["Step 1", "Step 2"]

    # Plan with just checkmarks (no "**Plan:" marker)
    plan_message = """✓ Completed step 1
▶ Working on step 2
• Pending step 3"""

    steps.append(plan_message)
    text, blocks = _build_progress_message(steps)

    # Plain text should show the full plan message
    assert text == plan_message

    # Should detect as plan and use section block
    assert blocks[0]["type"] == "section"
    assert "✓ Completed step 1" in blocks[0]["text"]["text"]

    # Previous steps should not be included
    assert "Step 1" not in blocks[0]["text"]["text"]


def test_handle_app_mention_chat_max_llm_turns_error() -> None:
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
        patch(
            "datahub_integrations.slack.command.mention.get_require_slack_oauth_binding",
            return_value=False,  # Disable OAuth requirement for this test
        ),
    ):
        mock_app = Mock()
        mock_generate.side_effect = AgentMaxLLMTurnsExceededError(
            "The model returned the following errors: input length and `max_tokens` exceed context limit"
        )
        mock_app.client.chat_postMessage.return_value = {"ts": "1234.9999"}
        mock_app.client.users_info.return_value = {"user": {"name": "test_user"}}

        handle_app_mention(mock_app, mock_event)

        mock_app.client.chat_update.assert_called_once()

        assert (
            mock_app.client.chat_update.call_args[1]["text"]
            == ":x: Uh, oh ! Looks like your question is too complex. Please try again with a simpler question.\n\n_Reference: message_id=1234.5678_"
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
        patch(
            "datahub_integrations.slack.command.mention.get_require_slack_oauth_binding",
            return_value=False,  # Disable OAuth requirement for this test
        ),
    ):
        mock_app = Mock()
        mock_generate.side_effect = AgentMaxTokensExceededError(
            "he model returned the following errors: Input is too long for requested model."
        )
        mock_app.client.chat_postMessage.return_value = {"ts": "1234.9999"}
        mock_app.client.users_info.return_value = {"user": {"name": "test_user"}}

        handle_app_mention(mock_app, mock_event)

        mock_app.client.chat_update.assert_called_once()

        assert (
            mock_app.client.chat_update.call_args[1]["text"]
            == ":x: Uh, oh ! Looks like I fetched too much information here. Please try asking your question in a new thread.\n\n_Reference: message_id=1234.5678_"
        )


def test_handle_app_mention_daily_limit_exceeded_error() -> None:
    """Test that LlmDailyLimitExceededException shows a user-friendly message."""
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
        patch(
            "datahub_integrations.slack.command.mention.get_require_slack_oauth_binding",
            return_value=False,  # Disable OAuth requirement for this test
        ),
    ):
        mock_app = Mock()
        mock_generate.side_effect = LlmDailyLimitExceededException(
            "Daily token limit (20,000,000) exceeded. Used today: 20,123,456"
        )
        mock_app.client.chat_postMessage.return_value = {"ts": "1234.9999"}
        mock_app.client.users_info.return_value = {"user": {"name": "test_user"}}

        handle_app_mention(mock_app, mock_event)

        mock_app.client.chat_update.assert_called_once()

        # Should show the user-friendly message with warning icon
        assert (
            mock_app.client.chat_update.call_args[1]["text"]
            == ":warning: AI features have reached their daily usage limit. Please try again later.\n\n_Reference: message_id=1234.5678_"
        )


def test_handle_feedback_with_message_contents_in_payload() -> None:
    channel_id = "C123"
    bot_message_ts = "1699123500.789012"
    user_id = "U123"
    user_name = "alice"
    thread_ts = "1699123456.123456"
    user_message_ts = "1699123456.123456"

    payload = FeedbackPayload(
        thread_ts=thread_ts,
        message_ts=user_message_ts,
        feedback="positive",
        message_contents="What is the schema?",
    )

    mock_app = Mock()
    mock_app.client.reactions_add.return_value = {"ok": True}

    with patch(
        "datahub_integrations.slack.command.mention.track_saas_event"
    ) as mock_track:
        handle_feedback(
            mock_app, channel_id, bot_message_ts, user_id, user_name, payload
        )

    mock_app.client.conversations_replies.assert_not_called()
    mock_track.assert_called_once()
    assert mock_track.call_args[0][0].message_contents == "What is the schema?"
    mock_app.client.reactions_add.assert_called_once_with(
        channel=channel_id, timestamp=bot_message_ts, name="thumbsup"
    )


def test_handle_feedback_fetches_message_from_api() -> None:
    channel_id = "C123"
    bot_message_ts = "1699123500.789012"
    user_id = "U123"
    user_name = "alice"
    thread_ts = "1699123456.123456"
    user_message_ts = "1699123456.123456"

    payload = FeedbackPayload(
        thread_ts=thread_ts,
        message_ts=user_message_ts,
        feedback="positive",
    )

    mock_app = Mock()
    mock_app.client.conversations_replies.return_value = {
        "messages": [{"ts": user_message_ts, "text": "User question"}],
        "ok": True,
    }
    mock_app.client.reactions_add.return_value = {"ok": True}

    with patch(
        "datahub_integrations.slack.command.mention.track_saas_event"
    ) as mock_track:
        handle_feedback(
            mock_app, channel_id, bot_message_ts, user_id, user_name, payload
        )

    mock_app.client.conversations_replies.assert_called_once_with(
        channel=channel_id,
        ts=thread_ts,
        latest=user_message_ts,
        limit=5,
        inclusive=True,
    )
    assert mock_track.call_args[0][0].message_contents == "User question"


# =============================================================================
# Tests for OAuth User Resolution and Connect Account Message
# =============================================================================


class TestBuildConnectAccountMessage:
    """Tests for _build_connect_account_message function."""

    def test_returns_tuple_of_text_and_blocks(self) -> None:
        """Should return a tuple of (text, blocks)."""
        text, blocks = _build_connect_account_message("U12345678")

        assert isinstance(text, str)
        assert isinstance(blocks, list)

    def test_text_mentions_user(self) -> None:
        """Should mention the Slack user in the message."""
        text, _ = _build_connect_account_message("U12345678")

        assert "<@U12345678>" in text

    def test_text_explains_connection_requirement(self) -> None:
        """Should explain that account connection is required."""
        text, _ = _build_connect_account_message("U12345678")

        assert "connect" in text.lower()
        assert "DataHub" in text

    def test_blocks_contain_section_with_text(self) -> None:
        """Should have a section block with the message text."""
        _, blocks = _build_connect_account_message("U12345678")

        section_blocks = [b for b in blocks if b.get("type") == "section"]
        assert len(section_blocks) >= 1
        assert section_blocks[0]["text"]["type"] == "mrkdwn"

    def test_blocks_contain_connect_button(self) -> None:
        """Should have an actions block with a connect button."""
        _, blocks = _build_connect_account_message("U12345678")

        action_blocks = [b for b in blocks if b.get("type") == "actions"]
        assert len(action_blocks) >= 1

        elements = action_blocks[0]["elements"]
        assert len(elements) >= 1

        button = elements[0]
        assert button["type"] == "button"
        assert "Connect" in button["text"]["text"]

    def test_connect_button_has_correct_url(self) -> None:
        """Should link to personal notifications settings with connect_slack param."""
        _, blocks = _build_connect_account_message("U12345678")

        action_blocks = [b for b in blocks if b.get("type") == "actions"]
        button = action_blocks[0]["elements"][0]

        assert "settings/personal-notifications" in button["url"]
        assert "connect_slack=true" in button["url"]

    def test_blocks_contain_context_helper_text(self) -> None:
        """Should have a context block with helper text."""
        _, blocks = _build_connect_account_message("U12345678")

        context_blocks = [b for b in blocks if b.get("type") == "context"]
        assert len(context_blocks) >= 1


class TestBuildAgentWithImpersonation:
    """Tests for _build_agent function with impersonation."""

    @pytest.fixture
    def mock_slack_client(self) -> MagicMock:
        """Create a mock Slack WebClient."""
        client = MagicMock()
        client.conversations_replies.return_value = {"messages": []}
        return client

    @pytest.fixture
    def mock_event(self) -> SlackMentionEvent:
        """Create a mock SlackMentionEvent."""
        return SlackMentionEvent(
            channel_id="C12345678",
            message_ts="1234567890.123456",
            user_id="U12345678",
            message_text="@datahub what is this dataset?",
        )

    @patch("datahub_integrations.slack.command.mention.graph_as_user")
    @patch("datahub_integrations.slack.command.mention.graph")
    @patch(
        "datahub_integrations.slack.command.mention.create_data_catalog_explorer_agent"
    )
    @patch("datahub_integrations.slack.command.mention.slack_config")
    def test_without_user_urn_uses_system_graph(
        self,
        mock_slack_config: Any,
        mock_create_agent: Any,
        mock_graph: Any,
        mock_graph_as_user: Any,
        mock_slack_client: MagicMock,
        mock_event: SlackMentionEvent,
    ) -> None:
        """When no user URN provided, should use system graph (no impersonation)."""
        from datahub_integrations.slack.command.mention import _build_agent

        # Setup mocks
        mock_thread_history = MagicMock()
        mock_thread_history.is_limited_history.return_value = False
        mock_thread_history.get_chat_history.return_value = MagicMock()
        mock_slack_config.get_slack_history_cache.return_value.get_thread.return_value = mock_thread_history
        mock_create_agent.return_value = MagicMock()

        _build_agent(mock_slack_client, mock_event, datahub_user_urn=None)

        # Should NOT call graph_as_user
        mock_graph_as_user.assert_not_called()

    @patch("datahub_integrations.slack.command.mention.graph_as_user")
    @patch("datahub_integrations.slack.command.mention.graph")
    @patch(
        "datahub_integrations.slack.command.mention.create_data_catalog_explorer_agent"
    )
    @patch("datahub_integrations.slack.command.mention.slack_config")
    def test_with_user_urn_uses_impersonated_graph(
        self,
        mock_slack_config: Any,
        mock_create_agent: Any,
        mock_graph: Any,
        mock_graph_as_user: Any,
        mock_slack_client: MagicMock,
        mock_event: SlackMentionEvent,
    ) -> None:
        """When user URN provided, should use impersonated graph."""
        from datahub_integrations.slack.command.mention import _build_agent

        # Setup mocks
        mock_thread_history = MagicMock()
        mock_thread_history.is_limited_history.return_value = False
        mock_thread_history.get_chat_history.return_value = MagicMock()
        mock_slack_config.get_slack_history_cache.return_value.get_thread.return_value = mock_thread_history
        mock_create_agent.return_value = MagicMock()
        mock_impersonated_graph = MagicMock()
        mock_graph_as_user.return_value = mock_impersonated_graph

        _build_agent(
            mock_slack_client, mock_event, datahub_user_urn="urn:li:corpuser:testuser"
        )

        # Should call graph_as_user with the user URN
        mock_graph_as_user.assert_called_once_with("urn:li:corpuser:testuser")


class TestHandleAppMentionWithOAuth:
    """Integration tests for handle_app_mention with OAuth user resolution."""

    @patch("datahub_integrations.slack.command.mention.get_require_slack_oauth_binding")
    @patch("datahub_integrations.slack.command.mention.resolve_slack_user_to_datahub")
    @patch("datahub_integrations.slack.command.mention.fetch_thread_history")
    def test_oauth_required_user_not_connected_shows_connect_prompt(
        self,
        mock_fetch_history: Any,
        mock_resolve_user: Any,
        mock_get_flag: Any,
    ) -> None:
        """When OAuth required and user not connected, should show connect prompt."""
        mock_get_flag.return_value = True
        # User not connected - should_prompt_connection is True
        mock_resolve_user.return_value = UserResolutionResult(
            user_urn=None, requires_connection=True
        )

        mock_event = SlackMentionEvent(
            channel_id="C123",
            message_ts="1234.5678",
            user_id="U123",
            message_text="@datahub test",
        )

        mock_app = Mock()
        mock_app.client.chat_postMessage.return_value = {"ts": "1234.9999"}
        mock_app.client.users_info.return_value = {"user": {"name": "test_user"}}

        handle_app_mention(mock_app, mock_event)

        # Should update message with connect prompt
        mock_app.client.chat_update.assert_called_once()
        call_kwargs = mock_app.client.chat_update.call_args[1]

        # Check that the message contains connect account info
        assert "connect" in call_kwargs["text"].lower()
        assert "<@U123>" in call_kwargs["text"]

        # Check blocks contain a button
        blocks = call_kwargs["blocks"]
        action_blocks = [b for b in blocks if b.get("type") == "actions"]
        assert len(action_blocks) >= 1

    @patch("datahub_integrations.slack.command.mention.get_require_slack_oauth_binding")
    @patch("datahub_integrations.slack.command.mention.resolve_slack_user_to_datahub")
    @patch("datahub_integrations.slack.command.mention._build_agent")
    @patch("datahub_integrations.slack.command.mention._generate_mention_response")
    @patch("datahub_integrations.slack.command.mention.track_saas_event")
    @patch("datahub_integrations.slack.command.mention.fetch_thread_history")
    def test_oauth_not_required_proceeds_without_resolution(
        self,
        mock_fetch_history: Any,
        mock_track: Any,
        mock_generate: Any,
        mock_build_agent: Any,
        mock_resolve_user: Any,
        mock_get_flag: Any,
    ) -> None:
        """When OAuth not required, should proceed without user resolution."""
        mock_get_flag.return_value = False
        # OAuth not required - no connection prompt needed
        mock_resolve_user.return_value = UserResolutionResult(
            user_urn=None, requires_connection=False
        )

        mock_event = SlackMentionEvent(
            channel_id="C123",
            message_ts="1234.5678",
            user_id="U123",
            message_text="@datahub test",
        )

        mock_app = Mock()
        mock_app.client.chat_postMessage.return_value = {"ts": "1234.9999"}
        mock_app.client.users_info.return_value = {"user": {"name": "test_user"}}

        mock_agent = MagicMock()
        mock_agent.history.is_followup_datahub_ask_question = False
        mock_agent.history.messages = []
        mock_agent.history.num_tool_calls = 0
        mock_agent.history.num_tool_call_errors = 0
        mock_agent.history.json.return_value = "{}"
        mock_agent.history.reduction_sequence_json = "[]"
        mock_agent.history.num_reducers_applied = 0
        mock_agent.session_id = "test-session"
        mock_build_agent.return_value = (mock_agent, False)

        mock_generate.return_value = ("Test response", [])

        handle_app_mention(mock_app, mock_event)

        # Should call _build_agent with None for user_urn
        mock_build_agent.assert_called_once()
        call_args = mock_build_agent.call_args
        assert call_args[0][2] is None  # datahub_user_urn should be None
