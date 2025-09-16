from typing import Dict

from datahub_integrations.chat.chat_history import (
    AssistantMessage,
    HumanMessage,
    Message,
)
from datahub_integrations.teams.teams_history import (
    _MAX_MESSAGES_PER_CONVERSATION,
    TeamsConversationHistory,
)


class TestTeamsConversationHistoryBounds:
    """Test that conversation history maintains bounded message windows."""

    def test_message_limit_enforcement(self) -> None:
        """Test that messages are removed when limit is exceeded."""
        conv_history = TeamsConversationHistory("test-conversation")

        # Add messages up to the limit
        for i in range(_MAX_MESSAGES_PER_CONVERSATION):
            message_ts = f"msg_{i}"
            message = HumanMessage(text=f"Message {i}")
            conv_history.add_message(message_ts, message)

        assert conv_history.message_count() == _MAX_MESSAGES_PER_CONVERSATION

        # Add one more message - should trigger removal of oldest
        newest_ts = f"msg_{_MAX_MESSAGES_PER_CONVERSATION}"
        newest_message = HumanMessage(text=f"Message {_MAX_MESSAGES_PER_CONVERSATION}")

        conv_history.add_message(newest_ts, newest_message)

        # Should still be at the limit
        assert conv_history.message_count() == _MAX_MESSAGES_PER_CONVERSATION

        # Oldest message should be gone
        chat_history = conv_history.get_chat_history()
        message_texts = [
            msg.text
            for msg in chat_history.messages
            if hasattr(msg, "text") and msg.text is not None
        ]
        assert "Message 0" not in message_texts
        assert f"Message {_MAX_MESSAGES_PER_CONVERSATION}" in message_texts

    def test_thinking_messages_cleanup(self) -> None:
        """Test that thinking messages are cleaned up when associated messages are removed."""
        conv_history = TeamsConversationHistory("test-conversation")

        # Add a message and its thinking
        message_ts = "msg_0"
        message = HumanMessage(text="Test message")
        thinking = [
            AssistantMessage(text="Thinking step 1"),
            AssistantMessage(text="Thinking step 2"),
        ]

        conv_history.add_message(message_ts, message)
        conv_history.add_thinking(message_ts, thinking)

        # Fill up to the limit to force removal of the first message
        for i in range(1, _MAX_MESSAGES_PER_CONVERSATION + 1):
            conv_history.add_message(f"msg_{i}", HumanMessage(text=f"Message {i}"))

        # The thinking for msg_0 should be gone
        chat_history = conv_history.get_chat_history()
        thinking_texts: list[str] = []
        for msg in chat_history.messages:
            if isinstance(msg, list):
                thinking_texts.extend([m.text for m in msg if hasattr(m, "text")])

        assert "Thinking step 1" not in thinking_texts
        assert "Thinking step 2" not in thinking_texts

    def test_set_full_history_truncation(self) -> None:
        """Test that set_full_history truncates when given more messages than the limit."""
        conv_history = TeamsConversationHistory("test-conversation")

        # Create more messages than the limit
        messages: Dict[str, Message] = {}
        for i in range(_MAX_MESSAGES_PER_CONVERSATION + 10):
            messages[f"msg_{i}"] = HumanMessage(text=f"Message {i}")

        conv_history.set_full_history(messages)

        # Should be truncated to the limit
        assert conv_history.message_count() == _MAX_MESSAGES_PER_CONVERSATION

        # Should keep the most recent messages
        chat_history = conv_history.get_chat_history()
        message_texts = [
            msg.text
            for msg in chat_history.messages
            if hasattr(msg, "text") and msg.text is not None and msg.text is not None
        ]

        # Should have the last _MAX_MESSAGES_PER_CONVERSATION messages
        expected_start = (
            _MAX_MESSAGES_PER_CONVERSATION + 10 - _MAX_MESSAGES_PER_CONVERSATION
        )
        for i in range(expected_start, _MAX_MESSAGES_PER_CONVERSATION + 10):
            assert f"Message {i}" in message_texts

    def test_thinking_skipped_for_removed_messages(self) -> None:
        """Test that thinking is not added for messages that have been removed."""
        conv_history = TeamsConversationHistory("test-conversation")

        # Fill up to the limit
        for i in range(_MAX_MESSAGES_PER_CONVERSATION):
            conv_history.add_message(f"msg_{i}", HumanMessage(text=f"Message {i}"))

        # Try to add thinking for a message that will be removed
        removed_message_ts = "msg_0"
        thinking = [AssistantMessage(text="This thinking should be skipped")]

        # Add one more message to trigger removal of msg_0
        conv_history.add_message(
            f"msg_{_MAX_MESSAGES_PER_CONVERSATION}",
            HumanMessage(text=f"Message {_MAX_MESSAGES_PER_CONVERSATION}"),
        )

        # Now try to add thinking for the removed message
        conv_history.add_thinking(removed_message_ts, thinking)

        # The thinking should not be present
        chat_history = conv_history.get_chat_history()
        thinking_texts: list[str] = []
        for msg in chat_history.messages:
            if isinstance(msg, list):
                thinking_texts.extend([m.text for m in msg if hasattr(m, "text")])

        assert "This thinking should be skipped" not in thinking_texts

    def test_chat_history_metadata(self) -> None:
        """Test that ChatHistory includes metadata about message limits."""
        conv_history = TeamsConversationHistory("test-conversation")

        # Add some messages
        for i in range(5):
            conv_history.add_message(f"msg_{i}", HumanMessage(text=f"Message {i}"))

        chat_history = conv_history.get_chat_history()

        # Check metadata
        assert chat_history.extra_properties["conversation_id"] == "test-conversation"
        assert (
            chat_history.extra_properties["max_messages_per_conversation"]
            == _MAX_MESSAGES_PER_CONVERSATION
        )
        assert chat_history.extra_properties["current_message_count"] == 5

    def test_empty_conversation(self) -> None:
        """Test behavior with empty conversation."""
        conv_history = TeamsConversationHistory("empty-conversation")

        assert conv_history.message_count() == 0
        assert conv_history.is_limited_history()

        chat_history = conv_history.get_chat_history()
        assert len(chat_history.messages) == 0
        assert chat_history.extra_properties["current_message_count"] == 0

    def test_single_message(self) -> None:
        """Test behavior with single message."""
        conv_history = TeamsConversationHistory("single-message")

        conv_history.add_message("msg_1", HumanMessage(text="Single message"))

        assert conv_history.message_count() == 1
        chat_history = conv_history.get_chat_history()
        assert len(chat_history.messages) == 1
        assert (
            hasattr(chat_history.messages[0], "text")
            and chat_history.messages[0].text == "Single message"
        )

    def test_exact_limit_boundary(self) -> None:
        """Test behavior at exact limit boundary."""
        conv_history = TeamsConversationHistory("exact-limit")

        # Add exactly the limit number of messages
        for i in range(_MAX_MESSAGES_PER_CONVERSATION):
            conv_history.add_message(f"msg_{i}", HumanMessage(text=f"Message {i}"))

        assert conv_history.message_count() == _MAX_MESSAGES_PER_CONVERSATION

        # Add one more - should trigger removal
        conv_history.add_message(
            f"msg_{_MAX_MESSAGES_PER_CONVERSATION}",
            HumanMessage(text=f"Message {_MAX_MESSAGES_PER_CONVERSATION}"),
        )

        # Should still be at limit
        assert conv_history.message_count() == _MAX_MESSAGES_PER_CONVERSATION

        # First message should be gone
        chat_history = conv_history.get_chat_history()
        message_texts = [
            msg.text
            for msg in chat_history.messages
            if hasattr(msg, "text") and msg.text is not None
        ]
        assert "Message 0" not in message_texts
        assert f"Message {_MAX_MESSAGES_PER_CONVERSATION}" in message_texts

    def test_multiple_thinking_cleanup(self) -> None:
        """Test cleanup of multiple thinking messages when messages are removed."""
        conv_history = TeamsConversationHistory("multiple-thinking")

        # Add messages with thinking
        for i in range(_MAX_MESSAGES_PER_CONVERSATION):
            message_ts = f"msg_{i}"
            message = HumanMessage(text=f"Message {i}")
            thinking = [AssistantMessage(text=f"Thinking for message {i}")]

            conv_history.add_message(message_ts, message)
            conv_history.add_thinking(message_ts, thinking)

        # Add one more message to trigger cleanup
        conv_history.add_message(
            f"msg_{_MAX_MESSAGES_PER_CONVERSATION}",
            HumanMessage(text=f"Message {_MAX_MESSAGES_PER_CONVERSATION}"),
        )

        # Check that thinking for removed message is gone
        chat_history = conv_history.get_chat_history()
        thinking_texts: list[str] = []
        for msg in chat_history.messages:
            if hasattr(msg, "text") and msg.text is not None:
                thinking_texts.append(msg.text)

        assert "Thinking for message 0" not in thinking_texts
        assert "Thinking for message 1" in thinking_texts  # Should still be there

    def test_set_full_history_with_exact_limit(self) -> None:
        """Test set_full_history with exactly the limit number of messages."""
        conv_history = TeamsConversationHistory("exact-limit-set")

        # Create exactly the limit number of messages
        messages: Dict[str, Message] = {}
        for i in range(_MAX_MESSAGES_PER_CONVERSATION):
            messages[f"msg_{i}"] = HumanMessage(text=f"Message {i}")

        conv_history.set_full_history(messages)

        # Should have all messages
        assert conv_history.message_count() == _MAX_MESSAGES_PER_CONVERSATION

        chat_history = conv_history.get_chat_history()
        message_texts = [
            msg.text
            for msg in chat_history.messages
            if hasattr(msg, "text") and msg.text is not None
        ]

        # All messages should be present
        for i in range(_MAX_MESSAGES_PER_CONVERSATION):
            assert f"Message {i}" in message_texts

    def test_set_full_history_with_fewer_than_limit(self) -> None:
        """Test set_full_history with fewer messages than the limit."""
        conv_history = TeamsConversationHistory("fewer-than-limit")

        # Create fewer messages than the limit
        messages: Dict[str, Message] = {}
        for i in range(10):
            messages[f"msg_{i}"] = HumanMessage(text=f"Message {i}")

        conv_history.set_full_history(messages)

        # Should have all messages
        assert conv_history.message_count() == 10

        chat_history = conv_history.get_chat_history()
        message_texts = [
            msg.text
            for msg in chat_history.messages
            if hasattr(msg, "text") and msg.text is not None
        ]

        # All messages should be present
        for i in range(10):
            assert f"Message {i}" in message_texts
