from typing import Sequence

from datahub_integrations.chat.chat_history import (
    AssistantMessage,
    HumanMessage,
    Message,
    ReasoningMessage,
    ToolCallRequest,
    ToolResult,
)
from datahub_integrations.teams.teams_history import TeamsConversationHistory


class TestAIThinkingSaving:
    """Test that AI thinking messages are properly saved to conversation history."""

    def test_add_thinking_with_existing_message(self) -> None:
        """Test that thinking messages are added when the associated message exists."""
        conv_history = TeamsConversationHistory("test-conversation")

        # Add a user message
        message_ts = "msg_1"
        user_message = HumanMessage(text="What datasets do we have?")
        conv_history.add_message(message_ts, user_message)

        # Add thinking messages
        thinking_messages: Sequence[Message] = [
            ReasoningMessage(text="I need to search for datasets in the system"),
            ToolCallRequest(
                tool_use_id="call_1",
                tool_name="search_datasets",
                tool_input={"query": "datasets"},
            ),
            ToolResult(
                tool_request=ToolCallRequest(
                    tool_use_id="call_1",
                    tool_name="search_datasets",
                    tool_input={"query": "datasets"},
                ),
                result={"datasets": ["dataset1", "dataset2"]},
            ),
            ReasoningMessage(text="Found 2 datasets, I'll provide a summary"),
            AssistantMessage(text="Here are the datasets: dataset1, dataset2"),
        ]

        conv_history.add_thinking(message_ts, thinking_messages)

        # Verify thinking was saved
        assert message_ts in conv_history._thinking
        assert len(conv_history._thinking[message_ts]) == 5
        first_thinking = conv_history._thinking[message_ts][0]
        last_thinking = conv_history._thinking[message_ts][-1]
        assert (
            hasattr(first_thinking, "text")
            and first_thinking.text == "I need to search for datasets in the system"
        )
        assert (
            hasattr(last_thinking, "text")
            and last_thinking.text == "Here are the datasets: dataset1, dataset2"
        )

    def test_add_thinking_with_removed_message(self) -> None:
        """Test that thinking messages are not added when the associated message was removed."""
        conv_history = TeamsConversationHistory("test-conversation")

        # Add a message that will be removed
        message_ts = "msg_0"
        user_message = HumanMessage(text="This message will be removed")
        conv_history.add_message(message_ts, user_message)

        # Fill up to the limit to force removal of the first message
        from datahub_integrations.teams.teams_history import (
            _MAX_MESSAGES_PER_CONVERSATION,
        )

        for i in range(1, _MAX_MESSAGES_PER_CONVERSATION + 1):
            conv_history.add_message(f"msg_{i}", HumanMessage(text=f"Message {i}"))

        # Try to add thinking for the removed message
        thinking_messages: Sequence[Message] = [
            ReasoningMessage(text="This thinking should be skipped")
        ]
        conv_history.add_thinking(message_ts, thinking_messages)

        # Verify thinking was not saved
        assert message_ts not in conv_history._thinking

    def test_get_chat_history_includes_thinking(self) -> None:
        """Test that get_chat_history includes thinking messages in the correct order."""
        conv_history = TeamsConversationHistory("test-conversation")

        # Add user message
        message_ts = "msg_1"
        user_message = HumanMessage(text="What datasets do we have?")
        conv_history.add_message(message_ts, user_message)

        # Add thinking messages
        thinking_messages: Sequence[Message] = [
            ReasoningMessage(text="I need to search for datasets"),
            ToolCallRequest(
                tool_use_id="call_2",
                tool_name="search",
                tool_input={"query": "datasets"},
            ),
            ToolResult(
                tool_request=ToolCallRequest(
                    tool_use_id="call_2",
                    tool_name="search",
                    tool_input={"query": "datasets"},
                ),
                result={"count": 2},
            ),
            AssistantMessage(text="Found 2 datasets"),
        ]
        conv_history.add_thinking(message_ts, thinking_messages)

        # Get chat history
        chat_history = conv_history.get_chat_history()

        # Verify the thinking messages are included
        assert len(chat_history.messages) == 4  # 4 thinking messages
        assert isinstance(chat_history.messages[0], ReasoningMessage)
        assert isinstance(chat_history.messages[1], ToolCallRequest)
        assert isinstance(chat_history.messages[2], ToolResult)
        assert isinstance(chat_history.messages[3], AssistantMessage)

        # Verify the thinking messages replace the original user message
        first_msg = chat_history.messages[0]
        last_msg = chat_history.messages[-1]
        assert (
            hasattr(first_msg, "text")
            and first_msg.text == "I need to search for datasets"
        )
        assert hasattr(last_msg, "text") and last_msg.text == "Found 2 datasets"

    def test_thinking_cleanup_when_message_removed(self) -> None:
        """Test that thinking messages are cleaned up when the associated message is removed."""
        conv_history = TeamsConversationHistory("test-conversation")

        # Add messages with thinking
        for i in range(3):
            message_ts = f"msg_{i}"
            user_message = HumanMessage(text=f"Message {i}")
            conv_history.add_message(message_ts, user_message)

            # Add thinking for each message
            thinking = [ReasoningMessage(text=f"Thinking for message {i}")]
            conv_history.add_thinking(message_ts, thinking)

        # Verify all thinking is present
        assert len(conv_history._thinking) == 3

        # Fill up to the limit to force removal of the first message
        from datahub_integrations.teams.teams_history import (
            _MAX_MESSAGES_PER_CONVERSATION,
        )

        for i in range(3, _MAX_MESSAGES_PER_CONVERSATION + 1):
            conv_history.add_message(f"msg_{i}", HumanMessage(text=f"Message {i}"))

        # Verify thinking for removed message is gone
        assert "msg_0" not in conv_history._thinking
        assert "msg_1" in conv_history._thinking  # Should still be there
        assert "msg_2" in conv_history._thinking  # Should still be there

    def test_multiple_thinking_messages_per_conversation(self) -> None:
        """Test handling multiple thinking message sets in one conversation."""
        conv_history = TeamsConversationHistory("test-conversation")

        # Add first user message with thinking
        msg1_ts = "msg_1"
        conv_history.add_message(msg1_ts, HumanMessage(text="First question"))
        thinking1: Sequence[Message] = [
            ReasoningMessage(text="Processing first question"),
            AssistantMessage(text="First answer"),
        ]
        conv_history.add_thinking(msg1_ts, thinking1)

        # Add second user message with thinking
        msg2_ts = "msg_2"
        conv_history.add_message(msg2_ts, HumanMessage(text="Second question"))
        thinking2: Sequence[Message] = [
            ReasoningMessage(text="Processing second question"),
            ToolCallRequest(
                tool_use_id="call_3", tool_name="search", tool_input={"query": "test"}
            ),
            ToolResult(
                tool_request=ToolCallRequest(
                    tool_use_id="call_3",
                    tool_name="search",
                    tool_input={"query": "test"},
                ),
                result={"data": "test"},
            ),
            AssistantMessage(text="Second answer"),
        ]
        conv_history.add_thinking(msg2_ts, thinking2)

        # Verify both thinking sets are saved
        assert len(conv_history._thinking) == 2
        assert len(conv_history._thinking[msg1_ts]) == 2
        assert len(conv_history._thinking[msg2_ts]) == 4

        # Verify chat history includes both thinking sets
        chat_history = conv_history.get_chat_history()
        assert len(chat_history.messages) == 6  # 2 + 4 thinking messages

        # Verify order is preserved
        msg0 = chat_history.messages[0]
        msg2 = chat_history.messages[2]
        msg_last = chat_history.messages[-1]
        assert hasattr(msg0, "text") and msg0.text == "Processing first question"
        assert hasattr(msg2, "text") and msg2.text == "Processing second question"
        assert hasattr(msg_last, "text") and msg_last.text == "Second answer"

    def test_thinking_with_mixed_message_types(self) -> None:
        """Test that thinking can handle various message types correctly."""
        conv_history = TeamsConversationHistory("test-conversation")

        message_ts = "msg_1"
        conv_history.add_message(message_ts, HumanMessage(text="Complex question"))

        # Create thinking with various message types
        thinking_messages: Sequence[Message] = [
            ReasoningMessage(text="Let me think about this"),
            ToolCallRequest(
                tool_use_id="call_4", tool_name="get_data", tool_input={"id": "123"}
            ),
            ToolResult(
                tool_request=ToolCallRequest(
                    tool_use_id="call_4", tool_name="get_data", tool_input={"id": "123"}
                ),
                result={"name": "test", "value": 42},
            ),
            ReasoningMessage(text="Now I'll format the response"),
            AssistantMessage(text="Here's the formatted response"),
        ]

        conv_history.add_thinking(message_ts, thinking_messages)

        # Verify all message types are preserved
        chat_history = conv_history.get_chat_history()
        assert len(chat_history.messages) == 5

        # Check specific message types
        assert isinstance(chat_history.messages[0], ReasoningMessage)
        assert isinstance(chat_history.messages[1], ToolCallRequest)
        assert isinstance(chat_history.messages[2], ToolResult)
        assert isinstance(chat_history.messages[3], ReasoningMessage)
        assert isinstance(chat_history.messages[4], AssistantMessage)

        # Verify content is preserved
        assert chat_history.messages[1].tool_name == "get_data"
        assert chat_history.messages[1].tool_input == {"id": "123"}
        assert chat_history.messages[2].result == {"name": "test", "value": 42}
