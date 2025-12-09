from pathlib import Path

from datahub.testing.compare_metadata_json import assert_metadata_files_equal

from datahub_integrations.chat.chat_history import (
    AssistantMessage,
    ChatHistory,
    HumanMessage,
    Message,
    ReasoningMessage,
    ToolCallRequest,
    ToolResult,
    ToolResultError,
)

_resource_dir = Path(__file__).parent


def test_chat_history(tmp_path: Path) -> None:
    chat_history = ChatHistory(
        messages=[
            HumanMessage(text="Hello, bot!"),
            ReasoningMessage(text="Let me say hello back to the user"),
        ]
    )
    chat_history.add_message(AssistantMessage(text="Hello!"))

    # Test that we can serialize out properly.
    output_file = tmp_path / "chat_history.json"
    chat_history.save_file(output_file)

    assert_metadata_files_equal(
        output_file,
        golden_path=_resource_dir / "golden_chat_history.json",
    )

    # Test that we can deserialize back to the same object.
    deserialized_chat_history = ChatHistory.load_file(output_file)
    assert chat_history == deserialized_chat_history


def test_chat_history_properties_single_question() -> None:
    """Test properties for a single human question with multiple tool interactions."""
    # Initialize ChatHistory with all messages in sequence
    chat_history = ChatHistory(
        messages=[
            HumanMessage(text="What's the weather like?"),
            ToolCallRequest(
                tool_use_id="call_1",
                tool_name="get_weather",
                tool_input={"location": "New York"},
            ),
            ToolResult(
                tool_request=ToolCallRequest(
                    tool_use_id="call_1",
                    tool_name="get_weather",
                    tool_input={"location": "New York"},
                ),
                result={"temperature": 72, "condition": "sunny"},
            ),
            ToolCallRequest(
                tool_use_id="call_2",
                tool_name="get_forecast",
                tool_input={"location": "New York", "days": 3},
            ),
            ToolResult(
                tool_request=ToolCallRequest(
                    tool_use_id="call_2",
                    tool_name="get_forecast",
                    tool_input={"location": "New York", "days": 3},
                ),
                result="3-day forecast data",
            ),
            AssistantMessage(text="The weather is sunny and 72°F"),
        ]
    )

    # Test properties
    assert chat_history.num_tool_calls == 2
    assert chat_history.num_tool_results == 2
    assert chat_history.num_tool_call_errors == 0
    assert chat_history.num_reducers_applied == 0
    assert chat_history.reduction_sequence_json is None


def test_chat_history_properties_multiple_questions() -> None:
    """Test properties for multiple human questions with tool interactions."""
    # Initialize ChatHistory with all messages in sequence
    chat_history = ChatHistory(
        messages=[
            # First question
            HumanMessage(text="What's the weather?"),
            ToolCallRequest(
                tool_use_id="call_1",
                tool_name="get_weather",
                tool_input={"location": "NYC"},
            ),
            ToolResult(
                tool_request=ToolCallRequest(
                    tool_use_id="call_1",
                    tool_name="get_weather",
                    tool_input={"location": "NYC"},
                ),
                result={"temperature": 70},
            ),
            AssistantMessage(text="It's 70°F"),
            # Second question with errors
            HumanMessage(text="What about tomorrow?"),
            ToolCallRequest(
                tool_use_id="call_2",
                tool_name="get_forecast",
                tool_input={"location": "NYC", "date": "tomorrow"},
            ),
            ToolResult(
                tool_request=ToolCallRequest(
                    tool_use_id="call_2",
                    tool_name="get_forecast",
                    tool_input={"location": "NYC", "date": "tomorrow"},
                ),
                result={"temperature": 75, "condition": "cloudy"},
            ),
            ToolCallRequest(
                tool_use_id="call_3",
                tool_name="get_news",
                tool_input={"topic": "weather"},
            ),
            ToolResultError(
                tool_request=ToolCallRequest(
                    tool_use_id="call_3",
                    tool_name="get_news",
                    tool_input={"topic": "weather"},
                ),
                error="Service unavailable",
            ),
            AssistantMessage(text="Tomorrow will be 75°F and cloudy"),
        ]
    )

    # Test properties for the second question
    assert chat_history.num_tool_calls == 2  # Only counts since last human message
    assert (
        chat_history.num_tool_results == 1
    )  # Only counts successful results since last human message
    assert (
        chat_history.num_tool_call_errors == 1
    )  # Only counts errors since last human message
    assert chat_history.num_reducers_applied == 0
    assert chat_history.reduction_sequence_json is None


def test_chat_history_reducer_properties() -> None:
    """Test reducer-related properties."""
    # Initialize ChatHistory with messages
    chat_history = ChatHistory(
        messages=[
            HumanMessage(text="Hello"),
            AssistantMessage(text="Hi there!"),
        ]
    )

    # Test initial state
    assert chat_history.num_reducers_applied == 0
    assert chat_history.reduction_sequence_json is None

    # Apply a reducer
    reduced_messages: list[Message] = [
        HumanMessage(text="Hello"),
        AssistantMessage(text="Hi there!"),
    ]
    reducer_metadata = {
        "reducer_name": "ConversationSummarizer",
        "num_tokens_before": 100,
        "num_tokens_after": 50,
        "num_messages_before": 2,
        "num_messages_after": 2,
    }
    chat_history.set_reduced_history(reduced_messages, reducer_metadata)

    # Test after reducer application
    assert chat_history.num_reducers_applied == 1
    assert chat_history.reduction_sequence_json is not None
    assert "ConversationSummarizer" in chat_history.reduction_sequence_json

    # Apply another reducer
    another_reducer_metadata = {
        "reducer_name": "ConversationSummarizer",
        "num_tokens_before": 50,
        "num_tokens_after": 25,
        "num_messages_before": 2,
        "num_messages_after": 1,
    }
    chat_history.set_reduced_history(
        [HumanMessage(text="Hello")], another_reducer_metadata
    )

    # Test multiple reducers
    assert chat_history.num_reducers_applied == 2
    reducers_data = chat_history.extra_properties["reducers"]
    assert len(reducers_data) == 2
    assert reducers_data[0]["reducer_name"] == "ConversationSummarizer"
    assert reducers_data[1]["reducer_name"] == "ConversationSummarizer"


def test_chat_history_followup_flag() -> None:
    """Follow-up flag should flip once the assistant has responded in the thread."""
    chat_history = ChatHistory(messages=[HumanMessage(text="First question?")])
    assert chat_history.is_followup_datahub_ask_question is False

    chat_history.add_message(AssistantMessage(text="First answer."))
    assert chat_history.is_followup_datahub_ask_question is False

    chat_history.add_message(HumanMessage(text="Second question?"))
    assert chat_history.is_followup_datahub_ask_question is True


def test_chat_history_followup_flag_limited_history() -> None:
    """Limited histories should surface `None` for unknown prior answers."""
    chat_history = ChatHistory(
        messages=[HumanMessage(text="Question without context?")],
        extra_properties={"is_limited_history": True},
    )
    assert chat_history.is_followup_datahub_ask_question is None

    chat_history.add_message(AssistantMessage(text="Known answer."))
    chat_history.add_message(HumanMessage(text="Another question?"))
    assert chat_history.is_followup_datahub_ask_question is True
