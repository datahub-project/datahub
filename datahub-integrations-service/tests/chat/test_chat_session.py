from typing import List

from datahub_integrations.chat.chat_history import (
    AssistantMessage,
    ChatHistory,
    HumanMessage,
    ReasoningMessage,
)
from datahub_integrations.chat.chat_session import (
    FilteredProgressListener,
    NextMessage,
)


def test_get_progress_steps_extracts_reasoning_messages() -> None:
    """Test that progress updates are extracted from ReasoningMessage instances."""
    history = ChatHistory(
        messages=[
            HumanMessage(text="Hello"),
            ReasoningMessage(text="Thinking about the question"),
            AssistantMessage(text="Here's my response"),
            ReasoningMessage(text="Processing the request"),
        ]
    )

    updates = FilteredProgressListener.get_progress_updates(history, start_offset=0)

    assert len(updates) == 2
    assert updates[0].text == "Thinking about the question"
    assert updates[0].message_type == "THINKING"
    assert updates[1].text == "Processing the request"
    assert updates[1].message_type == "THINKING"


def test_get_progress_steps_respects_start_offset() -> None:
    """Test that only messages after start_offset are considered."""
    history = ChatHistory(
        messages=[
            ReasoningMessage(text="First reasoning"),
            ReasoningMessage(text="Second reasoning"),
            ReasoningMessage(text="Third reasoning"),
        ]
    )

    updates = FilteredProgressListener.get_progress_updates(history, start_offset=1)

    assert len(updates) == 2
    assert updates[0].text == "Second reasoning"
    assert updates[1].text == "Third reasoning"


def test_callback_called_on_changes() -> None:
    """Test that callback is called when progress updates change."""
    from datahub_integrations.chat.chat_session import ProgressUpdate

    history = ChatHistory(messages=[HumanMessage(text="Hello")])
    callback_calls = []

    def mock_callback(updates: List[ProgressUpdate]) -> None:
        callback_calls.append(updates.copy())

    listener = FilteredProgressListener(history, mock_callback)

    # Add a reasoning message and trigger update
    history.add_message(ReasoningMessage(text="Processing"))
    listener._handle_history_updated()

    assert len(callback_calls) == 1
    assert len(callback_calls[0]) == 1
    assert callback_calls[0][0].text == "Processing"
    assert callback_calls[0][0].message_type == "THINKING"


def test_sanitize_progress_step_replaces_trailing_colon() -> None:
    """Test that trailing colons are replaced with periods."""
    assert (
        FilteredProgressListener._sanitize_progress_step("Processing data:")
        == "Processing data."
    )
    assert (
        FilteredProgressListener._sanitize_progress_step("Loading files: ")
        == "Loading files."
    )


def test_sanitize_progress_step_preserves_other_colons() -> None:
    """Test that colons in the middle of strings are not affected."""
    assert (
        FilteredProgressListener._sanitize_progress_step("Processing data")
        == "Processing data"
    )
    assert (
        FilteredProgressListener._sanitize_progress_step("User: admin is working")
        == "User: admin is working"
    )


def test_get_progress_steps_sanitizes_reasoning_messages() -> None:
    """Test that progress updates are sanitized when extracted from ReasoningMessage instances."""
    history = ChatHistory(
        messages=[
            HumanMessage(text="Hello"),
            ReasoningMessage(text="Thinking about the question:"),
            AssistantMessage(text="Here's my response"),
            ReasoningMessage(text="Processing the request: "),
        ]
    )

    updates = FilteredProgressListener.get_progress_updates(history, start_offset=0)

    assert len(updates) == 2
    assert updates[0].text == "Thinking about the question."
    assert updates[1].text == "Processing the request."


def test_too_many_suggestions_truncates() -> None:
    # Test that too many suggestions are truncated with a warning instead of raising an error
    suggestions = [
        chr(i) for i in range(ord("a"), ord("z") + 1)
    ]  # [a, b, ..., z] (26 suggestions)
    message = NextMessage(text="This is a normal response", suggestions=suggestions)
    # Should be truncated to 4 suggestions (MAX_SUGGESTIONS)
    assert len(message.suggestions) == 4
    assert message.suggestions == ["a", "b", "c", "d"]
