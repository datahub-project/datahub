from typing import List

from datahub_integrations.chat.chat_history import (
    AssistantMessage,
    ChatHistory,
    HumanMessage,
    ReasoningMessage,
)
from datahub_integrations.chat.chat_session import FilteredProgressListener


def test_get_progress_steps_extracts_reasoning_messages() -> None:
    """Test that progress steps are extracted from ReasoningMessage instances."""
    history = ChatHistory(
        messages=[
            HumanMessage(text="Hello"),
            ReasoningMessage(text="Thinking about the question"),
            AssistantMessage(text="Here's my response"),
            ReasoningMessage(text="Processing the request"),
        ]
    )

    steps = FilteredProgressListener.get_progress_steps(history, start_offset=0)

    assert len(steps) == 2
    assert steps[0] == "Thinking about the question"
    assert steps[1] == "Processing the request"


def test_get_progress_steps_respects_start_offset() -> None:
    """Test that only messages after start_offset are considered."""
    history = ChatHistory(
        messages=[
            ReasoningMessage(text="First reasoning"),
            ReasoningMessage(text="Second reasoning"),
            ReasoningMessage(text="Third reasoning"),
        ]
    )

    steps = FilteredProgressListener.get_progress_steps(history, start_offset=1)

    assert len(steps) == 2
    assert steps[0] == "Second reasoning"
    assert steps[1] == "Third reasoning"


def test_callback_called_on_changes() -> None:
    """Test that callback is called when progress steps change."""
    history = ChatHistory(messages=[HumanMessage(text="Hello")])
    callback_calls = []

    def mock_callback(steps: List[str]) -> None:
        callback_calls.append(steps.copy())

    listener = FilteredProgressListener(history, mock_callback)

    # Add a reasoning message and trigger update
    history.add_message(ReasoningMessage(text="Processing"))
    listener._handle_history_updated()

    assert len(callback_calls) == 1
    assert callback_calls[0] == ["Processing"]


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
    """Test that progress steps are sanitized when extracted from ReasoningMessage instances."""
    history = ChatHistory(
        messages=[
            HumanMessage(text="Hello"),
            ReasoningMessage(text="Thinking about the question:"),
            AssistantMessage(text="Here's my response"),
            ReasoningMessage(text="Processing the request: "),
        ]
    )

    steps = FilteredProgressListener.get_progress_steps(history, start_offset=0)

    assert len(steps) == 2
    assert steps[0] == "Thinking about the question."
    assert steps[1] == "Processing the request."
