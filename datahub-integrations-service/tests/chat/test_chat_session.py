from typing import Any, List, cast
from unittest.mock import MagicMock

from datahub_integrations.chat.chat_history import (
    AssistantMessage,
    ChatHistory,
    HumanMessage,
    ReasoningMessage,
)
from datahub_integrations.chat.chat_session import (
    ChatSession,
    FilteredProgressListener,
    NextMessage,
    _strip_reasoning_tag,
    respond_to_user,
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


def test_strip_reasoning_tag_removes_reasoning_tag() -> None:
    """Test that <reasoning> tags and their contents are removed."""
    text = "<reasoning>internal thoughts</reasoning>Here is the answer"
    result = _strip_reasoning_tag(text)
    assert result == "Here is the answer"


def test_strip_reasoning_tag_removes_nested_xml() -> None:
    """Test that <reasoning> tags with nested XML are completely removed."""
    text = "<reasoning><action>search</action><rationale>finding data</rationale></reasoning>The search results are here"
    result = _strip_reasoning_tag(text)
    assert result == "The search results are here"


def test_strip_reasoning_tag_removes_multiple_reasoning_tags() -> None:
    """Test that multiple <reasoning> tags are all removed."""
    text = "<reasoning>first thought</reasoning>Answer part 1<reasoning>second thought</reasoning>Answer part 2"
    result = _strip_reasoning_tag(text)
    assert result == "Answer part 1Answer part 2"


def test_strip_reasoning_tag_handles_no_reasoning_tag() -> None:
    """Test that text without reasoning tags is returned unchanged."""
    text = "This is a normal response with no reasoning tags"
    result = _strip_reasoning_tag(text)
    assert result == "This is a normal response with no reasoning tags"


def test_strip_reasoning_tag_preserves_markdown() -> None:
    """Test that markdown formatting is preserved after stripping reasoning tags."""
    text = "<reasoning>thinking</reasoning>Here is **bold** text and [a link](http://example.com)"
    result = _strip_reasoning_tag(text)
    assert result == "Here is **bold** text and [a link](http://example.com)"


def test_strip_reasoning_tag_handles_whitespace() -> None:
    """Test that whitespace around reasoning tags is handled correctly."""
    text = "Start\n<reasoning>thoughts</reasoning>\nEnd"
    result = _strip_reasoning_tag(text)
    assert "thoughts" not in result
    # The text should contain Start and End, though whitespace may vary
    assert "Start" in result
    assert "End" in result


def test_respond_to_user_strips_reasoning_tags(monkeypatch) -> None:
    """Test that respond_to_user strips <reasoning> tags from the response."""
    # Mock get_datahub_client to avoid needing a real client
    mock_client = MagicMock()
    mock_client._graph.frontend_base_url = "https://test.acryl.io"
    monkeypatch.setattr(
        "datahub_integrations.chat.chat_session.get_datahub_client", lambda: mock_client
    )

    # Test that reasoning tags are stripped
    response_with_xml = "<reasoning>internal thoughts</reasoning>Here is the actual response for the user"
    result = respond_to_user(response_with_xml)

    assert "internal thoughts" not in result.text
    assert "Here is the actual response for the user" in result.text


def test_respond_to_user_preserves_markdown(monkeypatch) -> None:
    """Test that respond_to_user preserves markdown formatting after stripping XML."""
    # Mock get_datahub_client
    mock_client = MagicMock()
    mock_client._graph.frontend_base_url = "https://test.acryl.io"
    monkeypatch.setattr(
        "datahub_integrations.chat.chat_session.get_datahub_client", lambda: mock_client
    )

    response_with_xml = "<reasoning>searching...</reasoning>Found **bold text** and [a link](http://example.com)"
    result = respond_to_user(response_with_xml)

    assert "searching" not in result.text
    assert "**bold text**" in result.text or "<strong>bold text</strong>" in result.text
    assert "link" in result.text


def test_respond_to_user_with_nested_reasoning_xml(monkeypatch) -> None:
    """Test that respond_to_user strips complex nested XML in reasoning tags."""
    # Mock get_datahub_client
    mock_client = MagicMock()
    mock_client._graph.frontend_base_url = "https://test.acryl.io"
    monkeypatch.setattr(
        "datahub_integrations.chat.chat_session.get_datahub_client", lambda: mock_client
    )

    # Simulate LLM including reasoning with nested XML tags
    response_with_nested_xml = """<reasoning>
<action>search for datasets</action>
<rationale>Need to find the right table</rationale>
<confidence>high</confidence>
</reasoning>I found the dataset you were looking for."""

    result = respond_to_user(response_with_nested_xml)

    # All reasoning content should be removed
    assert "action" not in result.text
    assert "search for datasets" not in result.text
    assert "rationale" not in result.text
    assert "confidence" not in result.text

    # But the actual response should be preserved
    assert "I found the dataset you were looking for." in result.text


def test_respond_to_user_handles_suggestions(monkeypatch) -> None:
    """Test that respond_to_user properly handles follow-up suggestions."""
    # Mock get_datahub_client
    mock_client = MagicMock()
    mock_client._graph.frontend_base_url = "https://test.acryl.io"
    monkeypatch.setattr(
        "datahub_integrations.chat.chat_session.get_datahub_client", lambda: mock_client
    )

    response = "Here is the answer"
    suggestions = ["What about X?", "Tell me more about Y"]

    result = respond_to_user(response, follow_up_suggestions=suggestions)

    assert result.text == "Here is the answer"
    assert result.suggestions == suggestions


def test_fallback_path_strips_reasoning_tags(monkeypatch) -> None:
    """
    Test that the fallback path (when LLM outputs directly without calling respond_to_user)
    also strips <reasoning> tags from the response.
    """
    # Mock get_datahub_client
    mock_client = MagicMock()
    mock_client._graph.frontend_base_url = "https://test.acryl.io"

    # Create a minimal ChatSession
    session = ChatSession(
        tools=[],
        client=mock_client,
    )

    # Simulate the content block that would come from Bedrock with XML tags
    content_block = {
        "text": "<reasoning>internal thoughts</reasoning>This is the final response to the user"
    }

    # Call _handle_text_content with is_end_turn=True to trigger fallback path
    session._handle_text_content(
        content_block=cast(Any, content_block), is_end_turn=True, is_last_block=True
    )

    # Check that the last message is an AssistantMessage with XML stripped
    last_message = session.history.messages[-1]
    assert isinstance(last_message, AssistantMessage)
    assert "internal thoughts" not in last_message.text
    assert "This is the final response to the user" in last_message.text


def test_fallback_path_strips_nested_reasoning_xml(monkeypatch) -> None:
    """Test fallback path with complex nested XML in reasoning tags."""
    # Mock get_datahub_client
    mock_client = MagicMock()
    mock_client._graph.frontend_base_url = "https://test.acryl.io"

    # Create a minimal ChatSession
    session = ChatSession(
        tools=[],
        client=mock_client,
    )

    # Simulate complex nested XML
    content_block = {
        "text": """<reasoning>
<action>get_entities</action>
<rationale>User asked for dataset info</rationale>
<user_requested>dataset information</user_requested>
<confidence>high</confidence>
</reasoning>The dataset contains customer data with 50 columns."""
    }

    # Call _handle_text_content with is_end_turn=True
    session._handle_text_content(
        content_block=cast(Any, content_block), is_end_turn=True, is_last_block=True
    )

    # Check that all nested XML is removed
    last_message = session.history.messages[-1]
    assert isinstance(last_message, AssistantMessage)
    assert "action" not in last_message.text
    assert "get_entities" not in last_message.text
    assert "rationale" not in last_message.text
    assert "user_requested" not in last_message.text

    # But actual response is preserved
    assert "The dataset contains customer data with 50 columns." in last_message.text
