"""Unit tests for datahub_ai_conversation_client.py"""

from unittest.mock import Mock

import pytest

from datahub_integrations.chat.chat_history import (
    AssistantMessage,
    ChatHistory,
    HumanMessage,
)
from datahub_integrations.chat.datahub_ai_conversation_client import (
    DataHubAiConversationClient,
    map_origin_type_to_chat_type,
)
from datahub_integrations.chat.types import ChatType


@pytest.fixture
def mock_datahub_client() -> Mock:
    """Create a mock DataHub client."""
    client = Mock()
    client._graph = Mock()
    client._graph.execute_graphql = Mock(
        return_value={"getDataHubAiConversation": {"messages": []}}
    )
    return client


@pytest.fixture
def conversation_manager(mock_datahub_client: Mock) -> DataHubAiConversationClient:
    """Create a conversation client with mocked client."""
    return DataHubAiConversationClient(client=mock_datahub_client)


def test_conversation_manager_initialization(
    conversation_manager: DataHubAiConversationClient,
) -> None:
    """Test that DataHubAiConversationClient initializes correctly."""
    assert conversation_manager.client is not None
    assert hasattr(conversation_manager, "_conversations")


def test_load_from_graphql_empty_conversation(
    conversation_manager: DataHubAiConversationClient, mock_datahub_client: Mock
) -> None:
    """Test loading an empty conversation from GraphQL."""
    mock_datahub_client._graph.execute_graphql.return_value = {
        "getDataHubAiConversation": {"messages": []}
    }

    history = conversation_manager.load_chat_history_from_graphql(
        "urn:li:dataHubAiConversation:test123"
    )

    assert isinstance(history, ChatHistory)
    assert len(history.messages) == 0
    mock_datahub_client._graph.execute_graphql.assert_called_once()


def test_load_from_graphql_with_messages(
    conversation_manager: DataHubAiConversationClient, mock_datahub_client: Mock
) -> None:
    """Test loading a conversation with messages from GraphQL."""
    mock_datahub_client._graph.execute_graphql.return_value = {
        "getDataHubAiConversation": {
            "messages": [
                {
                    "type": "TEXT",
                    "content": {"text": "Hello!"},
                    "actor": {"type": "USER"},
                    "time": 1234567890,
                },
                {
                    "type": "TEXT",
                    "content": {"text": "Hi there!"},
                    "actor": {"type": "AGENT"},
                    "time": 1234567900,
                },
            ]
        }
    }

    history = conversation_manager.load_chat_history_from_graphql(
        "urn:li:dataHubAiConversation:test123"
    )

    assert isinstance(history, ChatHistory)
    assert len(history.messages) == 2
    assert isinstance(history.messages[0], HumanMessage)
    assert history.messages[0].text == "Hello!"
    assert isinstance(history.messages[1], AssistantMessage)
    assert history.messages[1].text == "Hi there!"


def test_load_from_graphql_conversation_not_found(
    conversation_manager: DataHubAiConversationClient, mock_datahub_client: Mock
) -> None:
    """Test loading a non-existent conversation."""
    mock_datahub_client._graph.execute_graphql.return_value = {
        "getDataHubAiConversation": None
    }

    history = conversation_manager.load_chat_history_from_graphql(
        "urn:li:dataHubAiConversation:test123"
    )

    # Should return empty history when conversation not found
    assert isinstance(history, ChatHistory)
    assert len(history.messages) == 0


def test_load_from_graphql_handles_tool_calls(
    conversation_manager: DataHubAiConversationClient, mock_datahub_client: Mock
) -> None:
    """Test loading conversation with tool call messages."""
    mock_datahub_client._graph.execute_graphql.return_value = {
        "getDataHubAiConversation": {
            "messages": [
                {
                    "type": "TOOL_CALL",
                    "content": {"text": "search_datasets(query='test')"},
                    "actor": {"type": "AGENT"},
                    "time": 1234567890,
                },
                {
                    "type": "TOOL_RESULT",
                    "content": {"text": "Found 5 datasets"},
                    "actor": {"type": "AGENT"},
                    "time": 1234567900,
                },
            ]
        }
    }

    history = conversation_manager.load_chat_history_from_graphql(
        "urn:li:dataHubAiConversation:test123"
    )

    assert isinstance(history, ChatHistory)
    assert len(history.messages) == 2


def test_graphql_query_format(
    conversation_manager: DataHubAiConversationClient, mock_datahub_client: Mock
) -> None:
    """Test that GraphQL query is formatted correctly."""
    conversation_manager.load_chat_history_from_graphql(
        "urn:li:dataHubAiConversation:test123"
    )

    # Verify the query was called
    call_args = mock_datahub_client._graph.execute_graphql.call_args
    assert call_args is not None

    # Check that the query includes the conversation URN
    query = call_args[0][0]  # First positional argument
    assert "getDataHubAiConversation" in query

    # Check variables
    variables = call_args[1]["variables"]
    assert variables["urn"] == "urn:li:dataHubAiConversation:test123"


def test_conversation_manager_with_invalid_urn() -> None:
    """Test that DataHubAiConversationClient handles invalid URNs."""
    mock_client = Mock()
    mock_client._graph = Mock()
    mock_client._graph.execute_graphql.return_value = {"getDataHubAiConversation": None}

    # Should not raise exception during initialization
    manager = DataHubAiConversationClient(client=mock_client)

    # Should return empty history for invalid URN
    history = manager.load_chat_history_from_graphql("invalid-urn")
    assert isinstance(history, ChatHistory)
    assert len(history.messages) == 0


def test_graphql_error_handling(
    conversation_manager: DataHubAiConversationClient, mock_datahub_client: Mock
) -> None:
    """Test handling of GraphQL errors."""
    mock_datahub_client._graph.execute_graphql.return_value = {
        "errors": [{"message": "GraphQL error"}]
    }

    # Should handle error gracefully and return empty history
    history = conversation_manager.load_chat_history_from_graphql(
        "urn:li:dataHubAiConversation:test123"
    )

    assert isinstance(history, ChatHistory)
    assert len(history.messages) == 0


def test_conversation_manager_client_reference(
    conversation_manager: DataHubAiConversationClient, mock_datahub_client: Mock
) -> None:
    """Test that conversation client maintains correct client reference."""
    assert conversation_manager.client is mock_datahub_client
    assert conversation_manager.client._graph is not None


# Origin type mapping tests
def test_map_datahub_ui_origin_type() -> None:
    """Test mapping DATAHUB_UI origin type."""
    result = map_origin_type_to_chat_type("DATAHUB_UI")
    assert result == ChatType.DATAHUB_UI


def test_map_slack_origin_type() -> None:
    """Test mapping SLACK origin type."""
    result = map_origin_type_to_chat_type("SLACK")
    assert result == ChatType.SLACK


def test_map_teams_origin_type() -> None:
    """Test mapping TEAMS origin type."""
    result = map_origin_type_to_chat_type("TEAMS")
    assert result == ChatType.TEAMS


def test_map_unknown_origin_type() -> None:
    """Test mapping unknown origin type (should default to DEFAULT)."""
    result = map_origin_type_to_chat_type("UNKNOWN_TYPE")
    assert result == ChatType.DEFAULT


def test_map_none_origin_type() -> None:
    """Test mapping None origin type (should default to DEFAULT)."""
    result = map_origin_type_to_chat_type(None)
    assert result == ChatType.DEFAULT


def test_map_empty_origin_type() -> None:
    """Test mapping empty string origin type (should default to DEFAULT)."""
    result = map_origin_type_to_chat_type("")
    assert result == ChatType.DEFAULT


# Save message tests
def test_save_message_to_conversation() -> None:
    """Test save_message_to_conversation appends messages and emits MCP."""
    from datahub.metadata.schema_classes import (
        DataHubAiConversationActorTypeClass,
        DataHubAiConversationInfoClass,
        DataHubAiConversationMessageTypeClass,
    )

    # Mock client with _graph
    mock_client = Mock()
    mock_graph = Mock()
    mock_client._graph = mock_graph

    # Mock existing conversation with no title
    from datahub.metadata.schema_classes import AuditStampClass

    existing_conversation = Mock(spec=DataHubAiConversationInfoClass)
    existing_conversation.messages = []
    existing_conversation.title = None
    existing_conversation.originType = "DATAHUB_UI"
    existing_conversation.created = AuditStampClass(
        time=1234567890, actor="urn:li:corpuser:admin"
    )
    # Mock to_obj to return proper dict
    existing_conversation.to_obj.return_value = {
        "messages": [],
        "created": {"time": 1234567890, "actor": "urn:li:corpuser:admin"},
        "originType": "DATAHUB_UI",
    }

    mock_graph.get_aspect = Mock(return_value=existing_conversation)
    mock_graph.emit = Mock()

    client = DataHubAiConversationClient(client=mock_client)

    # Call save_message_to_conversation
    client.save_message_to_conversation(
        conversation_urn="urn:li:dataHubAiConversation:123",
        actor_urn="urn:li:corpuser:test",
        actor_type=DataHubAiConversationActorTypeClass.USER,  # type: ignore[arg-type]
        message_type=DataHubAiConversationMessageTypeClass.TEXT,  # type: ignore[arg-type]
        text="Hello world",
        timestamp=1234567890,
    )

    # Verify get_aspect was called
    mock_graph.get_aspect.assert_called_once_with(
        entity_urn="urn:li:dataHubAiConversation:123",
        aspect_type=DataHubAiConversationInfoClass,
    )

    # Verify emit was called with emit_mode=SYNC_PRIMARY
    from datahub.emitter.rest_emitter import EmitMode

    mock_graph.emit.assert_called_once()
    call_args = mock_graph.emit.call_args
    assert call_args[1]["emit_mode"] == EmitMode.SYNC_PRIMARY

    # Verify emit was called with the new aspect
    # The original aspect should not be modified; a new one is created
    assert len(existing_conversation.messages) == 0  # Original not modified

    # Check that emit was called with a new aspect containing the message
    call_args = mock_graph.emit.call_args
    emitted_mcp = call_args[0][0]
    emitted_aspect = emitted_mcp.aspect

    assert len(emitted_aspect.messages) == 1
    assert emitted_aspect.messages[0].content.text == "Hello world"
    assert emitted_aspect.title == "Hello world"


def test_save_message_to_conversation_nonexistent_urn() -> None:
    """Test save_message_to_conversation handles error when conversation doesn't exist."""
    from datahub.metadata.schema_classes import (
        DataHubAiConversationActorTypeClass,
        DataHubAiConversationMessageTypeClass,
    )

    # Mock client with _graph
    mock_client = Mock()
    mock_graph = Mock()
    mock_client._graph = mock_graph

    # Mock non-existent conversation (returns None)
    mock_graph.get_aspect = Mock(return_value=None)
    mock_graph.emit = Mock()

    client = DataHubAiConversationClient(client=mock_client)

    # Should not raise, but should log error (error is caught internally)
    client.save_message_to_conversation(
        conversation_urn="urn:li:dataHubAiConversation:nonexistent",
        actor_urn="urn:li:corpuser:test",
        actor_type=DataHubAiConversationActorTypeClass.USER,  # type: ignore[arg-type]
        message_type=DataHubAiConversationMessageTypeClass.TEXT,  # type: ignore[arg-type]
        text="Hello",
        timestamp=1234567890,
    )

    # Verify emit was NOT called
    mock_graph.emit.assert_not_called()


def test_save_message_initializes_empty_messages_list() -> None:
    """Test save_message_to_conversation initializes empty messages list if None."""
    from datahub.metadata.schema_classes import (
        DataHubAiConversationActorTypeClass,
        DataHubAiConversationInfoClass,
        DataHubAiConversationMessageTypeClass,
    )

    # Mock client with _graph
    mock_client = Mock()
    mock_graph = Mock()
    mock_client._graph = mock_graph

    # Mock existing conversation with None messages
    from datahub.metadata.schema_classes import AuditStampClass

    existing_conversation = Mock(spec=DataHubAiConversationInfoClass)
    existing_conversation.messages = None
    existing_conversation.title = None
    existing_conversation.originType = "DATAHUB_UI"
    existing_conversation.created = AuditStampClass(
        time=1234567890, actor="urn:li:corpuser:admin"
    )
    # Mock to_obj to return proper dict
    existing_conversation.to_obj.return_value = {
        "messages": [],  # to_obj converts None to []
        "created": {"time": 1234567890, "actor": "urn:li:corpuser:admin"},
        "originType": "DATAHUB_UI",
    }

    mock_graph.get_aspect = Mock(return_value=existing_conversation)
    mock_graph.emit = Mock()

    client = DataHubAiConversationClient(client=mock_client)

    client.save_message_to_conversation(
        conversation_urn="urn:li:dataHubAiConversation:123",
        actor_urn="urn:li:corpuser:test",
        actor_type=DataHubAiConversationActorTypeClass.USER,  # type: ignore[arg-type]
        message_type=DataHubAiConversationMessageTypeClass.TEXT,  # type: ignore[arg-type]
        text="Hello",
        timestamp=1234567890,
    )

    # Verify a new aspect was created with the message
    # Original should remain None
    assert existing_conversation.messages is None

    # Check emitted aspect has the message
    call_args = mock_graph.emit.call_args
    emitted_mcp = call_args[0][0]
    emitted_aspect = emitted_mcp.aspect
    assert len(emitted_aspect.messages) == 1


def test_save_message_sets_title_on_first_user_message() -> None:
    """Test that title is set to first user message."""
    from datahub.metadata.schema_classes import (
        AuditStampClass,
        DataHubAiConversationActorTypeClass,
        DataHubAiConversationInfoClass,
        DataHubAiConversationMessageTypeClass,
    )

    # Mock client with _graph
    mock_client = Mock()
    mock_graph = Mock()
    mock_client._graph = mock_graph

    # Mock existing conversation with no title
    existing_conversation = Mock(spec=DataHubAiConversationInfoClass)
    existing_conversation.messages = []
    existing_conversation.title = None
    existing_conversation.originType = "DATAHUB_UI"
    existing_conversation.created = AuditStampClass(
        time=1234567890, actor="urn:li:corpuser:admin"
    )
    # Mock to_obj to return proper dict
    existing_conversation.to_obj.return_value = {
        "messages": [],
        "created": {"time": 1234567890, "actor": "urn:li:corpuser:admin"},
        "originType": "DATAHUB_UI",
    }

    mock_graph.get_aspect = Mock(return_value=existing_conversation)
    mock_graph.emit = Mock()

    client = DataHubAiConversationClient(client=mock_client)

    # First user message should set title
    client.save_message_to_conversation(
        conversation_urn="urn:li:dataHubAiConversation:123",
        actor_urn="urn:li:corpuser:test",
        actor_type=DataHubAiConversationActorTypeClass.USER,  # type: ignore[arg-type]
        message_type=DataHubAiConversationMessageTypeClass.TEXT,  # type: ignore[arg-type]
        text="What datasets do we have?",
        timestamp=1234567890,
    )

    # Verify title was set in emitted aspect
    call_args = mock_graph.emit.call_args
    emitted_mcp = call_args[0][0]
    emitted_aspect = emitted_mcp.aspect
    assert emitted_aspect.title == "What datasets do we have?"


def test_save_message_does_not_overwrite_existing_title() -> None:
    """Test that title is not overwritten if already set."""
    from datahub.metadata.schema_classes import (
        DataHubAiConversationActorTypeClass,
        DataHubAiConversationInfoClass,
        DataHubAiConversationMessageTypeClass,
    )

    # Mock client with _graph
    mock_client = Mock()
    mock_graph = Mock()
    mock_client._graph = mock_graph

    # Mock existing conversation with existing title
    from datahub.metadata.schema_classes import AuditStampClass

    existing_conversation = Mock(spec=DataHubAiConversationInfoClass)
    existing_conversation.messages = []
    existing_conversation.title = "Original Title"
    existing_conversation.originType = "DATAHUB_UI"
    existing_conversation.created = AuditStampClass(
        time=1234567890, actor="urn:li:corpuser:admin"
    )
    # Mock to_obj to return proper dict
    existing_conversation.to_obj.return_value = {
        "messages": [],
        "title": "Original Title",
        "created": {"time": 1234567890, "actor": "urn:li:corpuser:admin"},
        "originType": "DATAHUB_UI",
    }

    mock_graph.get_aspect = Mock(return_value=existing_conversation)
    mock_graph.emit = Mock()

    client = DataHubAiConversationClient(client=mock_client)

    # New message should not overwrite title
    client.save_message_to_conversation(
        conversation_urn="urn:li:dataHubAiConversation:123",
        actor_urn="urn:li:corpuser:test",
        actor_type=DataHubAiConversationActorTypeClass.USER,  # type: ignore[arg-type]
        message_type=DataHubAiConversationMessageTypeClass.TEXT,  # type: ignore[arg-type]
        text="What datasets do we have?",
        timestamp=1234567890,
    )

    # Verify title was NOT changed in emitted aspect
    call_args = mock_graph.emit.call_args
    emitted_mcp = call_args[0][0]
    emitted_aspect = emitted_mcp.aspect
    assert emitted_aspect.title == "Original Title"


def test_save_message_truncates_long_title() -> None:
    """Test that long titles are truncated to 100 characters."""
    from datahub.metadata.schema_classes import (
        DataHubAiConversationActorTypeClass,
        DataHubAiConversationInfoClass,
        DataHubAiConversationMessageTypeClass,
    )

    # Mock client with _graph
    mock_client = Mock()
    mock_graph = Mock()
    mock_client._graph = mock_graph

    # Mock existing conversation with no title
    from datahub.metadata.schema_classes import AuditStampClass

    existing_conversation = Mock(spec=DataHubAiConversationInfoClass)
    existing_conversation.messages = []
    existing_conversation.title = None
    existing_conversation.originType = "DATAHUB_UI"
    existing_conversation.created = AuditStampClass(
        time=1234567890, actor="urn:li:corpuser:admin"
    )
    # Mock to_obj to return proper dict
    existing_conversation.to_obj.return_value = {
        "messages": [],
        "created": {"time": 1234567890, "actor": "urn:li:corpuser:admin"},
        "originType": "DATAHUB_UI",
    }

    mock_graph.get_aspect = Mock(return_value=existing_conversation)
    mock_graph.emit = Mock()

    client = DataHubAiConversationClient(client=mock_client)

    # Very long message
    long_text = "a" * 200

    client.save_message_to_conversation(
        conversation_urn="urn:li:dataHubAiConversation:123",
        actor_urn="urn:li:corpuser:test",
        actor_type=DataHubAiConversationActorTypeClass.USER,  # type: ignore[arg-type]
        message_type=DataHubAiConversationMessageTypeClass.TEXT,  # type: ignore[arg-type]
        text=long_text,
        timestamp=1234567890,
    )

    # Verify title was truncated to 100 chars in emitted aspect
    call_args = mock_graph.emit.call_args
    emitted_mcp = call_args[0][0]
    emitted_aspect = emitted_mcp.aspect
    assert emitted_aspect.title is not None
    assert len(emitted_aspect.title) == 100
    assert emitted_aspect.title == "a" * 100


def test_conversation_message_limit_applied():
    """Test that conversations with too many messages are limited to the most recent N."""
    from datahub_integrations.chat.config import CHAT_MAX_CONVERSATION_MESSAGES
    from datahub_integrations.chat.datahub_ai_conversation_client import (
        convert_graphql_messages_to_chat_history,
    )

    # Create more messages than the limit
    num_messages = CHAT_MAX_CONVERSATION_MESSAGES + 100
    graphql_messages = []
    for i in range(num_messages):
        graphql_messages.append(
            {
                "type": "TEXT",
                "actor": {"type": "USER" if i % 2 == 0 else "AGENT"},
                "content": {"text": f"Message {i}"},
            }
        )

    # Convert to chat history
    chat_history = convert_graphql_messages_to_chat_history(graphql_messages)

    # Should be limited to most recent messages
    assert len(chat_history.messages) == CHAT_MAX_CONVERSATION_MESSAGES

    # Verify we kept the most recent messages (last N)
    # The last message should be "Message {num_messages - 1}"
    assert f"Message {num_messages - 1}" in chat_history.messages[-1].text


def test_conversation_under_limit_unchanged():
    """Test that conversations under the limit are not modified."""
    from datahub_integrations.chat.config import CHAT_MAX_CONVERSATION_MESSAGES
    from datahub_integrations.chat.datahub_ai_conversation_client import (
        convert_graphql_messages_to_chat_history,
    )

    # Create fewer messages than the limit
    num_messages = CHAT_MAX_CONVERSATION_MESSAGES - 10
    graphql_messages = []
    for i in range(num_messages):
        graphql_messages.append(
            {
                "type": "TEXT",
                "actor": {"type": "USER" if i % 2 == 0 else "AGENT"},
                "content": {"text": f"Message {i}"},
            }
        )

    # Convert to chat history
    chat_history = convert_graphql_messages_to_chat_history(graphql_messages)

    # Should keep all messages
    assert len(chat_history.messages) == num_messages


def test_conversation_at_limit_unchanged():
    """Test that conversations at exactly the limit are not modified."""
    from datahub_integrations.chat.config import CHAT_MAX_CONVERSATION_MESSAGES
    from datahub_integrations.chat.datahub_ai_conversation_client import (
        convert_graphql_messages_to_chat_history,
    )

    # Create exactly the limit number of messages
    graphql_messages = []
    for i in range(CHAT_MAX_CONVERSATION_MESSAGES):
        graphql_messages.append(
            {
                "type": "TEXT",
                "actor": {"type": "USER" if i % 2 == 0 else "AGENT"},
                "content": {"text": f"Message {i}"},
            }
        )

    # Convert to chat history
    chat_history = convert_graphql_messages_to_chat_history(graphql_messages)

    # Should keep all messages
    assert len(chat_history.messages) == CHAT_MAX_CONVERSATION_MESSAGES


def test_conversation_limit_with_invalid_messages():
    """Test that message limit works correctly even when some messages are invalid."""
    from datahub_integrations.chat.config import CHAT_MAX_CONVERSATION_MESSAGES
    from datahub_integrations.chat.datahub_ai_conversation_client import (
        convert_graphql_messages_to_chat_history,
    )

    # Create more messages than the limit, including some invalid ones
    num_messages = CHAT_MAX_CONVERSATION_MESSAGES + 100
    graphql_messages = []
    for i in range(num_messages):
        if i % 10 == 0:
            # Every 10th message is invalid (empty text)
            graphql_messages.append(
                {
                    "type": "TEXT",
                    "actor": {"type": "USER"},
                    "content": {"text": ""},  # Invalid: empty text
                }
            )
        else:
            graphql_messages.append(
                {
                    "type": "TEXT",
                    "actor": {"type": "USER" if i % 2 == 0 else "AGENT"},
                    "content": {"text": f"Message {i}"},
                }
            )

    # Convert to chat history
    chat_history = convert_graphql_messages_to_chat_history(graphql_messages)

    # Should be limited to most recent valid messages
    assert len(chat_history.messages) <= CHAT_MAX_CONVERSATION_MESSAGES


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
