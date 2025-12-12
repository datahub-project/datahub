"""Unit tests for chat_session_manager.py"""

from unittest.mock import Mock, patch

import pytest

from datahub_integrations.chat.agent.progress_tracker import ProgressUpdate
from datahub_integrations.chat.chat_api import ChatContext
from datahub_integrations.chat.chat_history import ChatHistory
from datahub_integrations.chat.chat_session_manager import (
    ChatMessageEvent,
    ChatSessionManager,
)
from datahub_integrations.chat.datahub_ai_conversation_client import (
    DataHubAiConversationClient,
)
from datahub_integrations.chat.types import ChatType, NextMessage


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
def mock_conversation_manager(mock_datahub_client: Mock) -> Mock:
    """Create a mock conversation client."""
    manager = Mock(spec=DataHubAiConversationClient)
    manager.client = mock_datahub_client
    manager.load_chat_history = Mock(return_value=ChatHistory(messages=[]))
    return manager


def test_create_manager_instance(mock_datahub_client: Mock) -> None:
    manager = ChatSessionManager(
        system_client=mock_datahub_client, tools_client=mock_datahub_client
    )
    assert isinstance(manager, ChatSessionManager)


def test_create_session(mock_datahub_client: Mock) -> None:
    manager = ChatSessionManager(
        system_client=mock_datahub_client, tools_client=mock_datahub_client
    )
    session = manager.create_session()
    assert session is not None


def test_chat_session_manager_loads_history(mock_datahub_client: Mock) -> None:
    """Test that ChatSessionManager loads conversation history."""
    # no-op

    with patch(
        "datahub_integrations.chat.chat_session_manager.DataHubAiConversationClient"
    ) as cm:
        instance = cm.return_value
        instance.load_conversation_with_metadata.return_value = (
            ChatHistory(messages=[]),
            Mock(),
            None,
        )

        manager = ChatSessionManager(
            system_client=mock_datahub_client, tools_client=mock_datahub_client
        )
        manager.load_session("urn:li:dataHubAiConversation:test")
        instance.load_conversation_with_metadata.assert_called_once()


def test_chat_session_manager_with_empty_history(mock_datahub_client: Mock) -> None:
    """Test creating a session manager with an empty conversation history."""
    with patch(
        "datahub_integrations.chat.chat_session_manager.DataHubAiConversationClient"
    ) as cm:
        instance = cm.return_value
        instance.load_conversation_with_metadata.return_value = (
            ChatHistory(messages=[]),
            Mock(),
            None,
        )
        manager = ChatSessionManager(
            system_client=mock_datahub_client, tools_client=mock_datahub_client
        )
        session = manager.load_session("urn:li:dataHubAiConversation:test")
        assert session is not None


def test_chat_session_manager_client_reference(mock_conversation_manager: Mock) -> None:
    """Test that session manager maintains reference to client through conversation client."""
    manager = ChatSessionManager(
        system_client=mock_conversation_manager.client,
        tools_client=mock_conversation_manager.client,
    )
    assert manager.conversation_manager is not None
    assert manager.conversation_manager.client is not None


def test_load_session_handles_any_urn() -> None:
    mock_client = Mock()
    with patch(
        "datahub_integrations.chat.chat_session_manager.DataHubAiConversationClient"
    ) as cm:
        instance = cm.return_value
        instance.load_conversation_with_metadata.return_value = (
            ChatHistory(messages=[]),
            Mock(),
            None,
        )
        manager = ChatSessionManager(
            system_client=mock_client, tools_client=mock_client
        )
        session = manager.load_session("urn:li:dataHubAiConversation:test")
        assert session is not None


def test_chat_session_manager_uses_composition() -> None:
    """Test that ChatSessionManager uses composition pattern."""
    # Verify that ChatSessionManager has the expected instance attributes for composition
    mock_client = Mock()

    manager = ChatSessionManager(system_client=mock_client, tools_client=mock_client)
    assert hasattr(manager, "conversation_manager")


def test_add_user_message(mock_datahub_client: Mock) -> None:
    manager = ChatSessionManager(
        system_client=mock_datahub_client, tools_client=mock_datahub_client
    )
    session = manager.create_session()
    initial = len(session.history.messages)
    manager.add_user_message(session, "hello")
    assert len(session.history.messages) == initial + 1


def test_chat_session_manager_set_progress_callback(mock_datahub_client: Mock) -> None:
    """Test that ChatSessionManager delegates set_progress_callback to AgentRunner."""
    mock_chat_session = Mock()
    mock_chat_session.set_progress_callback = Mock()
    mock_chat_session.history = ChatHistory(messages=[])

    with patch(
        "datahub_integrations.chat.chat_session_manager.DataHubAiConversationClient"
    ) as mock_conversation_manager_class:
        mock_conversation_manager = Mock()
        mock_conversation_manager.load_chat_history.return_value = ChatHistory(
            messages=[]
        )
        mock_conversation_manager_class.return_value = mock_conversation_manager

        manager = ChatSessionManager(
            system_client=mock_datahub_client, tools_client=mock_datahub_client
        )
        session = manager.create_session()
        test_callback = Mock()
        # Ensure _generate_with_progress wires the callback and calls generate_formatted_message
        from contextlib import nullcontext

        with patch.object(
            session, "set_progress_callback", return_value=nullcontext()
        ) as mock_set_progress:
            with patch.object(session, "generate_formatted_message"):
                manager._generate_with_progress(session, test_callback)
                mock_set_progress.assert_called_once_with(test_callback)


def test_generate_with_progress_without_callback(mock_datahub_client: Mock) -> None:
    """Test that _generate_with_progress works without a progress callback."""
    manager = ChatSessionManager(
        system_client=mock_datahub_client, tools_client=mock_datahub_client
    )
    session = manager.create_session()

    # Mock generate_formatted_message to return a response
    with patch.object(
        session,
        "generate_formatted_message",
        return_value=NextMessage(text="Test response", suggestions=[]),
    ) as mock_generate:
        result = manager._generate_with_progress(session, progress_callback=None)

        assert result.text == "Test response"
        mock_generate.assert_called_once()


def test_generate_with_progress_with_callback(mock_datahub_client: Mock) -> None:
    """Test that _generate_with_progress properly wires up progress callback."""
    manager = ChatSessionManager(
        system_client=mock_datahub_client, tools_client=mock_datahub_client
    )
    session = manager.create_session()

    from contextlib import nullcontext

    callback = Mock()

    with patch.object(
        session, "set_progress_callback", return_value=nullcontext()
    ) as mock_set_progress:
        with patch.object(
            session,
            "generate_formatted_message",
            return_value=NextMessage(text="Response", suggestions=[]),
        ):
            result = manager._generate_with_progress(
                session, progress_callback=callback
            )

            mock_set_progress.assert_called_once_with(callback)
            assert result.text == "Response"


# Test removed - conversation_urn is now required


def test_send_message_loads_existing_session(
    mock_datahub_client: Mock,
) -> None:
    """Test send_message loads existing session."""
    with patch(
        "datahub_integrations.chat.chat_session_manager.DataHubAiConversationClient"
    ) as mock_cm:
        mock_instance = mock_cm.return_value
        mock_instance.load_conversation_with_metadata.return_value = (
            ChatHistory(messages=[]),
            ChatType.SLACK,
            None,
        )
        mock_instance.save_message_to_conversation = Mock()

        manager = ChatSessionManager(
            system_client=mock_datahub_client, tools_client=mock_datahub_client
        )

        with patch.object(manager, "load_session") as mock_load:
            mock_session = Mock()
            mock_session.history = ChatHistory(messages=[])
            mock_session.generate_formatted_message = Mock(
                return_value=NextMessage(text="Response", suggestions=[])
            )
            mock_session.set_progress_callback = Mock(
                return_value=Mock(__enter__=Mock(), __exit__=Mock())
            )
            mock_load.return_value = mock_session

            urn = "urn:li:dataHubAiConversation:123"
            events = list(
                manager.send_message(
                    text="Hello", user_urn="urn:li:corpuser:test", conversation_urn=urn
                )
            )

            # Should load the existing session
            mock_load.assert_called_once_with(urn, "DataCatalogExplorer", None)

            # Check conversation_urn is set correctly in events
            assert all(e.conversation_urn == urn for e in events)


def test_send_message_yields_progress_updates(mock_datahub_client: Mock) -> None:
    """Test send_message yields progress updates from the callback."""
    manager = ChatSessionManager(
        system_client=mock_datahub_client, tools_client=mock_datahub_client
    )

    # Capture the progress callback
    captured_callback = None

    def mock_generate_with_progress(session, progress_callback=None):
        nonlocal captured_callback
        captured_callback = progress_callback

        # Simulate progress updates
        if progress_callback:
            progress_callback(
                [
                    ProgressUpdate(text="Thinking step 1", message_type="THINKING"),
                    ProgressUpdate(text="Thinking step 2", message_type="THINKING"),
                ]
            )

        return NextMessage(text="Final response", suggestions=[])

    with patch(
        "datahub_integrations.chat.chat_session_manager.DataHubAiConversationClient"
    ) as mock_cm:
        mock_instance = mock_cm.return_value
        mock_instance.load_conversation_with_metadata.return_value = (
            ChatHistory(messages=[]),
            ChatType.DATAHUB_UI,
            None,
        )
        mock_instance.save_message_to_conversation = Mock()

        with patch.object(
            manager, "_generate_with_progress", side_effect=mock_generate_with_progress
        ):
            urn = "urn:li:dataHubAiConversation:123"
            events = list(
                manager.send_message(
                    text="Test",
                    user_urn="urn:li:corpuser:test",
                    conversation_urn=urn,
                )
            )

            # Should have: user message + 2 progress + final response
            assert len(events) >= 4

            # Check progress events
            thinking_events = [e for e in events if e.message_type == "THINKING"]
            assert len(thinking_events) == 2
            assert thinking_events[0].text == "Thinking step 1"
            assert thinking_events[1].text == "Thinking step 2"


def test_send_message_handles_errors_gracefully(mock_datahub_client: Mock) -> None:
    """Test send_message yields error event when generation fails."""
    manager = ChatSessionManager(
        system_client=mock_datahub_client, tools_client=mock_datahub_client
    )

    def mock_generate_error(session, progress_callback=None):
        raise ValueError("Test error during generation")

    with patch(
        "datahub_integrations.chat.chat_session_manager.DataHubAiConversationClient"
    ) as mock_cm:
        mock_instance = mock_cm.return_value
        mock_instance.load_conversation_with_metadata.return_value = (
            ChatHistory(messages=[]),
            ChatType.DATAHUB_UI,
            None,
        )
        mock_instance.save_message_to_conversation = Mock()

        with patch.object(
            manager, "_generate_with_progress", side_effect=mock_generate_error
        ):
            urn = "urn:li:dataHubAiConversation:123"
            events = list(
                manager.send_message(
                    text="Test",
                    user_urn="urn:li:corpuser:test",
                    conversation_urn=urn,
                )
            )

            # Should have user message + error event
            assert len(events) >= 2

            # Last event should be an error
            error_event = events[-1]
            assert error_event.error is not None
            assert "Test error during generation" in error_event.error


def test_send_message_avoids_duplicate_progress_updates(
    mock_datahub_client: Mock,
) -> None:
    """Test that send_message doesn't send duplicate progress updates."""
    manager = ChatSessionManager(
        system_client=mock_datahub_client, tools_client=mock_datahub_client
    )

    captured_callback = None

    def mock_generate_with_progress(session, progress_callback=None):
        nonlocal captured_callback
        captured_callback = progress_callback

        # Simulate multiple callback invocations with cumulative updates
        # (as FilteredProgressListener does)
        if progress_callback:
            # First call - 1 update
            progress_callback([ProgressUpdate(text="Step 1", message_type="THINKING")])
            # Second call - 2 updates (cumulative)
            progress_callback(
                [
                    ProgressUpdate(text="Step 1", message_type="THINKING"),
                    ProgressUpdate(text="Step 2", message_type="THINKING"),
                ]
            )
            # Third call - 3 updates (cumulative)
            progress_callback(
                [
                    ProgressUpdate(text="Step 1", message_type="THINKING"),
                    ProgressUpdate(text="Step 2", message_type="THINKING"),
                    ProgressUpdate(text="Step 3", message_type="THINKING"),
                ]
            )

        return NextMessage(text="Done", suggestions=[])

    with patch(
        "datahub_integrations.chat.chat_session_manager.DataHubAiConversationClient"
    ) as mock_cm:
        mock_instance = mock_cm.return_value
        mock_instance.load_conversation_with_metadata.return_value = (
            ChatHistory(messages=[]),
            ChatType.DATAHUB_UI,
            None,
        )
        mock_instance.save_message_to_conversation = Mock()

        with patch.object(
            manager, "_generate_with_progress", side_effect=mock_generate_with_progress
        ):
            urn = "urn:li:dataHubAiConversation:123"
            events = list(
                manager.send_message(
                    text="Test",
                    user_urn="urn:li:corpuser:test",
                    conversation_urn=urn,
                )
            )

            # Should have: user message + 3 unique progress updates + final response
            thinking_events = [e for e in events if e.message_type == "THINKING"]

            # Should only have 3 thinking events (no duplicates)
            assert len(thinking_events) == 3
            assert thinking_events[0].text == "Step 1"
            assert thinking_events[1].text == "Step 2"
            assert thinking_events[2].text == "Step 3"


def test_send_message_yields_final_assistant_message(
    mock_datahub_client: Mock,
) -> None:
    """Test send_message yields the final assistant message."""
    manager = ChatSessionManager(
        system_client=mock_datahub_client, tools_client=mock_datahub_client
    )

    with patch(
        "datahub_integrations.chat.chat_session_manager.DataHubAiConversationClient"
    ) as mock_cm:
        mock_instance = mock_cm.return_value
        mock_instance.load_conversation_with_metadata.return_value = (
            ChatHistory(messages=[]),
            ChatType.DATAHUB_UI,
            None,
        )
        mock_instance.save_message_to_conversation = Mock()

        with patch.object(manager, "_generate_with_progress") as mock_generate:
            mock_generate.return_value = NextMessage(
                text="This is the AI response",
                suggestions=["suggestion1", "suggestion2"],
            )

            urn = "urn:li:dataHubAiConversation:123"
            events = list(
                manager.send_message(
                    text="Test",
                    user_urn="urn:li:corpuser:test",
                    conversation_urn=urn,
                )
            )

            # Last event should be the final assistant message
            final_event = events[-1]
            assert final_event.message_type == "TEXT"
            assert final_event.text == "This is the AI response"
            assert final_event.user_urn is None  # Not a user message


def test_chat_message_event_structure() -> None:
    """Test ChatMessageEvent dataclass structure."""
    event = ChatMessageEvent(
        message_type="THINKING",
        text="Test text",
        conversation_urn="urn:li:dataHubAiConversation:123",
        timestamp=1234567890,
        user_urn="urn:li:corpuser:test",
        error=None,
    )

    assert event.message_type == "THINKING"
    assert event.text == "Test text"
    assert event.conversation_urn == "urn:li:dataHubAiConversation:123"
    assert event.timestamp == 1234567890
    assert event.user_urn == "urn:li:corpuser:test"
    assert event.error is None


def test_chat_message_event_with_error() -> None:
    """Test ChatMessageEvent with error field."""
    event = ChatMessageEvent(
        message_type="TEXT",
        text="",
        conversation_urn="urn:li:dataHubAiConversation:123",
        timestamp=1234567890,
        error="Something went wrong",
    )

    assert event.error == "Something went wrong"
    assert event.user_urn is None


def test_load_session_with_different_chat_types(mock_datahub_client: Mock) -> None:
    """Test that load_session correctly passes chat_type to the agent factory."""
    with (
        patch(
            "datahub_integrations.chat.chat_session_manager.DataHubAiConversationClient"
        ) as mock_cm,
        patch(
            "datahub_integrations.chat.chat_session_manager.AGENT_FACTORIES"
        ) as mock_factories,
        patch("datahub_integrations.chat.chat_session_manager.mcp") as mock_mcp,
    ):
        mock_instance = mock_cm.return_value
        mock_agent = Mock()
        mock_factory = Mock(return_value=mock_agent)
        mock_factories.__contains__ = Mock(return_value=True)
        mock_factories.__getitem__ = Mock(return_value=mock_factory)

        # Test with SLACK type
        mock_instance.load_conversation_with_metadata.return_value = (
            ChatHistory(messages=[]),
            ChatType.SLACK,
            None,
        )

        manager = ChatSessionManager(
            system_client=mock_datahub_client, tools_client=mock_datahub_client
        )
        manager.load_session("urn:li:dataHubAiConversation:123")

        # Verify factory was called with SLACK type
        mock_factory.assert_called_with(
            client=mock_datahub_client,
            chat_type=ChatType.SLACK,
            history=ChatHistory(messages=[]),
            tools=[mock_mcp],
            context=None,
        )

        # Test with TEAMS type
        mock_instance.load_conversation_with_metadata.return_value = (
            ChatHistory(messages=[]),
            ChatType.TEAMS,
            None,
        )

        manager.load_session("urn:li:dataHubAiConversation:456")

        # Verify factory was called with TEAMS type
        mock_factory.assert_called_with(
            client=mock_datahub_client,
            chat_type=ChatType.TEAMS,
            history=ChatHistory(messages=[]),
            tools=[mock_mcp],
            context=None,
        )


def test_save_message_to_conversation(mock_datahub_client: Mock) -> None:
    """Test save_message_to_conversation appends messages and emits MCP."""
    # Mock existing conversation
    from datahub.metadata.schema_classes import (
        AuditStampClass,
        DataHubAiConversationActorTypeClass,
        DataHubAiConversationInfoClass,
        DataHubAiConversationMessageTypeClass,
    )

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

    # Mock _graph
    mock_graph = Mock()
    mock_datahub_client._graph = mock_graph
    mock_graph.get_aspect = Mock(return_value=existing_conversation)
    mock_graph.emit = Mock()

    manager = ChatSessionManager(
        system_client=mock_datahub_client, tools_client=mock_datahub_client
    )

    # Call save_message_to_conversation via conversation_manager
    manager.conversation_manager.save_message_to_conversation(
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

    # Verify emit was called with new aspect
    # Original should not be modified
    assert len(existing_conversation.messages) == 0

    # Check emitted aspect
    call_args = mock_graph.emit.call_args
    emitted_mcp = call_args[0][0]
    emitted_aspect = emitted_mcp.aspect
    assert len(emitted_aspect.messages) == 1
    assert emitted_aspect.messages[0].content.text == "Hello world"
    assert emitted_aspect.title == "Hello world"


def test_save_message_to_conversation_nonexistent_urn(
    mock_datahub_client: Mock,
) -> None:
    """Test save_message_to_conversation raises error when conversation doesn't exist."""
    from datahub.metadata.schema_classes import (
        DataHubAiConversationActorTypeClass,
        DataHubAiConversationMessageTypeClass,
    )

    # Mock _graph
    mock_graph = Mock()
    mock_datahub_client._graph = mock_graph
    mock_graph.get_aspect = Mock(return_value=None)
    mock_graph.emit = Mock()

    manager = ChatSessionManager(
        system_client=mock_datahub_client, tools_client=mock_datahub_client
    )

    # Should not raise, but should log error (error is caught internally)
    manager.conversation_manager.save_message_to_conversation(
        conversation_urn="urn:li:dataHubAiConversation:nonexistent",
        actor_urn="urn:li:corpuser:test",
        actor_type=DataHubAiConversationActorTypeClass.USER,  # type: ignore[arg-type]
        message_type=DataHubAiConversationMessageTypeClass.TEXT,  # type: ignore[arg-type]
        text="Hello",
        timestamp=1234567890,
    )

    # Verify emit was NOT called
    mock_graph.emit.assert_not_called()


def test_save_message_initializes_empty_messages_list(
    mock_datahub_client: Mock,
) -> None:
    """Test save_message_to_conversation initializes empty messages list if None."""
    # Mock existing conversation with None messages
    from datahub.metadata.schema_classes import (
        AuditStampClass,
        DataHubAiConversationActorTypeClass,
        DataHubAiConversationInfoClass,
        DataHubAiConversationMessageTypeClass,
    )

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

    # Mock _graph
    mock_graph = Mock()
    mock_datahub_client._graph = mock_graph
    mock_graph.get_aspect = Mock(return_value=existing_conversation)
    mock_graph.emit = Mock()

    manager = ChatSessionManager(
        system_client=mock_datahub_client, tools_client=mock_datahub_client
    )

    manager.conversation_manager.save_message_to_conversation(
        conversation_urn="urn:li:dataHubAiConversation:123",
        actor_urn="urn:li:corpuser:test",
        actor_type=DataHubAiConversationActorTypeClass.USER,  # type: ignore[arg-type]
        message_type=DataHubAiConversationMessageTypeClass.TEXT,  # type: ignore[arg-type]
        text="Hello",
        timestamp=1234567890,
    )

    # Verify new aspect was created
    # Original remains None
    assert existing_conversation.messages is None

    # Check emitted aspect
    call_args = mock_graph.emit.call_args
    emitted_mcp = call_args[0][0]
    emitted_aspect = emitted_mcp.aspect
    assert len(emitted_aspect.messages) == 1


def test_send_message_saves_messages(
    mock_datahub_client: Mock,
) -> None:
    """Test send_message saves user and AI messages."""
    # Mock existing conversation
    from datahub.metadata.schema_classes import (
        AuditStampClass,
        DataHubAiConversationInfoClass,
    )

    existing_conversation = Mock(spec=DataHubAiConversationInfoClass)
    existing_conversation.messages = []
    existing_conversation.originType = "DATAHUB_UI"
    existing_conversation.created = AuditStampClass(
        time=1234567890, actor="urn:li:corpuser:admin"
    )

    mock_datahub_client.get_aspect = Mock(return_value=existing_conversation)
    mock_datahub_client.emit = Mock()

    with patch(
        "datahub_integrations.chat.chat_session_manager.DataHubAiConversationClient"
    ) as mock_cm:
        mock_instance = mock_cm.return_value
        mock_instance.load_conversation_with_metadata.return_value = (
            ChatHistory(messages=[]),
            ChatType.DATAHUB_UI,
            None,
        )
        # Need to set up save_message_to_conversation to do nothing (not raise)
        mock_instance.save_message_to_conversation = Mock()

        manager = ChatSessionManager(
            system_client=mock_datahub_client, tools_client=mock_datahub_client
        )

        with patch.object(manager, "_generate_with_progress") as mock_generate:
            mock_generate.return_value = NextMessage(text="AI response", suggestions=[])

            urn = "urn:li:dataHubAiConversation:123"
            list(
                manager.send_message(
                    text="User message",
                    user_urn="urn:li:corpuser:test",
                    conversation_urn=urn,
                )
            )

            # Verify save_message_to_conversation was called twice (user + AI)
            assert mock_instance.save_message_to_conversation.call_count == 2


# Test removed - conversation_urn is now required


def test_send_message_saves_thinking_messages(
    mock_datahub_client: Mock,
) -> None:
    """Test that THINKING messages are saved to the conversation."""
    from datahub.metadata.schema_classes import (
        AuditStampClass,
        DataHubAiConversationInfoClass,
    )

    # Mock existing conversation
    existing_conversation = Mock(spec=DataHubAiConversationInfoClass)
    existing_conversation.messages = []
    existing_conversation.originType = "DATAHUB_UI"
    existing_conversation.created = AuditStampClass(
        time=1234567890, actor="urn:li:corpuser:admin"
    )

    with patch(
        "datahub_integrations.chat.chat_session_manager.DataHubAiConversationClient"
    ) as mock_cm:
        mock_instance = mock_cm.return_value
        mock_instance.load_conversation_with_metadata.return_value = (
            ChatHistory(messages=[]),
            ChatType.DATAHUB_UI,
            None,
        )
        # Mock save_message_to_conversation
        mock_instance.save_message_to_conversation = Mock()

        manager = ChatSessionManager(
            system_client=mock_datahub_client, tools_client=mock_datahub_client
        )

        # Mock _generate_with_progress to return progress updates with THINKING messages
        def mock_generate_with_thinking(session, callback):
            # Simulate THINKING updates
            callback(
                [
                    ProgressUpdate(message_type="THINKING", text="Analyzing query..."),
                    ProgressUpdate(
                        message_type="THINKING", text="Searching knowledge base..."
                    ),
                ]
            )
            return NextMessage(text="Here's the answer", suggestions=[])

        with patch.object(
            manager, "_generate_with_progress", side_effect=mock_generate_with_thinking
        ):
            urn = "urn:li:dataHubAiConversation:123"
            # Consume all events to allow the stream to complete
            list(
                manager.send_message(
                    text="User message",
                    user_urn="urn:li:corpuser:test",
                    conversation_urn=urn,
                )
            )

            # Verify THINKING messages were saved
            # Should have: 1 user TEXT + 2 THINKING + 1 AI TEXT = 4 saves
            assert mock_instance.save_message_to_conversation.call_count == 4

            # Check that THINKING messages were saved
            thinking_calls = [
                call
                for call in mock_instance.save_message_to_conversation.call_args_list
                if call[1].get("message_type")
                and "THINKING" in str(call[1]["message_type"])
            ]
            assert len(thinking_calls) == 2

            # Verify THINKING message contents
            assert "Analyzing query" in str(thinking_calls[0])
            assert "Searching knowledge base" in str(thinking_calls[1])


def test_send_message_stops_saving_on_early_exit(
    mock_datahub_client: Mock,
) -> None:
    """Test that messages stop being saved if generator exits early (client disconnect)."""
    from datahub.metadata.schema_classes import (
        AuditStampClass,
        DataHubAiConversationInfoClass,
    )

    # Mock existing conversation
    existing_conversation = Mock(spec=DataHubAiConversationInfoClass)
    existing_conversation.messages = []
    existing_conversation.originType = "DATAHUB_UI"
    existing_conversation.created = AuditStampClass(
        time=1234567890, actor="urn:li:corpuser:admin"
    )

    with patch(
        "datahub_integrations.chat.chat_session_manager.DataHubAiConversationClient"
    ) as mock_cm:
        mock_instance = mock_cm.return_value
        mock_instance.load_conversation_with_metadata.return_value = (
            ChatHistory(messages=[]),
            ChatType.DATAHUB_UI,
            None,
        )
        # Mock save_message_to_conversation
        mock_instance.save_message_to_conversation = Mock()

        manager = ChatSessionManager(
            system_client=mock_datahub_client, tools_client=mock_datahub_client
        )

        # Mock _generate_with_progress to return progress updates
        def mock_generate_with_slow_thinking(session, callback):
            import time

            # Simulate first THINKING update
            callback([ProgressUpdate(message_type="THINKING", text="Thinking...")])
            # Wait a bit before returning (simulating long generation)
            time.sleep(0.1)
            return NextMessage(text="Answer", suggestions=[])

        with patch.object(
            manager,
            "_generate_with_progress",
            side_effect=mock_generate_with_slow_thinking,
        ):
            urn = "urn:li:dataHubAiConversation:123"
            gen = manager.send_message(
                text="User message",
                user_urn="urn:li:corpuser:test",
                conversation_urn=urn,
            )

            # Consume only first 2 events (user message + first THINKING), then stop
            # This simulates client disconnecting early
            events = []
            for i, event in enumerate(gen):
                events.append(event)
                if i >= 1:  # Stop after 2 events
                    break

            # Give the background thread a moment to finish
            import time

            time.sleep(0.2)

            # Should have saved: 1 user TEXT + 1 THINKING = 2 saves
            # Final AI TEXT should NOT be saved because we exited early
            save_count = mock_instance.save_message_to_conversation.call_count
            assert save_count <= 2, f"Expected ≤2 saves, got {save_count}"

            # Verify no final TEXT message was saved (only user and thinking)
            text_saves = [
                call
                for call in mock_instance.save_message_to_conversation.call_args_list
                if call[1].get("message_type")
                and "TEXT" in str(call[1]["message_type"])
            ]
            # Should only have the initial user TEXT message, not the final AI response
            assert len(text_saves) == 1


def test_load_session_with_message_context(mock_datahub_client: Mock) -> None:
    """Test that load_session accepts and combines message context."""
    with (
        patch(
            "datahub_integrations.chat.chat_session_manager.DataHubAiConversationClient"
        ) as mock_cm,
        patch(
            "datahub_integrations.chat.chat_session_manager.AGENT_FACTORIES"
        ) as mock_factories,
        patch("datahub_integrations.chat.chat_session_manager.mcp") as mock_mcp,
    ):
        mock_instance = mock_cm.return_value
        mock_agent = Mock()
        mock_factory = Mock(return_value=mock_agent)
        mock_factories.__contains__ = Mock(return_value=True)
        mock_factories.__getitem__ = Mock(return_value=mock_factory)

        conversation_context = "User is editing a source"
        mock_instance.load_conversation_with_metadata.return_value = (
            ChatHistory(messages=[]),
            ChatType.DATAHUB_UI,
            conversation_context,
        )

        manager = ChatSessionManager(
            system_client=mock_datahub_client, tools_client=mock_datahub_client
        )

        message_context = ChatContext(
            text="Current step: Configure Recipe",
            entity_urns=["urn:li:dataSource:123"],
        )

        manager.load_session(
            "urn:li:dataHubAiConversation:123", message_context=message_context
        )

        # Verify factory was called with combined context
        expected_context = (
            "User is editing a source\n\n"
            "Current Context: Current step: Configure Recipe"
        )
        mock_factory.assert_called_with(
            client=mock_datahub_client,
            chat_type=ChatType.DATAHUB_UI,
            history=ChatHistory(messages=[]),
            tools=[mock_mcp],
            context=expected_context,
        )


def test_load_session_with_only_message_context(mock_datahub_client: Mock) -> None:
    """Test load_session when only message context is provided."""
    with (
        patch(
            "datahub_integrations.chat.chat_session_manager.DataHubAiConversationClient"
        ) as mock_cm,
        patch(
            "datahub_integrations.chat.chat_session_manager.AGENT_FACTORIES"
        ) as mock_factories,
        patch("datahub_integrations.chat.chat_session_manager.mcp") as mock_mcp,
    ):
        mock_instance = mock_cm.return_value
        mock_agent = Mock()
        mock_factory = Mock(return_value=mock_agent)
        mock_factories.__contains__ = Mock(return_value=True)
        mock_factories.__getitem__ = Mock(return_value=mock_factory)

        # No conversation context
        mock_instance.load_conversation_with_metadata.return_value = (
            ChatHistory(messages=[]),
            ChatType.DATAHUB_UI,
            None,
        )

        manager = ChatSessionManager(
            system_client=mock_datahub_client, tools_client=mock_datahub_client
        )

        message_context = ChatContext(text="Current step: Test Connection")

        manager.load_session(
            "urn:li:dataHubAiConversation:123", message_context=message_context
        )

        # Only message context should be used
        mock_factory.assert_called_with(
            client=mock_datahub_client,
            chat_type=ChatType.DATAHUB_UI,
            history=ChatHistory(messages=[]),
            tools=[mock_mcp],
            context="Current step: Test Connection",
        )


def test_load_session_without_message_context(mock_datahub_client: Mock) -> None:
    """Test load_session without message context uses only conversation context."""
    with (
        patch(
            "datahub_integrations.chat.chat_session_manager.DataHubAiConversationClient"
        ) as mock_cm,
        patch(
            "datahub_integrations.chat.chat_session_manager.AGENT_FACTORIES"
        ) as mock_factories,
        patch("datahub_integrations.chat.chat_session_manager.mcp") as mock_mcp,
    ):
        mock_instance = mock_cm.return_value
        mock_agent = Mock()
        mock_factory = Mock(return_value=mock_agent)
        mock_factories.__contains__ = Mock(return_value=True)
        mock_factories.__getitem__ = Mock(return_value=mock_factory)

        conversation_context = "User is viewing run details"
        mock_instance.load_conversation_with_metadata.return_value = (
            ChatHistory(messages=[]),
            ChatType.DATAHUB_UI,
            conversation_context,
        )

        manager = ChatSessionManager(
            system_client=mock_datahub_client, tools_client=mock_datahub_client
        )

        # No message context provided
        manager.load_session("urn:li:dataHubAiConversation:123")

        # Only conversation context should be used
        mock_factory.assert_called_with(
            client=mock_datahub_client,
            chat_type=ChatType.DATAHUB_UI,
            history=ChatHistory(messages=[]),
            tools=[mock_mcp],
            context=conversation_context,
        )


def test_send_message_with_message_context(mock_datahub_client: Mock) -> None:
    """Test send_message passes message context to load_session."""
    manager = ChatSessionManager(
        system_client=mock_datahub_client, tools_client=mock_datahub_client
    )

    message_context = ChatContext(
        text="Current step: Configure Recipe",
        entity_urns=["urn:li:dataSource:123"],
    )

    with patch(
        "datahub_integrations.chat.chat_session_manager.DataHubAiConversationClient"
    ) as mock_cm:
        mock_instance = mock_cm.return_value
        mock_instance.load_conversation_with_metadata.return_value = (
            ChatHistory(messages=[]),
            ChatType.DATAHUB_UI,
            "Base context",
        )
        mock_instance.save_message_to_conversation = Mock()

        with patch.object(manager, "load_session") as mock_load:
            mock_session = Mock()
            mock_session.history = ChatHistory(messages=[])
            mock_session.generate_formatted_message = Mock(
                return_value=NextMessage(text="Response", suggestions=[])
            )
            mock_session.set_progress_callback = Mock(
                return_value=Mock(__enter__=Mock(), __exit__=Mock())
            )
            mock_load.return_value = mock_session

            urn = "urn:li:dataHubAiConversation:123"
            list(
                manager.send_message(
                    text="Hello",
                    user_urn="urn:li:corpuser:test",
                    conversation_urn=urn,
                    message_context=message_context,
                )
            )

            # Verify load_session was called with message_context
            mock_load.assert_called_once_with(
                urn, "DataCatalogExplorer", message_context
            )


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
