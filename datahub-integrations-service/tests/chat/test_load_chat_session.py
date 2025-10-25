"""
Test the new load_chat_session functionality.
"""

from unittest.mock import Mock, patch

from datahub_integrations.chat.chat_history import ChatHistory
from datahub_integrations.chat.chat_session_manager import ChatSessionManager
from datahub_integrations.chat.types import ChatType


class TestLoadChatSession:
    """Test the load_chat_session instance method."""

    @patch("datahub_integrations.chat.chat_session_manager.DataHubAiConversationClient")
    @patch("datahub_integrations.chat.chat_session_manager.ChatSession")
    @patch("datahub_integrations.chat.chat_session_manager.mcp")
    def test_load_chat_session_with_datahub_ui_origin_type(
        self, mock_mcp, mock_chat_session_class, mock_conversation_client_class
    ):
        """Test loading a chat session with DATAHUB_UI origin type."""
        # Setup mocks
        mock_client = Mock()
        mock_conversation_client = Mock()
        mock_conversation_client_class.return_value = mock_conversation_client

        # Mock the conversation metadata
        mock_chat_history = ChatHistory()
        mock_conversation_client.load_conversation_with_metadata.return_value = (
            mock_chat_history,
            ChatType.DATAHUB_UI,
        )

        # Mock ChatSession
        mock_chat_session = Mock()
        mock_chat_session_class.return_value = mock_chat_session

        # Test
        manager = ChatSessionManager(client=mock_client)
        result = manager.load_chat_session(
            "urn:li:dataHubAiConversation:test123",
        )

        # Verify
        assert result is not None

        # Verify that the conversation client was called correctly
        mock_conversation_client.load_conversation_with_metadata.assert_called_once_with(
            "urn:li:dataHubAiConversation:test123"
        )

        # Verify that ChatSession was created with the correct type
        mock_chat_session_class.assert_called_once_with(
            tools=[mock_mcp],
            client=mock_client,
            chat_type=ChatType.DATAHUB_UI,
        )

        # Verify that the history was set on created ChatSession
        created_session = mock_chat_session_class.return_value
        assert created_session.history == mock_chat_history

    @patch("datahub_integrations.chat.chat_session_manager.DataHubAiConversationClient")
    @patch("datahub_integrations.chat.chat_session_manager.ChatSession")
    @patch("datahub_integrations.chat.chat_session_manager.mcp")
    def test_load_chat_session_with_slack_origin_type(
        self, mock_mcp, mock_chat_session_class, mock_conversation_client_class
    ):
        """Test loading a chat session with SLACK origin type."""
        # Setup mocks
        mock_client = Mock()
        mock_conversation_client = Mock()
        mock_conversation_client_class.return_value = mock_conversation_client

        # Mock the conversation metadata with SLACK origin type
        mock_chat_history = ChatHistory()
        mock_conversation_client.load_conversation_with_metadata.return_value = (
            mock_chat_history,
            ChatType.SLACK,
        )

        # Mock ChatSession
        mock_chat_session = Mock()
        mock_chat_session_class.return_value = mock_chat_session

        # Test
        manager = ChatSessionManager(client=mock_client)
        result = manager.load_chat_session(
            "urn:li:dataHubAiConversation:slack123",
        )

        # Verify
        assert result is not None

        # Verify that ChatSession was created with SLACK type
        mock_chat_session_class.assert_called_once_with(
            tools=[mock_mcp],
            client=mock_client,
            chat_type=ChatType.SLACK,
        )

    @patch("datahub_integrations.chat.chat_session_manager.DataHubAiConversationClient")
    @patch("datahub_integrations.chat.chat_session_manager.ChatSession")
    @patch("datahub_integrations.chat.chat_session_manager.mcp")
    def test_load_chat_session_with_unknown_origin_type(
        self, mock_mcp, mock_chat_session_class, mock_conversation_client_class
    ):
        """Test loading a chat session with unknown origin type (should default to DEFAULT)."""
        # Setup mocks
        mock_client = Mock()
        mock_conversation_client = Mock()
        mock_conversation_client_class.return_value = mock_conversation_client

        # Mock the conversation metadata with unknown origin type
        mock_chat_history = ChatHistory()
        mock_conversation_client.load_conversation_with_metadata.return_value = (
            mock_chat_history,
            ChatType.DEFAULT,
        )

        # Mock ChatSession
        mock_chat_session = Mock()
        mock_chat_session_class.return_value = mock_chat_session

        # Test
        manager = ChatSessionManager(client=mock_client)
        result = manager.load_chat_session(
            "urn:li:dataHubAiConversation:unknown123",
        )

        # Verify
        assert result is not None

        # Verify that ChatSession was created with DEFAULT type
        mock_chat_session_class.assert_called_once_with(
            tools=[mock_mcp],
            client=mock_client,
            chat_type=ChatType.DEFAULT,
        )
