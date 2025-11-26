"""
Test the load_session functionality.
"""

from unittest.mock import Mock, patch

from datahub_integrations.chat.chat_history import ChatHistory
from datahub_integrations.chat.chat_session_manager import ChatSessionManager
from datahub_integrations.chat.types import ChatType


class TestLoadChatSession:
    """Test the load_session instance method."""

    @patch("datahub_integrations.chat.chat_session_manager.DataHubAiConversationClient")
    @patch("datahub_integrations.chat.chat_session_manager.AGENT_FACTORIES")
    @patch("datahub_integrations.chat.chat_session_manager.mcp")
    def test_load_chat_session_with_datahub_ui_origin_type(
        self, mock_mcp, mock_agent_factories, mock_conversation_client_class
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

        # Mock agent factory
        mock_agent = Mock()
        mock_factory = Mock(return_value=mock_agent)
        mock_agent_factories.__contains__ = Mock(return_value=True)
        mock_agent_factories.__getitem__ = Mock(return_value=mock_factory)

        # Test
        manager = ChatSessionManager(
            system_client=mock_client, tools_client=mock_client
        )
        result = manager.load_session(
            "urn:li:dataHubAiConversation:test123",
        )

        # Verify
        assert result is not None
        assert result == mock_agent

        # Verify that the conversation client was called correctly
        mock_conversation_client.load_conversation_with_metadata.assert_called_once_with(
            "urn:li:dataHubAiConversation:test123"
        )

        # Verify that factory was called with the correct type
        mock_factory.assert_called_once_with(
            client=mock_client,
            chat_type=ChatType.DATAHUB_UI,
            history=mock_chat_history,
            tools=[mock_mcp],
        )

    @patch("datahub_integrations.chat.chat_session_manager.DataHubAiConversationClient")
    @patch("datahub_integrations.chat.chat_session_manager.AGENT_FACTORIES")
    @patch("datahub_integrations.chat.chat_session_manager.mcp")
    def test_load_chat_session_with_slack_origin_type(
        self, mock_mcp, mock_agent_factories, mock_conversation_client_class
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

        # Mock agent factory
        mock_agent = Mock()
        mock_factory = Mock(return_value=mock_agent)
        mock_agent_factories.__contains__ = Mock(return_value=True)
        mock_agent_factories.__getitem__ = Mock(return_value=mock_factory)

        # Test
        manager = ChatSessionManager(
            system_client=mock_client, tools_client=mock_client
        )
        result = manager.load_session(
            "urn:li:dataHubAiConversation:slack123",
        )

        # Verify
        assert result is not None

        # Verify that factory was called with SLACK type
        mock_factory.assert_called_once_with(
            client=mock_client,
            chat_type=ChatType.SLACK,
            history=mock_chat_history,
            tools=[mock_mcp],
        )

    @patch("datahub_integrations.chat.chat_session_manager.DataHubAiConversationClient")
    @patch("datahub_integrations.chat.chat_session_manager.AGENT_FACTORIES")
    @patch("datahub_integrations.chat.chat_session_manager.mcp")
    def test_load_chat_session_with_unknown_origin_type(
        self, mock_mcp, mock_agent_factories, mock_conversation_client_class
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

        # Mock agent factory
        mock_agent = Mock()
        mock_factory = Mock(return_value=mock_agent)
        mock_agent_factories.__contains__ = Mock(return_value=True)
        mock_agent_factories.__getitem__ = Mock(return_value=mock_factory)

        # Test
        manager = ChatSessionManager(
            system_client=mock_client, tools_client=mock_client
        )
        result = manager.load_session(
            "urn:li:dataHubAiConversation:unknown123",
        )

        # Verify
        assert result is not None

        # Verify that factory was called with DEFAULT type
        mock_factory.assert_called_once_with(
            client=mock_client,
            chat_type=ChatType.DEFAULT,
            history=mock_chat_history,
            tools=[mock_mcp],
        )
