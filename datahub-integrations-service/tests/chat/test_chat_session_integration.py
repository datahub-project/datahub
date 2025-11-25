"""Integration tests for ChatSession with extra LLM instructions."""

from unittest.mock import MagicMock, patch

from datahub.sdk.main_client import DataHubClient

from datahub_integrations.chat.chat_history import ChatHistory, HumanMessage
from datahub_integrations.chat.chat_session import ChatSession
from datahub_integrations.mcp.mcp_server import mcp


class TestChatSessionIntegration:
    """Test ChatSession integration with extra LLM instructions."""

    @patch("datahub_integrations.chat.chat_session.get_extra_llm_instructions")
    def test_chat_session_uses_extra_instructions_from_graphql(
        self, mock_get_instructions: MagicMock
    ) -> None:
        """Test that ChatSession properly retrieves and uses extra instructions."""
        # Setup
        mock_get_instructions.return_value = "Always be concise and technical."

        mock_client = MagicMock(spec=DataHubClient)
        mock_graph = MagicMock()
        mock_client._graph = mock_graph

        # Create ChatSession
        # NOTE: This will call get_extra_llm_instructions during AgentRunner init
        # for token estimation (line 186 in agent_runner.py)
        chat_session = ChatSession(
            tools=[mcp],
            client=mock_client,
            history=ChatHistory(messages=[HumanMessage(text="Test message")]),
        )

        # Get system messages
        # NOTE: This will call get_extra_llm_instructions again
        system_messages = chat_session._agent_runner._get_system_messages()

        # Verify
        assert len(system_messages) == 2  # Base prompt + extra instructions
        assert "CUSTOMER-SPECIFIC REQUIREMENTS" in system_messages[1]["text"]
        assert "Always be concise and technical." in system_messages[1]["text"]

        # Verify the function was called with the client
        # It's called twice: once during init (token estimation), once during _get_system_messages
        assert mock_get_instructions.call_count >= 1
        mock_get_instructions.assert_called_with(mock_client)

    @patch("datahub_integrations.chat.chat_session.get_extra_llm_instructions")
    def test_chat_session_handles_no_extra_instructions(
        self, mock_get_instructions: MagicMock
    ) -> None:
        """Test that ChatSession works correctly when no extra instructions are available."""
        # Setup
        mock_get_instructions.return_value = None

        mock_client = MagicMock(spec=DataHubClient)
        mock_graph = MagicMock()
        mock_client._graph = mock_graph

        # Create ChatSession
        chat_session = ChatSession(
            tools=[mcp],
            client=mock_client,
            history=ChatHistory(messages=[HumanMessage(text="Test message")]),
        )

        # Get system messages
        system_messages = chat_session._agent_runner._get_system_messages()

        # Verify
        assert len(system_messages) == 1  # Only base prompt
        assert "DataHub AI" in system_messages[0]["text"]
        assert "CUSTOMER-SPECIFIC REQUIREMENTS" not in system_messages[0]["text"]

    def test_chat_session_uses_override_instructions(self) -> None:
        """Test that override instructions take precedence over GraphQL instructions."""
        # Setup
        mock_client = MagicMock(spec=DataHubClient)
        mock_graph = MagicMock()
        mock_client._graph = mock_graph

        override_instructions = "Override: Be extra helpful."

        # Create ChatSession with override
        chat_session = ChatSession(
            tools=[mcp],
            client=mock_client,
            history=ChatHistory(messages=[HumanMessage(text="Test message")]),
            extra_instructions_override=override_instructions,
        )

        # Get system messages
        with patch(
            "datahub_integrations.chat.chat_session.get_extra_llm_instructions"
        ) as mock_get:
            mock_get.return_value = "GraphQL instructions - should not be used"
            system_messages = chat_session._agent_runner._get_system_messages()

            # Verify override is used and GraphQL function is not called
            assert len(system_messages) == 2
            assert "Override: Be extra helpful." in system_messages[1]["text"]
            mock_get.assert_not_called()

    @patch("datahub_integrations.chat.chat_session.get_extra_llm_instructions")
    def test_chat_session_handles_graphql_error_gracefully(
        self, mock_get_instructions: MagicMock
    ) -> None:
        """Test that ChatSession handles GraphQL errors appropriately."""
        # Setup - simulate GraphQL error
        mock_get_instructions.return_value = None  # Function now returns None on error

        mock_client = MagicMock(spec=DataHubClient)
        mock_graph = MagicMock()
        mock_client._graph = mock_graph

        # Create ChatSession
        chat_session = ChatSession(
            tools=[mcp],
            client=mock_client,
            history=ChatHistory(messages=[HumanMessage(text="Test message")]),
        )

        # Get system messages should handle gracefully (return None)
        system_messages = chat_session._agent_runner._get_system_messages()

        # Should work fine with no extra instructions (returns None on error)
        assert len(system_messages) == 1  # Only base prompt
