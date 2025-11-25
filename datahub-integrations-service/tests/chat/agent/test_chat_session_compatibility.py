"""
Tests for ChatSession backward compatibility.

These tests verify that ChatSession maintains its API and works correctly
after refactoring to use AgentRunner internally.
"""

from unittest.mock import Mock

import pytest
from datahub.sdk.main_client import DataHubClient

from datahub_integrations.chat.chat_history import ChatHistory, HumanMessage
from datahub_integrations.chat.chat_session import ChatSession
from datahub_integrations.chat.types import ChatType


@pytest.fixture
def mock_client():
    """Create a mock DataHub client."""
    client = Mock(spec=DataHubClient)
    client._graph = Mock()
    client._graph.frontend_base_url = "https://datahub.example.com"
    return client


class TestChatSessionAPI:
    """Test that ChatSession maintains its public API."""

    def test_chat_session_initialization(self, mock_client):
        """Test ChatSession initialization with basic parameters."""
        session = ChatSession(
            tools=[],
            client=mock_client,
        )

        assert session.client == mock_client
        assert session.session_id is not None
        assert isinstance(session.history, ChatHistory)
        assert session.chat_type == ChatType.DEFAULT
        assert isinstance(session.plan_cache, dict)

    def test_chat_session_with_history(self, mock_client):
        """Test ChatSession with existing history."""
        history = ChatHistory()
        history.add_message(HumanMessage(text="Test message"))

        session = ChatSession(
            tools=[],
            client=mock_client,
            history=history,
        )

        assert len(session.history.messages) == 1
        assert session.history.messages[0].text == "Test message"

    def test_chat_session_with_chat_type(self, mock_client):
        """Test ChatSession with specific chat type."""
        session = ChatSession(
            tools=[],
            client=mock_client,
            chat_type=ChatType.SLACK,
        )

        assert session.chat_type == ChatType.SLACK

    def test_chat_session_with_extra_instructions(self, mock_client):
        """Test ChatSession with extra instructions override."""
        session = ChatSession(
            tools=[],
            client=mock_client,
            extra_instructions_override="Custom instructions",
        )

        assert session.extra_instructions_override == "Custom instructions"


class TestChatSessionProperties:
    """Test ChatSession properties."""

    def test_session_id_property(self, mock_client):
        """Test session_id property."""
        session = ChatSession(tools=[], client=mock_client)

        assert session.session_id is not None
        assert isinstance(session.session_id, str)

    def test_history_property(self, mock_client):
        """Test history property (getter)."""
        session = ChatSession(tools=[], client=mock_client)

        assert isinstance(session.history, ChatHistory)
        assert len(session.history.messages) == 0

    def test_history_property_setter(self, mock_client):
        """Test history property (setter)."""
        session = ChatSession(tools=[], client=mock_client)

        new_history = ChatHistory()
        new_history.add_message(HumanMessage(text="New message"))

        session.history = new_history

        assert len(session.history.messages) == 1
        assert session.history.messages[0].text == "New message"

    def test_tools_property(self, mock_client):
        """Test tools property."""
        session = ChatSession(tools=[], client=mock_client)

        assert isinstance(session.tools, list)

    def test_tool_map_property(self, mock_client):
        """Test tool_map property."""
        session = ChatSession(tools=[], client=mock_client)

        assert isinstance(session.tool_map, dict)

    def test_context_reducers_property(self, mock_client):
        """Test context_reducers property."""
        session = ChatSession(tools=[], client=mock_client)

        # Should have default reducers
        assert session.context_reducers is not None


class TestChatSessionMethods:
    """Test ChatSession methods."""

    def test_get_plannable_tools(self, mock_client):
        """Test get_plannable_tools method."""
        session = ChatSession(tools=[], client=mock_client)

        plannable_tools = session.get_plannable_tools()

        assert isinstance(plannable_tools, list)

    def test_is_respond_to_user(self, mock_client):
        """Test is_respond_to_user class method."""
        from datahub_integrations.chat.chat_history import ToolCallRequest, ToolResult

        # Create a mock respond_to_user tool result
        tool_request = ToolCallRequest(
            tool_use_id="test-id", tool_name="respond_to_user", tool_input={}
        )
        tool_result = ToolResult(tool_request=tool_request, result={"text": "response"})

        assert ChatSession.is_respond_to_user(tool_result) is True

        # Test with non-respond_to_user tool
        other_tool_request = ToolCallRequest(
            tool_use_id="test-id-2", tool_name="other_tool", tool_input={}
        )
        other_tool_result = ToolResult(tool_request=other_tool_request, result={})

        assert ChatSession.is_respond_to_user(other_tool_result) is False

    def test_set_progress_callback(self, mock_client):
        """Test set_progress_callback context manager."""
        session = ChatSession(tools=[], client=mock_client)

        callback_called = []

        def progress_callback(updates):
            callback_called.append(True)

        # Should not raise any errors
        with session.set_progress_callback(progress_callback):
            pass


class TestChatSessionPlanCache:
    """Test ChatSession plan_cache attribute (business logic specific to ChatSession)."""

    def test_plan_cache_exists(self, mock_client):
        """Test that plan_cache exists and is a dict."""
        session = ChatSession(tools=[], client=mock_client)

        assert hasattr(session, "plan_cache")
        assert isinstance(session.plan_cache, dict)
        assert len(session.plan_cache) == 0

    def test_plan_cache_can_be_modified(self, mock_client):
        """Test that plan_cache can be used like a normal dict."""
        session = ChatSession(tools=[], client=mock_client)

        session.plan_cache["test_plan"] = {"data": "value"}

        assert "test_plan" in session.plan_cache
        assert session.plan_cache["test_plan"]["data"] == "value"


def test_chat_session_exceptions_backward_compat():
    """Test that ChatSession exceptions are still available."""
    from datahub_integrations.chat.chat_session import (
        ChatMaxToolCallsExceededError,
        ChatOutputMaxTokensExceededError,
        ChatSessionError,
        ChatSessionMaxTokensExceededError,
    )

    # Should be able to instantiate exceptions
    assert ChatSessionError("test")
    assert ChatSessionMaxTokensExceededError("test")
    assert ChatOutputMaxTokensExceededError("test")
    assert ChatMaxToolCallsExceededError("test")
