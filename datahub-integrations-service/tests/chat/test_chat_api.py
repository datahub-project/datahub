"""Unit tests for chat_api.py"""

from unittest.mock import Mock, patch

import pytest
from fastapi import HTTPException

from datahub_integrations.chat.chat_api import (
    ChatMessageRequest,
    event_to_sse,
    get_auth_token,
    get_system_client,
    get_tools_client,
)
from datahub_integrations.chat.chat_session_manager import ChatMessageEvent


def test_chat_message_request_validation() -> None:
    """Test ChatMessageRequest validation."""
    # Valid request
    request = ChatMessageRequest(
        conversation_urn="urn:li:dataHubAiConversation:test",
        user_urn="urn:li:corpuser:admin",
        text="Hello, AI!",
    )

    assert request.conversation_urn == "urn:li:dataHubAiConversation:test"
    assert request.user_urn == "urn:li:corpuser:admin"
    assert request.text == "Hello, AI!"


def test_get_tools_client_with_bearer_token() -> None:
    """Test that get_tools_client creates a client with user token from Bearer header."""
    with patch("datahub_integrations.chat.chat_api.DataHubGraph") as mock_graph_class:
        with patch(
            "datahub_integrations.chat.chat_api.DataHubClient"
        ) as mock_client_class:
            mock_graph = Mock()
            mock_graph_class.return_value = mock_graph
            mock_client = Mock()
            mock_client_class.return_value = mock_client

            client = get_tools_client(authorization="Bearer test_token_123")

            assert client is mock_client
            # Verify DataHubGraph was created with the token
            mock_graph_class.assert_called_once()
            config_arg = mock_graph_class.call_args[0][0]
            assert config_arg.token == "test_token_123"

            # Verify DataHubClient was created with the graph
            mock_client_class.assert_called_once_with(graph=mock_graph)


def test_get_tools_client_token_without_bearer_prefix() -> None:
    """Test that get_tools_client handles token without Bearer prefix."""
    with patch("datahub_integrations.chat.chat_api.DataHubGraph") as mock_graph_class:
        with patch(
            "datahub_integrations.chat.chat_api.DataHubClient"
        ) as mock_client_class:
            mock_graph = Mock()
            mock_graph_class.return_value = mock_graph
            mock_client = Mock()
            mock_client_class.return_value = mock_client

            client = get_tools_client(authorization="just_a_token")

            assert client is mock_client
            # Verify token was used correctly (without Bearer prefix)
            config_arg = mock_graph_class.call_args[0][0]
            assert config_arg.token == "just_a_token"


def test_get_tools_client_missing_authorization() -> None:
    """Test that get_tools_client raises HTTPException when authorization is missing."""
    with pytest.raises(HTTPException) as exc_info:
        get_tools_client(authorization=None)  # type: ignore[arg-type]

    assert exc_info.value.status_code == 401
    assert "Missing Authorization header" in exc_info.value.detail


def test_get_tools_client_empty_token() -> None:
    """Test that get_tools_client raises HTTPException when token is empty."""
    with pytest.raises(HTTPException) as exc_info:
        get_tools_client(authorization="Bearer ")

    assert exc_info.value.status_code == 401
    assert "Empty token" in exc_info.value.detail


def test_get_system_client() -> None:
    """Test that get_system_client creates a client with system credentials."""
    with patch("datahub_integrations.app.graph") as mock_graph:
        with patch(
            "datahub_integrations.chat.chat_api.DataHubClient"
        ) as mock_client_class:
            mock_client = Mock()
            mock_client_class.return_value = mock_client

            client = get_system_client()

            assert client is mock_client
            # Verify DataHubClient was created with the system graph
            mock_client_class.assert_called_once_with(graph=mock_graph)


def test_chat_message_request_urns() -> None:
    """Test that ChatMessageRequest handles different URN formats."""
    request = ChatMessageRequest(
        conversation_urn="urn:li:dataHubAiConversation:12345",
        user_urn="urn:li:corpuser:testuser",
        text="Test message",
    )

    assert (
        request.conversation_urn and "dataHubAiConversation" in request.conversation_urn
    )
    assert "corpuser" in request.user_urn


def test_get_auth_token_valid() -> None:
    """Test extracting a valid Bearer token."""
    token = get_auth_token("Bearer test_token_12345")
    assert token == "test_token_12345"


def test_get_auth_token_missing_header() -> None:
    """Test that missing Authorization header raises 401."""
    with pytest.raises(HTTPException) as exc_info:
        get_auth_token(None)

    assert exc_info.value.status_code == 401
    assert "Missing Authorization header" in exc_info.value.detail


def test_get_auth_token_invalid_format() -> None:
    """Test that non-Bearer format raises 401."""
    with pytest.raises(HTTPException) as exc_info:
        get_auth_token("Basic some_credentials")

    assert exc_info.value.status_code == 401
    assert "Invalid authorization header format" in exc_info.value.detail


def test_get_auth_token_empty_token() -> None:
    """Test that empty token raises 401."""
    with pytest.raises(HTTPException) as exc_info:
        get_auth_token("Bearer ")

    assert exc_info.value.status_code == 401
    assert "Empty token" in exc_info.value.detail


def test_event_to_sse_user_message() -> None:
    """Test converting a user message event to SSE format."""
    event = ChatMessageEvent(
        message_type="TEXT",
        text="Hello AI",
        conversation_urn="urn:li:dataHubAiConversation:123",
        timestamp=1234567890,
        user_urn="urn:li:corpuser:test",
    )

    sse_data = event_to_sse(event)

    # No top-level "type" field - that's in the event: line
    assert sse_data["conversation_urn"] == "urn:li:dataHubAiConversation:123"
    assert sse_data["message"]["type"] == "TEXT"
    assert sse_data["message"]["content"]["text"] == "Hello AI"
    assert sse_data["message"]["actor"]["type"] == "USER"
    assert sse_data["message"]["actor"]["actor"] == "urn:li:corpuser:test"
    assert sse_data["message"]["time"] == 1234567890


def test_event_to_sse_agent_message() -> None:
    """Test converting an agent message event to SSE format."""
    event = ChatMessageEvent(
        message_type="THINKING",
        text="Processing your request",
        conversation_urn="urn:li:dataHubAiConversation:123",
        timestamp=1234567890,
        user_urn=None,  # Agent message
    )

    sse_data = event_to_sse(event)

    # No top-level "type" field - that's in the event: line
    assert sse_data["message"]["type"] == "THINKING"
    assert sse_data["message"]["content"]["text"] == "Processing your request"
    assert sse_data["message"]["actor"]["type"] == "AGENT"
    assert sse_data["message"]["actor"]["actor"] == "urn:li:corpuser:datahub-ai"


def test_event_to_sse_error_event() -> None:
    """Test converting an error event to SSE format."""
    event = ChatMessageEvent(
        message_type="TEXT",
        text="",
        conversation_urn="urn:li:dataHubAiConversation:123",
        timestamp=1234567890,
        error="Something went wrong",
    )

    sse_data = event_to_sse(event)

    # No top-level "type" field - that's in the event: line
    assert sse_data["error"] == "Something went wrong"
    assert sse_data["message"] == "Failed to process message"
    assert sse_data["conversation_urn"] == "urn:li:dataHubAiConversation:123"


def test_event_to_sse_different_message_types() -> None:
    """Test that different message types are preserved in SSE conversion."""
    message_types = ["TEXT", "THINKING", "TOOL_CALL", "TOOL_RESULT"]

    for msg_type in message_types:
        event = ChatMessageEvent(
            message_type=msg_type,
            text=f"Test {msg_type}",
            conversation_urn="urn:li:dataHubAiConversation:123",
            timestamp=1234567890,
        )

        sse_data = event_to_sse(event)

        assert sse_data["message"]["type"] == msg_type
        assert sse_data["message_type"] == msg_type


def test_event_to_sse_returns_dict() -> None:
    """Test that event_to_sse returns a plain dict (not wrapped in a model)."""
    event = ChatMessageEvent(
        message_type="TEXT",
        text="Test",
        conversation_urn="urn:li:dataHubAiConversation:123",
        timestamp=1234567890,
    )

    sse_data = event_to_sse(event)

    # Should return a dict, not a Pydantic model
    assert isinstance(sse_data, dict)
    assert "conversation_urn" in sse_data
    assert "message" in sse_data


def test_event_to_sse_includes_empty_attachments_and_mentions() -> None:
    """Test that SSE format includes empty lists for attachments and mentions."""
    event = ChatMessageEvent(
        message_type="TEXT",
        text="Test",
        conversation_urn="urn:li:dataHubAiConversation:123",
        timestamp=1234567890,
    )

    sse_data = event_to_sse(event)

    assert sse_data["message"]["content"]["attachments"] == []
    assert sse_data["message"]["content"]["mentions"] == []


@pytest.mark.asyncio
async def test_send_streaming_message_endpoint_structure() -> None:
    """Test that send_streaming_message returns proper StreamingResponse."""
    from datahub_integrations.chat.chat_api import send_streaming_message

    request = ChatMessageRequest(
        text="Test message",
        user_urn="urn:li:corpuser:test",
        conversation_urn="urn:li:dataHubAiConversation:123",
    )

    # Mock the manager and its methods
    with patch(
        "datahub_integrations.chat.chat_api.get_system_client"
    ) as mock_get_system_client:
        with patch(
            "datahub_integrations.chat.chat_api.get_tools_client"
        ) as mock_get_tools_client:
            with patch(
                "datahub_integrations.chat.chat_api.ChatSessionManager"
            ) as mock_manager_class:
                mock_system_client = Mock()
                mock_tools_client = Mock()
                mock_get_system_client.return_value = mock_system_client
                mock_get_tools_client.return_value = mock_tools_client

                mock_manager = Mock()
                mock_manager_class.return_value = mock_manager

                # Mock send_message to return some events
                mock_manager.send_message.return_value = iter(
                    [
                        ChatMessageEvent(
                            message_type="TEXT",
                            text="Test",
                            conversation_urn="urn:li:dataHubAiConversation:123",
                            timestamp=1234567890,
                            user_urn="urn:li:corpuser:test",
                        ),
                        ChatMessageEvent(
                            message_type="TEXT",
                            text="Response",
                            conversation_urn="urn:li:dataHubAiConversation:123",
                            timestamp=1234567891,
                        ),
                    ]
                )

                response = send_streaming_message(
                    request, authorization="Bearer test_token"
                )

                # Verify response is a StreamingResponse
                assert response.media_type == "text/event-stream"
                assert response.headers["Cache-Control"] == "no-cache"
                assert response.headers["Connection"] == "keep-alive"


@pytest.mark.asyncio
async def test_send_streaming_message_calls_manager_correctly() -> None:
    """Test that send_streaming_message calls ChatSessionManager with correct args."""
    from datahub_integrations.chat.chat_api import send_streaming_message

    request = ChatMessageRequest(
        text="Hello AI",
        user_urn="urn:li:corpuser:test",
        conversation_urn="urn:li:dataHubAiConversation:123",
    )

    with patch(
        "datahub_integrations.chat.chat_api.get_system_client"
    ) as mock_get_system_client:
        with patch(
            "datahub_integrations.chat.chat_api.get_tools_client"
        ) as mock_get_tools_client:
            with patch(
                "datahub_integrations.chat.chat_api.ChatSessionManager"
            ) as mock_manager_class:
                mock_system_client = Mock()
                mock_tools_client = Mock()
                mock_get_system_client.return_value = mock_system_client
                mock_get_tools_client.return_value = mock_tools_client

                mock_manager = Mock()
                mock_manager_class.return_value = mock_manager
                mock_manager.send_message.return_value = iter([])

                response = send_streaming_message(
                    request, authorization="Bearer test_token"
                )

                # Consume the stream to actually execute the generator
                chunks = []
                async for chunk in response.body_iterator:
                    chunks.append(chunk)

                # Verify manager was created with both clients
                mock_manager_class.assert_called_once_with(
                    system_client=mock_system_client, tools_client=mock_tools_client
                )

                # Verify send_message was called with correct args
                mock_manager.send_message.assert_called_once_with(
                    text="Hello AI",
                    user_urn="urn:li:corpuser:test",
                    conversation_urn="urn:li:dataHubAiConversation:123",
                    agent_name=None,
                    message_context=None,
                )


@pytest.mark.asyncio
async def test_send_streaming_message_handles_errors() -> None:
    """Test that send_streaming_message handles errors gracefully."""
    from datahub_integrations.chat.chat_api import send_streaming_message

    request = ChatMessageRequest(
        text="Test",
        user_urn="urn:li:corpuser:test",
        conversation_urn="urn:li:dataHubAiConversation:123",
    )

    with patch(
        "datahub_integrations.chat.chat_api.get_tools_client"
    ) as mock_get_tools_client:
        # Simulate an error during client creation
        mock_get_tools_client.side_effect = Exception("Connection failed")

        response = send_streaming_message(request, authorization="Bearer test_token")

        # Response should still be a StreamingResponse
        assert response.media_type == "text/event-stream"

        # Collect the stream content to verify error is in the stream
        chunks = []
        async for chunk in response.body_iterator:
            # Ensure chunk is decoded to string if it's bytes or memoryview
            if isinstance(chunk, bytes):
                chunks.append(chunk.decode("utf-8"))
            elif isinstance(chunk, memoryview):
                chunks.append(bytes(chunk).decode("utf-8"))
            else:
                chunks.append(chunk)

        content = "".join(chunks)  # Already strings from SSE

        # Should contain error event
        assert "error" in content
        assert "Connection failed" in content


@pytest.mark.asyncio
async def test_send_streaming_message_sends_completion_event() -> None:
    """Test that send_streaming_message sends a completion event at the end."""
    from datahub_integrations.chat.chat_api import send_streaming_message

    request = ChatMessageRequest(
        text="Test",
        user_urn="urn:li:corpuser:test",
        conversation_urn="urn:li:dataHubAiConversation:123",
    )

    with patch(
        "datahub_integrations.chat.chat_api.get_system_client"
    ) as mock_get_system_client:
        with patch(
            "datahub_integrations.chat.chat_api.get_tools_client"
        ) as mock_get_tools_client:
            with patch(
                "datahub_integrations.chat.chat_api.ChatSessionManager"
            ) as mock_manager_class:
                mock_system_client = Mock()
                mock_tools_client = Mock()
                mock_get_system_client.return_value = mock_system_client
                mock_get_tools_client.return_value = mock_tools_client

                mock_manager = Mock()
                mock_manager_class.return_value = mock_manager
                mock_manager.send_message.return_value = iter(
                    [
                        ChatMessageEvent(
                            message_type="TEXT",
                            text="Response",
                            conversation_urn="urn:li:dataHubAiConversation:123",
                            timestamp=1234567890,
                        )
                    ]
                )

                response = send_streaming_message(
                    request, authorization="Bearer test_token"
                )

                # Collect stream content
                chunks = []
                async for chunk in response.body_iterator:
                    # Ensure chunk is decoded to string if it's bytes or memoryview
                    if isinstance(chunk, bytes):
                        chunks.append(chunk.decode("utf-8"))
                    elif isinstance(chunk, memoryview):
                        chunks.append(bytes(chunk).decode("utf-8"))
                    else:
                        chunks.append(chunk)

                content = "".join(chunks)  # Already strings from SSE

                # Should contain a completion event (in the event: line, not the data)
                assert "event: complete" in content
                # Should contain the conversation_urn in the completion data
                assert (
                    '"conversation_urn": "urn:li:dataHubAiConversation:123"' in content
                )


def test_message_length_limit_enforced():
    """Test that messages exceeding the maximum length are rejected."""
    from datahub_integrations.chat.chat_api import send_streaming_message
    from datahub_integrations.chat.config import CHAT_MAX_MESSAGE_LENGTH

    # Create a message that exceeds the limit
    long_text = "a" * (CHAT_MAX_MESSAGE_LENGTH + 1)
    request = ChatMessageRequest(
        text=long_text,
        user_urn="urn:li:corpuser:test",
        conversation_urn="urn:li:dataHubAiConversation:123",
    )

    # Should raise HTTPException with 400 status
    with pytest.raises(HTTPException) as exc_info:
        send_streaming_message(request)

    assert exc_info.value.status_code == 400
    assert "exceeds maximum length" in exc_info.value.detail
    assert str(CHAT_MAX_MESSAGE_LENGTH) in exc_info.value.detail


def test_message_at_length_limit_accepted():
    """Test that messages at exactly the maximum length are accepted."""
    from datahub_integrations.chat.chat_api import send_streaming_message
    from datahub_integrations.chat.config import CHAT_MAX_MESSAGE_LENGTH

    # Create a message at exactly the limit
    text_at_limit = "a" * CHAT_MAX_MESSAGE_LENGTH
    request = ChatMessageRequest(
        text=text_at_limit,
        user_urn="urn:li:corpuser:test",
        conversation_urn="urn:li:dataHubAiConversation:123",
    )

    with patch(
        "datahub_integrations.chat.chat_api.get_system_client"
    ) as mock_get_system_client:
        with patch(
            "datahub_integrations.chat.chat_api.get_tools_client"
        ) as mock_get_tools_client:
            with patch(
                "datahub_integrations.chat.chat_api.ChatSessionManager"
            ) as mock_manager_class:
                mock_system_client = Mock()
                mock_tools_client = Mock()
                mock_get_system_client.return_value = mock_system_client
                mock_get_tools_client.return_value = mock_tools_client

                mock_manager = Mock()
                mock_manager_class.return_value = mock_manager
                mock_manager.send_message.return_value = iter([])

                # Should not raise an exception
                response = send_streaming_message(
                    request, authorization="Bearer test_token"
                )
                assert response is not None


def test_empty_message_accepted():
    """Test that empty messages are accepted (validation is on length, not emptiness)."""
    from datahub_integrations.chat.chat_api import send_streaming_message

    request = ChatMessageRequest(
        text="",
        user_urn="urn:li:corpuser:test",
        conversation_urn="urn:li:dataHubAiConversation:123",
    )

    with patch(
        "datahub_integrations.chat.chat_api.get_system_client"
    ) as mock_get_system_client:
        with patch(
            "datahub_integrations.chat.chat_api.get_tools_client"
        ) as mock_get_tools_client:
            with patch(
                "datahub_integrations.chat.chat_api.ChatSessionManager"
            ) as mock_manager_class:
                mock_system_client = Mock()
                mock_tools_client = Mock()
                mock_get_system_client.return_value = mock_system_client
                mock_get_tools_client.return_value = mock_tools_client

                mock_manager = Mock()
                mock_manager_class.return_value = mock_manager
                mock_manager.send_message.return_value = iter([])

                # Should not raise an exception
                response = send_streaming_message(
                    request, authorization="Bearer test_token"
                )
                assert response is not None


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
