"""
Simplified API endpoints for DataHub-backed chat functionality.

This module provides only the essential streaming endpoint that is actually used
by the Java GraphQL service.
"""

import json
from typing import Any, Dict, Iterator

from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.graph.config import DatahubClientConfig
from datahub.sdk.main_client import DataHubClient
from fastapi import APIRouter, Header, HTTPException
from fastapi.responses import StreamingResponse
from loguru import logger
from pydantic import BaseModel

from datahub_integrations.app import DATAHUB_SERVER
from datahub_integrations.chat.chat_session_manager import (
    ChatMessageEvent,
    ChatSessionManager,
)
from datahub_integrations.chat.config import CHAT_MAX_MESSAGE_LENGTH

# Create API router
router = APIRouter(prefix="/api/chat", tags=["chat"])


class ChatMessageRequest(BaseModel):
    """Request model for sending a chat message."""

    text: str
    conversation_urn: str
    user_urn: str
    agent_name: str | None = None


def get_system_client() -> DataHubClient:
    """
    Get DataHub client instance with system credentials for conversation management.

    This client uses system authentication (via DATAHUB_SYSTEM_CLIENT_ID/SECRET
    or DATAHUB_GMS_API_TOKEN) and is used for conversation persistence operations
    that don't require user-specific permissions.

    Returns:
        DataHubClient configured with system credentials
    """
    from datahub_integrations.app import graph

    logger.info("Creating DataHub client with system credentials")
    return DataHubClient(graph=graph)


def get_tools_client(authorization: str) -> DataHubClient:
    """
    Get DataHub client instance with user credentials for tool execution.

    Creates a DataHubClient configured with the user's credentials from the
    Authorization header. This client is specifically used for tool calls where
    user-level permissions must be enforced.

    Args:
        authorization: Authorization header value (e.g., "Bearer <token>")

    Returns:
        DataHubClient configured with user credentials

    Raises:
        HTTPException: If authorization header is missing or invalid
    """
    if not authorization:
        raise HTTPException(status_code=401, detail="Missing Authorization header")

    # Extract token from authorization header
    if authorization.startswith("Bearer "):
        token = authorization.split(" ", 1)[1]
    else:
        # If it doesn't have Bearer prefix, assume it's already just the token
        token = authorization

    if not token:
        raise HTTPException(
            status_code=401, detail="Empty token in Authorization header"
        )

    logger.info("Creating DataHub tools client with user credentials")

    # Create DataHub graph with user's token
    graph = DataHubGraph(
        DatahubClientConfig(
            server=DATAHUB_SERVER,
            token=token,
        )
    )
    return DataHubClient(graph=graph)


def get_auth_token(authorization: str | None = Header(None)) -> str:
    """Extract and validate the Bearer token from the Authorization header."""
    if not authorization:
        raise HTTPException(status_code=401, detail="Missing Authorization header")

    if not authorization.startswith("Bearer "):
        raise HTTPException(
            status_code=401,
            detail="Invalid authorization header format; expected 'Bearer <token>'",
        )

    token = authorization.split(" ", 1)[1]
    if not token:
        raise HTTPException(status_code=401, detail="Empty token")

    return token


def event_to_sse(event: ChatMessageEvent) -> Dict[str, Any]:
    """Convert a ChatMessageEvent to SSE data format (no 'type' field - that's in event name)."""
    # Handle error events
    if event.error:
        return {
            "conversation_urn": event.conversation_urn,
            "error": event.error,
            "message": "Failed to process message",
        }

    # Build message data (type is in the event name, not needed in payload)
    return {
        "conversation_urn": event.conversation_urn,
        "message": {
            "type": event.message_type,
            "time": event.timestamp,
            "actor": {
                "type": "USER" if event.user_urn else "AGENT",
                "actor": event.user_urn or "urn:li:corpuser:datahub-ai",
            },
            "content": {
                "text": event.text,
                "attachments": [],
                "mentions": [],
            },
        },
        "message_type": event.message_type,
    }


@router.post("/message")
def send_streaming_message(
    request: ChatMessageRequest,
    authorization: str = Header(...),
) -> StreamingResponse:
    """
    Send a message to a chat conversation with streaming progress updates.
    Returns a Server-Sent Events (SSE) stream with progress updates.

    This endpoint requires user credentials via the Authorization header. The user's
    credentials are specifically used for tool execution, ensuring that tool calls
    respect user permissions. Conversation management uses system credentials.

    Args:
        request: Chat message request containing conversation URN, user URN, and text
        authorization: Required Authorization header with user's Bearer token

    Returns:
        StreamingResponse with Server-Sent Events

    Note: Client disconnect detection is not reliable in the current architecture
    because requests pass through a Java intermediate server that continues
    consuming the stream even when the frontend disconnects.
    """
    # Validate message length
    if len(request.text) > CHAT_MAX_MESSAGE_LENGTH:
        raise HTTPException(
            status_code=400,
            detail=f"Message exceeds maximum length of {CHAT_MAX_MESSAGE_LENGTH} characters (got {len(request.text)})",
        )

    def generate_stream() -> Iterator[str]:
        try:
            # Get system client for conversation management
            system_client = get_system_client()

            # Get tools client with user credentials for tool execution
            tools_client = get_tools_client(authorization=authorization)

            manager = ChatSessionManager(
                system_client=system_client, tools_client=tools_client
            )

            logger.info(
                f"Starting message stream for conversation: {request.conversation_urn}"
            )

            # Stream domain events from manager and convert to SSE format
            for event in manager.send_message(
                text=request.text,
                user_urn=request.user_urn,
                conversation_urn=request.conversation_urn,
                agent_name=request.agent_name,
            ):
                sse_data = event_to_sse(event)

                # Use appropriate event name based on whether it's an error
                event_name = "error" if event.error else "message"
                yield f"event: {event_name}\ndata: {json.dumps(sse_data)}\n\n"

            # Send completion event after all messages
            complete_data = {
                "conversation_urn": request.conversation_urn,
            }
            yield f"event: complete\ndata: {json.dumps(complete_data)}\n\n"

        except Exception as e:
            logger.exception("Failed to process streaming chat message")
            error_data = {
                "error": str(e),
                "message": "Failed to process message",
                "conversation_urn": request.conversation_urn,
            }
            yield f"event: error\ndata: {json.dumps(error_data)}\n\n"

    return StreamingResponse(
        generate_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "Content-Type": "text/event-stream",
        },
    )
