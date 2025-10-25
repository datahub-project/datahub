"""
Simplified API endpoints for DataHub-backed chat functionality.

This module provides only the essential streaming endpoint that is actually used
by the Java GraphQL service.
"""

import json
from typing import Any, Dict, Iterator

from datahub.sdk.main_client import DataHubClient
from fastapi import APIRouter, Header, HTTPException
from fastapi.responses import StreamingResponse
from loguru import logger
from pydantic import BaseModel

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


def get_datahub_client() -> DataHubClient:
    """Get DataHub client instance."""
    return DataHubClient.from_env()


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
def send_streaming_message(request: ChatMessageRequest) -> StreamingResponse:
    """
    Send a message to a chat conversation with streaming progress updates.
    Returns a Server-Sent Events (SSE) stream with progress updates.

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
            # Get DataHub client and create manager
            client = get_datahub_client()
            manager = ChatSessionManager(client=client)

            logger.info(
                f"Starting message stream for conversation: {request.conversation_urn}"
            )

            # Stream domain events from manager and convert to SSE format
            for event in manager.send_message(
                text=request.text,
                user_urn=request.user_urn,
                conversation_urn=request.conversation_urn,
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
