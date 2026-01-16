"""
Chat Routes - API endpoints for chat conversations.

Implements REST endpoints and Server-Sent Events (SSE) for real-time chat.
"""

import asyncio
import json
import sys
from pathlib import Path
from typing import List

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import StreamingResponse
from loguru import logger

# Add backend directory to path
backend_dir = Path(__file__).parent.parent.parent
if str(backend_dir) not in sys.path:
    sys.path.insert(0, str(backend_dir))

from api.dependencies import get_chat_engine, get_conversation_or_404, ensure_integrations_service, get_config
from api.models import (
    ConversationModel,
    CreateConversationRequest,
    MessageModel,
    SendMessageRequest,
    SendMessageResponse,
)
from core.chat_engine import ChatEngine
from core.conversation_manager import Conversation

router = APIRouter(prefix="/api/conversations", tags=["chat"])


def convert_conversation_to_model(conv: Conversation) -> ConversationModel:
    """
    Convert Conversation to ConversationModel.

    Args:
        conv: Conversation instance

    Returns:
        ConversationModel for API response
    """
    messages = [
        MessageModel(
            id=msg.get("id"),
            role=msg["role"],
            content=msg["content"],
            timestamp=msg["timestamp"],
            duration=msg.get("duration"),
            event_count=msg.get("event_count"),
            success=msg.get("success"),
            is_auto=msg.get("is_auto", False),
        )
        for msg in conv.messages
    ]

    return ConversationModel(
        id=conv.id,
        urn=conv.urn,
        title=conv.title,
        messages=messages,
        created_at=conv.created_at,
    )


@router.get("", response_model=List[ConversationModel])
async def list_conversations(engine: ChatEngine = Depends(get_chat_engine)):
    """
    List all conversations.

    Args:
        engine: ChatEngine instance (injected)

    Returns:
        List of conversations
    """
    conversations = engine.list_conversations()
    return [convert_conversation_to_model(conv) for conv in conversations]


@router.post("", response_model=ConversationModel, status_code=201)
async def create_conversation(
    request: CreateConversationRequest, engine: ChatEngine = Depends(get_chat_engine)
):
    """
    Create a new conversation.

    Args:
        request: Create conversation request
        engine: ChatEngine instance (injected)

    Returns:
        Created conversation
    """
    conv = engine.create_conversation(request.title)
    logger.info(f"Created conversation: {conv.id}")
    return convert_conversation_to_model(conv)


@router.get("/{conv_id}", response_model=ConversationModel)
async def get_conversation(
    conv_id: str, conv: Conversation = Depends(get_conversation_or_404)
):
    """
    Get a conversation by ID.

    Args:
        conv_id: Conversation ID
        conv: Conversation instance (injected)

    Returns:
        Conversation details
    """
    return convert_conversation_to_model(conv)


@router.delete("/{conv_id}", status_code=204)
async def delete_conversation(
    conv_id: str, engine: ChatEngine = Depends(get_chat_engine)
):
    """
    Delete a conversation.

    Args:
        conv_id: Conversation ID
        engine: ChatEngine instance (injected)

    Raises:
        HTTPException: If conversation not found
    """
    success = engine.delete_conversation(conv_id)
    if not success:
        raise HTTPException(status_code=404, detail=f"Conversation {conv_id} not found")

    logger.info(f"Deleted conversation: {conv_id}")
    return None


@router.get("/{conv_id}/messages", response_model=List[MessageModel])
async def get_messages(conv: Conversation = Depends(get_conversation_or_404)):
    """
    Get all messages in a conversation.

    Args:
        conv: Conversation instance (injected)

    Returns:
        List of messages
    """
    messages = [
        MessageModel(
            id=msg.get("id"),
            role=msg["role"],
            content=msg["content"],
            timestamp=msg["timestamp"],
            duration=msg.get("duration"),
            event_count=msg.get("event_count"),
            success=msg.get("success"),
            is_auto=msg.get("is_auto", False),
        )
        for msg in conv.messages
    ]
    return messages


@router.post("/{conv_id}/messages")
async def send_message(
    conv_id: str,
    request: SendMessageRequest,
    engine: ChatEngine = Depends(get_chat_engine),
):
    """
    Send a message in a conversation.

    Supports both standard JSON response and Server-Sent Events (SSE) streaming.

    Args:
        conv_id: Conversation ID
        request: Send message request
        engine: ChatEngine instance (injected)

    Returns:
        StreamingResponse for SSE or JSON response
    """
    # Auto-start integrations service if needed
    from connection_manager import ConnectionManager
    manager = ConnectionManager()
    config = manager.load_active_config()
    service_status = ensure_integrations_service(config)
    logger.debug(f"Integrations service status: {service_status}")

    # Check if conversation exists
    conv = engine.get_conversation(conv_id)
    if not conv:
        raise HTTPException(status_code=404, detail=f"Conversation {conv_id} not found")

    # For now, return SSE stream by default
    async def event_generator():
        """Generate Server-Sent Events."""
        # Use a queue to bridge sync generator (thread) and async generator (event loop)
        queue = asyncio.Queue()
        loop = asyncio.get_running_loop()

        def run_generator_in_thread():
            """Run the blocking generator in a thread and put events in queue."""
            try:
                for event in engine.send_message_stream(
                    conv_id=conv_id,
                    message=request.message,
                    is_auto=request.is_auto,
                ):
                    # Put event in queue (thread-safe)
                    asyncio.run_coroutine_threadsafe(queue.put(event), loop)

                # Signal completion
                asyncio.run_coroutine_threadsafe(queue.put(None), loop)
            except Exception as e:
                logger.error(f"Error in generator thread: {e}")
                asyncio.run_coroutine_threadsafe(
                    queue.put({"event_type": "error", "error": str(e)}),
                    loop
                )

        # Start the generator in a thread pool
        loop.run_in_executor(None, run_generator_in_thread)

        # Stream events from queue
        try:
            while True:
                event = await queue.get()

                if event is None:  # Completion signal
                    break

                event_type = event["event_type"]

                if event_type == "message":
                    # Stream message event - already formatted from chat_engine
                    msg = event["message"]

                    # Convert message to serializable dict if needed
                    if not isinstance(msg, dict):
                        # Handle Pydantic models or dataclasses
                        if hasattr(msg, 'model_dump'):
                            msg = msg.model_dump()
                        elif hasattr(msg, '__dict__'):
                            msg = {k: v for k, v in msg.__dict__.items() if not k.startswith('_')}
                        else:
                            # Fallback - convert to string
                            msg = {"type": "TEXT", "content": {"text": str(msg)}}

                    # Extract message type from the message or event
                    # The message object has type="internal" but we need the actual type (THINKING, TOOL_CALL, etc.)
                    message_type = event.get("message_type") or msg.get("type", "TEXT")

                    # Forward the properly formatted message with message_type
                    yield f"event: message\n"
                    yield f"data: {json.dumps({'message': msg, 'message_type': message_type})}\n\n"
                    # Force async yield to flush immediately
                    await asyncio.sleep(0)

                elif event_type == "complete":
                    # Stream completion event
                    yield f"event: done\n"
                    yield f"data: {json.dumps({'duration': event.get('duration', 0)})}\n\n"
                    break

                elif event_type == "error":
                    # Stream error event
                    yield f"event: error\n"
                    yield f"data: {json.dumps({'error': event['error']})}\n\n"
                    break

        except Exception as e:
            logger.error(f"Error in event stream: {e}")
            yield f"event: error\n"
            yield f"data: {json.dumps({'error': str(e)})}\n\n"

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",  # Disable nginx buffering
        },
    )


@router.post("/{conv_id}/messages/sync", response_model=SendMessageResponse)
async def send_message_sync(
    conv_id: str,
    request: SendMessageRequest,
    engine: ChatEngine = Depends(get_chat_engine),
):
    """
    Send a message in a conversation (synchronous, no streaming).

    Args:
        conv_id: Conversation ID
        request: Send message request
        engine: ChatEngine instance (injected)

    Returns:
        Response with success status and duration
    """
    # Check if conversation exists
    conv = engine.get_conversation(conv_id)
    if not conv:
        raise HTTPException(status_code=404, detail=f"Conversation {conv_id} not found")

    # Send message (blocking)
    success, error_msg, duration = engine.send_message(
        conv_id=conv_id,
        message=request.message,
        is_auto=request.is_auto,
    )

    return SendMessageResponse(
        success=success,
        error=error_msg,
        duration=duration,
    )
