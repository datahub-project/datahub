"""
Archived Conversations Routes - API endpoints for viewing archived conversations.

These endpoints provide read-only access to conversations stored in DataHub
(from Slack, Teams, DataHub UI, etc.).
"""

import sys
from pathlib import Path
from urllib.parse import unquote

from fastapi import APIRouter, Depends, HTTPException, Query
from loguru import logger

parent_dir = Path(__file__).parent.parent.parent.parent.parent
if str(parent_dir) not in sys.path:
    sys.path.insert(0, str(parent_dir))

backend_dir = Path(__file__).parent.parent.parent
if str(backend_dir) not in sys.path:
    sys.path.insert(0, str(backend_dir))

from api.dependencies import get_datahub_graph, get_telemetry_client, get_conversation_client
from api.models import (
    ArchivedConversationListModel,
    ArchivedConversationModel,
    ArchivedConversationWithTelemetryModel,
    ArchivedMessageModel,
    ConversationTelemetryModel,
    ToolCallModel,
)
from core.conversation_health import compute_health_status
from core.datahub_conversation_client import ChatUIDataHubClient
from core.telemetry_client import TelemetryClient

try:
    from datahub.ingestion.graph.client import DataHubGraph
except ImportError:
    DataHubGraph = None  # type: ignore

router = APIRouter(prefix="/api/archived-conversations", tags=["archived"])


@router.post("/refresh")
async def refresh_conversation_list(
    client: ChatUIDataHubClient = Depends(get_conversation_client),
):
    """
    Invalidate the conversation list cache and force a fresh fetch on next request.

    This is useful when new conversations have been created and you want to see them
    immediately without waiting for the cache TTL (60 minutes) to expire.

    Returns:
        Success message
    """
    try:
        client.invalidate_list_cache()
        client.invalidate_conversation_cache()  # Also clear individual conversation caches
        logger.info("Conversation list and individual conversation caches invalidated via API request")
        return {"success": True, "message": "All conversation caches cleared. Next request will fetch fresh data."}
    except Exception as e:
        logger.error(f"Failed to refresh conversation list: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("", response_model=ArchivedConversationListModel)
async def list_archived_conversations(
    start: int = Query(0, ge=0, description="Starting offset for pagination"),
    count: int = Query(20, ge=1, le=100, description="Number of conversations to fetch"),
    origin_type: str | None = Query(
        None,
        regex="^(DATAHUB_UI|SLACK|TEAMS|INGESTION_UI)$",
        description="Filter by origin type",
    ),
    sort_by: str = Query(
        "max_thinking_time",
        regex="^(max_thinking_time|num_turns|created)$",
        description="Sort field: max_thinking_time, num_turns, or created",
    ),
    sort_desc: bool = Query(
        True,
        description="Sort descending (true) or ascending (false)",
    ),
    client: ChatUIDataHubClient = Depends(get_conversation_client),
):
    """
    List archived conversations from DataHub.

    Returns conversations from all sources (Slack, Teams, DataHub UI) with pagination and sorting.
    Uses a singleton client with caching to avoid redundant GMS queries when filtering/sorting.

    Args:
        start: Starting offset for pagination
        count: Number of conversations to fetch (max 100)
        origin_type: Optional filter by origin (DATAHUB_UI, SLACK, TEAMS, INGESTION_UI)
        sort_by: Sort field (max_thinking_time, num_turns, created) - default: max_thinking_time
        sort_desc: Sort descending (true) or ascending (false) - default: true
        client: ChatUIDataHubClient singleton (injected)

    Returns:
        List of archived conversations with pagination info
    """
    try:
        result = client.list_archived_conversations(start, count, origin_type, sort_by, sort_desc)

        conversations = [
            client.convert_to_ui_format(conv)
            for conv in result.get("conversations", [])
        ]

        return ArchivedConversationListModel(
            conversations=conversations,
            total=result.get("total", 0),
            start=start,
            count=len(conversations),
        )
    except Exception as e:
        logger.error(f"Failed to list archived conversations: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{urn:path}", response_model=ArchivedConversationWithTelemetryModel)
async def get_archived_conversation(
    urn: str,
    include_telemetry: bool = Query(
        True,
        description="Include tool call telemetry data (tool calls, durations, errors)",
    ),
    client: ChatUIDataHubClient = Depends(get_conversation_client),
    telemetry_client: TelemetryClient = Depends(get_telemetry_client),
):
    """
    Get a specific archived conversation with full message history.

    Optionally enriches the conversation with tool call telemetry data including:
    - All tool calls made during the conversation
    - Tool execution durations and result sizes
    - Error information for failed tool calls
    - Aggregate metrics (total calls, errors, thinking time)

    Args:
        urn: Conversation URN (URL-encoded, e.g., urn%3Ali%3AdataHubAiConversation%3A123)
        include_telemetry: If true, fetch and include tool call telemetry (default: false)
        client: ChatUIDataHubClient singleton (injected)
        telemetry_client: TelemetryClient singleton (injected)

    Returns:
        Full conversation with all messages and optional telemetry data

    Raises:
        HTTPException: 404 if conversation not found, 500 on error
    """
    try:
        decoded_urn = unquote(urn)
        logger.debug(
            f"Fetching archived conversation: {decoded_urn} (telemetry={include_telemetry})"
        )

        conversation = client.get_archived_conversation(decoded_urn)

        if not conversation:
            raise HTTPException(
                status_code=404, detail=f"Conversation {decoded_urn} not found"
            )

        conversation_dict = client.convert_to_ui_format(conversation)

        # Compute health status from messages
        try:
            messages = conversation_dict.get("messages", [])
            # Convert dict messages to ArchivedMessageModel objects
            message_models = [ArchivedMessageModel(**msg) for msg in messages]
            health_status = compute_health_status(message_models)
            conversation_dict["health_status"] = health_status.model_dump()
            logger.debug(
                f"Computed health status for {decoded_urn}: "
                f"abandoned={health_status.is_abandoned}, "
                f"unanswered={health_status.unanswered_questions_count}"
            )
        except Exception as e:
            # Log error but don't fail the request
            logger.warning(f"Failed to compute health status for {decoded_urn}: {e}")
            conversation_dict["health_status"] = None

        # Optionally enrich with telemetry data
        if include_telemetry:
            try:
                logger.debug(f"Enriching conversation {decoded_urn} with telemetry")

                # Check if conversation already has telemetry data from client
                existing_telemetry = conversation_dict.get("telemetry")

                # Fetch tool calls
                tool_calls = telemetry_client.get_tool_calls_for_conversation(
                    decoded_urn
                )

                # Use existing interaction events if available, otherwise fetch
                if existing_telemetry and existing_telemetry.get("interaction_events"):
                    logger.debug(f"Using {len(existing_telemetry['interaction_events'])} interaction events from conversation client")
                    interaction_events = existing_telemetry["interaction_events"]
                else:
                    # Fetch interaction events for conversation-level metrics
                    interaction_events = (
                        telemetry_client.get_interaction_events_for_conversation(decoded_urn)
                    )

                # Calculate average response time from interaction events
                avg_response_time = None
                total_thinking_time = None

                if interaction_events:
                    response_times = [
                        ie.get("response_generation_duration_sec", 0)
                        for ie in interaction_events
                        if ie.get("response_generation_duration_sec") is not None
                    ]
                    if response_times:
                        avg_response_time = round(
                            sum(response_times) / len(response_times), 3
                        )
                        total_thinking_time = round(sum(response_times), 3)

                # Build telemetry model
                from api.models import InteractionEventModel

                interaction_event_models = [
                    InteractionEventModel(
                        message_id=ie.get("message_id", ""),
                        timestamp=ie.get("timestamp", 0),
                        num_tool_calls=ie.get("num_tool_calls", 0),
                        response_generation_duration_sec=ie.get(
                            "response_generation_duration_sec", 0.0
                        ),
                        full_history=ie.get("full_history", "{}"),
                    )
                    for ie in interaction_events
                ]

                telemetry = ConversationTelemetryModel(
                    num_tool_calls=len(tool_calls),
                    num_tool_call_errors=sum(
                        1 for tc in tool_calls if tc.get("is_error")
                    ),
                    tool_calls=[ToolCallModel(**tc) for tc in tool_calls],
                    interaction_events=interaction_event_models,
                    avg_response_time=avg_response_time,
                    total_thinking_time=total_thinking_time,
                )

                conversation_dict["telemetry"] = telemetry.model_dump()

                logger.info(
                    f"Enriched conversation {decoded_urn} with telemetry: "
                    f"{telemetry.num_tool_calls} tool calls, {telemetry.num_tool_call_errors} errors"
                )

            except Exception as e:
                # Log error but don't fail the request
                logger.warning(
                    f"Failed to fetch telemetry for conversation {decoded_urn}: {e}"
                )
                # Set telemetry to None (will be excluded from response)
                conversation_dict["telemetry"] = None

        return ArchivedConversationWithTelemetryModel(**conversation_dict)

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get archived conversation {urn}: {e}")
        raise HTTPException(status_code=500, detail=str(e))
