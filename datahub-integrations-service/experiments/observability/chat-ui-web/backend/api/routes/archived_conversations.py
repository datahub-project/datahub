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

from api.dependencies import get_datahub_graph
from api.models import ArchivedConversationListModel, ArchivedConversationModel
from core.datahub_conversation_client import ChatUIDataHubClient

try:
    from datahub.ingestion.graph.client import DataHubGraph
except ImportError:
    DataHubGraph = None  # type: ignore

router = APIRouter(prefix="/api/archived-conversations", tags=["archived"])


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
    graph: "DataHubGraph" = Depends(get_datahub_graph),  # type: ignore
):
    """
    List archived conversations from DataHub.

    Returns conversations from all sources (Slack, Teams, DataHub UI) with pagination and sorting.

    Args:
        start: Starting offset for pagination
        count: Number of conversations to fetch (max 100)
        origin_type: Optional filter by origin (DATAHUB_UI, SLACK, TEAMS, INGESTION_UI)
        sort_by: Sort field (max_thinking_time, num_turns, created) - default: max_thinking_time
        sort_desc: Sort descending (true) or ascending (false) - default: true
        graph: DataHub graph client (injected)

    Returns:
        List of archived conversations with pagination info
    """
    try:
        client = ChatUIDataHubClient(graph)
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


@router.get("/{urn:path}", response_model=ArchivedConversationModel)
async def get_archived_conversation(
    urn: str,
    graph: "DataHubGraph" = Depends(get_datahub_graph),  # type: ignore
):
    """
    Get a specific archived conversation with full message history.

    Args:
        urn: Conversation URN (URL-encoded, e.g., urn%3Ali%3AdataHubAiConversation%3A123)
        graph: DataHub graph client (injected)

    Returns:
        Full conversation with all messages

    Raises:
        HTTPException: 404 if conversation not found, 500 on error
    """
    try:
        decoded_urn = unquote(urn)
        logger.debug(f"Fetching archived conversation: {decoded_urn}")

        client = ChatUIDataHubClient(graph)
        conversation = client.get_archived_conversation(decoded_urn)

        if not conversation:
            raise HTTPException(
                status_code=404, detail=f"Conversation {decoded_urn} not found"
            )

        return client.convert_to_ui_format(conversation)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get archived conversation {urn}: {e}")
        raise HTTPException(status_code=500, detail=str(e))
