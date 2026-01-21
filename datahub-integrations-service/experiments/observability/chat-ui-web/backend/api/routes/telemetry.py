"""
Telemetry Routes - API endpoints for tool call telemetry and analytics.

These endpoints provide access to tool call telemetry data from DataHub's
usage events, enabling observability into agent behavior and tool usage patterns.
"""

import sys
from pathlib import Path
from typing import List, Optional
from urllib.parse import unquote

from fastapi import APIRouter, Depends, HTTPException, Query
from loguru import logger

# Add backend directory to path
backend_dir = Path(__file__).parent.parent.parent
if str(backend_dir) not in sys.path:
    sys.path.insert(0, str(backend_dir))

parent_dir = backend_dir.parent.parent
if str(parent_dir) not in sys.path:
    sys.path.insert(0, str(parent_dir))

from api.dependencies import get_telemetry_client
from api.models import (
    ConversationTelemetryModel,
    ToolCallModel,
    ToolUsageAggregateModel,
)
from core.telemetry_client import TelemetryClient

try:
    from datahub.ingestion.graph.client import DataHubGraph
except ImportError:
    DataHubGraph = None  # type: ignore

router = APIRouter(prefix="/api/telemetry", tags=["telemetry"])


@router.get("/conversations/{urn:path}/tool-calls", response_model=List[ToolCallModel])
async def get_conversation_tool_calls(
    urn: str,
    start_time: Optional[int] = Query(
        None, description="Start timestamp in epoch milliseconds"
    ),
    end_time: Optional[int] = Query(
        None, description="End timestamp in epoch milliseconds"
    ),
    client: TelemetryClient = Depends(get_telemetry_client),
):
    """
    Get all tool calls for a specific conversation.

    Returns detailed information about each tool execution including:
    - Tool name and input arguments
    - Execution duration and result size
    - Error status and messages
    - Timestamps for timeline analysis

    Args:
        urn: Conversation URN (URL-encoded, e.g., urn%3Ali%3AdataHubAiConversation%3A123)
        start_time: Optional start timestamp filter (epoch milliseconds)
        end_time: Optional end timestamp filter (epoch milliseconds)
        client: TelemetryClient instance (injected)

    Returns:
        List of tool call executions sorted by timestamp

    Raises:
        HTTPException: 500 on error
    """
    try:
        decoded_urn = unquote(urn)
        logger.info(f"Fetching tool calls for conversation: {decoded_urn}")
        tool_calls = client.get_tool_calls_for_conversation(
            decoded_urn, start_time=start_time, end_time=end_time
        )

        logger.info(
            f"Retrieved {len(tool_calls)} tool calls for conversation {decoded_urn}"
        )

        return [ToolCallModel(**tc) for tc in tool_calls]

    except Exception as e:
        logger.error(f"Failed to get tool calls for conversation {urn}: {e}")
        raise HTTPException(status_code=500, detail=str(e)) from e


@router.get(
    "/conversations/{urn:path}/telemetry", response_model=ConversationTelemetryModel
)
async def get_conversation_telemetry(
    urn: str,
    client: TelemetryClient = Depends(get_telemetry_client),
):
    """
    Get aggregated telemetry for a conversation.

    Returns summary metrics including:
    - Total tool call count and error count
    - List of all tool executions
    - Average response times
    - Total thinking/processing time

    Args:
        urn: Conversation URN (URL-encoded)
        client: TelemetryClient instance (injected)

    Returns:
        Aggregated telemetry metrics for the conversation

    Raises:
        HTTPException: 404 if no telemetry found, 500 on error
    """
    try:
        decoded_urn = unquote(urn)
        logger.info(f"Fetching telemetry for conversation: {decoded_urn}")

        # Fetch tool calls
        tool_calls = client.get_tool_calls_for_conversation(decoded_urn)

        # Fetch interaction events for conversation-level metrics
        interaction_events = client.get_interaction_events_for_conversation(decoded_urn)

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
                avg_response_time = round(sum(response_times) / len(response_times), 3)
                total_thinking_time = round(sum(response_times), 3)

        # Build telemetry model
        telemetry = ConversationTelemetryModel(
            num_tool_calls=len(tool_calls),
            num_tool_call_errors=sum(1 for tc in tool_calls if tc.get("is_error")),
            tool_calls=[ToolCallModel(**tc) for tc in tool_calls],
            avg_response_time=avg_response_time,
            total_thinking_time=total_thinking_time,
        )

        logger.info(
            f"Retrieved telemetry for conversation {decoded_urn}: "
            f"{telemetry.num_tool_calls} tool calls, {telemetry.num_tool_call_errors} errors"
        )

        return telemetry

    except Exception as e:
        logger.error(f"Failed to get telemetry for conversation {urn}: {e}")
        raise HTTPException(status_code=500, detail=str(e)) from e


@router.get("/tool-usage/aggregate", response_model=ToolUsageAggregateModel)
async def get_aggregate_tool_usage(
    start_time: Optional[int] = Query(
        None, description="Start timestamp in epoch milliseconds"
    ),
    end_time: Optional[int] = Query(
        None, description="End timestamp in epoch milliseconds"
    ),
    conversation_urns: Optional[str] = Query(
        None,
        description="Comma-separated list of conversation URNs to filter (optional)",
    ),
    client: TelemetryClient = Depends(get_telemetry_client),
):
    """
    Get aggregated tool usage statistics across conversations.

    Returns analytics across all tool calls including:
    - Usage counts per tool
    - Error counts per tool
    - Average execution duration per tool

    Useful for identifying:
    - Most frequently used tools
    - Tools with high error rates
    - Performance bottlenecks (slow tools)

    Args:
        start_time: Optional start timestamp filter (epoch milliseconds)
        end_time: Optional end timestamp filter (epoch milliseconds)
        conversation_urns: Optional comma-separated list of conversation URNs to filter
        client: TelemetryClient instance (injected)

    Returns:
        Aggregated tool usage statistics

    Raises:
        HTTPException: 500 on error
    """
    try:
        logger.info(
            f"Fetching aggregate tool usage (start={start_time}, end={end_time}, "
            f"conversations={conversation_urns})"
        )

        # Parse conversation URNs if provided
        urn_list = None
        if conversation_urns:
            urn_list = [
                urn.strip() for urn in conversation_urns.split(",") if urn.strip()
            ]
        aggregates = client.aggregate_tool_usage(
            conversation_urns=urn_list,
            start_time=start_time,
            end_time=end_time,
        )

        total_calls = sum(aggregates["tool_counts"].values())
        total_errors = sum(aggregates["tool_errors"].values())

        logger.info(
            f"Retrieved tool usage: {len(aggregates['tool_counts'])} unique tools, "
            f"{total_calls} total calls, {total_errors} errors"
        )

        return ToolUsageAggregateModel(**aggregates)

    except Exception as e:
        logger.error(f"Failed to get aggregate tool usage: {e}")
        raise HTTPException(status_code=500, detail=str(e)) from e
