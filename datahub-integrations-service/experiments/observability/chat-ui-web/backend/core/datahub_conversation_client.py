"""
DataHub Conversation Client - Integration layer for archived conversations.

This module provides a wrapper around DataHubAiConversationClient from
datahub-integrations-service to fetch and convert archived conversations
from DataHub's GraphQL API for display in the chat-ui-web frontend.

Enhanced to also fetch Slack/Teams conversations from telemetry events.
"""

from typing import Any, Dict, List, Optional
import time
import json

from loguru import logger

import sys
from pathlib import Path

parent_dir = Path(__file__).parent.parent.parent.parent
if str(parent_dir) not in sys.path:
    sys.path.insert(0, str(parent_dir))

from datahub_integrations.chat.datahub_ai_conversation_client import (
    DataHubAiConversationClient,
)
from datahub.sdk.main_client import DataHubClient
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.graph.filters import RemovedStatusFilter
from datahub.metadata.schema_classes import DataHubAiConversationInfoClass

# Import TelemetryClient for fetching usage events
backend_dir = Path(__file__).parent.parent
if str(backend_dir) not in sys.path:
    sys.path.insert(0, str(backend_dir))

from core.telemetry_client import TelemetryClient

# Schema introspection query to discover available fields
INTROSPECTION_QUERY = """
query IntrospectListConversations {
  __type(name: "Query") {
    fields {
      name
      args {
        name
        type {
          name
          kind
          ofType {
            name
            kind
          }
        }
      }
    }
  }
  conversationType: __type(name: "DataHubAiConversation") {
    fields {
      name
    }
  }
}
"""


class ChatUIDataHubClient:
    """Client for fetching archived conversations from DataHub with dynamic schema detection."""

    def __init__(self, graph: DataHubGraph, cache_ttl: int = 300):
        """
        Initialize the client and discover schema capabilities.

        Args:
            graph: DataHubGraph client
            cache_ttl: Time-to-live for conversation cache in seconds (default: 5 minutes)
        """
        self.graph = graph
        # Create DataHubClient wrapper for conversation client
        self.datahub_client = DataHubClient(graph=graph)
        self.conversation_client = DataHubAiConversationClient(self.datahub_client)

        # Telemetry client for fetching usage events (Slack/Teams conversations)
        self.telemetry_client = TelemetryClient(graph, cache_ttl=0)  # Disable telemetry client's own cache

        # Cached schema capabilities (discovered on first use)
        self._schema_capabilities: Optional[Dict[str, bool]] = None
        self._list_query: Optional[str] = None

        # Conversation cache: {urn: (conversation_dict, timestamp)}
        self._conversation_cache: Dict[str, tuple[Dict[str, Any], float]] = {}
        self._cache_ttl = cache_ttl

        # DataHub entity list cache: (conversations_list, timestamp)
        self._list_cache: Optional[tuple[List[Dict[str, Any]], float]] = None
        self._list_cache_ttl = 3600  # 60 minutes

        # Telemetry events cache: (conversations_list, timestamp)
        self._telemetry_list_cache: Optional[tuple[List[Dict[str, Any]], float]] = None
        self._telemetry_cache_ttl = 3600  # 60 minutes

    def _discover_schema_capabilities(self) -> Dict[str, bool]:
        """
        Introspect GraphQL schema to discover available fields and arguments.

        Returns:
            Dictionary with capabilities: has_origin_type_arg, has_context_field, has_origin_type_field
        """
        try:
            result = self.graph.execute_graphql(INTROSPECTION_QUERY)

            # Find listDataHubAiConversations query
            has_origin_type_arg = False
            query_type = result.get("__type", {})
            for field in query_type.get("fields", []):
                if field.get("name") == "listDataHubAiConversations":
                    for arg in field.get("args", []):
                        if arg.get("name") == "originType":
                            has_origin_type_arg = True
                            break
                    break

            # Find DataHubAiConversation type fields
            has_context_field = False
            has_origin_type_field = False
            conversation_type = result.get("conversationType", {})
            for field in conversation_type.get("fields", []):
                field_name = field.get("name")
                if field_name == "context":
                    has_context_field = True
                elif field_name == "originType":
                    has_origin_type_field = True

            capabilities = {
                "has_origin_type_arg": has_origin_type_arg,
                "has_context_field": has_context_field,
                "has_origin_type_field": has_origin_type_field,
            }
            logger.info(f"Discovered schema capabilities: {capabilities}")
            return capabilities

        except Exception as e:
            logger.warning(f"Schema introspection failed: {e}. Using minimal query.")
            # Fallback to most conservative query
            return {
                "has_origin_type_arg": False,
                "has_context_field": False,
                "has_origin_type_field": False,
            }

    def _build_list_query(self, capabilities: Dict[str, bool]) -> str:
        """
        Build the listDataHubAiConversations query based on schema capabilities.

        Args:
            capabilities: Schema capabilities from introspection

        Returns:
            GraphQL query string
        """
        # Build query header with parameters
        if capabilities["has_origin_type_arg"]:
            query_header = "query listDataHubAiConversations($start: Int, $count: Int, $originType: DataHubAiConversationOriginType)"
            query_args = "start: $start, count: $count, originType: $originType"
        else:
            query_header = "query listDataHubAiConversations($start: Int, $count: Int)"
            query_args = "start: $start, count: $count"

        # Build conversation fields
        conversation_fields = [
            "urn",
            "title",
            "created { time actor }",
            "lastUpdated { time actor }",
            "messageCount",
        ]

        if capabilities["has_origin_type_field"]:
            conversation_fields.append("originType")

        if capabilities["has_context_field"]:
            conversation_fields.append("context { text entityUrns }")

        conversation_fields_str = "\n            ".join(conversation_fields)

        query = f"""
{query_header} {{
    listDataHubAiConversations({query_args}) {{
        conversations {{
            {conversation_fields_str}
        }}
        total
    }}
}}
"""
        logger.debug(f"Built dynamic query: {query}")
        return query

    def _fetch_all_conversations(self) -> List[Dict[str, Any]]:
        """
        Fetch ALL conversations from DataHub (no pagination, no filtering).

        Returns:
            List of all conversation dictionaries
        """
        logger.info("Fetching ALL conversations from DataHub (cache miss)")

        # Fetch all URNs (use large count to get everything)
        all_urns = self.graph.list_all_entity_urns(
            entity_type="dataHubAiConversation",
            start=0,
            count=10000  # Large number to get all
        )

        if not all_urns:
            logger.warning("No conversations found in DataHub")
            return []

        logger.info(f"Found {len(all_urns)} total conversation URNs, fetching details...")

        conversations = []
        for urn in all_urns:
            try:
                # Fetch full conversation info
                conversation_info = self.graph.get_aspect(
                    entity_urn=urn, aspect_type=DataHubAiConversationInfoClass
                )

                if not conversation_info:
                    continue

                # Convert to dictionary format
                created_time = conversation_info.created.time if conversation_info.created else 0
                created_actor = conversation_info.created.actor if conversation_info.created else None
                message_count = len(conversation_info.messages) if conversation_info.messages else 0

                # Calculate thinking time metrics per turn
                max_thinking_time_ms = 0
                num_turns = 0
                if conversation_info.messages:
                    current_turn_thinking = 0
                    prev_time = None
                    in_turn = False

                    for msg in conversation_info.messages:
                        msg_type = msg.type if msg.type else "TEXT"
                        msg_time = msg.time if msg.time else 0
                        actor_type = msg.actor.type if msg.actor and msg.actor.type else "USER"

                        if actor_type == "USER":
                            if in_turn:
                                max_thinking_time_ms = max(max_thinking_time_ms, current_turn_thinking)
                            num_turns += 1
                            current_turn_thinking = 0
                            in_turn = True
                        elif actor_type == "AGENT" and in_turn:
                            if prev_time is not None and msg_type == "THINKING":
                                duration = msg_time - prev_time
                                current_turn_thinking += duration

                        prev_time = msg_time

                    if in_turn and current_turn_thinking > 0:
                        max_thinking_time_ms = max(max_thinking_time_ms, current_turn_thinking)

                conversation_dict = {
                    "urn": urn,
                    "title": conversation_info.title if conversation_info.title else None,
                    "created": {
                        "time": created_time,
                        "actor": created_actor,
                    },
                    "lastUpdated": {
                        "time": created_time,
                        "actor": created_actor,
                    },
                    "messageCount": message_count,
                    "originType": conversation_info.originType if conversation_info.originType else None,
                    "context": None,
                    "maxThinkingTimeMs": max_thinking_time_ms,
                    "numTurns": num_turns,
                }

                if conversation_info.context:
                    conversation_dict["context"] = {
                        "text": conversation_info.context.text if conversation_info.context.text else None,
                        "entityUrns": conversation_info.context.entityUrns if conversation_info.context.entityUrns else None,
                    }

                conversations.append(conversation_dict)

            except Exception as e:
                logger.warning(f"Failed to fetch conversation {urn}: {e}")
                continue

        logger.info(f"Successfully fetched {len(conversations)} conversations")
        return conversations

    def _group_events_by_thread(self, events: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
        """
        Group telemetry events by chat_id (Slack/Teams thread).

        Args:
            events: List of ChatbotInteraction event hits

        Returns:
            Dict mapping chat_id to list of events (sorted by timestamp)
        """
        threads = {}
        for event in events:
            source = event.get("_source", {})
            chat_id = source.get("chat_id")
            if not chat_id:
                logger.warning(f"Event missing chat_id: {event.get('_id')}")
                continue

            if chat_id not in threads:
                threads[chat_id] = []
            threads[chat_id].append(event)

        # Sort events within each thread by timestamp
        for chat_id in threads:
            threads[chat_id] = sorted(threads[chat_id], key=lambda e: e.get("_source", {}).get("timestamp", 0))

        logger.debug(f"Grouped {len(events)} events into {len(threads)} threads")
        return threads

    def _get_slack_conversation_type(self, chat_id: str) -> str:
        """
        Determine Slack conversation type from chat_id.

        Slack channel IDs follow a prefix convention:
        - C* = Public channel
        - G* = Private channel/group
        - D* = Direct message (DM)

        Args:
            chat_id: Chat ID in format "slack_v1_chat_{channel_id}_{thread_ts}"

        Returns:
            "channel", "private_channel", "dm", or None if not Slack or unknown
        """
        # Extract channel_id from chat_id
        # Format: slack_v1_chat_C099XLRAAP2_1768687956.664159
        if not chat_id or not chat_id.startswith("slack_v1_chat_"):
            return None

        parts = chat_id.split("_")
        if len(parts) >= 4:
            channel_id = parts[3]  # e.g., "C099XLRAAP2"

            if channel_id.startswith("C"):
                return "channel"
            elif channel_id.startswith("G"):
                return "private_channel"
            elif channel_id.startswith("D"):
                return "dm"

        return None

    def _parse_full_history_messages(
        self, full_history_json: str, event_timestamp: int, chatbot: str, thread_events: List[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """
        Parse full_history JSON and convert to ArchivedMessageModel format.

        IMPORTANT: Assigns timestamps based on which event/turn each message belongs to,
        using thread_events to map turns to their interaction event timestamps.

        Args:
            full_history_json: JSON string from event._source.full_history
            event_timestamp: Event timestamp (milliseconds) - used as fallback
            chatbot: "slack" or "teams"
            thread_events: List of all events in this thread (in timestamp order)

        Returns:
            List of message dicts in ArchivedMessageModel format
        """
        try:
            if isinstance(full_history_json, str):
                history = json.loads(full_history_json)
            else:
                history = full_history_json

            raw_messages = history.get("messages", [])
            converted_messages = []

            # Build timestamp map: turn_index -> event_timestamp
            turn_timestamps = {}
            if thread_events:
                for i, event in enumerate(thread_events):
                    turn_timestamps[i] = event.get("_source", {}).get("timestamp", event_timestamp)

            # Track which turn we're in (0-indexed)
            current_turn = 0
            current_turn_timestamp = turn_timestamps.get(0, event_timestamp)
            message_offset = 0  # Offset within current turn (in milliseconds)

            # Track tool_call messages to detect respond_to_user results
            tool_call_map = {}  # tool_use_id -> tool_name

            for i, msg in enumerate(raw_messages):
                msg_type = msg.get("type")
                text = msg.get("text", "")

                # When we see a human message, it marks the start of a new turn
                # (except for the first one which starts turn 0)
                if msg_type == "human" and i > 0:
                    current_turn += 1
                    current_turn_timestamp = turn_timestamps.get(current_turn, event_timestamp)
                    message_offset = 0  # Reset offset for new turn

                # Convert telemetry message type to ArchivedMessageModel format
                # Add small offset (1ms per message) within turn to enable duration calculation
                msg_timestamp = current_turn_timestamp + message_offset

                if msg_type == "human":
                    # User message
                    converted_messages.append({
                        "type": "TEXT",
                        "role": "user",
                        "content": text,
                        "timestamp": msg_timestamp,
                    })
                elif msg_type == "internal":
                    # Thinking block
                    converted_messages.append({
                        "type": "THINKING",
                        "role": "assistant",
                        "content": text,
                        "timestamp": msg_timestamp,
                    })
                elif msg_type == "tool_call":
                    # Tool invocation - track for respond_to_user detection
                    tool_use_id = msg.get("tool_use_id")
                    tool_name = msg.get("tool_name")

                    if tool_use_id and tool_name:
                        tool_call_map[tool_use_id] = tool_name

                    converted_messages.append({
                        "type": "TOOL_CALL",
                        "role": "assistant",
                        "content": json.dumps({
                            "tool_use_id": tool_use_id,
                            "tool_name": tool_name,
                            "tool_input": msg.get("tool_input"),
                        }),
                        "timestamp": msg_timestamp,
                    })
                elif msg_type == "tool_result":
                    # Tool response - check if this is respond_to_user (contains final response text)
                    tool_use_id = msg.get("tool_request", {}).get("tool_use_id")
                    tool_name = tool_call_map.get(tool_use_id, "")
                    result = msg.get("result", {})

                    # Special handling for respond_to_user: extract text as final response
                    if tool_name == "respond_to_user" and isinstance(result, dict) and "text" in result:
                        response_text = result.get("text", "")
                        if response_text:
                            # Add as TEXT message (final response)
                            converted_messages.append({
                                "type": "TEXT",
                                "role": "assistant",
                                "content": response_text,
                                "timestamp": msg_timestamp,
                            })
                    else:
                        # Regular tool result - keep as TOOL_RESULT
                        converted_messages.append({
                            "type": "TOOL_RESULT",
                            "role": "assistant",
                            "content": json.dumps({
                                "tool_use_id": tool_use_id,
                                "result": result,
                                "is_error": msg.get("is_error", False),
                            }),
                            "timestamp": msg_timestamp,
                        })
                elif msg_type == "assistant":
                    # AI response
                    converted_messages.append({
                        "type": "TEXT",
                        "role": "assistant",
                        "content": text,
                        "timestamp": msg_timestamp,
                    })

                # Increment offset for next message (1ms per message)
                message_offset += 1

            logger.debug(f"Parsed {len(converted_messages)} messages from full_history across {current_turn + 1} turns")
            return converted_messages

        except Exception as e:
            logger.warning(f"Failed to parse full_history: {e}")
            return []

    def _calculate_thinking_metrics(
        self,
        messages: List[Dict[str, Any]],
        thread_events: List[Dict[str, Any]] = None
    ) -> tuple[int, int]:
        """
        Calculate max_thinking_time_ms and num_turns from messages.

        Args:
            messages: Parsed messages from full_history
            thread_events: Optional list of telemetry events for this thread
                          (used to extract actual response_generation_duration_sec)

        Returns:
            (max_thinking_time_ms, num_turns)
        """
        num_turns = 0
        max_thinking_time_ms = 0

        # Count user messages as turns
        for msg in messages:
            if msg.get("role") == "user" and msg.get("type") == "TEXT":
                num_turns += 1

        # Use actual duration from telemetry events if available
        if thread_events:
            max_duration_sec = 0.0
            for event in thread_events:
                event_source = event.get("_source", {})
                duration_sec = event_source.get("response_generation_duration_sec")
                if duration_sec is not None and duration_sec > max_duration_sec:
                    max_duration_sec = duration_sec

            if max_duration_sec > 0:
                max_thinking_time_ms = int(max_duration_sec * 1000)
                logger.debug(f"Using actual duration: {max_duration_sec}s ({max_thinking_time_ms}ms)")
            else:
                # Fallback to heuristic if no duration data
                thinking_messages = [m for m in messages if m.get("type") == "THINKING"]
                if thinking_messages:
                    max_thinking_time_ms = len(thinking_messages) * 1000
                    logger.debug(f"Using heuristic duration: {len(thinking_messages)} thinking blocks")
        else:
            # No telemetry events, use heuristic
            thinking_messages = [m for m in messages if m.get("type") == "THINKING"]
            if thinking_messages:
                max_thinking_time_ms = len(thinking_messages) * 1000

        return max_thinking_time_ms, num_turns

    def _fetch_telemetry_conversations(self) -> List[Dict[str, Any]]:
        """
        Fetch ALL Slack/Teams conversations from telemetry events using pagination.

        Uses time-based pagination to fetch events in batches of 100 (GMS limit).
        Continues fetching until all events are retrieved or we hit a safety limit.

        Steps:
        1. Query ChatbotInteraction events in time-descending order
        2. Use oldest timestamp from each batch to fetch next batch
        3. Continue until no more events or safety limit reached
        4. Group events by chat_id (thread)
        5. Parse full_history and calculate metrics
        6. Convert to ArchivedConversationModel format

        Returns:
            List of conversation dicts (same format as DataHub conversations)
        """
        logger.info("Fetching Slack/Teams conversations from telemetry events (with pagination)")

        try:
            all_hits = []
            page_size = 100  # Max size that GMS can handle
            max_pages = 20  # Safety limit (2000 events max)
            oldest_timestamp = None

            # Paginate through all events
            for page_num in range(max_pages):
                # Build query with time window
                query = {
                    "query": {
                        "term": {"type": "ChatbotInteraction"}
                    },
                    "sort": [{"timestamp": "desc"}],
                    "size": page_size,
                }

                # Add time filter if we have an oldest timestamp from previous page
                if oldest_timestamp is not None:
                    query["query"] = {
                        "bool": {
                            "must": [
                                {"term": {"type": "ChatbotInteraction"}},
                                {"range": {"timestamp": {"lt": oldest_timestamp}}}
                            ]
                        }
                    }

                logger.debug(f"Telemetry query (page {page_num + 1}): timestamp < {oldest_timestamp if oldest_timestamp else 'now'}")

                result = self.telemetry_client.query_usage_events(query, use_cache=False)
                page_hits = result.get("hits", {}).get("hits", [])
                total_available = result.get("hits", {}).get("total", {}).get("value", 0)

                if not page_hits:
                    logger.info(f"No more events found after page {page_num + 1}")
                    break

                all_hits.extend(page_hits)

                # Update oldest timestamp for next page
                last_event = page_hits[-1]
                oldest_timestamp = last_event.get("_source", {}).get("timestamp")

                logger.info(f"Page {page_num + 1}: Fetched {len(page_hits)} events (total so far: {len(all_hits)} of {total_available})")

                # Stop if we got fewer events than page size (last page)
                if len(page_hits) < page_size:
                    logger.info(f"Reached last page (got {len(page_hits)} < {page_size} events)")
                    break

            # Filter for Slack/Teams events client-side
            hits = [
                hit for hit in all_hits
                if hit.get("_source", {}).get("chatbot") in ["slack", "teams"]
            ]

            if not all_hits:
                logger.info("No ChatbotInteraction telemetry events found")
                return []

            logger.info(f"Fetched {len(all_hits)} total ChatbotInteraction events across {page_num + 1} pages, {len(hits)} from Slack/Teams")

            # Group events by thread (chat_id)
            threads = self._group_events_by_thread(hits)

            # Convert each thread to a conversation
            conversations = []
            for chat_id, thread_events in threads.items():
                try:
                    # Get metadata from first event
                    first_event = thread_events[0]
                    last_event = thread_events[-1]
                    first_source = first_event.get("_source", {})

                    chatbot = first_source.get("chatbot", "unknown")
                    origin_type = "SLACK" if chatbot == "slack" else "TEAMS" if chatbot == "teams" else "UNKNOWN"

                    # Parse messages from LAST event only (full_history contains complete conversation)
                    # Each event's full_history contains the entire conversation up to that point,
                    # so we only need the last event to get the complete history without duplicates
                    all_messages = []
                    if thread_events:
                        last_event = thread_events[-1]
                        event_source = last_event.get("_source", {})
                        full_history = event_source.get("full_history")
                        event_timestamp = event_source.get("timestamp", 0)

                        if full_history:
                            all_messages = self._parse_full_history_messages(
                                full_history, event_timestamp, chatbot, thread_events
                            )

                    if not all_messages:
                        logger.warning(f"Thread {chat_id} has no messages, skipping")
                        continue

                    # Extract title from first user message
                    first_user_message = next(
                        (m for m in all_messages if m.get("role") == "user"),
                        None
                    )
                    title = "Untitled Conversation"
                    if first_user_message:
                        content = first_user_message.get("content", "")
                        # Remove Slack mention syntax
                        content = content.replace("<@U09B2P0M1T7>", "").strip()
                        # Truncate to first 100 chars
                        title = content[:100] + "..." if len(content) > 100 else content

                    # Calculate metrics (pass thread_events to use actual response duration)
                    max_thinking_time_ms, num_turns = self._calculate_thinking_metrics(
                        all_messages, thread_events=thread_events
                    )

                    # Determine conversation type for Slack
                    slack_conversation_type = None
                    if chatbot == "slack":
                        slack_conversation_type = self._get_slack_conversation_type(chat_id)

                    # Compute health status from messages
                    health_status = None
                    try:
                        from api.models import ArchivedMessageModel
                        from core.conversation_health import compute_health_status as calc_health

                        # Convert messages to models for health computation
                        message_models = [
                            ArchivedMessageModel(
                                role=msg.get("role"),
                                content=msg.get("content", ""),
                                timestamp=msg.get("timestamp", 0),
                                message_type=msg.get("type", "TEXT"),
                                agent_name=msg.get("agent_name"),
                                user_name=msg.get("user_name"),
                                actor_urn=msg.get("actor_urn"),
                            )
                            for msg in all_messages
                        ]
                        health_status_obj = calc_health(message_models)
                        health_status = health_status_obj.model_dump()
                    except Exception as e:
                        logger.warning(f"Failed to compute health status for {chat_id}: {e}")

                    # Create conversation dict
                    conversation_dict = {
                        "urn": f"telemetry:{chat_id}",  # Synthetic URN
                        "title": title,
                        "created": {
                            "time": first_source.get("timestamp", 0),
                            "actor": first_source.get("actorUrn"),
                        },
                        "lastUpdated": {
                            "time": last_event.get("_source", {}).get("timestamp", 0),
                            "actor": last_event.get("_source", {}).get("actorUrn"),
                        },
                        "messageCount": len(all_messages),
                        "originType": origin_type,
                        "slackConversationType": slack_conversation_type,  # channel, dm, private_channel
                        "context": None,  # Telemetry events don't have context
                        "maxThinkingTimeMs": max_thinking_time_ms,
                        "numTurns": num_turns,
                        "health_status": health_status,  # Pre-computed health status
                        "_telemetry_chat_id": chat_id,  # Store for later retrieval
                        "_telemetry_events": thread_events,  # Cache events for get_archived_conversation
                    }

                    conversations.append(conversation_dict)

                except Exception as e:
                    logger.warning(f"Failed to convert thread {chat_id}: {e}")
                    continue

            logger.info(f"Successfully converted {len(conversations)} telemetry threads to conversations")
            return conversations

        except Exception as e:
            logger.error(f"Failed to fetch telemetry conversations: {e}")
            logger.warning("Telemetry fetch failed, will continue with DataHub conversations only")
            # Return empty list to allow DataHub conversations to still be shown
            return []

    def list_archived_conversations(
        self,
        start: int = 0,
        count: int = 20,
        origin_type: Optional[str] = None,
        sort_by: str = "max_thinking_time",
        sort_desc: bool = True,
    ) -> Dict[str, Any]:
        """
        List archived conversations with client-side filtering, sorting, and pagination.

        This method fetches from BOTH DataHub entities AND telemetry events, merges them,
        and caches the results. Filters/sorts/paginates on the backend to eliminate
        redundant GMS calls when changing filters or sort order.

        Args:
            start: Starting offset for pagination
            count: Number of conversations to fetch
            origin_type: Filter by origin type (DATAHUB_UI, SLACK, TEAMS, INGESTION_UI)
            sort_by: Sort field - "max_thinking_time", "num_turns", "created" (default: max_thinking_time)
            sort_desc: Sort descending (True) or ascending (False) - default: True

        Returns:
            Dictionary with 'conversations' list and 'total' count
        """
        try:
            current_time = time.time()

            # Fetch DataHub entity conversations
            datahub_conversations = []
            if self._list_cache is not None:
                cached_conversations, cached_time = self._list_cache
                if current_time - cached_time < self._list_cache_ttl:
                    age = current_time - cached_time
                    logger.info(f"Using cached DataHub conversation list ({len(cached_conversations)} conversations, age: {age:.1f}s)")
                    datahub_conversations = cached_conversations
                else:
                    logger.info("DataHub list cache expired, fetching fresh data")
                    datahub_conversations = self._fetch_all_conversations()
                    self._list_cache = (datahub_conversations, current_time)
            else:
                # Cache miss - fetch all
                datahub_conversations = self._fetch_all_conversations()
                self._list_cache = (datahub_conversations, current_time)

            # Fetch telemetry conversations (Slack/Teams)
            telemetry_conversations = []
            if self._telemetry_list_cache is not None:
                cached_telemetry, cached_time = self._telemetry_list_cache
                if current_time - cached_time < self._telemetry_cache_ttl:
                    age = current_time - cached_time
                    logger.info(f"Using cached telemetry conversation list ({len(cached_telemetry)} conversations, age: {age:.1f}s)")
                    telemetry_conversations = cached_telemetry
                else:
                    logger.info("Telemetry cache expired, fetching fresh data")
                    telemetry_conversations = self._fetch_telemetry_conversations()
                    self._telemetry_list_cache = (telemetry_conversations, current_time)
            else:
                # Cache miss - fetch all
                telemetry_conversations = self._fetch_telemetry_conversations()
                self._telemetry_list_cache = (telemetry_conversations, current_time)

            # Merge both sources
            all_conversations = datahub_conversations + telemetry_conversations
            logger.info(f"Merged conversations: {len(datahub_conversations)} DataHub + {len(telemetry_conversations)} telemetry = {len(all_conversations)} total")

            # Filter by origin_type client-side
            filtered_conversations = all_conversations
            if origin_type:
                filtered_conversations = [
                    c for c in all_conversations
                    if c.get("originType") == origin_type
                ]
                logger.info(f"Filtered to {len(filtered_conversations)} conversations with origin_type={origin_type}")

            # Sort client-side
            if sort_by == "max_thinking_time":
                filtered_conversations = sorted(filtered_conversations, key=lambda c: c.get("maxThinkingTimeMs", 0), reverse=sort_desc)
            elif sort_by == "num_turns":
                filtered_conversations = sorted(filtered_conversations, key=lambda c: c.get("numTurns", 0), reverse=sort_desc)
            elif sort_by == "created":
                filtered_conversations = sorted(filtered_conversations, key=lambda c: c.get("created", {}).get("time", 0), reverse=sort_desc)

            # Paginate client-side
            total = len(filtered_conversations)
            end = min(start + count, total)
            page_conversations = filtered_conversations[start:end]

            sort_direction = "desc" if sort_desc else "asc"
            logger.info(f"Returning page {start}-{end} of {total} conversations (sort: {sort_by} {sort_direction})")

            return {
                "conversations": page_conversations,
                "total": total,
            }

        except Exception as e:
            logger.error(f"Failed to list conversations: {e}")
            return {
                "conversations": [],
                "total": 0,
            }

    def get_archived_conversation(self, urn: str) -> Optional[Dict[str, Any]]:
        """
        Get full conversation details including messages.

        Handles both:
        - DataHub conversation entities (via entity get API)
        - Telemetry conversations (via usage events API)

        Args:
            urn: Conversation URN (real or telemetry synthetic)

        Returns:
            Conversation dictionary with messages, or None if not found
        """
        # Check cache first
        current_time = time.time()
        if urn in self._conversation_cache:
            cached_conversation, cached_time = self._conversation_cache[urn]
            if current_time - cached_time < self._cache_ttl:
                logger.info(f"Returning cached conversation: {urn} (age: {current_time - cached_time:.1f}s)")
                return cached_conversation
            else:
                logger.debug(f"Cache expired for {urn}, fetching fresh data")
                del self._conversation_cache[urn]

        # Check if this is a telemetry conversation
        if urn.startswith("telemetry:"):
            return self._get_telemetry_conversation(urn)

        try:
            logger.info(f"Fetching DataHub conversation via entity get API: {urn}")

            # Use entity get API to fetch conversation (not filtered by user)
            conversation_info = self.graph.get_aspect(
                entity_urn=urn, aspect_type=DataHubAiConversationInfoClass
            )

            if not conversation_info:
                logger.warning(f"Conversation not found: {urn}")
                return None

            # Convert to GraphQL-like format for backward compatibility
            result = {
                "urn": urn,
                "title": conversation_info.title,
                "originType": conversation_info.originType,
                "context": None,
                "messages": []
            }

            # Add context if available
            if conversation_info.context:
                result["context"] = {
                    "text": conversation_info.context.text if conversation_info.context.text else None,
                    "entityUrns": conversation_info.context.entityUrns if conversation_info.context.entityUrns else None,
                }

            # Convert messages
            if conversation_info.messages:
                for msg in conversation_info.messages:
                    message_dict = {
                        "type": msg.type if msg.type else "TEXT",
                        "time": msg.time if msg.time else 0,
                        "actor": {
                            "type": msg.actor.type if msg.actor and msg.actor.type else "USER",
                            "actor": msg.actor.actor if msg.actor and msg.actor.actor else None,
                        },
                        "content": {
                            "text": msg.content.text if msg.content and msg.content.text else "",
                        },
                        "agentName": msg.agentName if msg.agentName else None,
                    }
                    result["messages"].append(message_dict)

            logger.info(f"Successfully fetched conversation with {len(result['messages'])} messages")

            # Cache the result
            self._conversation_cache[urn] = (result, current_time)
            logger.debug(f"Cached conversation {urn} (cache size: {len(self._conversation_cache)})")

            return result

        except Exception as e:
            logger.error(f"Failed to get archived conversation {urn}: {e}")
            return None

    def _get_telemetry_conversation(self, urn: str) -> Optional[Dict[str, Any]]:
        """
        Get full conversation details from telemetry events.

        Args:
            urn: Telemetry URN (format: "telemetry:<chat_id>")

        Returns:
            Conversation dictionary with messages, or None if not found
        """
        try:
            # Extract chat_id from URN
            chat_id = urn.replace("telemetry:", "")
            logger.info(f"Fetching telemetry conversation: {chat_id}")

            # First check if we have it in the telemetry list cache
            if self._telemetry_list_cache is not None:
                cached_telemetry, cached_time = self._telemetry_list_cache
                logger.debug(f"Checking telemetry cache with {len(cached_telemetry)} conversations")
                for conv in cached_telemetry:
                    if conv.get("urn") == urn:
                        # Found in cache, reconstruct with full messages
                        logger.info(f"Found conversation {chat_id} in telemetry cache")

                        # Get events from cached data
                        cached_events = conv.get("_telemetry_events", [])
                        logger.debug(f"Retrieved {len(cached_events)} cached events from conversation")

                        # Parse messages from LAST event only
                        all_messages = []
                        if cached_events:
                            last_event = cached_events[-1]
                            event_source = last_event.get("_source", {})
                            full_history = event_source.get("full_history")
                            event_timestamp = event_source.get("timestamp", 0)
                            chatbot = event_source.get("chatbot", "unknown")

                            if full_history:
                                all_messages = self._parse_full_history_messages(
                                    full_history, event_timestamp, chatbot, cached_events
                                )

                        # Build interaction_events for frontend
                        interaction_events = []
                        for event in cached_events:
                            event_source = event.get("_source", {})
                            interaction_events.append({
                                "message_id": event_source.get("message_id", ""),
                                "timestamp": event_source.get("timestamp", 0),
                                "num_tool_calls": event_source.get("num_tool_calls", 0),
                                "response_generation_duration_sec": event_source.get("response_generation_duration_sec", 0),
                                "full_history": event_source.get("full_history", ""),
                            })

                        logger.debug(f"Built {len(interaction_events)} interaction_events from cached data")

                        # Build full conversation with messages
                        result = {
                            "urn": urn,
                            "title": conv.get("title"),
                            "originType": conv.get("originType"),
                            "context": None,
                            "messages": all_messages,
                            "telemetry": {
                                "num_tool_calls": sum(e["num_tool_calls"] for e in interaction_events),
                                "num_tool_call_errors": 0,
                                "tool_calls": [],
                                "interaction_events": interaction_events,
                                "avg_response_time": sum(e["response_generation_duration_sec"] for e in interaction_events) / len(interaction_events) if interaction_events else None,
                                "total_thinking_time": sum(e["response_generation_duration_sec"] for e in interaction_events) if interaction_events else None,
                            },
                        }

                        # Cache the result
                        current_time = time.time()
                        self._conversation_cache[urn] = (result, current_time)

                        logger.info(f"Returning cached conversation: {len(all_messages)} messages, {len(interaction_events)} events")
                        return result

            # Not in cache, query telemetry directly
            logger.info(f"Conversation {chat_id} not in cache, querying telemetry directly")

            query = {
                "query": {
                    "bool": {
                        "must": [
                            {"term": {"type": "ChatbotInteraction"}},
                            {"term": {"chat_id.keyword": chat_id}},
                        ]
                    }
                },
                "sort": [{"timestamp": "asc"}],
                "size": 100,
            }

            result = self.telemetry_client.query_usage_events(query, use_cache=False)
            hits = result.get("hits", {}).get("hits", [])
            logger.debug(f"Direct query returned {len(hits)} events")

            if not hits:
                logger.warning(f"No telemetry events found for chat_id: {chat_id}")
                return None

            # Parse messages from LAST event only (same as _fetch_telemetry_conversations)
            first_source = hits[0].get("_source", {})
            last_source = hits[-1].get("_source", {})
            chatbot = first_source.get("chatbot", "unknown")
            origin_type = "SLACK" if chatbot == "slack" else "TEAMS" if chatbot == "teams" else "UNKNOWN"

            # Parse messages from last event's full_history
            all_messages = []
            full_history = last_source.get("full_history")
            event_timestamp = last_source.get("timestamp", 0)
            if full_history:
                all_messages = self._parse_full_history_messages(
                    full_history, event_timestamp, chatbot, hits
                )

            # Extract title
            first_user_message = next(
                (m for m in all_messages if m.get("role") == "user"),
                None
            )
            title = "Untitled Conversation"
            if first_user_message:
                content = first_user_message.get("content", "")
                content = content.replace("<@U09B2P0M1T7>", "").strip()
                title = content[:100] + "..." if len(content) > 100 else content

            # Build interaction_events for frontend
            interaction_events = []
            for event in hits:
                event_source = event.get("_source", {})
                interaction_events.append({
                    "message_id": event_source.get("message_id", ""),
                    "timestamp": event_source.get("timestamp", 0),
                    "num_tool_calls": event_source.get("num_tool_calls", 0),
                    "response_generation_duration_sec": event_source.get("response_generation_duration_sec", 0),
                    "full_history": event_source.get("full_history", ""),
                })

            logger.debug(f"Built {len(interaction_events)} interaction_events from direct query")

            conversation = {
                "urn": urn,
                "title": title,
                "originType": origin_type,
                "context": None,
                "messages": all_messages,
                "telemetry": {
                    "num_tool_calls": sum(e["num_tool_calls"] for e in interaction_events),
                    "num_tool_call_errors": 0,
                    "tool_calls": [],
                    "interaction_events": interaction_events,
                    "avg_response_time": sum(e["response_generation_duration_sec"] for e in interaction_events) / len(interaction_events) if interaction_events else None,
                    "total_thinking_time": sum(e["response_generation_duration_sec"] for e in interaction_events) if interaction_events else None,
                },
            }

            # Cache the result
            current_time = time.time()
            self._conversation_cache[urn] = (conversation, current_time)

            logger.info(f"Successfully fetched telemetry conversation: {len(all_messages)} messages, {len(interaction_events)} events")
            return conversation

        except Exception as e:
            logger.error(f"Failed to get telemetry conversation {urn}: {e}")
            return None

    def convert_to_ui_format(
        self, graphql_conversation: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Convert DataHub conversation format to chat-ui-web format.

        Handles both DataHub entity conversations and telemetry conversations.

        Args:
            graphql_conversation: Conversation dict from GraphQL or telemetry

        Returns:
            Conversation dict in UI format
        """
        created_ms = graphql_conversation.get("created", {}).get("time", 0)
        last_updated_ms = graphql_conversation.get("lastUpdated", {}).get("time", 0)

        messages = []
        for msg in graphql_conversation.get("messages", []):
            # Check if this is already in simplified format (from telemetry)
            if "role" in msg and "content" in msg and "timestamp" in msg and "type" in msg:
                # Already in UI format (from telemetry parsing)
                messages.append({
                    "role": msg.get("role"),
                    "content": msg.get("content", ""),
                    "timestamp": msg.get("timestamp", 0),
                    "message_type": msg.get("type", "TEXT"),
                    "agent_name": None,
                    "user_name": None,
                    "actor_urn": None,
                })
            else:
                # DataHub entity format - convert to UI format
                actor_info = msg.get("actor", {})
                actor_type = actor_info.get("type", "USER")
                actor_urn = actor_info.get("actor")  # e.g., "urn:li:corpuser:admin"
                role = "user" if actor_type == "USER" else "assistant"

                # Extract username from URN (e.g., "urn:li:corpuser:admin" -> "admin")
                user_display_name = None
                if actor_urn and actor_type == "USER":
                    # URN format: urn:li:corpuser:username
                    parts = actor_urn.split(":")
                    if len(parts) >= 4 and parts[2] == "corpuser":
                        user_display_name = parts[3]

                messages.append(
                    {
                        "role": role,
                        "content": msg.get("content", {}).get("text", ""),
                        "timestamp": msg.get("time", 0),
                        "message_type": msg.get("type", "TEXT"),
                        "agent_name": msg.get("agentName"),
                        "user_name": user_display_name,
                        "actor_urn": actor_urn,
                    }
                )

        return {
            "id": graphql_conversation["urn"],
            "urn": graphql_conversation["urn"],
            "title": graphql_conversation.get("title") or "Untitled Conversation",
            "messages": messages,
            "created_at": created_ms / 1000.0,
            "updated_at": last_updated_ms / 1000.0,
            "origin_type": graphql_conversation.get("originType"),
            "slack_conversation_type": graphql_conversation.get("slackConversationType"),
            "message_count": graphql_conversation.get("messageCount", len(messages)),
            "context": graphql_conversation.get("context"),
            "is_archived": True,
            "max_thinking_time_ms": graphql_conversation.get("maxThinkingTimeMs", 0),
            "num_turns": graphql_conversation.get("numTurns", 0),
            "telemetry": graphql_conversation.get("telemetry"),
            "health_status": graphql_conversation.get("health_status"),
        }

    def invalidate_list_cache(self):
        """Invalidate both DataHub and telemetry conversation list caches to force a fresh fetch on next request."""
        if self._list_cache is not None:
            logger.info("Invalidating DataHub conversation list cache")
            self._list_cache = None
        if self._telemetry_list_cache is not None:
            logger.info("Invalidating telemetry conversation list cache")
            self._telemetry_list_cache = None

    def invalidate_conversation_cache(self, urn: Optional[str] = None):
        """
        Invalidate cached conversation(s).

        Args:
            urn: If provided, invalidate only this conversation. Otherwise, clear entire cache.
        """
        if urn:
            if urn in self._conversation_cache:
                logger.info(f"Invalidating cache for conversation {urn}")
                del self._conversation_cache[urn]
        else:
            logger.info("Invalidating all conversation caches")
            self._conversation_cache.clear()
