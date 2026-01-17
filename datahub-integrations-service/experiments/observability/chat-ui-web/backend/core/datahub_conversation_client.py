"""
DataHub Conversation Client - Integration layer for archived conversations.

This module provides a wrapper around DataHubAiConversationClient from
datahub-integrations-service to fetch and convert archived conversations
from DataHub's GraphQL API for display in the chat-ui-web frontend.
"""

from typing import Any, Dict, List, Optional
import time

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

        # Cached schema capabilities (discovered on first use)
        self._schema_capabilities: Optional[Dict[str, bool]] = None
        self._list_query: Optional[str] = None

        # Conversation cache: {urn: (conversation_dict, timestamp)}
        self._conversation_cache: Dict[str, tuple[Dict[str, Any], float]] = {}
        self._cache_ttl = cache_ttl

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

    def list_archived_conversations(
        self,
        start: int = 0,
        count: int = 20,
        origin_type: Optional[str] = None,
        sort_by: str = "max_thinking_time",
        sort_desc: bool = True,
    ) -> Dict[str, Any]:
        """
        List ALL archived conversations from DataHub using entity search API with pagination.

        This uses entity search instead of GraphQL to get conversations from ALL users,
        not just the current authenticated user.

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
            logger.info(f"Searching for conversations with REST API (start={start}, count={count}, origin_type={origin_type})")

            # Use REST API to list all entity URNs with pagination
            # DataHubAiConversation is not in the GraphQL EntityType enum, so we use REST API directly
            urn_list = self.graph.list_all_entity_urns(
                entity_type="dataHubAiConversation",
                start=start,
                count=count
            )

            if urn_list is None:
                logger.warning("list_all_entity_urns returned None")
                return {"conversations": [], "total": 0}

            entities = urn_list

            # Get total count by trying to fetch a large offset (hacky but works)
            # TODO: Find a better way to get total count
            all_urns_sample = self.graph.list_all_entity_urns(
                entity_type="dataHubAiConversation",
                start=0,
                count=10000  # Large number to get approximate total
            )
            total = len(all_urns_sample) if all_urns_sample else 0

            logger.info(f"Found ~{total} total conversations, fetched {len(entities)} for this page")

            conversations = []
            for urn in entities:

                # Fetch full conversation info using entity get API
                conversation_info = self.graph.get_aspect(
                    entity_urn=urn, aspect_type=DataHubAiConversationInfoClass
                )

                if conversation_info:
                    # Filter by origin_type if specified
                    if origin_type and conversation_info.originType != origin_type:
                        continue

                    # Convert to GraphQL-like format for backward compatibility
                    created_time = conversation_info.created.time if conversation_info.created else 0
                    created_actor = conversation_info.created.actor if conversation_info.created else None
                    message_count = len(conversation_info.messages) if conversation_info.messages else 0

                    # Calculate thinking time metrics per turn
                    # A turn = one user message (and its assistant response)
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

                            # Detect turn boundaries - count USER messages as turns
                            if actor_type == "USER":
                                # New turn starting - finalize previous turn
                                if in_turn:
                                    max_thinking_time_ms = max(max_thinking_time_ms, current_turn_thinking)
                                # Increment turn count for each user message
                                num_turns += 1
                                current_turn_thinking = 0
                                in_turn = True
                            elif actor_type == "AGENT" and in_turn:
                                # Accumulate thinking time in this turn
                                if prev_time is not None and msg_type == "THINKING":
                                    duration = msg_time - prev_time
                                    current_turn_thinking += duration

                            prev_time = msg_time

                        # Finalize last turn
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
                            "time": created_time,  # Use created time as fallback
                            "actor": created_actor,
                        },
                        "messageCount": message_count,
                        "originType": conversation_info.originType if conversation_info.originType else None,
                        "context": None,
                        "maxThinkingTimeMs": max_thinking_time_ms,
                        "numTurns": num_turns,
                    }

                    # Add context if available
                    if conversation_info.context:
                        conversation_dict["context"] = {
                            "text": conversation_info.context.text if conversation_info.context.text else None,
                            "entityUrns": conversation_info.context.entityUrns if conversation_info.context.entityUrns else None,
                        }

                    conversations.append(conversation_dict)

            # Sort conversations based on sort_by parameter
            if sort_by == "max_thinking_time":
                conversations.sort(key=lambda c: c.get("maxThinkingTimeMs", 0), reverse=sort_desc)
            elif sort_by == "num_turns":
                conversations.sort(key=lambda c: c.get("numTurns", 0), reverse=sort_desc)
            elif sort_by == "created":
                conversations.sort(key=lambda c: c.get("created", {}).get("time", 0), reverse=sort_desc)

            sort_direction = "desc" if sort_desc else "asc"
            logger.info(f"Returning {len(conversations)} conversations (filtered by origin_type: {origin_type}, sorted by: {sort_by} {sort_direction})")

            return {
                "conversations": conversations,
                "total": total,
            }

        except Exception as e:
            logger.error(f"Failed to list conversations via entity search: {e}")
            # Fallback to empty result
            return {
                "conversations": [],
                "total": 0,
            }

    def get_archived_conversation(self, urn: str) -> Optional[Dict[str, Any]]:
        """
        Get full conversation details including messages using entity get API with caching.

        Args:
            urn: Conversation URN

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

        try:
            logger.info(f"Fetching conversation via entity get API: {urn}")

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

    def convert_to_ui_format(
        self, graphql_conversation: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Convert DataHub conversation format to chat-ui-web format.

        Args:
            graphql_conversation: Conversation dict from GraphQL

        Returns:
            Conversation dict in UI format
        """
        created_ms = graphql_conversation.get("created", {}).get("time", 0)
        last_updated_ms = graphql_conversation.get("lastUpdated", {}).get("time", 0)

        messages = []
        for msg in graphql_conversation.get("messages", []):
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
            "message_count": graphql_conversation.get("messageCount", len(messages)),
            "context": graphql_conversation.get("context"),
            "is_archived": True,
            "max_thinking_time_ms": graphql_conversation.get("maxThinkingTimeMs", 0),
            "num_turns": graphql_conversation.get("numTurns", 0),
        }
