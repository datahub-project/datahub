"""
Telemetry Client - Query tool call telemetry via GMS OpenAPI.

This module provides a client for fetching tool call telemetry data from DataHub's
usage events index through the GMS OpenAPI endpoint at:
    POST /openapi/v2/analytics/datahub_usage_events/_search

All queries go through GMS (not direct Elasticsearch) which provides:
- Authentication via DataHub token
- Authorization checks (ANALYTICS + READ permission)
- Query validation and controlled access
- Abstraction from underlying search engine (ES/OpenSearch)
"""

import json
import time
from typing import Any, Dict, List, Optional

import requests
from loguru import logger

try:
    from datahub.ingestion.graph.client import DataHubGraph
except ImportError:
    DataHubGraph = None  # type: ignore


class TelemetryClient:
    """Client for querying tool call telemetry from DataHub via GMS API."""

    def __init__(self, graph: "DataHubGraph", cache_ttl: int = 300):
        """
        Initialize the telemetry client.

        Args:
            graph: DataHubGraph client with authentication
            cache_ttl: Cache time-to-live in seconds (default: 5 minutes)
        """
        self.graph = graph
        self.base_url = graph.config.server.rstrip("/")
        self.token = graph.config.token
        self._cache_ttl = cache_ttl

        # Cache for query results: {query_hash: (result, timestamp)}
        self._query_cache: Dict[str, tuple[Dict[str, Any], float]] = {}

    def _get_cache_key(self, query: Dict[str, Any]) -> str:
        """Generate cache key from query dict."""
        import hashlib
        import json

        query_str = json.dumps(query, sort_keys=True)
        return hashlib.md5(query_str.encode()).hexdigest()

    def _get_cached_result(self, query: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Get cached result if not expired."""
        cache_key = self._get_cache_key(query)
        if cache_key in self._query_cache:
            result, timestamp = self._query_cache[cache_key]
            age_seconds = time.time() - timestamp
            if age_seconds < self._cache_ttl:
                logger.info(
                    f"✓ Cache hit for telemetry query (age: {age_seconds:.1f}s, TTL: {self._cache_ttl}s)"
                )
                return result
            else:
                # Expired
                logger.debug(f"Cache expired for query: {cache_key[:8]}...")
                del self._query_cache[cache_key]
        return None

    def _cache_result(self, query: Dict[str, Any], result: Dict[str, Any]) -> None:
        """Cache query result."""
        cache_key = self._get_cache_key(query)
        self._query_cache[cache_key] = (result, time.time())
        logger.debug(f"Cached telemetry query result (TTL: {self._cache_ttl}s)")

    def _parse_tool_calls_from_full_history(
        self, full_history: str, message_timestamp: int
    ) -> List[Dict[str, Any]]:
        """
        Parse tool calls from full_history JSON field.

        This is a fallback when ChatbotToolCall events are not available.
        Extracts tool call information from the conversation history.

        Args:
            full_history: JSON string containing conversation message history
            message_timestamp: Timestamp of the message (used for relative timing)

        Returns:
            List of tool call dictionaries with available fields
        """
        try:
            history = json.loads(full_history)
            messages = history.get("messages", [])

            tool_calls = []

            for i, msg in enumerate(messages):
                msg_type = msg.get("type")

                # Tool call message
                if msg_type == "tool_call":
                    tool_call = {
                        "tool_name": msg.get("tool_name"),
                        "tool_input": msg.get("tool_input"),
                        "tool_use_id": msg.get("tool_use_id"),
                        "timestamp": message_timestamp,  # Use message timestamp
                        "execution_duration_sec": None,  # Not available in history
                        "result_length": None,
                        "is_error": False,
                        "error": None,
                    }
                    tool_calls.append(tool_call)

                # Tool result message (may contain error info)
                elif msg_type == "tool_result":
                    tool_request = msg.get("tool_request", {})
                    tool_use_id = tool_request.get("tool_use_id")

                    # Match with previous tool call
                    for tc in tool_calls:
                        if tc.get("tool_use_id") == tool_use_id:
                            result = msg.get("result")
                            tc["is_error"] = msg.get("is_error", False)
                            tc["error"] = msg.get("error")
                            if result:
                                tc["result_length"] = len(str(result))
                            break

            logger.debug(
                f"Parsed {len(tool_calls)} tool calls from full_history JSON"
            )
            return tool_calls

        except Exception as e:
            logger.warning(f"Failed to parse tool calls from full_history: {e}")
            return []

    def query_usage_events(
        self,
        query: Dict[str, Any],
        use_cache: bool = True,
    ) -> Dict[str, Any]:
        """
        Execute raw Elasticsearch query against datahub_usage_event index via GMS API.

        Args:
            query: Elasticsearch query DSL
            use_cache: Whether to use cached results (default: True)

        Returns:
            Elasticsearch response with hits

        Raises:
            requests.HTTPError: If the request fails
        """
        # Check cache first
        if use_cache:
            cached = self._get_cached_result(query)
            if cached is not None:
                return cached

        url = f"{self.base_url}/openapi/v2/analytics/datahub_usage_events/_search"
        headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
        }

        try:
            logger.debug(f"Querying GMS telemetry API: {url}")
            response = requests.post(url, json=query, headers=headers, timeout=30)
            response.raise_for_status()

            result = response.json()

            # Cache successful results
            if use_cache:
                self._cache_result(query, result)

            return result

        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to query usage events via GMS: {e}")
            raise

    def get_tool_calls_for_conversation(
        self,
        conversation_urn: str,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """
        Fetch all tool call records for a conversation.

        Tries two approaches:
        1. Query for ChatbotToolCall events (granular per-tool data)
        2. Fallback: Parse tool calls from ChatbotInteraction full_history JSON

        Args:
            conversation_urn: Conversation URN (e.g., urn:li:dataHubAiConversation:123)
            start_time: Optional start timestamp in epoch milliseconds
            end_time: Optional end timestamp in epoch milliseconds

        Returns:
            List of tool call events, sorted by timestamp ascending
        """
        # First, try to query for ChatbotToolCall events
        must_clauses: List[Dict[str, Any]] = [
            {"term": {"type": "ChatbotToolCall"}},
            {"term": {"ui_conversation_urn.keyword": conversation_urn}},
        ]

        if start_time is not None:
            must_clauses.append({"range": {"timestamp": {"gte": start_time}}})
        if end_time is not None:
            must_clauses.append({"range": {"timestamp": {"lte": end_time}}})

        query = {
            "query": {"bool": {"must": must_clauses}},
            "sort": [{"timestamp": "asc"}],
            "size": 1000,  # Support up to 1000 tool calls per conversation
        }

        try:
            result = self.query_usage_events(query)
            hits = result.get("hits", {}).get("hits", [])

            tool_calls = []
            for hit in hits:
                source = hit.get("_source", {})
                tool_calls.append({
                    "tool_name": source.get("tool_name"),
                    "tool_input": source.get("tool_input"),
                    "execution_duration_sec": source.get("tool_execution_duration_sec", 0.0),
                    "result_length": source.get("tool_result_length"),
                    "is_error": source.get("tool_result_is_error", False),
                    "error": source.get("tool_error"),
                    "timestamp": source.get("timestamp"),
                })

            if tool_calls:
                logger.info(
                    f"Found {len(tool_calls)} ChatbotToolCall events for conversation {conversation_urn}"
                )
                return tool_calls

            # Fallback: No ChatbotToolCall events found, parse from ChatbotInteraction
            logger.info(
                f"No ChatbotToolCall events found for {conversation_urn}, "
                "falling back to parsing full_history from ChatbotInteraction events"
            )

            interaction_events = self.get_interaction_events_for_conversation(
                conversation_urn
            )

            all_tool_calls = []
            for interaction in interaction_events:
                full_history = interaction.get("full_history")
                if full_history:
                    parsed_calls = self._parse_tool_calls_from_full_history(
                        full_history, interaction.get("timestamp", 0)
                    )
                    all_tool_calls.extend(parsed_calls)

            logger.info(
                f"Parsed {len(all_tool_calls)} tool calls from full_history for conversation {conversation_urn}"
            )
            return all_tool_calls

        except Exception as e:
            logger.error(
                f"Failed to fetch tool calls for conversation {conversation_urn}: {e}"
            )
            # Return empty list on error (fail gracefully)
            return []

    def get_interaction_events_for_conversation(
        self,
        conversation_urn: str,
    ) -> List[Dict[str, Any]]:
        """
        Fetch all ChatbotInteractionEvent records for a conversation.

        These events contain conversation-level metrics like num_tool_calls,
        num_tool_call_errors, full_history, and response_generation_duration_sec.

        Args:
            conversation_urn: Conversation URN

        Returns:
            List of interaction events, sorted by timestamp ascending
        """
        query = {
            "query": {
                "bool": {
                    "must": [
                        {"term": {"type": "ChatbotInteraction"}},
                        {"term": {"ui_conversation_urn.keyword": conversation_urn}},
                    ]
                }
            },
            "sort": [{"timestamp": "asc"}],
            "size": 100,  # Support up to 100 turns per conversation
        }

        try:
            result = self.query_usage_events(query)
            hits = result.get("hits", {}).get("hits", [])

            interactions = []
            for hit in hits:
                source = hit.get("_source", {})
                interactions.append({
                    "message_id": source.get("message_id"),
                    "chat_id": source.get("chat_id"),
                    "num_tool_calls": source.get("num_tool_calls", 0),
                    "num_tool_call_errors": source.get("num_tool_call_errors", 0),
                    "response_generation_duration_sec": source.get(
                        "response_generation_duration_sec", 0.0
                    ),
                    "full_history": source.get("full_history"),
                    "timestamp": source.get("timestamp"),
                })

            logger.info(
                f"Found {len(interactions)} interaction events for conversation {conversation_urn}"
            )
            return interactions

        except Exception as e:
            logger.error(
                f"Failed to fetch interaction events for conversation {conversation_urn}: {e}"
            )
            return []

    def aggregate_tool_usage(
        self,
        conversation_urns: Optional[List[str]] = None,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        Aggregate tool call statistics across conversations.

        Tries two approaches:
        1. Aggregate ChatbotToolCall events (if available)
        2. Fallback: Parse and aggregate from ChatbotInteraction full_history

        Args:
            conversation_urns: Optional list of conversation URNs to filter
            start_time: Optional start timestamp in epoch milliseconds
            end_time: Optional end timestamp in epoch milliseconds

        Returns:
            Dictionary with:
                - tool_counts: Dict[tool_name, count]
                - tool_errors: Dict[tool_name, error_count]
                - avg_duration: Dict[tool_name, avg_seconds] (None if from fallback)
        """
        # Build query for ChatbotToolCall events
        must_clauses: List[Dict[str, Any]] = [
            {"term": {"type": "ChatbotToolCall"}},
        ]

        if conversation_urns:
            must_clauses.append(
                {"terms": {"ui_conversation_urn.keyword": conversation_urns}}
            )
        if start_time is not None:
            must_clauses.append({"range": {"timestamp": {"gte": start_time}}})
        if end_time is not None:
            must_clauses.append({"range": {"timestamp": {"lte": end_time}}})

        query = {
            "query": {"bool": {"must": must_clauses}},
            "aggs": {
                "by_tool": {
                    "terms": {"field": "tool_name.keyword", "size": 100},
                    "aggs": {
                        "avg_duration": {
                            "avg": {"field": "tool_execution_duration_sec"}
                        },
                        "error_count": {
                            "filter": {"term": {"tool_result_is_error": True}}
                        },
                    },
                }
            },
            "size": 0,  # Only return aggregations, not individual hits
        }

        try:
            result = self.query_usage_events(query, use_cache=True)
            aggregations = result.get("aggregations", {})
            by_tool = aggregations.get("by_tool", {})
            buckets = by_tool.get("buckets", [])

            tool_counts: Dict[str, int] = {}
            tool_errors: Dict[str, int] = {}
            avg_duration: Dict[str, float] = {}

            for bucket in buckets:
                tool_name = bucket.get("key")
                count = bucket.get("doc_count", 0)
                avg_dur = bucket.get("avg_duration", {}).get("value")
                error_count = bucket.get("error_count", {}).get("doc_count", 0)

                tool_counts[tool_name] = count
                if error_count > 0:
                    tool_errors[tool_name] = error_count
                if avg_dur is not None:
                    avg_duration[tool_name] = round(avg_dur, 3)

            if tool_counts:
                logger.info(
                    f"Aggregated tool usage from ChatbotToolCall events: {len(tool_counts)} unique tools, "
                    f"{sum(tool_counts.values())} total calls"
                )
                return {
                    "tool_counts": tool_counts,
                    "tool_errors": tool_errors,
                    "avg_duration": avg_duration,
                }

            # Fallback: Parse from ChatbotInteraction events
            logger.info(
                "No ChatbotToolCall events found, falling back to parsing full_history"
            )

            # Query for ChatbotInteraction events with filters
            interaction_must_clauses: List[Dict[str, Any]] = [
                {"term": {"type": "ChatbotInteraction"}},
            ]

            if conversation_urns:
                interaction_must_clauses.append(
                    {"terms": {"ui_conversation_urn.keyword": conversation_urns}}
                )
            if start_time is not None:
                interaction_must_clauses.append({"range": {"timestamp": {"gte": start_time}}})
            if end_time is not None:
                interaction_must_clauses.append({"range": {"timestamp": {"lte": end_time}}})

            interaction_query = {
                "query": {"bool": {"must": interaction_must_clauses}},
                "sort": [{"timestamp": "asc"}],
                "size": 1000,
            }

            interaction_result = self.query_usage_events(interaction_query, use_cache=True)
            interaction_hits = interaction_result.get("hits", {}).get("hits", [])

            # Parse tool calls from full_history and aggregate
            tool_counts = {}
            tool_errors = {}

            for hit in interaction_hits:
                source = hit.get("_source", {})
                full_history = source.get("full_history")
                if full_history:
                    parsed_calls = self._parse_tool_calls_from_full_history(
                        full_history, source.get("timestamp", 0)
                    )
                    for tc in parsed_calls:
                        tool_name = tc.get("tool_name", "unknown")
                        tool_counts[tool_name] = tool_counts.get(tool_name, 0) + 1
                        if tc.get("is_error"):
                            tool_errors[tool_name] = tool_errors.get(tool_name, 0) + 1

            logger.info(
                f"Aggregated tool usage from full_history: {len(tool_counts)} unique tools, "
                f"{sum(tool_counts.values())} total calls"
            )

            return {
                "tool_counts": tool_counts,
                "tool_errors": tool_errors,
                "avg_duration": {},  # Not available from full_history
            }

        except Exception as e:
            logger.error(f"Failed to aggregate tool usage: {e}")
            return {
                "tool_counts": {},
                "tool_errors": {},
                "avg_duration": {},
            }
