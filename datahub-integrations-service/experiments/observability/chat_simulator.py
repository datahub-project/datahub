#!/usr/bin/env python3
"""
Chat Simulator - Dual-purpose module for realistic DataHub chat traffic.

This module can be:
1. IMPORTED as a library (by chat_ui.py or other tools)
2. EXECUTED as a CLI tool with full traffic generation features

Features:
- DataHub backend sketching (discovers entities, platforms, assets)
- AWS Bedrock question generation (contextual, realistic questions)
- Multi-turn conversation management (with follow-ups)
- DuckDB conversation logging (CLI mode only)
- Prometheus metrics (CLI mode only)
- Chat API client for DataHub integrations service

Library Usage:
    from chat_simulator import DataHubSketcher, ChatClient, ConversationGenerator

    sketcher = DataHubSketcher(gms_url, token)
    sketch = sketcher.create_sketch()

    client = ChatClient(service_url, token)
    result = client.send_message(message, conversation_urn, user_urn)

CLI Usage:
    python chat_simulator.py --conversations 5 --delay 3
    python chat_simulator.py --continuous --max-length 10
"""

import argparse
import json
import os
import random
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import boto3
import duckdb
import httpx
import yaml
from loguru import logger
from prometheus_client import Counter, Histogram

# =============================================================================
# CONFIGURATION - IMPORTABLE
# =============================================================================
# Configuration loading and constants that can be imported by other modules


# Load configuration from ~/.datahubenv if it exists
def load_datahub_config():
    """Load DataHub config from ~/.datahubenv file."""
    datahubenv_path = Path.home() / ".datahubenv"
    if datahubenv_path.exists():
        try:
            with open(datahubenv_path, "r") as f:
                config = yaml.safe_load(f)
                if config and "gms" in config:
                    gms_config = config["gms"]
                    return {
                        "server": gms_config.get("server"),
                        "token": gms_config.get("token"),
                    }
        except Exception as e:
            logger.warning(f"Failed to load ~/.datahubenv: {e}")
    return {}


# Load config from ~/.datahubenv as defaults
_datahub_config = load_datahub_config()

# Configuration (prioritize env vars, then ~/.datahubenv, then hardcoded defaults)
SERVICE_URL = os.environ.get("INTEGRATIONS_SERVICE_URL", "http://localhost:9003")
GMS_URL = os.environ.get(
    "DATAHUB_GMS_URL", _datahub_config.get("server", "http://localhost:8080")
)
GMS_TOKEN = os.environ.get(
    "DATAHUB_GMS_API_TOKEN", _datahub_config.get("token", "datahubtoken")
)
AWS_REGION = os.environ.get("AWS_REGION", "us-west-2")
AWS_PROFILE = os.environ.get("AWS_PROFILE")  # Optional, uses default if not set


# =============================================================================
# PROMETHEUS METRICS - CLI ONLY
# =============================================================================
# Metrics for monitoring traffic generation (only used when run as CLI tool)

# Prometheus Metrics - Constants
METRIC_CHAT_REQUEST_DURATION = "traffic_gen_chat_request_duration_seconds"
METRIC_LLM_CALL_DURATION = "traffic_gen_llm_call_duration_seconds"
METRIC_TOOL_CALL_DURATION = "traffic_gen_tool_call_duration_seconds"
METRIC_CHAT_MESSAGES_TOTAL = "traffic_gen_chat_messages_total"

LABEL_STATUS = "status"
LABEL_MODEL = "model"
LABEL_TOOL_NAME = "tool_name"

STATUS_SUCCESS = "success"
STATUS_FAILURE = "failure"

# Standard latency buckets: 100ms to 3min
LATENCY_BUCKETS = (
    0.1,
    0.25,
    0.5,
    1.0,
    2.5,
    5.0,
    10.0,
    15.0,
    30.0,
    60.0,
    90.0,
    120.0,
    180.0,
)

# Lazy initialization of metrics to avoid duplicate registration errors
# when module is imported multiple times (e.g., in Streamlit)
_metrics_initialized = False
chat_request_latency = None
llm_call_latency = None
tool_call_latency = None
chat_messages_total = None


def _init_metrics():
    """Initialize Prometheus metrics (idempotent)."""
    global \
        _metrics_initialized, \
        chat_request_latency, \
        llm_call_latency, \
        tool_call_latency, \
        chat_messages_total

    if _metrics_initialized:
        return

    try:
        # Overall request-response latency (user question -> full answer)
        chat_request_latency = Histogram(
            METRIC_CHAT_REQUEST_DURATION,
            "Latency of chat requests from question to full response",
            buckets=LATENCY_BUCKETS,
        )

        # LLM call latency (per LLM invocation)
        llm_call_latency = Histogram(
            METRIC_LLM_CALL_DURATION,
            "Latency of individual LLM calls",
            [LABEL_MODEL],
            buckets=LATENCY_BUCKETS,
        )

        # Tool call latency (per tool execution)
        tool_call_latency = Histogram(
            METRIC_TOOL_CALL_DURATION,
            "Latency of individual tool calls",
            [LABEL_TOOL_NAME],
            buckets=LATENCY_BUCKETS,
        )

        # Message counts
        chat_messages_total = Counter(
            METRIC_CHAT_MESSAGES_TOTAL,
            "Total number of chat messages sent",
            [LABEL_STATUS],
        )

        _metrics_initialized = True
    except ValueError as e:
        # Metrics already registered - this is fine in Streamlit reloads
        logger.debug(f"Metrics already registered (expected in Streamlit): {e}")
        _metrics_initialized = True


# =============================================================================
# CONSTANTS - IMPORTABLE
# =============================================================================

# Entity types to sketch
ENTITY_TYPES = [
    "dataset",
    "dashboard",
    "chart",
    "dataJob",
    "dataFlow",
    "mlModel",
    "mlFeature",
    "glossaryTerm",
    "tag",
    "container",
]


# =============================================================================
# DATA CLASSES - IMPORTABLE
# =============================================================================
# Data structures that can be imported by other modules


@dataclass
class DataHubSketch:
    """Sketch of DataHub backend for context."""

    entity_counts: dict[str, int]
    platforms: list[str]
    top_datasets: list[dict[str, Any]]
    top_dashboards: list[dict[str, Any]]
    sample_tags: list[str]
    sample_glossary_terms: list[str]
    sample_domains: list[str] | None = None

    def to_context_string(self) -> str:
        """Convert sketch to context string for Anthropic."""
        context = "DataHub Instance Overview:\n\n"

        # Entity counts
        context += "Entity Counts:\n"
        for entity_type, count in sorted(
            self.entity_counts.items(), key=lambda x: x[1], reverse=True
        ):
            context += f"  - {entity_type}: {count:,}\n"

        # Platforms
        if self.platforms:
            context += f"\nData Platforms: {', '.join(self.platforms)}\n"

        # Top datasets
        if self.top_datasets:
            context += f"\nTop Datasets ({len(self.top_datasets)}):\n"
            for ds in self.top_datasets[:5]:
                name = ds.get("name", "unknown")
                platform = ds.get("platform", "unknown")
                context += f"  - {platform}.{name}\n"

        # Top dashboards
        if self.top_dashboards:
            context += f"\nTop Dashboards ({len(self.top_dashboards)}):\n"
            for dash in self.top_dashboards[:5]:
                title = dash.get("title", "unknown")
                context += f"  - {title}\n"

        # Tags
        if self.sample_tags:
            context += f"\nSample Tags: {', '.join(self.sample_tags[:10])}\n"

        # Glossary terms
        if self.sample_glossary_terms:
            context += (
                f"\nGlossary Terms: {', '.join(self.sample_glossary_terms[:10])}\n"
            )

        # Domains
        if self.sample_domains:
            context += f"\nDomains: {', '.join(self.sample_domains[:10])}\n"

        return context


@dataclass
class ConversationState:
    """State of an ongoing conversation."""

    conversation_urn: str
    user_urn: str
    messages: list[dict[str, Any]]  # Full message objects with metadata
    started_at: float
    message_count: int

    def add_message(
        self,
        role: str,
        content: str,
        success: bool = True,
        duration: float = 0.0,
        error: str | None = None,
        event_count: int = 0,
    ) -> None:
        """Add a message to the conversation history with metadata."""
        self.messages.append(
            {
                "role": role,
                "content": content,
                "timestamp": datetime.now(),
                "success": success,
                "duration": duration,
                "error": error,
                "event_count": event_count,
            }
        )
        if role == "user":
            self.message_count += 1


# =============================================================================
# SERVICE CLASSES - IMPORTABLE
# =============================================================================
# Core service classes for DataHub interaction that can be imported


class DataHubSketcher:
    """Sketch DataHub backend to understand available data."""

    def __init__(self, gms_url: str, token: str):
        self.gms_url = gms_url.rstrip("/")
        self.token = token
        self.client = httpx.Client(timeout=30.0)

    def search_entities(
        self, entity_type: str, query: str = "*", count: int = 20
    ) -> dict:
        """Search for entities using DataHub search API."""
        url = f"{self.gms_url}/entities?action=search"
        payload = {
            "input": query,
            "entity": entity_type,
            "start": 0,
            "count": count,
        }
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.token}",
        }

        try:
            response = self.client.post(url, json=payload, headers=headers)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.warning(f"Failed to search {entity_type}: {e}")
            return {"value": {"numEntities": 0, "entities": []}}

    def get_entity_counts(self) -> dict[str, int]:
        """Get count of entities per type."""
        counts = {}
        for entity_type in ENTITY_TYPES:
            result = self.search_entities(entity_type, query="*", count=1)
            counts[entity_type] = result.get("value", {}).get("numEntities", 0)
        return counts

    def get_platforms(self) -> list[str]:
        """Get list of data platforms using GraphQL faceted aggregation."""
        # Use the same approach as the frontend - aggregateAcrossEntities with platform facets
        url = f"{self.gms_url}/api/graphql"
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.token}",
        }

        # GraphQL query for platform aggregations
        query = """
        query aggregateAcrossEntities($input: AggregateAcrossEntitiesInput!) {
            aggregateAcrossEntities(input: $input) {
                facets {
                    field
                    aggregations {
                        value
                        count
                    }
                }
            }
        }
        """

        variables = {
            "input": {
                "query": "*",
                "facets": ["platform"],
                "searchFlags": {"maxAggValues": 100},
            }
        }

        try:
            response = self.client.post(
                url, json={"query": query, "variables": variables}, headers=headers
            )
            response.raise_for_status()
            data = response.json()

            platforms = set()
            facets = (
                data.get("data", {})
                .get("aggregateAcrossEntities", {})
                .get("facets", [])
            )

            for facet in facets:
                if facet.get("field") == "platform":
                    for agg in facet.get("aggregations", []):
                        platform_urn = agg.get("value")
                        count = agg.get("count", 0)

                        # Extract platform name from URN: urn:li:dataPlatform:snowflake -> snowflake
                        if platform_urn and count > 0:
                            if platform_urn.startswith("urn:li:dataPlatform:"):
                                platform_name = platform_urn.split(
                                    "urn:li:dataPlatform:"
                                )[-1]
                                # Skip "None" platform
                                if platform_name != "None":
                                    platforms.add(platform_name)

            return sorted(list(platforms))

        except Exception as e:
            logger.warning(
                f"Failed to get platforms via GraphQL, falling back to search: {e}"
            )
            # Fallback to old method if GraphQL fails
            return self._get_platforms_fallback()

    def _get_platforms_fallback(self) -> list[str]:
        """Fallback method to get platforms by sampling."""
        result = self.search_entities("dataset", query="*", count=1000)
        platforms = set()

        for entity in result.get("value", {}).get("entities", []):
            urn = entity.get("entity")
            if isinstance(urn, str) and "dataPlatform:" in urn:
                try:
                    platform_part = urn.split("(")[1].split(",")[0]
                    platform = platform_part.split("dataPlatform:")[-1]
                    if platform != "None":
                        platforms.add(platform)
                except Exception:
                    pass

        return sorted(list(platforms))

    def _get_facet_aggregations(
        self,
        facets: list[str],
        max_values: int = 1000,
        entity_types: list[str] | None = None,
    ) -> list[dict]:
        """
        Get faceted aggregations using GraphQL.

        Args:
            facets: List of facet fields to aggregate (e.g., ["tags", "glossaryTerms", "domains"])
            max_values: Maximum number of values per facet
            entity_types: Optional list of entity types to filter by

        Returns:
            List of facet dictionaries with aggregations
        """
        url = f"{self.gms_url}/api/graphql"
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.token}",
        }

        query = """
        query aggregateAcrossEntities($input: AggregateAcrossEntitiesInput!) {
            aggregateAcrossEntities(input: $input) {
                facets {
                    field
                    aggregations {
                        value
                        count
                        entity {
                            urn
                            type
                            ... on Tag {
                                name
                                properties {
                                    name
                                }
                            }
                            ... on GlossaryTerm {
                                name
                                properties {
                                    name
                                }
                            }
                            ... on Domain {
                                properties {
                                    name
                                }
                            }
                        }
                    }
                }
            }
        }
        """

        variables: dict = {
            "input": {
                "query": "*",
                "facets": facets,
                "searchFlags": {"maxAggValues": max_values},
            }
        }

        if entity_types:
            variables["input"]["types"] = entity_types

        try:
            response = self.client.post(
                url, json={"query": query, "variables": variables}, headers=headers
            )
            response.raise_for_status()
            data = response.json()

            return (
                data.get("data", {})
                .get("aggregateAcrossEntities", {})
                .get("facets", [])
            )

        except Exception as e:
            logger.warning(f"Failed to get facet aggregations: {e}")
            return []

    def get_top_datasets(self, count: int = 100) -> list[dict]:
        """Get top datasets with metadata."""
        # Increased default to 100 to get better variety across platforms
        result = self.search_entities("dataset", query="*", count=count)
        datasets = []

        for entity in result.get("value", {}).get("entities", []):
            urn = entity.get("entity", {})
            if isinstance(urn, str):
                # Parse dataset URN: urn:li:dataset:(urn:li:dataPlatform:platform,name,env)
                try:
                    parts = urn.split(",")
                    if len(parts) >= 2:
                        platform_part = parts[0].split(":")[-1]
                        name = parts[1]
                        # Skip "None" platform datasets for better context
                        if platform_part != "None":
                            datasets.append(
                                {"urn": urn, "platform": platform_part, "name": name}
                            )
                except Exception:
                    pass

        return datasets

    def get_top_dashboards(self, count: int = 20) -> list[dict]:
        """Get top dashboards with metadata."""
        result = self.search_entities("dashboard", query="*", count=count)
        dashboards = []

        for entity in result.get("value", {}).get("entities", []):
            # Extract dashboard title from entity
            urn = entity.get("entity")
            if isinstance(urn, str):
                # Try to extract meaningful name from URN
                try:
                    title = urn.split(",")[-1].replace(")", "")
                    dashboards.append({"urn": urn, "title": title})
                except Exception:
                    pass

        return dashboards

    def get_sample_tags(self, count: int = 100) -> list[str]:
        """Get sample tags using search API (facets limited to 20)."""
        # Note: Facet aggregations have a backend hard limit of 20
        # Use search API instead to get all tags
        result = self.search_entities("tag", query="*", count=count)
        tags = []

        for entity in result.get("value", {}).get("entities", []):
            urn = entity.get("entity")
            if isinstance(urn, str) and urn.startswith("urn:li:tag:"):
                tag_name = urn.split("urn:li:tag:")[-1]
                tags.append(tag_name)

        return tags

    def get_sample_glossary_terms(self, count: int = 100) -> list[str]:
        """Get sample glossary terms using search API (facets limited to 20)."""
        # Note: Facet aggregations have a backend hard limit of 20
        # Use search API instead to get all glossary terms
        result = self.search_entities("glossaryTerm", query="*", count=count)
        terms = []

        for entity in result.get("value", {}).get("entities", []):
            urn = entity.get("entity")
            if isinstance(urn, str) and urn.startswith("urn:li:glossaryTerm:"):
                # Extract term name from URN
                term_name = urn.split("urn:li:glossaryTerm:")[-1]
                terms.append(term_name)

        return terms

    def get_sample_domains(self, count: int = 100) -> list[str]:
        """Get sample domains using GraphQL listDomains query."""
        url = f"{self.gms_url}/api/graphql"
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.token}",
        }

        # GraphQL query to list domains (same as frontend)
        query = """
        query listDomains($input: ListDomainsInput!) {
            listDomains(input: $input) {
                start
                count
                total
                domains {
                    urn
                    type
                    properties {
                        name
                        description
                    }
                }
            }
        }
        """

        variables = {
            "input": {
                "start": 0,
                "count": count,
            }
        }

        try:
            response = self.client.post(
                url, json={"query": query, "variables": variables}, headers=headers
            )
            response.raise_for_status()
            data = response.json()

            domains = []
            domain_list = data.get("data", {}).get("listDomains", {}).get("domains", [])

            for domain in domain_list:
                domain_name = domain.get("properties", {}).get("name")
                if domain_name:
                    domains.append(domain_name)

            return domains

        except Exception as e:
            logger.warning(f"Failed to get domains via GraphQL: {e}")
            return []

    def create_sketch(self) -> DataHubSketch:
        """Create complete sketch of DataHub instance."""
        logger.info("Creating DataHub sketch...")

        sketch = DataHubSketch(
            entity_counts=self.get_entity_counts(),
            platforms=self.get_platforms(),
            top_datasets=self.get_top_datasets(),
            top_dashboards=self.get_top_dashboards(),
            sample_tags=self.get_sample_tags(),
            sample_glossary_terms=self.get_sample_glossary_terms(),
            sample_domains=self.get_sample_domains(),
        )

        logger.info(
            f"Sketch complete: {len(sketch.platforms)} platforms, "
            f"{sum(sketch.entity_counts.values())} total entities"
        )

        return sketch


class ConversationGenerator:
    """Generate realistic conversations using AWS Bedrock."""

    def __init__(self, bedrock_client: Any, sketch: DataHubSketch):
        self.client = bedrock_client
        self.sketch = sketch
        self.context_string = sketch.to_context_string()
        # Use Claude 3.5 Haiku via Bedrock
        self.model_id = "us.anthropic.claude-3-5-haiku-20241022-v1:0"

    def _invoke_bedrock(self, prompt: str, max_tokens: int = 200) -> str:
        """Invoke Bedrock with Claude model."""
        try:
            request_body = {
                "anthropic_version": "bedrock-2023-05-31",
                "max_tokens": max_tokens,
                "messages": [
                    {
                        "role": "user",
                        "content": prompt,
                    }
                ],
            }

            response = self.client.invoke_model(
                modelId=self.model_id,
                body=json.dumps(request_body),
            )

            response_body = json.loads(response["body"].read())
            return response_body["content"][0]["text"].strip()

        except Exception as e:
            error_msg = str(e)

            # Check if it's an SSO token expiry error
            if "Token has expired" in error_msg or "TokenRetrievalError" in error_msg:
                logger.error(f"AWS SSO token expired: {e}")
                raise RuntimeError(
                    "AWS SSO token has expired. Please run: aws sso login --profile <your-profile>"
                ) from e

            logger.error(f"Bedrock invocation failed: {e}")
            raise

    def generate_initial_question(self) -> str:
        """Generate an initial question to start a conversation."""
        prompt = f"""You are a data analyst exploring a DataHub instance. Based on the following information about the DataHub instance, generate a realistic initial question you might ask.

{self.context_string}

Generate ONE realistic question that a data analyst might ask when exploring this data catalog. The question should:
- Be specific and actionable
- Reference actual entities/platforms that exist
- Be something the DataHub AI assistant could help with
- Be conversational and natural

Examples of good questions:
- "Show me the most popular datasets from Snowflake"
- "What dashboards track user engagement metrics?"
- "Find datasets tagged as PII"
- "What's the lineage for the revenue_daily table?"

Return ONLY the question text, nothing else."""

        # No fallback - fail hard so we can see errors
        question = self._invoke_bedrock(prompt, max_tokens=200)
        return question

    def generate_followup_question(self, conversation_history: list[dict]) -> str:
        """Generate a follow-up question based on conversation history."""
        # Filter to only user and assistant messages (skip tool calls, reasoning, etc.)
        relevant_messages = [
            msg
            for msg in conversation_history
            if msg.get("role") in ["user", "assistant"]
        ]

        # Use more context - last 10 messages or all if less (5 Q&A pairs)
        recent_messages = relevant_messages[-10:]

        # Build conversation context with cleaner formatting
        # Limit message length to avoid overwhelming the prompt
        conversation_text = "\n".join(
            [
                f"{'USER' if msg['role'] == 'user' else 'ASSISTANT'}: {msg['content'][:500]}"
                for msg in recent_messages
            ]
        )

        # Count Q&A pairs for context
        question_count = len([m for m in relevant_messages if m.get("role") == "user"])

        prompt = f"""You are a data analyst having a conversation with a DataHub AI assistant. Based on the conversation history below, generate a natural follow-up question.

DataHub Instance Context:
{self.context_string}

Conversation History ({question_count} questions asked so far):
{conversation_text}

Generate ONE realistic follow-up question that:
- Builds naturally on the previous conversation
- Explores a related aspect or digs deeper into what was discussed
- References specific entities, platforms, or concepts mentioned earlier
- Is specific and actionable (not vague like "tell me more")
- Sounds conversational and natural

Good follow-up patterns:
- After getting a list: "Can you show me the lineage for [specific item from the list]?"
- After seeing metrics: "How does this compare to [related metric/entity]?"
- After exploring one domain: "What about datasets in the [different domain] domain?"
- After one platform: "Are there similar tables in [different platform]?"

Return ONLY the question text, nothing else."""

        # No fallback - fail hard so we can see errors
        question = self._invoke_bedrock(prompt, max_tokens=200)
        return question


class ChatClient:
    """Client for sending messages to DataHub chat API.

    Returns ChatHistory object with rich message types including:
    - HumanMessage: User messages
    - AssistantMessage: Bot responses
    - ReasoningMessage: Internal thinking
    - ToolCallRequest: Tool call requests
    - ToolResult: Tool execution results
    """

    def __init__(self, service_url: str, token: str):
        self.service_url = service_url
        self.token = token

    def send_message_stream(
        self, message: str, conversation_urn: str, user_urn: str, timeout: int = 60
    ):
        """Send a chat message and stream events as they arrive.

        Yields:
            dict with:
                - event_type: str ("message", "complete", "error")
                - message: ChatHistory message object (if event_type == "message")
                - history: ChatHistory (if event_type == "complete")
                - error: str (if event_type == "error")
                - duration: float (if event_type == "complete")
                - event_count: int (if event_type == "complete")
        """
        from datahub_integrations.chat.chat_history import (
            AssistantMessage,
            ChatHistory,
            HumanMessage,
            ReasoningMessage,
        )

        url = f"{self.service_url}/private/api/chat/message"
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.token}",
        }
        payload = {
            "text": message,
            "conversation_urn": conversation_urn,
            "user_urn": user_urn,
            "agent_name": "DataCatalogExplorer",
        }

        start_time = time.time()
        history = ChatHistory()
        event_count = 0
        error = None

        try:
            with httpx.Client(timeout=timeout) as client:
                with client.stream(
                    "POST", url, headers=headers, json=payload
                ) as response:
                    response.raise_for_status()

                    # Process SSE stream and yield events in real-time
                    for line in response.iter_lines():
                        if not line or line.startswith(":"):
                            continue

                        if line.startswith("event:"):
                            event_count += 1
                        elif line.startswith("data:"):
                            data_str = line.split(":", 1)[1].strip()
                            try:
                                data = json.loads(data_str)

                                # Check for error
                                if "error" in data:
                                    error = data["error"]
                                    yield {"event_type": "error", "error": error}
                                    continue

                                # Check for completion event
                                if "conversation_urn" in data and "message" not in data:
                                    continue

                                # Parse message
                                if "message" not in data:
                                    continue

                                msg = data["message"]
                                msg_type = data.get(
                                    "message_type", msg.get("type", "TEXT")
                                )
                                content = msg.get("content", {})
                                text = content.get("text", "")
                                actor = msg.get("actor", {})
                                actor_type = actor.get("type", "AGENT")

                                # Map message types to ChatHistory message classes
                                new_message = None
                                if msg_type == "TEXT":
                                    if actor_type == "USER":
                                        new_message = HumanMessage(text=text)
                                    else:
                                        new_message = AssistantMessage(text=text)

                                elif msg_type == "THINKING":
                                    new_message = ReasoningMessage(text=text)

                                elif msg_type == "TOOL_CALL":
                                    new_message = ReasoningMessage(
                                        text=f"[Tool Call] {text}"
                                    )

                                elif msg_type == "TOOL_RESULT":
                                    new_message = ReasoningMessage(
                                        text=f"[Tool Result] {text}"
                                    )

                                if new_message:
                                    history.add_message(new_message)
                                    # Yield the new message immediately with original message_type
                                    yield {
                                        "event_type": "message",
                                        "message": new_message,
                                        "message_type": msg_type,  # Preserve original type
                                        "message_data": data["message"],  # Preserve original data
                                    }

                            except json.JSONDecodeError:
                                continue

            # Stream complete
            duration = time.time() - start_time
            yield {
                "event_type": "complete",
                "history": history,
                "duration": duration,
                "event_count": event_count,
                "success": error is None,
                "error": error,
            }

        except Exception as e:
            duration = time.time() - start_time
            error_msg = str(e)
            yield {
                "event_type": "error",
                "error": error_msg,
                "duration": duration,
                "event_count": event_count,
            }

    def send_message(
        self, message: str, conversation_urn: str, user_urn: str, timeout: int = 60
    ) -> dict:
        """Send a chat message and get response with full ChatHistory.

        Returns:
            dict with:
                - history: ChatHistory object with all messages
                - success: bool indicating if request succeeded
                - duration: float time in seconds
                - event_count: int number of SSE events
                - error: Optional[str] error message if failed
        """
        # Import here to avoid circular dependency issues
        from datahub_integrations.chat.chat_history import (
            AssistantMessage,
            ChatHistory,
            HumanMessage,
            ReasoningMessage,
        )

        url = f"{self.service_url}/private/api/chat/message"
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.token}",
        }
        payload = {
            "text": message,
            "conversation_urn": conversation_urn,
            "user_urn": user_urn,
            "agent_name": "DataCatalogExplorer",
        }

        start_time = time.time()
        history = ChatHistory()
        event_count = 0
        error = None
        last_assistant_text = ""  # Track last assistant response for legacy compat

        try:
            with httpx.Client(timeout=timeout) as client:
                with client.stream(
                    "POST", url, headers=headers, json=payload
                ) as response:
                    response.raise_for_status()

                    # Process SSE stream
                    for line in response.iter_lines():
                        if not line or line.startswith(":"):
                            continue

                        if line.startswith("event:"):
                            event_count += 1
                        elif line.startswith("data:"):
                            data_str = line.split(":", 1)[1].strip()
                            try:
                                data = json.loads(data_str)

                                # Check for error
                                if "error" in data:
                                    error = data["error"]
                                    continue

                                # Check for completion event
                                if "conversation_urn" in data and "message" not in data:
                                    # This is the completion event
                                    continue

                                # Parse message
                                if "message" not in data:
                                    continue

                                msg = data["message"]
                                msg_type = data.get(
                                    "message_type", msg.get("type", "TEXT")
                                )
                                content = msg.get("content", {})
                                text = content.get("text", "")
                                actor = msg.get("actor", {})
                                actor_type = actor.get("type", "AGENT")

                                # Map message types to ChatHistory message classes
                                if msg_type == "TEXT":
                                    if actor_type == "USER":
                                        history.add_message(HumanMessage(text=text))
                                    else:
                                        history.add_message(AssistantMessage(text=text))
                                        last_assistant_text = text

                                elif msg_type == "THINKING":
                                    history.add_message(ReasoningMessage(text=text))

                                elif msg_type == "TOOL_CALL":
                                    # For now, store tool calls as reasoning messages
                                    # TODO: Parse into proper ToolCallRequest objects
                                    history.add_message(
                                        ReasoningMessage(text=f"[Tool Call] {text}")
                                    )

                                elif msg_type == "TOOL_RESULT":
                                    # For now, store tool results as reasoning messages
                                    # TODO: Parse into proper ToolResult objects
                                    history.add_message(
                                        ReasoningMessage(text=f"[Tool Result] {text}")
                                    )

                                elif msg_type == "KEEPALIVE":
                                    # Ignore keepalive messages
                                    pass

                            except json.JSONDecodeError:
                                logger.debug(
                                    f"Failed to parse SSE data: {data_str[:100]}"
                                )
                                pass

        except httpx.HTTPStatusError as e:
            error = f"HTTP {e.response.status_code}"
        except Exception as e:
            error = str(e)

        duration = time.time() - start_time

        return {
            "success": error is None,
            "duration": duration,
            "history": history,
            "response": last_assistant_text,  # Legacy field for backward compat
            "event_count": event_count,
            "error": error,
        }


# =============================================================================
# CLI-ONLY CLASSES
# =============================================================================
# Classes used only when running as CLI tool (not imported by library users)


class ConversationLogger:
    """Log conversations to DuckDB for analytics and visualization."""

    def __init__(self, db_path: str):
        self.db_path = db_path
        self.conn = duckdb.connect(db_path)
        self._init_schema()

    def _init_schema(self):
        """Initialize database schema."""
        # Conversations table
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS conversations (
                conversation_id VARCHAR PRIMARY KEY,
                user_urn VARCHAR,
                started_at TIMESTAMP,
                ended_at TIMESTAMP,
                message_count INTEGER,
                success_count INTEGER,
                error_count INTEGER,
                total_duration_seconds DOUBLE,
                avg_response_time_seconds DOUBLE
            )
        """)

        # Messages table
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS messages (
                message_id VARCHAR PRIMARY KEY,
                conversation_id VARCHAR,
                timestamp TIMESTAMP,
                role VARCHAR,  -- 'user' or 'assistant'
                content TEXT,
                success BOOLEAN,
                duration_seconds DOUBLE,
                error TEXT,
                event_count INTEGER,
                FOREIGN KEY (conversation_id) REFERENCES conversations(conversation_id)
            )
        """)

        # Create indexes
        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_conv_started
            ON conversations(started_at DESC)
        """)

        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_msg_conv
            ON messages(conversation_id, timestamp)
        """)

    def log_conversation(self, conversation: ConversationState):
        """Log a completed conversation."""
        # Calculate stats - only count assistant message success/errors
        assistant_messages = [
            m for m in conversation.messages if m["role"] == "assistant"
        ]

        success_count = sum(1 for m in assistant_messages if m.get("success", False))
        error_count = len(assistant_messages) - success_count

        total_duration = sum(m.get("duration", 0.0) for m in conversation.messages)
        avg_response = (
            total_duration / len(assistant_messages) if assistant_messages else 0.0
        )

        # Insert conversation
        self.conn.execute(
            """
            INSERT INTO conversations VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
            [
                conversation.conversation_urn,
                conversation.user_urn,
                datetime.fromtimestamp(conversation.started_at),
                datetime.now(),
                conversation.message_count,
                success_count,
                error_count,
                total_duration,
                avg_response,
            ],
        )

        # Insert messages
        for idx, msg in enumerate(conversation.messages):
            message_id = f"{conversation.conversation_urn}-{idx}"
            self.conn.execute(
                """
                INSERT INTO messages VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
                [
                    message_id,
                    conversation.conversation_urn,
                    msg.get("timestamp", datetime.now()),
                    msg["role"],
                    msg["content"],
                    msg.get("success", True if msg["role"] == "user" else False),
                    msg.get("duration", 0.0),
                    msg.get("error"),
                    msg.get("event_count", 0),
                ],
            )

        self.conn.commit()

    def get_stats(self) -> dict:
        """Get overall statistics."""
        stats = self.conn.execute("""
            SELECT
                COUNT(*) as total_conversations,
                SUM(message_count) as total_messages,
                AVG(message_count) as avg_messages_per_conv,
                SUM(success_count) as total_successes,
                SUM(error_count) as total_errors,
                AVG(avg_response_time_seconds) as avg_response_time
            FROM conversations
        """).fetchone()

        if not stats:
            return {
                "total_conversations": 0,
                "total_messages": 0,
                "avg_messages_per_conv": 0,
                "total_successes": 0,
                "total_errors": 0,
                "avg_response_time": 0,
            }

        return {
            "total_conversations": stats[0] or 0,
            "total_messages": stats[1] or 0,
            "avg_messages_per_conv": stats[2] or 0,
            "total_successes": stats[3] or 0,
            "total_errors": stats[4] or 0,
            "avg_response_time": stats[5] or 0,
        }

    def close(self):
        """Close database connection."""
        self.conn.close()


# =============================================================================
# CLI UTILITIES
# =============================================================================
# Utility functions for CLI interface


def print_banner(log_file: str | None = None):
    """Print welcome banner."""
    print("=" * 70)
    print("🤖 DataHub Realistic Traffic Generator")
    print("=" * 70)
    print(f"Service URL: {SERVICE_URL}")
    print(f"GMS URL:     {GMS_URL}")
    print("Dashboard:   http://localhost:8501 (if running)")
    if log_file:
        print(f"Log DB:      {log_file}")
    print("=" * 70)
    print()


# =============================================================================
# TRAFFIC GENERATION - CLI ONLY
# =============================================================================
# Main traffic generation loop (only used when run as CLI tool)


def run_traffic_generation(
    num_conversations: int | None,
    max_conversation_length: int,
    delay: float,
    continuous: bool,
    log_file: str | None = None,
) -> None:
    """Run the traffic generation."""

    # Initialize Prometheus metrics (needed for CLI mode)
    _init_metrics()

    # Initialize conversation log database
    if not log_file:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = f"/tmp/traffic_conversations_{timestamp}.duckdb"

    print_banner(log_file)
    logger.info(f"Logging conversations to: {log_file}")
    conversation_logger = ConversationLogger(log_file)

    # Initialize AWS session
    logger.info("Initializing AWS Bedrock client...")
    try:
        if AWS_PROFILE:
            session = boto3.Session(profile_name=AWS_PROFILE, region_name=AWS_REGION)
        else:
            session = boto3.Session(region_name=AWS_REGION)

        bedrock_client = session.client("bedrock-runtime")
        logger.info(f"✓ Connected to Bedrock in {AWS_REGION}")
    except Exception as e:
        logger.error(f"Failed to initialize Bedrock client: {e}")
        logger.error("Make sure you have AWS credentials configured")
        logger.error("Run: aws sso login --profile your-profile")
        sys.exit(1)

    # Initialize clients
    chat_client = ChatClient(SERVICE_URL, GMS_TOKEN)

    # Create DataHub sketch
    logger.info("Sketching DataHub backend...")
    sketcher = DataHubSketcher(GMS_URL, GMS_TOKEN)
    sketch = sketcher.create_sketch()

    logger.info("\nDataHub Sketch:")
    print(sketch.to_context_string())
    print()

    # Initialize conversation generator
    generator = ConversationGenerator(bedrock_client, sketch)

    # Track active conversation
    current_conversation: ConversationState | None = None

    # Statistics
    total_messages = 0
    total_conversations = 0
    success_count = 0
    error_count = 0

    logger.info("Starting traffic generation...")
    print()

    try:
        while True:
            # Check if we need to start a new conversation
            if (
                current_conversation is None
                or current_conversation.message_count >= max_conversation_length
            ):
                # Log completed conversation
                if current_conversation:
                    duration = time.time() - current_conversation.started_at
                    logger.info(
                        f"Conversation {current_conversation.conversation_urn} completed: "
                        f"{current_conversation.message_count} messages in {duration:.1f}s"
                    )

                    # Log to database
                    try:
                        conversation_logger.log_conversation(current_conversation)
                        logger.debug("Conversation logged to database")
                    except Exception as e:
                        logger.error(f"Failed to log conversation: {e}")

                    print()

                total_conversations += 1
                conversation_urn = f"urn:li:conversation:realistic-{int(time.time())}-{random.randint(1000, 9999)}"
                user_urn = f"urn:li:corpuser:analyst-{random.randint(100, 999)}"

                current_conversation = ConversationState(
                    conversation_urn=conversation_urn,
                    user_urn=user_urn,
                    messages=[],
                    started_at=time.time(),
                    message_count=0,
                )

                logger.info(f"🆕 Starting conversation {total_conversations}")
                logger.info(f"   URN: {conversation_urn}")
                logger.info(f"   User: {user_urn}")
                print()

                # Check if we've reached conversation limit
                if (
                    not continuous
                    and num_conversations
                    and total_conversations > num_conversations
                ):
                    break

            # Generate question
            if current_conversation.message_count == 0:
                question = generator.generate_initial_question()
                logger.info("📝 Generated initial question")
            else:
                question = generator.generate_followup_question(
                    current_conversation.messages
                )
                logger.info(
                    f"📝 Generated follow-up ({current_conversation.message_count + 1}/{max_conversation_length})"
                )

            total_messages += 1
            timestamp = datetime.now().strftime("%H:%M:%S")
            print(f"[{timestamp}] Q: {question}")

            # Send message
            result = chat_client.send_message(
                question,
                current_conversation.conversation_urn,
                current_conversation.user_urn,
            )

            # Update state - add user message (always successful)
            current_conversation.add_message("user", question)

            # Add assistant response with metadata
            if result["success"]:
                success_count += 1
                current_conversation.add_message(
                    "assistant",
                    result["response"],
                    success=True,
                    duration=result["duration"],
                    event_count=result["event_count"],
                )
                print(
                    f"  ✓ Success ({result['duration']:.2f}s, {result['event_count']} events)"
                )
                if result["response"]:
                    preview = result["response"][:100].replace("\n", " ")
                    print(f"  A: {preview}...")
            else:
                error_count += 1
                current_conversation.add_message(
                    "assistant",
                    result.get("response", ""),
                    success=False,
                    duration=result["duration"],
                    error=result["error"],
                    event_count=result["event_count"],
                )
                print(f"  ✗ Error: {result['error']}")

            # Print stats
            success_rate = (success_count / total_messages) * 100
            print(
                f"  📊 Total: {total_messages} msgs, {total_conversations} convs, "
                f"{success_rate:.1f}% success"
            )
            print()

            # Wait before next message
            time.sleep(delay)

    except KeyboardInterrupt:
        print("\n\n⚠️  Traffic generation interrupted")

    # Log final conversation if exists
    if current_conversation and current_conversation.message_count > 0:
        try:
            conversation_logger.log_conversation(current_conversation)
            logger.debug("Final conversation logged to database")
        except Exception as e:
            logger.error(f"Failed to log final conversation: {e}")

    # Get database stats
    try:
        db_stats = conversation_logger.get_stats()
    except Exception as e:
        logger.error(f"Failed to get database stats: {e}")
        db_stats = {}

    # Close logger
    conversation_logger.close()

    # Final summary
    print("=" * 70)
    print("📊 Traffic Generation Summary")
    print("=" * 70)
    print(f"Conversations:        {total_conversations}")
    print(f"Total Messages:       {total_messages}")
    print(f"Successful:           {success_count}")
    print(f"Failed:               {error_count}")
    if total_messages > 0:
        print(f"Success Rate:         {(success_count / total_messages) * 100:.1f}%")
        print(
            f"Avg per Conversation: {total_messages / total_conversations:.1f} messages"
        )

    # Database stats
    if db_stats:
        print()
        print("📊 Database Stats:")
        print(f"Logged Conversations: {db_stats['total_conversations']}")
        print(f"Logged Messages:      {db_stats['total_messages']}")
        if db_stats["avg_response_time"] > 0:
            print(f"Avg Response Time:    {db_stats['avg_response_time']:.2f}s")
        print()
        print(f"Log Database:         {log_file}")

    print("=" * 70)


# =============================================================================
# CLI ENTRY POINT
# =============================================================================
# Command-line interface (only executed when run as script)


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Generate realistic chat traffic using Anthropic and DataHub context",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument(
        "--conversations",
        "-c",
        type=int,
        help="Number of conversations to simulate (omit for continuous)",
    )
    parser.add_argument(
        "--continuous",
        action="store_true",
        help="Run continuously until interrupted",
    )
    parser.add_argument(
        "--max-length",
        "-l",
        type=int,
        default=10,
        help="Maximum messages per conversation (default: 10)",
    )
    parser.add_argument(
        "--delay",
        "-d",
        type=float,
        default=3.0,
        help="Delay between messages in seconds (default: 3.0)",
    )

    args = parser.parse_args()

    # Validate arguments
    if not args.continuous and not args.conversations:
        parser.error("Must specify either --conversations or --continuous")

    try:
        run_traffic_generation(
            num_conversations=args.conversations,
            max_conversation_length=args.max_length,
            delay=args.delay,
            continuous=args.continuous,
        )
    except Exception:
        logger.exception("Traffic generation failed")
        sys.exit(1)


if __name__ == "__main__":
    main()
