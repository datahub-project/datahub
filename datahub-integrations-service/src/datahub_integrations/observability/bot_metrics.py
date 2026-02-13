"""Bot and plugin observability metrics.

This module provides instrumentation for bot command execution, external API calls,
OAuth flows, and external MCP plugin operations with low-cardinality labels
for Prometheus compatibility.
"""

import time
from enum import Enum
from typing import Optional

from opentelemetry import metrics

from datahub_integrations.observability.metrics_constants import (
    AI_PLUGIN_OAUTH_DURATION,
    AI_PLUGIN_OAUTH_TOTAL,
    BOT_DATAHUB_QUERY_DURATION,
    BOT_DATAHUB_QUERY_TOTAL,
    BOT_OAUTH_DURATION,
    BOT_OAUTH_TOTAL,
    BOT_SLACK_API_DURATION,
    BOT_SLACK_API_TOTAL,
    BOT_TEAMS_API_DURATION,
    BOT_TEAMS_API_TOTAL,
    EXTERNAL_MCP_PLUGIN_DISCOVERY_DURATION,
    EXTERNAL_MCP_PLUGIN_DISCOVERY_TOTAL,
    LABEL_API_METHOD,
    LABEL_OAUTH_FLOW,
    LABEL_OAUTH_STEP,
    LABEL_PLATFORM,
    LABEL_PLUGIN_NAME,
    LABEL_QUERY_TYPE,
    LABEL_STATUS,
)


class BotPlatform(str, Enum):
    """Bot platforms for low-cardinality labeling."""

    SLACK = "slack"
    TEAMS = "teams"


class BotCommand(str, Enum):
    """Bot commands for low-cardinality labeling."""

    SEARCH = "search"
    GET = "get"
    ASK = "ask"
    HELP = "help"
    MENTION = "mention"


class OAuthFlow(str, Enum):
    """OAuth flow types for low-cardinality labeling."""

    INSTALL = "install"
    CALLBACK = "callback"
    REFRESH = "refresh"


# Get OpenTelemetry meter
_meter = metrics.get_meter(__name__)

# DataHub GraphQL query metrics
_datahub_query_duration = _meter.create_histogram(
    name=BOT_DATAHUB_QUERY_DURATION,
    description="Duration of DataHub GraphQL queries from bots",
    unit="s",
)

_datahub_query_counter = _meter.create_counter(
    name=BOT_DATAHUB_QUERY_TOTAL,
    description="Total DataHub GraphQL queries from bots",
    unit="queries",
)

# Slack API call metrics
_slack_api_duration = _meter.create_histogram(
    name=BOT_SLACK_API_DURATION,
    description="Duration of Slack API calls",
    unit="s",
)

_slack_api_counter = _meter.create_counter(
    name=BOT_SLACK_API_TOTAL,
    description="Total Slack API calls",
    unit="calls",
)

# Teams API call metrics
_teams_api_duration = _meter.create_histogram(
    name=BOT_TEAMS_API_DURATION,
    description="Duration of Teams API calls",
    unit="s",
)

_teams_api_counter = _meter.create_counter(
    name=BOT_TEAMS_API_TOTAL,
    description="Total Teams API calls",
    unit="calls",
)

# OAuth flow metrics
_oauth_duration = _meter.create_histogram(
    name=BOT_OAUTH_DURATION,
    description="Duration of OAuth flows",
    unit="s",
)

_oauth_counter = _meter.create_counter(
    name=BOT_OAUTH_TOTAL,
    description="Total OAuth flows",
    unit="flows",
)


def record_datahub_query(
    platform: BotPlatform,
    query_type: str,
    duration_seconds: float,
    success: bool,
) -> None:
    """Record DataHub GraphQL query metrics.

    Args:
        platform: Bot platform (SLACK or TEAMS)
        query_type: Type of query (search, entity, subscription, incident)
        duration_seconds: Query duration in seconds
        success: Whether the query succeeded
    """
    labels = {
        LABEL_PLATFORM: platform.value,
        LABEL_QUERY_TYPE: query_type,
        LABEL_STATUS: "success" if success else "error",
    }
    _datahub_query_duration.record(duration_seconds, attributes=labels)
    _datahub_query_counter.add(1, attributes=labels)


def record_slack_api_call(
    api_method: str,
    duration_seconds: float,
    success: bool,
) -> None:
    """Record Slack API call metrics.

    Args:
        api_method: Slack API method (chat.postMessage, conversations.history, etc.)
        duration_seconds: Call duration in seconds
        success: Whether the call succeeded
    """
    labels = {
        LABEL_PLATFORM: BotPlatform.SLACK.value,
        LABEL_API_METHOD: api_method,
        LABEL_STATUS: "success" if success else "error",
    }
    _slack_api_duration.record(duration_seconds, attributes=labels)
    _slack_api_counter.add(1, attributes=labels)


def record_teams_api_call(
    api_method: str,
    duration_seconds: float,
    success: bool,
) -> None:
    """Record Teams API call metrics.

    Args:
        api_method: Teams API method (bot.send, bot.update, graph.user, etc.)
        duration_seconds: Call duration in seconds
        success: Whether the call succeeded
    """
    labels = {
        LABEL_PLATFORM: BotPlatform.TEAMS.value,
        LABEL_API_METHOD: api_method,
        LABEL_STATUS: "success" if success else "error",
    }
    _teams_api_duration.record(duration_seconds, attributes=labels)
    _teams_api_counter.add(1, attributes=labels)


def record_oauth_flow(
    platform: BotPlatform,
    flow: OAuthFlow,
    duration_seconds: float,
    success: bool,
) -> None:
    """Record OAuth flow metrics.

    Args:
        platform: Bot platform (SLACK or TEAMS)
        flow: OAuth flow type (INSTALL, CALLBACK, REFRESH)
        duration_seconds: Flow duration in seconds
        success: Whether the flow succeeded
    """
    labels = {
        LABEL_PLATFORM: platform.value,
        LABEL_OAUTH_FLOW: flow.value,
        LABEL_STATUS: "success" if success else "error",
    }
    _oauth_duration.record(duration_seconds, attributes=labels)
    _oauth_counter.add(1, attributes=labels)


# External MCP plugin discovery metrics
_plugin_discovery_duration = _meter.create_histogram(
    name=EXTERNAL_MCP_PLUGIN_DISCOVERY_DURATION,
    description="Duration of external MCP plugin tool discovery",
    unit="s",
)

_plugin_discovery_counter = _meter.create_counter(
    name=EXTERNAL_MCP_PLUGIN_DISCOVERY_TOTAL,
    description="Total external MCP plugin discovery attempts",
    unit="discoveries",
)


def record_plugin_discovery(
    plugin_name: str,
    duration_seconds: float,
    success: bool,
) -> None:
    """Record external MCP plugin discovery metrics.

    Args:
        plugin_name: Human-readable name of the plugin being discovered.
        duration_seconds: Discovery duration in seconds.
        success: Whether the discovery succeeded.
    """
    labels = {
        LABEL_PLUGIN_NAME: plugin_name,
        LABEL_STATUS: "success" if success else "error",
    }
    _plugin_discovery_duration.record(duration_seconds, attributes=labels)
    _plugin_discovery_counter.add(1, attributes=labels)


# AI plugin OAuth flow metrics (user connecting credentials to external services)
# Separate from bot OAuth metrics (bot_oauth_*) which track bot installation flows.


class AiPluginOAuthStep(str, Enum):
    """Steps in the AI plugin OAuth flow."""

    CONNECT = "connect"
    CALLBACK = "callback"
    REFRESH = "refresh"


_ai_plugin_oauth_duration = _meter.create_histogram(
    name=AI_PLUGIN_OAUTH_DURATION,
    description="Duration of AI plugin OAuth flows (user credential connections)",
    unit="s",
)

_ai_plugin_oauth_counter = _meter.create_counter(
    name=AI_PLUGIN_OAUTH_TOTAL,
    description="Total AI plugin OAuth flow attempts",
    unit="flows",
)


def record_ai_plugin_oauth_flow(
    *,
    step: AiPluginOAuthStep,
    duration_seconds: float,
    success: bool,
) -> None:
    """Record AI plugin OAuth flow metrics.

    Tracks user-initiated OAuth flows for connecting credentials to external
    services (MCP servers). Separate from bot OAuth metrics which track
    Slack/Teams bot installation flows.

    Args:
        step: Which step of the OAuth flow (connect, callback, refresh).
        duration_seconds: Flow duration in seconds.
        success: Whether the flow step succeeded.
    """
    labels = {
        LABEL_OAUTH_STEP: step.value,
        LABEL_STATUS: "success" if success else "error",
    }
    _ai_plugin_oauth_duration.record(duration_seconds, attributes=labels)
    _ai_plugin_oauth_counter.add(1, attributes=labels)


class datahub_query_tracker:
    """Context manager for tracking DataHub GraphQL queries.

    Example:
        with datahub_query_tracker("search", BotPlatform.SLACK):
            results = graph.execute_graphql(SEARCH_QUERY, variables)
    """

    def __init__(self, query_type: str, platform: BotPlatform):
        self.query_type = query_type
        self.platform = platform
        self.start_time: Optional[float] = None

    def __enter__(self) -> "datahub_query_tracker":
        self.start_time = time.perf_counter()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        if self.start_time is not None:
            duration = time.perf_counter() - self.start_time
            success = exc_type is None
            record_datahub_query(
                platform=self.platform,
                query_type=self.query_type,
                duration_seconds=duration,
                success=success,
            )
        # Don't suppress exceptions
