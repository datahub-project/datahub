"""Metric registry: Single source of truth for all OpenTelemetry metrics.

This module defines all metrics in the application with their configurations,
including bucket profiles for histograms. The registry is used to automatically
generate OpenTelemetry Views and ensure consistency across the codebase.
"""

from dataclasses import dataclass
from enum import Enum

from datahub_integrations.observability.metrics_constants import (
    ACTIONS_EXECUTION_DURATION,
    ACTIONS_VENV_SETUP_DURATION,
    ANALYTICS_QUERY_DURATION,
    BOT_DATAHUB_QUERY_DURATION,
    BOT_OAUTH_DURATION,
    BOT_SLACK_API_DURATION,
    BOT_TEAMS_API_DURATION,
    GENAI_LLM_CALL_DURATION,
    GENAI_TOOL_CALL_DURATION,
    GENAI_USER_MESSAGE_DURATION,
)


class BucketProfile(str, Enum):
    """Histogram bucket profile types."""

    FAST = "fast"  # API calls, queries, routing (10ms-5s)
    SLOW = "slow"  # LLM calls, batch jobs, long-running (1s-5min)


class MetricType(str, Enum):
    """OpenTelemetry metric instrument types."""

    COUNTER = "counter"
    HISTOGRAM = "histogram"
    GAUGE = "gauge"
    UP_DOWN_COUNTER = "up_down_counter"


@dataclass(frozen=True)
class DurationMetricDefinition:
    """Definition for a duration histogram metric.

    Attributes:
        name: Metric name (must match constant in metrics_constants.py)
        description: Human-readable description of what this metric measures
        bucket_profile: Which bucket configuration to use (fast or slow)
        labels: Expected label names for this metric (for documentation)
    """

    name: str
    description: str
    bucket_profile: BucketProfile
    labels: list[str]


# ============================================================================
# DURATION METRICS REGISTRY
# ============================================================================
# This is the single source of truth for all duration histograms.
# Each metric defined here will automatically get an OpenTelemetry View
# with the appropriate bucket configuration.

DURATION_METRICS: list[DurationMetricDefinition] = [
    # ========================================================================
    # HTTP Server Metrics (auto-instrumented by opentelemetry-instrumentation-fastapi)
    # ========================================================================
    DurationMetricDefinition(
        name="http.server.request.duration",
        description="HTTP request duration (OpenTelemetry semantic convention)",
        bucket_profile=BucketProfile.FAST,
        labels=["http.request.method", "http.response.status_code", "url.scheme"],
    ),
    DurationMetricDefinition(
        name="http.server.duration",
        description="HTTP request duration (legacy OpenTelemetry convention)",
        bucket_profile=BucketProfile.FAST,
        labels=["http.method", "http.status_code", "http.scheme"],
    ),
    DurationMetricDefinition(
        name="http_server_request_duration",
        description="HTTP request duration (alternative naming)",
        bucket_profile=BucketProfile.FAST,
        labels=["method", "status_code"],
    ),
    # ========================================================================
    # Bot API Call Metrics
    # ========================================================================
    DurationMetricDefinition(
        name=BOT_DATAHUB_QUERY_DURATION,
        description="DataHub GraphQL query latency from Slack/Teams bots",
        bucket_profile=BucketProfile.FAST,
        labels=["platform", "query_type", "status"],
    ),
    DurationMetricDefinition(
        name=BOT_SLACK_API_DURATION,
        description="Slack API call latency (chat.postMessage, users.info, etc)",
        bucket_profile=BucketProfile.FAST,
        labels=["platform", "api_method", "status"],
    ),
    DurationMetricDefinition(
        name=BOT_TEAMS_API_DURATION,
        description="Microsoft Teams API call latency",
        bucket_profile=BucketProfile.FAST,
        labels=["platform", "api_method", "status"],
    ),
    DurationMetricDefinition(
        name=BOT_OAUTH_DURATION,
        description="OAuth flow latency (install, callback, refresh)",
        bucket_profile=BucketProfile.FAST,
        labels=["platform", "oauth_flow", "status"],
    ),
    # ========================================================================
    # Analytics Engine Metrics
    # ========================================================================
    DurationMetricDefinition(
        name=ANALYTICS_QUERY_DURATION,
        description="Analytics query execution (schema, preview, query operations)",
        bucket_profile=BucketProfile.FAST,
        labels=["operation", "status"],
    ),
    # ========================================================================
    # Actions Framework Metrics
    # ========================================================================
    DurationMetricDefinition(
        name=ACTIONS_VENV_SETUP_DURATION,
        description="Action virtual environment setup time",
        bucket_profile=BucketProfile.FAST,
        labels=["action_urn", "action_type", "stage"],
    ),
    DurationMetricDefinition(
        name=ACTIONS_EXECUTION_DURATION,
        description="Action pipeline stage execution time (can be long-running)",
        bucket_profile=BucketProfile.SLOW,
        labels=["action_urn", "action_type", "stage"],
    ),
    # ========================================================================
    # Slack/Teams Command Metrics (created by @otel_instrument decorator)
    # ========================================================================
    DurationMetricDefinition(
        name="slack_command_duration_seconds",
        description="Slack/Teams command execution latency",
        bucket_profile=BucketProfile.FAST,
        labels=["platform", "command"],
    ),
    # ========================================================================
    # GenAI Three-Tier Metrics
    # ========================================================================
    DurationMetricDefinition(
        name=GENAI_USER_MESSAGE_DURATION,
        description="User message processing latency (Tier 1: request to response)",
        bucket_profile=BucketProfile.SLOW,
        labels=["ai_module", "platform", "status"],
    ),
    DurationMetricDefinition(
        name=GENAI_LLM_CALL_DURATION,
        description="Individual LLM API call latency (Tier 2)",
        bucket_profile=BucketProfile.SLOW,
        labels=["provider", "model", "ai_module", "status"],
    ),
    DurationMetricDefinition(
        name=GENAI_TOOL_CALL_DURATION,
        description="Individual tool execution latency (Tier 3)",
        bucket_profile=BucketProfile.SLOW,
        labels=["ai_module", "tool", "status"],
    ),
]


# ============================================================================
# REGISTRY UTILITIES
# ============================================================================


def get_metrics_by_profile(
    profile: BucketProfile,
) -> list[DurationMetricDefinition]:
    """Get all duration metrics for a specific bucket profile.

    Args:
        profile: Bucket profile to filter by (FAST or SLOW)

    Returns:
        List of metrics matching the profile
    """
    return [m for m in DURATION_METRICS if m.bucket_profile == profile]


def get_all_metric_names() -> list[str]:
    """Get all registered metric names.

    Returns:
        List of all metric names in the registry
    """
    return [m.name for m in DURATION_METRICS]


def validate_metric_exists(name: str) -> bool:
    """Check if a metric is registered.

    Args:
        name: Metric name to check

    Returns:
        True if metric is registered, False otherwise
    """
    return name in get_all_metric_names()
