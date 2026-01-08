"""OpenTelemetry observability module for datahub-integrations-service."""

from datahub_integrations.observability.bot_metrics import (
    BotCommand,
    BotPlatform,
    OAuthFlow,
    datahub_query_tracker,
    record_datahub_query,
    record_oauth_flow,
    record_slack_api_call,
    record_teams_api_call,
)
from datahub_integrations.observability.config import ObservabilityConfig
from datahub_integrations.observability.cost import (
    CostCalculator,
    CostEstimate,
    CostTracker,
    CostTrackerProtocol,
    TokenUsage,
    get_cost_tracker,
)
from datahub_integrations.observability.cost_utils import (
    detect_provider_and_normalize_model,
)
from datahub_integrations.observability.decorators import (
    HasProviderAndModelInfo,
    otel_counter,
    otel_duration,
    otel_instrument,
    otel_llm_call,
    otel_span,
)
from datahub_integrations.observability.metrics_registry import (
    BucketProfile,
    DurationMetricDefinition,
    get_all_metric_names,
    get_metrics_by_profile,
    validate_metric_exists,
)
from datahub_integrations.observability.otel_config import (
    OTELConfigurationError,
    initialize_observability,
)

__all__ = [
    "ObservabilityConfig",
    "OTELConfigurationError",
    "initialize_observability",
    "otel_duration",
    "otel_counter",
    "otel_span",
    "otel_instrument",
    "otel_llm_call",
    "HasProviderAndModelInfo",
    # Metric registry
    "BucketProfile",
    "DurationMetricDefinition",
    "get_all_metric_names",
    "get_metrics_by_profile",
    "validate_metric_exists",
    # Cost tracking
    "TokenUsage",
    "CostEstimate",
    "CostCalculator",
    "CostTracker",
    "CostTrackerProtocol",
    "get_cost_tracker",
    "detect_provider_and_normalize_model",
    # Bot metrics
    "BotPlatform",
    "BotCommand",
    "OAuthFlow",
    "datahub_query_tracker",
    "record_datahub_query",
    "record_slack_api_call",
    "record_teams_api_call",
    "record_oauth_flow",
]
