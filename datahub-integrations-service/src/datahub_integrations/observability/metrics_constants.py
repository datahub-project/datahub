"""Constants for observability metrics.

This module centralizes all metric names to ensure consistency between
metric creation and consumption (e.g., in dashboards and tests).
"""

from enum import Enum


class AIModule(str, Enum):
    """AI modules in DataHub integrations service.

    Each value represents a distinct AI-powered feature/module.
    Use this enum for the ai_module label in metrics to ensure consistency.
    """

    # Conversational AI interface
    CHAT = "chat"

    # Auto-generating entity and column descriptions
    DESCRIPTION_GENERATION = "description_generation"

    # Suggesting glossary terms for entities
    TERMS_SUGGESTION = "terms_suggestion"

    # Suggesting descriptions for queries
    QUERY_DESCRIPTION = "query_description"


# === GenAI Metrics ===

# Token usage metrics
GENAI_LLM_TOKENS_TOTAL = "genai_llm_tokens_total"

# Cost metrics (note: OpenTelemetry appends unit suffix)
GENAI_LLM_COST_TOTAL = "genai_llm_cost_total"
GENAI_LLM_COST_TOTAL_WITH_UNIT = (
    "genai_llm_cost_USD_total"  # What appears in Prometheus
)

GENAI_LLM_COST_PER_CALL = "genai_llm_cost_per_call"
GENAI_LLM_COST_PER_CALL_WITH_UNIT = (
    "genai_llm_cost_per_call_USD"  # What appears in Prometheus
)

# Three-tier tracking metrics

# Tier 1: User messages (top level - user requests)
GENAI_USER_MESSAGES_TOTAL = "genai_user_messages_total"

# Tier 2: LLM calls (middle level - API calls, can be multiple per user message)
GENAI_LLM_CALLS_TOTAL = "genai_llm_calls_total"

# Tier 3: Tool calls (bottom level - tool invocations, can be multiple per LLM call)
GENAI_TOOL_CALLS_TOTAL = "genai_tool_calls_total"

# Latency metrics for three-tier tracking

# Tier 1: User message latency (overall request-response time)
GENAI_USER_MESSAGE_DURATION = "genai_user_message_duration_seconds"

# Tier 2: LLM call latency (per LLM invocation)
GENAI_LLM_CALL_DURATION = "genai_llm_call_duration_seconds"

# Tier 3: Tool call latency (per tool execution)
GENAI_TOOL_CALL_DURATION = "genai_tool_call_duration_seconds"

# GenAI metric labels
LABEL_PROVIDER = "provider"
LABEL_MODEL = "model"
LABEL_AI_MODULE = "ai_module"
LABEL_STATUS = "status"
LABEL_TOKEN_TYPE = "token_type"
LABEL_TOOL = "tool"  # For tool call tracking

# Token types
TOKEN_TYPE_PROMPT = "prompt"
TOKEN_TYPE_COMPLETION = "completion"
TOKEN_TYPE_CACHE_READ = "cache_read"
TOKEN_TYPE_CACHE_WRITE = "cache_write"

# === HTTP Metrics ===

# HTTP request metrics (OpenTelemetry semantic conventions)
HTTP_SERVER_DURATION = "http_server_duration_milliseconds"
HTTP_SERVER_DURATION_COUNT = "http_server_duration_milliseconds_count"
HTTP_SERVER_DURATION_SUM = "http_server_duration_milliseconds_sum"
HTTP_SERVER_ACTIVE_REQUESTS = "http_server_active_requests"

# HTTP metric labels
LABEL_HTTP_METHOD = "http_method"
LABEL_HTTP_STATUS_CODE = "http_status_code"
LABEL_HTTP_TARGET = "http_target"
LABEL_HTTP_SCHEME = "http_scheme"
LABEL_HTTP_HOST = "http_host"

# === Process Metrics ===

# Process resource usage (OpenTelemetry semantic conventions)
PROCESS_CPU_UTILIZATION = "process_cpu_utilization"
PROCESS_CPU_TIME = "process_cpu_time_seconds_total"
PROCESS_MEMORY_USAGE = "process_memory_usage_bytes"
PROCESS_MEMORY_VIRTUAL = "process_memory_virtual_bytes"
PROCESS_THREAD_COUNT = "process_thread_count"

# === System Metrics ===

SYSTEM_CPU_UTILIZATION = "system_cpu_utilization"
SYSTEM_MEMORY_USAGE = "system_memory_usage_bytes"
SYSTEM_MEMORY_UTILIZATION = "system_memory_utilization"

# === Actions Metrics ===

# Phase 1: Basic execution tracking
ACTIONS_VENV_SETUP_DURATION = "actions_venv_setup_duration_seconds"
ACTIONS_EXECUTION_DURATION = "actions_execution_duration_seconds"
ACTIONS_EXECUTION_TOTAL = "actions_execution_total"

# Phase 2: State and lifecycle tracking
# Base names (used when creating metrics)
ACTIONS_RUNNING_COUNT_BASE = "actions_running_count"
ACTIONS_INFO_BASE = "actions_info"
ACTIONS_SUCCESS_TOTAL_BASE = "actions_success_total"
ACTIONS_ERROR_TOTAL_BASE = "actions_error_total"

# Exported names (what appears in Prometheus - OpenTelemetry appends unit)
ACTIONS_RUNNING_COUNT = "actions_running_count_actions"  # unit="actions" appended
ACTIONS_INFO = "actions_info"  # unit="1" not appended
ACTIONS_SUCCESS_TOTAL = "actions_success_total_executions"  # unit="executions" appended
ACTIONS_ERROR_TOTAL = "actions_error_total_executions"  # unit="executions" appended

# Phase 3: Event and asset processing tracking
# Base names (used when creating metrics)
ACTIONS_ASSETS_PROCESSED_BASE = "actions_assets_processed_total"
ACTIONS_ASSETS_IMPACTED_BASE = "actions_assets_impacted_total"
ACTIONS_ACTIONS_EXECUTED_BASE = "actions_actions_executed_total"
ACTIONS_EVENTS_PROCESSED_BASE = "actions_events_processed_total"
ACTIONS_EVENT_LAG_BASE = "actions_event_processing_lag_seconds"

# Exported names (what appears in Prometheus - OpenTelemetry inserts unit before _total)
ACTIONS_ASSETS_PROCESSED = "actions_assets_processed_assets_total"
ACTIONS_ASSETS_IMPACTED = "actions_assets_impacted_assets_total"
ACTIONS_ACTIONS_EXECUTED = "actions_actions_executed_actions_total"
ACTIONS_EVENTS_PROCESSED = "actions_events_processed_events_total"
ACTIONS_EVENT_LAG = "actions_event_processing_lag_seconds"  # unit already in name

# Actions metric labels
LABEL_ACTION_URN = "action_urn"
LABEL_ACTION_NAME = "action_name"
LABEL_ACTION_TYPE = "action_type"
LABEL_STAGE = "stage"
LABEL_STATE = "state"
LABEL_EXECUTOR_ID = "executor_id"
LABEL_ERROR_TYPE = "error_type"

# === Slack Metrics ===

SLACK_COMMAND_DURATION = "slack_command_duration_seconds"
SLACK_COMMAND_TOTAL = "slack_command_total"

# === Bot Metrics ===

# DataHub GraphQL query metrics from bots
BOT_DATAHUB_QUERY_DURATION = "bot_datahub_query_duration_seconds"
BOT_DATAHUB_QUERY_TOTAL = "bot_datahub_query_total"

# Slack API call metrics
BOT_SLACK_API_DURATION = "bot_slack_api_duration_seconds"
BOT_SLACK_API_TOTAL = "bot_slack_api_total"

# Teams API call metrics
BOT_TEAMS_API_DURATION = "bot_teams_api_duration_seconds"
BOT_TEAMS_API_TOTAL = "bot_teams_api_total"

# OAuth flow metrics
BOT_OAUTH_DURATION = "bot_oauth_duration_seconds"
BOT_OAUTH_TOTAL = "bot_oauth_total"

# Bot metric labels
LABEL_PLATFORM = "platform"  # slack, teams
LABEL_COMMAND = "command"  # search, get, ask, help, mention
LABEL_API_METHOD = "api_method"  # chat.postMessage, bot.send, graph.user, etc.
LABEL_QUERY_TYPE = "query_type"  # search, entity, subscription, incident
LABEL_OAUTH_FLOW = "oauth_flow"  # install, callback, refresh

# === Analytics Metrics ===

ANALYTICS_QUERY_DURATION = "analytics_query_duration_seconds"
ANALYTICS_QUERY_TOTAL = "analytics_query_total"

# === Kafka Metrics ===

KAFKA_OFFSET = "kafka_offset"
KAFKA_MESSAGES_TOTAL = "kafka_messages_total"

# Kafka consumer lag monitoring (from datahub-actions subprocesses)
KAFKA_CONSUMER_LAG = "kafka_consumer_lag"

# Kafka metric labels
LABEL_TOPIC = "topic"
LABEL_PIPELINE_NAME = "pipeline_name"
