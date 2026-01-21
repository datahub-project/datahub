"""
API Models - Pydantic models for request/response validation.

These models define the API contract for the FastAPI application.
"""

from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class ConnectionModeEnum(str, Enum):
    """Connection mode options."""

    QUICKSTART = "quickstart"
    EMBEDDED = "embedded"
    LOCAL_SERVICE = "local_service"
    REMOTE = "remote"
    LOCAL = "local"
    CUSTOM = "custom"
    GRAPHQL_DIRECT = "graphql_direct"


class ConnectionConfigModel(BaseModel):
    """Connection configuration model."""

    mode: ConnectionModeEnum = Field(
        default=ConnectionModeEnum.QUICKSTART, description="Connection mode"
    )
    integrations_url: Optional[str] = Field(
        default="http://localhost:9003",
        description="Integrations service URL (not needed for embedded mode)",
    )
    gms_url: str = Field(default="http://localhost:8080", description="GMS URL")
    gms_token: Optional[str] = Field(
        default=None, description="GMS authentication token"
    )
    kube_namespace: Optional[str] = Field(
        default=None, description="Kubernetes namespace (for remote mode)"
    )
    kube_context: Optional[str] = Field(
        default=None, description="Kubernetes context (for remote mode)"
    )
    pod_name: Optional[str] = Field(
        default=None, description="Pod name (for remote mode)"
    )
    pod_label_selector: str = Field(
        default="app=datahub-integrations",
        description="Pod label selector (for remote mode)",
    )
    local_port: int = Field(default=9003, description="Local port for port forwarding")
    remote_port: int = Field(
        default=9003, description="Remote port for port forwarding"
    )
    aws_region: str = Field(default="us-west-2", description="AWS region")
    aws_profile: Optional[str] = Field(default=None, description="AWS profile")
    name: Optional[str] = Field(default=None, description="Configuration name")
    description: Optional[str] = Field(
        default=None, description="Configuration description"
    )


class MessageModel(BaseModel):
    """Message model."""

    id: Optional[str] = Field(default=None, description="Message ID")
    role: str = Field(..., description="Message role (user or assistant)")
    content: str = Field(..., description="Message content")
    timestamp: datetime = Field(
        default_factory=datetime.now, description="Message timestamp"
    )
    duration: Optional[float] = Field(
        default=None, description="Processing duration (for assistant messages)"
    )
    event_count: Optional[int] = Field(
        default=None, description="Number of events (for assistant messages)"
    )
    success: Optional[bool] = Field(
        default=None, description="Success status (for assistant messages)"
    )
    is_auto: bool = Field(default=False, description="Auto-generated message flag")


class ConversationModel(BaseModel):
    """Conversation model."""

    id: str = Field(..., description="Conversation ID")
    urn: str = Field(..., description="Conversation URN")
    title: str = Field(default="New conversation", description="Conversation title")
    messages: List[MessageModel] = Field(
        default_factory=list, description="List of messages"
    )
    created_at: datetime = Field(
        default_factory=datetime.now, description="Creation timestamp"
    )


class CreateConversationRequest(BaseModel):
    """Request to create a new conversation."""

    title: str = Field(default="New conversation", description="Conversation title")


class SendMessageRequest(BaseModel):
    """Request to send a message."""

    message: str = Field(..., description="Message text", min_length=1)
    is_auto: bool = Field(default=False, description="Auto-generated message flag")


class SendMessageResponse(BaseModel):
    """Response after sending a message."""

    success: bool = Field(..., description="Success status")
    error: Optional[str] = Field(default=None, description="Error message if failed")
    duration: float = Field(..., description="Processing duration in seconds")
    message_id: Optional[str] = Field(default=None, description="Generated message ID")


class HealthResponse(BaseModel):
    """Health check response."""

    status: str = Field(..., description="Health status")
    timestamp: datetime = Field(
        default_factory=datetime.now, description="Check timestamp"
    )
    details: Optional[Dict[str, Any]] = Field(
        default=None, description="Additional details"
    )


class DataHubHealthResponse(BaseModel):
    """DataHub connection health response."""

    connected: bool = Field(..., description="Connection status")
    gms_url: str = Field(..., description="GMS URL being checked")
    error: Optional[str] = Field(default=None, description="Error message if failed")
    timestamp: datetime = Field(
        default_factory=datetime.now, description="Check timestamp"
    )


class UpdateConfigRequest(BaseModel):
    """Request to update configuration."""

    config: ConnectionConfigModel = Field(..., description="New configuration")


class TestConnectionRequest(BaseModel):
    """Request to test a connection."""

    config: ConnectionConfigModel = Field(..., description="Configuration to test")


class TestConnectionResponse(BaseModel):
    """Response from connection test."""

    success: bool = Field(..., description="Test result")
    message: str = Field(..., description="Result message")
    details: Optional[Dict[str, Any]] = Field(
        default=None, description="Additional details"
    )


class ProfileModel(BaseModel):
    """DataHub instance profile - represents WHERE to connect."""

    name: str = Field(..., description="Profile name (unique identifier)")
    description: Optional[str] = Field(default=None, description="Profile description")
    gms_url: str = Field(..., description="DataHub GMS URL")
    gms_token: Optional[str] = Field(
        default=None, description="GMS authentication token"
    )

    # Optional kubectl info (used when connection_mode = remote)
    kube_context: Optional[str] = Field(default=None, description="Kubernetes context")
    kube_namespace: Optional[str] = Field(
        default=None, description="Kubernetes namespace"
    )

    # Status
    is_active: bool = Field(
        default=False, description="Whether this profile is currently active"
    )
    is_readonly: bool = Field(
        default=False,
        description="Whether this profile is read-only (from external source)",
    )
    source: Optional[str] = Field(
        default=None, description="Source of the profile (e.g., 'datahubenv', 'user')"
    )

    # Token status
    token_expires_at: Optional[str] = Field(
        default=None, description="Token expiration timestamp (ISO format)"
    )
    token_expired: Optional[bool] = Field(
        default=None, description="Whether the token has expired"
    )
    token_expiring_soon: Optional[bool] = Field(
        default=None, description="Whether the token expires within 24 hours"
    )

    # Metadata
    created_at: Optional[str] = Field(default=None, description="Creation timestamp")
    updated_at: Optional[str] = Field(default=None, description="Last update timestamp")


class CreateProfileRequest(BaseModel):
    """Request to create/update a profile."""

    name: str = Field(..., description="Profile name")
    description: Optional[str] = Field(default=None, description="Profile description")
    gms_url: str = Field(..., description="DataHub GMS URL")
    gms_token: Optional[str] = Field(
        default=None, description="GMS authentication token"
    )
    kube_context: Optional[str] = Field(default=None, description="Kubernetes context")
    kube_namespace: Optional[str] = Field(
        default=None, description="Kubernetes namespace"
    )


class ArchivedMessageModel(BaseModel):
    """Message in an archived conversation."""

    role: str = Field(..., description="Message role (user or assistant)")
    content: str = Field(..., description="Message content")
    timestamp: int = Field(..., description="Message timestamp (milliseconds)")
    message_type: str = Field(
        default="TEXT",
        description="Message type (TEXT, THINKING, TOOL_CALL, TOOL_RESULT)",
    )
    agent_name: Optional[str] = Field(
        default=None, description="Agent that generated this message"
    )
    user_name: Optional[str] = Field(
        default=None, description="Username extracted from actor URN"
    )
    actor_urn: Optional[str] = Field(
        default=None, description="Full actor URN (e.g., urn:li:corpuser:admin)"
    )


class ConversationHealthStatus(BaseModel):
    """Health and quality metrics for a conversation."""

    is_abandoned: bool = Field(
        default=False,
        description="True if last turn has no response or incomplete response",
    )
    abandonment_reason: Optional[str] = Field(
        default=None,
        description="Reason for abandonment: 'no_response_at_all', 'incomplete_response', etc.",
    )
    unanswered_questions_count: int = Field(
        default=0,
        description="Number of user questions without assistant TEXT responses",
    )
    completion_rate: float = Field(
        default=1.0,
        description="Percentage of turns with complete responses (0.0 to 1.0)",
    )
    has_errors: bool = Field(
        default=False,
        description="True if conversation contains error messages or failed tool calls",
    )
    last_message_role: Optional[str] = Field(
        default=None, description="Role of last message: 'user' or 'assistant'"
    )


class ArchivedConversationModel(BaseModel):
    """Archived conversation from DataHub."""

    id: str = Field(..., description="Conversation URN (used as ID)")
    urn: str = Field(..., description="Conversation URN")
    title: str = Field(..., description="Conversation title")
    messages: List[ArchivedMessageModel] = Field(
        default_factory=list, description="Conversation messages"
    )
    created_at: float = Field(..., description="Creation timestamp (seconds)")
    updated_at: float = Field(..., description="Last update timestamp (seconds)")
    origin_type: str = Field(..., description="Origin type (DATAHUB_UI, SLACK, TEAMS)")
    slack_conversation_type: Optional[str] = Field(
        default=None,
        description="Slack conversation type (channel, dm, private_channel) - only for SLACK origin",
    )
    message_count: int = Field(..., description="Number of messages")
    context: Optional[Dict[str, Any]] = Field(
        default=None, description="Conversation context"
    )
    is_archived: bool = Field(
        default=True, description="Always true for archived conversations"
    )
    max_thinking_time_ms: int = Field(
        default=0, description="Maximum thinking time across all turns in milliseconds"
    )
    num_turns: int = Field(default=0, description="Number of conversation turns")
    telemetry: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Telemetry data with interaction events (for Slack/Teams conversations)",
    )
    health_status: Optional[ConversationHealthStatus] = Field(
        default=None,
        description="Computed health and quality metrics for this conversation",
    )


class ArchivedConversationListModel(BaseModel):
    """List of archived conversations with pagination."""

    conversations: List[ArchivedConversationModel] = Field(default_factory=list)
    total: int = Field(..., description="Total number of archived conversations")
    start: int = Field(..., description="Starting offset")
    count: int = Field(..., description="Number of results returned")


# Telemetry Models


class ToolCallModel(BaseModel):
    """Tool call execution details."""

    tool_name: str = Field(..., description="Name of the tool that was called")
    tool_input: Optional[Dict[str, Any]] = Field(
        default=None, description="Input arguments passed to the tool"
    )
    execution_duration_sec: Optional[float] = Field(
        default=None,
        description="Tool execution duration in seconds (null if not available)",
    )
    result_length: Optional[int] = Field(
        default=None, description="Length of the tool result in characters"
    )
    is_error: bool = Field(
        ..., description="Whether the tool call resulted in an error"
    )
    error: Optional[str] = Field(
        default=None, description="Error message if the tool call failed"
    )
    timestamp: int = Field(..., description="Tool call timestamp (epoch milliseconds)")


class InteractionEventModel(BaseModel):
    """ChatbotInteraction event with full conversation history."""

    message_id: str = Field(..., description="Message ID")
    timestamp: int = Field(..., description="Event timestamp (epoch milliseconds)")
    num_tool_calls: int = Field(..., description="Number of tool calls in this turn")
    response_generation_duration_sec: float = Field(
        ..., description="Response generation time for this turn"
    )
    full_history: str = Field(
        ..., description="Complete conversation history JSON for this turn"
    )


class ConversationTelemetryModel(BaseModel):
    """Telemetry data for a conversation."""

    num_tool_calls: int = Field(..., description="Total number of tool calls")
    num_tool_call_errors: int = Field(
        ..., description="Number of tool calls that resulted in errors"
    )
    tool_calls: List[ToolCallModel] = Field(
        default_factory=list, description="List of all tool call executions"
    )
    interaction_events: List[InteractionEventModel] = Field(
        default_factory=list,
        description="ChatbotInteraction events with full_history for reconstruction",
    )
    avg_response_time: Optional[float] = Field(
        default=None,
        description="Average response generation time in seconds (from interaction events)",
    )
    total_thinking_time: Optional[float] = Field(
        default=None,
        description="Total thinking/processing time across all turns in seconds",
    )


class ArchivedConversationWithTelemetryModel(ArchivedConversationModel):
    """Archived conversation enriched with telemetry data."""

    telemetry: Optional[ConversationTelemetryModel] = Field(
        default=None, description="Tool call telemetry data (optional)"
    )


class ToolUsageAggregateModel(BaseModel):
    """Aggregate tool usage statistics."""

    tool_counts: Dict[str, int] = Field(
        default_factory=dict,
        description="Number of calls per tool (tool_name -> count)",
    )
    tool_errors: Dict[str, int] = Field(
        default_factory=dict,
        description="Number of errors per tool (tool_name -> error_count)",
    )
    avg_duration: Dict[str, float] = Field(
        default_factory=dict,
        description="Average execution duration per tool in seconds (tool_name -> avg_seconds)",
    )


class ClusterInfo(BaseModel):
    """Cluster information with context and namespace."""

    context: str = Field(..., description="Full kubectl context ARN")
    context_name: str = Field(
        ..., description="Shortened cluster name (e.g., 'usw2-saas-01-prod')"
    )
    namespace: str = Field(..., description="Kubernetes namespace")
    customer_name: Optional[str] = Field(
        default=None, description="Customer name extracted from namespace"
    )
    cluster_region: Optional[str] = Field(
        default=None, description="AWS region (e.g., 'us-west-2')"
    )
    cluster_env: Optional[str] = Field(
        default=None, description="Environment: 'prod', 'staging', or 'poc'"
    )
    is_trial: bool = Field(default=False, description="True for trial clusters")


class ClusterIndexResponse(BaseModel):
    """Response containing searchable cluster index."""

    clusters: List[ClusterInfo] = Field(
        default_factory=list, description="List of clusters"
    )
    total: int = Field(..., description="Total number of clusters")
    mode: str = Field(..., description="Filter mode: 'all' or 'trials'")
    cached: bool = Field(..., description="Whether results are from cache")
    cache_age_seconds: Optional[int] = Field(
        default=None, description="Cache age in seconds"
    )
    error: Optional[str] = Field(
        default=None, description="Error message if cluster discovery failed"
    )
    vpn_required: bool = Field(
        default=False, description="True if VPN connection is required"
    )


# Search Models


class SearchRequest(BaseModel):
    """Search request model."""

    query: str = Field(..., description="Search query text", min_length=1)
    start: int = Field(default=0, description="Starting offset for pagination")
    count: int = Field(default=20, description="Number of results to return")
    types: List[str] = Field(
        default_factory=list, description="Entity types to search (empty = all)"
    )


class MatchedField(BaseModel):
    """Matched field in search result."""

    name: str = Field(..., description="Field name")
    value: str = Field(..., description="Field value")


class SearchResultEntity(BaseModel):
    """Entity data in search result."""

    urn: str = Field(..., description="Entity URN")
    type: str = Field(..., description="Entity type")
    name: Optional[str] = Field(None, description="Entity name")
    properties: Dict[str, Any] = Field(
        default_factory=dict, description="Additional entity properties"
    )


class SearchResultItem(BaseModel):
    """Individual search result."""

    entity: SearchResultEntity = Field(..., description="Entity data")
    matchedFields: List[MatchedField] = Field(
        default_factory=list, description="Fields that matched"
    )
    score: Optional[float] = Field(None, description="Relevance score")


class SearchResponse(BaseModel):
    """Search response model."""

    start: int = Field(..., description="Starting offset")
    count: int = Field(..., description="Number of results returned")
    total: int = Field(..., description="Total number of results")
    searchResults: List[SearchResultItem] = Field(
        default_factory=list, description="Search results"
    )


class ExplainRequest(BaseModel):
    """Explain request model."""

    query: str = Field(..., description="Search query")
    documentId: str = Field(..., description="Document URN to explain")
    entityName: str = Field(default="dataset", description="Entity type")


class ExplainResponse(BaseModel):
    """Explain response model."""

    index: str = Field(..., description="Index name")
    documentId: str = Field(..., description="Document ID")
    matched: bool = Field(..., description="Whether document matched")
    score: Optional[float] = Field(None, description="Relevance score")
    explanation: Dict[str, Any] = Field(
        default_factory=dict, description="Score explanation"
    )


# AI Ranking Analysis Models


class RankingAnalysisItem(BaseModel):
    """Single result for ranking analysis."""

    urn: str = Field(..., description="Entity URN")
    type: str = Field(..., description="Entity type")
    name: str = Field(..., description="Entity name")
    position: int = Field(..., description="Position in search results (0-indexed)")
    score: Optional[float] = Field(None, description="Search score")
    matched: bool = Field(..., description="Whether document matched")
    explanation: Dict[str, Any] = Field(
        default_factory=dict, description="Elasticsearch explanation"
    )


class RankingAnalysisRequest(BaseModel):
    """Request to analyze ranking of selected results."""

    query: str = Field(..., description="Search query", min_length=1)
    results: List[RankingAnalysisItem] = Field(
        ..., description="Selected results to analyze", min_items=1, max_items=20
    )


class RankingAnalysisResponse(BaseModel):
    """Response with AI-generated ranking analysis."""

    analysis: str = Field(..., description="AI-generated analysis text")
    model: str = Field(..., description="Model used for analysis")
    tokens: Dict[str, int] = Field(default_factory=dict, description="Token usage")


class SearchConfigResponse(BaseModel):
    """Search configuration and field boosts."""

    field_weights: Dict[str, float] = Field(
        default_factory=dict, description="Field boost scores"
    )
    config_notes: str = Field(
        ..., description="Configuration details and search behavior"
    )
