"""
API Models - Pydantic models for request/response validation.

These models define the API contract for the FastAPI application.
"""

from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional, Union

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
    searchType: str = Field(
        default="DFS_QUERY_THEN_FETCH",
        description="Elasticsearch search type: QUERY_THEN_FETCH or DFS_QUERY_THEN_FETCH",
    )
    functionScoreOverride: Optional[str] = Field(
        default=None,
        description="Function score override JSON for debugging Stage 1 ranking (UI-based override)",
    )
    rescoreEnabled: Optional[bool] = Field(
        default=None,
        description="Enable/disable Stage 2 rescore for this query (null = use global config)",
    )
    rescoreFormulaOverride: Optional[str] = Field(
        default=None,
        description="Override the rescore formula expression (exp4j syntax)",
    )
    rescoreSignalsOverride: Optional[str] = Field(
        default=None,
        description="Override rescore signal definitions as JSON",
    )


class MatchedField(BaseModel):
    """Matched field in search result."""

    name: str = Field(..., description="Field name")
    value: str = Field(..., description="Field value")


class CustomProperty(BaseModel):
    """Custom property key-value pair."""

    key: str = Field(..., description="Property key")
    value: str = Field(..., description="Property value")


class SearchResultEntity(BaseModel):
    """Entity data in search result."""

    urn: str = Field(..., description="Entity URN")
    type: str = Field(..., description="Entity type")
    name: Optional[str] = Field(None, description="Entity name")
    properties: Dict[str, Any] = Field(
        default_factory=dict, description="Additional entity properties"
    )
    editableProperties: Dict[str, Any] = Field(
        default_factory=dict,
        description="User-edited properties from DataHub UI",
    )
    # Ranking-relevant metadata fields
    tags: List[str] = Field(
        default_factory=list, description="Tags assigned to this entity"
    )
    glossaryTerms: List[str] = Field(
        default_factory=list, description="Glossary terms assigned to this entity"
    )
    domain: Optional[str] = Field(None, description="Domain this entity belongs to")
    subTypes: List[str] = Field(
        default_factory=list, description="Entity subtypes (e.g., 'Model' for dbt)"
    )
    browsePath: List[str] = Field(
        default_factory=list, description="Browse path hierarchy"
    )
    customProperties: List[CustomProperty] = Field(
        default_factory=list, description="Custom properties"
    )


class SignalExplanationModel(BaseModel):
    """Individual signal explanation from Stage 2 rescore."""

    name: str = Field(..., description="Signal name")
    rawValue: Optional[Any] = Field(None, description="Raw value before normalization")
    numericValue: float = Field(0.0, description="Numeric value")
    normalizedValue: float = Field(1.0, description="Value after normalization")
    boost: float = Field(1.0, description="Boost exponent applied")
    contribution: float = Field(1.0, description="Final contribution (pow(normalized, boost))")


class RescoreExplanationModel(BaseModel):
    """Stage 2 rescore explanation with signal breakdown."""

    model_config = {"extra": "allow"}

    documentId: Optional[str] = Field(None, description="Document URN")
    bm25Score: float = Field(0.0, description="Original ES/BM25 score")
    rescoreValue: float = Field(0.0, description="Raw rescore formula result")
    rescoreBoost: float = Field(0.0, description="Dynamic boost for rescored items")
    finalScore: float = Field(0.0, description="rescoreValue + rescoreBoost")
    formula: str = Field("", description="exp4j formula used")
    signals: Optional[
        Union[List[SignalExplanationModel], Dict[str, SignalExplanationModel]]
    ] = Field(None, description="Signal breakdown (array or map)")


class SearchResultItem(BaseModel):
    """Individual search result."""

    entity: SearchResultEntity = Field(..., description="Entity data")
    matchedFields: List[MatchedField] = Field(
        default_factory=list, description="Fields that matched"
    )
    score: Optional[float] = Field(None, description="Relevance score")
    explanation: Optional[Dict[str, Any]] = Field(
        None,
        description="Elasticsearch scoring explanation (embedded from search response)",
    )
    rescoreExplanation: Optional[RescoreExplanationModel] = Field(
        None,
        description="Java/exp4j Stage 2 rescore explanation with signal breakdown",
    )


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
    score: Optional[float] = Field(
        None, description="Production-accurate relevance score (multi-index)"
    )
    singleIndexScore: Optional[float] = Field(
        None, description="Single-index explain score (for comparison)"
    )
    searchedEntityTypes: List[str] = Field(
        default_factory=list, description="Entity types searched for IDF calculation"
    )
    explanation: Dict[str, Any] = Field(
        default_factory=dict, description="Score explanation"
    )


# AI Ranking Analysis Models


class RankingAnalysisItem(BaseModel):
    """Single result for ranking analysis."""

    urn: str = Field(..., description="Entity URN")
    type: str = Field(..., description="Entity type")
    name: str = Field(..., description="Entity name")
    position: int = Field(..., description="Position in search results (1-indexed)")
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


# Full Search Configuration Models (Stage 1 + Stage 2)


class Stage1PresetModel(BaseModel):
    """Stage 1 (function score) preset configuration."""

    name: str = Field(..., description="Preset name")
    description: str = Field(..., description="Preset description")
    config: str = Field(..., description="JSON configuration string")


class Stage1ConfigurationModel(BaseModel):
    """Stage 1 (function score) configuration."""

    source: str = Field(default="pdl", description="Configuration source (pdl or yaml)")
    functionScore: Optional[str] = Field(
        None, description="Function score JSON if available"
    )
    presets: List[Stage1PresetModel] = Field(
        default_factory=list, description="Available presets"
    )


class SignalNormalizationModel(BaseModel):
    """Signal normalization configuration."""

    type: str = Field(..., description="Normalization type")
    inputMin: Optional[float] = Field(None, description="Input minimum value")
    inputMax: Optional[float] = Field(None, description="Input maximum value")
    steepness: Optional[float] = Field(None, description="Sigmoid steepness")
    outputMin: Optional[float] = Field(None, description="Output minimum value")
    outputMax: Optional[float] = Field(None, description="Output maximum value")
    trueValue: Optional[float] = Field(None, description="Boolean true value")
    falseValue: Optional[float] = Field(None, description="Boolean false value")
    scale: Optional[float] = Field(None, description="Linear decay scale")


class SignalConfigModel(BaseModel):
    """Individual signal configuration."""

    name: str = Field(..., description="Signal name")
    normalizedName: str = Field(..., description="Normalized variable name")
    fieldPath: str = Field(..., description="Elasticsearch field path")
    type: str = Field(..., description="Signal type")
    boost: float = Field(..., description="Signal boost exponent")
    normalization: SignalNormalizationModel = Field(
        ..., description="Normalization config"
    )


class Stage2PresetModel(BaseModel):
    """Stage 2 (rescore) preset configuration."""

    name: str = Field(..., description="Preset name")
    description: str = Field(..., description="Preset description")
    config: str = Field(..., description="JSON configuration string")


class Stage2ConfigurationModel(BaseModel):
    """Stage 2 (rescore) configuration."""

    enabled: bool = Field(..., description="Whether Stage 2 rescoring is enabled")
    mode: str = Field(
        default="JAVA_EXP4J", description="Rescore mode (JAVA_EXP4J, etc.)"
    )
    windowSize: int = Field(..., description="Number of top results to rescore")
    formula: str = Field(..., description="Rescore formula")
    signals: List[SignalConfigModel] = Field(
        default_factory=list, description="Signal configurations"
    )
    presets: List[Stage2PresetModel] = Field(
        default_factory=list, description="Available presets"
    )


class SearchConfigurationModel(BaseModel):
    """Complete search configuration (Stage 1 + Stage 2)."""

    stage1: Stage1ConfigurationModel = Field(..., description="Stage 1 configuration")
    stage2: Stage2ConfigurationModel = Field(..., description="Stage 2 configuration")


# Search Debug API Models


class SearchDebugRequest(BaseModel):
    """Request for debug search with full stage explanations."""

    query: str = Field(..., description="Search query text", min_length=1)
    count: int = Field(default=20, description="Number of results to return")
    types: List[str] = Field(
        default_factory=list, description="Entity types to search (empty = all)"
    )
    searchType: str = Field(
        default="DFS_QUERY_THEN_FETCH",
        description="Elasticsearch search type: QUERY_THEN_FETCH or DFS_QUERY_THEN_FETCH",
    )
    includeStage1Explain: bool = Field(
        default=True, description="Include Stage 1 (ES BM25) explanation"
    )
    includeStage2Explain: bool = Field(
        default=True, description="Include Stage 2 (rescore) explanation"
    )
    rescoreEnabled: Optional[bool] = Field(
        default=None, description="Enable/disable Stage 2 rescore (null = use config)"
    )
    rescoreFormulaOverride: Optional[str] = Field(
        default=None, description="Override the rescore formula expression (exp4j syntax)"
    )
    rescoreSignalsOverride: Optional[str] = Field(
        default=None, description="Override rescore signal definitions as JSON"
    )
    rescoreOverride: Optional[str] = Field(
        default=None,
        description="Legacy rescore override JSON blob (deprecated, use formula/signals overrides)",
    )
    functionScoreOverride: Optional[str] = Field(
        default=None, description="Function score override JSON for Stage 1"
    )


class SignalDebugInfo(BaseModel):
    """Debug information for a single signal."""

    name: str = Field(..., description="Signal name")
    rawValue: Optional[Any] = Field(None, description="Raw value from document")
    numericValue: float = Field(
        default=0.0, description="Numeric value for computation"
    )
    normalizedValue: float = Field(default=1.0, description="Normalized value")
    boost: float = Field(default=1.0, description="Boost exponent")
    contribution: float = Field(
        default=1.0, description="Final contribution: pow(normalized, boost)"
    )


class Stage2DebugInfo(BaseModel):
    """Stage 2 rescore debug information."""

    formula: Optional[str] = Field(None, description="exp4j formula used")
    rescoreValue: float = Field(..., description="Raw rescore value from formula")
    rescoreBoost: float = Field(..., description="Power-of-10 boost applied")
    signals: List[SignalDebugInfo] = Field(
        default_factory=list, description="Signal breakdown"
    )


class Stage3DebugInfo(BaseModel):
    """Stage 3 boost debug information."""

    withinRescoreWindow: bool = Field(
        ..., description="Whether result was within rescore window"
    )
    boostApplied: float = Field(..., description="Boost value applied (power of 10)")
    reason: str = Field(..., description="Explanation of boost")


class DebugScores(BaseModel):
    """Score breakdown for a debug result."""

    stage1Score: float = Field(
        ..., description="Stage 1 ES score (BM25 + function scores)"
    )
    stage2RescoreValue: Optional[float] = Field(
        None, description="Stage 2 rescore value (before boost)"
    )
    stage2Boost: Optional[float] = Field(None, description="Stage 2 power-of-10 boost")
    finalScore: float = Field(..., description="Final score after all stages")


class EntityMetadata(BaseModel):
    """Comprehensive entity metadata for debugging."""

    # Basic info
    platform: Optional[str] = Field(None, description="Data platform name")
    description: Optional[str] = Field(None, description="Entity description")

    # Usage stats (signal sources)
    viewCount: Optional[int] = Field(None, description="Number of views")
    queryCount: Optional[int] = Field(None, description="Number of queries")
    uniqueUserCount: Optional[int] = Field(None, description="Number of unique users")

    # Quality signals
    hasDescription: Optional[bool] = Field(
        None, description="Whether entity has description"
    )
    hasOwners: Optional[bool] = Field(None, description="Whether entity has owners")
    deprecated: Optional[bool] = Field(None, description="Whether entity is deprecated")

    # Additional context
    owners: List[str] = Field(default_factory=list, description="Owner URNs or names")
    tags: List[str] = Field(default_factory=list, description="Tag names")
    glossaryTerms: List[str] = Field(
        default_factory=list, description="Glossary term names"
    )
    domain: Optional[str] = Field(None, description="Domain name")

    # Raw properties for debugging
    properties: Dict[str, Any] = Field(
        default_factory=dict, description="Raw entity properties"
    )


class DebugResultItem(BaseModel):
    """Individual result in debug response."""

    rank: int = Field(..., description="Position in results (1-indexed)")
    urn: str = Field(..., description="Entity URN")
    name: Optional[str] = Field(None, description="Entity name")
    description: Optional[str] = Field(None, description="Entity description")
    type: str = Field(..., description="Entity type")
    scores: DebugScores = Field(..., description="Score breakdown")
    stage1Explanation: Optional[Dict[str, Any]] = Field(
        None, description="Stage 1 ES explain output"
    )
    stage2Explanation: Optional[Stage2DebugInfo] = Field(
        None, description="Stage 2 rescore breakdown"
    )
    stage3Explanation: Optional[Stage3DebugInfo] = Field(
        None, description="Stage 3 boost explanation"
    )
    matchedFields: List[MatchedField] = Field(
        default_factory=list, description="Fields that matched the query"
    )
    entity: EntityMetadata = Field(
        default_factory=EntityMetadata, description="Comprehensive entity metadata"
    )


class DebugTiming(BaseModel):
    """Timing breakdown for debug search."""

    totalMs: float = Field(..., description="Total request time in milliseconds")
    gmsCallMs: float = Field(..., description="Time spent calling GMS")
    processingMs: float = Field(..., description="Time spent processing results")


class DebugConfiguration(BaseModel):
    """Configuration used for this search."""

    rescoreEnabled: bool = Field(..., description="Whether Stage 2 rescore was enabled")
    rescoreWindowSize: int = Field(..., description="Number of results rescored")
    rescoreFormula: Optional[str] = Field(None, description="Rescore formula used")
    signalCount: int = Field(default=0, description="Number of signals configured")


class SignalExtractionSummary(BaseModel):
    """Summary of signal extraction across all results."""

    signalName: str = Field(..., description="Signal name")
    extractedCount: int = Field(..., description="Number of results with this signal")
    avgRawValue: Optional[float] = Field(None, description="Average raw value")
    avgNormalizedValue: float = Field(..., description="Average normalized value")
    avgContribution: float = Field(..., description="Average contribution")


class DebugSummary(BaseModel):
    """Summary statistics for debug search."""

    avgStage1Score: float = Field(..., description="Average Stage 1 score")
    avgFinalScore: float = Field(..., description="Average final score")
    rescoreWindowUsed: int = Field(..., description="Actual rescore window used")
    signalStats: List[SignalExtractionSummary] = Field(
        default_factory=list, description="Per-signal statistics"
    )


class SearchDebugResponse(BaseModel):
    """Response for debug search with full explanations."""

    query: str = Field(..., description="Query that was executed")
    totalResults: int = Field(..., description="Total matching results")
    returnedResults: int = Field(..., description="Number of results returned")
    timing: DebugTiming = Field(..., description="Timing breakdown")
    configuration: DebugConfiguration = Field(..., description="Configuration used")
    results: List[DebugResultItem] = Field(
        default_factory=list, description="Debug results with explanations"
    )
    summary: DebugSummary = Field(..., description="Summary statistics")


# Validation API Models


class RankExpectation(BaseModel):
    """Expected rank for a specific entity."""

    urn: str = Field(..., description="Entity URN")
    expectedRank: int = Field(..., description="Expected rank position (1-indexed)")
    tolerance: int = Field(
        default=0, description="Allowed deviation from expected rank"
    )


class AssertionType(str, Enum):
    """Types of assertions for validation."""

    RANK_EQUALS = "RANK_EQUALS"
    RANK_WITHIN = "RANK_WITHIN"
    SCORE_ABOVE = "SCORE_ABOVE"
    SCORE_BELOW = "SCORE_BELOW"
    SIGNAL_PRESENT = "SIGNAL_PRESENT"
    RANK_BEFORE = "RANK_BEFORE"


class ValidationAssertion(BaseModel):
    """A single validation assertion."""

    type: AssertionType = Field(..., description="Assertion type")
    urn: str = Field(..., description="Entity URN to check")
    threshold: Optional[float] = Field(
        None, description="Threshold for score assertions"
    )
    signal: Optional[str] = Field(None, description="Signal name for SIGNAL_PRESENT")
    expectedValue: Optional[Any] = Field(None, description="Expected value")
    beforeUrn: Optional[str] = Field(
        None, description="URN that should rank lower (for RANK_BEFORE)"
    )


class ValidateRequest(BaseModel):
    """Request to validate search ranking."""

    query: str = Field(..., description="Search query", min_length=1)
    count: int = Field(default=50, description="Number of results to check")
    types: List[str] = Field(default_factory=list, description="Entity types to search")
    expectations: List[RankExpectation] = Field(
        default_factory=list, description="Expected rankings"
    )
    assertions: List[ValidationAssertion] = Field(
        default_factory=list, description="Additional assertions"
    )


class AssertionResult(BaseModel):
    """Result of a single assertion."""

    type: str = Field(..., description="Assertion type")
    urn: str = Field(..., description="Entity URN checked")
    passed: bool = Field(..., description="Whether assertion passed")
    expected: Optional[Any] = Field(None, description="Expected value")
    actual: Optional[Any] = Field(None, description="Actual value")
    tolerance: Optional[int] = Field(None, description="Tolerance used")
    message: str = Field(..., description="Human-readable result message")


class ValidateResponse(BaseModel):
    """Response from ranking validation."""

    passed: bool = Field(..., description="Whether all assertions passed")
    totalAssertions: int = Field(..., description="Total number of assertions")
    passedAssertions: int = Field(..., description="Number of passed assertions")
    failedAssertions: int = Field(..., description="Number of failed assertions")
    results: List[AssertionResult] = Field(
        default_factory=list, description="Individual assertion results"
    )
    searchResults: List[DebugResultItem] = Field(
        default_factory=list, description="Search results used for validation"
    )


# Compare API Models


class CompareSearchConfig(BaseModel):
    """Configuration for one side of a comparison."""

    query: str = Field(..., description="Search query")
    rescoreEnabled: Optional[bool] = Field(None, description="Enable/disable rescore")
    rescoreFormulaOverride: Optional[str] = Field(
        None, description="Rescore formula override"
    )
    rescoreSignalsOverride: Optional[str] = Field(
        None, description="Rescore signals override JSON"
    )
    rescoreOverride: Optional[str] = Field(
        None,
        description="Legacy rescore override JSON blob (deprecated, use formula/signals overrides)",
    )
    functionScoreOverride: Optional[str] = Field(
        None, description="Function score override"
    )
    label: str = Field(default="", description="Label for this configuration")


class CompareRequest(BaseModel):
    """Request to compare two search configurations."""

    baseline: CompareSearchConfig = Field(..., description="Baseline configuration")
    experiment: CompareSearchConfig = Field(..., description="Experiment configuration")
    count: int = Field(default=20, description="Number of results to compare")
    types: List[str] = Field(default_factory=list, description="Entity types to search")


class PositionChange(BaseModel):
    """Position change for an entity between baseline and experiment."""

    urn: str = Field(..., description="Entity URN")
    name: Optional[str] = Field(None, description="Entity name")
    baselineRank: int = Field(..., description="Rank in baseline")
    experimentRank: int = Field(..., description="Rank in experiment")
    change: int = Field(..., description="Rank change (positive = improved)")
    baselineScore: float = Field(..., description="Score in baseline")
    experimentScore: float = Field(..., description="Score in experiment")


class SignalImpact(BaseModel):
    """Impact of a signal across baseline vs experiment."""

    signalName: str = Field(..., description="Signal name")
    avgBaselineContribution: float = Field(
        ..., description="Avg contribution in baseline"
    )
    avgExperimentContribution: float = Field(
        ..., description="Avg contribution in experiment"
    )
    changePercent: float = Field(..., description="Percentage change")


class ComparisonMetrics(BaseModel):
    """Ranking correlation metrics."""

    kendallTau: float = Field(..., description="Kendall's tau correlation (-1 to 1)")
    spearmanRho: float = Field(..., description="Spearman's rho correlation (-1 to 1)")
    positionChanges: List[PositionChange] = Field(
        default_factory=list, description="Entities with rank changes"
    )
    newInTopK: List[str] = Field(
        default_factory=list, description="URNs new in experiment top-K"
    )
    droppedFromTopK: List[str] = Field(
        default_factory=list, description="URNs dropped from top-K"
    )
    unchanged: int = Field(..., description="Number of entities with same rank")


class CompareResponse(BaseModel):
    """Response from search comparison."""

    baseline: CompareSearchConfig = Field(..., description="Baseline config used")
    experiment: CompareSearchConfig = Field(..., description="Experiment config used")
    comparison: ComparisonMetrics = Field(..., description="Ranking comparison metrics")
    signalImpact: List[SignalImpact] = Field(
        default_factory=list, description="Per-signal impact analysis"
    )


# Signals API Models


class SignalNormalizationInfo(BaseModel):
    """Normalization configuration for a signal."""

    type: str = Field(..., description="Normalization type")
    inputMin: Optional[float] = Field(None, description="Min input value")
    inputMax: Optional[float] = Field(None, description="Max input value")
    steepness: Optional[float] = Field(None, description="Sigmoid steepness")
    scale: Optional[float] = Field(None, description="Decay scale (days)")
    outputMin: float = Field(..., description="Min output value")
    outputMax: float = Field(..., description="Max output value")
    trueValue: Optional[float] = Field(None, description="Boolean true value")
    falseValue: Optional[float] = Field(None, description="Boolean false value")


class SignalInfo(BaseModel):
    """Information about a configured signal."""

    name: str = Field(..., description="Signal name")
    normalizedName: str = Field(..., description="Variable name in formula")
    fieldPath: str = Field(..., description="Document field path")
    type: str = Field(
        ..., description="Signal type (SCORE, NUMERIC, BOOLEAN, TIMESTAMP)"
    )
    description: Optional[str] = Field(None, description="Signal description")
    normalization: SignalNormalizationInfo = Field(
        ..., description="Normalization config"
    )
    boost: float = Field(..., description="Boost exponent")


class SignalsResponse(BaseModel):
    """Response with available signals."""

    signals: List[SignalInfo] = Field(
        default_factory=list, description="Configured signals"
    )
    formula: Optional[str] = Field(None, description="Current rescore formula")
    normalizationTypes: List[str] = Field(
        default_factory=list, description="Available normalization types"
    )


class DebugConfigResponse(BaseModel):
    """Full debug configuration response."""

    rescore: DebugConfiguration = Field(..., description="Rescore configuration")
    search: Dict[str, Any] = Field(
        default_factory=dict, description="Search configuration"
    )
    fieldWeights: Dict[str, float] = Field(
        default_factory=dict, description="Field boost weights"
    )
