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
        default="http://localhost:9003", description="Integrations service URL (not needed for embedded mode)"
    )
    gms_url: str = Field(default="http://localhost:8080", description="GMS URL")
    gms_token: Optional[str] = Field(default=None, description="GMS authentication token")
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
    gms_token: Optional[str] = Field(default=None, description="GMS authentication token")

    # Optional kubectl info (used when connection_mode = remote)
    kube_context: Optional[str] = Field(default=None, description="Kubernetes context")
    kube_namespace: Optional[str] = Field(default=None, description="Kubernetes namespace")

    # Status
    is_active: bool = Field(default=False, description="Whether this profile is currently active")
    is_readonly: bool = Field(default=False, description="Whether this profile is read-only (from external source)")
    source: Optional[str] = Field(default=None, description="Source of the profile (e.g., 'datahubenv', 'user')")

    # Token status
    token_expires_at: Optional[str] = Field(default=None, description="Token expiration timestamp (ISO format)")
    token_expired: Optional[bool] = Field(default=None, description="Whether the token has expired")
    token_expiring_soon: Optional[bool] = Field(default=None, description="Whether the token expires within 24 hours")

    # Metadata
    created_at: Optional[str] = Field(default=None, description="Creation timestamp")
    updated_at: Optional[str] = Field(default=None, description="Last update timestamp")


class CreateProfileRequest(BaseModel):
    """Request to create/update a profile."""

    name: str = Field(..., description="Profile name")
    description: Optional[str] = Field(default=None, description="Profile description")
    gms_url: str = Field(..., description="DataHub GMS URL")
    gms_token: Optional[str] = Field(default=None, description="GMS authentication token")
    kube_context: Optional[str] = Field(default=None, description="Kubernetes context")
    kube_namespace: Optional[str] = Field(default=None, description="Kubernetes namespace")
