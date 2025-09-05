"""
Data models for DataHub Cloud Router.
"""

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any, Dict, Optional

from pydantic import BaseModel, Field, validator


class DeploymentType(str, Enum):
    """Deployment types for DataHub instances."""

    CLOUD = "cloud"
    ON_PREMISE = "on_premise"
    HYBRID = "hybrid"


class HealthStatus(str, Enum):
    """Health status of DataHub instances."""

    HEALTHY = "healthy"
    UNHEALTHY = "unhealthy"
    UNKNOWN = "unknown"
    DEGRADED = "degraded"


@dataclass
class DataHubInstance:
    """Represents a DataHub instance configuration."""

    id: str
    name: str
    deployment_type: DeploymentType
    url: str
    connection_id: Optional[str] = None
    api_token: Optional[str] = None
    health_check_url: Optional[str] = None
    is_active: bool = True
    is_default: bool = False
    max_concurrent_requests: int = 100
    timeout_seconds: int = 30
    health_status: HealthStatus = HealthStatus.UNKNOWN
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    last_health_check: Optional[datetime] = None


@dataclass
class TenantMapping:
    """Maps a tenant to a DataHub instance."""

    id: str
    tenant_id: str
    instance_id: str
    team_id: Optional[str] = None
    created_by: str = "system"
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


@dataclass
class UnknownTenant:
    """Tracks unknown tenants for admin review."""

    id: str
    tenant_id: str
    team_id: Optional[str] = None
    first_seen: Optional[datetime] = None
    last_seen: Optional[datetime] = None
    event_count: int = 1


class DatabaseConfig(BaseModel):
    """Configuration for database connections."""

    type: str = "sqlite"
    path: Optional[str] = None
    host: Optional[str] = None
    port: Optional[int] = None
    username: Optional[str] = None
    password: Optional[str] = None
    database: Optional[str] = None


class RoutingEvent(BaseModel):
    """Represents a routing event."""

    event_id: str
    tenant_id: str
    team_id: Optional[str] = None
    event_type: str
    timestamp: datetime
    source_url: str
    target_url: Optional[str] = None
    status: str
    error_message: Optional[str] = None
    processing_time_ms: Optional[int] = None


# OAuth State Models


class OAuthStateVersion(str, Enum):
    """OAuth state schema versions for evolution."""

    V1 = "1"  # Storage-free design with all context in state


class OAuthFlowType(str, Enum):
    """Types of OAuth flows supported."""

    PLATFORM_INTEGRATION = "platform_integration"
    PERSONAL_NOTIFICATIONS = "personal_notifications"
    ADMIN_SETUP = "admin_setup"


class OAuthStateV1(BaseModel):
    """V1 OAuth state - storage-free design with all context encoded."""

    # Core routing (required)
    url: str = Field(..., description="Target DataHub instance URL")
    nonce: str = Field(..., description="Cryptographically secure random nonce")

    # Security metadata
    timestamp: int = Field(..., description="Unix timestamp when state was created")
    version: OAuthStateVersion = Field(
        default=OAuthStateVersion.V1, description="State schema version"
    )

    # Flow context (replaces session storage)
    flow_type: OAuthFlowType = Field(
        default=OAuthFlowType.PLATFORM_INTEGRATION, description="Type of OAuth flow"
    )

    # Routing context
    tenant_id: Optional[str] = Field(None, description="Microsoft tenant ID")
    user_urn: Optional[str] = Field(
        None, description="DataHub user URN for personal flows"
    )

    # Navigation context
    redirect_path: Optional[str] = Field(
        None, description="Path to redirect after OAuth completion"
    )
    origin_path: Optional[str] = Field(None, description="Original path user came from")

    # Client context
    client_metadata: Optional[Dict[str, Any]] = Field(
        None, description="Additional client-specific data"
    )

    class Config:
        use_enum_values = True
        json_encoders = {
            OAuthStateVersion: lambda v: v.value,
            OAuthFlowType: lambda v: v.value,
        }

    @validator("nonce")
    def validate_nonce_security(cls, v):
        """Ensure nonce has sufficient entropy."""
        if len(v) < 32:
            raise ValueError(
                "Nonce must have at least 32 characters (256 bits entropy)"
            )
        return v

    @validator("url")
    def validate_url_format(cls, v):
        """Basic URL validation."""
        if not v.startswith(("http://", "https://")):
            raise ValueError("URL must be http:// or https://")
        return v

    @validator("redirect_path", "origin_path")
    def validate_paths(cls, v):
        """Validate paths are relative and safe."""
        if v and (v.startswith("http") or "//" in v):
            raise ValueError("Paths must be relative, not absolute URLs")
        return v


class OAuthStateDecoded(BaseModel):
    """Decoded OAuth state data for processing."""

    # Core fields
    url: str
    nonce: str
    timestamp: int
    version: OAuthStateVersion

    # Flow context
    flow_type: OAuthFlowType
    tenant_id: Optional[str] = None
    user_urn: Optional[str] = None

    # Navigation context
    redirect_path: Optional[str] = None
    origin_path: Optional[str] = None

    # Client context
    client_metadata: Optional[Dict[str, Any]] = None

    class Config:
        use_enum_values = True

    @classmethod
    def from_v1_state(cls, state_data: OAuthStateV1) -> "OAuthStateDecoded":
        """Create from V1 state data."""
        return cls(
            url=state_data.url,
            nonce=state_data.nonce,
            timestamp=state_data.timestamp,
            version=state_data.version,
            flow_type=state_data.flow_type,
            tenant_id=state_data.tenant_id,
            user_urn=state_data.user_urn,
            redirect_path=state_data.redirect_path,
            origin_path=state_data.origin_path,
            client_metadata=state_data.client_metadata,
        )
