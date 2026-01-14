"""Hightouch API entities"""

from datetime import datetime
from typing import Any, Dict, Optional, Union

from pydantic import BaseModel, ConfigDict, Field


class _HightouchBaseModel(BaseModel):
    """Base model for Hightouch entities with common config"""

    model_config = ConfigDict(
        populate_by_name=True,  # Allow both camelCase and snake_case
        coerce_numbers_to_str=True,  # Auto-convert numbers to strings for ID fields
        protected_namespaces=(),  # Allow model_ field names (for model_id)
    )


class HightouchSourceConnection(_HightouchBaseModel):
    """Represents a Hightouch Source (database/warehouse connection)"""

    id: str
    name: str
    slug: str
    type: str
    workspace_id: str = Field(alias="workspaceId")
    created_at: datetime = Field(alias="createdAt")
    updated_at: datetime = Field(alias="updatedAt")
    configuration: Dict[str, Any] = Field(default_factory=dict)


class HightouchModel(_HightouchBaseModel):
    """Represents a Hightouch Model (SQL query)"""

    id: str
    name: str
    slug: str
    workspace_id: str = Field(alias="workspaceId")
    source_id: str = Field(alias="sourceId")
    query_type: str = Field(alias="queryType")
    created_at: datetime = Field(alias="createdAt")
    updated_at: datetime = Field(alias="updatedAt")
    primary_key: Optional[str] = Field(default=None, alias="primaryKey")
    description: Optional[str] = None
    is_schema: bool = Field(default=False, alias="isSchema")
    tags: Optional[Dict[str, str]] = None
    raw_sql: Optional[str] = Field(default=None, alias="rawSql")
    query_schema: Optional[str] = Field(default=None, alias="querySchema")


class HightouchDestination(_HightouchBaseModel):
    """Represents a Hightouch Destination"""

    id: str
    name: str
    slug: str
    type: str
    workspace_id: str = Field(alias="workspaceId")
    created_at: datetime = Field(alias="createdAt")
    updated_at: datetime = Field(alias="updatedAt")
    configuration: Dict[str, Any] = Field(default_factory=dict)


class HightouchSync(_HightouchBaseModel):
    """Represents a Hightouch Sync"""

    id: str
    slug: str
    workspace_id: str = Field(alias="workspaceId")
    model_id: str = Field(alias="modelId")
    destination_id: str = Field(alias="destinationId")
    created_at: datetime = Field(alias="createdAt")
    updated_at: datetime = Field(alias="updatedAt")
    configuration: Dict[str, Any] = Field(default_factory=dict)
    schedule: Optional[Dict[str, Any]] = None
    disabled: bool = False
    primary_key: Optional[str] = Field(default=None, alias="primaryKey")


class HightouchSyncRun(_HightouchBaseModel):
    """Represents a Hightouch Sync Run"""

    id: str
    sync_id: Optional[str] = Field(default=None, alias="syncId")
    status: str
    started_at: datetime = Field(alias="startedAt")
    finished_at: Optional[datetime] = Field(default=None, alias="finishedAt")
    created_at: datetime = Field(alias="createdAt")
    error: Optional[Union[str, Dict[str, Any]]] = None
    completion_ratio: Optional[float] = Field(default=None, alias="completionRatio")
    planned_rows: Optional[Dict[str, int]] = Field(default=None, alias="plannedRows")
    successful_rows: Optional[Dict[str, int]] = Field(
        default=None, alias="successfulRows"
    )
    failed_rows: Optional[Dict[str, int]] = Field(default=None, alias="failedRows")
    query_size: Optional[int] = Field(default=None, alias="querySize")


class ColumnLineage(BaseModel):
    """Represents column-level lineage"""

    source_column: str
    destination_column: str


class HightouchUser(_HightouchBaseModel):
    """Represents a Hightouch User"""

    id: str
    email: str
    name: Optional[str] = None
    created_at: Optional[datetime] = Field(default=None, alias="createdAt")


class FieldMapping(BaseModel):
    """Represents a field mapping in a sync"""

    source_field: str
    destination_field: str
    is_primary_key: bool = False
