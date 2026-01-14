"""Hightouch API entities"""

from datetime import datetime
from typing import Any, Dict, List, Optional, Union

from pydantic import BaseModel, ConfigDict, Field, model_validator


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
    query_schema: Optional[Union[str, Dict[str, Any], List[Any]]] = Field(
        default=None, alias="querySchema"
    )

    @model_validator(mode="before")
    @classmethod
    def extract_raw_sql(cls, data: Any) -> Any:
        """Extract SQL from nested 'raw' object if not already present in rawSql"""
        if not isinstance(data, dict):
            return data

        # If rawSql is already present, no need to extract
        if "rawSql" in data or "raw_sql" in data:
            return data

        # Extract SQL from nested 'raw' object
        if "raw" in data and isinstance(data["raw"], dict) and "sql" in data["raw"]:
            data["rawSql"] = data["raw"]["sql"]

        return data


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


class HightouchContract(_HightouchBaseModel):
    """Represents a Hightouch Event Contract (data quality validation rule)"""

    id: str
    name: str
    workspace_id: str = Field(alias="workspaceId")
    source_id: Optional[str] = Field(default=None, alias="sourceId")
    model_id: Optional[str] = Field(default=None, alias="modelId")
    created_at: datetime = Field(alias="createdAt")
    updated_at: datetime = Field(alias="updatedAt")
    description: Optional[str] = None
    enabled: bool = True
    # Contract validation rules
    rules: Optional[Dict[str, Any]] = None
    # Contract enforcement level (error, warn, etc.)
    severity: Optional[str] = None


class HightouchContractRun(_HightouchBaseModel):
    """Represents a Contract validation run result"""

    id: str
    contract_id: str = Field(alias="contractId")
    status: str  # passed, failed, error
    created_at: datetime = Field(alias="createdAt")
    started_at: Optional[datetime] = Field(default=None, alias="startedAt")
    finished_at: Optional[datetime] = Field(default=None, alias="finishedAt")
    error: Optional[Union[str, Dict[str, Any]]] = None
    # Validation results
    total_rows_checked: Optional[int] = Field(default=None, alias="totalRowsChecked")
    rows_passed: Optional[int] = Field(default=None, alias="rowsPassed")
    rows_failed: Optional[int] = Field(default=None, alias="rowsFailed")
