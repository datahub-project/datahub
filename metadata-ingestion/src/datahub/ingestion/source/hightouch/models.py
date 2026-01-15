from datetime import datetime
from typing import Any, Dict, List, Optional, Union

from pydantic import BaseModel, ConfigDict, Field, model_validator


class _HightouchBaseModel(BaseModel):
    model_config = ConfigDict(
        populate_by_name=True,
        coerce_numbers_to_str=True,
        protected_namespaces=(),
    )


class HightouchSchemaField(_HightouchBaseModel):
    name: str
    type: str
    description: Optional[str] = None
    is_primary_key: bool = False


class HightouchSourceConnection(_HightouchBaseModel):
    id: str
    name: str
    slug: str
    type: str
    workspace_id: str = Field(alias="workspaceId")
    created_at: datetime = Field(alias="createdAt")
    updated_at: datetime = Field(alias="updatedAt")
    configuration: Dict[str, Any] = Field(default_factory=dict)


class HightouchModel(_HightouchBaseModel):
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
    folder_id: Optional[str] = Field(default=None, alias="folderId")
    raw_sql: Optional[str] = Field(default=None, alias="rawSql")
    query_schema: Optional[Union[str, Dict[str, Any], List[Any]]] = Field(
        default=None, alias="querySchema"
    )

    @model_validator(mode="before")
    @classmethod
    def extract_raw_sql(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data

        if "rawSql" in data or "raw_sql" in data:
            return data

        if "raw" in data and isinstance(data["raw"], dict) and "sql" in data["raw"]:
            data["rawSql"] = data["raw"]["sql"]

        return data


class HightouchDestination(_HightouchBaseModel):
    id: str
    name: str
    slug: str
    type: str
    workspace_id: str = Field(alias="workspaceId")
    created_at: datetime = Field(alias="createdAt")
    updated_at: datetime = Field(alias="updatedAt")
    configuration: Dict[str, Any] = Field(default_factory=dict)


class HightouchSync(_HightouchBaseModel):
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
    referenced_columns: Optional[List[str]] = Field(
        default=None, alias="referencedColumns"
    )
    tags: Optional[Dict[str, str]] = None


class HightouchSyncRun(_HightouchBaseModel):
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


class HightouchUser(_HightouchBaseModel):
    id: str
    email: str
    name: Optional[str] = None
    created_at: Optional[datetime] = Field(default=None, alias="createdAt")


class HightouchFieldMapping(BaseModel):
    source_field: str
    destination_field: str
    is_primary_key: bool = False


class HightouchContract(_HightouchBaseModel):
    id: str
    name: str
    workspace_id: str = Field(alias="workspaceId")
    source_id: Optional[str] = Field(default=None, alias="sourceId")
    model_id: Optional[str] = Field(default=None, alias="modelId")
    created_at: datetime = Field(alias="createdAt")
    updated_at: datetime = Field(alias="updatedAt")
    description: Optional[str] = None
    enabled: bool = True
    rules: Optional[Dict[str, Any]] = None
    severity: Optional[str] = None


class HightouchContractRun(_HightouchBaseModel):
    id: str
    contract_id: str = Field(alias="contractId")
    status: str
    created_at: datetime = Field(alias="createdAt")
    started_at: Optional[datetime] = Field(default=None, alias="startedAt")
    finished_at: Optional[datetime] = Field(default=None, alias="finishedAt")
    error: Optional[Union[str, Dict[str, Any]]] = None
    total_rows_checked: Optional[int] = Field(default=None, alias="totalRowsChecked")
    rows_passed: Optional[int] = Field(default=None, alias="rowsPassed")
    rows_failed: Optional[int] = Field(default=None, alias="rowsFailed")
