from datetime import datetime
from typing import Any, Dict, List, Optional, Set, Union

from pydantic import BaseModel, ConfigDict, Field, model_validator

from datahub.metadata.schema_classes import (
    FineGrainedLineageClass,
    SchemaFieldClass,
)
from datahub.sdk.dataset import Dataset


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


class HightouchWorkspace(_HightouchBaseModel):
    id: str
    name: str
    slug: str
    created_at: datetime = Field(alias="createdAt")
    updated_at: datetime = Field(alias="updatedAt")


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


class HightouchFieldMapping(_HightouchBaseModel):
    source_field: str
    destination_field: str
    is_primary_key: bool = False


class HightouchColumnPair(_HightouchBaseModel):
    source_field: str
    destination_field: str


class HightouchContractEvent(_HightouchBaseModel):
    type: str
    json_schema: Dict[str, Any] = Field(default_factory=dict, alias="schema")
    name: Optional[str] = None
    slug: Optional[str] = None
    version: Optional[str] = None
    on_schema_violation: Optional[str] = Field(default=None, alias="onSchemaViolation")
    on_undeclared_fields: Optional[str] = Field(
        default=None, alias="onUndeclaredFields"
    )


class HightouchEventSource(_HightouchBaseModel):
    id: str
    name: str


class HightouchContract(_HightouchBaseModel):
    id: str
    name: str
    slug: Optional[str] = None
    description: Optional[str] = None
    workspace_id: Optional[str] = Field(default=None, alias="workspaceId")
    on_undeclared_schema: Optional[str] = Field(
        default=None, alias="onUndeclaredSchema"
    )
    events: List[HightouchContractEvent] = Field(default_factory=list)
    event_sources: List[HightouchEventSource] = Field(
        default_factory=list, alias="eventSources"
    )


class HightouchDestinationLineageInfo(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    upstreams: Set[str] = Field(default_factory=set)
    fine_grained_lineages: List[FineGrainedLineageClass] = Field(default_factory=list)


class HightouchModelDatasetResult(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    dataset: Dataset
    schema_fields: List[SchemaFieldClass] = Field(default_factory=list)
