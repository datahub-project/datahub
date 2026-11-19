from datetime import datetime
from typing import Any, Dict, List, Optional, Union

from pydantic import BaseModel, ConfigDict, Field


class _HightouchBaseModel(BaseModel):
    # Hightouch ids are numbers in the API but strings in recipes and URNs.
    model_config = ConfigDict(
        populate_by_name=True,
        coerce_numbers_to_str=True,
        protected_namespaces=(),
    )


class HightouchUserReference(_HightouchBaseModel):
    id: str
    name: Optional[str] = None
    email: Optional[str] = None


class HightouchSourceConnection(_HightouchBaseModel):
    id: str
    name: str
    slug: str
    type: str
    workspace_id: str = Field(alias="workspaceId")
    configuration: Dict[str, Any] = Field(default_factory=dict)


class HightouchTableQuery(_HightouchBaseModel):
    name: str


class HightouchRawQuery(_HightouchBaseModel):
    sql: str


class HightouchDbtQuery(_HightouchBaseModel):
    database: Optional[str] = None
    schema_name: Optional[str] = Field(default=None, alias="schema")
    name: Optional[str] = None


class HightouchModel(_HightouchBaseModel):
    id: str
    name: str
    slug: str
    workspace_id: str = Field(alias="workspaceId")
    source_id: str = Field(alias="sourceId")
    query_type: str = Field(alias="queryType")
    created_at: datetime = Field(alias="createdAt")
    updated_at: datetime = Field(alias="updatedAt")
    created_by: Optional[HightouchUserReference] = Field(
        default=None, alias="createdBy"
    )
    primary_key: Optional[str] = Field(default=None, alias="primaryKey")
    description: Optional[str] = None
    is_schema: bool = Field(default=False, alias="isSchema")
    tags: Optional[Dict[str, str]] = None
    # At most one of these is set, matching queryType. `name` above is the display
    # name; the warehouse table of a table model is `table.name`.
    table: Optional[HightouchTableQuery] = None
    raw: Optional[HightouchRawQuery] = None
    dbt: Optional[HightouchDbtQuery] = None

    @property
    def raw_sql(self) -> Optional[str]:
        return self.raw.sql if self.raw else None

    @property
    def table_name(self) -> Optional[str]:
        """The warehouse table a table or dbt model reads, qualified as far as the
        API qualifies it."""
        if self.table:
            return self.table.name
        if self.dbt and self.dbt.name:
            return ".".join(
                part
                for part in (self.dbt.database, self.dbt.schema_name, self.dbt.name)
                if part
            )
        return None


class HightouchDestination(_HightouchBaseModel):
    id: str
    name: str
    slug: str
    type: str
    workspace_id: str = Field(alias="workspaceId")
    configuration: Dict[str, Any] = Field(default_factory=dict)


class HightouchSync(_HightouchBaseModel):
    id: str
    slug: str
    workspace_id: str = Field(alias="workspaceId")
    model_id: str = Field(alias="modelId")
    destination_id: str = Field(alias="destinationId")
    created_at: datetime = Field(alias="createdAt")
    updated_at: datetime = Field(alias="updatedAt")
    created_by: Optional[HightouchUserReference] = Field(
        default=None, alias="createdBy"
    )
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
    status: str
    started_at: datetime = Field(alias="startedAt")
    finished_at: Optional[datetime] = Field(default=None, alias="finishedAt")
    error: Optional[Union[str, Dict[str, Any]]] = None
    completion_ratio: Optional[float] = Field(default=None, alias="completionRatio")
    planned_rows: Optional[Dict[str, int]] = Field(default=None, alias="plannedRows")
    successful_rows: Optional[Dict[str, int]] = Field(
        default=None, alias="successfulRows"
    )
    failed_rows: Optional[Dict[str, int]] = Field(default=None, alias="failedRows")
    query_size: Optional[int] = Field(default=None, alias="querySize")


class HightouchFieldMapping(_HightouchBaseModel):
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
    workspace_id: str = Field(alias="workspaceId")
    on_undeclared_schema: Optional[str] = Field(
        default=None, alias="onUndeclaredSchema"
    )
    events: List[HightouchContractEvent] = Field(default_factory=list)
    event_sources: List[HightouchEventSource] = Field(
        default_factory=list, alias="eventSources"
    )
