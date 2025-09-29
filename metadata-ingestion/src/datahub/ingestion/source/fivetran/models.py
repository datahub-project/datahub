from typing import Dict, List, Optional, Union

from pydantic import BaseModel, Field, root_validator, validator


class ColumnLineage(BaseModel):
    source_column: str
    destination_column: str
    source_column_type: Optional[str] = None
    destination_column_type: Optional[str] = None


class TableLineage(BaseModel):
    source_table: str
    destination_table: str
    column_lineage: List[ColumnLineage]
    source_schema: Optional[str] = None
    destination_schema: Optional[str] = None
    source_database: Optional[str] = None
    destination_database: Optional[str] = None
    source_platform: Optional[str] = None
    destination_platform: Optional[str] = None
    source_env: Optional[str] = None
    destination_env: Optional[str] = None
    connector_type_id: Optional[str] = None
    connector_name: Optional[str] = None
    destination_id: Optional[str] = None


class Job(BaseModel):
    job_id: str
    start_time: int
    end_time: int
    status: str


# Job/Sync API Response Models


class FivetranJobResponse(BaseModel):
    """Job response from Fivetran API."""

    id: Optional[str] = None
    started_at: Optional[str] = None
    completed_at: Optional[str] = None
    start_time: Optional[str] = None  # Alternative field name
    end_time: Optional[str] = None  # Alternative field name
    status: Optional[str] = None
    end_message_data: Optional[str] = None

    class Config:
        extra = "allow"


class FivetranSyncResponse(BaseModel):
    """Sync response from Fivetran API."""

    id: Optional[str] = None
    started_at: Optional[str] = None
    completed_at: Optional[str] = None
    finished_at: Optional[str] = None  # Alternative field name
    status: Optional[str] = None

    class Config:
        extra = "allow"


class FivetranDestinationConfig(BaseModel):
    """Destination configuration from Fivetran API."""

    database: Optional[str] = None
    dataset: Optional[str] = None
    data_set_location: Optional[str] = None
    project_id: Optional[str] = None
    host: Optional[str] = None
    user: Optional[str] = None
    role: Optional[str] = None

    class Config:
        extra = "allow"


class FivetranDestinationResponse(BaseModel):
    """Destination response from Fivetran API."""

    id: str
    service: Optional[str] = None
    config: Optional[FivetranDestinationConfig] = None
    name: Optional[str] = None

    class Config:
        extra = "allow"


class FivetranConnectorStatus(BaseModel):
    """Connector status from Fivetran API."""

    setup_state: str
    sync_state: Optional[str] = None
    update_state: Optional[str] = None
    is_historical_sync: Optional[bool] = None

    class Config:
        extra = "allow"


class FivetranConnectorResponse(BaseModel):
    """Connector response from Fivetran API."""

    id: str
    group_id: Optional[str] = None  # Direct group_id field
    group: Optional[Dict[str, str]] = None  # Nested group structure
    service: str
    schema_: Optional[str] = Field(None, alias="schema")
    connector_name: Optional[str] = Field(None, alias="name")
    created_by: Optional[str] = None
    paused: bool = False
    sync_frequency: int = 0
    schedule: Optional[Dict[str, int]] = None  # For nested sync_frequency
    status: Optional[FivetranConnectorStatus] = None

    @root_validator(skip_on_failure=True)
    def extract_nested_fields(cls, values):
        """Extract fields from nested structures."""
        # Extract group_id from nested group structure
        if values.get("group_id") is None:
            group = values.get("group")
            if isinstance(group, dict) and "id" in group:
                values["group_id"] = group["id"]

        # Extract sync_frequency from nested schedule structure
        if values.get("sync_frequency") == 0:
            schedule = values.get("schedule")
            if isinstance(schedule, dict) and "sync_frequency" in schedule:
                values["sync_frequency"] = schedule["sync_frequency"]

        return values

    class Config:
        extra = "allow"
        allow_population_by_field_name = True


class FivetranTableColumn(BaseModel):
    """Table column from Fivetran schema API."""

    name: str
    name_in_destination: Optional[str] = None
    enabled: bool = True
    hashed: Optional[bool] = None
    primary_key: Optional[bool] = None

    class Config:
        extra = "allow"


class FivetranTable(BaseModel):
    """Table from Fivetran schema API."""

    name: str
    name_in_destination: Optional[str] = None
    enabled: bool = True
    columns: Optional[
        Dict[str, Union[Dict[str, Union[str, bool]], FivetranTableColumn]]
    ] = Field(default_factory=dict)

    @validator("columns", pre=True)
    def convert_columns(cls, v):
        """Convert columns list to dict format."""
        if isinstance(v, dict):
            return v
        elif isinstance(v, list):
            return {
                col.get("name", f"col_{i}"): col
                for i, col in enumerate(v)
                if isinstance(col, dict)
            }
        return {}

    class Config:
        extra = "allow"


class FivetranSchema(BaseModel):
    """Schema from Fivetran API."""

    name: str
    name_in_destination: Optional[str] = None
    enabled: bool = True
    tables: Optional[
        Dict[str, Union[Dict[str, Union[str, bool, Dict]], FivetranTable]]
    ] = Field(default_factory=dict)

    @validator("tables", pre=True)
    def convert_tables(cls, v):
        """Convert tables list to dict format."""
        if isinstance(v, dict):
            return v
        elif isinstance(v, list):
            return {
                table.get("name", f"table_{i}"): table
                for i, table in enumerate(v)
                if isinstance(table, dict)
            }
        return {}

    class Config:
        extra = "allow"


class Connector(BaseModel):
    connector_id: str
    connector_name: str
    connector_type: str
    paused: bool
    sync_frequency: int
    destination_id: str
    user_id: Optional[str] = None
    lineage: List[TableLineage] = Field(default_factory=list)
    jobs: List[Job] = Field(default_factory=list)
    additional_properties: Dict[str, Union[str, bool, int, float, None]] = Field(
        default_factory=dict
    )
