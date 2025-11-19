import datetime
from typing import Dict, List, Union

from pydantic import BaseModel, Field

# Google Sheets Specific Models (from response_models.py)


class FivetranConnectionWarnings(BaseModel):
    """Warning details from Fivetran connection status."""

    code: str
    message: str
    details: Dict


class FivetranConnectionStatus(BaseModel):
    """Connection status details from Fivetran API."""

    setup_state: str
    schema_status: str
    sync_state: str
    update_state: str
    is_historical_sync: bool
    warnings: List[FivetranConnectionWarnings] = Field(default_factory=list)


class FivetranConnectionConfig(BaseModel):
    """Connection configuration (Google Sheets specific fields)."""

    auth_type: str
    sheet_id: str  # URL to the Google Sheet
    named_range: str


class FivetranConnectionSourceSyncDetails(BaseModel):
    """Source sync timing details."""

    last_synced: datetime.datetime


class FivetranConnectionDetails(BaseModel):
    """
    Connection details from Fivetran API.
    Note: This primarily captures Google Sheets connector fields.
    """

    id: str  # Source ID
    group_id: str  # Destination ID
    service: str  # Connector Type
    created_at: datetime.datetime
    succeeded_at: datetime.datetime
    paused: bool
    sync_frequency: int  # Sync Frequency (minutes)
    status: FivetranConnectionStatus
    config: FivetranConnectionConfig
    source_sync_details: FivetranConnectionSourceSyncDetails


# Core Lineage Models


class ColumnLineage(BaseModel):
    source_column: str
    destination_column: str
    source_column_type: Union[str, None] = None
    destination_column_type: Union[str, None] = None


class TableLineage(BaseModel):
    source_table: str
    destination_table: str
    column_lineage: List[ColumnLineage]
    source_schema: Union[str, None] = None
    destination_schema: Union[str, None] = None
    source_database: Union[str, None] = None
    destination_database: Union[str, None] = None
    source_platform: Union[str, None] = None
    destination_platform: Union[str, None] = None
    source_env: Union[str, None] = None
    destination_env: Union[str, None] = None
    connector_type_id: Union[str, None] = None
    connector_name: Union[str, None] = None
    destination_id: Union[str, None] = None


class Job(BaseModel):
    job_id: str
    start_time: int
    end_time: int
    status: str


class Connector(BaseModel):
    """Unified Connector model replacing dataclass."""

    connector_id: str
    connector_name: str
    connector_type: str
    paused: bool
    sync_frequency: int
    destination_id: str
    user_id: Union[str, None] = None
    lineage: List[TableLineage] = Field(default_factory=list)
    jobs: List[Job] = Field(default_factory=list)
    additional_properties: Dict[str, Union[str, bool, int, float, None]] = Field(
        default_factory=dict
    )


# Job/Sync API Response Models


class FivetranJobResponse(BaseModel):
    """Job response from Fivetran API."""

    id: Union[str, None] = None
    started_at: Union[str, None] = None
    completed_at: Union[str, None] = None
    start_time: Union[str, None] = None  # Alternative field name
    end_time: Union[str, None] = None  # Alternative field name
    status: Union[str, None] = None
    end_message_data: Union[str, None] = None

    model_config = {"extra": "allow"}


class FivetranSyncResponse(BaseModel):
    """Sync response from Fivetran API."""

    id: Union[str, None] = None
    started_at: Union[str, None] = None
    completed_at: Union[str, None] = None
    finished_at: Union[str, None] = None  # Alternative field name
    status: Union[str, None] = None

    model_config = {"extra": "allow"}


class FivetranDestinationConfig(BaseModel):
    """Destination configuration from Fivetran API."""

    database: Union[str, None] = None
    dataset: Union[str, None] = None
    data_set_location: Union[str, None] = None
    project_id: Union[str, None] = None
    host: Union[str, None] = None
    user: Union[str, None] = None
    role: Union[str, None] = None

    model_config = {"extra": "allow"}


class FivetranDestinationResponse(BaseModel):
    """Destination response from Fivetran API."""

    id: str
    service: Union[str, None] = None
    config: Union[FivetranDestinationConfig, None] = None
    name: Union[str, None] = None

    model_config = {"extra": "allow"}


class FivetranConnectorStatus(BaseModel):
    """Connector status from Fivetran API."""

    setup_state: str
    sync_state: Union[str, None] = None
    update_state: Union[str, None] = None
    is_historical_sync: Union[bool, None] = None

    model_config = {"extra": "allow"}


class FivetranConnectorResponse(BaseModel):
    """Connector response from Fivetran API."""

    id: str
    group_id: Union[str, None] = None  # Direct group_id field
    group: Union[Dict[str, str], None] = None  # Nested group structure
    service: str
    schema_: Union[str, None] = Field(None, alias="schema")
    connector_name: Union[str, None] = Field(None, alias="name")
    created_by: Union[str, None] = None
    paused: bool = False
    sync_frequency: int = 0
    schedule: Union[Dict[str, int], None] = None  # For nested sync_frequency
    status: Union[FivetranConnectorStatus, None] = None

    model_config = {"extra": "allow", "populate_by_name": True}

    def __init__(self, **data):
        super().__init__(**data)
        # Extract sync_frequency from schedule if present and sync_frequency is default
        if (
            self.sync_frequency == 0
            and self.schedule
            and isinstance(self.schedule, dict)
        ):
            schedule_sync_freq = self.schedule.get("sync_frequency")
            if schedule_sync_freq is not None:
                self.sync_frequency = schedule_sync_freq

        # Extract group_id from nested group object if present and group_id is not set
        if not self.group_id and self.group and isinstance(self.group, dict):
            group_id = self.group.get("id")
            if group_id:
                self.group_id = group_id


class FivetranTableColumn(BaseModel):
    """Table column from Fivetran schema API."""

    name: str
    name_in_destination: Union[str, None] = None
    enabled: bool = True
    hashed: Union[bool, None] = None
    primary_key: Union[bool, None] = None

    model_config = {"extra": "allow"}


class FivetranTable(BaseModel):
    """Table from Fivetran schema API."""

    name: str
    name_in_destination: Union[str, None] = None
    enabled: bool = True
    columns: Union[
        Dict[str, Union[Dict[str, Union[str, bool]], FivetranTableColumn]], None
    ] = Field(default_factory=dict)

    model_config = {"extra": "allow"}


class FivetranSchema(BaseModel):
    """Schema from Fivetran API."""

    name: str
    name_in_destination: Union[str, None] = None
    enabled: bool = True
    tables: Union[
        Dict[str, Union[Dict[str, Union[str, bool, Dict]], FivetranTable]], None
    ] = Field(default_factory=dict)

    model_config = {"extra": "allow"}
