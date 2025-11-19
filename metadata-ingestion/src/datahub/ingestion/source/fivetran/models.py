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
    source_column_type: str | None = None
    destination_column_type: str | None = None


class TableLineage(BaseModel):
    source_table: str
    destination_table: str
    column_lineage: List[ColumnLineage]
    source_schema: str | None = None
    destination_schema: str | None = None
    source_database: str | None = None
    destination_database: str | None = None
    source_platform: str | None = None
    destination_platform: str | None = None
    source_env: str | None = None
    destination_env: str | None = None
    connector_type_id: str | None = None
    connector_name: str | None = None
    destination_id: str | None = None


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
    user_id: str | None = None
    lineage: List[TableLineage] = Field(default_factory=list)
    jobs: List[Job] = Field(default_factory=list)
    additional_properties: Dict[str, Union[str, bool, int, float, None]] = Field(
        default_factory=dict
    )


# Job/Sync API Response Models


class FivetranJobResponse(BaseModel):
    """Job response from Fivetran API."""

    id: str | None = None
    started_at: str | None = None
    completed_at: str | None = None
    start_time: str | None = None  # Alternative field name
    end_time: str | None = None  # Alternative field name
    status: str | None = None
    end_message_data: str | None = None

    model_config = {"extra": "allow"}


class FivetranSyncResponse(BaseModel):
    """Sync response from Fivetran API."""

    id: str | None = None
    started_at: str | None = None
    completed_at: str | None = None
    finished_at: str | None = None  # Alternative field name
    status: str | None = None

    model_config = {"extra": "allow"}


class FivetranDestinationConfig(BaseModel):
    """Destination configuration from Fivetran API."""

    database: str | None = None
    dataset: str | None = None
    data_set_location: str | None = None
    project_id: str | None = None
    host: str | None = None
    user: str | None = None
    role: str | None = None

    model_config = {"extra": "allow"}


class FivetranDestinationResponse(BaseModel):
    """Destination response from Fivetran API."""

    id: str
    service: str | None = None
    config: FivetranDestinationConfig | None = None
    name: str | None = None

    model_config = {"extra": "allow"}


class FivetranConnectorStatus(BaseModel):
    """Connector status from Fivetran API."""

    setup_state: str
    sync_state: str | None = None
    update_state: str | None = None
    is_historical_sync: bool | None = None

    model_config = {"extra": "allow"}


class FivetranConnectorResponse(BaseModel):
    """Connector response from Fivetran API."""

    id: str
    group_id: str | None = None  # Direct group_id field
    group: Dict[str, str] | None = None  # Nested group structure
    service: str
    schema_: str | None = Field(None, alias="schema")
    connector_name: str | None = Field(None, alias="name")
    created_by: str | None = None
    paused: bool = False
    sync_frequency: int = 0
    schedule: Dict[str, int] | None = None  # For nested sync_frequency
    status: FivetranConnectorStatus | None = None

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
    name_in_destination: str | None = None
    enabled: bool = True
    hashed: bool | None = None
    primary_key: bool | None = None

    model_config = {"extra": "allow"}


class FivetranTable(BaseModel):
    """Table from Fivetran schema API."""

    name: str
    name_in_destination: str | None = None
    enabled: bool = True
    columns: (
        Dict[str, Union[Dict[str, Union[str, bool]], FivetranTableColumn]] | None
    ) = Field(default_factory=dict)

    model_config = {"extra": "allow"}


class FivetranSchema(BaseModel):
    """Schema from Fivetran API."""

    name: str
    name_in_destination: str | None = None
    enabled: bool = True
    tables: (
        Dict[str, Union[Dict[str, Union[str, bool, Dict]], FivetranTable]] | None
    ) = Field(default_factory=dict)

    model_config = {"extra": "allow"}
