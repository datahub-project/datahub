import datetime
from typing import Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field


class FivetranConnectionConfig(BaseModel):
    """Connection config for Google Sheets connector."""

    model_config = ConfigDict(extra="ignore")

    auth_type: str  # Auth Type
    sheet_id: str  # Sheet ID - URL to the Google Sheet
    named_range: str  # Named Range


class FivetranConnectionDetails(BaseModel):
    """
    Note: This response class only captures fields that are relevant to the Google Sheets Connector.
    Extra fields from the API response are ignored via ConfigDict.
    """

    model_config = ConfigDict(extra="ignore")

    id: str  # Source ID
    group_id: str  # Destination ID
    service: str  # Connector Type
    created_at: datetime.datetime
    succeeded_at: Optional[datetime.datetime] = (
        None  # Succeeded At (may be None if connector hasn't succeeded yet)
    )
    paused: bool  # Paused Status
    sync_frequency: int  # Sync Frequency (minutes)
    config: FivetranConnectionConfig  # Connection Config

    """
    # Sample Response for Google Sheets Connector
    {
        "code": "Success",
        "data": {
            "id": "dialectical_remindful",
            "group_id": "empties_classification",
            "service": "google_sheets",
            "service_version": 1,
            "schema": "fivetran_google_sheets.fivetran_google_sheets",
            "connected_by": "sewn_restrained",
            "created_at": "2025-10-06T17:53:01.554289Z",
            "succeeded_at": "2025-10-06T22:55:45.275000Z",
            "failed_at": null,
            "paused": true,
            "pause_after_trial": false,
            "sync_frequency": 360,
            "data_delay_threshold": 0,
            "data_delay_sensitivity": "NORMAL",
            "private_link_id": null,
            "networking_method": "Directly",
            "proxy_agent_id": null,
            "schedule_type": "auto",
            "status": {
                "setup_state": "connected",
                "schema_status": "ready",
                "sync_state": "paused",
                "update_state": "on_schedule",
                "is_historical_sync": false,
                "tasks": [],
                "warnings": [
                    {
                        "code": "snowflake_discontinuing_password_auth",
                        "message": "Snowflake is discontinuing username/password authentication",
                        "details": {}
                    }
                ]
            },
            "config": {
                "auth_type": "ServiceAccount",
                "sheet_id": "https://docs.google.com/spreadsheets/d/1A82PdLAE7NXLLb5JcLPKeIpKUMytXQba5Z-Ei-mbXLo/edit?gid=0#gid=0",
                "named_range": "Fivetran_Test_Range"
            },
            "source_sync_details": {
                "last_synced": "2025-10-06T22:55:27.371Z"
            }
        }
    }
    """


class FivetranDestinationConfig(BaseModel):
    """Destination-level config returned by `GET /v1/destinations/{id}`.

    The shape varies by `service` — only fields useful for URN construction
    are typed explicitly; the rest are tolerated via `extra='ignore'`.
    """

    model_config = ConfigDict(extra="ignore")

    # Common across destinations (most expose at least one of these).
    database: Optional[str] = None  # Snowflake / Databricks
    project_id: Optional[str] = None  # BigQuery
    catalog: Optional[str] = None  # Databricks Unity

    # Managed Data Lake fields. The same `service: managed_data_lake`
    # covers three cloud backings, discriminated by which fields are
    # populated:
    #   - AWS S3:   `bucket` (no `gcs_project_id`)
    #   - GCS:      `bucket` + `gcs_project_id`
    #   - ADLS Gen2:`storage_account_name` + `container_name`
    bucket: Optional[str] = None
    prefix_path: Optional[str] = None
    region: Optional[str] = None
    table_format: Optional[str] = None  # "ICEBERG" / "DELTA"
    gcs_project_id: Optional[str] = None
    storage_account_name: Optional[str] = None
    container_name: Optional[str] = None
    # When `true`, Fivetran registers the Iceberg tables in AWS Glue (in
    # addition to writing them to S3). Used by FivetranSource to auto-route
    # the URN platform to `glue` when the user hasn't pinned one — they
    # still must supply `database` per-destination because the actual Glue
    # database name isn't exposed via REST. The other catalog toggles
    # (`should_maintain_tables_in_bqms`, `should_maintain_tables_in_one_lake`)
    # aren't modeled because DataHub has no native source for those
    # catalogs.
    should_maintain_tables_in_glue: Optional[bool] = None


class FivetranDestinationDetails(BaseModel):
    """Subset of `GET /v1/destinations/{id}` fields needed for URN routing.

    Sample response for a Managed Data Lake destination:
    {
      "id": "test_destination_id",
      "service": "managed_data_lake",
      "region": "AWS_US_EAST_1",
      "group_id": "...",
      "setup_status": "CONNECTED",
      "config": { "bucket": "...", "prefix_path": "fivetran", ... }
    }
    """

    model_config = ConfigDict(extra="ignore")

    id: str
    service: str  # "snowflake" | "bigquery" | "databricks" | "managed_data_lake" | ...
    region: Optional[str] = None
    group_id: Optional[str] = None
    setup_status: Optional[str] = None
    config: FivetranDestinationConfig = FivetranDestinationConfig()


class FivetranGroup(BaseModel):
    """One row in `GET /v1/groups`."""

    model_config = ConfigDict(extra="ignore")

    id: str
    name: Optional[str] = None


class FivetranListGroupsResponse(BaseModel):
    model_config = ConfigDict(extra="ignore")

    items: List[FivetranGroup]
    next_cursor: Optional[str] = None


class FivetranListedConnection(BaseModel):
    """One row in `GET /v1/groups/{gid}/connections`."""

    # `schema` shadows BaseModel.schema(), which mypy flags. We expose it
    # under `schema_` and use a Pydantic alias so the JSON field name
    # remains `schema` per Fivetran's response contract.
    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    id: str
    schema_: str = Field(alias="schema")
    service: str
    paused: bool
    sync_frequency: int
    group_id: str
    connected_by: Optional[str] = None


class FivetranListConnectionsResponse(BaseModel):
    model_config = ConfigDict(extra="ignore")

    items: List[FivetranListedConnection]
    next_cursor: Optional[str] = None


class FivetranColumn(BaseModel):
    model_config = ConfigDict(extra="ignore")

    name_in_destination: str
    enabled: bool = True
    is_primary_key: bool = False


class FivetranTable(BaseModel):
    model_config = ConfigDict(extra="ignore")

    name_in_destination: str
    enabled: bool = True
    columns: Dict[str, FivetranColumn] = {}


class FivetranSchema(BaseModel):
    model_config = ConfigDict(extra="ignore")

    name_in_destination: str
    enabled: bool = True
    tables: Dict[str, FivetranTable] = {}


class FivetranConnectionSchemas(BaseModel):
    """`GET /v1/connections/{id}/schemas` — top-level `schemas` dict."""

    model_config = ConfigDict(extra="ignore")

    schemas: Dict[str, FivetranSchema] = {}


class FivetranListedUser(BaseModel):
    model_config = ConfigDict(extra="ignore")

    id: str
    email: Optional[str] = None
    given_name: Optional[str] = None
    family_name: Optional[str] = None


class FivetranListUsersResponse(BaseModel):
    model_config = ConfigDict(extra="ignore")

    items: List[FivetranListedUser]
    next_cursor: Optional[str] = None
