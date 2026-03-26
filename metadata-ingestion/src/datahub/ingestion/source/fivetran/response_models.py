import datetime
from typing import Optional

from pydantic import BaseModel, ConfigDict


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
