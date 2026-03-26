"""Shared data models for Microsoft Fabric Core API entities.

These models correspond to the Fabric Core API (api.fabric.microsoft.com/v1)
and are shared across all Fabric connectors.

References:
- Workspaces API: https://learn.microsoft.com/en-us/rest/api/fabric/core/workspaces
- Items API: https://learn.microsoft.com/en-us/rest/api/fabric/core/items
"""

from dataclasses import dataclass
from typing import Optional

from pydantic import model_validator

from datahub.emitter.mcp_builder import ContainerKey
from datahub.utilities.str_enum import StrEnum

# Shared platform name for Fabric workspace containers.
# All Fabric connectors (OneLake, Data Factory, etc.) use this so that a
# workspace appears as a single node in DataHub regardless of which
# connector ingested it.
FABRIC_WORKSPACE_PLATFORM = "fabric"


class FabricItemType:
    DATA_PIPELINE = "DataPipeline"


class WorkspaceKey(ContainerKey):
    """Container key for Fabric workspaces.

    Shared across all Fabric connectors so that workspaces produce the same
    URN regardless of which connector is running.
    """

    platform: str = FABRIC_WORKSPACE_PLATFORM
    workspace_id: str

    @model_validator(mode="after")
    def _validate_workspace_platform(self) -> "WorkspaceKey":
        if type(self) is WorkspaceKey and self.platform != FABRIC_WORKSPACE_PLATFORM:
            raise ValueError(
                f"WorkspaceKey platform must be '{FABRIC_WORKSPACE_PLATFORM}', "
                f"got '{self.platform}'"
            )
        return self


@dataclass
class FabricWorkspace:
    """Microsoft Fabric workspace metadata.

    Returned by the Core API GET /workspaces endpoint.
    Shared across all Fabric connectors (OneLake, Data Factory, etc.).

    Reference: https://learn.microsoft.com/en-us/rest/api/fabric/core/workspaces/list-workspaces
    """

    id: str
    name: str
    description: Optional[str] = None
    type: Optional[str] = None
    capacity_id: Optional[str] = None


@dataclass
class FabricItem:
    """A generic Fabric item returned by the Core API.

    The Core API GET /workspaces/{id}/items endpoint returns items of any type.
    Connector-specific clients build richer domain models on top of this.

    Reference: https://learn.microsoft.com/en-us/rest/api/fabric/core/items/list-items
    """

    id: str
    name: str
    type: str  # e.g. "DataPipeline", "CopyJob", "Dataflow", "Lakehouse", "Warehouse"
    workspace_id: str
    description: Optional[str] = None


class ItemJobStatus(StrEnum):
    """Fabric Job Scheduler status values.

    Reference: https://learn.microsoft.com/en-us/rest/api/fabric/core/job-scheduler/list-item-job-instances
    """

    NOT_STARTED = "NotStarted"
    IN_PROGRESS = "InProgress"
    COMPLETED = "Completed"
    FAILED = "Failed"
    CANCELLED = "Cancelled"
    DEDUPED = "Deduped"


class InvokeType(StrEnum):
    """Fabric Job Scheduler invoke type values.

    Reference: https://learn.microsoft.com/en-us/rest/api/fabric/core/job-scheduler/list-item-job-instances
    """

    SCHEDULED = "Scheduled"
    MANUAL = "Manual"


@dataclass
class FabricConnection:
    """A Fabric connection parsed from the Core API List Connections response.

    Used by lineage resolvers to map an activity's connection reference to a
    DataHub platform via FABRIC_CONNECTION_PLATFORM_MAP.

    Reference: https://learn.microsoft.com/en-us/rest/api/fabric/core/connections/list-connections
    """

    id: str
    display_name: str
    connection_type: Optional[
        str
    ]  # connectionDetails.type — e.g. "SQL", "Snowflake", "AmazonS3"
    connection_path: Optional[str] = (
        None  # connectionDetails.path — e.g. "server;database"
    )

    @classmethod
    def from_dict(cls, data: dict) -> "FabricConnection":
        connection_details = data.get("connectionDetails") or {}
        return cls(
            id=data["id"],
            display_name=data.get("displayName", ""),
            connection_type=connection_details.get("type"),
            connection_path=connection_details.get("path"),
        )


@dataclass
class FabricJobInstance:
    """A single job execution instance from the Fabric Job Scheduler API.

    Returned by GET /workspaces/{workspaceId}/items/{itemId}/jobs/instances.
    Generic across all item types (pipelines, copy jobs, dataflows, etc.).

    Reference: https://learn.microsoft.com/en-us/rest/api/fabric/core/job-scheduler/list-item-job-instances
    """

    id: str
    item_id: str
    workspace_id: str
    status: str
    start_time_utc: Optional[str] = None  # ISO 8601
    end_time_utc: Optional[str] = None  # ISO 8601
    invoke_type: Optional[str] = None
    failure_reason: Optional[str] = None
