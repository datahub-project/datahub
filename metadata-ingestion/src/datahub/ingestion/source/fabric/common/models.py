"""Shared data models for Microsoft Fabric Core API entities.

These models correspond to the Fabric Core API (api.fabric.microsoft.com/v1)
and are shared across all Fabric connectors.

References:
- Workspaces API: https://learn.microsoft.com/en-us/rest/api/fabric/core/workspaces
- Items API: https://learn.microsoft.com/en-us/rest/api/fabric/core/items
"""

from dataclasses import dataclass
from typing import Iterable, Optional

from datahub.emitter.mcp_builder import ContainerKey
from datahub.ingestion.source.common.subtypes import GenericContainerSubTypes
from datahub.ingestion.source.fabric.common.constants import FABRIC_APP_BASE_URL
from datahub.sdk.container import Container
from datahub.utilities.str_enum import StrEnum

# Shared platform name for Fabric workspace containers.
# All Fabric connectors (OneLake, Data Factory, etc.) use this so that a
# workspace appears as a single node in DataHub regardless of which
# connector ingested it.
FABRIC_WORKSPACE_PLATFORM = "fabric"


class FabricItemType:
    DATA_PIPELINE = "DataPipeline"


class WorkspaceKey(ContainerKey):
    """Container key for Microsoft Fabric workspaces.

    Shared across all Fabric connectors so that workspaces produce the same
    URN regardless of which connector is running.
    """

    workspace_id: str


def make_workspace_key(
    workspace_id: str,
    platform_instance: Optional[str],
    env: Optional[str],
) -> WorkspaceKey:
    """Create the canonical WorkspaceKey for a Fabric workspace."""
    return WorkspaceKey(
        platform=FABRIC_WORKSPACE_PLATFORM,
        instance=platform_instance,
        env=env,
        workspace_id=workspace_id,
    )


def build_workspace_container(
    workspace: "FabricWorkspace",
    platform_instance: Optional[str],
    env: Optional[str],
) -> Iterable[Container]:
    """Yield the workspace Container for a Fabric workspace.

    Shared by all Fabric connectors so that the same physical workspace
    produces exactly one container node in DataHub regardless of which
    connector ingested it.
    """
    container_key = make_workspace_key(
        workspace_id=workspace.id,
        platform_instance=platform_instance,
        env=env,
    )
    yield Container(
        container_key=container_key,
        display_name=workspace.name,
        description=workspace.description,
        subtype=GenericContainerSubTypes.FABRIC_WORKSPACE,
        external_url=f"{FABRIC_APP_BASE_URL}/groups/{workspace.id}/list",
        parent_container=None,
        qualified_name=workspace.id,
    )


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
    connection_type: str  # connectionDetails.type — e.g. "SQL", "Snowflake", "AmazonS3"
    connection_path: Optional[str] = (
        None  # connectionDetails.path — e.g. "server;database"
    )

    @classmethod
    def from_dict(cls, data: dict) -> "FabricConnection":
        connection_details = data.get("connectionDetails") or {}
        return cls(
            id=data["id"],
            display_name=data.get("displayName", ""),
            connection_type=connection_details.get("type", ""),
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
