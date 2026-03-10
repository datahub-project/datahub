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
from datahub.sdk.container import Container

# Shared platform name for Fabric workspace containers.
# All Fabric connectors (OneLake, Data Factory, etc.) use this so that a
# workspace appears as a single node in DataHub regardless of which
# connector ingested it.
FABRIC_WORKSPACE_PLATFORM = "fabric"


class WorkspaceKey(ContainerKey):
    """Container key for Microsoft Fabric workspaces.

    Shared across all Fabric connectors so that workspaces produce the same
    URN regardless of which connector is running.
    """

    workspace_id: str


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
    container_key = WorkspaceKey(
        platform=FABRIC_WORKSPACE_PLATFORM,
        instance=platform_instance,
        env=env,
        workspace_id=workspace.id,
    )
    yield Container(
        container_key=container_key,
        display_name=workspace.name,
        description=workspace.description,
        subtype=GenericContainerSubTypes.FABRIC_WORKSPACE,
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
