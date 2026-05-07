"""Shared utility functions for Microsoft Fabric connectors."""

from datetime import datetime, timezone
from typing import Iterable, Optional

from datahub.ingestion.source.common.subtypes import GenericContainerSubTypes
from datahub.ingestion.source.fabric.common.constants import FABRIC_APP_BASE_URL
from datahub.ingestion.source.fabric.common.models import (
    FabricWorkspace,
    WorkspaceKey,
)
from datahub.sdk.container import Container


def parse_iso_datetime(iso_str: str) -> datetime:
    """Parse an ISO 8601 timestamp string into a timezone-aware datetime.

    Handles the Fabric API conventions:
    - "Z" suffix for UTC
    - Missing timezone (assumed UTC)
    """
    dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def make_workspace_key(
    workspace_id: str,
    platform_instance: Optional[str],
    env: Optional[str],
) -> WorkspaceKey:
    """Create the canonical WorkspaceKey for a Fabric workspace."""
    return WorkspaceKey(
        instance=platform_instance,
        env=env,
        workspace_id=workspace_id,
    )


def build_workspace_container(
    workspace: FabricWorkspace,
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
