"""Shared utility functions for Microsoft Fabric connectors."""

from datetime import datetime, timezone
from typing import Iterable, List, Optional, Tuple

from datahub.ingestion.source.common.subtypes import GenericContainerSubTypes
from datahub.ingestion.source.fabric.common.constants import FABRIC_APP_BASE_URL
from datahub.ingestion.source.fabric.common.models import (
    FabricWorkspace,
    WorkspaceKey,
)
from datahub.ingestion.source.fabric.common.urn_generator import make_workspace_name
from datahub.metadata.urns import CorpUserUrn
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


def owners_from_user_principal_name(
    user_principal_name: Optional[str],
    *,
    parent_user_principal_name: Optional[str] = None,
    ownership_type: str = "TECHNICAL_OWNER",
    cascade: bool = False,
) -> Optional[List[Tuple[CorpUserUrn, str]]]:
    """Return DataHub owners for a Fabric userPrincipalName.

    Uses the entity's own userPrincipalName. When that is missing and
    ``cascade`` is set, uses the parent entity's userPrincipalName instead.
    ``ownership_type`` is the DataHub ownership type (for example
    TECHNICAL_OWNER or BUSINESS_OWNER).
    """
    principal_name = user_principal_name
    if not principal_name and cascade:
        principal_name = parent_user_principal_name
    if not principal_name:
        return None
    return [(CorpUserUrn(principal_name), ownership_type)]


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
        qualified_name=make_workspace_name(workspace.id),
    )
