"""Common container keys for Microsoft Fabric connectors."""

from typing import Literal

from datahub.emitter.mcp_builder import ContainerKey

WORKSPACE_PLATFORM = "fabric"


class WorkspaceKey(ContainerKey):
    """Container key for Fabric workspaces."""

    platform: Literal["fabric"] = WORKSPACE_PLATFORM
    workspace_id: str
