"""Common container keys for Microsoft Fabric connectors."""

from datahub.emitter.mcp_builder import ContainerKey


class WorkspaceKey(ContainerKey):
    """Container key for Fabric workspaces."""

    workspace_id: str
