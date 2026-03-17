"""Common container keys for Microsoft Fabric connectors."""

from pydantic import model_validator

from datahub.emitter.mcp_builder import ContainerKey

WORKSPACE_PLATFORM = "fabric"


class WorkspaceKey(ContainerKey):
    """Container key for Fabric workspaces."""

    platform: str = WORKSPACE_PLATFORM
    workspace_id: str

    @model_validator(mode="after")
    def _validate_workspace_platform(self) -> "WorkspaceKey":
        if type(self) is WorkspaceKey and self.platform != WORKSPACE_PLATFORM:
            raise ValueError(
                f"WorkspaceKey platform must be '{WORKSPACE_PLATFORM}', got '{self.platform}'"
            )
        return self
