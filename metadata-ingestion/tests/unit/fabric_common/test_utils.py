"""Unit tests for shared Fabric connector utilities."""

from datahub.ingestion.source.fabric.common.models import FabricWorkspace
from datahub.ingestion.source.fabric.common.utils import build_workspace_container


def test_workspace_container_includes_external_url() -> None:
    workspace = FabricWorkspace(
        id="ws-123",
        name="Test Workspace",
        description="Test description",
    )

    container = next(
        iter(
            build_workspace_container(
                workspace=workspace,
                platform_instance="instance-1",
                env="PROD",
            )
        )
    )

    assert (
        container.external_url
        == f"https://app.fabric.microsoft.com/groups/{workspace.id}/list"
    )
