"""Unit tests for shared Fabric connector utilities."""

from datahub.ingestion.source.fabric.common.models import (
    FabricWorkspace,
    default_identity_user_principal_name,
)
from datahub.ingestion.source.fabric.common.utils import (
    build_workspace_container,
    owners_from_user_principal_name,
)


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


def test_default_identity_user_principal_name() -> None:
    item = {
        "defaultIdentity": {
            "id": "obj-1",
            "displayName": "Jane Doe",
            "type": "User",
            "userDetails": {"userPrincipalName": "jane.doe@contoso.com"},
        }
    }

    assert default_identity_user_principal_name(item) == "jane.doe@contoso.com"
    assert default_identity_user_principal_name({}) is None
    assert (
        default_identity_user_principal_name(
            {"defaultIdentity": {"type": "ServicePrincipal"}}
        )
        is None
    )
    assert owners_from_user_principal_name("jane.doe@contoso.com") == [
        "jane.doe@contoso.com"
    ]
    assert owners_from_user_principal_name(None) is None
