"""Unit tests for common Microsoft Fabric keys."""

import pytest
from pydantic import ValidationError

from datahub.emitter.mce_builder import datahub_guid
from datahub.ingestion.source.fabric.common import (
    WORKSPACE_PLATFORM as WORKSPACE_PLATFORM_FROM_PACKAGE,
)
from datahub.ingestion.source.fabric.common.keys import (
    WORKSPACE_PLATFORM,
    WorkspaceKey,
)


def test_workspace_key_guid_uses_fabric_platform() -> None:
    key = WorkspaceKey(
        instance="instance-1",
        env="PROD",
        workspace_id="ws-123",
    )

    expected = datahub_guid(
        {
            "platform": "fabric",
            "instance": "instance-1",
            "workspace_id": "ws-123",
        }
    )
    not_expected = datahub_guid(
        {
            "platform": "fabric-onelake",
            "instance": "instance-1",
            "workspace_id": "ws-123",
        }
    )

    assert key.guid() == expected
    assert key.guid() != not_expected


def test_workspace_key_rejects_non_fabric_platform() -> None:
    with pytest.raises(ValidationError):
        WorkspaceKey(
            platform="fabric-onelake",
            instance="instance-1",
            env="PROD",
            workspace_id="ws-123",
        )

    key = WorkspaceKey(instance="instance-1", env="PROD", workspace_id="ws-123")
    assert key.platform == WORKSPACE_PLATFORM


def test_workspace_exports_from_fabric_common_package() -> None:
    assert WORKSPACE_PLATFORM_FROM_PACKAGE == WORKSPACE_PLATFORM
