"""Unit tests for common Microsoft Fabric keys."""

from datahub.emitter.mce_builder import datahub_guid
from datahub.ingestion.source.fabric.common.keys import WorkspaceKey


def test_workspace_key_guid_uses_fabric_platform() -> None:
    key = WorkspaceKey(
        platform="fabric",
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
