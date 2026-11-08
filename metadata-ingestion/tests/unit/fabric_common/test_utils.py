"""Unit tests for shared Fabric connector utilities."""

from datetime import datetime, timezone

import pytest

from datahub.ingestion.source.fabric.common.models import FabricWorkspace
from datahub.ingestion.source.fabric.common.utils import (
    build_workspace_container,
    parse_iso_datetime,
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


@pytest.mark.parametrize(
    "iso_str",
    ["2024-03-01T10:20:30Z", "2024-03-01T10:20:30", "2024-03-01T11:20:30+01:00"],
)
def test_fabric_timestamps_parse_to_the_same_utc_instant(iso_str: str) -> None:
    """Fabric mixes a Z suffix, an explicit offset and no zone at all for the
    same instant; all must compare equal once parsed."""
    assert parse_iso_datetime(iso_str) == datetime(
        2024, 3, 1, 10, 20, 30, tzinfo=timezone.utc
    )
