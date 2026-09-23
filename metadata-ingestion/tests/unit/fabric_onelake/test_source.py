"""Unit tests for Fabric OneLake source key hierarchy and item processing."""

from typing import Iterator, List
from unittest.mock import MagicMock, patch

import pytest

from datahub.emitter.mce_builder import datahub_guid
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.fabric.common.models import (
    FABRIC_WORKSPACE_PLATFORM,
    FabricWorkspace,
    WorkspaceKey,
)
from datahub.ingestion.source.fabric.onelake.models import FabricLakehouse
from datahub.ingestion.source.fabric.onelake.source import (
    PLATFORM,
    FabricOneLakeSource,
    LakehouseKey,
    LakehouseSchemaKey,
    WarehouseKey,
    WarehouseSchemaKey,
    _iter_isolated,
)


def test_lakehouse_parent_workspace_uses_fabric_platform() -> None:
    key = LakehouseKey(
        platform=PLATFORM,
        instance="instance-1",
        env="PROD",
        workspace_id="ws-123",
        lakehouse_id="lh-456",
    )

    parent = key.parent_key()
    assert isinstance(parent, WorkspaceKey)
    assert parent.platform == FABRIC_WORKSPACE_PLATFORM
    assert parent.workspace_id == "ws-123"


def test_warehouse_parent_workspace_uses_fabric_platform() -> None:
    key = WarehouseKey(
        platform=PLATFORM,
        instance="instance-1",
        env="PROD",
        workspace_id="ws-123",
        warehouse_id="wh-456",
    )

    parent = key.parent_key()
    assert isinstance(parent, WorkspaceKey)
    assert parent.platform == FABRIC_WORKSPACE_PLATFORM
    assert parent.workspace_id == "ws-123"


def test_schema_parent_chain_keeps_onelake_then_fabric() -> None:
    schema_key = LakehouseSchemaKey(
        platform=PLATFORM,
        instance="instance-1",
        env="PROD",
        workspace_id="ws-123",
        lakehouse_id="lh-456",
        schema_name="dbo",
    )

    lakehouse_parent = schema_key.parent_key()
    assert isinstance(lakehouse_parent, LakehouseKey)
    assert lakehouse_parent.platform == PLATFORM

    workspace_parent = lakehouse_parent.parent_key()
    assert isinstance(workspace_parent, WorkspaceKey)
    assert workspace_parent.platform == FABRIC_WORKSPACE_PLATFORM


def test_warehouse_schema_parent_chain_keeps_onelake_then_fabric() -> None:
    schema_key = WarehouseSchemaKey(
        platform=PLATFORM,
        instance="instance-1",
        env="PROD",
        workspace_id="ws-123",
        warehouse_id="wh-456",
        schema_name="sales",
    )

    warehouse_parent = schema_key.parent_key()
    assert isinstance(warehouse_parent, WarehouseKey)
    assert warehouse_parent.platform == PLATFORM

    workspace_parent = warehouse_parent.parent_key()
    assert isinstance(workspace_parent, WorkspaceKey)
    assert workspace_parent.platform == FABRIC_WORKSPACE_PLATFORM


def test_lakehouse_key_guid_uses_fabric_onelake_platform() -> None:
    key = LakehouseKey(
        platform=PLATFORM,
        instance="instance-1",
        env="PROD",
        workspace_id="ws-123",
        lakehouse_id="lh-456",
    )

    expected = datahub_guid(
        {
            "platform": "fabric-onelake",
            "instance": "instance-1",
            "workspace_id": "ws-123",
            "lakehouse_id": "lh-456",
        }
    )
    assert key.guid() == expected


def test_warehouse_key_guid_uses_fabric_onelake_platform() -> None:
    key = WarehouseKey(
        platform=PLATFORM,
        instance="instance-1",
        env="PROD",
        workspace_id="ws-123",
        warehouse_id="wh-456",
    )

    expected = datahub_guid(
        {
            "platform": "fabric-onelake",
            "instance": "instance-1",
            "workspace_id": "ws-123",
            "warehouse_id": "wh-456",
        }
    )
    assert key.guid() == expected


def test_norm_respects_convert_urns_to_lowercase() -> None:
    """_norm lowercases identifiers iff convert_urns_to_lowercase=True.

    _norm gates URN-bound identifier casing so the dataset URNs match what
    sqlglot emits during view-lineage parsing. Bypassing __init__ is fine here
    because _norm only reads self.config.convert_urns_to_lowercase.
    """
    src = MagicMock()

    src.config.convert_urns_to_lowercase = True
    assert FabricOneLakeSource._norm(src, "Sales") == "sales"
    assert FabricOneLakeSource._norm(src, "CUSTOMERS") == "customers"
    assert FabricOneLakeSource._norm(src, "already_lower") == "already_lower"

    src.config.convert_urns_to_lowercase = False
    assert FabricOneLakeSource._norm(src, "Sales") == "Sales"
    assert FabricOneLakeSource._norm(src, "CUSTOMERS") == "CUSTOMERS"


_WORKSPACE = FabricWorkspace(id="ws-1", name="Analytics")


def _make_source() -> FabricOneLakeSource:
    return FabricOneLakeSource.create(
        {
            "credential": {
                "authentication_method": "service_principal",
                "client_id": "test-client",
                "client_secret": "test-secret",
                "tenant_id": "test-tenant",
            },
        },
        PipelineContext(run_id="fabric-onelake-unit"),
    )


def _lakehouse(item_id: str, name: str) -> FabricLakehouse:
    return FabricLakehouse(
        id=item_id, name=name, workspace_id=_WORKSPACE.id, type="Lakehouse"
    )


def test_iter_isolated_reports_producer_errors() -> None:
    def producer() -> Iterator[int]:
        yield 1
        raise RuntimeError("listing broke")

    errors: List[Exception] = []
    assert list(_iter_isolated(producer(), on_error=errors.append)) == [1]
    assert len(errors) == 1 and isinstance(errors[0], RuntimeError)


def test_iter_isolated_does_not_swallow_consumer_errors() -> None:
    """An exception thrown in at the yield point is the consumer's, not the
    producer's: it must propagate rather than be reported as a fetch failure."""
    errors: List[Exception] = []
    wrapped = _iter_isolated(iter([1, 2, 3]), on_error=errors.append)
    assert next(wrapped) == 1
    with pytest.raises(ValueError, match="sink failed"):
        wrapped.throw(ValueError("sink failed"))
    assert errors == []


def test_failing_lakehouse_does_not_skip_the_next_one() -> None:
    source = _make_source()
    broken, healthy = _lakehouse("lh-1", "broken_lh"), _lakehouse("lh-2", "ok_lh")

    def process(
        workspace: FabricWorkspace, lakehouse: FabricLakehouse
    ) -> Iterator[str]:
        yield f"container:{lakehouse.id}"
        if lakehouse.id == broken.id:
            raise RuntimeError("tables API failed")
        yield f"table:{lakehouse.id}"

    with patch.object(source, "_process_lakehouse", side_effect=process):
        emitted = list(
            source._process_workspace_items(_WORKSPACE, [broken, healthy], [])
        )

    assert emitted == ["container:lh-1", "container:lh-2", "table:lh-2"]
    warnings = [
        w for w in source.report.warnings if w.title == "Failed to Process Lakehouse"
    ]
    assert len(warnings) == 1
    assert any("broken_lh" in c for c in warnings[0].context)
    source.close()
