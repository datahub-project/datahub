"""Unit tests for Fabric OneLake source key hierarchy."""

from typing import Callable, Dict, Iterator, List, Optional
from unittest.mock import MagicMock

import pytest

from datahub.emitter.mce_builder import datahub_guid
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.fabric.common.models import (
    FABRIC_WORKSPACE_PLATFORM,
    FabricWorkspace,
    WorkspaceKey,
)
from datahub.ingestion.source.fabric.onelake.models import FabricTable
from datahub.ingestion.source.fabric.onelake.profiling import FabricProfileTarget
from datahub.ingestion.source.fabric.onelake.source import (
    PLATFORM,
    FabricOneLakeSource,
    LakehouseKey,
    LakehouseSchemaKey,
    WarehouseKey,
    WarehouseSchemaKey,
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


def test_usage_table_allow_list_is_case_insensitive() -> None:
    src = MagicMock()
    src._ingested_dataset_names = set()
    FabricOneLakeSource._register_ingested_dataset(src, "ws-1.wh-1.dbo.Orders")

    assert FabricOneLakeSource._is_usage_table_allowed(src, "ws-1.wh-1.dbo.orders")
    assert not FabricOneLakeSource._is_usage_table_allowed(
        src, "ws-1.wh-1.information_schema.columns"
    )
    assert not FabricOneLakeSource._is_usage_table_allowed(src, "ws-1.wh-1.dbo.tab")


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


def test_warehouse_without_catalog_is_reported_as_a_warning() -> None:
    source = FabricOneLakeSource.create({}, PipelineContext(run_id="wh-404"))

    def list_warehouse_tables(
        workspace_id: str,
        warehouse_id: str,
        *,
        on_degraded: Optional[Callable[[str], None]] = None,
    ) -> Iterator[FabricTable]:
        assert on_degraded is not None
        on_degraded(f"warehouse {warehouse_id} has no OneLake schema catalog")
        return iter([])

    source.client = MagicMock()
    source.client.list_warehouse_tables.side_effect = list_warehouse_tables
    workspace = FabricWorkspace(id="ws-1", name="sales-ws")

    emitted = list(
        source._process_item_tables(
            workspace=workspace,
            item_id="wh-1",
            item_type="Warehouse",
            item_container_key=WarehouseKey(workspace_id="ws-1", warehouse_id="wh-1"),
            item_display_name="staging_wh",
            schema_map={},
            emitted_schemas=set(),
            profile_targets=[],
        )
    )

    assert emitted == []
    titles = [w.title for w in source.report.warnings]
    assert titles == ["Warehouse Tables Unavailable"]


def _profiling_source(run_id: str) -> FabricOneLakeSource:
    return FabricOneLakeSource.create(
        {
            "profiling": {"enabled": True},
            "table_pattern": {"deny": ["dbo.orders_staging"]},
        },
        PipelineContext(run_id=run_id),
    )


def test_only_ingested_tables_become_profile_targets() -> None:
    source = _profiling_source("profile-targets")
    source.client = MagicMock()
    source.client.list_warehouse_tables.return_value = iter(
        [
            FabricTable(
                name=name, schema_name="dbo", item_id="wh-1", workspace_id="ws-1"
            )
            for name in ["orders", "orders_staging"]
        ]
    )
    profile_targets: List[FabricProfileTarget] = []

    list(
        source._process_item_tables(
            workspace=FabricWorkspace(id="ws-1", name="sales-ws"),
            item_id="wh-1",
            item_type="Warehouse",
            item_container_key=WarehouseKey(workspace_id="ws-1", warehouse_id="wh-1"),
            item_display_name="sales_wh",
            schema_map={},
            emitted_schemas=set(),
            profile_targets=profile_targets,
        )
    )

    assert profile_targets == [
        FabricProfileTarget(
            schema_name="dbo", table_name="orders", dataset_name="ws-1.wh-1.dbo.orders"
        )
    ]


_TARGET = FabricProfileTarget(
    schema_name="dbo", table_name="orders", dataset_name="ws-1.wh-1.dbo.orders"
)


def test_profile_item_profiles_on_the_item_engine(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = _profiling_source("profile-item")
    schema_client = MagicMock()
    profile_wu = MagicMock(spec=MetadataWorkUnit)
    seen: Dict[str, object] = {}

    def emit_dataset_profiles(**kwargs: object) -> Iterator[MetadataWorkUnit]:
        seen.update(kwargs)
        yield profile_wu

    monkeypatch.setattr(
        "datahub.ingestion.source.fabric.onelake.source.emit_dataset_profiles",
        emit_dataset_profiles,
    )

    workunits = list(source._profile_item("ws-1", "wh-1", schema_client, [_TARGET]))

    assert workunits == [profile_wu]
    schema_client.get_engine.assert_called_once_with("ws-1", "wh-1")
    assert seen["engine"] is schema_client.get_engine.return_value
    assert seen["targets"] == [_TARGET]


def test_profile_item_without_endpoint_warns_and_skips() -> None:
    source = _profiling_source("profile-no-endpoint")

    assert list(source._profile_item("ws-1", "wh-1", None, [_TARGET])) == []
    assert [w.title for w in source.report.warnings] == ["Profiling Skipped"]


def test_profile_item_engine_failure_warns_and_skips() -> None:
    source = _profiling_source("profile-engine-error")
    schema_client = MagicMock()
    schema_client.get_engine.side_effect = ValueError("no endpoint")

    assert list(source._profile_item("ws-1", "wh-1", schema_client, [_TARGET])) == []
    assert [w.title for w in source.report.warnings] == ["Profiling Skipped"]
