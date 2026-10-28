"""Unit tests for Fabric OneLake source key hierarchy."""

import pathlib
from typing import Callable, Dict, Iterator, List, Optional, Tuple
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
from datahub.ingestion.source.fabric.onelake.client import IncompleteCatalogListing
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
from datahub.ingestion.source.state.entity_removal_state import GenericCheckpointState
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalHandler,
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


def test_failed_table_listing_warns_without_failing_the_run() -> None:
    src = MagicMock()
    src.client.list_lakehouse_tables.side_effect = IncompleteCatalogListing("truncated")

    emitted = list(
        FabricOneLakeSource._process_item_tables(
            src,
            FabricWorkspace(id="ws-1", name="workspace"),
            "lh-1",
            "Lakehouse",
            LakehouseKey(
                platform=PLATFORM,
                env="PROD",
                workspace_id="ws-1",
                lakehouse_id="lh-1",
            ),
            item_display_name="Lakehouse",
            schema_map={},
            emitted_schemas=set(),
            profile_targets=[],
            shortcuts={},
        )
    )

    assert emitted == []
    src.report.warning.assert_called_once()
    src.report.failure.assert_not_called()
    src._preserve_item_urns_from_last_checkpoint.assert_called_once_with(
        "ws-1", "lh-1", "Lakehouse"
    )


def _checkpoint_handler(
    urns: List[str],
) -> Tuple[StaleEntityRemovalHandler, GenericCheckpointState]:
    state_provider = MagicMock()
    state_provider.is_stateful_ingestion_configured.return_value = True
    config = MagicMock()
    config.stateful_ingestion.remove_stale_metadata = True
    config.stateful_ingestion.ignore_new_state = False
    config.stateful_ingestion.ignore_old_state = False

    current_state = GenericCheckpointState()
    current_checkpoint = MagicMock()
    current_checkpoint.state = current_state
    state_provider.get_current_checkpoint.return_value = current_checkpoint

    last_checkpoint = MagicMock()
    last_checkpoint.state = GenericCheckpointState(urns=urns)
    state_provider.get_last_checkpoint.return_value = last_checkpoint

    handler = StaleEntityRemovalHandler(
        state_provider=state_provider,
        report=MagicMock(),
        config=config,
        state_type_class=GenericCheckpointState,
        pipeline_name="fabric-onelake",
        run_id="run",
        platform=PLATFORM,
    )
    return handler, current_state


def test_failed_listing_keeps_only_that_items_previous_urns() -> None:
    kept = (
        "urn:li:dataset:(urn:li:dataPlatform:fabric-onelake,ws-1.lh-1.dbo.orders,PROD)"
    )
    other_item = (
        "urn:li:dataset:(urn:li:dataPlatform:fabric-onelake,ws-1.lh-2.dbo.orders,PROD)"
    )
    other_platform = (
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,ws-1.lh-1.dbo.orders,PROD)"
    )
    handler, current_state = _checkpoint_handler(
        [kept, other_item, other_platform, "urn:li:container:already-there"]
    )

    src = MagicMock()
    src._stale_handler = handler
    src.config.platform_instance = None
    src.config.extract_schemas = True
    src.config.env = "PROD"
    src._make_schema_key = lambda workspace_id, item_id, item_type, schema_name: (
        FabricOneLakeSource._make_schema_key(
            src, workspace_id, item_id, item_type, schema_name
        )
    )

    FabricOneLakeSource._preserve_item_urns_from_last_checkpoint(
        src, "ws-1", "lh-1", "Lakehouse"
    )

    schema_urn = LakehouseSchemaKey(
        platform=PLATFORM,
        env="PROD",
        workspace_id="ws-1",
        lakehouse_id="lh-1",
        schema_name="dbo",
    ).as_urn()
    assert current_state.urns == [kept, schema_urn]


def test_warehouse_without_catalog_is_reported_as_info() -> None:
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
    assert list(source.report.warnings) == []
    assert [i.title for i in source.report.infos] == ["Warehouse Tables Unavailable"]


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


def test_workunit_processors_keep_the_stale_removal_handler(
    tmp_path: pathlib.Path,
) -> None:
    source = FabricOneLakeSource.create(
        {
            "stateful_ingestion": {
                "enabled": True,
                "state_provider": {
                    "type": "file",
                    "config": {"filename": str(tmp_path / "state.json")},
                },
            }
        },
        PipelineContext(run_id="run", pipeline_name="fabric"),
    )

    source.get_workunit_processors()

    assert isinstance(source._stale_handler, StaleEntityRemovalHandler)
