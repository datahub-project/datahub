"""Unit tests for Fabric OneLake source key hierarchy."""

from unittest.mock import MagicMock

from datahub.emitter.mce_builder import datahub_guid
from datahub.ingestion.source.fabric.common.models import (
    FABRIC_WORKSPACE_PLATFORM,
    FabricWorkspace,
    WorkspaceKey,
)
from datahub.ingestion.source.fabric.onelake.client import IncompleteCatalogListing
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
    urns: list[str],
) -> tuple[MagicMock, GenericCheckpointState]:
    state_provider = MagicMock()
    state_provider.is_stateful_ingestion_configured.return_value = True
    state_provider._usecase_handlers = {}

    def register(handler: StaleEntityRemovalHandler) -> None:
        state_provider._usecase_handlers[handler.job_id] = handler

    state_provider.register_stateful_ingestion_usecase_handler.side_effect = register

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

    StaleEntityRemovalHandler(
        state_provider=state_provider,
        report=MagicMock(),
        config=config,
        state_type_class=GenericCheckpointState,
        pipeline_name="fabric-onelake",
        run_id="run",
        platform=PLATFORM,
    )
    return state_provider, current_state


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
    state_provider, current_state = _checkpoint_handler(
        [kept, other_item, other_platform, "urn:li:container:already-there"]
    )

    src = MagicMock()
    src.state_provider = state_provider
    src.config.platform_instance = None
    src.config.extract_schemas = True
    src.config.env = "PROD"
    src._stale_entity_removal_handler = lambda: (
        FabricOneLakeSource._stale_entity_removal_handler(src)
    )
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
