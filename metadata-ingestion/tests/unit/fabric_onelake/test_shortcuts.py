"""Lakehouse shortcut parsing, origin properties, and upstream lineage."""

from unittest.mock import MagicMock

from datahub.emitter.mce_builder import make_tag_urn
from datahub.ingestion.source.fabric.common.models import FabricWorkspace
from datahub.ingestion.source.fabric.onelake.models import FabricColumn, FabricTable
from datahub.ingestion.source.fabric.onelake.shortcuts import (
    matching_column_pairs,
    parse_table_shortcut,
)
from datahub.ingestion.source.fabric.onelake.source import (
    PLATFORM,
    FabricOneLakeSource,
    LakehouseKey,
)
from datahub.metadata.schema_classes import DatasetLineageTypeClass

ONELAKE_SHORTCUT = {
    "path": "Tables/dbo",
    "name": "customers",
    "target": {
        "type": "OneLake",
        "oneLake": {
            "workspaceId": "ws-1",
            "itemId": "lh-2",
            "path": "Tables/sales/orders",
        },
    },
}


def test_schemas_enabled_onelake_shortcut() -> None:
    shortcut = parse_table_shortcut(ONELAKE_SHORTCUT)
    assert shortcut is not None
    assert shortcut.lookup_key() == ("dbo", "customers")
    assert shortcut.origin_name == "orders"
    assert shortcut.origin_path == "Tables/sales/orders"
    assert shortcut.upstream_workspace_id == "ws-1"
    assert shortcut.upstream_item_id == "lh-2"
    assert shortcut.upstream_schema_name == "sales"
    assert shortcut.upstream_table_name == "orders"


def test_schemas_disabled_shortcut_defaults_schema_to_dbo() -> None:
    shortcut = parse_table_shortcut(
        {
            "path": "Tables",
            "name": "customers",
            "target": {
                "type": "OneLake",
                "oneLake": {
                    "workspaceId": "ws-1",
                    "itemId": "lh-2",
                    "path": "Tables/orders",
                },
            },
        }
    )
    assert shortcut is not None
    assert shortcut.schema_name == "dbo"
    assert shortcut.table_name == "customers"
    assert shortcut.origin_name == "orders"
    assert shortcut.upstream_schema_name == "dbo"


def test_file_shortcut_is_ignored() -> None:
    assert (
        parse_table_shortcut(
            {
                "path": "Files/raw",
                "name": "landing",
                "target": {"type": "AmazonS3"},
            }
        )
        is None
    )


def test_external_shortcut_has_origin_and_no_upstream() -> None:
    shortcut = parse_table_shortcut(
        {
            "path": "Tables/dbo",
            "name": "events",
            "target": {
                "type": "AmazonS3",
                "amazonS3": {
                    "location": "https://bucket.s3.us-east-1.amazonaws.com",
                    "subpath": "raw/events",
                },
            },
        }
    )
    assert shortcut is not None
    assert shortcut.origin_name == "events"
    assert (
        shortcut.origin_path == "https://bucket.s3.us-east-1.amazonaws.com/raw/events"
    )
    assert shortcut.upstream_workspace_id is None


def test_dataverse_origin_is_table_name() -> None:
    shortcut = parse_table_shortcut(
        {
            "path": "Tables/dbo",
            "name": "accounts_shortcut",
            "target": {
                "type": "Dataverse",
                "dataverse": {
                    "environmentDomain": "https://org.crm.dynamics.com",
                    "deltaLakeFolder": "deltalake",
                    "tableName": "account",
                },
            },
        }
    )
    assert shortcut is not None
    assert shortcut.origin_name == "account"
    assert shortcut.origin_path == "https://org.crm.dynamics.com/deltalake/account"
    assert shortcut.upstream_item_id is None


def test_shortcut_upstream_uses_copy_lineage() -> None:
    src = MagicMock()
    src.config.shortcuts.include_lineage = True
    src.config.platform_instance = None
    src.config.env = "PROD"
    src._norm.side_effect = lambda name: name.lower()
    src._shortcut_upstream_dataset_name.side_effect = lambda shortcut: (
        FabricOneLakeSource._shortcut_upstream_dataset_name(src, shortcut)
    )
    src._copy_lineage.side_effect = (
        lambda upstream_dataset_name, column_pairs, downstream_urn=None: (
            FabricOneLakeSource._copy_lineage(
                src,
                upstream_dataset_name,
                column_pairs,
                downstream_urn=downstream_urn,
            )
        )
    )

    shortcut = parse_table_shortcut(
        {
            **ONELAKE_SHORTCUT,
            "target": {
                "type": "OneLake",
                "oneLake": {
                    "workspaceId": "ws-1",
                    "itemId": "lh-2",
                    "path": "Tables/Sales/Orders",
                },
            },
        }
    )
    assert shortcut is not None
    lineage = FabricOneLakeSource._shortcut_upstream(src, shortcut)
    assert lineage is not None
    assert len(lineage.upstreams) == 1
    assert lineage.upstreams[0].type == DatasetLineageTypeClass.COPY
    assert lineage.upstreams[0].dataset == (
        "urn:li:dataset:(urn:li:dataPlatform:fabric-onelake,ws-1.lh-2.sales.orders,PROD)"
    )


def _lineage_source() -> MagicMock:
    src = MagicMock()
    src.config.platform_instance = None
    src.config.env = "PROD"
    src.config.shortcuts.include_lineage = True
    src._norm.side_effect = lambda name: name
    src._register_ingested_dataset.return_value = None
    src._ingested_columns = {}
    src._deferred_shortcut_datasets = []
    src._shortcut_properties.side_effect = lambda shortcut: (
        FabricOneLakeSource._shortcut_properties(src, shortcut)
    )
    src._shortcut_upstream_dataset_name.side_effect = lambda shortcut: (
        FabricOneLakeSource._shortcut_upstream_dataset_name(src, shortcut)
    )
    src._shortcut_upstream.side_effect = lambda shortcut: (
        FabricOneLakeSource._shortcut_upstream(src, shortcut)
    )
    src._copy_lineage.side_effect = (
        lambda upstream_dataset_name, column_pairs, downstream_urn=None: (
            FabricOneLakeSource._copy_lineage(
                src,
                upstream_dataset_name,
                column_pairs,
                downstream_urn=downstream_urn,
            )
        )
    )
    src._resolve_workspace_display_name.return_value = "Sales Workspace"
    src._resolve_item_display_name.return_value = "Sales Lakehouse"
    src.report.shortcuts_found = 0
    return src


def _column(name: str) -> FabricColumn:
    return FabricColumn(name=name, data_type="varchar", is_nullable=True)


def _table_dataset(
    src: MagicMock,
    *,
    item_id: str,
    schema_name: str,
    table_name: str,
    columns: list[FabricColumn],
    shortcut: object = None,
) -> list:
    return list(
        FabricOneLakeSource._create_table_dataset(
            src,
            FabricWorkspace(id="ws-1", name="workspace"),
            item_id,
            schema_name,
            FabricTable(
                name=table_name,
                schema_name=schema_name,
                item_id=item_id,
                workspace_id="ws-1",
            ),
            LakehouseKey(
                platform=PLATFORM,
                workspace_id="ws-1",
                lakehouse_id=item_id,
                env="PROD",
            ),
            columns,
            shortcut=shortcut,
        )
    )


def test_shortcut_table_has_tag_origin_properties_and_lineage() -> None:
    src = _lineage_source()

    shortcut = parse_table_shortcut(ONELAKE_SHORTCUT)
    assert shortcut is not None
    # Lineage shortcuts are held until the origin table may have been ingested.
    assert (
        _table_dataset(
            src,
            item_id="lh-1",
            schema_name="dbo",
            table_name="customers",
            columns=[],
            shortcut=shortcut,
        )
        == []
    )
    assert len(src._deferred_shortcut_datasets) == 1
    dataset = src._deferred_shortcut_datasets[0][0]
    assert str(dataset.platform) == f"urn:li:dataPlatform:{PLATFORM}"
    assert dataset.display_name == "customers"
    assert dataset.tags is not None
    assert [tag.tag for tag in dataset.tags] == [make_tag_urn("shortcut")]
    assert dataset.custom_properties == {
        "shortcut_origin_name": "orders",
        "shortcut_origin_path": "Tables/sales/orders",
        "shortcut_origin_workspace_id": "ws-1",
        "shortcut_origin_workspace_name": "Sales Workspace",
        "shortcut_origin_item_id": "lh-2",
        "shortcut_origin_item_name": "Sales Lakehouse",
    }
    assert dataset.upstreams is not None
    assert dataset.upstreams.upstreams[0].type == DatasetLineageTypeClass.COPY
    assert src.report.shortcuts_found == 1


def test_origin_names_are_resolved_once_per_workspace_and_item() -> None:
    src = MagicMock()
    src._workspace_display_names = {}
    src._item_display_names = {}
    src.client.get_workspace_display_name.return_value = "Sales Workspace"
    src.client.get_item_display_name.return_value = None

    for _ in range(3):
        assert (
            FabricOneLakeSource._resolve_workspace_display_name(src, "ws-1")
            == "Sales Workspace"
        )
        assert (
            FabricOneLakeSource._resolve_item_display_name(src, "ws-1", "lh-2") is None
        )

    src.client.get_workspace_display_name.assert_called_once_with("ws-1")
    src.client.get_item_display_name.assert_called_once_with("ws-1", "lh-2")


def test_external_shortcut_properties_omit_workspace_and_item() -> None:
    src = MagicMock()
    shortcut = parse_table_shortcut(
        {
            "path": "Tables/dbo",
            "name": "events",
            "target": {
                "type": "AdlsGen2",
                "adlsGen2": {
                    "location": "https://account.dfs.core.windows.net",
                    "subpath": "container/raw/events",
                },
            },
        }
    )
    assert shortcut is not None
    assert FabricOneLakeSource._shortcut_properties(src, shortcut) == {
        "shortcut_origin_name": "events",
        "shortcut_origin_path": "https://account.dfs.core.windows.net/container/raw/events",
    }
    src._resolve_workspace_display_name.assert_not_called()


def test_matching_column_pairs_is_case_insensitive() -> None:
    assert matching_column_pairs(
        ["orderid", "amount", "note"],
        ["OrderId", "Amount"],
    ) == [("orderid", "OrderId"), ("amount", "Amount")]


def test_shortcut_column_lineage_when_origin_was_ingested() -> None:
    src = _lineage_source()
    origin = _table_dataset(
        src,
        item_id="lh-2",
        schema_name="sales",
        table_name="orders",
        columns=[_column("OrderId"), _column("Amount")],
    )
    assert len(origin) == 1

    shortcut = parse_table_shortcut(ONELAKE_SHORTCUT)
    assert shortcut is not None
    assert (
        _table_dataset(
            src,
            item_id="lh-1",
            schema_name="dbo",
            table_name="customers",
            columns=[_column("orderid"), _column("amount"), _column("note")],
            shortcut=shortcut,
        )
        == []
    )

    emitted = list(FabricOneLakeSource._emit_deferred_shortcut_datasets(src))
    assert len(emitted) == 1
    lineage = emitted[0].upstreams
    assert lineage is not None
    assert lineage.upstreams[0].dataset == (
        "urn:li:dataset:(urn:li:dataPlatform:fabric-onelake,ws-1.lh-2.sales.orders,PROD)"
    )
    assert lineage.fineGrainedLineages is not None
    assert len(lineage.fineGrainedLineages) == 2
    downstreams = {
        field.downstreams[0].rsplit(",", 1)[-1].rstrip(")")
        for field in lineage.fineGrainedLineages
    }
    assert downstreams == {"orderid", "amount"}


def test_shortcut_column_lineage_skipped_when_origin_not_ingested() -> None:
    src = _lineage_source()
    shortcut = parse_table_shortcut(ONELAKE_SHORTCUT)
    assert shortcut is not None
    _table_dataset(
        src,
        item_id="lh-1",
        schema_name="dbo",
        table_name="customers",
        columns=[_column("orderid")],
        shortcut=shortcut,
    )
    emitted = list(FabricOneLakeSource._emit_deferred_shortcut_datasets(src))
    assert len(emitted) == 1
    lineage = emitted[0].upstreams
    assert lineage is not None
    assert lineage.fineGrainedLineages is None
    assert lineage.upstreams[0].type == DatasetLineageTypeClass.COPY


def test_shortcut_upstream_skipped_when_lineage_disabled() -> None:
    src = MagicMock()
    src.config.shortcuts.include_lineage = False
    shortcut = parse_table_shortcut(ONELAKE_SHORTCUT)
    assert shortcut is not None
    assert FabricOneLakeSource._shortcut_upstream(src, shortcut) is None
