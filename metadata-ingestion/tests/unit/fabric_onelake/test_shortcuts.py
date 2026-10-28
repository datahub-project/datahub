"""Lakehouse shortcut parsing, origin properties, and upstream lineage."""

from typing import Iterator, List, Optional
from unittest.mock import MagicMock

from datahub.emitter.mce_builder import make_tag_urn
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.fabric.common.models import FabricWorkspace
from datahub.ingestion.source.fabric.onelake.models import FabricColumn, FabricTable
from datahub.ingestion.source.fabric.onelake.shortcuts import (
    LakehouseTableShortcut,
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


def test_nested_tables_path_is_not_a_table_shortcut() -> None:
    assert (
        parse_table_shortcut(
            {
                "path": "Tables/schema/subdir",
                "name": "customers",
                "target": {"type": "AmazonS3"},
            }
        )
        is None
    )


def test_disabled_shortcut_detection_is_not_an_empty_result() -> None:
    src = MagicMock()
    src.config.shortcuts.enabled = False
    assert FabricOneLakeSource._load_lakehouse_shortcuts(src, "ws-1", "lh-1") is None
    src.client.list_shortcuts.assert_not_called()


def test_lakehouse_with_no_table_shortcuts_returns_an_empty_dict() -> None:
    src = MagicMock()
    src.config.shortcuts.enabled = True
    src.client.list_shortcuts.return_value = []
    assert FabricOneLakeSource._load_lakehouse_shortcuts(src, "ws-1", "lh-1") == {}


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


def _source(include_lineage: bool = True) -> FabricOneLakeSource:
    src = FabricOneLakeSource.create(
        {"shortcuts": {"include_lineage": include_lineage}},
        PipelineContext(run_id="shortcuts"),
    )
    client = MagicMock()
    client.get_workspace_display_name.return_value = "Sales Workspace"
    client.get_item_display_name.return_value = "Sales Lakehouse"
    src.client = client
    return src


def _client(src: FabricOneLakeSource) -> MagicMock:
    assert isinstance(src.client, MagicMock)
    return src.client


def _column(name: str) -> FabricColumn:
    return FabricColumn(name=name, data_type="varchar", is_nullable=True)


def _table_dataset(
    src: FabricOneLakeSource,
    *,
    item_id: str,
    schema_name: str,
    table_name: str,
    columns: List[FabricColumn],
    shortcut: Optional[LakehouseTableShortcut] = None,
) -> list:
    return list(
        src._create_table_dataset(
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


def _onelake_shortcut() -> LakehouseTableShortcut:
    shortcut = parse_table_shortcut(ONELAKE_SHORTCUT)
    assert shortcut is not None
    return shortcut


def test_shortcut_table_has_tag_and_origin_properties() -> None:
    src = _source()

    # Lineage shortcuts are held until the origin table may have been ingested.
    assert (
        _table_dataset(
            src,
            item_id="lh-1",
            schema_name="dbo",
            table_name="customers",
            columns=[],
            shortcut=_onelake_shortcut(),
        )
        == []
    )
    [dataset] = list(src._emit_deferred_shortcut_datasets())
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
    assert src.report.shortcuts_found == 1


def test_origin_names_are_resolved_once_per_workspace_and_item() -> None:
    src = _source()
    _client(src).get_item_display_name.return_value = None

    for _ in range(3):
        assert src._resolve_workspace_display_name("ws-1") == "Sales Workspace"
        assert src._resolve_item_display_name("ws-1", "lh-2") is None

    _client(src).get_workspace_display_name.assert_called_once_with("ws-1")
    _client(src).get_item_display_name.assert_called_once_with("ws-1", "lh-2")


def test_external_shortcut_properties_omit_workspace_and_item() -> None:
    src = _source()
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
    assert src._shortcut_properties(shortcut) == {
        "shortcut_origin_name": "events",
        "shortcut_origin_path": "https://account.dfs.core.windows.net/container/raw/events",
    }
    _client(src).get_workspace_display_name.assert_not_called()


def test_matching_column_pairs_is_case_insensitive() -> None:
    assert matching_column_pairs(
        ["orderid", "amount", "note"],
        ["OrderId", "Amount"],
    ) == [("orderid", "OrderId"), ("amount", "Amount")]


def test_copy_lineage_with_columns_when_origin_was_ingested() -> None:
    src = _source()
    origin = _table_dataset(
        src,
        item_id="lh-2",
        schema_name="sales",
        table_name="orders",
        columns=[_column("OrderId"), _column("Amount")],
    )
    assert len(origin) == 1
    _table_dataset(
        src,
        item_id="lh-1",
        schema_name="dbo",
        table_name="customers",
        columns=[_column("orderid"), _column("amount"), _column("note")],
        shortcut=_onelake_shortcut(),
    )

    [dataset] = list(src._emit_deferred_shortcut_datasets())

    lineage = dataset.upstreams
    assert lineage is not None
    assert lineage.upstreams[0].type == DatasetLineageTypeClass.COPY
    assert lineage.upstreams[0].dataset == (
        "urn:li:dataset:(urn:li:dataPlatform:fabric-onelake,ws-1.lh-2.sales.orders,PROD)"
    )
    assert lineage.fineGrainedLineages is not None
    downstreams = set()
    for field in lineage.fineGrainedLineages:
        assert field.downstreams
        downstreams.add(field.downstreams[0].rsplit(",", 1)[-1].rstrip(")"))
    assert downstreams == {"orderid", "amount"}
    assert src.report.shortcut_upstreams_not_ingested == []


def test_no_lineage_when_origin_was_not_ingested() -> None:
    src = _source()
    _table_dataset(
        src,
        item_id="lh-1",
        schema_name="dbo",
        table_name="customers",
        columns=[_column("orderid")],
        shortcut=_onelake_shortcut(),
    )

    [dataset] = list(src._emit_deferred_shortcut_datasets())

    assert dataset.upstreams is None
    assert list(src.report.shortcut_upstreams_not_ingested) == [
        "ws-1.lh-2.sales.orders"
    ]


def test_no_lineage_when_lineage_disabled() -> None:
    src = _source(include_lineage=False)

    [dataset] = _table_dataset(
        src,
        item_id="lh-1",
        schema_name="dbo",
        table_name="customers",
        columns=[],
        shortcut=_onelake_shortcut(),
    )

    assert dataset.upstreams is None
    assert dataset.tags is not None


def test_target_deeper_than_schema_and_table_has_no_upstream() -> None:
    shortcut = parse_table_shortcut(
        {
            **ONELAKE_SHORTCUT,
            "target": {
                "type": "OneLake",
                "oneLake": {
                    "workspaceId": "ws-1",
                    "itemId": "lh-2",
                    "path": "Tables/a/b/c",
                },
            },
        }
    )
    assert shortcut is not None
    assert shortcut.upstream_table_name is None
    assert shortcut.origin_path == "Tables/a/b/c"


def test_non_object_target_is_parsed_without_origin() -> None:
    shortcut = parse_table_shortcut({**ONELAKE_SHORTCUT, "target": "OneLake"})
    assert shortcut is not None
    assert shortcut.origin_name == ""
    assert shortcut.upstream_table_name is None


def test_shortcut_parse_failure_warns_instead_of_raising() -> None:
    src = _source()

    def list_shortcuts(workspace_id: str, lakehouse_id: str) -> Iterator[object]:
        yield ONELAKE_SHORTCUT
        yield None  # not a mapping, so parsing it raises

    _client(src).list_shortcuts.side_effect = list_shortcuts

    assert src._load_lakehouse_shortcuts("ws-1", "lh-1") == {}
    assert [w.title for w in src.report.warnings] == ["Failed to List Shortcuts"]
