from typing import Any, Dict, Iterator, List
from unittest.mock import MagicMock

import pytest
from databricks.sdk.errors import NotFound, PermissionDenied, Unauthenticated
from databricks.sdk.service import catalog as sdk_catalog
from databricks.sdk.service.catalog import (
    CatalogInfo,
    ColumnInfo,
    SchemaInfo,
    TableInfo,
    TableType,
)
from databricks.sdk.service.workspace import ObjectInfo, ObjectType

from datahub.ingestion.agent.filter_check import check_filters
from datahub.ingestion.agent.probe_methods import list_probe_methods, run_probe_method
from datahub.ingestion.agent.verdicts import ProbeConnectionError
from datahub.ingestion.source.unity.config import UnityCatalogSourceConfig
from datahub.ingestion.source.unity.proxy import TableInfoWithGeneration
from datahub.ingestion.source.unity.unity_probe import UnityCatalogMetadataProbe

BASE: Dict[str, Any] = {
    "workspace_url": "https://example.cloud.databricks.com",
    "token": "t",
}


def _config(**extra: Any) -> UnityCatalogSourceConfig:
    return UnityCatalogSourceConfig.model_validate({**BASE, **extra})


def _fake_ws() -> MagicMock:
    ws = MagicMock()
    ws.config.warehouse_id = None
    return ws


def _probe(ws: MagicMock, **extra: Any) -> UnityCatalogMetadataProbe:
    return UnityCatalogMetadataProbe(ws, _config(**extra))


def test_probe_methods_advertise_the_unity_commands_not_the_sqlalchemy_ones() -> None:
    commands = {s.command for s in list_probe_methods("unity-catalog")}
    assert {"catalogs", "sql"} <= commands
    # The inherited SQLAlchemy getters could not authenticate against Databricks.
    assert "containers" not in commands
    assert "foreign_keys" not in commands


def test_the_databricks_alias_reaches_the_same_provider() -> None:
    assert {s.command for s in list_probe_methods("databricks")} == {
        s.command for s in list_probe_methods("unity-catalog")
    }


def test_catalogs_lists_every_catalog_including_ones_the_pattern_denies() -> None:
    ws = _fake_ws()
    ws.catalogs.list.return_value = [
        CatalogInfo(name="main"),
        CatalogInfo(name="my cat"),
    ]
    probe = _probe(ws, catalog_pattern={"deny": ["^main$"]})
    assert probe.catalogs(limit=10) == ["main", "my cat"]


def test_catalogs_honours_a_pinned_list_and_reports_a_missing_entry() -> None:
    ws = _fake_ws()

    def get(name: str, include_browse: bool) -> CatalogInfo:
        if name == "typo":
            raise NotFound("catalog not found")
        return CatalogInfo(name=name)

    ws.catalogs.get.side_effect = get
    probe = _probe(ws, catalogs=["main", "typo"])
    assert probe.catalogs(limit=10) == ["main"]
    ws.catalogs.list.assert_not_called()
    assert any("typo" in w for w in probe.warnings)


def test_catalogs_adds_hive_metastore_when_ingestion_would_read_it() -> None:
    ws = _fake_ws()
    ws.catalogs.list.return_value = [
        CatalogInfo(name="main"),
        CatalogInfo(name="hive_metastore"),
    ]
    probe = _probe(ws, warehouse_id="w1", include_hive_metastore=True)
    assert probe.catalogs(limit=10) == ["hive_metastore", "main"]


def test_catalogs_stops_paging_at_the_limit() -> None:
    ws = _fake_ws()
    pulled: List[str] = []

    def gen() -> Iterator[CatalogInfo]:
        for i in range(1000):
            pulled.append(str(i))
            yield CatalogInfo(name=f"c{i}")

    ws.catalogs.list.return_value = gen()
    assert len(_probe(ws).catalogs(limit=3)) == 3
    assert len(pulled) <= 4


def _serve(monkeypatch: pytest.MonkeyPatch, ws: MagicMock) -> None:
    monkeypatch.setattr(
        UnityCatalogMetadataProbe,
        "for_config",
        classmethod(lambda cls, config: cls(ws, config)),
    )


def test_schemas_lists_raw_names_and_carries_the_catalog_as_parent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ws = _fake_ws()
    ws.catalogs.get.return_value = CatalogInfo(name="my cat")
    ws.schemas.list.return_value = [
        SchemaInfo(name="analytics"),
        SchemaInfo(name="information_schema"),
    ]
    _serve(monkeypatch, ws)
    result = run_probe_method("unity-catalog", BASE, "schemas", {"catalog": "my cat"})
    assert result.result == ["analytics", "information_schema"]
    assert result.parent_path == ["my cat"]
    assert result.kind == "Schema"
    assert ws.schemas.list.call_args.kwargs["catalog_name"] == "my cat"


def test_schemas_of_an_unknown_catalog_is_a_caller_error() -> None:
    ws = _fake_ws()
    ws.catalogs.get.side_effect = NotFound("no such catalog")
    with pytest.raises(ValueError, match="no catalog 'typo'"):
        _probe(ws).schemas(catalog="typo")


def test_schemas_the_credential_cannot_browse_degrade_with_a_warning() -> None:
    ws = _fake_ws()
    ws.catalogs.get.return_value = CatalogInfo(name="main")
    ws.schemas.list.side_effect = PermissionDenied("no USE CATALOG")
    probe = _probe(ws)
    assert probe.schemas(catalog="main") == []
    # Empty WITH a reason, never a silent empty.
    assert any("main" in w and "403" in w for w in probe.warnings)


def test_hive_metastore_schemas_are_not_probed_and_say_so() -> None:
    ws = _fake_ws()
    probe = _probe(ws)
    assert probe.schemas(catalog="hive_metastore") == []
    ws.catalogs.get.assert_not_called()
    assert any("hive_metastore" in w for w in probe.warnings)


def test_sdk_error_text_never_reaches_the_caller() -> None:
    # An unparseable response embeds the whole request log in the message.
    leaky = "unable to parse response. Request log: GET /api/2.1 Authorization: Bearer s3cr3t"
    ws = _fake_ws()
    ws.catalogs.get.return_value = CatalogInfo(name="main")
    ws.schemas.list.side_effect = Unauthenticated(leaky)
    with pytest.raises(ProbeConnectionError) as raised:
        _probe(ws).schemas(catalog="main")
    assert "s3cr3t" not in str(raised.value)
    assert "Unauthenticated" in str(raised.value) and "401" in str(raised.value)

    ws.schemas.list.side_effect = PermissionDenied(leaky)
    probe = _probe(ws)
    assert probe.schemas(catalog="main") == []
    assert probe.warnings and not any("s3cr3t" in w for w in probe.warnings)

    ws.catalogs.get.side_effect = NotFound(leaky)
    with pytest.raises(ValueError) as missing:
        _probe(ws).schemas(catalog="main")
    assert "s3cr3t" not in str(missing.value)


def test_a_client_that_cannot_be_built_is_reported_without_the_sdk_text(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def broken(config: UnityCatalogSourceConfig) -> MagicMock:
        raise ValueError(b'{"error":"invalid_client","secret":"s3cr3t"}')

    monkeypatch.setattr(
        "datahub.ingestion.source.unity.unity_probe.create_workspace_client", broken
    )
    with pytest.raises(ProbeConnectionError) as raised:
        UnityCatalogMetadataProbe.for_config(_config())
    assert "s3cr3t" not in str(raised.value)


_HAS_METRIC_VIEW = getattr(TableType, "METRIC_VIEW", None) is not None


def _tables_ws() -> MagicMock:
    ws = _fake_ws()
    ws.catalogs.get.return_value = CatalogInfo(name="main")
    rows = [
        TableInfoWithGeneration(name="orders", table_type=TableType.MANAGED),
        TableInfoWithGeneration(name="v_orders", table_type=TableType.VIEW),
        TableInfoWithGeneration(
            name="mv_orders", table_type=TableType.MATERIALIZED_VIEW
        ),
        TableInfoWithGeneration(name="returns", table_type=TableType.EXTERNAL),
    ]
    if _HAS_METRIC_VIEW:
        rows.append(
            TableInfoWithGeneration(name="kpi", table_type=TableType.METRIC_VIEW)
        )
    ws.tables.list.return_value = rows
    return ws


def test_tables_views_and_metric_views_split_the_way_process_tables_does() -> None:
    probe = _probe(_tables_ws())
    assert probe.tables(catalog="main", schema="analytics") == ["orders", "returns"]
    assert probe.views(catalog="main", schema="analytics") == ["v_orders", "mv_orders"]
    if _HAS_METRIC_VIEW:
        assert probe.metric_views(catalog="main", schema="analytics") == ["kpi"]


def test_stopping_at_the_limit_restores_the_sdk_class_ingestion_patches() -> None:
    # proxy.tables swaps TableInfo for TableInfoWithGeneration around its
    # loop; abandoning the generator mid-loop must not leave the swap behind.
    original = sdk_catalog.TableInfo
    assert _probe(_tables_ws()).tables(catalog="main", schema="analytics", limit=1) == [
        "orders"
    ]
    assert sdk_catalog.TableInfo is original


def test_a_listed_table_round_trips_into_the_verdict_ingestion_makes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ws = _tables_ws()
    _serve(monkeypatch, ws)
    config = {**BASE, "table_pattern": {"allow": [r"^main\.analytics\.orders$"]}}
    listed = run_probe_method(
        "unity-catalog", config, "tables", {"catalog": "main", "schema": "analytics"}
    )
    assert listed.kind is not None and isinstance(listed.result, list)
    verdict = check_filters(
        source_type="unity-catalog",
        config_dict=config,
        kind=listed.kind,
        parent_path=listed.parent_path,
        names=listed.result,
    )
    assert [(r.target, r.included) for r in verdict.results] == [
        ("main.analytics.orders", True),
        ("main.analytics.returns", False),
    ]
    assert verdict.warnings == []


def test_hive_metastore_tables_are_not_probed_and_say_so() -> None:
    ws = _fake_ws()
    probe = _probe(ws)
    assert probe.tables(catalog="hive_metastore", schema="default") == []
    assert probe.columns(catalog="hive_metastore", schema="default", table="t") == []
    ws.tables.list.assert_not_called()
    ws.tables.get.assert_not_called()
    assert any("hive_metastore" in w for w in probe.warnings)


def test_tables_of_a_schema_the_credential_cannot_read_degrade_with_a_warning() -> None:
    ws = _fake_ws()
    ws.catalogs.get.return_value = CatalogInfo(name="main")
    ws.tables.list.side_effect = PermissionDenied("no USE SCHEMA")
    probe = _probe(ws)
    assert probe.views(catalog="main", schema="analytics") == []
    assert any("main.analytics" in w for w in probe.warnings)


def test_columns_are_structural_metadata_only() -> None:
    ws = _fake_ws()
    ws.tables.get.return_value = TableInfo(
        name="orders",
        columns=[
            ColumnInfo(name="id", type_text="bigint", nullable=False, comment="pk")
        ],
    )
    cols = _probe(ws).columns(catalog="main", schema="analytics", table="orders")
    assert cols == [
        {
            "name": "id",
            "type": "bigint",
            "nullable": False,
            "comment": "pk",
            "partition_index": None,
        }
    ]
    assert ws.tables.get.call_args.kwargs["full_name"] == "main.analytics.orders"


def test_columns_of_an_unknown_table_is_a_caller_error() -> None:
    ws = _fake_ws()
    ws.tables.get.side_effect = NotFound("no such table")
    with pytest.raises(ValueError, match="main.analytics.typo"):
        _probe(ws).columns(catalog="main", schema="analytics", table="typo")


def test_notebooks_list_paths_even_while_include_notebooks_is_off() -> None:
    ws = _fake_ws()
    ws.workspace.list.return_value = [
        ObjectInfo(object_type=ObjectType.DIRECTORY, object_id=2, path="/Shared"),
        ObjectInfo(object_type=ObjectType.NOTEBOOK, object_id=1, path="/Shared/etl"),
        ObjectInfo(object_type=ObjectType.FILE, object_id=3, path="/Shared/a.csv"),
        ObjectInfo(object_type=ObjectType.NOTEBOOK, object_id=4, path="/Users/x/nb"),
    ]
    assert _probe(ws).notebooks(limit=10) == ["/Shared/etl", "/Users/x/nb"]
    assert _probe(ws).notebooks(limit=1) == ["/Shared/etl"]


def test_notebooks_the_credential_cannot_list_degrade_with_a_warning() -> None:
    ws = _fake_ws()
    ws.workspace.list.side_effect = PermissionDenied("no workspace access")
    probe = _probe(ws)
    assert probe.notebooks() == []
    assert probe.warnings
