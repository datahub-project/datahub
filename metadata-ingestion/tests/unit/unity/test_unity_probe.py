from typing import Any, Dict, Iterator, List
from unittest.mock import MagicMock

from databricks.sdk.errors import NotFound
from databricks.sdk.service.catalog import CatalogInfo

from datahub.ingestion.agent.probe_methods import list_probe_methods
from datahub.ingestion.source.unity.config import UnityCatalogSourceConfig
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
