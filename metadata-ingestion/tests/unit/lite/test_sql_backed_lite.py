"""Behavioral tests run against every DataHub Lite storage engine.

The two engines share all of their logic via SqlBackedLite, so these cover the
dialect-sensitive seams: DDL, the JSON and LIKE expressions in search, and the
connection/cursor APIs.
"""

import pathlib
import sqlite3
import time
from typing import Iterator

import pytest

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.lite.lite_local import (
    DataHubLiteLocal,
    PathNotFoundException,
    SearchFlavor,
)
from datahub.lite.lite_util import get_datahub_lite
from datahub.lite.sql_backed_lite import SqlBackedLite
from datahub.lite.sqlite_lite import SqliteLite
from datahub.metadata.schema_classes import (
    DatasetPropertiesClass,
    StatusClass,
    SubTypesClass,
    SystemMetadataClass,
)

CUSTOMERS_URN = "urn:li:dataset:(urn:li:dataPlatform:mysql,shop.customers,PROD)"
ORDERS_URN = "urn:li:dataset:(urn:li:dataPlatform:mysql,shop.orders,PROD)"

ENGINES = ["sqlite", "duckdb"]


@pytest.fixture(params=ENGINES)
def engine(request: pytest.FixtureRequest) -> str:
    return request.param


@pytest.fixture
def db_file(tmp_path: pathlib.Path) -> str:
    return str(tmp_path / "lite" / "datahub.db")


def _open(engine: str, file: str, **config: object) -> DataHubLiteLocal:
    return get_datahub_lite(
        {"type": engine, "config": {"file": file, **config}},
        read_only=bool(config.get("read_only")),
    )


@pytest.fixture
def lite(engine: str, db_file: str) -> Iterator[DataHubLiteLocal]:
    instance = _open(engine, db_file)
    for urn, name in [(CUSTOMERS_URN, "customers"), (ORDERS_URN, "orders")]:
        instance.write(
            MetadataChangeProposalWrapper(
                entityUrn=urn,
                aspect=DatasetPropertiesClass(
                    name=name,
                    customProperties=(
                        {"view_definition": "select 1"} if name == "customers" else {}
                    ),
                ),
            )
        )
        instance.write(
            MetadataChangeProposalWrapper(
                entityUrn=urn, aspect=SubTypesClass(["Table"])
            )
        )
    yield instance


def _sys_version(lite: DataHubLiteLocal, urn: str, aspect: str) -> str:
    entity = lite.get(urn, aspects=[aspect], details=True)
    assert entity
    return entity[aspect]["__systemMetadata"]["properties"]["sysVersion"]  # type: ignore[index]


def test_write_and_get(lite: DataHubLiteLocal) -> None:
    assert sorted(lite.list_ids()) == [CUSTOMERS_URN, ORDERS_URN]

    entity = lite.get(CUSTOMERS_URN, aspects=["datasetProperties"])
    assert entity == {
        "urn": CUSTOMERS_URN,
        "datasetProperties": {
            "customProperties": {"view_definition": "select 1"},
            "name": "customers",
            "tags": [],
        },
    }

    typed = lite.get(CUSTOMERS_URN, aspects=["subTypes"], typed=True)
    assert typed
    assert typed["subTypes"] == SubTypesClass(["Table"])

    assert (
        lite.get("urn:li:dataset:(urn:li:dataPlatform:mysql,nope,PROD)", None) is None
    )


def test_write_versions_only_on_change(lite: DataHubLiteLocal) -> None:
    lite.write(
        MetadataChangeProposalWrapper(
            entityUrn=CUSTOMERS_URN, aspect=StatusClass(removed=False)
        )
    )
    assert _sys_version(lite, CUSTOMERS_URN, "status") == "1"

    # An identical aspect is a no-op as far as versioning is concerned.
    lite.write(
        MetadataChangeProposalWrapper(
            entityUrn=CUSTOMERS_URN, aspect=StatusClass(removed=False)
        )
    )
    assert _sys_version(lite, CUSTOMERS_URN, "status") == "1"

    lite.write(
        MetadataChangeProposalWrapper(
            entityUrn=CUSTOMERS_URN, aspect=StatusClass(removed=True)
        )
    )
    assert _sys_version(lite, CUSTOMERS_URN, "status") == "2"
    entity = lite.get(CUSTOMERS_URN, aspects=["status"])
    assert entity
    assert entity["status"] == {"removed": True}


def test_search_free_text_is_case_insensitive(lite: DataHubLiteLocal) -> None:
    for query in ("customers", "CUSTOMERS"):
        hits = {s.id for s in lite.search(query, SearchFlavor.FREE_TEXT)}
        assert hits == {CUSTOMERS_URN}


def test_search_exact_uses_json_operators(lite: DataHubLiteLocal) -> None:
    hits = [
        s.id
        for s in lite.search(
            "metadata -> '$.customProperties' ->> '$.view_definition' IS NOT NULL",
            SearchFlavor.EXACT,
        )
    ]
    assert hits == [CUSTOMERS_URN]


def test_ls_walks_the_browse_hierarchy(lite: DataHubLiteLocal) -> None:
    lite.reindex()

    assert [b.name for b in lite.ls("/")] == ["databases"]
    assert [b.name for b in lite.ls("/databases")] == ["mysql"]
    assert [b.name for b in lite.ls("/databases/mysql/instances/default")] == ["tables"]
    assert sorted(
        b.name for b in lite.ls("/databases/mysql/instances/default/tables")
    ) == ["customers", "orders"]


def test_ls_suggests_completions_for_a_partial_path(lite: DataHubLiteLocal) -> None:
    lite.reindex()

    suggestions = [
        b.auto_complete.suggested_path
        for b in lite.ls("/databases/mys")
        if b.auto_complete
    ]
    assert suggestions == ["/databases/mysql"]

    with pytest.raises(PathNotFoundException):
        lite.ls("/does-not-exist")


def test_get_all_aspects_round_trips(lite: DataHubLiteLocal) -> None:
    mcps = list(lite.get_all_aspects())
    assert {mcp.entityUrn for mcp in mcps} == {CUSTOMERS_URN, ORDERS_URN}
    assert all(mcp.systemMetadata is not None for mcp in mcps)


def test_reopening_preserves_metadata(
    engine: str, db_file: str, lite: DataHubLiteLocal
) -> None:
    lite.close()

    reopened = _open(engine, db_file)
    assert sorted(reopened.list_ids()) == [CUSTOMERS_URN, ORDERS_URN]
    reopened.close()


def test_read_only_instance_reads_without_reindexing(
    engine: str, db_file: str, lite: DataHubLiteLocal
) -> None:
    lite.close()

    read_only = _open(engine, db_file, read_only=True)
    assert sorted(read_only.list_ids()) == [CUSTOMERS_URN, ORDERS_URN]
    assert [b.name for b in read_only.ls("/")] == ["databases"]
    # close() must not try to reindex a read-only database.
    read_only.close()


def test_destroy_removes_the_database(
    engine: str, db_file: str, lite: DataHubLiteLocal
) -> None:
    # Destroy while the instance is still open -- that is how `lite nuke`
    # calls it.
    lite.destroy()
    assert list(pathlib.Path(db_file).parent.iterdir()) == []


def test_destroy_cleans_up_wal_sidecars(db_file: str) -> None:
    # WAL mode is where the "-wal"/"-shm" sidecars actually appear, and they
    # only go away for good once the connection is closed.
    wal_lite = _open("sqlite", db_file, options={"journal_mode": "WAL"})
    wal_lite.write(
        MetadataChangeProposalWrapper(
            entityUrn=ORDERS_URN, aspect=StatusClass(removed=False)
        )
    )
    assert any(p.name.endswith("-wal") for p in pathlib.Path(db_file).parent.iterdir())

    wal_lite.destroy()
    assert list(pathlib.Path(db_file).parent.iterdir()) == []


def test_sqlite_free_text_search_avoids_the_json_arrow_operator() -> None:
    # Free-text search is the default `lite search`, so it has to run on the
    # system SQLite. `->>` needs 3.38 (2022-02) and Ubuntu 22.04 ships 3.37,
    # so the shared query must go through json_extract on this engine. There
    # is no way to exercise that against a modern library, hence the direct
    # assertion on the expression.
    assert "->>" not in SqliteLite._json_text("metadata", "$.name")


def test_sqlite_refuses_a_duckdb_file(db_file: str) -> None:
    # The upgrade footgun: a pre-existing DuckDB instance opened under the new
    # default engine must fail loudly rather than look empty.
    duckdb_lite = _open("duckdb", db_file)
    duckdb_lite.write(
        MetadataChangeProposalWrapper(
            entityUrn=ORDERS_URN, aspect=StatusClass(removed=False)
        )
    )
    duckdb_lite.close()

    with pytest.raises(sqlite3.DatabaseError):
        _open("sqlite", db_file)


def test_sqlite_options_are_applied_as_pragmas(db_file: str) -> None:
    lite = _open("sqlite", db_file, options={"journal_mode": "WAL"})
    assert lite.sqlite_client.execute("PRAGMA journal_mode").fetchone() == ("wal",)  # type: ignore[attr-defined]
    lite.close()


def test_get_as_of_returns_every_requested_aspect(lite: DataHubLiteLocal) -> None:
    # Two aspects written before the cutoff, then both changed after it.
    early = int(time.time() * 1000.0)
    cutoff = early + 1000
    for aspect in (StatusClass(removed=False), SubTypesClass(["Table"])):
        lite.write(
            MetadataChangeProposalWrapper(
                entityUrn=ORDERS_URN,
                aspect=aspect,
                systemMetadata=SystemMetadataClass(lastObserved=early),
            )
        )
    for aspect in (StatusClass(removed=True), SubTypesClass(["View"])):
        lite.write(
            MetadataChangeProposalWrapper(
                entityUrn=ORDERS_URN,
                aspect=aspect,
                systemMetadata=SystemMetadataClass(lastObserved=cutoff + 1000),
            )
        )

    entity = lite.get(ORDERS_URN, aspects=["status", "subTypes"], as_of=cutoff)
    assert entity == {
        "urn": ORDERS_URN,
        "status": {"removed": False},
        "subTypes": {"typeNames": ["Table"]},
    }


def test_write_raises_and_rolls_back_on_failure(
    lite: DataHubLiteLocal, monkeypatch: pytest.MonkeyPatch
) -> None:
    before = sorted(lite.list_ids())

    def boom(*args: object, **kwargs: object) -> None:
        raise RuntimeError("storage is on fire")

    monkeypatch.setattr(lite, "_execute", boom)
    with pytest.raises(RuntimeError):
        lite.write(
            MetadataChangeProposalWrapper(
                entityUrn="urn:li:tag:pii", aspect=StatusClass(removed=False)
            )
        )

    monkeypatch.undo()
    assert sorted(lite.list_ids()) == before


def test_sqlite_warns_when_a_duckdb_sibling_exists(
    tmp_path: pathlib.Path, caplog: pytest.LogCaptureFixture
) -> None:
    lite_dir = tmp_path / "lite"
    lite_dir.mkdir()
    (lite_dir / "datahub.duckdb").write_bytes(b"not really a duckdb file")

    with caplog.at_level("WARNING"):
        lite = _open("sqlite", str(lite_dir / "datahub.db"))
    lite.close()

    assert "datahub.duckdb" in caplog.text

    # No warning once the sqlite instance exists.
    caplog.clear()
    with caplog.at_level("WARNING"):
        reopened = _open("sqlite", str(lite_dir / "datahub.db"))
    reopened.close()
    assert "datahub.duckdb" not in caplog.text


def test_add_edge_updates_destination_and_label_together(
    lite: DataHubLiteLocal,
) -> None:
    assert isinstance(lite, SqlBackedLite)

    # Changing both halves of an existing edge builds a two-column UPDATE.
    lite.add_edge("src:1", "name", "dst:A", dst_label="label-A")

    lite.add_edge("src:1", "name", "dst:B", dst_label="label-B", remove_existing=True)
    assert lite._execute(
        "SELECT src_id, relnship, dst_id, dst_label FROM metadata_edge_v2 "
        "WHERE src_id = 'src:1'"
    ) == [("src:1", "name", "dst:B", "label-B")]

    # Either half alone still works.
    lite.add_edge("src:1", "name", "dst:C", dst_label="label-B", remove_existing=True)
    lite.add_edge("src:1", "name", "dst:C", dst_label="label-C")
    assert lite._execute(
        "SELECT dst_id, dst_label FROM metadata_edge_v2 WHERE src_id = 'src:1'"
    ) == [("dst:C", "label-C")]
