"""Behavioral tests run against every DataHub Lite storage engine.

The two engines share all of their logic via SqlBackedLite, so these cover the
dialect-sensitive seams: DDL, the JSON and LIKE expressions in search, and the
connection/cursor APIs.
"""

import gzip
import json
import pathlib
import signal
import sqlite3
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import nullcontext
from typing import Iterator, Sequence
from unittest.mock import Mock

import duckdb
import pytest
from click.testing import CliRunner

from datahub.cli import lite_cli
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.lite.lite_local import (
    DataHubLiteLocal,
    LiteTransactionError,
    LiteVersionConflict,
    PathNotFoundException,
    SearchFlavor,
)
from datahub.lite.lite_util import DataHubLiteWrapper, LiteLocalConfig, get_datahub_lite
from datahub.lite.sql_backed_lite import SqlBackedLite
from datahub.lite.sqlite_lite import SqliteLite
from datahub.metadata.schema_classes import (
    DatasetPropertiesClass,
    DatasetSnapshotClass,
    MetadataChangeEventClass,
    MySqlDDLClass,
    SchemaMetadataClass,
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


def _state(lite: DataHubLiteLocal) -> dict[str, list[tuple]]:
    assert isinstance(lite, SqlBackedLite)
    return {
        "aspects": lite._execute(
            "SELECT * FROM metadata_aspect_v2 ORDER BY urn, aspect_name, version"
        ),
        "edges": lite._execute(
            "SELECT * FROM metadata_edge_v2 ORDER BY src_id, relnship, dst_id"
        ),
    }


def test_released_database_reads_and_accepts_conditional_writes(
    engine: str, db_file: str
) -> None:
    fixture = pathlib.Path(__file__).parent / "fixtures" / f"{engine}-1.7.0.19.db.gz"
    path = pathlib.Path(db_file)
    path.parent.mkdir(parents=True)
    path.write_bytes(gzip.decompress(fixture.read_bytes()))
    urn = "urn:li:dataset:(urn:li:dataPlatform:mysql,my_db.my_schema.events,PROD)"
    lite = _open(engine, db_file)
    try:
        history = lite.list_versions(urn, "datasetProperties")
        assert [v.version for v in history] == [1, 2]
        assert history[0].value == DatasetPropertiesClass(name="first")
        assert lite.get_aspect(urn, "datasetProperties") == history[1]
        assert history[1].createdon == 2000
        assert history[1].system_metadata.lastObserved == 2000
        assert lite.write(
            MetadataChangeProposalWrapper(
                entityUrn=urn, aspect=DatasetPropertiesClass(name="third")
            ),
            if_version_match=2,
        ) == {"datasetProperties": 3}
    finally:
        lite.close()


def test_numbered_history_keeps_original_observation_metadata(
    lite: DataHubLiteLocal,
) -> None:
    for name, observed, expected_version in [
        ("first", 1000, 2),
        ("second", 2000, 3),
        ("third", 3000, 4),
        ("third", 4000, 4),
    ]:
        versions = lite.write(
            MetadataChangeProposalWrapper(
                entityUrn=CUSTOMERS_URN,
                aspect=DatasetPropertiesClass(name=name),
                systemMetadata=SystemMetadataClass(lastObserved=observed),
            )
        )
        assert versions == {"datasetProperties": expected_version}
        latest = lite.get_aspect(CUSTOMERS_URN, "datasetProperties")
        assert latest is not None
        assert latest.version == expected_version
        assert latest.system_metadata.properties == {
            "sysVersion": str(expected_version)
        }
        assert isinstance(lite, SqlBackedLite)
        live = lite._execute_one(
            "SELECT metadata, createdon FROM metadata_aspect_v2 WHERE urn = ? AND aspect_name = ? AND version = 0",
            [CUSTOMERS_URN, "datasetProperties"],
        )
        assert live is not None
        assert json.loads(live[0]) == latest.value.to_obj()
        assert live[1] == latest.createdon

    history = lite.list_versions(CUSTOMERS_URN, "datasetProperties")
    assert [v.version for v in history] == [1, 2, 3, 4]
    assert lite.get_aspect(CUSTOMERS_URN, "datasetProperties", 3) == history[2]
    assert history[2].value == DatasetPropertiesClass(name="second")
    assert latest is not None
    assert latest.createdon == 3000
    assert latest.system_metadata.lastObserved == 3000
    live_metadata = lite.get(CUSTOMERS_URN, ["datasetProperties"], details=True)
    assert live_metadata is not None
    live_properties = live_metadata["datasetProperties"]
    assert isinstance(live_properties, dict)
    assert live_properties["__systemMetadata"]["lastObserved"] == 4000
    assert lite.write(
        MetadataChangeProposalWrapper(
            entityUrn=CUSTOMERS_URN, aspect=StatusClass(removed=False)
        )
    ) == {"status": 1}
    assert lite.get_aspect(CUSTOMERS_URN, "status", 1) is not None
    for urn, aspect, version in [
        (CUSTOMERS_URN, "status", 9),
        (CUSTOMERS_URN, "unknown", 1),
        ("urn:li:tag:missing", "status", 1),
    ]:
        assert lite.get_aspect(urn, aspect, version) is None
    assert lite.list_versions(CUSTOMERS_URN, "unknown") == []
    for version in (0, -1):
        with pytest.raises(ValueError):
            lite.get_aspect(CUSTOMERS_URN, "status", version)


def test_free_text_search_does_not_match_historical_names(
    lite: DataHubLiteLocal,
) -> None:
    for name in ("old_name", "new_name"):
        lite.write(
            MetadataChangeProposalWrapper(
                entityUrn=CUSTOMERS_URN, aspect=DatasetPropertiesClass(name=name)
            )
        )
    assert not list(lite.search("old_name", SearchFlavor.FREE_TEXT))
    assert [hit.id for hit in lite.search("new_name", SearchFlavor.FREE_TEXT)] == [
        CUSTOMERS_URN
    ]


def test_transaction_reads_own_writes_and_rolls_back_all_state(
    lite: DataHubLiteLocal,
) -> None:
    before = _state(lite)
    with pytest.raises(RuntimeError):
        with lite.transaction():
            for urn in (CUSTOMERS_URN, ORDERS_URN):
                result = lite.write(
                    MetadataChangeProposalWrapper(
                        entityUrn=urn, aspect=DatasetPropertiesClass(name="staged")
                    )
                )
                assert result == {"datasetProperties": 2}
                assert lite.get_aspect(urn, "datasetProperties", 2) is not None
                live = lite.get(urn, ["datasetProperties"], typed=True)
                assert live is not None
                assert live["datasetProperties"] == DatasetPropertiesClass(
                    name="staged"
                )
            raise RuntimeError("cancel revision")
    assert _state(lite) == before
    with lite.transaction():
        assert lite.write(
            MetadataChangeProposalWrapper(
                entityUrn=CUSTOMERS_URN, aspect=StatusClass(removed=False)
            )
        ) == {"status": 1}
    assert lite.get_aspect(CUSTOMERS_URN, "status", 1) is not None


@pytest.mark.parametrize("read_method", ["search", "single_row"])
def test_caught_database_read_failure_rolls_back_writes(
    lite: DataHubLiteLocal, read_method: str
) -> None:
    assert isinstance(lite, SqlBackedLite)
    before = _state(lite)
    with pytest.raises(LiteTransactionError):
        with lite.transaction():
            lite.write(
                MetadataChangeProposalWrapper(
                    entityUrn=CUSTOMERS_URN, aspect=StatusClass(removed=False)
                )
            )
            with pytest.raises((sqlite3.DatabaseError, duckdb.Error)):
                invalid_json = "json_extract('invalid-json', '$.name')"
                if read_method == "search":
                    list(lite.search(f"{invalid_json} IS NOT NULL", SearchFlavor.EXACT))
                else:
                    lite._execute_one(f"SELECT {invalid_json}")
    assert _state(lite) == before
    assert lite.write(
        MetadataChangeProposalWrapper(
            entityUrn=CUSTOMERS_URN, aspect=StatusClass(removed=False)
        )
    ) == {"status": 1}


def test_reindex_failure_preserves_browse_index(
    lite: DataHubLiteLocal, monkeypatch: pytest.MonkeyPatch
) -> None:
    assert isinstance(lite, SqlBackedLite)
    lite.reindex()
    before = _state(lite)

    def fail_platform_edges(*args: object, **kwargs: object) -> None:
        raise OSError("platform edge failed")

    with monkeypatch.context() as patch:
        patch.setattr(
            lite, "_create_edges_from_data_platform_instance", fail_platform_edges
        )
        with pytest.raises(OSError):
            lite.reindex()
    assert _state(lite) == before
    lite.reindex()
    assert _state(lite) == before


@pytest.mark.parametrize("conditional_mce", [False, True])
def test_invalid_write_marks_transaction_rollback_only(
    lite: DataHubLiteLocal, conditional_mce: bool
) -> None:
    before = _state(lite)
    record = MetadataChangeProposalWrapper(
        entityUrn=CUSTOMERS_URN, aspect=StatusClass(removed=False)
    )
    with pytest.raises(LiteTransactionError):
        with lite.transaction():
            lite.write(record)
            with pytest.raises(ValueError):
                if conditional_mce:
                    lite.write(
                        MetadataChangeEventClass(
                            proposedSnapshot=DatasetSnapshotClass(
                                urn=CUSTOMERS_URN, aspects=[StatusClass(removed=True)]
                            )
                        ),
                        if_version_match=1,
                    )
                else:
                    lite.write(record, if_version_match=0)
    assert _state(lite) == before


def test_later_storage_failure_rolls_back_prior_writes(
    lite: DataHubLiteLocal, monkeypatch: pytest.MonkeyPatch
) -> None:
    assert isinstance(lite, SqlBackedLite)
    execute = lite._execute
    before = _state(lite)
    failed_urn = "urn:li:tag:third"

    def fail_third(query: str, params: Sequence[object] = ()) -> list[tuple]:
        if (
            query.startswith("INSERT INTO metadata_aspect_v2")
            and params[0] == failed_urn
        ):
            raise OSError("disk unavailable")
        return execute(query, params)

    monkeypatch.setattr(lite, "_execute", fail_third)
    with pytest.raises(OSError):
        with lite.transaction():
            for urn in (CUSTOMERS_URN, ORDERS_URN, failed_urn):
                lite.write(
                    MetadataChangeProposalWrapper(
                        entityUrn=urn, aspect=StatusClass(removed=False)
                    )
                )
    assert _state(lite) == before


@pytest.mark.parametrize("explicit_transaction", [False, True])
def test_hook_failure_rolls_back_aspect_and_edges(
    lite: DataHubLiteLocal, monkeypatch: pytest.MonkeyPatch, explicit_transaction: bool
) -> None:
    assert isinstance(lite, SqlBackedLite)
    before = _state(lite)
    add_edge = lite.add_edge

    def fail_after_edge(*args: object, **kwargs: object) -> None:
        add_edge(CUSTOMERS_URN, "name", "staged", remove_existing=True)
        raise OSError("edge hook failed")

    monkeypatch.setattr(lite, "add_edge", fail_after_edge)
    with pytest.raises(OSError):
        with lite.transaction() if explicit_transaction else nullcontext():
            lite.write(
                MetadataChangeProposalWrapper(
                    entityUrn=CUSTOMERS_URN,
                    aspect=DatasetPropertiesClass(name="staged"),
                )
            )
    assert _state(lite) == before


def test_standalone_mce_is_atomic(
    lite: DataHubLiteLocal, monkeypatch: pytest.MonkeyPatch
) -> None:
    assert isinstance(lite, SqlBackedLite)
    before = _state(lite)
    execute = lite._execute

    def fail_status(query: str, params: Sequence[object] = ()) -> list[tuple]:
        if query.startswith("INSERT INTO metadata_aspect_v2") and params[1] == "status":
            raise OSError("second aspect failed")
        return execute(query, params)

    record = MetadataChangeEventClass(
        proposedSnapshot=DatasetSnapshotClass(
            urn=CUSTOMERS_URN,
            aspects=[DatasetPropertiesClass(name="staged"), StatusClass(removed=False)],
        )
    )
    with pytest.raises(ValueError):
        lite.write(record, if_version_match=1)
    monkeypatch.setattr(lite, "_execute", fail_status)
    with pytest.raises(OSError):
        lite.write(record)
    assert _state(lite) == before
    monkeypatch.undo()
    assert lite.write(record) == {"datasetProperties": 2, "status": 1}


def test_remove_edge_failure_is_not_swallowed(
    lite: DataHubLiteLocal, monkeypatch: pytest.MonkeyPatch
) -> None:
    assert isinstance(lite, SqlBackedLite)
    before = _state(lite)
    execute = lite._execute

    def fail_delete(query: str, params: Sequence[object] = ()) -> list[tuple]:
        if query.startswith("DELETE FROM metadata_edge_v2"):
            raise OSError("edge delete failed")
        return execute(query, params)

    monkeypatch.setattr(lite, "_execute", fail_delete)
    with pytest.raises(LiteTransactionError):
        with lite.transaction():
            lite.write(
                MetadataChangeProposalWrapper(
                    entityUrn=CUSTOMERS_URN, aspect=StatusClass(removed=False)
                )
            )
            with pytest.raises(OSError):
                lite.remove_edge(CUSTOMERS_URN, "name")
    assert _state(lite) == before


@pytest.mark.parametrize("operation", ["reindex", "close", "destroy", "transaction"])
def test_transaction_rejects_lifecycle_operations(
    lite: DataHubLiteLocal, operation: str
) -> None:
    before = _state(lite)
    with pytest.raises(RuntimeError):
        with lite.transaction():
            lite.write(
                MetadataChangeProposalWrapper(
                    entityUrn=CUSTOMERS_URN, aspect=StatusClass(removed=False)
                )
            )
            if operation == "transaction":
                with lite.transaction():
                    pytest.fail("nested transaction was accepted")
            else:
                getattr(lite, operation)()
    assert _state(lite) == before
    assert pathlib.Path(lite.location()).exists()


def test_conditional_writes_and_caught_conflicts(lite: DataHubLiteLocal) -> None:
    record = MetadataChangeProposalWrapper(
        entityUrn=CUSTOMERS_URN, aspect=StatusClass(removed=False)
    )
    with pytest.raises(LiteVersionConflict) as absent:
        lite.write(record, if_version_match=1)
    assert absent.value.current_version == -1
    assert lite.write(record, if_version_match=-1) == {"status": 1}
    assert lite.write(record, if_version_match=1) == {"status": 1}
    record.aspect = StatusClass(removed=True)
    assert lite.write(record, if_version_match=1) == {"status": 2}
    before = _state(lite)
    for expected in (-1, 1):
        with pytest.raises(LiteVersionConflict) as conflict:
            lite.write(record, if_version_match=expected)
        assert conflict.value.current_version == 2
        assert _state(lite) == before
    with pytest.raises(LiteTransactionError):
        with lite.transaction():
            lite.write(
                MetadataChangeProposalWrapper(
                    entityUrn=ORDERS_URN, aspect=StatusClass(removed=False)
                )
            )
            with pytest.raises(LiteVersionConflict):
                lite.write(record, if_version_match=1)
            # Reads remain usable, but the caught error must not permit a commit.
            assert lite.get_aspect(ORDERS_URN, "status") is not None
            with pytest.raises(LiteTransactionError):
                lite.write(record)
    assert _state(lite) == before
    assert lite.write(record, if_version_match=2) == {"status": 2}


def test_sqlite_conditional_writers_cannot_overwrite_each_other(db_file: str) -> None:
    initial = _open("sqlite", db_file)
    initial.write(
        MetadataChangeProposalWrapper(
            entityUrn=CUSTOMERS_URN, aspect=DatasetPropertiesClass(name="initial")
        )
    )
    initial.close()
    ready = threading.Barrier(2)

    def attempt(name: str) -> str:
        connection = _open("sqlite", db_file)
        try:
            base = connection.get_aspect(CUSTOMERS_URN, "datasetProperties")
            assert base is not None and base.version == 1
            ready.wait(timeout=10)
            try:
                connection.write(
                    MetadataChangeProposalWrapper(
                        entityUrn=CUSTOMERS_URN,
                        aspect=DatasetPropertiesClass(name=name),
                    ),
                    if_version_match=base.version,
                )
                return "written"
            except LiteVersionConflict as conflict:
                assert conflict.current_version == 2
                return "conflict"
        finally:
            connection.close()

    with ThreadPoolExecutor(max_workers=2) as pool:
        assert sorted(pool.map(attempt, ["first", "second"])) == ["conflict", "written"]
    reopened = _open("sqlite", db_file, read_only=True)
    try:
        assert [
            v.version
            for v in reopened.list_versions(CUSTOMERS_URN, "datasetProperties")
        ] == [1, 2]
    finally:
        reopened.close()


def test_forwarding_wrapper_delegates_and_only_forwards_success(
    lite: DataHubLiteLocal,
) -> None:
    sink = Mock()
    wrapper = DataHubLiteWrapper(lite, sink)
    record = MetadataChangeProposalWrapper(
        entityUrn=CUSTOMERS_URN, aspect=StatusClass(removed=False)
    )
    assert wrapper.write(record, if_version_match=-1) == {"status": 1}
    sink.write_record_async.assert_called_once()
    assert wrapper.get_aspect(CUSTOMERS_URN, "status", 1) == lite.get_aspect(
        CUSTOMERS_URN, "status", 1
    )
    assert wrapper.list_versions(CUSTOMERS_URN, "status") == lite.list_versions(
        CUSTOMERS_URN, "status"
    )
    with pytest.raises(LiteVersionConflict):
        wrapper.write(record, if_version_match=-1)
    sink.write_record_async.assert_called_once()
    with pytest.raises(NotImplementedError):
        with wrapper.transaction():
            pytest.fail("forwarding transaction was accepted")


@pytest.mark.skipif(sys.platform == "win32", reason="SIGKILL is unavailable on Windows")
def test_process_death_does_not_publish_partial_transaction(
    engine: str, db_file: str
) -> None:
    committed = _open(engine, db_file)
    with committed.transaction():
        for name in ("first", "second", "third"):
            committed.write(
                MetadataChangeProposalWrapper(
                    entityUrn=f"urn:li:document:{name}",
                    aspect=StatusClass(removed=False),
                )
            )
    before = _state(committed)
    committed.close()
    child = subprocess.run(
        [
            sys.executable,
            "-c",
            """
import os
import signal
import sys
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.lite.lite_util import get_datahub_lite
from datahub.metadata.schema_classes import StatusClass
lite = get_datahub_lite({"type": sys.argv[1], "config": {"file": sys.argv[2]}})
with lite.transaction():
    for name in ("first", "second"):
        lite.write(MetadataChangeProposalWrapper(entityUrn=f"urn:li:document:{name}", aspect=StatusClass(removed=True)), if_version_match=1)
    os.kill(os.getpid(), signal.SIGKILL)
""",
            engine,
            db_file,
        ],
        check=False,
        capture_output=True,
        timeout=30,
    )
    assert child.returncode == -signal.SIGKILL, child.stderr.decode()
    reopened = _open(engine, db_file)
    try:
        # close() rebuilt the browse index before the child ran; compare aspects
        # and verify there are no uncommitted versions after recovery.
        assert _state(reopened)["aspects"] == before["aspects"]
        assert all(
            len(reopened.list_versions(f"urn:li:document:{name}", "status")) == 1
            for name in ("first", "second", "third")
        )
    finally:
        reopened.close()


@pytest.mark.skipif(sys.platform == "win32", reason="SIGKILL is unavailable on Windows")
def test_process_death_preserves_previous_browse_index(
    engine: str, db_file: str, lite: DataHubLiteLocal
) -> None:
    lite.reindex()
    before = _state(lite)
    lite.close()
    child = subprocess.run(
        [
            sys.executable,
            "-c",
            """
import os
import signal
import sys
from datahub.lite.lite_util import get_datahub_lite
lite = get_datahub_lite({"type": sys.argv[1], "config": {"file": sys.argv[2]}})
def interrupt_rebuild(*args):
    os.kill(os.getpid(), signal.SIGKILL)
lite.post_update_hook = interrupt_rebuild
lite.reindex()
""",
            engine,
            db_file,
        ],
        check=False,
        capture_output=True,
        timeout=30,
    )
    assert child.returncode == -signal.SIGKILL, child.stderr.decode()
    reopened = _open(engine, db_file)
    try:
        assert _state(reopened) == before
        assert sorted(
            item.name
            for item in reopened.ls("/databases/mysql/instances/default/tables")
        ) == ["customers", "orders"]
    finally:
        reopened.close()


def test_invalid_standalone_write_does_not_acquire_sqlite_lock(db_file: str) -> None:
    writer = _open("sqlite", db_file)
    candidate = _open("sqlite", db_file, options={"busy_timeout": 0})
    try:
        with writer.transaction():
            with pytest.raises(ValueError):
                candidate.write(
                    MetadataChangeProposalWrapper(
                        entityUrn=CUSTOMERS_URN, aspect=StatusClass(removed=False)
                    ),
                    if_version_match=0,
                )
    finally:
        candidate.close()
        writer.close()


def test_cli_version_and_history(
    engine: str, db_file: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = _open(engine, db_file)
    for removed in (False, True):
        store.write(
            MetadataChangeProposalWrapper(
                entityUrn=CUSTOMERS_URN, aspect=StatusClass(removed=removed)
            )
        )
    store.close()
    config = LiteLocalConfig(type=engine, config={"file": db_file})
    monkeypatch.setattr(lite_cli, "get_lite_config", lambda: config)
    runner = CliRunner()
    args = ["--urn", CUSTOMERS_URN, "--aspect", "status"]
    result = runner.invoke(lite_cli.lite, ["get", *args, "--version", "1"])
    assert result.exit_code == 0, result.output
    assert json.loads(result.output)["value"] == {"removed": False}
    assert json.loads(result.output)["version"] == 1
    result = runner.invoke(lite_cli.lite, ["get", *args, "--version", "99"])
    assert result.exit_code == 0 and json.loads(result.output) is None
    result = runner.invoke(lite_cli.lite, ["history", *args])
    assert result.exit_code == 0, result.output
    assert [v["version"] for v in json.loads(result.output)] == [1, 2]
    for extra in (
        ["--aspect", "datasetProperties"],
        ["--asof", "2020-01-01"],
        ["--path", "/"],
        ["--version", "0"],
    ):
        result = runner.invoke(lite_cli.lite, ["get", *args, "--version", "1", *extra])
        assert result.exit_code == 2
    result = runner.invoke(
        lite_cli.lite, ["get", "--urn", CUSTOMERS_URN, "--version", "1"]
    )
    assert result.exit_code == 2


def test_cli_history_preserves_stored_union_representation(
    engine: str, db_file: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = _open(engine, db_file)
    store.write(
        MetadataChangeProposalWrapper(
            entityUrn=CUSTOMERS_URN,
            aspect=SchemaMetadataClass(
                schemaName="events",
                platform="urn:li:dataPlatform:mysql",
                version=0,
                hash="",
                platformSchema=MySqlDDLClass(
                    tableSchema="CREATE TABLE events (id INT)"
                ),
                fields=[],
            ),
        )
    )
    stored = store.get(CUSTOMERS_URN, ["schemaMetadata"])
    assert stored is not None
    store.close()
    config = LiteLocalConfig(type=engine, config={"file": db_file})
    monkeypatch.setattr(lite_cli, "get_lite_config", lambda: config)
    runner = CliRunner()
    args = ["--urn", CUSTOMERS_URN, "--aspect", "schemaMetadata"]
    version = runner.invoke(lite_cli.lite, ["get", *args, "--version", "1"])
    history = runner.invoke(lite_cli.lite, ["history", *args])
    assert version.exit_code == 0, version.output
    assert history.exit_code == 0, history.output
    assert json.loads(version.output)["value"] == stored["schemaMetadata"]
    assert json.loads(history.output)[0]["value"] == stored["schemaMetadata"]


def test_write_preserves_default_and_supplied_system_metadata(
    lite: DataHubLiteLocal,
) -> None:
    record = MetadataChangeProposalWrapper(
        entityUrn=CUSTOMERS_URN, aspect=StatusClass(removed=False)
    )
    lite.write(record)
    assert record.systemMetadata is not None
    live = lite.get(CUSTOMERS_URN, ["status"], details=True)
    assert live is not None and isinstance(live["status"], dict)
    assert live["status"]["__systemMetadata"]["runId"] == record.systemMetadata.runId
    record.aspect = StatusClass(removed=True)
    record.systemMetadata = SystemMetadataClass(
        lastObserved=1000, runId="example-run", properties={"source": "example"}
    )
    lite.write(record)
    snapshot = lite.get_aspect(CUSTOMERS_URN, "status")
    assert snapshot is not None
    assert snapshot.system_metadata.runId == "example-run"
    assert snapshot.system_metadata.properties == {
        "source": "example",
        "sysVersion": "2",
    }
