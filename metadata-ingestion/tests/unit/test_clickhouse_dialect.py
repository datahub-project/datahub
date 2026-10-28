"""ClickHouse reflection helpers (the dialect monkey-patches) and the lineage
paths that run outside the query log: materialized-view TO targets and the
system.tables lineage map."""

from types import SimpleNamespace
from typing import Dict, Iterable, List, Optional, Tuple
from unittest.mock import MagicMock, patch

import pytest
from clickhouse_sqlalchemy import types as ch_types
from clickhouse_sqlalchemy.drivers.base import ClickHouseDialect
from sqlalchemy.sql import sqltypes

from datahub.emitter.mce_builder import make_dataset_urn
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.sql import clickhouse
from datahub.ingestion.source.sql.clickhouse import ClickHouseConfig, ClickHouseSource
from datahub.metadata.schema_classes import DatasetLineageTypeClass


class _Connection:
    """Answers each reflection query from the system table it reads."""

    def __init__(self, rows_by_table: Dict[str, List[object]]) -> None:
        self._rows_by_table = rows_by_table

    def execute(self, query: object, params: Optional[dict] = None) -> "_Result":
        sql = str(query)
        for table, rows in self._rows_by_table.items():
            if table in sql:
                return _Result(rows)
        raise AssertionError(f"unexpected query: {sql}")


class _Result:
    def __init__(self, rows: List[object]) -> None:
        self._rows = rows

    def __iter__(self):
        return iter(self._rows)

    def fetchone(self) -> Optional[object]:
        return self._rows[0] if self._rows else None


def _tables_conn(
    rows: Iterable[Tuple[str, str, Optional[str], Optional[str]]],
) -> _Connection:
    return _Connection(
        {
            "system.tables": [
                SimpleNamespace(
                    database=db,
                    table_name=name,
                    comment=comment,
                    properties=props,
                    relkind="v" if name.startswith("v_") else "r",
                    relname=name,
                )
                for db, name, comment, props in rows
            ]
        }
    )


@pytest.fixture
def dialect() -> ClickHouseDialect:
    return ClickHouseDialect()


def test_table_comment_carries_stringified_engine_properties(
    dialect: ClickHouseDialect,
) -> None:
    conn = _tables_conn(
        [
            ("db", "t1", "a comment", '{"engine": "MergeTree", "total_rows": 42}'),
            ("db", "t2", None, None),
        ]
    )
    assert dialect.get_table_comment(conn, "t1", schema="db") == {
        "text": "a comment",
        "properties": {"engine": "MergeTree", "total_rows": "42"},
    }
    assert dialect.get_table_comment(conn, "t2", schema="db") == {
        "text": None,
        "properties": {},
    }
    assert dialect.get_table_comment(conn, "missing", schema="db") == {"text": None}


def test_table_and_view_names_are_split_by_engine_and_scoped_to_schema(
    dialect: ClickHouseDialect,
) -> None:
    conn = _tables_conn(
        [
            ("db", "t", None, None),
            ("db", "v_1", None, None),
            ("other", "t2", None, None),
        ]
    )
    assert dialect.get_table_names(conn, schema="db") == ["t"]
    assert dialect.get_view_names(conn, schema="db") == ["v_1"]


@pytest.mark.parametrize(
    "spec, nullable, inner_type",
    [
        ("Nullable(Int32)", True, ch_types.Int32),
        ("LowCardinality(String)", False, ch_types.String),
        ("LowCardinality(Nullable(String))", True, ch_types.String),
    ],
)
def test_column_info_unwraps_nullable_and_low_cardinality(
    dialect: ClickHouseDialect, spec: str, nullable: bool, inner_type: type
) -> None:
    info = dialect._get_column_info("c", spec, "doc")
    assert info["nullable"] is nullable
    assert isinstance(info["type"], inner_type)
    assert info["full_type"] == spec
    assert info["comment"] == "doc"


def test_unparseable_column_type_degrades_to_null_type(
    dialect: ClickHouseDialect,
) -> None:
    with patch.object(
        ClickHouseDialect, "_get_column_type", side_effect=TypeError("nested")
    ):
        info = dialect._get_column_info("c", "Map(String, Array(Tuple(a Int8)))", None)
    assert isinstance(info["type"], sqltypes.NullType)
    assert info["full_type"] == "Map(String, Array(Tuple(a Int8)))"


def test_columns_for_a_schema_come_from_one_batched_system_columns_read(
    dialect: ClickHouseDialect,
) -> None:
    def col(db: str, table: str, name: str, type_: str) -> SimpleNamespace:
        return SimpleNamespace(
            database=db, table_name=table, name=name, type=type_, comment=""
        )

    conn = _Connection(
        {
            "system.columns": [
                col("db", "t", "id", "UInt64"),
                col("db", "t", "name", "Nullable(String)"),
                col("db", "other", "x", "UInt8"),
            ]
        }
    )
    columns = dialect.get_columns(conn, "t", schema="db")
    assert [(c["name"], c["nullable"]) for c in columns] == [
        ("id", False),
        ("name", True),
    ]


def test_view_definition_is_empty_when_the_relation_is_not_a_view(
    dialect: ClickHouseDialect,
) -> None:
    present = _Connection({"system.tables": [("CREATE VIEW db.v AS SELECT 1",)]})
    absent = _Connection({"system.tables": []})
    assert (
        dialect.get_view_definition(present, "v", schema="db")
        == "CREATE VIEW db.v AS SELECT 1"
    )
    assert dialect.get_view_definition(absent, "t", schema="db") == ""


def _source(**config_kwargs: object) -> ClickHouseSource:
    config = ClickHouseConfig.model_validate(
        {"host_port": "localhost:8123", "include_table_lineage": False, **config_kwargs}
    )
    return ClickHouseSource(config, PipelineContext(run_id="test"))


def _urn(name: str) -> str:
    return make_dataset_urn("clickhouse", name, "PROD")


def test_materialized_view_with_to_clause_registers_source_lineage_for_both_targets() -> (
    None
):
    source = _source()
    source.aggregator = MagicMock()
    definition = "CREATE MATERIALIZED VIEW db.mv TO db.target AS SELECT id FROM db.src"
    source._add_view_to_aggregator(_urn("db.mv"), definition, "db", None)

    registered = [
        call.kwargs["view_urn"]
        for call in source.aggregator.add_view_definition.call_args_list
    ]
    assert registered == [_urn("db.mv"), _urn("db.target")]


def test_unqualified_to_target_resolves_against_the_default_database() -> None:
    source = _source()
    assert source._extract_to_table_urn(
        _urn("db.mv"), "create materialized view mv to target as select 1", "db"
    ) == _urn("db.target")
    assert (
        source._extract_to_table_urn(_urn("db.v"), "CREATE VIEW v AS SELECT 1", "db")
        is None
    )


def test_plain_views_register_once() -> None:
    source = _source()
    source.aggregator = MagicMock()
    source._add_view_to_aggregator(
        _urn("db.v"), "CREATE VIEW db.v AS SELECT 1", "db", None
    )
    assert source.aggregator.add_view_definition.call_count == 1


class _LineageConnection:
    def __init__(
        self, all_tables: List[str], lineage_rows: List[Dict[str, str]]
    ) -> None:
        self._all_tables = all_tables
        self._lineage_rows = lineage_rows

    def __enter__(self) -> "_LineageConnection":
        return self

    def __exit__(self, *args: object) -> None:
        return None

    def execute(self, query: object) -> "_MappingResult":
        if "AS table_name" in str(query):
            rows = [
                {"database": t.split(".")[0], "table_name": t.split(".")[1]}
                for t in self._all_tables
            ]
            return _MappingResult(rows)
        return _MappingResult(self._lineage_rows)


class _MappingResult:
    def __init__(self, rows: List[Dict[str, str]]) -> None:
        self._rows = rows

    def mappings(self) -> "_MappingResult":
        return self

    def fetchall(self) -> List[Dict[str, str]]:
        return self._rows

    def __iter__(self):
        return iter(self._rows)


def _lineage_row(target: str, source: str) -> Dict[str, str]:
    target_schema, target_table = target.split(".")
    source_schema, source_table = source.split(".")
    return {
        "target_schema": target_schema,
        "target_table": target_table,
        "source_schema": source_schema,
        "source_table": source_table,
    }


def _view_lineage_source(
    monkeypatch: pytest.MonkeyPatch,
    all_tables: List[str],
    rows: List[Dict[str, str]],
    **config_kwargs: object,
) -> ClickHouseSource:
    engine = MagicMock()
    engine.connect.return_value = _LineageConnection(all_tables, rows)
    monkeypatch.setattr(clickhouse, "create_engine", lambda *a, **k: engine)
    return _source(
        include_table_lineage=True,
        include_tables=False,
        include_materialized_views=False,
        **config_kwargs,
    )


def test_view_lineage_map_merges_upstreams_and_drops_vanished_sources(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = _view_lineage_source(
        monkeypatch,
        all_tables=["db.src", "db.src2", "db.v"],
        rows=[
            _lineage_row("db.v", "db.src"),
            _lineage_row("db.v", "db.src2"),
            _lineage_row("db.v", "db.dropped_meanwhile"),
        ],
    )
    mcp = source.get_lineage_mcp(_urn("db.v"))
    assert mcp is not None
    upstreams = mcp.aspect.upstreams  # type: ignore[union-attr]
    assert {u.dataset for u in upstreams} == {_urn("db.src"), _urn("db.src2")}
    assert {u.type for u in upstreams} == {DatasetLineageTypeClass.VIEW}
    assert source.get_lineage_mcp(_urn("db.src")) is None


def test_view_lineage_respects_table_pattern(monkeypatch: pytest.MonkeyPatch) -> None:
    source = _view_lineage_source(
        monkeypatch,
        all_tables=["db.src", "db.v", "db.denied_v"],
        rows=[_lineage_row("db.v", "db.src"), _lineage_row("db.denied_v", "db.src")],
        table_pattern={"deny": ["^db\\.denied_.*"]},
    )
    assert source.get_lineage_mcp(_urn("db.v")) is not None
    assert source.get_lineage_mcp(_urn("db.denied_v")) is None


def test_lineage_query_failure_leaves_lineage_empty_instead_of_failing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine = MagicMock()
    engine.connect.side_effect = [
        _LineageConnection(["db.v"], []),  # the all-tables read succeeds
        RuntimeError("query_log too large"),  # the lineage read does not
    ]
    monkeypatch.setattr(clickhouse, "create_engine", lambda *a, **k: engine)
    source = _source(
        include_table_lineage=True,
        include_tables=False,
        include_materialized_views=False,
    )
    assert source.get_lineage_mcp(_urn("db.v")) is None
