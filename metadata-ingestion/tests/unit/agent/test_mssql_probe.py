from pathlib import Path
from typing import Dict, List

import pytest
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.sql import quoted_name

from datahub.ingestion.agent.config_validation import validate_source_config
from datahub.ingestion.agent.filter_check import check_filters
from datahub.ingestion.agent.probe_methods import list_probe_methods
from datahub.ingestion.agent.recipe import validate_recipe
from datahub.ingestion.source.sql.mssql.mssql_probe import SqlServerMetadataProbe
from datahub.ingestion.source.sql.mssql.source import SQLServerConfig

_BASE: Dict[str, object] = {"host_port": "h:1433", "username": "u", "password": "p"}
_ODBC: Dict[str, object] = {
    **_BASE,
    "uri_args": {"driver": "ODBC Driver 18 for SQL Server"},
}


def test_an_odbc_recipe_validates_as_ingestion_validates_it() -> None:
    recipe = {"source": {"type": "mssql-odbc", "config": _ODBC}}
    assert validate_recipe(recipe)["errors"] == []


def test_uri_args_are_still_refused_on_the_pytds_source_type() -> None:
    recipe = {"source": {"type": "mssql", "config": _ODBC}}
    assert validate_recipe(recipe)["valid"] is False


def test_probe_filter_accepts_an_odbc_recipe() -> None:
    result = check_filters(
        source_type="mssql-odbc",
        config_dict=_ODBC,
        kind="Schema",
        parent_path=[],
        names=["dbo"],
    )
    assert result.results[0].included


@pytest.mark.parametrize(
    "source_type, config, scheme",
    [("mssql", _BASE, "mssql+pytds"), ("mssql-odbc", _ODBC, "mssql+pyodbc")],
)
def test_the_probe_dials_the_driver_ingestion_dials(
    source_type: str, config: Dict[str, object], scheme: str
) -> None:
    validated = validate_source_config(SQLServerConfig, source_type, config)
    assert validated.probe_sql_alchemy_url().startswith(f"{scheme}://")


@pytest.mark.parametrize(
    "extra, single, pinned",
    [
        ({}, False, ""),
        ({"database": "DemoData"}, True, "DemoData"),
        (
            {
                "sqlalchemy_uri": "mssql+pyodbc:///?odbc_connect=DRIVER%3D%7Bx%7D%3BDATABASE%3DNewData%3B"
            },
            True,
            "NewData",
        ),
        ({"sqlalchemy_uri": "mssql+pytds://u:p@h:1433"}, True, ""),
    ],
)
def test_single_database_detection_matches_get_inspectors(
    extra: Dict[str, object], single: bool, pinned: str
) -> None:
    config = SQLServerConfig.model_validate({**_BASE, **extra})
    assert config.is_single_database_recipe() is single
    if single:
        assert config.pinned_database_name() == pinned


def test_database_pattern_is_declared_not_guessed() -> None:
    result = check_filters(
        source_type="mssql",
        config_dict={**_BASE, "database_pattern": {"deny": ["^NewData$"]}},
        kind="Database",
        parent_path=[],
        names=["NewData", "DemoData"],
    )
    assert result.pattern_field == "database_pattern"
    assert [v.included for v in result.results] == [False, True]
    assert result.warnings == []


def test_an_odbc_engine_learns_to_read_sql_variant() -> None:
    """_add_output_converters, applied per connection: the probe never runs
    SQLServerSource.__init__, where ingestion installs it."""
    from datahub.ingestion.source.sql.mssql.source import add_sql_variant_converter

    added: Dict[int, object] = {}

    class _DbapiConnection:
        def add_output_converter(self, sql_type: int, func: object) -> None:
            added[sql_type] = func

    add_sql_variant_converter(_DbapiConnection())
    assert list(added) == [-150]
    # pytds connections have no such method; that must not fail the connection.
    add_sql_variant_converter(object())


def _sqlite(tmp_path: Path, name: str, table: str) -> Engine:
    engine = create_engine(f"sqlite:///{tmp_path}/{name}.db")
    with engine.begin() as conn:
        conn.exec_driver_sql(f"CREATE TABLE IF NOT EXISTS {table} (id INTEGER)")
        conn.exec_driver_sql(
            f"CREATE VIEW IF NOT EXISTS v_{table} AS SELECT id FROM {table}"
        )
    return engine


class _FakeServer(SqlServerMetadataProbe):
    """The real provider with its two server round-trips replaced: each
    "database" is a sqlite file whose one table is named after it."""

    server_databases: List[str] = ["DemoData", "NewData"]

    def __init__(self, engine: Engine, config: SQLServerConfig, tmp_path: Path) -> None:
        super().__init__(engine, config)
        self._tmp_path = tmp_path
        self.opened: List[str] = []

    def _list_databases(self) -> List[str]:
        return list(self.server_databases)

    def _open_database_engine(self, name: str) -> Engine:
        self.opened.append(name)
        return _sqlite(self._tmp_path, name, f"t_{name.lower()}")


def _probe(tmp_path: Path, **config: object) -> _FakeServer:
    cfg = SQLServerConfig.model_validate({**_BASE, **config})
    return _FakeServer(_sqlite(tmp_path, "default", "t_default"), cfg, tmp_path)


def test_multi_database_recipe_refuses_an_unnamed_database(tmp_path: Path) -> None:
    with _probe(tmp_path) as probe:
        with pytest.raises(ValueError, match="databases"):
            probe.tables(schema="main")
        with pytest.raises(ValueError, match="databases"):
            probe.containers()
        assert probe.opened == []


def test_database_is_resolved_to_the_servers_spelling(tmp_path: Path) -> None:
    with _probe(tmp_path) as probe:
        assert probe.tables(schema="main", database="demodata") == ["t_demodata"]
        assert probe.opened == ["DemoData"]
        # The caller's spelling is what travels in parent_path, so say which
        # one ingestion qualifies with.
        assert any("'DemoData'" in w for w in probe.warnings), probe.warnings
        # Cached: a second command on the same database opens nothing new.
        assert probe.views(schema="main", database="DEMODATA") == ["v_t_demodata"]
        assert probe.opened == ["DemoData"]


def test_an_unknown_or_system_database_is_refused_before_connecting(
    tmp_path: Path,
) -> None:
    with _probe(tmp_path) as probe:
        for name in ("master", "NoSuchDb", "DemoData]; DROP TABLE x --"):
            with pytest.raises(ValueError):
                probe.tables(schema="main", database=name)
        assert probe.opened == []


def test_multi_database_recipe_lists_every_database_including_denied_ones(
    tmp_path: Path,
) -> None:
    with _probe(tmp_path, database_pattern={"deny": ["^NewData$"]}) as probe:
        assert probe.databases() == ["DemoData", "NewData"]


def test_pinned_recipe_lists_and_reads_only_its_pin(tmp_path: Path) -> None:
    with _probe(tmp_path, database="DemoData") as probe:
        assert probe.databases() == ["DemoData"]
        assert probe.tables(schema="main") == ["t_default"]
        assert probe.tables(schema="main", database="demodata") == ["t_default"]
        with pytest.raises(ValueError, match="DemoData"):
            probe.tables(schema="main", database="NewData")
        assert probe.opened == []


def test_the_database_travels_in_parent_path() -> None:
    specs = {s.command: s for s in list_probe_methods("mssql", _BASE)}
    assert specs["tables"].parent_params == ("database", "schema")
    assert specs["views"].parent_params == ("database", "schema")
    assert specs["containers"].parent_params == ("database",)
    assert specs["containers"].kind == "Schema"
    assert specs["databases"].kind == "Database"


def test_the_sql_scope_is_checked_as_tsql(tmp_path: Path) -> None:
    with _probe(tmp_path, database="DemoData") as probe:
        assert probe.sql_dialect == "tsql"


@pytest.mark.parametrize(
    "schema",
    [
        "main' OR '1'='1",
        "main]; DROP TABLE t_default; --",
        "main; SELECT 1",
        "main -- comment",
        "main UNION SELECT name FROM sys.sql_logins",
        # The dialect would read this as database `NewData`, owner `dbo`, and
        # switch to it with USE -- a database the command was not scoped to.
        "NewData.dbo",
    ],
)
def test_a_schema_the_server_does_not_list_never_reaches_the_dialect(
    tmp_path: Path, schema: str
) -> None:
    with _probe(tmp_path, database="DemoData") as probe:
        for call in (
            lambda: probe.tables(schema=schema),
            lambda: probe.views(schema=schema),
            lambda: probe.columns(schema=schema, table="t_default"),
        ):
            with pytest.raises(ValueError, match="containers"):
                call()


@pytest.mark.parametrize(
    "table",
    ["t_default' --", "t_default]; DROP TABLE t_default; --", "x UNION SELECT 1"],
)
def test_a_table_the_server_does_not_list_never_reaches_the_dialect(
    tmp_path: Path, table: str
) -> None:
    with _probe(tmp_path, database="DemoData") as probe:
        for call in (
            lambda: probe.columns(schema="main", table=table),
            lambda: probe.indexes(schema="main", table=table),
            lambda: probe.primary_key(schema="main", table=table),
            lambda: probe.foreign_keys(schema="main", table=table),
            lambda: probe.view_definition(schema="main", view=table),
        ):
            with pytest.raises(ValueError):
                call()


def test_per_object_commands_read_the_named_database(tmp_path: Path) -> None:
    with _probe(tmp_path) as probe:
        columns = probe.columns(schema="MAIN", table="T_NEWDATA", database="NewData")
        assert [c["name"] for c in columns] == ["id"]
        assert probe.view_definition(
            schema="main", view="v_t_newdata", database="NewData"
        )
        assert probe.opened == ["NewData"]


def test_quote_schemas_is_honoured_like_get_allowed_schemas(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    seen: List[object] = []

    class _Recorder:
        def get_schema_names(self) -> List[str]:
            return ["John.Doe"]

        def get_table_names(self, schema: object) -> List[str]:
            seen.append(schema)
            return []

    for quote in (True, False):
        with _probe(tmp_path, database="DemoData", quote_schemas=quote) as probe:
            monkeypatch.setattr(probe, "_insp", _Recorder())
            probe.tables(schema="john.doe")
            if not quote:
                assert any("quote_schemas" in w for w in probe.warnings)
    assert isinstance(seen[0], quoted_name) and seen[0].quote is True
    assert seen[0] == "John.Doe"
    assert seen[1] == "John.Doe" and not isinstance(seen[1], quoted_name)


def test_exit_disposes_every_engine_it_opened(tmp_path: Path) -> None:
    probe = _probe(tmp_path)
    with probe:
        probe.tables(schema="main", database="DemoData")
        assert list(probe._database_engines) == ["DemoData"]
    assert probe._database_engines == {}
