from pathlib import Path
from typing import Dict, List, Tuple, cast

import pytest
from sqlalchemy import create_engine
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.sql import quoted_name

from datahub.ingestion.agent.config_validation import validate_source_config
from datahub.ingestion.agent.filter_check import check_filters
from datahub.ingestion.agent.probe_methods import list_probe_methods
from datahub.ingestion.agent.recipe import validate_recipe
from datahub.ingestion.agent.sql_gate import SqlScopeError, check_query_scope
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


_PROC = "Stored Procedure"


def test_procedure_verdicts_match_loop_stored_procedures() -> None:
    config = {
        **_BASE,
        "convert_urns_to_lowercase": True,
        "procedure_pattern": {
            "deny": ["^DemoData\\.Foo\\.NewProc$"],
            "ignoreCase": False,
        },
    }
    result = check_filters(
        source_type="mssql",
        config_dict=config,
        kind=_PROC,
        parent_path=["DemoData", "Foo"],
        names=["NewProc", "OtherProc"],
    )
    assert result.pattern_field == "procedure_pattern"
    # Not lowercased: loop_stored_procedures builds the name by hand, unlike
    # get_identifier, so a case-sensitive deny still bites.
    assert [(v.target, v.included) for v in result.results] == [
        ("DemoData.Foo.NewProc", False),
        ("DemoData.Foo.OtherProc", True),
    ]


def test_a_pinned_recipe_judges_procedures_under_its_pin() -> None:
    result = check_filters(
        source_type="mssql",
        config_dict={
            **_BASE,
            "database": "DemoData",
            "procedure_pattern": {"deny": ["^DemoData\\.Foo\\.NewProc$"]},
        },
        kind=_PROC,
        parent_path=["Foo"],
        names=["NewProc"],
    )
    assert result.results[0].target == "DemoData.Foo.NewProc"
    assert result.results[0].excluded_by == "procedure_pattern"


def test_a_procedure_in_an_excluded_schema_is_excluded_by_that_schema() -> None:
    result = check_filters(
        source_type="mssql",
        config_dict={**_BASE, "schema_pattern": {"deny": ["^Foo$"]}},
        kind=_PROC,
        parent_path=["DemoData", "Foo"],
        names=["NewProc"],
    )
    assert result.results[0].excluded_by == "schema_pattern"


def test_a_procedure_in_an_excluded_database_is_excluded_by_that_database() -> None:
    result = check_filters(
        source_type="mssql",
        config_dict={**_BASE, "database_pattern": {"deny": ["^DemoData$"]}},
        kind=_PROC,
        parent_path=["DemoData", "Foo"],
        names=["NewProc"],
    )
    assert result.results[0].excluded_by == "database_pattern"


def test_include_stored_procedures_false_excludes_every_procedure() -> None:
    result = check_filters(
        source_type="mssql",
        config_dict={**_BASE, "include_stored_procedures": False},
        kind=_PROC,
        parent_path=["DemoData", "Foo"],
        names=["NewProc"],
    )
    assert result.results[0].excluded_by == "include_stored_procedures"


@pytest.mark.parametrize(
    "schema",
    [
        "x' UNION SELECT name FROM sys.sql_logins --",
        "main'; DROP PROCEDURE p; --",
        "main]",
    ],
)
def test_procedures_refuses_a_schema_the_server_does_not_list(
    tmp_path: Path, schema: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    from datahub.ingestion.source.sql.mssql.source import SQLServerSource

    def _never(conn: object, db_name: str, schema: str) -> List[Dict[str, str]]:
        raise AssertionError("an unlisted schema reached the procedure query")

    monkeypatch.setattr(SQLServerSource, "_get_stored_procedures", staticmethod(_never))
    with _probe(tmp_path, database="DemoData") as probe:
        with pytest.raises(ValueError, match="containers"):
            probe.procedures(schema=schema)


def test_procedures_lists_names_from_the_ingestion_fetcher(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from datahub.ingestion.source.sql.mssql.source import SQLServerSource

    calls: List[object] = []

    def fake(conn: object, db_name: str, schema: str) -> List[Dict[str, str]]:
        calls.append((db_name, schema))
        return [{"db": db_name, "schema": schema, "name": "NewProc"}]

    monkeypatch.setattr(SQLServerSource, "_get_stored_procedures", staticmethod(fake))
    with _probe(tmp_path, database="DemoData") as probe:
        assert probe.procedures(schema="MAIN") == ["NewProc"]
    with _probe(tmp_path) as probe:
        assert probe.procedures(schema="main", database="newdata") == ["NewProc"]
    # Server spelling of the schema; the pinned name, or the server's
    # spelling of the named database.
    assert calls == [("DemoData", "main"), ("NewData", "main")]


def test_the_procedure_query_binds_the_schema_and_quotes_the_database() -> None:
    from datahub.ingestion.source.sql.mssql.source import SQLServerSource

    sent: List[Tuple[str, Dict[str, str]]] = []

    class _Conn:
        """Records the statement; the one Connection method the fetcher uses."""

        def execute(self, statement: object, params: Dict[str, str]) -> "_Conn":
            sent.append((str(statement), params))
            return self

        def mappings(self) -> List[Dict[str, str]]:
            return []

    SQLServerSource._get_stored_procedures(
        cast(Connection, _Conn()), "Odd]Db", "Foo's"
    )
    statement, params = sent[0]
    assert "[Odd]]Db].[sys].[procedures]" in statement
    assert "Foo's" not in statement
    assert params == {"schema": "Foo's"}


def test_pinned_recipe_database_verdicts_follow_the_pin() -> None:
    config = {
        **_BASE,
        "database": "DemoData",
        "database_pattern": {"deny": ["^DemoData$"]},
    }
    result = check_filters(
        source_type="mssql",
        config_dict=config,
        kind="Database",
        parent_path=[],
        names=["demodata", "NewData", "master"],
    )
    assert [(v.included, v.excluded_by) for v in result.results] == [
        (True, None),  # get_inspectors never consults database_pattern when pinned
        (False, "database"),  # ...and never opens another database
        (False, "database"),
    ]


def test_a_pinned_system_database_is_read() -> None:
    result = check_filters(
        source_type="mssql",
        config_dict={**_BASE, "database": "master"},
        kind="Database",
        parent_path=[],
        names=["master"],
    )
    assert result.results[0].included


def test_a_multi_database_recipe_still_excludes_system_databases() -> None:
    result = check_filters(
        source_type="mssql",
        config_dict=_BASE,
        kind="Database",
        parent_path=[],
        names=["master", "DemoData"],
    )
    assert [(v.included, v.excluded_by) for v in result.results] == [
        (False, "default_database"),
        (True, None),
    ]


def test_a_table_under_another_database_is_excluded_on_a_pinned_recipe() -> None:
    result = check_filters(
        source_type="mssql",
        config_dict={**_BASE, "database": "DemoData"},
        kind="Table",
        parent_path=["NewData", "dbo"],
        names=["orders"],
    )
    assert result.results[0].excluded_by == "database"


@pytest.mark.parametrize(
    "extra, parent_path, target",
    [
        # get_identifier prefixes config.database, whatever --parent spells.
        ({"database": "DemoData"}, ["demodata", "dbo"], "DemoData.dbo.orders"),
        ({"database": "DemoData"}, ["dbo"], "DemoData.dbo.orders"),
        # sqlalchemy_uri sets no current_database, so ingestion qualifies
        # nothing -- a --parent database must not add one.
        (
            {
                "sqlalchemy_uri": "mssql+pyodbc:///?odbc_connect=DRIVER%3D%7Bx%7D%3BDATABASE%3DNewData%3B"
            },
            ["NewData", "dbo"],
            "dbo.orders",
        ),
        ({}, ["DemoData", "dbo"], "DemoData.dbo.orders"),
    ],
)
def test_table_targets_are_the_identifier_ingestion_builds(
    extra: Dict[str, object], parent_path: List[str], target: str
) -> None:
    result = check_filters(
        source_type="mssql",
        config_dict={**_BASE, **extra},
        kind="Table",
        parent_path=parent_path,
        names=["orders"],
    )
    assert result.results[0].target == target
    assert result.results[0].included


def test_multi_database_recipe_warns_when_the_database_parent_is_missing() -> None:
    result = check_filters(
        source_type="mssql",
        config_dict={
            **_BASE,
            "table_pattern": {"allow": ["^DemoData\\.dbo\\.orders$"]},
        },
        kind="Table",
        parent_path=["dbo"],
        names=["orders"],
    )
    assert any("--parent" in w and "database" in w for w in result.warnings), (
        result.warnings
    )



@pytest.mark.parametrize(
    "query",
    [
        # Procedure source is metadata ingestion already emits
        # (include_stored_procedures_code), so the standard view of it is read.
        "SELECT routine_name, routine_definition FROM information_schema.routines",
        "SELECT * FROM DemoData.INFORMATION_SCHEMA.ROUTINES",
        "SELECT TOP 5 name FROM DemoData.sys.tables WHERE name LIKE 'P%'",
        "SELECT name FROM [DemoData].[sys].[procedures] WITH (NOLOCK)",
    ],
)
def test_the_sql_scope_admits_the_catalog_ingestion_reads(query: str) -> None:
    check_query_scope(
        query, platform="tsql", scope=SQLServerConfig.probe_catalog_scope()
    )


@pytest.mark.parametrize(
    "query",
    [
        "SELECT * FROM Foo.Items",
        "SELECT * FROM DemoData.Foo.Items",
        "SELECT * FROM [DemoData].[dbo].[orders]",
        "SELECT routine_name FROM information_schema.routines "
        "UNION SELECT ItemName FROM DemoData.Foo.Items",
        "SELECT name FROM sys.tables WHERE name IN (SELECT ItemName FROM Foo.Items)",
        "SELECT definition FROM DemoData.sys.sql_modules",
        "SELECT * FROM OPENQUERY(remote, 'SELECT * FROM t')",
        "EXEC sp_helptext 'Foo.NewProc'",
    ],
)
def test_the_sql_scope_never_opens_a_user_table(query: str) -> None:
    with pytest.raises(SqlScopeError):
        check_query_scope(
            query, platform="tsql", scope=SQLServerConfig.probe_catalog_scope()
        )


def test_sql_reaches_the_driver_with_no_parameter_set(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """pytds %-formats a statement whenever it is handed parameters, even an
    empty set, so `LIKE 'P%'` failed after the gate had cleared it. The query
    must reach cursor.execute alone."""
    calls: List[Tuple[object, ...]] = []

    class _Cursor:
        description = [("name",)]

        def execute(self, *args: object) -> None:
            calls.append(args)

        def fetchmany(self, n: int) -> List[Tuple[str]]:
            return [("Persons",)]

        def close(self) -> None:
            pass

    class _Raw:
        def cursor(self) -> _Cursor:
            return _Cursor()

    class _Conn:
        connection = _Raw()

        def __enter__(self) -> "_Conn":
            return self

        def __exit__(self, *exc: object) -> None:
            pass

    class _Engine:
        def connect(self) -> _Conn:
            return _Conn()

    with _probe(tmp_path, database="DemoData") as probe:
        monkeypatch.setattr(probe, "_engine", _Engine())
        rows = probe.execute_catalog_query(
            "SELECT name FROM sys.tables WHERE name LIKE 'P%'", limit=5
        )
        # The real engine is put back before __exit__ disposes it.
        monkeypatch.undo()
    assert calls == [("SELECT name FROM sys.tables WHERE name LIKE 'P%'",)]
    assert rows.columns == ["name"] and rows.rows == [["Persons"]]


def test_a_schema_resolved_to_another_spelling_says_so(tmp_path: Path) -> None:
    """parent_path carries the caller's `MAIN`; a case-sensitive pattern
    judges ingestion's `main`, so the caller has to be told which to pass."""
    with _probe(tmp_path, database="DemoData") as probe:
        assert probe.tables(schema="MAIN") == ["t_default"]
        assert any("'main'" in w for w in probe.warnings), probe.warnings
    with _probe(tmp_path, database="DemoData") as probe:
        probe.tables(schema="main")
        assert probe.warnings == []
