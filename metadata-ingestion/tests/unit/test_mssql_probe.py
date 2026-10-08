from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, cast

import pytest
from sqlalchemy import create_engine, event
from sqlalchemy.engine import Connection, Engine, make_url
from sqlalchemy.sql import quoted_name

from datahub.ingestion.agent.config_validation import validate_source_config
from datahub.ingestion.agent.filter_check import check_filters
from datahub.ingestion.agent.probe_methods import list_probe_methods
from datahub.ingestion.agent.recipe import validate_recipe
from datahub.ingestion.agent.sql_gate import SqlScopeError, check_query_scope
from datahub.ingestion.agent.sql_passthrough import QueryBudget
from datahub.ingestion.source.sql.mssql.mssql_probe import SqlServerMetadataProbe
from datahub.ingestion.source.sql.mssql.source import (
    SQLServerConfig,
    SQLServerSource,
    add_sql_variant_converter,
    database_name_from_url,
)
from datahub.ingestion.source.sql.sql_probe import execute_on_cursor

_BASE: Dict[str, object] = {"host_port": "h:1433", "username": "u", "password": "p"}
_ODBC: Dict[str, object] = {
    **_BASE,
    "uri_args": {"driver": "ODBC Driver 18 for SQL Server"},
}


def test_an_odbc_recipe_validates_as_ingestion_validates_it() -> None:
    recipe: Dict[str, object] = {"source": {"type": "mssql-odbc", "config": _ODBC}}
    assert validate_recipe(recipe)["errors"] == []


def test_uri_args_are_still_refused_on_the_pytds_source_type() -> None:
    recipe: Dict[str, object] = {"source": {"type": "mssql", "config": _ODBC}}
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
    "database" is a sqlite file whose one table is named after it. A name in
    `unreachable` gets an engine that cannot connect."""

    server_databases: List[str] = ["DemoData", "NewData"]

    def __init__(self, engine: Engine, config: SQLServerConfig, tmp_path: Path) -> None:
        super().__init__(engine, config)
        self._tmp_path = tmp_path
        self.opened: List[str] = []
        self.disposed: List[str] = []
        self.unreachable: List[str] = []

    def _list_databases(self) -> List[str]:
        return list(self.server_databases)

    def _open_database_engine(self, name: str) -> Engine:
        self.opened.append(name)
        if name in self.unreachable:
            engine = create_engine(f"sqlite:///{self._tmp_path}/missing/{name}.db")
        else:
            engine = _sqlite(self._tmp_path, name, f"t_{name.lower()}")

        def _disposed(_engine: Engine) -> None:
            self.disposed.append(name)

        event.listen(engine, "engine_disposed", _disposed)
        return engine


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
    specs = {s.command: s for s in list_probe_methods("mssql")}
    assert specs["tables"].parent_params == ("database", "schema")
    assert specs["views"].parent_params == ("database", "schema")
    assert specs["containers"].parent_params == ("database",)
    assert specs["containers"].kind == "Schema"
    assert specs["databases"].kind == "Database"


def test_the_sql_scope_parses_tsql(tmp_path: Path) -> None:
    query = "SELECT TOP 5 [name] FROM [DemoData].[sys].[tables] WHERE [name] LIKE 'P%'"
    scope = SQLServerConfig.probe_catalog_scope()
    with _probe(tmp_path, database="DemoData") as probe:
        assert probe.sql_dialect is not None
        check_query_scope(query, platform=probe.sql_dialect, scope=scope)
        # Read as another dialect, the brackets and TOP do not parse.
        with pytest.raises(SqlScopeError):
            check_query_scope(query, platform="postgres", scope=scope)


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


@pytest.mark.parametrize("quote", [True, False])
def test_quote_schemas_is_the_choice_get_allowed_schemas_makes(quote: bool) -> None:
    config = SQLServerConfig.model_validate({**_BASE, "quote_schemas": quote})
    argument = config.schema_argument("John.Doe")
    assert argument == "John.Doe"
    assert isinstance(argument, quoted_name) is quote


def _dotted_schema(tmp_path: Path) -> Engine:
    """sqlite with a second database attached as `John.Doe`, so the
    connection lists a schema with a dot in its name."""
    engine = _sqlite(tmp_path, "default", "t_default")
    _sqlite(tmp_path, "dotted", "t_dotted").dispose()

    def _attach(dbapi_connection: Any, _record: Any) -> None:
        dbapi_connection.execute(
            f"ATTACH DATABASE '{tmp_path}/dotted.db' AS \"John.Doe\""
        )

    event.listen(engine, "connect", _attach)
    # Drop the connection _sqlite pooled before the listener existed.
    engine.dispose()
    return engine


@pytest.mark.parametrize("quote", [True, False])
def test_an_unquoted_dotted_schema_says_how_ingestion_reads_it(
    tmp_path: Path, quote: bool
) -> None:
    config = SQLServerConfig.model_validate(
        {**_BASE, "database": "DemoData", "quote_schemas": quote}
    )
    with _FakeServer(_dotted_schema(tmp_path), config, tmp_path) as probe:
        probe.tables(schema="John.Doe")
        warned = any("quote_schemas" in w for w in probe.warnings)
    assert warned is not quote


def test_exit_disposes_every_engine_it_opened(tmp_path: Path) -> None:
    with _probe(tmp_path) as probe:
        probe.tables(schema="main", database="DemoData")
        probe.views(schema="main", database="DemoData")
        assert probe.disposed == []
    assert probe.disposed == ["DemoData"]


def test_a_database_that_cannot_be_inspected_keeps_no_engine(tmp_path: Path) -> None:
    with _probe(tmp_path) as probe:
        probe.unreachable.append("NewData")
        for _ in range(2):
            with pytest.raises(Exception):  # noqa: B017 -- the driver's own error
                probe.tables(schema="main", database="NewData")
        # Each failed attempt's engine was disposed at once, not leaked.
        assert probe.disposed == ["NewData", "NewData"]
    assert probe.disposed == ["NewData", "NewData"]


def test_databases_of_a_recipe_naming_no_database_is_empty_and_says_why(
    tmp_path: Path,
) -> None:
    with _probe(tmp_path, **_NO_DATABASE_URI) as probe:
        assert probe.databases() == []
        assert any("default" in w for w in probe.warnings), probe.warnings


def test_a_recipe_naming_no_database_refuses_a_named_one(tmp_path: Path) -> None:
    with _probe(tmp_path, **_NO_DATABASE_URI) as probe:
        assert probe.tables(schema="main") == ["t_default"]
        with pytest.raises(ValueError, match="--database"):
            probe.tables(schema="main", database="DemoData")


def test_procedures_on_a_recipe_naming_no_database_are_refused(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Ingestion would query `[].[sys].[procedures]` here and fail.
    def _never(conn: object, db_name: str, schema: str) -> List[Dict[str, str]]:
        raise AssertionError("the procedure query ran with no database")

    monkeypatch.setattr(SQLServerSource, "_get_stored_procedures", staticmethod(_never))
    with _probe(tmp_path, **_NO_DATABASE_URI) as probe:
        with pytest.raises(ValueError, match="sqlalchemy_uri"):
            probe.procedures(schema="main")


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
    def _never(conn: object, db_name: str, schema: str) -> List[Dict[str, str]]:
        raise AssertionError("an unlisted schema reached the procedure query")

    monkeypatch.setattr(SQLServerSource, "_get_stored_procedures", staticmethod(_never))
    with _probe(tmp_path, database="DemoData") as probe:
        with pytest.raises(ValueError, match="containers"):
            probe.procedures(schema=schema)


def test_procedures_lists_names_from_the_ingestion_fetcher(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
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
    sent: List[Tuple[str, Dict[str, str]]] = []

    class _Conn:
        """Records the statement; the one Connection method the fetcher uses."""

        def execute(self, statement: object, params: Dict[str, str]) -> "_Conn":
            sent.append((str(statement), params))
            return self

        def mappings(self) -> List[Dict[str, str]]:
            return []

    SQLServerSource._get_stored_procedures(cast(Connection, _Conn()), "Odd]Db", "Foo's")
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


_NO_DATABASE_URI: Dict[str, object] = {"sqlalchemy_uri": "mssql+pytds://u:p@h:1433"}


def test_a_recipe_naming_no_database_never_excludes_a_database() -> None:
    """get_inspectors' single branch reads the login's default database --
    often `master` -- and consults neither database_pattern nor the system
    list, so neither may exclude a name here."""
    result = check_filters(
        source_type="mssql",
        config_dict={**_NO_DATABASE_URI, "database_pattern": {"deny": ["^Other$"]}},
        kind="Database",
        parent_path=[],
        names=["master", "Other"],
    )
    assert [(v.included, v.excluded_by) for v in result.results] == [
        (True, None),
        (True, None),
    ]
    assert any("default" in w for w in result.warnings), result.warnings


def test_a_table_in_master_is_not_excluded_on_a_recipe_naming_no_database() -> None:
    result = check_filters(
        source_type="mssql",
        config_dict=_NO_DATABASE_URI,
        kind="Table",
        parent_path=["master", "dbo"],
        names=["t1"],
    )
    assert result.results[0].included


def test_a_recipe_naming_no_database_emits_no_procedures() -> None:
    """loop_stored_procedures reads `[{get_db_name}].[sys].[procedures]`, and
    get_db_name is "" here: SQL Server rejects `[]`, so no procedure is
    emitted, whatever procedure_pattern allows."""
    result = check_filters(
        source_type="mssql",
        config_dict=_NO_DATABASE_URI,
        kind=_PROC,
        parent_path=["dbo"],
        names=["NewProc"],
    )
    assert result.results[0].excluded_by == "sqlalchemy_uri"
    assert any("sqlalchemy_uri" in w for w in result.warnings), result.warnings
    # include_stored_procedures off is still the reason it states first.
    switched_off = check_filters(
        source_type="mssql",
        config_dict={**_NO_DATABASE_URI, "include_stored_procedures": False},
        kind=_PROC,
        parent_path=["dbo"],
        names=["NewProc"],
    )
    assert switched_off.results[0].excluded_by == "include_stored_procedures"


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


def test_sql_reaches_the_driver_with_no_parameter_set() -> None:
    """pytds %-formats a statement whenever it is handed parameters, even an
    empty set, so `LIKE 'P%'` failed after the gate had cleared it. The query
    must reach cursor.execute alone."""
    calls: List[Tuple[object, ...]] = []
    closed: List[bool] = []

    class _Cursor:
        description = [("name",)]

        def execute(self, *args: object) -> None:
            calls.append(args)

        def fetchmany(self, n: int) -> List[Tuple[str]]:
            return [("Persons",)]

        def close(self) -> None:
            closed.append(True)

    query = "SELECT name FROM sys.tables WHERE name LIKE 'P%'"
    rows = execute_on_cursor(_Cursor(), query, limit=5)
    assert calls == [(query,)]
    assert rows.columns == ["name"] and rows.rows == [["Persons"]]
    assert closed == [True]


def test_sql_closes_the_cursor_when_the_query_fails() -> None:
    closed: List[bool] = []

    class _Cursor:
        description = None

        def execute(self, *args: object) -> None:
            raise RuntimeError("server refused the query")

        def fetchmany(self, n: int) -> List[Tuple[str]]:
            raise AssertionError("fetched after a failed execute")

        def close(self) -> None:
            closed.append(True)

    with pytest.raises(RuntimeError):
        execute_on_cursor(_Cursor(), "SELECT name FROM sys.tables", limit=5)
    assert closed == [True]


def test_a_schema_resolved_to_another_spelling_says_so(tmp_path: Path) -> None:
    """parent_path carries the caller's `MAIN`; a case-sensitive pattern
    judges ingestion's `main`, so the caller has to be told which to pass."""
    with _probe(tmp_path, database="DemoData") as probe:
        assert probe.tables(schema="MAIN") == ["t_default"]
        assert any("'main'" in w for w in probe.warnings), probe.warnings
    with _probe(tmp_path, database="DemoData") as probe:
        probe.tables(schema="main")
        assert probe.warnings == []


def test_a_procedure_judged_without_a_parent_keeps_its_bare_name() -> None:
    result = check_filters(
        source_type="mssql",
        config_dict={**_BASE, "procedure_pattern": {"deny": ["^NewProc$"]}},
        kind=_PROC,
        parent_path=[],
        names=["NewProc"],
    )
    assert result.results[0].target == "NewProc"
    assert any("no parent" in w for w in result.warnings), result.warnings


class _FakePyodbc:
    """Just enough DB-API module for SQLAlchemy to build (not connect) a
    pyodbc engine, which needs the native ODBC library to import."""

    paramstyle = "qmark"
    version = "5.0.0"
    Error = Exception

    class Cursor:
        pass


@pytest.mark.parametrize(
    "url, module, listens",
    [
        ("mssql+pyodbc://u:p@h/db?driver=x", _FakePyodbc, True),
        ("mssql+pytds://u:p@h/db", None, False),
    ],
)
def test_the_probe_engine_reads_sql_variant_on_pyodbc_only(
    url: str, module: Optional[type], listens: bool
) -> None:
    """The converter is an ODBC hook, so only a pyodbc engine gets it."""
    engine = create_engine(url, **({"module": module} if module else {}))
    settings = SQLServerConfig.model_validate(_BASE).probe_engine_settings(
        QueryBudget(timeout_seconds=30)
    )
    before = len(engine.pool.dispatch.connect)

    assert settings.prepare is not None
    settings.prepare(engine)
    assert len(engine.pool.dispatch.connect) == before + (1 if listens else 0)


@pytest.mark.parametrize(
    "url, database",
    [
        ('mssql+pytds://u:p@h:1433/"DemoData"', "DemoData"),
        ("mssql+pytds://u:p@h:1433", ""),
        (
            "mssql+pyodbc:///?odbc_connect=DRIVER%3D%7Bx%7D%3BDATABASE%3DNewData%3B",
            "NewData",
        ),
        pytest.param(
            "mssql+pyodbc:///?odbc_connect=DRIVER%3D%7Bx%7D%3BDATABASE%3DNewData",
            "NewData",
            marks=pytest.mark.xfail(
                strict=True,
                reason="DATABASE= must end in ';' today; fixing it changes "
                "ingestion's database name for such recipes",
            ),
        ),
    ],
)
def test_the_database_name_ingestion_reads_from_a_url(url: str, database: str) -> None:
    assert database_name_from_url(make_url(url)) == database
