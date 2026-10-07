"""The MSSQL probe against a live SQL Server, checked against real ingestion.

The unit tests stand sqlite in for SQL Server. Only a server proves the parts
that are SQL Server's own: per-database engines, `[John.Doe]` quoting,
MS_Description, pytds' paramstyle with a `%` in a literal -- and, above all,
that `probe filter` says "included" for exactly the objects ingestion emits.
"""

import os
from pathlib import Path
from typing import Callable, Dict, Iterator, List, Optional, Sequence, Set

import pytest

from datahub.ingestion.agent.filter_check import check_filters
from datahub.ingestion.agent.probe_methods import run_probe_method
from datahub.ingestion.source.common.subtypes import (
    DatasetContainerSubTypes,
    DatasetSubTypes,
)
from datahub.metadata.schema_classes import (
    ContainerClass,
    ContainerPropertiesClass,
    DataJobInfoClass,
    SubTypesClass,
)
from datahub.metadata.urns import DataJobUrn, DatasetUrn
from tests.integration.mssql.common import run_sqlcmd, sa_password
from tests.test_helpers.probe_parity import (
    EmittedIndex,
    FanOut,
    JudgedRecord,
    ParityListing,
    ParityReport,
    assert_probe_parity,
    pipeline_ingestion,
)

# The parity harness masks its reports against the process-global registry, as
# the CLI does; a secret an earlier test in the batch registered would otherwise
# redact any of this fixture's identifiers that contain it.
pytestmark = pytest.mark.usefixtures("_isolate_secret_registry")

_PROC = "Stored Procedure"
_STORED_PROCEDURES = ".stored_procedures"


def _config(**extra: object) -> Dict[str, object]:
    return {
        "host_port": f"localhost:{os.environ['MSSQL_PORT']}",
        "username": "sa",
        "password": sa_password(),
        **extra,
    }


def _run(
    command: str,
    config: Optional[Dict[str, object]] = None,
    source_type: str = "mssql",
    **kwargs: object,
) -> Dict[str, object]:
    return dict(
        run_probe_method(
            source_type=source_type,
            command=command,
            config_dict=config if config is not None else _config(),
            kwargs=dict(kwargs),
        ).to_dict()
    )


def _names(result: Dict[str, object]) -> List[str]:
    listed = result["result"]
    assert isinstance(listed, list)
    return [str(name) for name in listed]


def _strings(result: Dict[str, object], key: str) -> List[str]:
    value = result[key]
    assert isinstance(value, list)
    return [str(item) for item in value]


def _records(result: Dict[str, object]) -> List[Dict[str, object]]:
    value = result["result"]
    assert isinstance(value, list)
    assert all(isinstance(item, dict) for item in value)
    return value


def _mapping(result: Dict[str, object]) -> Dict[str, object]:
    value = result["result"]
    assert isinstance(value, dict)
    return value


def _rows(result: Dict[str, object]) -> List[List[object]]:
    rows = _mapping(result)["rows"]
    assert isinstance(rows, list)
    return rows


def _included(
    config: Dict[str, object], kind: str, parent_path: List[str], names: List[str]
) -> Dict[str, bool]:
    verdicts = check_filters(
        source_type="mssql",
        config_dict=config,
        kind=kind,
        parent_path=parent_path,
        names=names,
    )
    return {v.name: v.included for v in verdicts.results}


def _sqlcmd(*statements: str) -> None:
    ret = run_sqlcmd("-Q", "; ".join(statements))
    assert ret.returncode == 0, ret.stdout + ret.stderr


@pytest.fixture(scope="module")
def odd_databases(mssql_runner: object) -> Iterator[None]:
    """Two databases setup.sql (and so the goldens) leaves out: one whose
    name holds `]` with a schema holding `'` and a procedure in it, and one
    with a case-sensitive collation holding schemas `Foo` and `FOO`."""
    _sqlcmd("CREATE DATABASE [Odd]]Db]")
    _sqlcmd(
        "USE [Odd]]Db]",
        "EXEC('CREATE SCHEMA [It''s]')",
        "EXEC('CREATE TABLE [It''s].t1 (id int)')",
        "EXEC('CREATE VIEW [It''s].v1 AS SELECT id FROM [It''s].t1')",
        "EXEC('CREATE PROCEDURE [It''s].[Proc1] AS SELECT 1')",
    )
    _sqlcmd("CREATE DATABASE CsData COLLATE Latin1_General_CS_AS")
    _sqlcmd(
        "USE CsData",
        "EXEC('CREATE SCHEMA Foo')",
        "EXEC('CREATE SCHEMA FOO')",
        "EXEC('CREATE TABLE Foo.t_lower (id int)')",
        "EXEC('CREATE TABLE FOO.t_upper (id int)')",
    )
    yield
    # Dropped here rather than left to the container's teardown, so a reused
    # container never shows them to the all-database golden recipes.
    for database in ("[Odd]]Db]", "CsData"):
        _sqlcmd(
            f"ALTER DATABASE {database} SET SINGLE_USER WITH ROLLBACK IMMEDIATE",
            f"DROP DATABASE {database}",
        )


def _database_of(index: EmittedIndex, urn: str) -> Optional[str]:
    """The name of the container `urn` sits in, if it sits in one."""
    parent = next(
        (a.container for a in index.aspects[urn] if isinstance(a, ContainerClass)),
        None,
    )
    if parent is None:
        return None
    return next(
        (
            a.name
            for a in index.aspects.get(parent, [])
            if isinstance(a, ContainerPropertiesClass)
        ),
        None,
    )


def _schemas_in(database: str) -> Callable[[EmittedIndex], Set[str]]:
    """Schema containers under `database`, by bare name: every database has a
    `dbo`, so EmittedIndex.container_names would merge them."""

    def emitted(index: EmittedIndex) -> Set[str]:
        names: Set[str] = set()
        for urn, aspects in index.aspects.items():
            if not urn.startswith("urn:li:container:"):
                continue
            is_schema = any(
                isinstance(a, SubTypesClass)
                and DatasetContainerSubTypes.SCHEMA in a.typeNames
                for a in aspects
            )
            properties = next(
                (a for a in aspects if isinstance(a, ContainerPropertiesClass)), None
            )
            if is_schema and properties and _database_of(index, urn) == database:
                names.add(properties.name)
        return names

    return emitted


def _datasets_in(database: str, sub_type: str) -> Callable[[EmittedIndex], Set[str]]:
    """Emitted datasets of one subtype in `database`, as database.schema.name."""

    def emitted(index: EmittedIndex) -> Set[str]:
        return {
            name
            for urn in index.urns("dataset", with_aspect=SubTypesClass)
            if any(
                isinstance(a, SubTypesClass) and sub_type in a.typeNames
                for a in index.aspects[urn]
            )
            and (name := DatasetUrn.from_string(urn).name).startswith(f"{database}.")
        }

    return emitted


def _procedures_in(database: str) -> Callable[[EmittedIndex], Set[str]]:
    """Emitted procedures in `database`, as database.schema.procedure: the
    flow is `<database>.<schema>.stored_procedures`."""

    def emitted(index: EmittedIndex) -> Set[str]:
        names: Set[str] = set()
        for urn in index.urns("dataJob", with_aspect=DataJobInfoClass):
            job = DataJobUrn.from_string(urn)
            flow = job.get_data_flow_urn().flow_id
            if flow.endswith(_STORED_PROCEDURES) and flow.startswith(f"{database}."):
                names.add(f"{flow[: -len(_STORED_PROCEDURES)]}.{job.job_id}")
        return names

    return emitted


def _under(database: str) -> Callable[[JudgedRecord], str]:
    """database.schema.name, whether or not the database is in parent_path
    (a pinned recipe's listings carry only the schema)."""

    def identity(record: JudgedRecord) -> str:
        return ".".join((database, record.parent_path[-1], record.name))

    return identity


def _listings_for(
    database: str,
    pinned: bool,
    expect_empty: bool = False,
    commands: Sequence[str] = ("tables", "views", "procedures"),
) -> List[ParityListing]:
    """Schemas, then `commands` in one database, every schema fanned out
    whether the recipe keeps it or not."""
    db_arg: Dict[str, object] = {} if pinned else {"database": database}
    fan_out = FanOut("containers", "schema", parent_kwargs=db_arg)
    identity = _under(database)
    return [
        ParityListing(
            f"schemas in {database}",
            "containers",
            emitted=_schemas_in(database),
            kwargs=db_arg,
            expect_empty=expect_empty,
        ),
        *(
            ParityListing(
                f"{command} in {database}",
                command,
                emitted=emitted,
                kwargs=db_arg,
                fan_out=fan_out,
                identity=identity,
                expect_empty=expect_empty,
            )
            for command, emitted in (
                ("tables", _datasets_in(database, DatasetSubTypes.TABLE)),
                ("views", _datasets_in(database, DatasetSubTypes.VIEW)),
                ("procedures", _procedures_in(database)),
            )
            if command in commands
        ),
    ]


def _excluded(report: ParityReport, label: str) -> Set[str]:
    return set(report.excluded_by(label))


# The filters a recipe may combine, each pointed at objects setup.sql creates.
_FILTERS: Dict[str, object] = {
    "schema_pattern": {"deny": ["^dbo$"]},
    "table_pattern": {"deny": [".*\\.Foo\\.Persons$"]},
    "view_pattern": {"deny": [".*\\.FooNew\\.View1$"]},
    "procedure_pattern": {"deny": ["DemoData\\.Foo\\.NewProc"]},
    # Not what is being compared, and slow or noisy against this fixture.
    "include_jobs": False,
    "include_lineage": False,
}


@pytest.mark.integration
def test_multi_database_verdicts_match_ingestion(
    mssql_runner: object, tmp_path: Path
) -> None:
    config = _config(
        database_pattern={"allow": ["^DemoData$"]},
        **_FILTERS,
    )
    report = assert_probe_parity(
        "mssql",
        config,
        pipeline_ingestion("mssql", tmp_path),
        [
            ParityListing(
                "databases",
                "databases",
                emitted=lambda index: index.container_names(
                    DatasetContainerSubTypes.DATABASE
                ),
            ),
            *_listings_for("DemoData", pinned=False),
            # database_pattern drops it, so ingestion emits nothing from it.
            # It holds no procedure, so there is none to list.
            *_listings_for(
                "NewData",
                pinned=False,
                expect_empty=True,
                commands=("tables", "views"),
            ),
        ],
    )
    # Each filter above excluded something, or it tested nothing.
    assert "DemoData.Foo.Persons" in _excluded(report, "tables in DemoData")
    assert "DemoData.Foo.NewProc" in _excluded(report, "procedures in DemoData")
    assert "NewData.FooNew.View1" in _excluded(report, "views in NewData")
    assert report.excluded_by("databases")["NewData"] == "database_pattern"


@pytest.mark.integration
def test_pinned_database_verdicts_match_ingestion(
    mssql_runner: object, tmp_path: Path
) -> None:
    config = _config(database="DemoData", **_FILTERS)
    report = assert_probe_parity(
        "mssql",
        config,
        pipeline_ingestion("mssql", tmp_path),
        _listings_for("DemoData", pinned=True),
    )
    assert "DemoData.Foo.Persons" in _excluded(report, "tables in DemoData")
    assert "DemoData.Foo.NewProc" in _excluded(report, "procedures in DemoData")
    # Ingestion never opens another database on a pinned recipe, whatever
    # database_pattern says, and the probe refuses to answer about one.
    assert _included(config, "Database", [], ["NewData", "demodata"]) == {
        "NewData": False,
        "demodata": True,
    }
    assert _included(config, "Table", ["NewData", "FooNew"], ["ItemsNew"]) == {
        "ItemsNew": False
    }
    with pytest.raises(ValueError, match="DemoData"):
        _run("tables", config=config, schema="FooNew", database="NewData")


@pytest.mark.integration
def test_a_quote_and_a_bracket_in_names_still_reach_their_procedures(
    odd_databases: None, tmp_path: Path
) -> None:
    """_get_stored_procedures binds the schema and bracket-quotes the
    database, so `Odd]Db`.`It's` is read rather than breaking the query."""
    config = _config(database="Odd]Db", include_jobs=False, include_lineage=False)
    report = assert_probe_parity(
        "mssql",
        config,
        pipeline_ingestion("mssql", tmp_path),
        _listings_for("Odd]Db", pinned=True),
    )
    assert report.kinds["procedures in Odd]Db"].included == {"Odd]Db.It's.Proc1"}


@pytest.mark.integration
def test_a_name_two_server_spellings_fold_to_is_refused(odd_databases: None) -> None:
    """A case-sensitive collation holds `Foo` and `FOO`; `foo` names neither,
    and picking one would read an object the caller did not name."""
    with pytest.raises(ValueError, match="FOO"):
        _run("tables", schema="foo", database="CsData")
    assert _names(_run("tables", schema="FOO", database="CsData")) == ["t_upper"]


@pytest.mark.integration
def test_every_command_answers_across_databases(mssql_runner: object) -> None:
    databases = _names(_run("databases"))
    assert {"DemoData", "NewData"} <= set(databases)
    assert "master" not in databases

    with pytest.raises(ValueError, match="--database"):
        _run("tables", schema="Foo")

    tables = _run("tables", schema="foo", database="demodata")
    assert "Items" in _names(tables)
    # The caller's spelling travels; the warning names the server's.
    assert tables["parent_path"] == ["demodata", "foo"]
    assert any("'DemoData'" in w for w in _strings(tables, "warnings"))

    assert _names(_run("views", schema="Foo", database="DemoData")) == ["PersonsView"]
    assert "FooNew" in _names(_run("containers", database="NewData"))

    columns = _run("columns", schema="Foo", table="Persons", database="DemoData")
    assert [c["name"] for c in _records(columns)] == [
        "ID",
        "LastName",
        "FirstName",
        "Age",
    ]
    pk = _run("primary_key", schema="Foo", table="Persons", database="DemoData")
    assert _mapping(pk)["constrained_columns"] == ["ID"]
    fks = _run("foreign_keys", schema="Foo", table="SalesReason", database="DemoData")
    assert _records(fks)[0]["referred_table"] == "Persons"
    definition = _run(
        "view_definition", schema="Foo", view="PersonsView", database="DemoData"
    )
    assert "Foo.Persons" in str(definition["result"])

    procedures = _run("procedures", schema="Foo", database="DemoData")
    assert procedures["kind"] == _PROC
    assert {"NewProc", "Proc.With.SpecialChar"} <= set(_names(procedures))

    comment = _run("table_comment", schema="Foo", table="Items", database="DemoData")
    assert comment["result"] == {"text": "Description for table Items of schema Foo."}

    # A `%` in a literal under pytds' pyformat paramstyle, and a three-part
    # name reaching another database's catalog.
    result = _run(
        "sql", query="SELECT TOP 5 name FROM DemoData.sys.tables WHERE name LIKE 'P%'"
    )
    assert sorted(str(row[0]) for row in _rows(result)) == ["Persons", "Products"]
    with pytest.raises(ValueError):
        _run("sql", query="SELECT * FROM DemoData.Foo.Items")


@pytest.mark.integration
@pytest.mark.parametrize(
    "schema",
    [
        "Foo' OR '1'='1",
        "Foo]; SELECT 1; --",
        "Foo UNION SELECT name FROM sys.sql_logins",
        # The dialect reads this as database NewData, owner FooNew, and would
        # switch to it with USE.
        "NewData.FooNew",
    ],
)
def test_injection_shaped_arguments_never_reach_the_server(
    mssql_runner: object,
    schema: str,
) -> None:
    for command in ("tables", "views", "procedures"):
        with pytest.raises(ValueError, match="containers"):
            _run(command, schema=schema, database="DemoData")
    with pytest.raises(ValueError):
        _run("columns", schema="Foo", table="Items' --", database="DemoData")
    with pytest.raises(ValueError):
        _run("tables", schema="Foo", database="DemoData]; SELECT 1; --")


@pytest.mark.integration
def test_quoted_schema_with_a_dot(
    mssql_runner: object,
) -> None:
    config = _config(database="DB_WITH@SPEC_SYMB", quote_schemas=True)
    procedures = _run("procedures", config=config, schema="John.Doe")
    assert _names(procedures) == ["InitialProcedure's"]


@pytest.mark.integration
def test_an_odbc_recipe_reads_sql_variant(
    mssql_runner: object,
) -> None:
    pytest.importorskip("pyodbc")
    config = _config(
        uri_args={
            "driver": "ODBC Driver 18 for SQL Server",
            "TrustServerCertificate": "yes",
        }
    )
    assert "DemoData" in _names(
        _run("databases", config=config, source_type="mssql-odbc")
    )
    # extended_properties.value is sql_variant: pyodbc raises on it unless the
    # converter is installed, and returns bytes if it decodes nothing.
    result = _run(
        "sql",
        config=config,
        source_type="mssql-odbc",
        query="SELECT value FROM DemoData.sys.extended_properties",
    )
    assert "Description for table Items of schema Foo." in [
        row[0] for row in _rows(result)
    ]
    comment = _run(
        "table_comment",
        config=config,
        source_type="mssql-odbc",
        schema="Foo",
        table="Items",
        database="DemoData",
    )
    assert comment["result"] == {"text": "Description for table Items of schema Foo."}
