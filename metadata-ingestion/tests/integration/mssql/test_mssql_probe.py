"""The MSSQL probe against a live SQL Server, checked against real ingestion.

The unit tests stand sqlite in for SQL Server. Only a server proves the parts
that are SQL Server's own: per-database engines, `[John.Doe]` quoting,
MS_Description, pytds' paramstyle with a `%` in a literal -- and, above all,
that `probe filter` says "included" for exactly the objects ingestion emits.
"""

import json
import os
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

import pytest
import yaml

from datahub.ingestion.agent.filter_check import check_filters
from datahub.ingestion.agent.probe_methods import run_probe_method
from datahub.ingestion.run.pipeline import Pipeline
from tests.integration.mssql.test_sql_server import mssql_runner  # noqa: F401

_PROC = "Stored Procedure"


def _fixture_password() -> str:
    # The throwaway container's own SA password, read from its compose file so
    # it lives in one place.
    compose = yaml.safe_load((Path(__file__).parent / "docker-compose.yml").read_text())
    return str(compose["services"]["testsqlserver"]["environment"]["SA_PASSWORD"])


def _config(**extra: object) -> Dict[str, object]:
    return {
        "host_port": f"localhost:{os.environ['MSSQL_PORT']}",
        "username": "sa",
        "password": _fixture_password(),
        **extra,
    }


def _run(
    command: str,
    config: Optional[Dict[str, object]] = None,
    source_type: str = "mssql",
    **kwargs: object,
) -> Dict[str, object]:
    return run_probe_method(
        source_type=source_type,
        command=command,
        config_dict=config if config is not None else _config(),
        kwargs=dict(kwargs),
    ).to_dict()


def _run_with(
    command: str, config: Dict[str, object], params: Dict[str, object]
) -> Dict[str, object]:
    return run_probe_method(
        source_type="mssql", command=command, config_dict=config, kwargs=params
    ).to_dict()


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


def _ingest(config: Dict[str, object], tmp_path: Path) -> Tuple[Set[str], Set[str]]:
    """Run real ingestion; return the datasets and procedures it emitted, as
    `database.schema.name` (dataset) and `database.schema.procedure`."""
    out = tmp_path / "mces.json"
    pipeline = Pipeline.create(
        {
            "run_id": "mssql-probe-test",
            "source": {"type": "mssql", "config": config},
            "sink": {"type": "file", "config": {"filename": str(out)}},
        }
    )
    pipeline.run()
    pipeline.raise_from_status()
    datasets: Set[str] = set()
    procedures: Set[str] = set()
    for record in json.loads(out.read_text()):
        # Tables and views are emitted as DatasetSnapshot MCEs; an upstream
        # lineage edge only names a dataset, so MCP urns would overcount.
        snapshot = (record.get("proposedSnapshot") or {}).get(
            "com.linkedin.pegasus2avro.metadata.snapshot.DatasetSnapshot"
        )
        if snapshot:
            # urn:li:dataset:(urn:li:dataPlatform:mssql,<name>,PROD)
            datasets.add(str(snapshot["urn"]).split(",")[1])
        urn = str(record.get("entityUrn") or "")
        aspect = record.get("aspectName")
        if urn.startswith("urn:li:dataJob:") and aspect == "dataJobInfo":
            # urn:li:dataJob:(urn:li:dataFlow:(mssql,<db>.<schema>.stored_procedures,PROD),<name>)
            flow, name = urn.rsplit("),", 1)
            container = flow.split(",")[1]
            if container.endswith(".stored_procedures"):
                procedures.add(
                    f"{container[: -len('.stored_procedures')]}.{name.rstrip(')')}"
                )
    return datasets, procedures


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


def _assert_probe_agrees_with_ingestion(
    config: Dict[str, object], tmp_path: Path
) -> Dict[str, Set[bool]]:
    """Walk what the probe lists, and check every verdict `probe filter` gives
    against what ingestion emitted -- including for the objects inside an
    excluded database, which the probe lists and ingestion never reaches."""
    datasets, procedures = _ingest(config, tmp_path)
    assert datasets, "ingestion emitted nothing; the comparison would be vacuous"

    pinned = bool(config.get("database"))
    databases = _names(_run("databases", config=config))
    database_verdicts = _included(config, "Database", [], databases)
    # Per kind, so one kind with both answers cannot hide another with one.
    outcomes: Dict[str, Set[bool]] = {"Table": set(), "View": set(), _PROC: set()}
    probed_datasets: Set[str] = set()
    probed_procedures: Set[str] = set()
    for database in databases:
        # A pinned recipe reads its own database through the base connection.
        db_arg: Dict[str, object] = {} if pinned else {"database": database}
        schemas = _run_with("containers", config, db_arg)
        schema_names = _names(schemas)
        assert schemas["parent_path"] == ([] if pinned else [database])
        for schema in schema_names:
            if schema.startswith("db_") or schema in ("sys", "INFORMATION_SCHEMA"):
                # Ingestion walks these too, but they hold nothing to compare.
                continue
            if "." in schema and not config.get("quote_schemas"):
                # Unquoted, the dialect -- ingestion's and the probe's alike --
                # reads `John.Doe` as database `John`; test_quoted_schema_with_a_dot
                # covers the quoted form.
                continue
            for command, kind in (("tables", "Table"), ("views", "View")):
                listing = _run_with(command, config, {"schema": schema, **db_arg})
                names = _names(listing)
                parent = _strings(listing, "parent_path")
                verdicts = _included(config, kind, parent, names)
                for name in names:
                    qualified = f"{database}.{schema}.{name}"
                    probed_datasets.add(qualified)
                    emitted = qualified in datasets
                    assert verdicts[name] == emitted, (
                        f"{kind} {database}.{schema}.{name}: probe says "
                        f"{verdicts[name]}, ingestion emitted {emitted}"
                    )
                    outcomes[kind].add(emitted)
            listing = _run_with("procedures", config, {"schema": schema, **db_arg})
            names = _names(listing)
            parent = _strings(listing, "parent_path")
            verdicts = _included(config, _PROC, parent, names)
            for name in names:
                qualified = f"{database}.{schema}.{name}"
                probed_procedures.add(qualified)
                emitted = qualified in procedures
                assert verdicts[name] == emitted, (
                    f"procedure {database}.{schema}.{name}: probe says "
                    f"{verdicts[name]}, ingestion emitted {emitted}"
                )
                outcomes[_PROC].add(emitted)
        if not database_verdicts[database]:
            assert not any(d.startswith(f"{database}.") for d in datasets)
    # Nothing ingestion emitted escaped the probe's listings.
    assert datasets <= probed_datasets, datasets - probed_datasets
    assert procedures <= probed_procedures, procedures - probed_procedures
    return outcomes


@pytest.mark.integration
def test_multi_database_verdicts_match_ingestion(
    mssql_runner: object,  # noqa: F811
    tmp_path: Path,
) -> None:
    config = _config(
        database_pattern={"deny": ["^NewData$", ".*SPEC_SYMB.*"]}, **_FILTERS
    )
    outcomes = _assert_probe_agrees_with_ingestion(config, tmp_path)
    # Each kind saw both answers, or its filter above tested nothing.
    assert outcomes == {k: {True, False} for k in ("Table", "View", _PROC)}


@pytest.mark.integration
def test_pinned_database_verdicts_match_ingestion(
    mssql_runner: object,  # noqa: F811
    tmp_path: Path,
) -> None:
    config = _config(database="DemoData", **_FILTERS)
    outcomes = _assert_probe_agrees_with_ingestion(config, tmp_path)
    # DemoData's one view is allowed; the denied one lives in NewData, which a
    # recipe pinned to DemoData never walks.
    assert outcomes == {"Table": {True, False}, "View": {True}, _PROC: {True, False}}
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
def test_every_command_answers_across_databases(
    mssql_runner: object,  # noqa: F811
) -> None:
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
    mssql_runner: object,  # noqa: F811
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
    mssql_runner: object,  # noqa: F811
) -> None:
    config = _config(database="DB_WITH@SPEC_SYMB", quote_schemas=True)
    procedures = _run("procedures", config=config, schema="John.Doe")
    assert _names(procedures) == ["InitialProcedure's"]


@pytest.mark.integration
def test_an_odbc_recipe_reads_sql_variant(
    mssql_runner: object,  # noqa: F811
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
    result = _run(
        "sql",
        config=config,
        source_type="mssql-odbc",
        query="SELECT value FROM DemoData.sys.extended_properties",
    )
    assert _rows(result)
