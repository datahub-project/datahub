"""Hostile identifiers never reach reflection SQL, on any SQLAlchemy dialect.

sqlite is the witness because its own reflection formats identifiers into the
statement text (`SELECT name FROM "<schema>".sqlite_master`, `PRAGMA
"<schema>".table_info("<table>")`), like the dialects this guards -- so a
payload that got through would show up in the recorded SQL. Every payload
carries one alphanumeric marker, which survives any quoting or escaping a
dialect applies, so "not in any statement" is a meaningful check.
"""

import pathlib
from typing import Callable, Dict, FrozenSet, Iterator, List, Tuple

import pytest
from sqlalchemy import create_engine, event, inspect
from sqlalchemy.engine import Engine
from sqlalchemy.exc import NoSuchTableError, OperationalError

from datahub.ingestion.agent.probe_methods import (
    _iter_specs,
    _provider_class,
    run_probe_method,
)
from datahub.ingestion.agent.verdicts import ProbeArgumentError
from datahub.ingestion.source.source_registry import source_registry
from datahub.ingestion.source.sql.sqlalchemy_probe import SqlAlchemyMetadataProbe

_MARKER = "zqx7hostile"

_PAYLOADS = [
    pytest.param(f"{_MARKER}'", id="single-quote"),
    pytest.param(f'{_MARKER}"', id="double-quote"),
    pytest.param(f"x' UNION SELECT '{_MARKER}' --", id="union-select"),
    pytest.param(f"main; SELECT '{_MARKER}'", id="statement-separator"),
    pytest.param(f"main -- {_MARKER}", id="line-comment"),
    pytest.param(f"main /* {_MARKER} */", id="block-comment"),
    pytest.param(f"{_MARKER}%s", id="pyformat-positional"),
    pytest.param(f"{_MARKER}%(x)s", id="pyformat-named"),
    pytest.param(f"{_MARKER}’ OR ‘1’=‘1", id="look-alike-quotes"),
    pytest.param(f"main\x00{_MARKER}", id="nul"),
]

# A legitimate value for every identifier parameter, so each case varies
# exactly one argument.
_LEGIT: Dict[str, str] = {"schema": "main", "table": "orders", "view": "active_orders"}


def _identifier_params(command: str) -> List[str]:
    spec = dict(_iter_specs(SqlAlchemyMetadataProbe))[command]
    return [
        p.name
        for p in spec.params
        if p.type == "str" and p.name != spec.scoped_sql_param
    ]


# Derived from the provider, not hand-listed, so a new command with a string
# argument joins the suite (and trips the guard below) automatically.
_CASES: List[Tuple[str, str]] = [
    (command, param)
    for command, _ in _iter_specs(SqlAlchemyMetadataProbe)
    for param in _identifier_params(command)
]

# A provider that overrides one of these commands and resolves identifiers
# itself, keyed by source type, with the commands it overrides. Each entry is
# a reviewed decision, not an exemption: its resolver must pass only
# catalog-listed strings to reflection.
_RESOLVES_ITS_OWN: Dict[str, FrozenSet[str]] = {
    # Reviewed: every database/schema/table/view reaches the dialect only as
    # the server-listed string mssql_probe._server_spelling returns. It accepts
    # a case-only match (SQL Server's default collation is case-insensitive)
    # where the base resolver refuses one, and warns with the server spelling.
    "mssql": frozenset(
        {
            "columns",
            "foreign_keys",
            "indexes",
            "primary_key",
            "table_comment",
            "tables",
            "view_definition",
            "views",
        }
    ),
}


@pytest.fixture
def engine(tmp_path: pathlib.Path) -> Iterator[Engine]:
    eng = create_engine(f"sqlite:///{tmp_path}/t.db")
    with eng.begin() as c:
        c.exec_driver_sql("CREATE TABLE orders (id INTEGER PRIMARY KEY)")
        c.exec_driver_sql("CREATE VIEW active_orders AS SELECT * FROM orders")
    try:
        yield eng
    finally:
        eng.dispose()


def _record(target: object) -> Tuple[List[str], Callable[..., None]]:
    """Every statement and its bound parameters, as one string each."""
    seen: List[str] = []

    def hook(
        conn: object,
        cursor: object,
        statement: str,
        parameters: object,
        context: object,
        executemany: bool,
    ) -> None:
        seen.append(f"{statement} {parameters!r}")

    event.listen(target, "before_cursor_execute", hook)
    return seen, hook


def test_every_caller_string_is_an_identifier_this_suite_covers() -> None:
    unknown = sorted({(c, p) for c, p in _CASES if p not in _LEGIT})
    assert not unknown, (
        f"{unknown}: a SqlAlchemyMetadataProbe command takes a string this "
        "suite does not know. Resolve it against an Inspector listing in "
        "sqlalchemy_probe.py before any reflection call, then add a "
        "legitimate value for it to _LEGIT."
    )
    assert {c for c, _ in _CASES} >= {
        "tables",
        "views",
        "columns",
        "foreign_keys",
        "primary_key",
        "indexes",
        "table_comment",
        "view_definition",
    }


def test_the_hook_sees_a_payload_that_reaches_reflection(engine: Engine) -> None:
    # Non-vacuity: without the probe's resolver in front, sqlite's reflection
    # puts the marker into executed SQL and the hook records it.
    seen, _ = _record(engine)
    with pytest.raises(OperationalError):
        inspect(engine).get_table_names(schema=f"{_MARKER}'")
    assert any(_MARKER in s for s in seen)

    seen_table, _ = _record(engine)
    with pytest.raises(NoSuchTableError):
        inspect(engine).get_columns(f"{_MARKER}'")
    assert any(_MARKER in s for s in seen_table)


@pytest.mark.parametrize("payload", _PAYLOADS)
@pytest.mark.parametrize(("command", "param"), _CASES)
def test_a_hostile_identifier_is_refused_before_it_reaches_sql(
    engine: Engine, command: str, param: str, payload: str
) -> None:
    probe = SqlAlchemyMetadataProbe(engine)
    seen, _ = _record(engine)
    kwargs: Dict[str, object] = {p: _LEGIT[p] for p in _identifier_params(command)}
    kwargs[param] = payload

    with pytest.raises(ProbeArgumentError):
        getattr(probe, command)(**kwargs)

    assert seen, "nothing was listed, so the check below would prove nothing"
    leaked = [s for s in seen if _MARKER in s]
    assert not leaked, leaked


def test_every_sqlalchemy_provider_inherits_the_resolving_commands() -> None:
    """The fix lives in the shared class, so it covers a source only while the
    source's provider still runs these commands from it. A subclass that
    overrides one must resolve its identifiers the same way and be listed in
    _RESOLVES_ITS_OWN.

    This checks only the base commands, by identity. A subclass's NEW
    identifier-taking `@probe_method` must follow the same rule and is not
    checked automatically."""
    guarded = {c for c, _ in _CASES}
    found: List[str] = []
    load_failures: List[Tuple[str, str]] = []
    unreviewed: Dict[str, List[str]] = {}
    for source_type in sorted(source_registry.mapping):
        try:
            provider = _provider_class(source_type)
        except Exception as e:
            # Usually an optional extra that is not installed. Recorded so a
            # missing core provider is reported with its cause.
            load_failures.append((source_type, type(e).__name__))
            continue
        if not (
            isinstance(provider, type) and issubclass(provider, SqlAlchemyMetadataProbe)
        ):
            continue
        found.append(source_type)
        own = {
            c
            for c in guarded
            if getattr(provider, c) is not getattr(SqlAlchemyMetadataProbe, c)
        }
        rest = own - _RESOLVES_ITS_OWN.get(source_type, frozenset())
        if rest:
            unreviewed[source_type] = sorted(rest)

    core = {"sqlalchemy", "mysql", "postgres", "trino"}
    assert core <= set(found), (
        f"core providers missing from the scan: {sorted(core - set(found))}; "
        f"found {found}; failed to load {load_failures}"
    )
    assert not unreviewed, (
        f"{unreviewed}: these providers override identifier-taking commands. "
        "Only a catalog-listed string may reach reflection: resolve every "
        "schema/table/view through sql_identifier_resolver.resolve_listed_name "
        "or an equivalent that only returns a listed string, then add the "
        "source to _RESOLVES_ITS_OWN after review."
    )


def test_the_framework_path_refuses_a_hostile_schema(
    tmp_path: pathlib.Path,
) -> None:
    db = tmp_path / "t.db"
    seed = create_engine(f"sqlite:///{db}")
    with seed.begin() as c:
        c.exec_driver_sql("CREATE TABLE orders (id INTEGER)")
    seed.dispose()

    # Class-level, because run_probe_method builds its own engine.
    seen, hook = _record(Engine)
    try:
        with pytest.raises(ProbeArgumentError):
            run_probe_method(
                "sqlalchemy",
                {"platform": "sqlite", "connect_uri": f"sqlite:///{db}"},
                "tables",
                {"schema": f"x' UNION SELECT '{_MARKER}' --"},
            )
    finally:
        event.remove(Engine, "before_cursor_execute", hook)
    assert seen
    assert not [s for s in seen if _MARKER in s]
