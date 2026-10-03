"""SqlAlchemyMetadataProbe hands reflection the catalog's names, never the caller's.

The security half -- hostile input never reaches SQL -- is in
test_sql_identifier_hostile_input.py. This file pins that legitimate input
keeps working: views and materialized views as `table`, a two-tier
normalizer's spelling, names with quotes in them, and one listing per probe.
"""

from pathlib import Path
from typing import Callable, Dict, Iterator, List, Optional, Tuple, cast

import pytest
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine, Inspector
from sqlalchemy.exc import DBAPIError, ProgrammingError

from datahub.ingestion.agent.probe_methods import run_probe_method
from datahub.ingestion.agent.verdicts import ProbeArgumentError
from datahub.ingestion.source.sql.sqlalchemy_probe import SqlAlchemyMetadataProbe


@pytest.fixture
def engine(tmp_path: Path) -> Iterator[Engine]:
    eng = create_engine(f"sqlite:///{tmp_path}/t.db")
    with eng.begin() as c:
        c.exec_driver_sql("CREATE TABLE orders (id INTEGER PRIMARY KEY, note TEXT)")
        c.exec_driver_sql("CREATE VIEW active_orders AS SELECT * FROM orders")
        c.exec_driver_sql('CREATE TABLE "o\'brien orders" (id INTEGER)')
    try:
        yield eng
    finally:
        eng.dispose()


class _CountingInspector:
    """Delegates to a real Inspector and records each call with its arguments."""

    def __init__(self, inner: Inspector) -> None:
        self._inner = inner
        self.calls: List[Tuple[str, Tuple[object, ...], Dict[str, object]]] = []

    def __getattr__(self, attr: str) -> object:
        target = getattr(self._inner, attr)
        if not callable(target):
            return target

        def recorded(*args: object, **kwargs: object) -> object:
            self.calls.append((attr, args, dict(kwargs)))
            return target(*args, **kwargs)

        return recorded

    def count(self, attr: str) -> int:
        return sum(1 for call in self.calls if call[0] == attr)


def _probe(engine: Engine) -> Tuple[SqlAlchemyMetadataProbe, _CountingInspector]:
    probe = SqlAlchemyMetadataProbe(engine)
    spy = _CountingInspector(probe._insp)
    probe._insp = cast(Inspector, spy)
    return probe, spy


def test_listed_names_still_resolve(engine: Engine) -> None:
    probe, _ = _probe(engine)
    assert probe.tables(schema="main") == ["o'brien orders", "orders"]
    assert probe.views(schema="main") == ["active_orders"]
    assert {c["name"] for c in probe.columns(schema="main", table="orders")} == {
        "id",
        "note",
    }
    assert "orders" in (
        probe.view_definition(schema="main", view="active_orders") or ""
    )
    assert probe.primary_key(schema="main", table="orders")["constrained_columns"] == [
        "id"
    ]


def test_a_view_can_be_passed_where_a_table_is_expected(engine: Engine) -> None:
    probe, _ = _probe(engine)
    names = {c["name"] for c in probe.columns(schema="main", table="active_orders")}
    assert names == {"id", "note"}


def test_a_listed_name_with_quotes_and_spaces_is_accepted(engine: Engine) -> None:
    # The defence is "only listed strings reach reflection", not a character
    # blacklist, so a real table with a quote in its name keeps working.
    probe, _ = _probe(engine)
    cols = probe.columns(schema="main", table="o'brien orders")
    assert [c["name"] for c in cols] == ["id"]


def test_reflection_receives_the_catalogs_string_not_the_callers(
    engine: Engine,
) -> None:
    probe, spy = _probe(engine)
    caller_schema = "".join(["ma", "in"])
    caller_table = "".join(["ord", "ers"])
    probe.columns(schema=caller_schema, table=caller_table)
    (reflected,) = [call for call in spy.calls if call[0] == "get_columns"]
    _, args, kwargs = reflected
    assert args[0] == "orders" and args[0] is not caller_table
    assert kwargs["schema"] == "main" and kwargs["schema"] is not caller_schema


def test_listings_are_read_once_per_probe_and_only_as_far_as_needed(
    engine: Engine,
) -> None:
    probe, spy = _probe(engine)
    probe.tables(schema="main")
    probe.tables(schema="main")
    probe.columns(schema="main", table="orders")
    probe.primary_key(schema="main", table="orders")
    assert spy.count("get_schema_names") == 1
    assert spy.count("get_table_names") == 1
    # `orders` was found among the tables, so no other listing was paid for.
    assert spy.count("get_view_names") == 0
    assert spy.count("get_materialized_view_names") == 0


def test_an_unknown_schema_is_refused_before_any_reflection(engine: Engine) -> None:
    probe, spy = _probe(engine)
    with pytest.raises(ProbeArgumentError):
        probe.columns(schema="nope", table="orders")
    assert spy.count("get_columns") == 0
    assert spy.count("get_table_names") == 0


def test_an_unknown_table_is_refused_before_any_reflection(engine: Engine) -> None:
    probe, spy = _probe(engine)
    with pytest.raises(ProbeArgumentError) as info:
        probe.indexes(schema="main", table="shipments")
    assert spy.count("get_indexes") == 0
    assert "tables" in str(info.value)


def test_a_table_is_not_a_view(engine: Engine) -> None:
    probe, _ = _probe(engine)
    with pytest.raises(ProbeArgumentError):
        probe.view_definition(schema="main", view="orders")


def test_a_case_only_mismatch_is_refused_with_the_listed_spelling(
    engine: Engine,
) -> None:
    probe, _ = _probe(engine)
    with pytest.raises(ProbeArgumentError) as schema_info:
        probe.tables(schema="MAIN")
    assert "'main'" in str(schema_info.value)
    with pytest.raises(ProbeArgumentError) as table_info:
        probe.columns(schema="main", table="ORDERS")
    assert "'orders'" in str(table_info.value)


class _TwoTierInspector:
    """A Doris-style external catalog: the server lists `cat.<db>`."""

    def __init__(self) -> None:
        self.table_schemas: List[str] = []

    def get_schema_names(self) -> List[str]:
        return ["cat.sales", "cat.ops"]

    def get_table_names(self, schema: str) -> List[str]:
        self.table_schemas.append(schema)
        return ["orders"]


def _strip_catalog(name: str) -> str:
    return name[len("cat.") :] if name.startswith("cat.") else name


def test_a_normalized_container_spelling_resolves_to_a_server_string() -> None:
    fake = _TwoTierInspector()
    probe = SqlAlchemyMetadataProbe.__new__(SqlAlchemyMetadataProbe)
    probe._insp = cast(Inspector, fake)
    probe.container_kind = "Database"
    normalizer: Callable[[str], str] = _strip_catalog
    probe.container_normalizer = normalizer

    # What `containers` prints is what a caller passes back.
    assert probe.containers() == ["sales", "ops"]
    caller = "".join(["sa", "les"])
    assert probe.tables(schema=caller) == ["orders"]
    assert fake.table_schemas == ["sales"]
    assert fake.table_schemas[0] is not caller
    # The raw server spelling is accepted too, as it was before.
    assert probe.tables(schema="cat.ops") == ["orders"]
    with pytest.raises(ProbeArgumentError) as info:
        probe.tables(schema="hr")
    assert "database" in str(info.value)


class _MatviewInspector:
    def __init__(self, has_matviews: bool) -> None:
        self._has_matviews = has_matviews
        self.reflected: List[Tuple[str, str]] = []

    def get_schema_names(self) -> List[str]:
        return ["public"]

    def get_table_names(self, schema: str) -> List[str]:
        return ["orders"]

    def get_view_names(self, schema: str) -> List[str]:
        return []

    def get_materialized_view_names(self, schema: str) -> List[str]:
        if not self._has_matviews:
            # The base Dialect's default.
            raise NotImplementedError()
        return ["daily_totals"]

    def get_columns(self, table: str, schema: str) -> List[Dict[str, object]]:
        self.reflected.append((schema, table))
        return [{"name": "d", "type": "DATE", "nullable": True, "default": None}]


def _matview_probe(
    has_matviews: bool,
) -> Tuple[SqlAlchemyMetadataProbe, _MatviewInspector]:
    fake = _MatviewInspector(has_matviews)
    probe = SqlAlchemyMetadataProbe.__new__(SqlAlchemyMetadataProbe)
    probe._insp = cast(Inspector, fake)
    return probe, fake


def test_a_materialized_view_resolves_as_a_table() -> None:
    probe, fake = _matview_probe(has_matviews=True)
    assert [
        c["name"] for c in probe.columns(schema="public", table="daily_totals")
    ] == ["d"]
    assert fake.reflected == [("public", "daily_totals")]


def test_a_dialect_without_materialized_views_still_refuses_cleanly() -> None:
    probe, fake = _matview_probe(has_matviews=False)
    with pytest.raises(ProbeArgumentError):
        probe.columns(schema="public", table="daily_totals")
    assert fake.reflected == []


def test_a_dialect_that_cannot_list_schemas_exits_on_the_unsupported_code(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Resolving a schema now lists schemas first, so a dialect without
    get_schema_names fails there. It must still reach run_probe_method's
    "does not support" branch (exit 2) rather than escape as a foreign error
    the CLI reports as an unreachable source (exit 3)."""
    db = tmp_path / "t.db"
    seed = create_engine(f"sqlite:///{db}")
    with seed.begin() as c:
        c.exec_driver_sql("CREATE TABLE orders (id INTEGER)")
    seed.dispose()

    reflected: List[str] = []

    def no_schemas(self: Inspector) -> List[str]:
        raise NotImplementedError()

    def table_names(self: Inspector, schema: str) -> List[str]:
        reflected.append(schema)
        return []

    monkeypatch.setattr(Inspector, "get_schema_names", no_schemas)
    monkeypatch.setattr(Inspector, "get_table_names", table_names)

    with pytest.raises(ProbeArgumentError) as info:
        run_probe_method(
            "sqlalchemy",
            {"platform": "sqlite", "connect_uri": f"sqlite:///{db}"},
            "tables",
            {"schema": "main"},
        )
    assert "does not support the 'tables' command" in str(info.value)
    assert reflected == []


class _FallbackInspector(_MatviewInspector):
    """Primary listings answer; the named listings fail the way a dialect's can."""

    def __init__(
        self,
        *,
        matviews: Optional[Exception] = None,
        views: Optional[Exception] = None,
        tables: Optional[Exception] = None,
    ) -> None:
        super().__init__(has_matviews=True)
        self._matviews, self._views, self._tables = matviews, views, tables

    def get_table_names(self, schema: str) -> List[str]:
        if self._tables:
            raise self._tables
        return ["orders"]

    def get_view_names(self, schema: str) -> List[str]:
        if self._views:
            raise self._views
        return []

    def get_materialized_view_names(self, schema: str) -> List[str]:
        if self._matviews:
            raise self._matviews
        return []


def _fallback_probe(inspector: _FallbackInspector) -> SqlAlchemyMetadataProbe:
    probe = SqlAlchemyMetadataProbe.__new__(SqlAlchemyMetadataProbe)
    probe._insp = cast(Inspector, inspector)
    return probe


def _db_error() -> DBAPIError:
    return ProgrammingError("SELECT 1", {}, Exception("column does not exist"))


def test_a_failing_materialized_view_listing_still_refuses_a_typo() -> None:
    fake = _FallbackInspector(matviews=_db_error())
    with pytest.raises(ProbeArgumentError) as info:
        _fallback_probe(fake).columns(schema="public", table="ordrs")
    assert "tables" in str(info.value)
    assert fake.reflected == []


def test_an_unimplemented_view_fallback_still_refuses_a_typo() -> None:
    fake = _FallbackInspector(views=NotImplementedError())
    with pytest.raises(ProbeArgumentError):
        _fallback_probe(fake).columns(schema="public", table="ordrs")
    assert fake.reflected == []


def test_a_failing_fallback_does_not_hide_a_primary_hit() -> None:
    fake = _FallbackInspector(matviews=_db_error())
    assert _fallback_probe(fake).columns(schema="public", table="orders")


def test_a_failing_primary_table_listing_still_propagates() -> None:
    fake = _FallbackInspector(tables=_db_error())
    with pytest.raises(DBAPIError):
        _fallback_probe(fake).columns(schema="public", table="orders")


@pytest.mark.parametrize(
    "inspector, unread",
    [
        (_FallbackInspector(matviews=_db_error()), "materialized-view"),
        (_FallbackInspector(views=_db_error()), "view"),
    ],
)
def test_a_refusal_says_which_fallback_listing_could_not_be_read(
    inspector: _FallbackInspector, unread: str
) -> None:
    """Still exit 2, so a typo is refused, but the caller learns the name
    may be one this connection cannot list."""
    with pytest.raises(ProbeArgumentError) as info:
        _fallback_probe(inspector).columns(schema="public", table="ordrs")
    message = str(info.value)
    assert (
        f"the {unread} listing could not be read (ProgrammingError); if it is "
        f"one, this connection cannot resolve it"
    ) in message
    assert "column does not exist" not in message


def test_a_dialect_without_the_fallback_listing_adds_no_such_note() -> None:
    fake = _FallbackInspector(views=NotImplementedError())
    with pytest.raises(ProbeArgumentError) as info:
        _fallback_probe(fake).columns(schema="public", table="ordrs")
    assert "could not be read" not in str(info.value)
