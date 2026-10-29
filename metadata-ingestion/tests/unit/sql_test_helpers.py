"""Builders shared by the SQL-family unit tests (sql_common and the connectors
built on it: Postgres, MSSQL, Oracle, ClickHouse, Athena, ...).

Not a test module: import from it with ``from tests.unit.sql_test_helpers import ...``.
"""

from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Union

from datahub.ingestion.source.sql.stored_procedures.models import BaseProcedure

Rows = List[Any]
Outcome = Union[Iterable[Any], Exception, Callable[[Optional[Dict[str, Any]]], Any]]


class FakeResult:
    """Just enough of a SQLAlchemy ``Result`` for the reflection and catalog
    reads the SQL sources make: iterable rows, ``fetchone``/``fetchall``/
    ``scalar``, and ``mappings()`` (rows are then expected to be dicts)."""

    def __init__(self, rows: Iterable[Any]) -> None:
        self._rows: Rows = list(rows)

    def __iter__(self):
        return iter(self._rows)

    def fetchall(self) -> Rows:
        return list(self._rows)

    def fetchone(self) -> Optional[Any]:
        return self._rows[0] if self._rows else None

    def scalar(self) -> Optional[Any]:
        first = self.fetchone()
        return first[0] if first is not None else None

    def mappings(self) -> "FakeResult":
        return self

    @property
    def returns_rows(self) -> bool:
        return bool(self._rows)


class FakeConnection:
    """A ``conn.execute`` stand-in that answers by a needle in the statement text.

    ``handlers`` maps a substring of the SQL to its outcome: rows to return, an
    exception to raise, or a callable taking the bound ``params`` and returning
    either. The first matching needle wins, so an empty-string needle acts as a
    catch-all when listed last. Statements nothing matches fail the test, so a
    source cannot silently run a query the test did not anticipate.

    Usable directly where the source takes a connection, as the result of
    ``engine.connect()`` (it is its own context manager), or as the
    ``side_effect`` of a mocked ``execute`` via ``FakeConnection(...).execute``.
    """

    def __init__(self, handlers: Mapping[str, Outcome]) -> None:
        self._handlers = dict(handlers)

    def execute(
        self, statement: Any, params: Optional[Dict[str, Any]] = None
    ) -> FakeResult:
        sql = str(statement)
        for needle, outcome in self._handlers.items():
            if needle in sql:
                if callable(outcome) and not isinstance(outcome, Exception):
                    outcome = outcome(params)
                if isinstance(outcome, Exception):
                    raise outcome
                return FakeResult(outcome)
        raise AssertionError(f"unexpected statement: {sql}")

    def __enter__(self) -> "FakeConnection":
        return self

    def __exit__(self, *args: object) -> None:
        return None


def make_procedure(
    name: str = "proc", definition: Optional[str] = None, **overrides: Any
) -> BaseProcedure:
    """A ``BaseProcedure`` with every optional field cleared unless overridden."""
    fields: Dict[str, Any] = {
        "name": name,
        "procedure_definition": definition,
        "created": None,
        "last_altered": None,
        "comment": None,
        "argument_signature": None,
        "return_type": None,
        "language": "SQL",
        "extra_properties": None,
    }
    fields.update(overrides)
    return BaseProcedure(**fields)
