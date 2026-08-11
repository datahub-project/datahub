from dataclasses import dataclass
from typing import Any, Dict, List, Sequence, TypeVar

from datahub.ingestion.agent.probe_methods import probe_method
from datahub.ingestion.agent.sql_gate import CatalogScope
from datahub.ingestion.agent.sql_query import sql_result

# `__enter__` must hand back the concrete provider, not this base: a caller writing
# `with SqlAlchemyMetadataProbe(...) as probe` otherwise sees only the base's members
# and loses the seven typed getters.
_SelfT = TypeVar("_SelfT", bound="SqlCatalogPassthrough")


@dataclass(frozen=True)
class CatalogRows:
    """One catalog result set as its driver yielded it, before shaping.

    A dataclass rather than a (columns, rows) tuple so a driver adapter cannot
    return them the wrong way round -- the two are both sequences, so the mistake
    would otherwise typecheck and produce a result set with column names for data.
    """

    columns: Sequence[str]
    rows: Sequence[Sequence[Any]]


class SqlCatalogPassthrough:
    """Supplies the `sql` probe command to a connector that is not SQLAlchemy-backed.

    Snowflake, BigQuery and the SQLAlchemy family each had this method, and the only
    real difference between the three was how the driver yields columns and rows --
    a DictCursor, a RowIterator with a schema, a SQLAlchemy CursorResult. Everything
    around that was copied: the declaration, the agent-facing docstring, the
    sql_result shaping, and the fetch-one-past-the-limit convention.

    That last one is why this is worth sharing rather than leaving to each
    connector. `truncated` is computed by comparing the rows returned against the
    limit, so a provider that fetches exactly `limit` rows reports
    `truncated: false` for a result set that was in fact cut short -- an agent then
    concludes it has seen every table in the catalog. The base owns the +1, so a
    driver adapter cannot get it wrong.

    A mixing class declares `sql_dialect` (a name sqlglot resolves -- the gate reads
    it, and refuses the query outright if it is missing), implements
    `execute_catalog_query`, and owns `__exit__`, since what closing means differs:
    dispose a connection pool, close a connection, close a client.
    """

    # What `probe sql` may read here. The default is information_schema only; a
    # dialect whose catalog lives elsewhere declares its own (see CatalogScope).
    catalog_scope: CatalogScope = CatalogScope()

    def execute_catalog_query(self, query: str, limit: int) -> CatalogRows:
        """Run one already-scope-checked query, returning at most `limit` rows.

        `limit` is the framework's clamped limit plus one, so returning everything
        asked for is what lets the caller above detect truncation. Do not re-clamp
        it, and do not fetch the whole result set to slice it afterwards -- on a
        paged API those discarded pages are real requests.
        """
        raise NotImplementedError(
            f"{type(self).__name__} must implement execute_catalog_query to supply "
            f"the `sql` probe command"
        )

    def __enter__(self: _SelfT) -> _SelfT:
        return self

    @probe_method(name="sql", scoped_sql_param="query", row_limit_param="limit")
    def sql(self, query: str, limit: int = 50) -> Dict[str, object]:
        """Run a read-only catalog query. Only a single SELECT over this dialect's
        catalog schemas is permitted -- the framework scope-checks `query` before
        this runs (see probe_methods._enforce_gates), so a user table, a second
        statement, or a vendor function is refused before the source sees it.
        Returns `columns` plus positional `rows`, with `truncated` telling you
        whether more exist beyond `limit`."""
        # One past the limit, so truncation is observed rather than inferred from a
        # full page.
        fetched = self.execute_catalog_query(query, limit + 1)
        return sql_result(fetched.columns, list(fetched.rows), limit)


def rows_from_mappings(
    records: Sequence[Dict[str, Any]],
) -> CatalogRows:
    """Shape a driver that yields dict-per-row (Snowflake's DictCursor).

    Column order comes from the first record rather than being assumed, and every
    later row is read through that same order so a driver that varies key order
    between rows cannot shear the result set.
    """
    if not records:
        return CatalogRows(columns=[], rows=[])
    columns: List[str] = list(records[0].keys())
    return CatalogRows(
        columns=columns,
        rows=[[record.get(column) for column in columns] for record in records],
    )
