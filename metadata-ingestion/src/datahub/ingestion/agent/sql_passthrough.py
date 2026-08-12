from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence, TypeVar

from datahub.ingestion.agent.probe_methods import clamp_item_limit, probe_method
from datahub.ingestion.agent.sql_gate import CatalogScope

# `__enter__` must hand back the concrete provider, not this base: a caller writing
# `with SqlAlchemyMetadataProbe(...) as probe` otherwise sees only the base's members
# and loses the seven typed getters.
_JSON_SAFE_TYPES = (str, int, float, bool)

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


@dataclass(frozen=True)
class QueryBudget:
    """What one probe query may spend, declared per connector.

    Distinct from MAX_PROBE_ITEMS, which bounds the rows a query *returns*. That
    is not a cost ceiling and was never doing that job: a query can return three
    rows and still scan a terabyte, and a warehouse that bills by bytes scanned
    charges the same either way.

    Declared per connector because only the connector knows which of these its
    driver can ask for, and asking the server is the only kind that counts --
    abandoning a cursor client-side stops us waiting for the query, not the
    warehouse running and billing it.

    The default is deliberately bounded rather than None. A connector author who
    never thinks about cost is the common case, and the framework's job is to
    make that case safe rather than to rely on them remembering.
    """

    # Wall-clock ceiling asked of the server. None where the driver has no way to
    # ask, which is stated rather than implied -- see describe().
    timeout_seconds: Optional[int] = 30

    # Hard byte ceiling, for warehouses that bill by bytes scanned. The job is
    # refused before it runs rather than killed partway through, so this is the
    # only one of the two that bounds spend rather than duration.
    max_bytes_billed: Optional[int] = None

    def describe(self) -> str:
        """What to tell a caller about the ceiling that actually applies."""
        parts = []
        if self.timeout_seconds is not None:
            parts.append(f"{self.timeout_seconds}s")
        if self.max_bytes_billed is not None:
            parts.append(f"{self.max_bytes_billed} bytes scanned")
        return ", ".join(parts) or "no server-side ceiling"


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

    # What one query here may spend. Applying it is the provider's job, since the
    # mechanism is per-driver; declaring it here is what makes a provider that
    # never thought about it still bounded (see QueryBudget).
    query_budget: QueryBudget = QueryBudget()

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


def _json_safe(value: object) -> object:
    # Catalog reads return dates, decimals, UUIDs and (on some drivers) bytes.
    # Coercing here keeps every caller free of a custom JSON encoder.
    if value is None or isinstance(value, _JSON_SAFE_TYPES):
        return value
    if isinstance(value, (bytes, bytearray)):
        return bytes(value).decode("utf-8", errors="replace")
    return str(value)


def sql_result(
    columns: Sequence[str], rows: Sequence[Sequence[Any]], limit: int
) -> Dict[str, object]:
    """Shape one catalog result set for probe output.

    Rows are positional with a separate column list, so a wide result does not
    repeat every column name on every row -- probe output is read by an agent
    with a finite context window.

    Callers fetch one row beyond `limit` so truncation is observed rather than
    inferred from a full page.

    Clamps `limit` again even though the framework already bounded the fetch
    (probe_methods._bounded_kwargs): this is the last thing every provider's
    result passes through, so it is where a provider that builds rows some other
    way still cannot emit more than MAX_PROBE_ITEMS.
    """
    limit = clamp_item_limit(limit)
    kept: List[List[object]] = [
        [_json_safe(value) for value in row] for row in rows[:limit]
    ]
    return {
        "columns": list(columns),
        "rows": kept,
        "row_count": len(kept),
        "truncated": len(rows) > limit,
    }
