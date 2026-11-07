from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence

from datahub.ingestion.agent.probe_methods import clamp_item_limit, probe_method
from datahub.ingestion.agent.provider_helpers import ProbeProviderBase
from datahub.ingestion.agent.redact import mask_identity_columns
from datahub.ingestion.agent.verdicts import ProbeArgumentError, ProbeInternalError

_JSON_SAFE_TYPES = (str, int, float, bool)

# What a probe calls itself to the server, so a slow-query log or a bill tells
# an agent looking around from ingestion, whose credentials and client it uses.
# The charset [a-z0-9_-] (63 max) meets the strictest label rule and keeps the
# string safe to interpolate where no bind parameter exists;
# test_query_attribution.py pins both.
PROBE_QUERY_LABEL = "datahub_recipe_probe"


@dataclass(frozen=True)
class CatalogRows:
    """One catalog result set as its driver yielded it, before shaping. Named
    fields, since two swapped sequences would still typecheck."""

    columns: Sequence[str]
    rows: Sequence[Sequence[Any]]


@dataclass(frozen=True)
class QueryBudget:
    """What one probe query may spend. Unlike MAX_PROBE_ITEMS (rows returned),
    this is cost: only a ceiling asked of the server stops a warehouse running
    and billing a query. Bounded by default, so an author who never thinks
    about cost is safe.
    """

    # Wall-clock ceiling asked of the server; None where the driver cannot ask.
    timeout_seconds: Optional[int] = 30

    # Byte ceiling for warehouses that bill by bytes scanned: the job is refused
    # before it runs, so this bounds spend rather than duration.
    max_bytes_billed: Optional[int] = None

    def __post_init__(self) -> None:
        # None is the one way to say unbounded; a non-positive number would be
        # described as a ceiling nothing enforces.
        for name in ("timeout_seconds", "max_bytes_billed"):
            value = getattr(self, name)
            if value is not None and value <= 0:
                raise ProbeArgumentError(
                    f"QueryBudget.{name} must be positive or None; got {value!r}. "
                    f"None means no ceiling -- a non-positive number would be "
                    f"reported as a ceiling that nothing enforces"
                )

    def describe(self) -> str:
        """What to tell a caller about the ceiling that actually applies."""
        parts = []
        if self.timeout_seconds is not None:
            parts.append(f"{self.timeout_seconds}s")
        if self.max_bytes_billed is not None:
            parts.append(f"{self.max_bytes_billed} bytes scanned")
        return ", ".join(parts) or "no server-side ceiling"


class SqlCatalogPassthrough(ProbeProviderBase):
    """Supplies the `sql` probe command to a provider that speaks SQL.

    The base owns the fetch-one-past-the-limit convention: `truncated` compares
    rows returned against the limit, so an adapter fetching exactly `limit`
    would report a cut-short result as complete. A subclass declares
    `sql_dialect` (the gate refuses a query without one) and `catalog_scope`
    (both on ProbeProviderBase; a provider of its own sets the scope on its
    class, the SQLAlchemy provider from the config's probe_catalog_scope),
    and implements `execute_catalog_query`.
    """

    # What one query may spend. The provider applies it, the mechanism being
    # per driver (the SQLAlchemy family through probe_engine_settings).
    query_budget: QueryBudget = QueryBudget()

    def execute_catalog_query(self, query: str, limit: int) -> CatalogRows:
        """Run one scope-checked query, returning at most `limit` rows. `limit`
        already includes the +1: do not re-clamp it, and do not fetch the whole
        result to slice it (a paged API's discarded pages are real requests)."""
        # Not NotImplementedError, which means a command the source lacks
        # (exit 2): a provider without its adapter is a defect (exit 1).
        raise ProbeInternalError(
            f"{type(self).__name__} must implement execute_catalog_query to supply "
            f"the `sql` probe command; this is a defect in the probe provider"
        )

    @probe_method(
        name="sql",
        scoped_sql_param="query",
        row_limit_param="limit",
        # Returns its own envelope and adds its own +1.
        shapes_own_result=True,
    )
    def sql(self, query: str, limit: int = 50) -> Dict[str, object]:
        """Run a read-only catalog query. Only a single SELECT over this dialect's
        catalog schemas is permitted -- the framework scope-checks `query` before
        this runs (see probe_methods._enforce_gates), so a user table, a second
        statement, or a vendor function is refused before the source sees it.
        Returns `columns` plus positional `rows`, with `truncated` telling you
        whether more exist beyond `limit`."""
        if self.query_budget.timeout_seconds is None and (
            self.query_budget.max_bytes_billed is None
        ):
            # Not "no ceiling": MySQL and MariaDB get a best-effort one the
            # budget does not claim, since it may not be in force.
            self._warn(
                "no time limit is guaranteed for `sql` on this connection, so "
                "a slow catalog query may run until the server finishes it"
            )
        # One past the limit, so truncation is observed.
        fetched = self.execute_catalog_query(query, limit + 1)
        return sql_result(fetched.columns, list(fetched.rows), limit)


def rows_from_mappings(
    records: Sequence[Dict[str, Any]],
) -> CatalogRows:
    """Shape a driver's dict-per-row result. Column order is the first record's,
    applied to every row, so varying key order cannot shear the result."""
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
    """Shape one catalog result set: positional rows under one column list, so
    a wide result does not repeat names per row. Callers fetch one row past
    `limit`. Every provider's result passes through here, so the limit is
    clamped and identity columns masked here, where none can route around it.
    """
    limit = clamp_item_limit(limit)
    safe_rows = mask_identity_columns(columns, rows[:limit])
    kept: List[List[object]] = [
        [_json_safe(value) for value in row] for row in safe_rows
    ]
    return {
        "columns": list(columns),
        "rows": kept,
        "row_count": len(kept),
        "truncated": len(rows) > limit,
    }
