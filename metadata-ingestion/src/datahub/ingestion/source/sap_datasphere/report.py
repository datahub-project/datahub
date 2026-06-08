import heapq
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalSourceReport,
)
from datahub.utilities.lossy_collections import LossyList

# Number of slowest individual API calls to retain for outlier spotting. Kept as
# a module-level constant (not a dataclass field) so it never leaks into the
# report's ``as_obj`` serialization regardless of underscore-prefix rules.
_SLOWEST_N = 10


@dataclass
class ApiCallStats:
    """Aggregated timing for one logical API operation (e.g. ``csn_fetch``).

    Memory is O(1) per operation — only running aggregates are kept, never
    per-call samples.
    """

    count: int = 0
    total_seconds: float = 0.0
    max_seconds: float = 0.0

    @property
    def avg_seconds(self) -> float:
        return self.total_seconds / self.count if self.count else 0.0

    def __repr__(self) -> str:
        return (
            f"count={self.count} total={self.total_seconds:.2f}s "
            f"avg={self.avg_seconds * 1000:.1f}ms max={self.max_seconds * 1000:.1f}ms"
        )


@dataclass
class SapDatasphereReport(StaleEntityRemovalSourceReport):
    spaces_scanned: int = 0
    spaces_filtered: int = 0
    assets_scanned: int = 0
    assets_filtered: int = 0
    assets_schema_fetched: int = 0
    # PR-1: Local Tables emitted via the supported /dwaas-core/ discovery
    # endpoint (gated on `include_local_tables`).
    local_tables_emitted: int = 0
    # M4: columns dropped from emitted schemas via `column_pattern`.
    columns_filtered: int = 0
    assets_schema_failed: LossyList[str] = field(default_factory=LossyList)
    # Tagged skip-reason buckets (replaces the older, ambiguous
    # `assets_skipped_unknown_platform`).
    assets_skipped_unknown_typeid: LossyList[str] = field(default_factory=LossyList)
    assets_skipped_unknown_connection: LossyList[str] = field(default_factory=LossyList)
    assets_skipped_disabled: LossyList[str] = field(default_factory=LossyList)
    # Surfaces from EDMX parsing and CSN fetch failures.
    assets_with_unknown_edm_types: LossyList[str] = field(default_factory=LossyList)
    assets_csn_fetch_failed: LossyList[str] = field(default_factory=LossyList)
    column_lineage_unresolved: LossyList[str] = field(default_factory=LossyList)
    """Per-asset records of column-lineage refs that could not be resolved to a known
    upstream column (e.g., `S1.VIEW.col1 -> MISSING_TABLE.x`). LossyList caps the
    entries so it doesn't grow unbounded at 1M-entity scale."""

    # Per-endpoint API timing aggregates, keyed by logical operation
    # (oauth_token / catalog_list / connections / csn_fetch / edmx_fetch).
    # Memory is O(#operations) — a fixed, tiny set of keys.
    api_timings: Dict[str, ApiCallStats] = field(default_factory=dict)
    # The N slowest individual API calls seen, for outlier spotting. Bounded
    # min-heap (seconds, operation, url); underscore-prefixed so it is excluded
    # from ``as_obj`` serialization.
    _slowest_api_calls_heap: List[Tuple[float, str, str]] = field(default_factory=list)
    # Human-readable view of the slowest calls, rebuilt on each record (<=N items
    # so the rebuild is cheap). This one IS serialized into the report.
    slowest_api_calls: List[str] = field(default_factory=list)

    def report_api_call(
        self, operation: str, seconds: float, url: Optional[str] = None
    ) -> None:
        """Record one outbound API call's latency under ``operation``.

        Hot path (up to ~millions of calls): a dict lookup/update plus a bounded
        heap op. Memory stays O(#operations + N).
        """
        stats = self.api_timings.get(operation)
        if stats is None:
            stats = ApiCallStats()
            self.api_timings[operation] = stats
        stats.count += 1
        stats.total_seconds += seconds
        if seconds > stats.max_seconds:
            stats.max_seconds = seconds
        # Maintain a bounded min-heap of the N slowest calls.
        entry = (seconds, operation, url or "")
        if len(self._slowest_api_calls_heap) < _SLOWEST_N:
            heapq.heappush(self._slowest_api_calls_heap, entry)
        elif seconds > self._slowest_api_calls_heap[0][0]:
            heapq.heapreplace(self._slowest_api_calls_heap, entry)
        # Rebuild the human-readable view (cheap; <=N items).
        self.slowest_api_calls = [
            f"{op}: {s * 1000:.0f}ms {u}".rstrip()
            for s, op, u in sorted(self._slowest_api_calls_heap, reverse=True)
        ]
