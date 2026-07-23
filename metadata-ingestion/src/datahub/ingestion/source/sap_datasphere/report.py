import heapq
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

from datahub.ingestion.source.sap_datasphere.constants import (
    SLOWEST_API_CALLS_RETAINED,
)
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalSourceReport,
)
from datahub.utilities.lossy_collections import LossyList


@dataclass
class ApiCallStats:
    # O(1) per operation — only running aggregates are kept, never per-call samples.
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
    # Assets whose schema came from the CSN elements map because no OData/EDMX
    # metadata URL was available (e.g. analytic models).
    assets_schema_from_csn: int = 0
    local_tables_emitted: int = 0
    columns_filtered: int = 0
    assets_schema_failed: LossyList[str] = field(default_factory=LossyList)
    assets_skipped_unknown_typeid: LossyList[str] = field(default_factory=LossyList)
    assets_skipped_unknown_connection: LossyList[str] = field(default_factory=LossyList)
    assets_skipped_disabled: LossyList[str] = field(default_factory=LossyList)
    assets_with_unknown_edm_types: LossyList[str] = field(default_factory=LossyList)
    assets_with_unknown_cds_types: LossyList[str] = field(default_factory=LossyList)
    assets_csn_fetch_failed: LossyList[str] = field(default_factory=LossyList)
    # Non-empty means the supportsAnalyticalQueries routing heuristic was wrong
    # for those assets but the sibling-type fallback recovered them.
    assets_csn_object_type_corrected: LossyList[str] = field(default_factory=LossyList)
    # HTTP 200 but an unexpected shape, so no schema could be parsed —
    # distinguishes a parse miss from a genuine no-schema base table.
    assets_csn_unparseable: LossyList[str] = field(default_factory=LossyList)
    column_lineage_unresolved: LossyList[str] = field(default_factory=LossyList)

    # Flow-based lineage (data / replication / transformation flows, task chains)
    # emitted as DataJobs under a per-space DataFlow.
    data_flows_scanned: int = 0
    data_flows_emitted: int = 0
    replication_flows_scanned: int = 0
    replication_flows_emitted: int = 0
    transformation_flows_scanned: int = 0
    transformation_flows_emitted: int = 0
    task_chains_scanned: int = 0
    task_chains_emitted: int = 0
    # A flow whose definition fetch failed, or which parsed to no IO edges.
    flows_fetch_failed: LossyList[str] = field(default_factory=LossyList)
    flows_unparseable: LossyList[str] = field(default_factory=LossyList)
    # A flow endpoint (source/target object) whose connection could not be mapped
    # to a DataHub platform, so its lineage edge was skipped.
    flow_endpoints_unresolved: LossyList[str] = field(default_factory=LossyList)

    # Federated Remote Tables and their external upstream lineage.
    remote_tables_scanned: int = 0
    remote_tables_emitted: int = 0
    # A remote table whose @DataWarehouse.remote connection could not be mapped.
    remote_table_source_unresolved: LossyList[str] = field(default_factory=LossyList)

    # Per-operation timing aggregates, keyed by logical operation
    # (oauth_token / catalog_list / connections / csn_fetch / edmx_fetch).
    api_timings: Dict[str, ApiCallStats] = field(default_factory=dict)
    # Bounded min-heap of the N slowest (seconds, operation, url); underscore
    # prefix keeps it out of as_obj serialization.
    _slowest_api_calls_heap: List[Tuple[float, str, str]] = field(default_factory=list)
    # Human-readable view of the slowest calls, rebuilt on each record; serialized.
    slowest_api_calls: List[str] = field(default_factory=list)

    def report_api_call(
        self, operation: str, seconds: float, url: Optional[str] = None
    ) -> None:
        stats = self.api_timings.get(operation)
        if stats is None:
            stats = ApiCallStats()
            self.api_timings[operation] = stats
        stats.count += 1
        stats.total_seconds += seconds
        if seconds > stats.max_seconds:
            stats.max_seconds = seconds
        entry = (seconds, operation, url or "")
        if len(self._slowest_api_calls_heap) < SLOWEST_API_CALLS_RETAINED:
            heapq.heappush(self._slowest_api_calls_heap, entry)
        elif seconds > self._slowest_api_calls_heap[0][0]:
            heapq.heapreplace(self._slowest_api_calls_heap, entry)
        self.slowest_api_calls = [
            f"{op}: {s * 1000:.0f}ms {u}".rstrip()
            for s, op, u in sorted(self._slowest_api_calls_heap, reverse=True)
        ]
