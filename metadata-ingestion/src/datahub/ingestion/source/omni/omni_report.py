import threading
from dataclasses import dataclass, field
from typing import Dict

from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalSourceReport,
)
from datahub.utilities.lossy_collections import LossyList


@dataclass
class _CallMetric:
    """Per-method API call count and cumulative time."""

    calls: int = 0
    time_seconds: float = 0.0


@dataclass
class OmniClientReport:
    """API client metrics tracking call counts and timing per method."""

    metrics: Dict[str, _CallMetric] = field(default_factory=dict)

    def __post_init__(self) -> None:
        self._metrics_lock = threading.Lock()

    def record_call(self, method_name: str, duration_seconds: float) -> None:
        """Thread-safe recording of API call metrics."""
        with self._metrics_lock:
            m = self.metrics.get(method_name)
            if m is None:
                m = _CallMetric()
                self.metrics[method_name] = m
            m.calls += 1
            m.time_seconds += duration_seconds


@dataclass
class OmniSourceReport(StaleEntityRemovalSourceReport):
    """Ingestion report for the Omni BI platform source."""

    filtered_models: LossyList[str] = field(default_factory=LossyList)
    filtered_documents: LossyList[str] = field(default_factory=LossyList)

    # Scanned = discovered from API, emitted = produced as DataHub entities
    connections_scanned: int = 0
    connections_emitted: int = 0
    models_scanned: int = 0
    models_emitted: int = 0
    topics_scanned: int = 0
    topics_emitted: int = 0
    views_emitted: int = 0
    documents_scanned: int = 0
    dashboards_emitted: int = 0
    charts_emitted: int = 0

    # Lineage metrics
    dataset_lineage_edges_emitted: int = 0
    view_to_physical_column_lineage_edges: int = 0
    view_to_physical_column_lineage_skipped_computed: int = 0
    fine_grained_lineage_edges_exact: int = 0
    fine_grained_lineage_edges_derived: int = 0
    fine_grained_lineage_edges_unresolved: int = 0
    client_report: OmniClientReport = field(default_factory=OmniClientReport)

    def __post_init__(self) -> None:
        super().__post_init__()  # type: ignore
        # Lock for thread-safe counter updates
        self._report_lock = threading.Lock()
        # Ensure client_report is initialized
        if not isinstance(self.client_report, OmniClientReport):
            self.client_report = OmniClientReport()

    def increment_counter(self, name: str, delta: int = 1) -> None:
        """Thread-safe counter increment.

        Args:
            name: Counter attribute name (e.g., "models_scanned")
            delta: Amount to increment by (default: 1)
        """
        with self._report_lock:
            current = getattr(self, name)
            setattr(self, name, current + delta)

    def report_model_filtered(self, model_id: str) -> None:
        self.filtered_models.append(model_id)

    def report_document_filtered(self, doc_id: str) -> None:
        self.filtered_documents.append(doc_id)
