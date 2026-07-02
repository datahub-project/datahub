import threading
from dataclasses import dataclass, field

from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalSourceReport,
)
from datahub.utilities.lossy_collections import LossyList


@dataclass
class OmniClientReport:
    """API client metrics tracking call counts and timing."""

    # Call counts per method
    test_connection_calls: int = 0
    list_connections_calls: int = 0
    list_models_calls: int = 0
    get_model_yaml_calls: int = 0
    get_topic_calls: int = 0
    list_documents_calls: int = 0
    get_dashboard_document_calls: int = 0
    get_document_queries_calls: int = 0
    list_folders_calls: int = 0

    # Accumulated call time (seconds) per method
    test_connection_time_seconds: float = 0.0
    list_connections_time_seconds: float = 0.0
    list_models_time_seconds: float = 0.0
    get_model_yaml_time_seconds: float = 0.0
    get_topic_time_seconds: float = 0.0
    list_documents_time_seconds: float = 0.0
    get_dashboard_document_time_seconds: float = 0.0
    get_document_queries_time_seconds: float = 0.0
    list_folders_time_seconds: float = 0.0

    def __post_init__(self) -> None:
        # Lock for thread-safe metric updates
        self._metrics_lock = threading.Lock()

    def record_call(self, method_name: str, duration_seconds: float) -> None:
        """Thread-safe recording of API call metrics.

        Args:
            method_name: Name of the API method (e.g., "get_topic")
            duration_seconds: How long the call took
        """
        with self._metrics_lock:
            # Increment call count
            call_count_attr = f"{method_name}_calls"
            current_count = getattr(self, call_count_attr, 0)
            setattr(self, call_count_attr, current_count + 1)

            # Accumulate time
            time_attr = f"{method_name}_time_seconds"
            current_time = getattr(self, time_attr, 0.0)
            setattr(self, time_attr, current_time + duration_seconds)


@dataclass
class OmniSourceReport(StaleEntityRemovalSourceReport):
    """Ingestion report for the Omni BI platform source."""

    filtered: LossyList[str] = field(default_factory=LossyList)
    connections_scanned: int = 0
    models_scanned: int = 0
    topics_scanned: int = 0
    documents_scanned: int = 0
    dashboards_scanned: int = 0
    semantic_datasets_emitted: int = 0
    dataset_lineage_edges_emitted: int = 0
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

    def report_dropped(self, name: str) -> None:
        self.filtered.append(name)
