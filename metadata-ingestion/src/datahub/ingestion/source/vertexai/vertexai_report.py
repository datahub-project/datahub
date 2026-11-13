import threading
from dataclasses import dataclass, field
from typing import Dict, List

from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalSourceReport,
)

# Phase 1 reads run in worker threads, so counters shared with them take a lock.
_COUNTER_LOCK = threading.Lock()


@dataclass
class VertexAISourceReport(StaleEntityRemovalSourceReport):
    urns_carried_forward: int = 0
    items_kept_after_stop: int = 0
    # Every lost read by project, in full: warnings and failures keep only samples.
    lost_reads: Dict[str, List[str]] = field(default_factory=dict)

    def report_urns_carried_forward(self, count: int) -> None:
        with _COUNTER_LOCK:
            self.urns_carried_forward += count

    def report_item_kept_after_stop(self) -> None:
        with _COUNTER_LOCK:
            self.items_kept_after_stop += 1

    def report_lost_read(self, project: str, context: str, temporary: bool) -> None:
        loss = "temporary" if temporary else "lasting"
        self.lost_reads.setdefault(project, []).append(f"{context}, loss={loss}")
