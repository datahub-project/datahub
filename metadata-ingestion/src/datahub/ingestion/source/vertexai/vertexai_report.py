import threading
from dataclasses import dataclass, field
from typing import Dict, List

from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalSourceReport,
)
from datahub.ingestion.source.vertexai.vertexai_constants import (
    PIPELINE_LINKS_ELSEWHERE,
)

# Phase 1 reads run in worker threads, so counters shared with them take a lock.
_COUNTER_LOCK = threading.Lock()


@dataclass
class VertexAISourceReport(StaleEntityRemovalSourceReport):
    urns_carried_forward: int = 0
    items_kept_after_stop: int = 0
    pipeline_task_details_without_step: int = 0
    pipeline_task_details_extra_roots: int = 0
    runs_skipped_without_time: int = 0
    pipeline_nested_steps_written: int = 0
    pipeline_loop_passes: int = 0
    pipeline_task_runs_skipped_not_started: int = 0
    pipeline_lineage_patches_sent: int = 0
    pipeline_lineage_links_sent: int = 0
    # Every lost read by project, in full: warnings and failures keep only samples.
    lost_reads: Dict[str, List[str]] = field(default_factory=dict)

    def report_urns_carried_forward(self, count: int) -> None:
        with _COUNTER_LOCK:
            self.urns_carried_forward += count

    def report_item_kept_after_stop(self) -> None:
        with _COUNTER_LOCK:
            self.items_kept_after_stop += 1

    def report_pipeline_task_details_without_step(self, count: int) -> None:
        with _COUNTER_LOCK:
            self.pipeline_task_details_without_step += count

    def report_pipeline_task_details_extra_roots(self, count: int) -> None:
        with _COUNTER_LOCK:
            self.pipeline_task_details_extra_roots += count

    def report_run_skipped_without_time(self) -> None:
        with _COUNTER_LOCK:
            self.runs_skipped_without_time += 1

    def report_pipeline_nested_step_written(self) -> None:
        with _COUNTER_LOCK:
            self.pipeline_nested_steps_written += 1

    def report_pipeline_loop_passes(self, count: int) -> None:
        with _COUNTER_LOCK:
            self.pipeline_loop_passes += count

    def report_pipeline_task_runs_skipped_not_started(self, count: int) -> None:
        with _COUNTER_LOCK:
            self.pipeline_task_runs_skipped_not_started += count

    def report_pipeline_lineage_sent(self, patches: int, links: int) -> None:
        with _COUNTER_LOCK:
            self.pipeline_lineage_patches_sent += patches
            self.pipeline_lineage_links_sent += links

    def report_lost_read(self, project: str, context: str, temporary: bool) -> None:
        loss = "temporary" if temporary else "lasting"
        self.lost_reads.setdefault(project, []).append(f"{context}, loss={loss}")

    def report_pipeline_links_elsewhere(self, context: str) -> None:
        # Its own entry, so the note is never lost among another entry's samples.
        self.warning(
            title="Models elsewhere may lose pipeline links",
            message=PIPELINE_LINKS_ELSEWHERE,
            context=context,
        )
