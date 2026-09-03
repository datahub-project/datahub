from dataclasses import dataclass, field
from typing import Optional

from typing_extensions import LiteralString

from datahub.ingestion.api.source import StructuredLogCategory
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalSourceReport,
)
from datahub.utilities.lossy_collections import LossyList


@dataclass
class MonteCarloSourceReport(StaleEntityRemovalSourceReport):
    monitors_scanned: int = 0
    custom_rules_scanned: int = 0
    assertions_emitted: int = 0
    alerts_scanned: int = 0
    run_events_emitted: int = 0
    job_executions_scanned: int = 0
    metric_points_fetched: int = 0

    # Monitors/custom rules dropped by an intentional name/type pattern filter,
    # distinct from per-item build failures. Used by the zero-assertion guard so
    # a deny-all pattern (scanned > 0, emitted == 0, all dropped) is not flagged.
    dropped: int = 0

    mcons_resolved: int = 0
    mcons_resolution_failed: int = 0
    build_failures: int = 0
    # Operator-visible skip counters. The inherited `warnings` / `filtered`
    # LossyLists cap stored samples; these ints give an exact total so the
    # ingestion report can show how much was skipped without the LossyList cap.
    warnings_count: int = 0
    mcons_unmapped_platform: LossyList[str] = field(default_factory=LossyList)
    filtered: LossyList[str] = field(default_factory=LossyList)

    def report_dropped(self, name: str) -> None:
        self.dropped += 1
        self.filtered.append(name)

    def report_monitor_scanned(self) -> None:
        self.monitors_scanned += 1

    def report_custom_rule_scanned(self) -> None:
        self.custom_rules_scanned += 1

    def report_assertion_emitted(self) -> None:
        self.assertions_emitted += 1

    def report_alert_scanned(self) -> None:
        self.alerts_scanned += 1

    def report_run_event_emitted(self) -> None:
        self.run_events_emitted += 1

    def report_job_execution_scanned(self) -> None:
        self.job_executions_scanned += 1

    def report_metric_point_fetched(self, count: int = 1) -> None:
        self.metric_points_fetched += count

    def report_mcon_resolved(self) -> None:
        self.mcons_resolved += 1

    def report_mcon_resolution_failed(self) -> None:
        self.mcons_resolution_failed += 1

    def report_build_failure(self) -> None:
        self.build_failures += 1

    def warning(
        self,
        message: LiteralString,
        context: Optional[str] = None,
        title: Optional[LiteralString] = None,
        exc: Optional[BaseException] = None,
        log: bool = True,
        log_category: Optional[StructuredLogCategory] = None,
    ) -> None:
        # Keep an exact running count alongside the inherited LossyList so the
        # report can surface "N warnings" without the LossyList's sample cap.
        self.warnings_count += 1
        super().warning(
            message=message,
            context=context,
            title=title,
            exc=exc,
            log=log,
            log_category=log_category,
        )
