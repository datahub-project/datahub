from dataclasses import dataclass, field

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

    mcons_resolved: int = 0
    mcons_resolution_failed: int = 0
    mcons_unmapped_platform: LossyList[str] = field(default_factory=LossyList)
    alerts_without_monitor: int = 0
    filtered: LossyList[str] = field(default_factory=LossyList)

    def report_dropped(self, name: str) -> None:
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

    def report_mcon_resolved(self) -> None:
        self.mcons_resolved += 1

    def report_mcon_resolution_failed(self) -> None:
        self.mcons_resolution_failed += 1
