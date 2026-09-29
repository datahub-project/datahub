from dataclasses import dataclass

from datahub.ingestion.api.report import Report


@dataclass
class ExternalDQReport(Report):
    rules_read: int = 0
    rules_skipped_invalid: int = 0
    rules_duplicate: int = 0
    rules_unresolved_dataset: int = 0
    assertions_emitted: int = 0
    results_read: int = 0
    results_skipped_invalid: int = 0
    results_skipped_future: int = 0
    results_unknown_rule: int = 0
    results_skipped_invalid_rule: int = 0
    results_unresolved_expired: int = 0
    results_skipped_retired: int = 0
    results_missed_late: int = 0
    results_already_emitted: int = 0
    run_events_emitted: int = 0
