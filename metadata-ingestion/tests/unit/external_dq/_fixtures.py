from typing import Any, Dict, Optional

from datahub.ingestion.source.state.checkpoint import Checkpoint

T0 = 1_700_000_000_000  # epoch millis used across tests


def rule_raw(**overrides: Any) -> Dict[str, Any]:
    row: Dict[str, Any] = {
        "rule_id": "r1",
        "dataset_path": ["main", "sales", "orders"],
        "column_paths": ["amount"],
        "rule_name": "amount is not null",
        "rule_type": "completeness",
        "rule_description": None,
        "dimension": "Completeness",
        "operator": "NOT_NULL",
        "threshold_min": None,
        "threshold_max": None,
        "threshold_value": None,
        "logic": "amount IS NOT NULL",
        "severity": "high",
        "is_active": True,
        "rule_version": "3",
        "external_url": None,
        "updated_at": T0,
    }
    row.update(overrides)
    return row


def result_raw(**overrides: Any) -> Dict[str, Any]:
    row: Dict[str, Any] = {
        "run_id": "run-1",
        "rule_id": "r1",
        "executed_at": T0,
        "status": "failure",
        "is_warning": None,
        "severity": None,
        "actual_value": 3.0,
        "evaluated_row_count": 100,
        "failed_row_count": 3,
        "missing_row_count": None,
        "operator_snapshot": "NOT_NULL",
        "threshold_min_snapshot": None,
        "threshold_max_snapshot": None,
        "threshold_value_snapshot": None,
        "rule_version_snapshot": "3",
        "error_type": None,
        "error_message": None,
        "external_url": None,
    }
    row.update(overrides)
    return row


class FakeStateProvider:
    """Stands in for StateProviderWrapper: one last checkpoint, one current one."""

    def __init__(self, last: Optional[Checkpoint] = None) -> None:
        self.last = last
        self.current: Optional[Checkpoint] = None
        self.handler: Any = None

    def is_stateful_ingestion_configured(self) -> bool:
        return True

    def register_stateful_ingestion_usecase_handler(self, handler: Any) -> None:
        self.handler = handler

    def get_last_checkpoint(self, job_id: str, cls: type) -> Optional[Checkpoint]:
        return self.last

    def get_current_checkpoint(self, job_id: str) -> Optional[Checkpoint]:
        if self.current is None:
            self.current = self.handler.create_checkpoint()
        return self.current
