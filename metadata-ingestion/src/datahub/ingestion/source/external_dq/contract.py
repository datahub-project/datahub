import enum
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, FrozenSet, List, Mapping, Optional, Tuple

from pydantic import BaseModel, ConfigDict, field_validator

CONTRACT_VERSION = 1

EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


def datetime_to_millis(dt: datetime) -> int:
    # Integer math: float timestamp() * 1000 can drop a millisecond, which would
    # break run dedup keyed on (rule_id, run_id) + executed_at.
    return (dt - EPOCH) // timedelta(milliseconds=1)


class LogicalType(str, enum.Enum):
    STRING = "STRING"
    BOOLEAN = "BOOLEAN"
    INT64 = "INT64"
    FLOAT64 = "FLOAT64"
    TIMESTAMP = "TIMESTAMP"
    ARRAY_STRING = "ARRAY_STRING"


@dataclass(frozen=True)
class ContractColumn:
    """A column every table implementing the contract must have. Which columns
    may be NULL per row is defined by the row models below."""

    name: str
    logical_type: LogicalType


_S = LogicalType.STRING
_B = LogicalType.BOOLEAN
_I = LogicalType.INT64
_F = LogicalType.FLOAT64
_T = LogicalType.TIMESTAMP
_A = LogicalType.ARRAY_STRING

RULES_COLUMNS: Tuple[ContractColumn, ...] = (
    ContractColumn("rule_id", _S),
    ContractColumn("dataset_path", _A),
    ContractColumn("column_paths", _A),
    ContractColumn("rule_name", _S),
    ContractColumn("rule_type", _S),
    ContractColumn("rule_description", _S),
    ContractColumn("dimension", _S),
    ContractColumn("operator", _S),
    ContractColumn("threshold_min", _F),
    ContractColumn("threshold_max", _F),
    ContractColumn("threshold_value", _F),
    ContractColumn("logic", _S),
    ContractColumn("severity", _S),
    ContractColumn("is_active", _B),
    ContractColumn("rule_version", _S),
    ContractColumn("external_url", _S),
    ContractColumn("updated_at", _T),
)

RESULTS_COLUMNS: Tuple[ContractColumn, ...] = (
    ContractColumn("run_id", _S),
    ContractColumn("rule_id", _S),
    ContractColumn("executed_at", _T),
    ContractColumn("status", _S),
    ContractColumn("is_warning", _B),
    ContractColumn("severity", _S),
    ContractColumn("actual_value", _F),
    ContractColumn("evaluated_row_count", _I),
    ContractColumn("failed_row_count", _I),
    ContractColumn("missing_row_count", _I),
    ContractColumn("operator_snapshot", _S),
    ContractColumn("threshold_min_snapshot", _F),
    ContractColumn("threshold_max_snapshot", _F),
    ContractColumn("threshold_value_snapshot", _F),
    ContractColumn("rule_version_snapshot", _S),
    ContractColumn("error_type", _S),
    ContractColumn("error_message", _S),
    ContractColumn("external_url", _S),
)

RULES_NAMES: FrozenSet[str] = frozenset(c.name for c in RULES_COLUMNS)
RESULTS_NAMES: FrozenSet[str] = frozenset(c.name for c in RESULTS_COLUMNS)

RESULT_STATUSES: FrozenSet[str] = frozenset({"SUCCESS", "FAILURE", "ERROR", "INIT"})
SEVERITIES: FrozenSet[str] = frozenset({"LOW", "MEDIUM", "HIGH"})


def _normalize_enum(value: Optional[str], allowed: FrozenSet[str]) -> Optional[str]:
    if value is None:
        return None
    normalized = value.strip().upper()
    if normalized not in allowed:
        raise ValueError(f"must be one of {sorted(allowed)}, got {value!r}")
    return normalized


def _non_blank(value: str) -> str:
    if not value.strip():
        raise ValueError("must not be blank")
    return value.strip()


class _ContractRow(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    # Non-contract ("extension") columns, stringified, passed through to DataHub.
    extras: Dict[str, str] = {}


class RuleRow(_ContractRow):
    rule_id: str
    dataset_path: List[str]
    column_paths: List[str] = []
    rule_name: str
    rule_type: str
    rule_description: Optional[str] = None
    dimension: Optional[str] = None
    operator: Optional[str] = None
    threshold_min: Optional[float] = None
    threshold_max: Optional[float] = None
    threshold_value: Optional[float] = None
    logic: Optional[str] = None
    severity: Optional[str] = None
    is_active: bool
    rule_version: Optional[str] = None
    external_url: Optional[str] = None
    updated_at: datetime

    @field_validator("rule_id", "rule_name", "rule_type")
    @classmethod
    def _required_text(cls, value: str) -> str:
        return _non_blank(value)

    @field_validator("dataset_path")
    @classmethod
    def _dataset_path(cls, value: List[str]) -> List[str]:
        if not value or any(not part.strip() for part in value):
            raise ValueError("must be a non-empty list of non-blank names")
        return [part.strip() for part in value]

    @field_validator("column_paths", mode="before")
    @classmethod
    def _column_paths(cls, value: Optional[List[str]]) -> List[str]:
        return [] if value is None else value

    @field_validator("column_paths")
    @classmethod
    def _column_paths_non_blank(cls, value: List[str]) -> List[str]:
        if any(not part.strip() for part in value):
            raise ValueError("must not contain blank column names")
        return [part.strip() for part in value]

    @field_validator("severity")
    @classmethod
    def _severity(cls, value: Optional[str]) -> Optional[str]:
        return _normalize_enum(value, SEVERITIES)


class ResultRow(_ContractRow):
    run_id: str
    rule_id: str
    executed_at: datetime
    status: str
    is_warning: bool = False
    severity: Optional[str] = None
    actual_value: Optional[float] = None
    evaluated_row_count: Optional[int] = None
    failed_row_count: Optional[int] = None
    missing_row_count: Optional[int] = None
    operator_snapshot: Optional[str] = None
    threshold_min_snapshot: Optional[float] = None
    threshold_max_snapshot: Optional[float] = None
    threshold_value_snapshot: Optional[float] = None
    rule_version_snapshot: Optional[str] = None
    error_type: Optional[str] = None
    error_message: Optional[str] = None
    external_url: Optional[str] = None

    @field_validator("run_id", "rule_id")
    @classmethod
    def _required_text(cls, value: str) -> str:
        return _non_blank(value)

    @field_validator("status")
    @classmethod
    def _status(cls, value: str) -> str:
        normalized = _normalize_enum(value, RESULT_STATUSES)
        assert normalized is not None
        return normalized

    @field_validator("is_warning", mode="before")
    @classmethod
    def _is_warning(cls, value: Optional[bool]) -> bool:
        return bool(value)

    @field_validator("severity")
    @classmethod
    def _severity(cls, value: Optional[str]) -> Optional[str]:
        return _normalize_enum(value, SEVERITIES)

    @property
    def executed_at_millis(self) -> int:
        return datetime_to_millis(self.executed_at)


def parse_rule_row(values: Mapping[str, Any], extras: Mapping[str, str]) -> RuleRow:
    return RuleRow.model_validate({**values, "extras": dict(extras)})


def parse_result_row(values: Mapping[str, Any], extras: Mapping[str, str]) -> ResultRow:
    return ResultRow.model_validate({**values, "extras": dict(extras)})
