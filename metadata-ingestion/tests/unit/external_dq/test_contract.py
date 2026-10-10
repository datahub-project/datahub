from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Dict, List

import pytest

from datahub.ingestion.source.external_dq.contract import (
    RESULTS_COLUMNS,
    RULES_COLUMNS,
    ContractColumn,
    LogicalType,
    datetime_to_millis,
    parse_result_row,
    parse_rule_row,
)
from datahub.ingestion.source.external_dq.types import coerce_value
from tests.unit.external_dq._fixtures import T0, result_raw, rule_raw


class _NumpyLikeArray:
    def tolist(self) -> List[Any]:
        return ["a", "b"]


def _coerce_all(raw: Dict[str, Any], contract: tuple) -> Dict[str, Any]:
    columns: List[ContractColumn] = list(contract)
    return {c.name: coerce_value(raw.get(c.name), c.logical_type) for c in columns}


def test_contract_column_order_is_fixed() -> None:
    assert [c.name for c in RULES_COLUMNS][:3] == [
        "rule_id",
        "dataset_path",
        "column_paths",
    ]
    assert [c.name for c in RESULTS_COLUMNS][:4] == [
        "run_id",
        "rule_id",
        "executed_at",
        "status",
    ]
    assert len(RULES_COLUMNS) == 17 and len(RESULTS_COLUMNS) == 18


def test_coerce_array_variants() -> None:
    assert coerce_value(["a", "b"], LogicalType.ARRAY_STRING) == ["a", "b"]
    assert coerce_value('["a", "b"]', LogicalType.ARRAY_STRING) == ["a", "b"]
    with pytest.raises(ValueError):
        coerce_value('{"a": 1}', LogicalType.ARRAY_STRING)
    # Driver-specific array objects are normalized by the reader, not here.
    with pytest.raises(ValueError):
        coerce_value(_NumpyLikeArray(), LogicalType.ARRAY_STRING)


def test_coerce_timestamp_is_utc_and_exact() -> None:
    from_millis = coerce_value(T0 + 123, LogicalType.TIMESTAMP)
    assert from_millis.tzinfo == timezone.utc
    assert datetime_to_millis(from_millis) == T0 + 123
    naive = coerce_value(datetime(2024, 1, 1, 12, 0), LogicalType.TIMESTAMP)
    assert naive == datetime(2024, 1, 1, 12, 0, tzinfo=timezone.utc)


def test_coerce_rejects_lossy_values() -> None:
    assert coerce_value("TRUE", LogicalType.BOOLEAN) is True
    with pytest.raises(ValueError):
        coerce_value(1.5, LogicalType.INT64)
    with pytest.raises(ValueError):
        coerce_value("maybe", LogicalType.BOOLEAN)


def test_coerce_rejects_non_finite_int64() -> None:
    with pytest.raises(ValueError):
        coerce_value(float("inf"), LogicalType.INT64)
    with pytest.raises(ValueError):
        coerce_value(Decimal("NaN"), LogicalType.INT64)


def test_coerce_rejects_non_finite_float64() -> None:
    # A NaN reaches the sink as a bare `NaN` JSON token, which GMS rejects after
    # the checkpoint has already recorded the row as emitted.
    for value in (float("nan"), "inf", Decimal("-Infinity")):
        with pytest.raises(ValueError):
            coerce_value(value, LogicalType.FLOAT64)


def test_coerce_timestamp_out_of_range_is_a_value_error() -> None:
    with pytest.raises(ValueError):
        coerce_value(1_700_000_000_000_000, LogicalType.TIMESTAMP)


def test_coerce_array_rejects_null_element() -> None:
    with pytest.raises(ValueError):
        coerce_value(["a", None], LogicalType.ARRAY_STRING)


def test_parse_rule_row_normalizes_and_keeps_extras() -> None:
    rule = parse_rule_row(
        _coerce_all(rule_raw(), RULES_COLUMNS), {"dataset_name": "orders"}
    )
    assert rule.severity == "HIGH"
    assert rule.extras == {"dataset_name": "orders"}
    assert (
        parse_rule_row(
            _coerce_all(rule_raw(column_paths=None), RULES_COLUMNS), {}
        ).column_paths
        == []
    )


@pytest.mark.parametrize(
    "override",
    [
        {"rule_id": "  "},
        {"dataset_path": []},
        {"is_active": None},
        {"severity": "urgent"},
        {"column_paths": [" "]},
    ],
)
def test_parse_rule_row_rejects_invalid(override: Dict[str, Any]) -> None:
    with pytest.raises(ValueError):
        parse_rule_row(_coerce_all(rule_raw(**override), RULES_COLUMNS), {})


def test_parse_result_row() -> None:
    result = parse_result_row(_coerce_all(result_raw(), RESULTS_COLUMNS), {})
    assert result.status == "FAILURE"
    assert result.is_warning is False
    assert result.executed_at_millis == T0
    with pytest.raises(ValueError):
        parse_result_row(_coerce_all(result_raw(status="WARN"), RESULTS_COLUMNS), {})
