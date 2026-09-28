from typing import List, Sequence

from datahub.ingestion.source.external_dq.contract import (
    RULES_COLUMNS,
    ContractColumn,
    LogicalType,
)
from datahub.ingestion.source.external_dq.types import DATABRICKS_TYPE_PROFILE
from datahub.ingestion.source.external_dq.validate import (
    PhysicalColumn,
    TableValidation,
    validate_table,
)

DBX_TYPES = {
    LogicalType.STRING: "string",
    LogicalType.BOOLEAN: "boolean",
    LogicalType.INT64: "bigint",
    LogicalType.FLOAT64: "double",
    LogicalType.TIMESTAMP: "timestamp",
    LogicalType.ARRAY_STRING: "array<string>",
}


def physical_for(contract: Sequence[ContractColumn]) -> List[PhysicalColumn]:
    return [
        PhysicalColumn(c.name, DBX_TYPES[c.logical_type], i + 1)
        for i, c in enumerate(contract)
    ]


def _validate(physical: List[PhysicalColumn], strict: bool = False) -> TableValidation:
    return validate_table(
        physical, RULES_COLUMNS, DATABRICKS_TYPE_PROFILE, strict_column_order=strict
    )


def test_matching_table_with_widened_types_is_valid() -> None:
    physical = physical_for(RULES_COLUMNS)
    physical[8] = PhysicalColumn("threshold_min", "DECIMAL(18, 4)", 9)  # FLOAT64
    physical[5] = PhysicalColumn("rule_description", "varchar(500)", 6)  # STRING
    result = _validate(physical + [PhysicalColumn("dataset_name", "string", 18)])
    assert result.errors == [] and result.warnings == []


def test_missing_column_and_lossy_type_are_errors() -> None:
    physical = [p for p in physical_for(RULES_COLUMNS) if p.name != "logic"]
    physical = [
        PhysicalColumn(p.name, "timestamp_ntz", p.position)
        if p.name == "updated_at"
        else p
        for p in physical
    ]
    result = _validate(physical)
    assert any("logic" in e for e in result.errors)
    assert any("updated_at" in e and "timestamp_ntz" in e for e in result.errors)


def test_column_order_is_warning_unless_strict() -> None:
    physical = physical_for(RULES_COLUMNS)
    first, second = physical[0], physical[1]
    physical[0] = PhysicalColumn(second.name, second.data_type, 1)
    physical[1] = PhysicalColumn(first.name, first.data_type, 2)
    assert _validate(physical).errors == []
    assert _validate(physical).warnings
    assert _validate(physical, strict=True).errors


def test_extension_column_before_contract_columns_is_an_order_issue() -> None:
    physical = [PhysicalColumn("dataset_name", "string", 0)] + physical_for(
        RULES_COLUMNS
    )
    assert _validate(physical).warnings


def test_empty_table_description_is_an_error() -> None:
    assert _validate([]).errors
