from dataclasses import dataclass
from typing import Dict

from datahub.metadata.schema_classes import (
    AssertionStdAggregationClass,
    AssertionStdOperatorClass,
    DatasetAssertionScopeClass,
)

SQLMESH_PLATFORM = "sqlmesh"


# Describes the semantics of each SQLMesh built-in audit. Every audit becomes a
# CUSTOM DataHub assertion (SQLMesh, not DataHub, executes them) — these values
# are carried as customProperties so the check's shape stays inspectable.
# Audits not listed here are emitted as CUSTOM without the semantic properties.
@dataclass
class _AuditAssertionParams:
    scope: str
    operator: str
    aggregation: str
    uses_columns: bool = True  # True when audit columns → individual field assertions
    row_count_threshold: bool = False  # True for number_of_rows


_SQLMESH_AUDIT_MAP: Dict[str, _AuditAssertionParams] = {
    "not_null": _AuditAssertionParams(
        scope=DatasetAssertionScopeClass.DATASET_COLUMN,
        operator=AssertionStdOperatorClass.NOT_NULL,
        aggregation=AssertionStdAggregationClass.IDENTITY,
    ),
    "unique_values": _AuditAssertionParams(
        scope=DatasetAssertionScopeClass.DATASET_COLUMN,
        operator=AssertionStdOperatorClass.EQUAL_TO,
        aggregation=AssertionStdAggregationClass.UNIQUE_PROPOTION,
    ),
    "unique_combination_of_columns": _AuditAssertionParams(
        scope=DatasetAssertionScopeClass.DATASET_ROWS,
        operator=AssertionStdOperatorClass._NATIVE_,
        aggregation=AssertionStdAggregationClass._NATIVE_,
        uses_columns=False,
    ),
    "number_of_rows": _AuditAssertionParams(
        scope=DatasetAssertionScopeClass.DATASET_ROWS,
        operator=AssertionStdOperatorClass.GREATER_THAN,
        aggregation=AssertionStdAggregationClass.ROW_COUNT,
        uses_columns=False,
        row_count_threshold=True,
    ),
    "forall": _AuditAssertionParams(
        scope=DatasetAssertionScopeClass.DATASET_ROWS,
        operator=AssertionStdOperatorClass._NATIVE_,
        aggregation=AssertionStdAggregationClass._NATIVE_,
        uses_columns=False,
    ),
    "accepted_range": _AuditAssertionParams(
        scope=DatasetAssertionScopeClass.DATASET_COLUMN,
        operator=AssertionStdOperatorClass.BETWEEN,
        aggregation=AssertionStdAggregationClass.IDENTITY,
    ),
    "accepted_values": _AuditAssertionParams(
        scope=DatasetAssertionScopeClass.DATASET_COLUMN,
        operator=AssertionStdOperatorClass.IN,
        aggregation=AssertionStdAggregationClass.IDENTITY,
    ),
}
