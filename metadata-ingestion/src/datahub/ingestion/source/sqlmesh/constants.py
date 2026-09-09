from typing import Dict, Tuple

from pydantic import BaseModel

from datahub.metadata.schema_classes import (
    AssertionStdAggregationClass,
    AssertionStdOperatorClass,
    DatasetAssertionScopeClass,
)

SQLMESH_PLATFORM = "sqlmesh"

# Platform registration / branding.
SQLMESH_DISPLAY_NAME = "SQLMesh"
SQLMESH_LOGO_URL = "assets/platforms/sqlmeshlogo.png"
DATASET_NAME_DELIMITER = "."

# Fallback / sentinel platform + naming values used in URN construction.
UNKNOWN_PLATFORM = "unknown"
SNOWFLAKE_PLATFORM = "snowflake"
DEFAULT_GATEWAY = "default"
PROD_ENVIRONMENT = "prod"

# SQLMesh environment_suffix_target modes (where the env name is appended).
ENV_SUFFIX_TARGET_SCHEMA = "schema"
ENV_SUFFIX_TARGET_TABLE = "table"
ENV_SUFFIX_TARGET_CATALOG = "catalog"

# SQLMesh model-kind names referenced directly in emission logic. The full set
# of kind → subtype mappings lives in MODEL_KIND_TO_SUBTYPE (sqlmesh_config).
MODEL_KIND_EXTERNAL = "EXTERNAL"
MODEL_KIND_EMBEDDED = "EMBEDDED"

# Container / dataset subtypes.
SUBTYPE_DATABASE = "Database"
SUBTYPE_SCHEMA = "Schema"
DEFAULT_MODEL_SUBTYPE = "Model"

# Field names in the audit-results JSON (produced by ``sqlmesh audit --output``).
AUDIT_RESULT_METADATA = "metadata"
AUDIT_RESULT_GENERATED_AT = "generated_at"
AUDIT_RESULT_RESULTS = "results"
AUDIT_RESULT_MODEL = "model"
AUDIT_RESULT_AUDIT = "audit"
AUDIT_RESULT_COLUMNS = "columns"
AUDIT_RESULT_STATUS = "status"
AUDIT_RESULT_FAILING_ROWS = "failing_rows"

# Audit status literals (compared case-insensitively after lower()).
AUDIT_STATUS_PASS = "pass"
AUDIT_STATUS_FAIL = "fail"
AUDIT_STATUS_SKIP = "skip"

# SQLGlot expression kwargs carried on ``model.audits`` entries.
AUDIT_KWARG_COLUMNS = "columns"
# Kwargs under which a non-standard audit carries its own SQL predicate.
AUDIT_LOGIC_KWARGS: Tuple[str, ...] = ("criteria", "condition")

# Assertion / incident / operation identifiers.
CUSTOM_ASSERTION_TYPE = "SQLMesh"
INGEST_ACTOR = "__sqlmesh_ingest__"
AUDIT_RUN_ID_PREFIX = "sqlmesh-audit-"
INCIDENT_CUSTOM_TYPE_PREFIX = "SQLMESH_AUDIT"
OPERATION_FINGERPRINT_REBUILD = "SQLMESH_FINGERPRINT_REBUILD"
NATIVE_RESULT_FAILING_ROWS = "failing_rows"

# customProperties keys (the ``sqlmesh.*`` namespace on emitted entities).
PROP_MODEL_NAME = "sqlmesh.model_name"
PROP_ENVIRONMENT = "sqlmesh.environment"
PROP_WAREHOUSE = "sqlmesh.warehouse"
PROP_GATEWAY = "sqlmesh.gateway"
PROP_PHYSICAL_TABLE = "sqlmesh.physical_table"
PROP_WAREHOUSE_INSTANCE = "sqlmesh.warehouse_instance"
PROP_MODEL_KIND = "sqlmesh.model_kind"
PROP_CRON = "sqlmesh.cron"
PROP_START = "sqlmesh.start"
PROP_TIME_COLUMN = "sqlmesh.time_column"
PROP_PARTITIONED_BY = "sqlmesh.partitioned_by"
PROP_GRAIN = "sqlmesh.grain"
PROP_AUDITS = "sqlmesh.audits"
PROP_FINGERPRINT_STALE = "sqlmesh.fingerprint_stale"
# Only provenance stays in customProperties; the check's semantics (scope /
# operator / aggregation / parameters / nativeParameters / fields) live on
# CustomAssertionInfo so the UI can render them structurally, matching dbt.
PROP_AUDIT = "sqlmesh.audit"


# Describes the semantics of each SQLMesh built-in audit so it can be emitted as
# a structured CUSTOM DataHub assertion (SQLMesh, not DataHub, executes them).
# scope / operator / aggregation and the AssertionStdParameters shape land on
# CustomAssertionInfo. Audits not listed here fall back to a NATIVE row-level
# CUSTOM assertion with no structured parameters.
class _AuditAssertionParams(BaseModel):
    scope: str
    operator: str
    aggregation: str
    uses_columns: bool = True  # True when audit columns → individual field assertions
    # AssertionStdParameters shape for the built-in (dbt parity). At most one of
    # these applies; all unset → the assertion carries no structured parameters.
    # const_value: a fixed scalar `value` with no backing kwarg.
    const_value: str = ""
    # value_kwarg: audit kwarg whose extracted scalar (or list, when
    # value_is_set) becomes the `value` parameter.
    value_kwarg: str = ""
    value_is_set: bool = False
    # min_kwarg / max_kwarg: audit kwargs → minValue / maxValue (accepted_range).
    min_kwarg: str = ""
    max_kwarg: str = ""


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
        const_value="1.0",
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
        value_kwarg="threshold",
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
        min_kwarg="min_v",
        max_kwarg="max_v",
    ),
    "accepted_values": _AuditAssertionParams(
        scope=DatasetAssertionScopeClass.DATASET_COLUMN,
        operator=AssertionStdOperatorClass.IN,
        aggregation=AssertionStdAggregationClass.IDENTITY,
        value_kwarg="is_in",
        value_is_set=True,
    ),
}
