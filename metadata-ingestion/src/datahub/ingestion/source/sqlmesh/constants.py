from typing import Dict, Tuple

from pydantic import BaseModel, Field

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
PROP_AUDIT = "sqlmesh.audit"
PROP_SCOPE = "sqlmesh.scope"
PROP_OPERATOR = "sqlmesh.operator"
PROP_AGGREGATION = "sqlmesh.aggregation"
PROP_NATIVE_PARAMETERS = "sqlmesh.native_parameters"
PROP_FIELDS = "sqlmesh.fields"
PROP_THRESHOLD = "sqlmesh.threshold"
PROP_MIN_VALUE = "sqlmesh.min_value"
PROP_MAX_VALUE = "sqlmesh.max_value"
PROP_ACCEPTED_VALUES = "sqlmesh.accepted_values"


# Describes the semantics of each SQLMesh built-in audit. Every audit becomes a
# CUSTOM DataHub assertion (SQLMesh, not DataHub, executes them) — these values
# are carried as customProperties so the check's shape stays inspectable.
# Audits not listed here are emitted as CUSTOM without the semantic properties.
class _AuditAssertionParams(BaseModel):
    scope: str
    operator: str
    aggregation: str
    uses_columns: bool = True  # True when audit columns → individual field assertions
    # Declarative extra customProperties, so all per-audit property mappings
    # live here rather than being hardcoded inline in _emit_single_audit.
    # literal_props: audit kwarg (a SQLGlot Literal) → customProperty key.
    literal_props: Dict[str, str] = Field(default_factory=dict)
    # expression_list_props: audit kwarg (a SQLGlot expression list) →
    # customProperty key; the extracted scalars are comma-joined.
    expression_list_props: Dict[str, str] = Field(default_factory=dict)


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
        literal_props={"threshold": PROP_THRESHOLD},
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
        literal_props={"min_v": PROP_MIN_VALUE, "max_v": PROP_MAX_VALUE},
    ),
    "accepted_values": _AuditAssertionParams(
        scope=DatasetAssertionScopeClass.DATASET_COLUMN,
        operator=AssertionStdOperatorClass.IN,
        aggregation=AssertionStdAggregationClass.IDENTITY,
        expression_list_props={"values": PROP_ACCEPTED_VALUES},
    ),
}
