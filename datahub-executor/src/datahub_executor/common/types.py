import logging
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional, Union

import pydantic.v1 as pydantic
from acryl.executor.request.execution_request import ExecutionRequest
from pydantic import BaseModel, Field, ValidationInfo, field_validator, model_validator

from datahub_executor.common.metric.types import (
    Metric,
)

logger = logging.getLogger(__name__)


class PermissiveBaseModel(BaseModel):
    class Config:
        extra = "allow"
        populate_by_name = (  # allow_population_by_field_name in pydantic v1
            True  # Allow parsing via both field name and alias
        )


# The following types are used bound from
# the GraphQL API response objects.
# We allow Pydantic to handle most of the mapping for us.


class AssertionStdOperator(Enum):
    BETWEEN = "BETWEEN"
    LESS_THAN = "LESS_THAN"
    LESS_THAN_OR_EQUAL_TO = "LESS_THAN_OR_EQUAL_TO"
    GREATER_THAN = "GREATER_THAN"
    GREATER_THAN_OR_EQUAL_TO = "GREATER_THAN_OR_EQUAL_TO"
    EQUAL_TO = "EQUAL_TO"
    NOT_EQUAL_TO = "NOT_EQUAL_TO"
    NULL = "NULL"
    NOT_NULL = "NOT_NULL"
    CONTAIN = "CONTAIN"
    END_WITH = "END_WITH"
    START_WITH = "START_WITH"
    REGEX_MATCH = "REGEX_MATCH"
    IN = "IN"
    IS_TRUE = "IS_TRUE"
    IS_FALSE = "IS_FALSE"
    NOT_IN = "NOT_IN"


class AssertionStdParameterType(Enum):
    STRING = "STRING"
    NUMBER = "NUMBER"
    LIST = "LIST"
    SET = "SET"
    SQL = "SQL"
    UNKNOWN = "UNKNOWN"


class MonitorType(Enum):
    """Enumeration of monitor types."""

    ASSERTION = "ASSERTION"


class AssertionType(Enum):
    """Enumeration of assertion types."""

    DATASET = "DATASET"
    FRESHNESS = "FRESHNESS"
    VOLUME = "VOLUME"
    SQL = "SQL"
    FIELD = "FIELD"
    DATA_SCHEMA = "DATA_SCHEMA"


class AssertionResultErrorType(Enum):
    """Enumeration of assertion result error types"""

    SOURCE_CONNECTION_ERROR = "SOURCE_CONNECTION_ERROR"
    SOURCE_QUERY_FAILED = "SOURCE_QUERY_FAILED"
    INSUFFICIENT_DATA = "INSUFFICIENT_DATA"
    INVALID_PARAMETERS = "INVALID_PARAMETERS"
    INVALID_SOURCE_TYPE = "INVALID_SOURCE_TYPE"
    UNSUPPORTED_PLATFORM = "UNSUPPORTED_PLATFORM"
    CUSTOM_SQL_ERROR = "CUSTOM_SQL_ERROR"
    FIELD_ASSERTION_ERROR = "FIELD_ASSERTION_ERROR"
    UNKNOWN_ERROR = "UNKNOWN_ERROR"


class FreshnessAssertionType(Enum):
    """Enumeration of freshness assertion types."""

    DATASET_CHANGE = "DATASET_CHANGE"


class VolumeAssertionType(Enum):
    """Enumeration of volume assertion types."""

    ROW_COUNT_TOTAL = "ROW_COUNT_TOTAL"
    ROW_COUNT_CHANGE = "ROW_COUNT_CHANGE"
    INCREMENTING_SEGMENT_ROW_COUNT_TOTAL = "INCREMENTING_SEGMENT_ROW_COUNT_TOTAL"
    INCREMENTING_SEGMENT_ROW_COUNT_CHANGE = "INCREMENTING_SEGMENT_ROW_COUNT_CHANGE"


class SQLAssertionType(Enum):
    """Enumeration of SQL assertion types."""

    METRIC = "METRIC"
    METRIC_CHANGE = "METRIC_CHANGE"


class FreshnessAssertionScheduleType(Enum):
    """Enumeration of freshness assertion schedule types."""

    CRON = "CRON"
    FIXED_INTERVAL = "FIXED_INTERVAL"
    SINCE_THE_LAST_CHECK = "SINCE_THE_LAST_CHECK"


class FieldAssertionType(Enum):
    """Enumeration of field assertion types."""

    FIELD_VALUES = "FIELD_VALUES"
    FIELD_METRIC = "FIELD_METRIC"


class SchemaAssertionCompatibility(Enum):
    """Enumeration of compatibility types for schema assertions"""

    EXACT_MATCH = "EXACT_MATCH"
    SUPERSET = "SUPERSET"
    SUBSET = "SUBSET"


class PartitionType(Enum):
    """Enumeration of partition types."""

    FULL_TABLE = "FULL_TABLE"
    QUERY = "QUERY"
    TIMESTAMP_FIELD = "TIMESTAMP_FIELD"
    PARTITION = "PARTITION"


class CalendarInterval(Enum):
    """Enumeration of calendar intervals."""

    MINUTE = "MINUTE"
    HOUR = "HOUR"
    DAY = "DAY"


class PartitionKeyFieldTransform(Enum):
    """Enumeration of partition key transform types."""

    DATE_DAY = "DATE_DAY"

    DATE_HOUR = "DATE_DAY"


class AssertionResultType(Enum):
    """Enumeration of assertion result types."""

    INIT = "INIT"
    SUCCESS = "SUCCESS"
    FAILURE = "FAILURE"
    ERROR = "ERROR"


class DatasetFilterType(Enum):
    """Enumeration of Filter types."""

    SQL = "SQL"


class DatasetFreshnessSourceType(Enum):
    """Enumeration of Dataset FRESHNESS source types result types."""

    # The freshness signal comes from a column / field value last updated.
    FIELD_VALUE = "FIELD_VALUE"

    # The freshness signal from a dataset / table last updated statistic, e.g. provided by the catalog.
    INFORMATION_SCHEMA = "INFORMATION_SCHEMA"

    # The freshness signal from underlying file system, e.g. file modification time.
    FILE_METADATA = "FILE_METADATA"

    # The freshness signal from the audit log.
    AUDIT_LOG = "AUDIT_LOG"

    # Determine whether the table has changed using an Operation aspect
    DATAHUB_OPERATION = "DATAHUB_OPERATION"


class DatasetVolumeSourceType(Enum):
    # Determine that a change has occurred by inspecting an information schema table, or other system metadata table.
    INFORMATION_SCHEMA = "INFORMATION_SCHEMA"

    # Determine the row count using a COUNT(*) query
    QUERY = "QUERY"

    # Determine the row count using the DataHub Dataset Profile aspect
    DATAHUB_DATASET_PROFILE = "DATAHUB_DATASET_PROFILE"


class DatasetSchemaSourceType(Enum):
    # Determine that a change has happened by consulting with the DataHub Information schema
    DATAHUB_SCHEMA = "DATAHUB_SCHEMA"


class EntityEventType(Enum):
    """Enumeration of Entity Events that we support retrieving using a particular connection"""

    # An update has been performed to some rows based on the value of a particular field.
    FIELD_UPDATE = "FIELD_UPDATE"

    # An update has been performed to the table, based on a dataset last updated statistic maintained by the source system
    INFORMATION_SCHEMA_UPDATE = "INFORMATION_SCHEMA_UPDATE"

    # An update has been performed to the table, based on update in underlying file system's metadata
    FILE_METADATA_UPDATE = "FILE_METADATA_UPDATE"

    # An update has been performed on a particular entity, as per an audit log.
    AUDIT_LOG_OPERATION = "AUDIT_LOG_OPERATION"

    # An update has been performed on a particular entity, as per an Operation aspect.
    DATAHUB_OPERATION = "DATAHUB_OPERATION"

    # A data job has completed successfully
    DATA_JOB_RUN_COMPLETED_SUCCESS = "DATA_JOB_RUN_COMPLETED_SUCCESS"

    # A data job has completed successfully
    DATA_JOB_RUN_COMPLETED_FAILURE = "DATA_JOB_RUN_COMPLETED_FAILURE"


class AssertionEvaluationParametersType(Enum):
    """Enumeration of evaluation parameter types for an assertion"""

    DATASET_FRESHNESS = "DATASET_FRESHNESS"
    DATASET_VOLUME = "DATASET_VOLUME"
    DATASET_SQL = "DATASET_SQL"
    DATASET_FIELD = "DATASET_FIELD"
    DATASET_SCHEMA = "DATASET_SCHEMA"


class FreshnessFieldKind(Enum):
    LAST_MODIFIED = "LAST_MODIFIED"
    HIGH_WATERMARK = "HIGH_WATERMARK"


class MonitorMode(Enum):
    ACTIVE = "ACTIVE"
    INACTIVE = "INACTIVE"
    PASSIVE = "PASSIVE"
    INIT = "INIT"


class AssertionValueChangeType(Enum):
    ABSOLUTE = "ABSOLUTE"
    PERCENTAGE = "PERCENTAGE"


class FieldValuesFailThresholdType(Enum):
    COUNT = "COUNT"
    PERCENTAGE = "PERCENTAGE"


class FieldTransformType(Enum):
    LENGTH = "LENGTH"


class FieldMetricType(Enum):
    UNIQUE_COUNT = "UNIQUE_COUNT"
    UNIQUE_PERCENTAGE = "UNIQUE_PERCENTAGE"
    NULL_COUNT = "NULL_COUNT"
    NULL_PERCENTAGE = "NULL_PERCENTAGE"
    MIN = "MIN"
    MAX = "MAX"
    MEAN = "MEAN"
    MEDIAN = "MEDIAN"
    STDDEV = "STDDEV"
    NEGATIVE_COUNT = "NEGATIVE_COUNT"
    NEGATIVE_PERCENTAGE = "NEGATIVE_PERCENTAGE"
    ZERO_COUNT = "ZERO_COUNT"
    ZERO_PERCENTAGE = "ZERO_PERCENTAGE"
    MIN_LENGTH = "MIN_LENGTH"
    MAX_LENGTH = "MAX_LENGTH"
    EMPTY_COUNT = "EMPTY_COUNT"
    EMPTY_PERCENTAGE = "EMPTY_PERCENTAGE"


class DatasetFieldSourceType(Enum):
    ALL_ROWS_QUERY = "ALL_ROWS_QUERY"
    CHANGED_ROWS_QUERY = "CHANGED_ROWS_QUERY"
    DATAHUB_DATASET_PROFILE = "DATAHUB_DATASET_PROFILE"


class IncrementingSegmentFieldTransformerType(Enum):
    """The 'standard' transformer type. Note that not all source systems will support all operators."""

    TIMESTAMP_MS_TO_MINUTE = "TIMESTAMP_MS_TO_MINUTE"
    TIMESTAMP_MS_TO_HOUR = "TIMESTAMP_MS_TO_HOUR"
    TIMESTAMP_MS_TO_DATE = "TIMESTAMP_MS_TO_DATE"
    TIMESTAMP_MS_TO_MONTH = "TIMESTAMP_MS_TO_MONTH"
    TIMESTAMP_MS_TO_YEAR = "TIMESTAMP_MS_TO_YEAR"
    FLOOR = "FLOOR"
    CEILING = "CEILING"
    NATIVE = "NATIVE"


class AssertionSourceType(Enum):
    NATIVE = "NATIVE"
    EXTERNAL = "EXTERNAL"
    INFERRED = "INFERRED"


class CronSchedule(PermissiveBaseModel):
    """The cron string"""

    cron: str

    timezone: str


class DatasetFilter(PermissiveBaseModel):
    """Filter applied to dataset"""

    type: DatasetFilterType

    sql: Optional[str] = None


class FreshnessCronSchedule(PermissiveBaseModel):
    """The cron string"""

    cron: str

    timezone: str

    # An optional start time offset from the cron schedule. If not provided, the boundary will be computed between the evaluation time and previous evaluation time.
    window_start_offset_ms: Optional[int] = Field(
        alias="windowStartOffsetMs", default=None
    )


class FixedIntervalSchedule(PermissiveBaseModel):
    """The unit of the fixed interval schedule"""

    unit: CalendarInterval

    multiple: int


class FreshnessAssertionSchedule(PermissiveBaseModel):
    """The type of the schedule"""

    type: FreshnessAssertionScheduleType

    cron: Optional[FreshnessCronSchedule] = None

    fixed_interval: Optional[FixedIntervalSchedule] = Field(
        alias="fixedInterval", default=None
    )


class SchemaFieldSpec(PermissiveBaseModel):
    """The schema field urn"""

    # The field path of the schema field"""
    path: str

    # The std DataHub type of the field
    type: str

    # The native type of the field collected from source
    native_type: str = Field(alias="nativeType")

    kind: Optional[FreshnessFieldKind] = None


class AuditLogSpec(PermissiveBaseModel):
    """The type of operation. If not provided all operations will be considered."""

    operation_types: Optional[List[str]] = Field(alias="operationTypes", default=None)

    user_name: Optional[str] = Field(alias="userName", default=None)


class DataHubOperationSpec(PermissiveBaseModel):
    """Information about the DataHub Operation aspect used to evaluate an assertion"""

    # The list of operation types that should be monitored. If not provided, a default set will be used.
    operation_types: Optional[List[str]] = Field(alias="operationTypes", default=None)

    # The list of custom operation types that should be monitored. If not provided, no custom operation types will be used.
    custom_operation_types: Optional[List[str]] = Field(
        alias="customOperationTypes", default=None
    )


class FreshnessFieldSpec(SchemaFieldSpec):
    kind: Optional[FreshnessFieldKind] = None


class DatasetFreshnessAssertionParameters(PermissiveBaseModel):
    """The type of the freshness signal"""

    source_type: DatasetFreshnessSourceType = Field(alias="sourceType")

    # A descriptor for a Dataset Field to use. Present when source_type is FIELD_LAST_UPDATED
    field: Optional[FreshnessFieldSpec] = None

    # A descriptor for a Dataset Column to use. Present when source_type is AUDIT_LOG_OPERATION
    audit_log: Optional[AuditLogSpec] = Field(alias="auditLog", default=None)

    # A descriptor for a DataHub operation to use. Present when source_type is DATAHUB_OPERATION
    datahub_operation: Optional[DataHubOperationSpec] = Field(
        alias="dataHubOperation", default=None
    )


class DatasetVolumeAssertionParameters(PermissiveBaseModel):
    source_type: DatasetVolumeSourceType = Field(alias="sourceType")


class DatasetSchemaAssertionParameters(PermissiveBaseModel):
    source_type: DatasetSchemaSourceType = Field(alias="sourceType")


class DatasetFieldAssertionParameters(PermissiveBaseModel):
    source_type: DatasetFieldSourceType = Field(alias="sourceType")

    changed_rows_field: Optional[FreshnessFieldSpec] = Field(
        alias="changedRowsField", default=None
    )


class FreshnessAssertion(PermissiveBaseModel):
    """The type of the FRESHNESS Assertion"""

    type: FreshnessAssertionType

    schedule: Optional[FreshnessAssertionSchedule] = None

    filter: Optional[DatasetFilter] = None


class AssertionStdParameter(PermissiveBaseModel):
    # The parameter value
    value: str

    # The type of the parameter
    type: AssertionStdParameterType


class AssertionStdParameters(PermissiveBaseModel):
    # The value parameter of an assertion
    value: Optional[AssertionStdParameter] = Field(default=None)

    # The maxValue parameter of an assertion
    max_value: Optional[AssertionStdParameter] = Field(alias="maxValue", default=None)

    # The minValue parameter of an assertion
    min_value: Optional[AssertionStdParameter] = Field(alias="minValue", default=None)


class RowCountTotal(PermissiveBaseModel):
    """Attributes defining an ROW_COUNT_TOTAL volume assertion."""

    # The operator GREATER_THAN, GREATER_THAN_OR_EQUAL_TO, EQUAL_TO, etc being applied to the assertion
    operator: AssertionStdOperator

    # The parameters provided as input to the operator.
    parameters: AssertionStdParameters

    class Config:
        arbitrary_types_allowed = True


class RowCountChange(PermissiveBaseModel):
    """Attributes defining an ROW_COUNT_CHANGE volume assertion."""

    # The type of the value used to evaluate the assertion: a fixed absolute value or a relative percentage.
    type: AssertionValueChangeType

    # The operator GREATER_THAN, GREATER_THAN_OR_EQUAL_TO, EQUAL_TO, etc being applied to the assertion
    operator: AssertionStdOperator

    # The parameters provided as input to the operator.
    parameters: AssertionStdParameters

    class Config:
        arbitrary_types_allowed = True


class IncrementingSegmentFieldTransformer(PermissiveBaseModel):
    """
    The definition of the transformer function that should be applied to a given field / column value in a dataset
    in order to determine the segment or bucket that it belongs to, which in turn is used to evaluate
    volume assertions.
    """

    # The 'standard' operator type. Note that not all source systems will support all operators.
    type: IncrementingSegmentFieldTransformerType

    # The 'native' transformer type, useful as a back door if a custom transformer is required.
    native_type: Optional[str] = Field(alias="nativeType", default=None)


class IncrementingSegmentSpec(PermissiveBaseModel):
    """Core attributes required to identify an incrementing segment in a table."""

    # The field to use to generate segments. It must be constantly incrementing as new rows are inserted.
    field: SchemaFieldSpec

    # Optional transformer function to apply to the field in order to obtain the final segment or bucket identifier.
    transformer: Optional[IncrementingSegmentFieldTransformer] = None


class IncrementingSegmentRowCountTotal(PermissiveBaseModel):
    segment: IncrementingSegmentSpec

    # The operator GREATER_THAN, GREATER_THAN_OR_EQUAL_TO, EQUAL_TO, etc being applied to the assertion
    operator: AssertionStdOperator

    # The parameters provided as input to the operator.
    parameters: AssertionStdParameters

    class Config:
        arbitrary_types_allowed = True


class IncrementingSegmentRowCountChange(PermissiveBaseModel):
    segment: IncrementingSegmentSpec

    type: AssertionValueChangeType

    # The operator GREATER_THAN, GREATER_THAN_OR_EQUAL_TO, EQUAL_TO, etc being applied to the assertion
    operator: AssertionStdOperator

    # The parameters provided as input to the operator.
    parameters: AssertionStdParameters

    class Config:
        arbitrary_types_allowed = True


class VolumeAssertion(PermissiveBaseModel):
    type: VolumeAssertionType

    row_count_total: Optional[RowCountTotal] = Field(
        alias="rowCountTotal", default=None
    )

    row_count_change: Optional[RowCountChange] = Field(
        alias="rowCountChange", default=None
    )

    incrementing_row_count_total: Optional[IncrementingSegmentRowCountTotal] = Field(
        alias="incrementingSegmentRowCountTotal", default=None
    )

    incrementing_row_count_change: Optional[IncrementingSegmentRowCountChange] = Field(
        alias="incrementingSegmentRowCountChange", default=None
    )

    filter: Optional[DatasetFilter] = None


class SQLAssertion(PermissiveBaseModel):
    type: SQLAssertionType

    statement: str

    change_type: Optional[AssertionValueChangeType] = Field(
        alias="changeType", default=None
    )

    # The operator GREATER_THAN, GREATER_THAN_OR_EQUAL_TO, EQUAL_TO, etc being applied to the assertion
    operator: AssertionStdOperator

    # The parameters provided as input to the operator.
    parameters: AssertionStdParameters


class FieldValuesFailThreshold(PermissiveBaseModel):
    type: FieldValuesFailThresholdType

    value: int = 0


class FieldTransform(PermissiveBaseModel):
    type: FieldTransformType


class FieldValuesAssertion(PermissiveBaseModel):
    """
    Attributes defining a field values assertion, which asserts that the values for a field / column
    of a dataset / table matches a set of expectations.
    In other words, this type of assertion acts as a semantic constraint applied to fields for a specific column.
    """

    field: SchemaFieldSpec

    operator: AssertionStdOperator

    parameters: Optional[AssertionStdParameters] = None

    transform: Optional[FieldTransform] = None

    fail_threshold: FieldValuesFailThreshold = Field(alias="failThreshold")

    exclude_nulls: bool = Field(alias="excludeNulls")

    @field_validator("parameters", mode="before")
    @classmethod
    def validate_parameters(
        cls, parameters: Union[dict, AssertionStdParameters], info: ValidationInfo
    ) -> Union[dict, AssertionStdParameters]:
        # Handle None case
        if parameters is None:
            return {}

        # Convert AssertionStdParameters to dict if needed
        params_dict = {}
        if isinstance(parameters, AssertionStdParameters):
            # Convert to dict for validation only
            params_dict = parameters.model_dump()
        else:
            params_dict = parameters

        operator = info.data.get("operator")
        # validate that the operator type is valid for the parameter type
        if operator in [
            AssertionStdOperator.LESS_THAN,
            AssertionStdOperator.LESS_THAN_OR_EQUAL_TO,
            AssertionStdOperator.GREATER_THAN,
            AssertionStdOperator.GREATER_THAN_OR_EQUAL_TO,
            AssertionStdOperator.EQUAL_TO,
            AssertionStdOperator.NOT_EQUAL_TO,
        ]:
            if not params_dict.get("value"):
                raise ValueError(
                    f"Parameter value is required when operator is {operator.name}"
                )

        if operator in [
            AssertionStdOperator.CONTAIN,
            AssertionStdOperator.END_WITH,
            AssertionStdOperator.START_WITH,
            AssertionStdOperator.IN,
            AssertionStdOperator.NOT_IN,
        ]:
            if not params_dict.get("value"):
                raise ValueError(
                    f"Parameter value is required when operator is {operator.name}"
                )

        if operator in [AssertionStdOperator.BETWEEN]:
            if not params_dict.get("minValue") or not params_dict.get("maxValue"):
                raise ValueError(
                    f"Parameter minValue and maxValue are required when operator is {operator.name}"
                )

        if operator in [AssertionStdOperator.REGEX_MATCH]:
            if not params_dict.get("value"):
                raise ValueError(
                    f"Parameter value is required when operator is {operator.name}"
                )

        # Return the original parameters
        return parameters

    @field_validator("transform", mode="before")
    @classmethod
    def validate_transform(
        cls, transform: Optional[FieldTransform], info: ValidationInfo
    ) -> Optional[FieldTransform]:
        if transform:
            parameters = info.data.get("parameters")
            field = info.data.get("field")
            if parameters and field:
                if not field.type == "STRING":
                    raise ValueError(
                        f"LENGTH Transform only valid on field type STRING, not {field.type}"
                    )

                if (
                    parameters.value
                    and not parameters.value.type == AssertionStdParameterType.NUMBER
                ):
                    raise ValueError(
                        f"LENGTH Transform only valid with type NUMBER, not {parameters.value.type}"
                    )

                if (
                    parameters.min_value
                    and not parameters.min_value.type
                    == AssertionStdParameterType.NUMBER
                ):
                    raise ValueError(
                        f"LENGTH Transform only valid with type NUMBER, not {parameters.min_value.type}"
                    )

                if (
                    parameters.max_value
                    and not parameters.max_value.type
                    == AssertionStdParameterType.NUMBER
                ):
                    raise ValueError(
                        f"LENGTH Transform only valid with type NUMBER, not {parameters.max_value.type}"
                    )

        return transform


class FieldMetricAssertion(PermissiveBaseModel):
    """
    Attributes defining a field metric assertion, which asserts an expectation against
    a common metric derived from the set of field / column values, for example:
    max, min, median, null count, null percentage, unique count, unique percentage, and more.
    """

    field: SchemaFieldSpec

    metric: FieldMetricType

    operator: AssertionStdOperator

    parameters: AssertionStdParameters


class FieldAssertion(PermissiveBaseModel):
    """Attributes defining a Field Assertion.."""

    type: FieldAssertionType

    field_values_assertion: Optional[FieldValuesAssertion] = Field(
        alias="fieldValuesAssertion", default=None
    )

    field_metric_assertion: Optional[FieldMetricAssertion] = Field(
        alias="fieldMetricAssertion", default=None
    )

    filter: Optional[DatasetFilter] = None


class SchemaFieldDataType(Enum):
    """
    A boolean type
    """

    BOOLEAN = "BOOLEAN"

    """
    A fixed bytestring type
    """
    FIXED = "FIXED"

    """
    A string type
    """
    STRING = "STRING"

    """
    A string of bytes
    """
    BYTES = "BYTES"

    """
    A number, including integers, floats, and doubles
    """
    NUMBER = "NUMBER"

    """
    A datestrings type
    """
    DATE = "DATE"

    """
    A timestamp type
    """
    TIME = "TIME"

    """
    An enum type
    """
    ENUM = "ENUM"

    """
    A NULL type
    """
    NULL = "NULL"

    """
    A map collection type
    """
    MAP = "MAP"

    """
    An array collection type
    """
    ARRAY = "ARRAY"

    """
    An union type
    """
    UNION = "ARRAY"

    """
    An complex struct type
    """
    STRUCT = "STRUCT"


class SchemaAssertionField(PermissiveBaseModel):
    """Attributes defining a Schema Assertion Field."""

    path: str

    type: SchemaFieldDataType

    nativeType: Optional[str] = None


class SchemaAssertion(PermissiveBaseModel):
    """Attributes defining a Schema Assertion."""

    compatibility: SchemaAssertionCompatibility

    fields: List[SchemaAssertionField] = Field(alias="fields")


class RawAspect(PermissiveBaseModel):
    """Payload representing data about a single aspect"""

    # The name of the aspect
    aspectName: str

    # JSON string containing the aspect's payload
    payload: str


class AssertionEntity(PermissiveBaseModel):
    """A unique identifier for the assertee (e.g. dataset urn). This represents the unique coordinates inside the data platform"""

    urn: str

    # A unique identifier for the platform urn
    platform_urn: str = Field(alias="platformUrn")

    # Platform instance id
    platform_instance: Optional[str] = Field(alias="platformInstance", default=None)

    # A list of sub-types for the entity, inside the platform
    sub_types: Optional[List[str]] = Field(alias="subTypes", default=None)

    # The entity/dataset's shortname
    table_name: Optional[str] = None

    # The entity/dataset's fully qualified name
    qualified_name: Optional[str] = Field(alias="qualifiedName", default=None)

    # Whether the entity exists (is soft deleted or not)
    exists: Optional[bool] = None


class PartitionKeyFieldSpec(PermissiveBaseModel):
    """Unique id for the partition key field"""

    id: str

    # The name of the source field feeding into the partition
    source_field_name: str

    # The transform to apply to the source field to compute the partition key field
    source_field_transform: Optional[PartitionKeyFieldTransform] = None

    def __init__(
        self,
        id: str,
        source_field_name: str,
        source_field_transform: Optional[PartitionKeyFieldTransform],
    ):
        self.id = id
        self.source_field_name = source_field_name
        self.source_field_transform = source_field_transform


class PartitionKeySpec(PermissiveBaseModel):
    """A specification of the fields that comprise a partition"""

    field_specs: List[PartitionKeyFieldSpec]

    def __init__(self, field_specs: List[PartitionKeyFieldSpec]):
        self.field_specs = field_specs


class PartitionKey(PermissiveBaseModel):
    """An identifier for a particular partition"""

    # Unique identifier for the partition, encoded.
    partition_id: str

    def __init__(self, partition_id: str):
        self.partition_id = partition_id


class PartitionSpec(PermissiveBaseModel):
    """A type of partition"""

    type: PartitionType

    # Raw encoded partition string
    partition: str

    # Definition of the fields of the partition
    partition_key_spec: PartitionKeySpec

    # A filter for a particular partition. If not provided, all partitions of the spec will be considered.
    partition_key: Optional[PartitionKey] = None

    def __init__(
        self,
        type: PartitionType,
        partition: str,
        partition_key_spec: PartitionKeySpec,
        partition_key: Optional[PartitionKey] = None,
    ):
        self.type = type
        self.partition = partition
        self.partition_key_spec = partition_key_spec
        self.partition_key = partition_key


class AssertionInfo(PermissiveBaseModel):
    """A unique identifier for the assertion"""

    # The type of the assertion
    type: AssertionType

    # An FRESHNESS Assertion Object
    freshness_assertion: Optional[FreshnessAssertion] = Field(
        alias="freshnessAssertion", default=None
    )

    # Volume Assertion Object
    volume_assertion: Optional[VolumeAssertion] = Field(
        alias="volumeAssertion", default=None
    )

    # SQL Assertion Object
    sql_assertion: Optional[SQLAssertion] = Field(alias="sqlAssertion", default=None)

    # Field Assertion Object
    field_assertion: Optional[FieldAssertion] = Field(
        alias="fieldAssertion", default=None
    )

    # Schema Assertion Object
    schema_assertion: Optional[SchemaAssertion] = Field(
        alias="schemaAssertion", default=None
    )

    # How the assertion was sourced
    source_type: Optional[AssertionSourceType] = Field(alias="sourceType", default=None)

    # The time at which the assertion was initially created
    source_created_time: Optional[int] = Field(alias="sourceCreatedTime", default=None)

    # Description for the assertion
    description: Optional[str] = None

    @property
    def is_inferred(self) -> bool:
        return self.source_type == AssertionSourceType.INFERRED

    @property
    def is_volume_row_count_total_assertion(self) -> bool:
        return (
            self.type == AssertionType.VOLUME
            and self.volume_assertion is not None
            and self.volume_assertion.type == VolumeAssertionType.ROW_COUNT_TOTAL
        )

    @property
    def is_field_metric_assertion(self) -> bool:
        return (
            self.type == AssertionType.FIELD
            and self.field_assertion is not None
            and self.field_assertion.type == FieldAssertionType.FIELD_METRIC
        )

    @model_validator(mode="before")
    @classmethod
    def extract_assertion_info(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        if "info" in values and "type" in values["info"]:
            values["type"] = values["info"]["type"]

        if (
            "info" in values
            and "source" in values["info"]
            and values["info"]["source"] is not None
        ):
            if "type" in values["info"]["source"]:
                values["sourceType"] = values["info"]["source"]["type"]
            if (
                "created" in values["info"]["source"]
                and values["info"]["source"]["created"] is not None
            ):
                values["sourceCreatedTime"] = values["info"]["source"]["created"][
                    "time"
                ]
        elif "source" in values and values["source"] is not None:
            if "type" in values["source"]:
                values["sourceType"] = values["source"]["type"]
            if (
                "created" in values["source"]
                and values["source"]["created"] is not None
            ):
                values["sourceCreatedTime"] = values["source"]["created"]["time"]

        if (
            "freshnessAssertion" not in values
            and "info" in values
            and "freshnessAssertion" in values["info"]
        ):
            values["freshnessAssertion"] = values["info"]["freshnessAssertion"]

        if (
            "volumeAssertion" not in values
            and "info" in values
            and "volumeAssertion" in values["info"]
        ):
            values["volumeAssertion"] = values["info"]["volumeAssertion"]

        if (
            "sqlAssertion" not in values
            and "info" in values
            and "sqlAssertion" in values["info"]
        ):
            values["sqlAssertion"] = values["info"]["sqlAssertion"]

        if (
            "fieldAssertion" not in values
            and "info" in values
            and "fieldAssertion" in values["info"]
        ):
            values["fieldAssertion"] = values["info"]["fieldAssertion"]

        if (
            "schemaAssertion" not in values
            and "info" in values
            and "schemaAssertion" in values["info"]
        ):
            values["schemaAssertion"] = values["info"]["schemaAssertion"]

        return values


class AssertionAdjustmentAlgorithm(Enum):
    CUSTOM = "CUSTOM"


class AssertionExclusionWindowType(str, Enum):
    """
    Enum representing the type of exclusion window.
    """

    FIXED_RANGE = "FIXED_RANGE"
    WEEKLY = "WEEKLY"
    HOLIDAY = "HOLIDAY"


class DayOfWeek(str, Enum):
    """
    Enum representing days of the week.
    """

    MONDAY = "MONDAY"
    TUESDAY = "TUESDAY"
    WEDNESDAY = "WEDNESDAY"
    THURSDAY = "THURSDAY"
    FRIDAY = "FRIDAY"
    SATURDAY = "SATURDAY"
    SUNDAY = "SUNDAY"


class WeeklyWindow(PermissiveBaseModel):
    """
    Represents a recurring time window that repeats weekly.
    """

    days_of_week: Optional[List[DayOfWeek]] = Field(default=None, alias="daysOfWeek")
    start_time: Optional[str] = Field(
        default="00:00", alias="startTime"
    )  # Default start of day
    end_time: Optional[str] = Field(
        default="23:59", alias="endTime"
    )  # Default end of day
    timezone: Optional[str] = Field(default="UTC", alias="timezone")  # Default to UTC


class HolidayWindow(PermissiveBaseModel):
    """
    Represents an exclusion window based on a holiday.
    """

    name: str
    region: Optional[str] = None
    timezone: Optional[str] = None


class AbsoluteTimeWindow(PermissiveBaseModel):
    """
    Represents a fixed time range for an exclusion window.
    """

    startTimeMillis: int
    endTimeMillis: int

    @field_validator("startTimeMillis", "endTimeMillis", mode="before")
    @classmethod
    def coerce_datetime_objects(cls, v: Any) -> int:
        # This exists purely to make it easier to pass datetime objects / strings in when testing.
        if not isinstance(v, int):
            dt = pydantic.TypeAdapter(datetime).validate_python(v)
            assert dt.tzinfo is not None, "datetime must be timezone-aware"
            return int(dt.timestamp() * 1000)
        return v


class AssertionExclusionWindow(PermissiveBaseModel):
    """
    Information about an assertion exclusion window.
    Used to exclude specific time periods from assertion evaluation or training.
    """

    type: AssertionExclusionWindowType
    display_name: Optional[str] = Field(default=None, alias="displayName")
    fixed_range: Optional[AbsoluteTimeWindow] = Field(default=None, alias="fixedRange")
    weekly: Optional[WeeklyWindow] = Field(default=None, alias="weekly")
    holiday: Optional[HolidayWindow] = None


class AssertionMonitorSensitivity(PermissiveBaseModel):
    level: int


class AssertionAdjustmentSettings(PermissiveBaseModel):
    """
    A set of settings that can be used to adjust assertion values
    This is mainly applied against inferred assertions
    """

    algorithm: Optional[AssertionAdjustmentAlgorithm] = None
    algorithmName: Optional[str] = None
    context: Optional[Dict[str, str]] = None
    exclusion_windows: Optional[List[AssertionExclusionWindow]] = Field(
        alias="exclusionWindows", default=None
    )
    training_data_lookback_window_days: Optional[int] = Field(
        alias="trainingDataLookbackWindowDays", default=None
    )
    sensitivity: Optional[AssertionMonitorSensitivity] = None


class Assertion(AssertionInfo):
    # A unique identifier for the assertion
    urn: str

    # The entity being asserted on
    entity: AssertionEntity

    # The urn of the connection required to evaluate the assertion. If there is no connection urn we are limited in terms of what we can do
    connection_urn: Optional[str] = Field(alias="connectionUrn", default=None)

    # We need "AssertionInfo" aspect in its original form when creating AssertionRunEvent aspect.
    # Ideally, we can write a complete mapper from "Assertion" type here to "AssertionInfo" aspect
    # that covers all existing assertion types (freshness, volume, custom, field, etc)
    # in `aspect_builder.py` and raw_info_aspect will not be needed, however at this point,
    # that seems more maintenance-heavy as compared to this workaround. In future, we may revisit this.

    # raw assertionInfo aspect
    raw_info_aspect: Optional[RawAspect] = None

    # It's not possible to define circular dependency due to how python parsing works, so
    # if provided, monitor is stored as json string and then parsed into pydantic model separately
    monitor: Optional[Dict] = None

    @model_validator(mode="before")
    @classmethod
    def extract_assertion(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        # Attempt to extract the entity field from the "relationships"
        # response object provided by the GraphQL API.
        if (
            "connectionUrn" not in values
            and "entity" in values
            and "platformUrn" in values["entity"]
        ):
            platform_urn = values["entity"]["platformUrn"]
            values["connectionUrn"] = platform_urn

        if (
            "rawInfoAspect" in values
            and values["rawInfoAspect"]
            and values["rawInfoAspect"][0]["aspectName"] == "assertionInfo"
        ):
            values["raw_info_aspect"] = values["rawInfoAspect"][0]

        return values


class AssertionEvaluationParameters(PermissiveBaseModel):
    # The type of the parameters"""
    type: AssertionEvaluationParametersType

    # Dataset FRESHNESS Parameters. Present if the type is DATASET_FRESHNESS
    dataset_freshness_parameters: Optional[DatasetFreshnessAssertionParameters] = Field(
        alias="datasetFreshnessParameters", default=None
    )

    # Dataset VOLUME Parameters. Present if the type is DATASET_VOLUME
    dataset_volume_parameters: Optional[DatasetVolumeAssertionParameters] = Field(
        alias="datasetVolumeParameters", default=None
    )

    # Dataset FIELD Parameters. Present if the type is DATASET_FIELD
    dataset_field_parameters: Optional[DatasetFieldAssertionParameters] = Field(
        alias="datasetFieldParameters", default=None
    )

    # Dataset DATA_SCHEMA Parameters. Present if the type is DATASET_SCHEMA
    dataset_schema_parameters: Optional[DatasetSchemaAssertionParameters] = Field(
        alias="datasetSchemaParameters", default=None
    )


class EvaluationTimeWindow(PermissiveBaseModel):
    # The starting time of the assertion validity window.
    start_time_millis: int = Field(alias="startTimeMillis")

    # The ending time of the assertion validity window.
    end_time_millis: int = Field(alias="endTimeMillis")


class EmbeddedAssertion(PermissiveBaseModel):
    # Spec for assertion to evaluate
    assertion: AssertionInfo

    # Validity period of the assertion, ie the window of time where it should be used in evaluation
    evaluation_time_window: Optional[EvaluationTimeWindow] = Field(
        alias="evaluationTimeWindow", default=None
    )

    # JSON string containing AssertionInfo GMS aspect's payload
    raw_assertion: str = Field(alias="rawAssertion")


class AssertionInferenceDetails(PermissiveBaseModel):
    # The time at which an inference was last generated.
    # This is very important - if an assertion cannot be generated for any reason,
    # this field will be set to null.
    #
    # This is used to determine whether we have generated a valid prediction for
    # a given smart assertion.
    generated_at: Optional[int] = Field(alias="generatedAt", default=None)


class AssertionEvaluationSpecContext(PermissiveBaseModel):
    # Currently used for Smart Assertions:
    # An embedded copy of the assertion used to evaluate which will overwrite the referenced assertion
    # if present and if the EmbeddedAssertion's evaluationTimeWindow period is valid
    embedded_assertions: Optional[List[EmbeddedAssertion]] = Field(
        alias="embeddedAssertions", default=None
    )

    # Deprecated: A legacy way to adjust the assertion using a standard deviation calculated offline.
    std_dev: Optional[float] = Field(alias="stdDev", default=None)

    # Currently used for Smart Assertion: Details about the last inference generated for a smart assertion.
    inference_details: Optional[AssertionInferenceDetails] = Field(
        alias="inferenceDetails", default=None
    )

    @property
    def has_embedded_assertions(self) -> bool:
        return (
            self.embedded_assertions is not None and len(self.embedded_assertions) > 0
        )

    @property
    def has_stddev(self) -> bool:
        return self.std_dev is not None


class AssertionEvaluationSpec(PermissiveBaseModel):
    # The assertion to be evaluated
    assertion: Assertion

    # The schedule on which to evaluate the assertions
    schedule: CronSchedule

    # The parameters required to evaluate an assertion
    parameters: AssertionEvaluationParameters

    # Additional context about assertion being evaluated.
    context: Optional[AssertionEvaluationSpecContext] = None

    # JSON string containing payload of AssertionEvaluationParameters GMS model
    raw_parameters: Optional[str] = Field(alias="rawParameters", default=None)


class AssertionMonitorSettings(PermissiveBaseModel):
    """Assertion Monitor Settings"""

    inference_settings: Optional[AssertionAdjustmentSettings] = Field(
        alias="inferenceSettings", default=None
    )


class AssertionMonitorMetricsCubeBootstrapState(str, Enum):
    """The state of the bootstrap process for an assertion."""

    PENDING = "PENDING"
    FAILED = "FAILED"
    COMPLETED = "COMPLETED"


class AssertionMonitorMetricsCubeBootstrapStatus(PermissiveBaseModel):
    """The status of the bootstrap process for an assertion."""

    state: AssertionMonitorMetricsCubeBootstrapState

    message: Optional[str] = None

    @model_validator(mode="before")
    @classmethod
    def extract_info(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        if "state" in values:
            values["state"] = AssertionMonitorMetricsCubeBootstrapState(values["state"])
        return values


class AssertionMonitorBootstrapStatus(PermissiveBaseModel):
    """The status of the bootstrap process for an assertion."""

    metrics_cube_bootstrap_status: Optional[
        AssertionMonitorMetricsCubeBootstrapStatus
    ] = Field(alias="metricsCubeBootstrapStatus", default=None)


class AssertionMonitor(PermissiveBaseModel):
    """A monitor that evaluates assertions"""

    assertions: List[AssertionEvaluationSpec]

    settings: Optional[AssertionMonitorSettings] = None

    bootstrap_status: Optional[AssertionMonitorBootstrapStatus] = Field(
        alias="bootstrapStatus", default=None
    )


class Monitor(PermissiveBaseModel):
    """An asset monitor"""

    urn: str

    type: MonitorType

    assertion_monitor: Optional[AssertionMonitor] = None

    mode: MonitorMode

    executor_id: Optional[str] = Field(alias="executorId", default=None)

    @model_validator(mode="before")
    @classmethod
    def extract_info(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        graphql_entity = (
            values["entity"] if "entity" in values else None
        )  # dataset entity associated with the monitor
        entity = None
        if graphql_entity:
            platform_urn = graphql_entity["platform"]["urn"]
            entity_urn = graphql_entity["urn"]
            exists = graphql_entity["exists"] if "exists" in graphql_entity else None

            table_name = (
                graphql_entity["properties"]["name"]
                if "properties" in graphql_entity
                and graphql_entity["properties"] is not None
                and "name" in graphql_entity["properties"]
                else None
            )
            qualified_name = (
                graphql_entity["properties"]["qualifiedName"]
                if "properties" in graphql_entity
                and graphql_entity["properties"] is not None
                and "qualifiedName" in graphql_entity["properties"]
                else None
            )

            sub_types = (
                graphql_entity["subTypes"]["typeNames"]
                if "subTypes" in graphql_entity
                and graphql_entity["subTypes"] is not None
                and "typeNames" in graphql_entity["subTypes"]
                else []
            )

            entity = {
                "urn": entity_urn,
                "platformUrn": platform_urn,
                "platformInstance": None,
                "subTypes": sub_types,
                "table_name": table_name,
                "qualified_name": qualified_name,
                "exists": exists,
            }

        if "info" in values:
            if "type" in values["info"]:
                values["type"] = values["info"]["type"]
            if "executorId" in values["info"]:
                values["executor_id"] = values["info"]["executorId"]
            if "assertionMonitor" in values["info"]:
                values["assertion_monitor"] = values["info"]["assertionMonitor"]

                assertions = values["assertion_monitor"]["assertions"]

                # These are a bit of HACKs: Copy the entity for the assertion down lower based on the monitor assertion!
                # This is done because we do need the entity at the monitor level AND the assertion level.
                # Both monitors & assertions do point to a single related entity, so for now this is okay.
                for assertion in assertions:
                    assertion["assertion"]["entity"] = entity

            if "status" in values["info"] and "mode" in values["info"]["status"]:
                values["mode"] = values["info"]["status"]["mode"]
        return values


class AssertionEvaluationContext:
    """Context provided during an Assertion Evaluation"""

    dry_run: bool = False

    online_smart_assertions: Optional[bool] = False

    monitor_urn: Optional[str] = Field(alias="monitorUrn", default=None)

    evaluation_spec: Optional[AssertionEvaluationSpec] = None

    """
    Base assertionInfo aspect value before assertion adjustments (e.g. sensitivity transformer) are applied
    """
    base_assertion_info: Optional[RawAspect] = None

    def __init__(
        self,
        dry_run: bool = False,
        online_smart_assertions: Optional[bool] = False,
        monitor_urn: Optional[str] = None,
        monitor: Optional[AssertionMonitor] = None,
        assertion_evaluation_spec: Optional[AssertionEvaluationSpec] = None,
        base_assertion: Optional[RawAspect] = None,
    ):
        self.dry_run = dry_run
        self.online_smart_assertions = online_smart_assertions
        self.monitor_urn = monitor_urn
        self.monitor = monitor
        self.evaluation_spec = assertion_evaluation_spec
        self.base_assertion_info = base_assertion


class AssertionEvaluationResultError:
    """The error associated with an assertion evaluation result."""

    def __init__(
        self, type: AssertionResultErrorType, properties: Optional[Dict[str, str]]
    ):
        self.type = type
        self.properties = properties


class AssertionEvaluationResult:
    """The result of evaluating an assertion."""

    def __init__(
        self,
        type: AssertionResultType,
        parameters: Optional[dict] = None,
        error: Optional[AssertionEvaluationResultError] = None,
        metric: Optional[Metric] = None,
    ):
        self.type = type
        self.parameters = parameters
        self.error = error
        self.metric = metric


class ConnectionDetails:
    _dict: Dict

    def __init__(self, _dict: Dict):
        self._dict = _dict


class EntityEvent:
    # The type of the event
    event_type: EntityEventType

    # The timestamp associated with the event
    event_time: int

    def __init__(self, event_type: EntityEventType, event_time: int):
        self.event_type = event_type
        self.event_time = event_time


class CloudLoggingConfig(PermissiveBaseModel):
    """Config for cloud logging"""

    s3_bucket: str
    s3_prefix: str
    remote_executor_logging_enabled: bool


class ExecutorConfig(PermissiveBaseModel):
    region: str
    executor_id: str = Field(alias="executorId")
    queue_url: str = Field(alias="queueUrl")
    access_key: str = Field(alias="accessKeyId")
    secret_key: str = Field(alias="secretKeyId")
    session_token: str = Field(alias="sessionToken")
    expiration: Optional[datetime] = None


class ExecutionRequestSchedule:
    execution_request: ExecutionRequest
    schedule: CronSchedule

    def __init__(
        self,
        execution_request: ExecutionRequest,
        schedule: CronSchedule,
    ):
        self.execution_request = execution_request
        self.schedule = schedule


class FetcherMode(Enum):
    """Mode of execution for a fetcher"""

    DEFAULT = "DEFAULT"
    REMOTE = "REMOTE"


class FetcherConfig(PermissiveBaseModel):
    """Config required to create a fetcher"""

    id: str
    enabled: bool = True
    mode: FetcherMode = FetcherMode.DEFAULT
    executor_ids: Optional[List[str]] = None
    refresh_interval: int


class ExecutionRequestStatus(PermissiveBaseModel):
    execution_request_id: str
    execution_request_urn: str
    executor_id: str

    ingestion_source_urn: str
    raw_input_aspect: Dict
    raw_signal_aspect: Dict

    last_observed: int
    status: str
    report: str

    start_time: int
    request_time: int


class SweeperAction(PermissiveBaseModel):
    action: str
    description: str
    args: Dict[str, Any]
    errors_fatal: bool = False


class Anomaly(PermissiveBaseModel):
    """
    Model representing a metric or operation anomaly.
    """

    timestamp_ms: int
    metric: Optional[Metric] = None

    def timestamp(self) -> datetime:
        """Convert timestamp_ms to a datetime object."""
        return datetime.fromtimestamp(self.timestamp_ms / 1000, timezone.utc)

    def __repr__(self) -> str:
        return f"Anomaly(timestamp_ms={self.timestamp_ms}, metric={self.metric})"
