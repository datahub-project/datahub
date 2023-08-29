import logging
from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import Field, root_validator

from datahub_monitors.common.config import PermissiveBaseModel

logger = logging.getLogger(__name__)

# The following types are used bound from
# the GraphQL API response objects.
# We allow Pydantic to handle most of the mapping for us.


class AssertionStdOperator(Enum):
    GREATER_THAN = "GREATER_THAN"
    GREATER_THAN_OR_EQUAL_TO = "GREATER_THAN_OR_EQUAL_TO"
    EQUAL_TO = "EQUAL_TO"
    LESS_THAN = "LESS_THAN"
    LESS_THAN_OR_EQUAL_TO = "LESS_THAN_OR_EQUAL_TO"
    BETWEEN = "BETWEEN"


class AssertionStdParameterType(Enum):
    NUMBER = "NUMBER"


class MonitorType(Enum):
    """Enumeration of monitor types."""

    ASSERTION = "ASSERTION"


class AssertionType(Enum):
    """Enumeration of assertion types."""

    DATASET = "DATASET"
    FRESHNESS = "FRESHNESS"
    VOLUME = "VOLUME"


class AssertionResultErrorType(Enum):
    """Enumeration of assertion result error types"""

    SOURCE_CONNECTION_ERROR = "SOURCE_CONNECTION_ERROR"
    SOURCE_QUERY_FAILED = "SOURCE_QUERY_FAILED"
    INSUFFICIENT_DATA = "INSUFFICIENT_DATA"
    INVALID_PARAMETERS = "INVALID_PARAMETERS"
    INVALID_SOURCE_TYPE = "INVALID_SOURCE_TYPE"
    UNSUPPORTED_PLATFORM = "UNSUPPORTED_PLATFORM"
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


class FreshnessAssertionScheduleType(Enum):
    """Enumeration of freshness assertion schedule types."""

    CRON = "CRON"
    FIXED_INTERVAL = "FIXED_INTERVAL"


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


class EntityEventType(Enum):
    """Enumeration of Entity Events that we support retrieving using a particular connection"""

    # An update has been performed to some rows based on the value of a particular field.
    FIELD_UPDATE = "FIELD_UPDATE"

    # An update has been performed to the table, based on a dataset last updated statistic maintained by the source system
    INFORMATION_SCHEMA_UPDATE = "INFORMATION_SCHEMA_UPDATE"

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


class FreshnessFieldKind(Enum):
    LAST_MODIFIED = "LAST_MODIFIED"
    HIGH_WATERMARK = "HIGH_WATERMARK"


class MonitorMode(Enum):
    ACTIVE = "ACTIVE"
    INACTIVE = "INACTIVE"


class AssertionValueChangeType(Enum):
    ABSOLUTE = "ABSOLUTE"
    PERCENTAGE = "PERCENTAGE"


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
    window_start_offset_ms: Optional[int] = Field(alias="windowStartOffsetMs")


class FixedIntervalSchedule(PermissiveBaseModel):
    """The unit of the fixed interval schedule"""

    unit: CalendarInterval

    multiple: int


class FreshnessAssertionSchedule(PermissiveBaseModel):
    """The type of the schedule"""

    type: FreshnessAssertionScheduleType

    cron: Optional[FreshnessCronSchedule] = None

    fixed_interval: Optional[FixedIntervalSchedule] = Field(alias="fixedInterval")


class SchemaFieldSpec(PermissiveBaseModel):
    """The schema field urn"""

    # The field path of the schema field"""
    path: str

    # The std DataHub type of the field
    type: str

    # The native type of the field collected from source
    native_type: Optional[str] = Field(alias="nativeType")

    kind: Optional[FreshnessFieldKind]


class AuditLogSpec(PermissiveBaseModel):
    """The type of operation. If not provided all operations will be considered."""

    operation_types: Optional[List[str]] = Field(alias="operationTypes")

    user_name: Optional[str] = Field(alias="userName")


class DataHubOperationSpec(PermissiveBaseModel):
    """Information about the DataHub Operation aspect used to evaluate an assertion"""

    # The list of operation types that should be monitored. If not provided, a default set will be used.
    operation_types: Optional[List[str]] = Field(alias="operationTypes")

    # The list of custom operation types that should be monitored. If not provided, no custom operation types will be used.
    custom_operation_types: Optional[List[str]] = Field(alias="customOperationTypes")


class DatasetFreshnessAssertionParameters(PermissiveBaseModel):
    """The type of the freshness signal"""

    source_type: DatasetFreshnessSourceType = Field(alias="sourceType")

    # A descriptor for a Dataset Field to use. Present when source_type is FIELD_LAST_UPDATED
    field: Optional[SchemaFieldSpec] = None

    # A descriptor for a Dataset Column to use. Present when source_type is AUDIT_LOG_OPERATION
    audit_log: Optional[AuditLogSpec] = Field(alias="auditLog")

    # A descriptor for a DataHub operation to use. Present when source_type is DATAHUB_OPERATION
    datahub_operation: Optional[DataHubOperationSpec] = Field(alias="dataHubOperation")


class DatasetVolumeAssertionParameters(PermissiveBaseModel):
    source_type: DatasetVolumeSourceType = Field(alias="sourceType")


class FreshnessAssertion(PermissiveBaseModel):
    """The type of the FRESHNESS Assertion"""

    type: FreshnessAssertionType

    schedule: FreshnessAssertionSchedule

    filter: Optional[DatasetFilter] = None


class AssertionStdParameter(PermissiveBaseModel):
    # The parameter value
    value: str

    # The type of the parameter
    type: AssertionStdParameterType


class AssertionStdParameters(PermissiveBaseModel):
    # The value parameter of an assertion
    value: Optional[AssertionStdParameter]

    # The maxValue parameter of an assertion
    max_value: Optional[AssertionStdParameter] = Field(alias="maxValue")

    # The minValue parameter of an assertion
    min_value: Optional[AssertionStdParameter] = Field(alias="minValue")


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
    native_type: Optional[str] = Field(alias="nativeType")


class IncrementingSegmentSpec(PermissiveBaseModel):
    """Core attributes required to identify an incrementing segment in a table."""

    # The field to use to generate segments. It must be constantly incrementing as new rows are inserted.
    field: SchemaFieldSpec

    # Optional transformer function to apply to the field in order to obtain the final segment or bucket identifier.
    transformer: Optional[IncrementingSegmentFieldTransformer]


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

    row_count_total: Optional[RowCountTotal] = Field(alias="rowCountTotal")

    row_count_change: Optional[RowCountChange] = Field(alias="rowCountChange")

    incrementing_row_count_total: Optional[IncrementingSegmentRowCountTotal] = Field(
        alias="incrementingSegmentRowCountTotal"
    )

    incrementing_row_count_change: Optional[IncrementingSegmentRowCountChange] = Field(
        alias="incrementingSegmentRowCountChange"
    )

    filter: Optional[DatasetFilter] = None


class AssertionEntity(PermissiveBaseModel):
    """A unique identifier for the assertee (e.g. dataset urn). This represents the unique coordinates inside the data platform"""

    urn: str

    # A unique identifier for the platform urn
    platform_urn: str = Field(alias="platformUrn")

    # Platform instance id
    platform_instance: Optional[str] = Field(alias="platformInstance")

    # A list of sub-types for the entity, inside the platform
    sub_types: Optional[List[str]] = Field(alias="subTypes")

    # The entity/dataset's shortname
    table_name: Optional[str]

    # The entity/dataset's fully qualified name
    qualified_name: Optional[str] = Field(alias="qualifiedName")


class PartitionKeyFieldSpec(PermissiveBaseModel):
    """Unique id for the partition key field"""

    id: str

    # The name of the source field feeding into the partition
    source_field_name: str

    # The transform to apply to the source field to compute the partition key field
    source_field_transform: Optional[PartitionKeyFieldTransform]

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
    partition_key: Optional[PartitionKey]

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


class Assertion(PermissiveBaseModel):
    """A unique identifier for the assertion"""

    # A unique identifier for the assertion
    urn: str

    # The type of the assertion
    type: AssertionType

    # The subType of the assertion
    # sub_type: AssertionSubType = Field(alias="subType")

    # The entity being asserted on
    entity: AssertionEntity

    # The urn of the connection required to evaluate the assertion. If there is no connection urn we are limited in terms of what we can do
    connection_urn: Optional[str] = Field(alias="connectionUrn")

    # An FRESHNESS Assertion Object
    freshness_assertion: Optional[FreshnessAssertion] = Field(
        alias="freshnessAssertion"
    )

    # Volume Assertion Object
    volume_assertion: Optional[VolumeAssertion] = Field(alias="volumeAssertion")

    @root_validator(pre=True)
    def extract(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        # Attempt to extract the entity field from the "relationships"
        # response object provided by the GraphQL API.
        if "entity" not in values:
            graphql_entity = values["relationships"]["relationships"][0]["entity"]
            platform_urn = graphql_entity["platform"]["urn"]
            entity_urn = graphql_entity["urn"]

            table_name = (
                graphql_entity["properties"]["name"]
                if "properties" in graphql_entity
                and "name" in graphql_entity["properties"]
                else None
            )
            qualified_name = (
                graphql_entity["properties"]["qualifiedName"]
                if "properties" in graphql_entity
                and "qualifiedName" in graphql_entity["properties"]
                else None
            )

            graphql_entity["urn"]
            sub_types = (
                graphql_entity["subTypes"]["typeNames"]
                if "subTypes" in graphql_entity
                and graphql_entity["subTypes"] is not None
                and "typeNames" in graphql_entity["subTypes"]
                else []
            )

            values["entity"] = {
                "urn": entity_urn,
                "platformUrn": platform_urn,
                "platformInstance": None,
                "subTypes": sub_types,
                "table_name": table_name,
                "qualified_name": qualified_name,
            }

        if "connectionUrn" not in values:
            graphql_entity = values["relationships"]["relationships"][0]["entity"]
            platform_urn = graphql_entity["platform"]["urn"]
            values["connectionUrn"] = platform_urn

        if "info" in values and "type" in values["info"]:
            values["type"] = values["info"]["type"]

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

        return values


class AssertionEvaluationParameters(PermissiveBaseModel):
    # The type of the parameters"""
    type: AssertionEvaluationParametersType

    # Dataset FRESHNESS Parameters. Present if the type is DATASET_FRESHNESS
    dataset_freshness_parameters: Optional[DatasetFreshnessAssertionParameters] = Field(
        alias="datasetFreshnessParameters"
    )

    # Dataset VOLUME Parameters. Present if the type is DATASET_VOLUME
    dataset_volume_parameters: Optional[DatasetVolumeAssertionParameters] = Field(
        alias="datasetVolumeParameters"
    )


class AssertionEvaluationSpec(PermissiveBaseModel):
    # The assertion to be evaluated
    assertion: Assertion

    # The schedule on which to evaluate the assertions
    schedule: CronSchedule

    # The parameters required to evaluate an assertion
    parameters: Optional[AssertionEvaluationParameters] = None


class AssertionMonitor(PermissiveBaseModel):
    """A monitor that evaluates assertions"""

    assertions: List[AssertionEvaluationSpec]


class Monitor(PermissiveBaseModel):
    """An asset monitor"""

    urn: str

    type: MonitorType

    assertion_monitor: Optional[AssertionMonitor]

    mode: MonitorMode

    @root_validator(pre=True)
    def extract_info(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        if "info" in values:
            if "type" in values["info"]:
                values["type"] = values["info"]["type"]
            if "assertionMonitor" in values["info"]:
                values["assertion_monitor"] = values["info"]["assertionMonitor"]
            if "status" in values["info"] and "mode" in values["info"]["status"]:
                values["mode"] = values["info"]["status"]["mode"]
        return values


class AssertionEvaluationContext:
    """Context provided during an Assertion Evaluation"""

    dry_run: bool = False

    monitor_urn: Optional[str] = Field(alias="monitorUrn")

    def __init__(self, dry_run: bool = False, monitor_urn: Optional[str] = None):
        self.dry_run = dry_run
        self.monitor_urn = monitor_urn


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
    ):
        self.type = type
        self.parameters = parameters
        self.error = error


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
