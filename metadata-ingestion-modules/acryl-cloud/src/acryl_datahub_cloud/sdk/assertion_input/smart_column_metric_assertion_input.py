import json
from datetime import datetime
from typing import Optional, Union

from acryl_datahub_cloud.sdk.assertion_input.assertion_input import (
    DEFAULT_EVERY_SIX_HOURS_SCHEDULE,
    HIGH_WATERMARK_ALLOWED_FIELD_TYPES,
    NO_PARAMETER_OPERATORS,
    RANGE_OPERATORS,
    SINGLE_VALUE_OPERATORS,
    AssertionIncidentBehavior,
    AssertionInfoInputType,
    DetectionMechanismInputTypes,
    ExclusionWindowInputTypes,
    FieldSpecType,
    InferenceSensitivity,
    _AllRowsQuery,
    _AllRowsQueryDataHubDatasetProfile,
    _AssertionInput,
    _ChangedRowsQuery,
    _DatasetProfile,
    _HasSmartAssertionInputs,
    _try_parse_and_validate_schema_classes_enum,
)
from acryl_datahub_cloud.sdk.entities.assertion import TagsInputType
from acryl_datahub_cloud.sdk.errors import (
    SDKNotYetSupportedError,
    SDKUsageError,
)
from datahub.emitter.enum_helpers import get_enum_options
from datahub.metadata import schema_classes as models
from datahub.metadata.urns import AssertionUrn, CorpUserUrn, DatasetUrn
from datahub.sdk.entity_client import EntityClient

# Keep this in sync with the frontend in getEligibleFieldColumns
# datahub-web-react/src/app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/steps/field/utils.ts
ALLOWED_COLUMN_TYPES_FOR_SMART_COLUMN_METRIC_ASSERTION = [
    models.StringTypeClass(),
    models.NumberTypeClass(),
    models.BooleanTypeClass(),
    models.DateTypeClass(),
    models.TimeTypeClass(),
    models.NullTypeClass(),
]

# Keep this in sync with FIELD_VALUES_OPERATOR_CONFIG in the frontend
# datahub-web-react/src/app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/steps/field/utils.ts
FIELD_VALUES_OPERATOR_CONFIG = {
    "StringTypeClass": [
        models.AssertionStdOperatorClass.NULL,
        models.AssertionStdOperatorClass.NOT_NULL,
        models.AssertionStdOperatorClass.EQUAL_TO,
        models.AssertionStdOperatorClass.IN,
        models.AssertionStdOperatorClass.GREATER_THAN_OR_EQUAL_TO,
        models.AssertionStdOperatorClass.REGEX_MATCH,
        models.AssertionStdOperatorClass.GREATER_THAN,
        models.AssertionStdOperatorClass.LESS_THAN,
        models.AssertionStdOperatorClass.BETWEEN,
    ],
    "NumberTypeClass": [
        models.AssertionStdOperatorClass.GREATER_THAN,
        models.AssertionStdOperatorClass.LESS_THAN,
        models.AssertionStdOperatorClass.BETWEEN,
        models.AssertionStdOperatorClass.NULL,
        models.AssertionStdOperatorClass.NOT_NULL,
        models.AssertionStdOperatorClass.EQUAL_TO,
        models.AssertionStdOperatorClass.IN,
        models.AssertionStdOperatorClass.GREATER_THAN_OR_EQUAL_TO,
        models.AssertionStdOperatorClass.NOT_EQUAL_TO,
    ],
    "BooleanTypeClass": [
        models.AssertionStdOperatorClass.IS_TRUE,
        models.AssertionStdOperatorClass.IS_FALSE,
        models.AssertionStdOperatorClass.NULL,
        models.AssertionStdOperatorClass.NOT_NULL,
    ],
    "DateTypeClass": [
        models.AssertionStdOperatorClass.NULL,
        models.AssertionStdOperatorClass.NOT_NULL,
    ],
    "TimeTypeClass": [
        models.AssertionStdOperatorClass.NULL,
        models.AssertionStdOperatorClass.NOT_NULL,
    ],
    "NullTypeClass": [
        models.AssertionStdOperatorClass.NULL,
        models.AssertionStdOperatorClass.NOT_NULL,
    ],
}

# Keep this in sync with FIELD_METRIC_TYPE_CONFIG in the frontend
# datahub-web-react/src/app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/steps/field/utils.ts
FIELD_METRIC_TYPE_CONFIG = {
    "StringTypeClass": [
        models.FieldMetricTypeClass.NULL_COUNT,
        models.FieldMetricTypeClass.NULL_PERCENTAGE,
        models.FieldMetricTypeClass.UNIQUE_COUNT,
        models.FieldMetricTypeClass.UNIQUE_PERCENTAGE,
        models.FieldMetricTypeClass.MAX_LENGTH,
        models.FieldMetricTypeClass.MIN_LENGTH,
        models.FieldMetricTypeClass.EMPTY_COUNT,
        models.FieldMetricTypeClass.EMPTY_PERCENTAGE,
    ],
    "NumberTypeClass": [
        models.FieldMetricTypeClass.NULL_COUNT,
        models.FieldMetricTypeClass.NULL_PERCENTAGE,
        models.FieldMetricTypeClass.UNIQUE_COUNT,
        models.FieldMetricTypeClass.UNIQUE_PERCENTAGE,
        models.FieldMetricTypeClass.MAX,
        models.FieldMetricTypeClass.MIN,
        models.FieldMetricTypeClass.MEAN,
        models.FieldMetricTypeClass.MEDIAN,
        models.FieldMetricTypeClass.STDDEV,
        models.FieldMetricTypeClass.NEGATIVE_COUNT,
        models.FieldMetricTypeClass.NEGATIVE_PERCENTAGE,
        models.FieldMetricTypeClass.ZERO_COUNT,
        models.FieldMetricTypeClass.ZERO_PERCENTAGE,
    ],
    "BooleanTypeClass": [
        models.FieldMetricTypeClass.NULL_COUNT,
        models.FieldMetricTypeClass.NULL_PERCENTAGE,
        models.FieldMetricTypeClass.UNIQUE_COUNT,
        models.FieldMetricTypeClass.UNIQUE_PERCENTAGE,
    ],
    "DateTypeClass": [
        models.FieldMetricTypeClass.NULL_COUNT,
        models.FieldMetricTypeClass.NULL_PERCENTAGE,
        models.FieldMetricTypeClass.UNIQUE_COUNT,
        models.FieldMetricTypeClass.UNIQUE_PERCENTAGE,
    ],
    "TimeTypeClass": [
        models.FieldMetricTypeClass.NULL_COUNT,
        models.FieldMetricTypeClass.NULL_PERCENTAGE,
        models.FieldMetricTypeClass.UNIQUE_COUNT,
        models.FieldMetricTypeClass.UNIQUE_PERCENTAGE,
    ],
    "NullTypeClass": [
        models.FieldMetricTypeClass.NULL_COUNT,
        models.FieldMetricTypeClass.NULL_PERCENTAGE,
        models.FieldMetricTypeClass.UNIQUE_COUNT,
        models.FieldMetricTypeClass.UNIQUE_PERCENTAGE,
    ],
}


MetricInputType = Union[models.FieldMetricTypeClass, str]
ValueInputType = Union[str, int, float]
ValueTypeInputType = Union[str, models.AssertionStdParameterTypeClass]
RangeInputType = tuple[ValueInputType, ValueInputType]
RangeTypeInputType = Union[
    str,
    tuple[str, str],
    ValueTypeInputType,
    tuple[ValueTypeInputType, ValueTypeInputType],
]
RangeTypeParsedType = tuple[ValueTypeInputType, ValueTypeInputType]
OperatorInputType = Union[str, models.AssertionStdOperatorClass]

DEFAULT_DETECTION_MECHANISM_SMART_COLUMN_METRIC_ASSERTION: _AllRowsQuery = (
    _AllRowsQuery()
)


class _SmartColumnMetricAssertionInput(_AssertionInput, _HasSmartAssertionInputs):
    """
    Input used to create a smart column metric assertion.

    This assertion is used to validate the value of a common field / column metric (e.g. aggregation) such as null count + percentage,
    min, max, median, and more. It uses AI to infer the assertion parameters.

    Example using the entity models, not comprehensive for all options:

    ```python
    models.AssertionInfoClass(
        type=models.AssertionTypeClass.FIELD,
        fieldAssertion=FieldAssertionInfoClass(
            type=models.FieldAssertionTypeClass.FIELD_METRIC,
            entity=str(self.dataset_urn),
            filter=DatasetFilterClass(
                type=models.DatasetFilterTypeClass.SQL,
                sql="SELECT * FROM dataset WHERE column_name = 'value'",  # Example filter
            ),
            fieldMetricAssertion=FieldMetricAssertionClass(
                field=SchemaFieldSpecClass(
                    path="column_name",  # The column name to validate
                    type="string",  # The type of the column
                    nativeType="string",  # The native type of the column
                ),
                metric=models.FieldMetricTypeClass.NULL_COUNT_PERCENTAGE,  # The metric to validate
                operator=models.AssertionStdOperatorClass.GREATER_THAN,  # The operator to use
                parameters=models.AssertionStdParametersClass(
                    value=models.AssertionStdParameterClass(
                        value=10,  # The value to validate
                        type=models.AssertionStdParameterTypeClass.NUMBER,  # The type of the value
                    ),
                ),
            ),
        ),
        source=models.AssertionSourceClass(
            type=models.AssertionSourceTypeClass.INFERRED,  # Smart assertions are of type inferred, not native
            created=AuditStampClass(
                time=1717929600,
                actor="urn:li:corpuser:jdoe",  # The actor who created the assertion
            ),
        ),
        lastUpdated=AuditStampClass(
            time=1717929600,
            actor="urn:li:corpuser:jdoe",  # The actor who last updated the assertion
        ),
        description="This assertion validates the null count percentage of the column 'column_name' is greater than 10.",  # Optional description of the assertion
    )
    ```

    ```python
    models.MonitorInfoClass(
        type=models.MonitorTypeClass.ASSERTION,
        status=models.MonitorStatusClass(
            mode=models.MonitorModeClass.ACTIVE,  # Active or Inactive
        ),
        assertionMonitor=AssertionMonitorClass(
            assertions=AssertionEvaluationSpecClass(
                assertion="urn:li:assertion:123",  # The assertion to monitor
                schedule=models.CronScheduleClass(
                    cron="0 0 * * *",  # The cron schedule
                    timezone="America/New_York",  # The timezone
                ),
                parameters=models.AssertionEvaluationParametersClass(
                    type=models.AssertionEvaluationParametersTypeClass.DATASET_FIELD,
                    datasetFieldParameters=models.DatasetFieldAssertionParametersClass(
                        sourceType=models.DatasetFieldAssertionSourceTypeClass.CHANGED_ROWS_QUERY,  # This can be ALL_ROWS_QUERY, CHANGED_ROWS_QUERY or DATAHUB_DATASET_PROFILE
                        changedRowsField=models.FreshnessFieldSpecClass(
                            path="column_name",
                            type="string",
                            nativeType="string",
                            kind=models.FreshnessFieldKindClass.HIGH_WATERMARK,  # This can be LAST_MODIFIED or HIGH_WATERMARK
                        ),
                    ),
                ),
            ),
            settings=models.AssertionMonitorSettingsClass(
                adjustmentSettings=models.AssertionAdjustmentSettingsClass(
                    algorithm=models.AdjustmentAlgorithmClass.CUSTOM,  # TODO: Do we need to set this in the SDK?
                    algorithmName="stddev",  # TODO: Do we need to set this in the SDK? What are acceptable values?
                    context={
                        "stdDev": "1.0",  # TODO: Do we need to set this in the SDK? What are acceptable values?
                    },
                    exclusionWindows=[models.AssertionExclusionWindowClass(
                        type=models.AssertionExclusionWindowTypeClass.FIXED_RANGE,
                        start=1717929600,
                        end=1717929600,
                    )],
                    trainingDataLookbackWindowDays=10,  # The number of days to look back for training data
                    sensitivity=models.AssertionMonitorSensitivityClass(
                        level=1,  # The sensitivity level
                    ),
                ),
            ),
        ),
    )
    ```
    """

    def __init__(
        self,
        *,
        # Required parameters
        dataset_urn: Union[str, DatasetUrn],
        entity_client: EntityClient,
        column_name: str,
        metric_type: MetricInputType,
        operator: OperatorInputType,
        # Optional parameters
        value: Optional[ValueInputType] = None,
        value_type: Optional[ValueTypeInputType] = None,
        range: Optional[RangeInputType] = None,
        range_type: Optional[RangeTypeInputType] = None,
        urn: Optional[Union[str, AssertionUrn]] = None,
        display_name: Optional[str] = None,
        enabled: bool = True,
        schedule: Optional[Union[str, models.CronScheduleClass]] = None,
        detection_mechanism: DetectionMechanismInputTypes = None,
        sensitivity: Optional[Union[str, InferenceSensitivity]] = None,
        exclusion_windows: Optional[ExclusionWindowInputTypes] = None,
        training_data_lookback_days: Optional[int] = None,
        incident_behavior: Optional[
            Union[AssertionIncidentBehavior, list[AssertionIncidentBehavior]]
        ] = None,
        tags: Optional[TagsInputType] = None,
        created_by: Union[str, CorpUserUrn],
        created_at: datetime,
        updated_by: Union[str, CorpUserUrn],
        updated_at: datetime,
    ):
        """
        Initialize a smart column metric assertion input.

        Args:
            dataset_urn: The dataset urn.
            entity_client: The entity client.
            column_name: The name of the column to validate.
            metric_type: The metric type to validate.
            operator: The operator to use.
            value: The value to validate.
            value_type: The type of the value.
            range: The range to validate.
            range_type: The type of the range.  If single value, we assume the same type for start and end.
            urn: The urn of the assertion.
            display_name: The display name of the assertion.
            enabled: Whether the assertion is enabled.
            schedule: The schedule of the assertion.
            detection_mechanism: The detection mechanism of the assertion.
            sensitivity: The sensitivity of the assertion.
            exclusion_windows: The exclusion windows of the assertion.
            training_data_lookback_days: The training data lookback days of the assertion.
            incident_behavior: The incident behavior of the assertion.
            tags: The tags of the assertion.
            created_by: The creator of the assertion.
            created_at: The creation time of the assertion.
            updated_by: The updater of the assertion.
            updated_at: The update time of the assertion.
        """
        # Parent will handle validation of common parameters:
        _AssertionInput.__init__(
            self,
            dataset_urn=dataset_urn,
            entity_client=entity_client,
            urn=urn,
            display_name=display_name,
            enabled=enabled,
            schedule=schedule,
            detection_mechanism=detection_mechanism,
            incident_behavior=incident_behavior,
            tags=tags,
            source_type=models.AssertionSourceTypeClass.INFERRED,  # Smart assertions are of type inferred, not native
            created_by=created_by,
            created_at=created_at,
            updated_by=updated_by,
            updated_at=updated_at,
            default_detection_mechanism=DEFAULT_DETECTION_MECHANISM_SMART_COLUMN_METRIC_ASSERTION,
        )
        _HasSmartAssertionInputs.__init__(
            self,
            sensitivity=sensitivity,
            exclusion_windows=exclusion_windows,
            training_data_lookback_days=training_data_lookback_days,
        )

        # Validate Smart Column Metric Assertion specific parameters
        self.metric_type = _try_parse_and_validate_schema_classes_enum(
            metric_type, models.FieldMetricTypeClass
        )
        self.column_name = self._try_parse_and_validate_column_name_is_valid_type(
            column_name
        )
        self.operator = _try_parse_and_validate_schema_classes_enum(
            operator, models.AssertionStdOperatorClass
        )

        # Set type annotations for both raw input or parsed parameters
        self.value_type: Optional[ValueTypeInputType] = None
        self.value: Optional[ValueInputType] = None
        if _is_value_required_for_operator(self.operator):
            self.value_type = _try_parse_and_validate_value_type(value_type)
            self.value = _try_parse_and_validate_value(value, self.value_type)
        else:
            # Set these to what was input for later validation, and skip parsing and validation
            self.value_type = value_type
            self.value = value

        # Set type annotations for both raw input or parsed parameters
        self.range_type: Optional[Union[RangeTypeInputType, RangeTypeParsedType]] = None
        self.range: Optional[RangeInputType] = None
        if _is_range_required_for_operator(self.operator):
            self.range_type = _try_parse_and_validate_range_type(range_type)
            self.range = _try_parse_and_validate_range(
                range, self.range_type, self.operator
            )
        else:
            # Set these to what was input for later validation, and skip parsing and validation
            self.range_type = range_type
            self.range = range

        _validate_operator_and_input_parameters(
            operator=self.operator,
            value=self.value,
            value_type=_try_parse_and_validate_value_type(self.value_type)
            if self.value_type is not None
            else None,
            range=self.range,
            range_type=_try_parse_and_validate_range_type(self.range_type)
            if self.range_type is not None
            else None,
        )

        # Validate compatibility:
        self._validate_field_type_and_operator_compatibility(
            self.column_name, self.operator
        )
        self._validate_field_type_and_metric_type_compatibility(
            self.column_name, self.metric_type
        )
        self._validate_operator_and_range_or_value_compatibility(
            self.operator,
            self.value,
            _try_parse_and_validate_value_type(self.value_type)
            if self.value_type is not None
            else None,
            self.range,
            _try_parse_and_validate_range_type(self.range_type)
            if self.range_type is not None
            else None,
        )

    def _create_monitor_info(
        self,
        assertion_urn: AssertionUrn,
        status: models.MonitorStatusClass,
        schedule: models.CronScheduleClass,
    ) -> models.MonitorInfoClass:
        """
        Create a MonitorInfoClass with all the necessary components.
        """
        source_type, field = self._convert_assertion_source_type_and_field()
        return models.MonitorInfoClass(
            type=models.MonitorTypeClass.ASSERTION,
            status=status,
            assertionMonitor=models.AssertionMonitorClass(
                assertions=[
                    models.AssertionEvaluationSpecClass(
                        assertion=str(assertion_urn),
                        schedule=schedule,
                        parameters=self._get_assertion_evaluation_parameters(
                            str(source_type), field
                        ),
                    ),
                ],
                settings=models.AssertionMonitorSettingsClass(
                    adjustmentSettings=models.AssertionAdjustmentSettingsClass(
                        sensitivity=self._convert_sensitivity(),
                        exclusionWindows=self._convert_exclusion_windows(),
                        trainingDataLookbackWindowDays=self.training_data_lookback_days,
                    ),
                ),
            ),
        )

    def _create_assertion_info(
        self, filter: Optional[models.DatasetFilterClass]
    ) -> AssertionInfoInputType:
        """
        Create a FieldAssertionInfoClass for a smart column metric assertion.

        Args:
            filter: Optional filter to apply to the assertion.

        Returns:
            A FieldAssertionInfoClass configured for smart column metric.
        """
        # Get the field spec for the column
        field_spec = self._get_schema_field_spec(self.column_name)

        # Create the field metric assertion
        field_metric_assertion = models.FieldMetricAssertionClass(
            field=field_spec,
            metric=self.metric_type,
            operator=self.operator,
            parameters=self._create_assertion_parameters(),
        )

        # Create the field assertion info
        return models.FieldAssertionInfoClass(
            type=models.FieldAssertionTypeClass.FIELD_METRIC,
            entity=str(self.dataset_urn),
            filter=filter,
            fieldMetricAssertion=field_metric_assertion,
            fieldValuesAssertion=None,  # Explicitly set to None since this is a field metric assertion
        )

    def _convert_schedule(self) -> models.CronScheduleClass:
        """
        Create a schedule for a smart column metric assertion.

        Returns:
            A CronScheduleClass with appropriate schedule settings.
        """
        if self.schedule is None:
            return DEFAULT_EVERY_SIX_HOURS_SCHEDULE

        return models.CronScheduleClass(
            cron=self.schedule.cron,
            timezone=self.schedule.timezone,
        )

    def _convert_schema_field_spec_to_freshness_field_spec(
        self, field_spec: models.SchemaFieldSpecClass
    ) -> models.FreshnessFieldSpecClass:
        """
        Convert a SchemaFieldSpecClass to a FreshnessFieldSpecClass.
        """
        return models.FreshnessFieldSpecClass(
            path=field_spec.path,
            type=field_spec.type,
            nativeType=field_spec.nativeType,
            kind=models.FreshnessFieldKindClass.HIGH_WATERMARK,
        )

    def _get_assertion_evaluation_parameters(
        self, source_type: str, field: Optional[FieldSpecType]
    ) -> models.AssertionEvaluationParametersClass:
        """
        Get evaluation parameters for a smart column metric assertion.
        Converts SchemaFieldSpecClass to FreshnessFieldSpecClass if needed.
        """
        if field is not None:
            if isinstance(field, models.SchemaFieldSpecClass):
                field = self._convert_schema_field_spec_to_freshness_field_spec(field)
            assert isinstance(field, models.FreshnessFieldSpecClass), (
                "Field must be FreshnessFieldSpecClass for monitor info"
            )
        return models.AssertionEvaluationParametersClass(
            type=models.AssertionEvaluationParametersTypeClass.DATASET_FIELD,
            datasetFieldParameters=models.DatasetFieldAssertionParametersClass(
                sourceType=source_type,
                changedRowsField=field,
            ),
        )

    def _convert_assertion_source_type_and_field(
        self,
    ) -> tuple[str, Optional[FieldSpecType]]:
        """
        Convert detection mechanism into source type and field specification for column metric assertions.

        Returns:
            A tuple of (source_type, field) where field may be None.
            Note that the source_type is a string, not a models.DatasetFieldAssertionSourceTypeClass (or other assertion source type) since
            the source type is not a enum in the code generated from the DatasetFieldSourceType enum in the PDL.

        Raises:
            SDKNotYetSupportedError: If the detection mechanism is not supported.
            SDKUsageError: If the field (column) is not found in the dataset,
            and the detection mechanism requires a field. Also if the field
            is not an allowed type for the detection mechanism.
        """
        source_type = models.DatasetFieldAssertionSourceTypeClass.ALL_ROWS_QUERY
        field = None

        if isinstance(self.detection_mechanism, _ChangedRowsQuery):
            source_type = models.DatasetFieldAssertionSourceTypeClass.CHANGED_ROWS_QUERY
            column_name = self._try_parse_and_validate_column_name_is_valid_type(
                self.detection_mechanism.column_name,  # The high watermark column name
                allowed_column_types=HIGH_WATERMARK_ALLOWED_FIELD_TYPES,
            )
            field = self._get_schema_field_spec(column_name)
        elif isinstance(self.detection_mechanism, _AllRowsQuery):
            source_type = models.DatasetFieldAssertionSourceTypeClass.ALL_ROWS_QUERY
            # For query-based detection, we don't need a field specification
            # as the query itself defines what data to analyze
        elif isinstance(
            self.detection_mechanism,
            (_AllRowsQueryDataHubDatasetProfile, _DatasetProfile),
        ):
            source_type = (
                models.DatasetFieldAssertionSourceTypeClass.DATAHUB_DATASET_PROFILE
            )
            # Note: This is only valid on the all rows query
        else:
            raise SDKNotYetSupportedError(
                f"Detection mechanism {self.detection_mechanism} is not supported"
            )

        return source_type, field

    def _validate_single_value_operator(
        self,
        operator: models.AssertionStdOperatorClass,
        value: Optional[ValueInputType],
        value_type: Optional[models.AssertionStdParameterTypeClass],
        range: Optional[RangeInputType],
        range_type: Optional[RangeTypeParsedType],
    ) -> None:
        """Validate parameters for a single value operator."""
        if value is None:
            raise SDKUsageError(f"Value is required for operator {operator}")
        if value_type is None:
            raise SDKUsageError(f"Value type is required for operator {operator}")
        if range is not None or range_type is not None:
            raise SDKUsageError(
                f"Range parameters should not be provided for operator {operator}"
            )

    def _validate_range_operator(
        self,
        operator: models.AssertionStdOperatorClass,
        value: Optional[ValueInputType],
        value_type: Optional[models.AssertionStdParameterTypeClass],
        range: Optional[RangeInputType],
        range_type: Optional[RangeTypeParsedType],
    ) -> None:
        """Validate parameters for a range operator."""
        if range is None:
            raise SDKUsageError(f"Range is required for operator {operator}")
        if range_type is None:
            raise SDKUsageError(f"Range type is required for operator {operator}")
        if value is not None or value_type is not None:
            raise SDKUsageError(
                f"Value parameters should not be provided for operator {operator}"
            )

    def _validate_no_parameter_operator(
        self,
        operator: models.AssertionStdOperatorClass,
        value: Optional[ValueInputType],
        value_type: Optional[models.AssertionStdParameterTypeClass],
        range: Optional[RangeInputType],
        range_type: Optional[RangeTypeParsedType],
    ) -> None:
        """Validate parameters for a no-parameter operator."""
        if value is not None or value_type is not None:
            raise SDKUsageError(
                f"Value parameters should not be provided for operator {operator}"
            )
        if range is not None or range_type is not None:
            raise SDKUsageError(
                f"Range parameters should not be provided for operator {operator}"
            )

    def _validate_operator_and_range_or_value_compatibility(
        self,
        operator: models.AssertionStdOperatorClass,
        value: Optional[ValueInputType] = None,
        value_type: Optional[models.AssertionStdParameterTypeClass] = None,
        range: Optional[RangeInputType] = None,
        range_type: Optional[RangeTypeParsedType] = None,
    ) -> None:
        """
        Validate that the operator has the appropriate parameters (range or value) based on its type.

        Args:
            operator: The operator to validate.
            value: Optional value parameter.
            value_type: Optional value type parameter.
            range: Optional range parameter.
            range_type: Optional range type parameter.

        Raises:
            SDKUsageError: If the operator parameters are not compatible with the operator type.
        """
        if operator in SINGLE_VALUE_OPERATORS:
            self._validate_single_value_operator(
                operator, value, value_type, range, range_type
            )
        elif operator in RANGE_OPERATORS:
            self._validate_range_operator(
                operator, value, value_type, range, range_type
            )
        elif operator in NO_PARAMETER_OPERATORS:
            self._validate_no_parameter_operator(
                operator, value, value_type, range, range_type
            )
        else:
            raise SDKUsageError(f"Unsupported operator type: {operator}")

    def _create_assertion_parameters(self) -> models.AssertionStdParametersClass:
        """
        Create assertion parameters based on the operator type and provided values.

        Returns:
            An AssertionStdParametersClass with the appropriate parameters.

        Raises:
            SDKUsageError: If the parameters are invalid for the operator type.
        """
        if self.operator in SINGLE_VALUE_OPERATORS:
            if self.value is None:
                raise SDKUsageError(f"Value is required for operator {self.operator}")
            if self.value_type is None:
                raise SDKUsageError(
                    f"Value type is required for operator {self.operator}"
                )
            return models.AssertionStdParametersClass(
                value=models.AssertionStdParameterClass(
                    value=str(self.value),
                    type=self.value_type,
                ),
            )
        elif self.operator in RANGE_OPERATORS:
            if self.range is None:
                raise SDKUsageError(f"Range is required for operator {self.operator}")
            if self.range_type is None:
                raise SDKUsageError(
                    f"Range type is required for operator {self.operator}"
                )
            # Ensure we have the parsed range type
            parsed_range_type = _try_parse_and_validate_range_type(self.range_type)
            return models.AssertionStdParametersClass(
                minValue=models.AssertionStdParameterClass(
                    value=str(self.range[0]),
                    type=parsed_range_type[0],
                ),
                maxValue=models.AssertionStdParameterClass(
                    value=str(self.range[1]),
                    type=parsed_range_type[1],
                ),
            )
        elif self.operator in NO_PARAMETER_OPERATORS:
            return models.AssertionStdParametersClass()
        else:
            raise SDKUsageError(f"Unsupported operator type: {self.operator}")

    def _try_parse_and_validate_column_name_is_valid_type(
        self,
        column_name: str,
        allowed_column_types: list[
            models.DictWrapper
        ] = ALLOWED_COLUMN_TYPES_FOR_SMART_COLUMN_METRIC_ASSERTION,
    ) -> str:
        """
        Parse and validate a column name. Determine from the field spec if the column exists and is of the appropriate type for the metric type.
        Validate that this is a column that is valid for the metric type, see also getEligibleFieldColumns and related functions in the frontend
        """
        field_spec = self._get_schema_field_spec(column_name)
        self._validate_field_type(
            field_spec,
            column_name,
            allowed_column_types,
            "smart column metric assertion",
        )
        return column_name

    def _assertion_type(self) -> str:
        """Get the assertion type."""
        return models.AssertionTypeClass.FIELD

    def _validate_field_type_and_operator_compatibility(
        self, column_name: str, operator: models.AssertionStdOperatorClass
    ) -> None:
        """Validate that the field type is compatible with the operator.

        See FIELD_VALUES_OPERATOR_CONFIG in the frontend for the allowed operators for each field type.

        Args:
            column_name: The name of the column to validate.
            operator: The operator to validate against.

        Raises:
            SDKUsageError: If the field type is not compatible with the operator.
        """
        field_spec = self._get_schema_field_spec(column_name)
        allowed_operators = FIELD_VALUES_OPERATOR_CONFIG.get(field_spec.type, [])
        if operator not in allowed_operators:
            raise SDKUsageError(
                f"Operator {operator} is not allowed for field type {field_spec.type} for column '{column_name}'. Allowed operators: {', '.join(str(op) for op in allowed_operators)}"
            )

    def _validate_field_type_and_metric_type_compatibility(
        self, column_name: str, metric_type: models.FieldMetricTypeClass
    ) -> None:
        """Validate that the metric type is compatible with the field type.

        See FIELD_METRIC_TYPE_CONFIG in the frontend for the allowed metric types for each field type.

        Args:
            column_name: The name of the column to validate.
            metric_type: The metric type to validate.

        Raises:
            SDKUsageError: If the metric type is not compatible with the field type.
        """
        field_spec = self._get_schema_field_spec(column_name)
        field_type = field_spec.type

        if field_type not in FIELD_METRIC_TYPE_CONFIG:
            raise SDKUsageError(
                f"Column {column_name} is of type {field_type}, which is not supported for smart column metric assertions"
            )

        allowed_metric_types = FIELD_METRIC_TYPE_CONFIG[field_type]
        if metric_type not in allowed_metric_types:
            raise SDKUsageError(
                f"Metric type {metric_type} is not allowed for field type {field_type}. Allowed metric types: {', '.join(str(mt) for mt in allowed_metric_types)}"
            )


def _try_parse_and_validate_value_type(
    value_type: Optional[ValueTypeInputType],
) -> models.AssertionStdParameterTypeClass:
    if value_type is None:
        raise SDKUsageError("Value type is required")

    return _try_parse_and_validate_schema_classes_enum(
        value_type, models.AssertionStdParameterTypeClass
    )


def _try_parse_and_validate_value(
    value: Optional[ValueInputType],
    value_type: ValueTypeInputType,
) -> ValueInputType:
    if value is None:
        raise SDKUsageError("Value parameter is required for the chosen operator")
    # Accept both Python types and JSON strings
    if isinstance(value, str):
        # Try to parse as JSON, but if it fails, treat as a raw string
        try:
            deserialized_value = json.loads(value)
        except json.JSONDecodeError:
            deserialized_value = value
    else:
        deserialized_value = value
    # Validate that the value is of the correct type
    if value_type == models.AssertionStdParameterTypeClass.NUMBER:
        if not isinstance(deserialized_value, (int, float)):
            raise SDKUsageError(f"Invalid value: {value}, must be a number")
    elif value_type == models.AssertionStdParameterTypeClass.STRING:
        if not isinstance(deserialized_value, str):
            raise SDKUsageError(f"Invalid value: {value}, must be a string")
    elif (
        value_type == models.AssertionStdParameterTypeClass.LIST
        or value_type == models.AssertionStdParameterTypeClass.SET
    ):
        raise SDKNotYetSupportedError(
            "List and set value types are not supported for smart column metric assertions"
        )
    elif value_type == models.AssertionStdParameterTypeClass.UNKNOWN:
        pass  # TODO: What to do with unknown?
    else:
        raise SDKUsageError(
            f"Invalid value type: {value_type}, valid options are {get_enum_options(models.AssertionStdParameterTypeClass)}"
        )
    return deserialized_value


def _is_range_required_for_operator(operator: models.AssertionStdOperatorClass) -> bool:
    return operator in RANGE_OPERATORS


def _is_value_required_for_operator(operator: models.AssertionStdOperatorClass) -> bool:
    return operator in SINGLE_VALUE_OPERATORS


def _is_no_parameter_operator(operator: models.AssertionStdOperatorClass) -> bool:
    return operator in NO_PARAMETER_OPERATORS


def _validate_operator_and_input_parameters(
    operator: models.AssertionStdOperatorClass,
    value: Optional[ValueInputType] = None,
    value_type: Optional[models.AssertionStdParameterTypeClass] = None,
    range: Optional[RangeInputType] = None,
    range_type: Optional[RangeTypeParsedType] = None,
) -> None:
    if _is_value_required_for_operator(operator):
        if value is None:
            raise SDKUsageError(f"Value is required for operator {operator}")
        if value_type is None:
            raise SDKUsageError(f"Value type is required for operator {operator}")
    elif _is_range_required_for_operator(operator):
        if range is None:
            raise SDKUsageError(f"Range is required for operator {operator}")
        if range_type is None:
            raise SDKUsageError(f"Range type is required for operator {operator}")
    elif _is_no_parameter_operator(operator):
        if value is not None or value_type is not None:
            raise SDKUsageError(
                f"Value parameters should not be provided for operator {operator}"
            )
        if range is not None or range_type is not None:
            raise SDKUsageError(
                f"Range parameters should not be provided for operator {operator}"
            )
    else:
        raise SDKUsageError(f"Unsupported operator type: {operator}")


def _try_parse_and_validate_range_type(
    range_type: Optional[RangeTypeInputType] = None,
) -> RangeTypeParsedType:
    if range_type is None:
        return (
            models.AssertionStdParameterTypeClass.UNKNOWN,
            models.AssertionStdParameterTypeClass.UNKNOWN,
        )
    if isinstance(range_type, tuple):
        return (
            _try_parse_and_validate_schema_classes_enum(
                range_type[0], models.AssertionStdParameterTypeClass
            ),
            _try_parse_and_validate_schema_classes_enum(
                range_type[1], models.AssertionStdParameterTypeClass
            ),
        )
    # Single value, we assume the same type for start and end:
    parsed_range_type = _try_parse_and_validate_schema_classes_enum(
        range_type, models.AssertionStdParameterTypeClass
    )
    return parsed_range_type, parsed_range_type


def _try_parse_and_validate_range(
    range: Optional[RangeInputType],
    range_type: RangeTypeParsedType,
    operator: models.AssertionStdOperatorClass,
) -> RangeInputType:
    if (range is None or range_type is None) and _is_range_required_for_operator(
        operator
    ):
        raise SDKUsageError(f"Range is required for operator {operator}")

    if range is None:
        raise SDKUsageError(f"Range is required for operator {operator}")

    range_start = _try_parse_and_validate_value(range[0], range_type[0])
    range_end = _try_parse_and_validate_value(range[1], range_type[1])

    return (range_start, range_end)
