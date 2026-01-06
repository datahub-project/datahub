"""
Column value assertion input module.

This module provides the input class for creating column value assertions that validate
individual row values against semantic constraints (e.g., "all values in column X must match pattern Y"
or "no NULL values allowed").

Key differences from column_metric_assertion (FIELD_METRIC):
- FIELD_METRIC: Validates aggregated metrics (NULL_COUNT, MEAN, MIN, etc.)
- FIELD_VALUES: Validates each individual row value against an operator/predicate
"""

from datetime import datetime
from enum import Enum
from typing import Optional, Union

from acryl_datahub_cloud.sdk.assertion_input.assertion_input import (
    DEFAULT_EVERY_SIX_HOURS_SCHEDULE,
    NO_PARAMETER_OPERATORS,
    RANGE_OPERATORS,
    SINGLE_VALUE_OPERATORS,
    AssertionIncidentBehaviorInputTypes,
    AssertionInfoInputType,
    DetectionMechanismInputTypes,
    FieldSpecType,
    _AllRowsQuery,
    _AllRowsQueryDataHubDatasetProfile,
    _AssertionInput,
    _ChangedRowsQuery,
    _DatasetProfile,
    _try_parse_and_validate_schema_classes_enum,
)
from acryl_datahub_cloud.sdk.assertion_input.column_metric_assertion_input import (
    _try_parse_and_validate_range,
    _try_parse_and_validate_range_type,
    _try_parse_and_validate_value,
    _try_parse_and_validate_value_type,
)
from acryl_datahub_cloud.sdk.assertion_input.column_metric_constants import (
    ALLOWED_COLUMN_TYPES_FOR_COLUMN_METRIC_ASSERTION,
    FIELD_VALUES_OPERATOR_CONFIG,
    OperatorInputType,
    RangeInputType,
    RangeTypeInputType,
    RangeTypeParsedType,
    ValueInputType,
    ValueType,
    ValueTypeInputType,
)
from acryl_datahub_cloud.sdk.entities.assertion import TagsInputType
from acryl_datahub_cloud.sdk.errors import (
    SDKNotYetSupportedError,
    SDKUsageError,
)
from datahub.metadata import schema_classes as models
from datahub.metadata.urns import AssertionUrn, CorpUserUrn, DatasetUrn
from datahub.sdk.entity_client import EntityClient


class FailThresholdType(str, Enum):
    """Enum for fail threshold types in column value assertions."""

    COUNT = "COUNT"
    PERCENTAGE = "PERCENTAGE"


class FieldTransformType(str, Enum):
    """Enum for field transform types in column value assertions."""

    LENGTH = "LENGTH"


FailThresholdInputType = Union[FailThresholdType, str]
FieldTransformInputType = Union[FieldTransformType, str, None]
ColumnValueAssertionParameters = Union[
    None,  # For operators that don't require parameters (NULL, NOT_NULL)
    ValueInputType,  # Single value
    RangeInputType,  # Range as tuple
]

# This represents the type information from existing GMS assertions:
# - Single value: (value, type)
# - Range: ((min_value, max_value), (min_type, max_type))
GmsCriteriaTypeInfo = Union[
    tuple[ValueInputType, models.AssertionStdParameterTypeClass],  # Single value
    tuple[
        tuple[ValueInputType, ValueInputType],  # (min, max) values
        tuple[
            models.AssertionStdParameterTypeClass,
            models.AssertionStdParameterTypeClass,
        ],  # (min_type, max_type)
    ],  # Range
]


def _get_default_detection_mechanism_column_value_assertion() -> _AllRowsQuery:
    """Factory function for creating default detection mechanism instances.

    Returns a new instance each time to avoid shared mutable state.
    """
    return _AllRowsQuery()


# This is used to validate that operators are compatible with transform outputs
FIELD_TRANSFORM_OUTPUT_TYPE: dict[str, str] = {
    models.FieldTransformTypeClass.LENGTH: "NUMBER",  # LENGTH(string) -> number
}


def _try_parse_fail_threshold_type(
    fail_threshold_type: Optional[FailThresholdInputType],
) -> FailThresholdType:
    """Parse and validate fail threshold type.

    Args:
        fail_threshold_type: The fail threshold type to parse.

    Returns:
        The parsed FailThresholdType.

    Raises:
        SDKUsageError: If the fail threshold type is invalid.
    """
    if fail_threshold_type is None:
        return FailThresholdType.COUNT

    if isinstance(fail_threshold_type, FailThresholdType):
        return fail_threshold_type

    if isinstance(fail_threshold_type, str):
        try:
            return FailThresholdType(fail_threshold_type.upper())
        except ValueError as e:
            raise SDKUsageError(
                f"Invalid fail threshold type: {fail_threshold_type}. "
                f"Valid options are: {[t.value for t in FailThresholdType]}"
            ) from e

    raise SDKUsageError(
        f"Invalid fail threshold type: {fail_threshold_type}. "
        f"Valid options are: {[t.value for t in FailThresholdType]}"
    )


def _try_parse_field_transform_type(
    field_transform: Optional[FieldTransformInputType],
) -> Optional[str]:
    """Parse and validate field transform type.

    Args:
        field_transform: The field transform type to parse.

    Returns:
        The parsed FieldTransformTypeClass string constant or None.

    Raises:
        SDKUsageError: If the field transform type is invalid.
    """
    if field_transform is None:
        return None

    if isinstance(field_transform, FieldTransformType):
        return models.FieldTransformTypeClass.LENGTH

    if isinstance(field_transform, str):
        if field_transform.upper() == "LENGTH":
            return models.FieldTransformTypeClass.LENGTH
        else:
            raise SDKUsageError(
                f"Invalid field transform type: {field_transform}. "
                f"Valid options are: {[t.value for t in FieldTransformType]}"
            )

    raise SDKUsageError(
        f"Invalid field transform type: {field_transform}. "
        f"Valid options are: {[t.value for t in FieldTransformType]}"
    )


def _validate_fail_threshold_value(
    fail_threshold_type: FailThresholdType,
    fail_threshold_value: int,
) -> None:
    """Validate fail threshold value based on the type.

    Args:
        fail_threshold_type: The type of fail threshold.
        fail_threshold_value: The value to validate.

    Raises:
        SDKUsageError: If the fail threshold value is invalid.
    """
    if fail_threshold_value < 0:
        raise SDKUsageError(
            f"Fail threshold value must be non-negative, got {fail_threshold_value}"
        )

    if (
        fail_threshold_type == FailThresholdType.PERCENTAGE
        and fail_threshold_value > 100
    ):
        raise SDKUsageError(
            f"Fail threshold value for PERCENTAGE must be between 0 and 100, "
            f"got {fail_threshold_value}"
        )


class _ColumnValueAssertionInput(_AssertionInput):
    """
    Input used to create a column value assertion.

    This assertion is used to validate individual row values in a column against
    semantic constraints (e.g., "all values in column X must match pattern Y" or
    "no NULL values allowed").

    Key differences from column_metric_assertion (FIELD_METRIC):
    - FIELD_METRIC: Validates aggregated metrics (NULL_COUNT, MEAN, MIN, etc.)
    - FIELD_VALUES: Validates each individual row value against an operator/predicate
    """

    def __init__(
        self,
        *,
        dataset_urn: Union[str, DatasetUrn],
        entity_client: EntityClient,
        column_name: str,
        operator: OperatorInputType,
        criteria_parameters: Optional[ColumnValueAssertionParameters] = None,
        transform: Optional[FieldTransformInputType] = None,
        fail_threshold_type: Optional[FailThresholdInputType] = None,
        fail_threshold_value: int = 0,
        exclude_nulls: bool = True,
        urn: Optional[Union[str, AssertionUrn]] = None,
        display_name: Optional[str] = None,
        enabled: bool = True,
        schedule: Optional[Union[str, models.CronScheduleClass]] = None,
        detection_mechanism: DetectionMechanismInputTypes = None,
        incident_behavior: Optional[AssertionIncidentBehaviorInputTypes] = None,
        tags: Optional[TagsInputType] = None,
        created_by: Union[str, CorpUserUrn],
        created_at: datetime,
        updated_by: Union[str, CorpUserUrn],
        updated_at: datetime,
        gms_criteria_type_info: Optional[GmsCriteriaTypeInfo] = None,
    ):
        """
        Initialize a column value assertion input.

        Args:
            dataset_urn: The dataset urn.
            entity_client: The entity client.
            column_name: The name of the column to validate.
            operator: The operator to use for the assertion.
            criteria_parameters: The criteria parameters (single value, range tuple, or None).
            transform: Optional transform to apply to field values before evaluation.
            fail_threshold_type: The type of failure threshold (COUNT or PERCENTAGE).
            fail_threshold_value: The failure threshold value (default 0 = all rows must pass).
            exclude_nulls: Whether to exclude nulls when evaluating the assertion.
            urn: The urn of the assertion.
            display_name: The display name of the assertion.
            enabled: Whether the assertion is enabled.
            schedule: The schedule of the assertion.
            detection_mechanism: The detection mechanism of the assertion.
            incident_behavior: The incident behavior of the assertion.
            tags: The tags of the assertion.
            created_by: The creator of the assertion.
            created_at: The creation time of the assertion.
            updated_by: The updater of the assertion.
            updated_at: The update time of the assertion.
            gms_criteria_type_info: Type info from existing GMS assertion for updates.
                Format: (value, type) for single values, or ((min, max), (min_type, max_type)) for ranges.
        """
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
            source_type=models.AssertionSourceTypeClass.NATIVE,
            created_by=created_by,
            created_at=created_at,
            updated_by=updated_by,
            updated_at=updated_at,
            default_detection_mechanism=_get_default_detection_mechanism_column_value_assertion(),
        )

        self.column_name = self._try_parse_and_validate_column_name_is_valid_type(
            column_name
        )
        self.operator = _try_parse_and_validate_schema_classes_enum(
            operator, models.AssertionStdOperatorClass
        )
        self.transform = _try_parse_field_transform_type(transform)
        self.fail_threshold_type = _try_parse_fail_threshold_type(fail_threshold_type)
        self.fail_threshold_value = fail_threshold_value
        self.exclude_nulls = exclude_nulls

        _validate_fail_threshold_value(
            self.fail_threshold_type, self.fail_threshold_value
        )

        if self.transform is not None:
            self._validate_transform_for_column_type()
            self._validate_operator_for_transform_output_type()

        self.criteria_parameters: Optional[ColumnValueAssertionParameters] = None
        self.criteria_type: Optional[Union[ValueTypeInputType, RangeTypeInputType]] = (
            None
        )

        if gms_criteria_type_info is not None:
            self._process_criteria_parameters_with_gms_type(
                criteria_parameters, gms_criteria_type_info
            )
        else:
            self._process_criteria_parameters(criteria_parameters)

        if self.transform is None:
            self._validate_field_type_and_operator_compatibility(
                self.column_name, self.operator
            )

    def _validate_transform_for_column_type(self) -> None:
        """Validate that the transform is compatible with the column type.

        LENGTH transform is only valid for STRING columns.

        Raises:
            SDKUsageError: If the transform is not compatible with the column type.
        """
        field_spec = self._get_schema_field_spec(self.column_name)
        if (
            self.transform == models.FieldTransformTypeClass.LENGTH
            and field_spec.type != "STRING"
        ):
            raise SDKUsageError(
                f"LENGTH transform is only valid for STRING columns, "
                f"but column '{self.column_name}' is of type {field_spec.type}"
            )

    def _validate_operator_for_transform_output_type(self) -> None:
        """Validate that the operator is compatible with the transform output type.

        When a transform is applied, the operator must be compatible with the
        transform's output type, not the original column type.
        For example: LENGTH(string_column) produces a NUMBER, so operators like
        REGEX_MATCH (which expect STRING) should be rejected.

        Raises:
            SDKUsageError: If the operator is not compatible with transform output.
        """
        if self.transform is None:
            return

        # Get the output type for this transform
        transform_output_type = FIELD_TRANSFORM_OUTPUT_TYPE.get(self.transform)
        if transform_output_type is None:
            raise SDKNotYetSupportedError(
                f"Transform {self.transform} is not yet supported for operator validation. "
                f"Please update FIELD_TRANSFORM_OUTPUT_TYPE mapping."
            )

        # Check if operator is allowed for the transform output type
        allowed_operators = FIELD_VALUES_OPERATOR_CONFIG.get(transform_output_type, [])
        if self.operator not in allowed_operators:
            raise SDKUsageError(
                f"Operator {self.operator} is not compatible with transform {self.transform}. "
                f"Transform {self.transform} produces {transform_output_type} values, "
                f"but operator {self.operator} is not valid for {transform_output_type} types. "
                f"Allowed operators for {transform_output_type}: "
                f"{', '.join(str(op) for op in allowed_operators)}"
            )

    def _infer_criteria_type_from_parameters(
        self,
        criteria_parameters: Optional[ColumnValueAssertionParameters],
    ) -> Optional[Union[ValueTypeInputType, RangeTypeInputType]]:
        """
        Infer the criteria type from the parameters based on Python types.
        """
        if criteria_parameters is None:
            return None

        if isinstance(criteria_parameters, tuple):
            if len(criteria_parameters) != 2:
                raise SDKUsageError(
                    "Range parameters must be a tuple of exactly 2 values"
                )
            inferred_min_type = self._infer_single_value_type(criteria_parameters[0])
            inferred_max_type = self._infer_single_value_type(criteria_parameters[1])
            return (inferred_min_type, inferred_max_type)
        else:
            return self._infer_single_value_type(criteria_parameters)

    def _infer_single_value_type(self, value: ValueInputType) -> ValueTypeInputType:
        """Infer the type of a single value based on its Python type."""
        if isinstance(value, (int, float)):
            return ValueType.NUMBER
        elif isinstance(value, str):
            return ValueType.STRING
        else:
            return ValueType.UNKNOWN

    def _process_criteria_parameters_with_gms_type(
        self,
        criteria_parameters: Optional[ColumnValueAssertionParameters],
        gms_type_info: Optional[Union[models.AssertionStdParameterTypeClass, tuple]],
    ) -> None:
        """Process criteria_parameters using explicit type information from GMS."""
        if criteria_parameters is None:
            self._process_none_parameters()
        elif isinstance(criteria_parameters, tuple):
            # For range parameters, pass explicit types if available
            # gms_type_info format: ((min_val, max_val), (min_type, max_type))
            explicit_types = None
            if (
                isinstance(gms_type_info, tuple)
                and len(gms_type_info) == 2
                and isinstance(gms_type_info[0], tuple)
            ):
                # Extract types from second element (should be tuple of types)
                explicit_types = (
                    gms_type_info[1] if isinstance(gms_type_info[1], tuple) else None
                )
            self._process_range_parameters(criteria_parameters, explicit_types)
        else:
            # For single value parameters, pass explicit type if available
            # gms_type_info format: (value, type)
            explicit_type = None
            if (
                isinstance(gms_type_info, tuple)
                and len(gms_type_info) >= 2
                and not isinstance(gms_type_info[0], tuple)
                and not isinstance(gms_type_info[1], tuple)
            ):
                # Single value format: extract type from second element
                explicit_type = gms_type_info[1]
            self._process_single_value_parameters(criteria_parameters, explicit_type)

    def _process_criteria_parameters(
        self,
        criteria_parameters: Optional[ColumnValueAssertionParameters],
    ) -> None:
        """Process the criteria_parameters with automatic type inference."""
        if criteria_parameters is None:
            self._process_none_parameters()
        elif isinstance(criteria_parameters, tuple):
            self._process_range_parameters(criteria_parameters)
        else:
            self._process_single_value_parameters(criteria_parameters)

    def _process_none_parameters(self) -> None:
        """Process None criteria_parameters.

        Raises:
            SDKUsageError: If the operator requires parameters but none are provided.
        """
        if self.operator in SINGLE_VALUE_OPERATORS:
            raise SDKUsageError(
                f"Single value is required for operator {self.operator}. "
                "Provide a criteria_parameters value."
            )
        if self.operator in RANGE_OPERATORS:
            raise SDKUsageError(
                f"Range parameters are required for operator {self.operator}. "
                "Provide a tuple of (min_value, max_value) as criteria_parameters."
            )
        self.criteria_parameters = None
        self.criteria_type = None

    def _process_range_parameters(
        self,
        criteria_parameters: tuple,
        explicit_types: Optional[
            tuple[
                models.AssertionStdParameterTypeClass,
                models.AssertionStdParameterTypeClass,
            ]
        ] = None,
    ) -> None:
        """Process tuple criteria_parameters for range operators.

        Args:
            criteria_parameters: The range parameters (min, max).
            explicit_types: Optional explicit types from GMS (min_type, max_type).
                If provided, these types are used directly. Otherwise, types are
                inferred from the parameters.
        """
        if self.operator not in RANGE_OPERATORS:
            raise SDKUsageError(
                f"Operator {self.operator} does not support range parameters. "
                "Provide a single value instead of a tuple."
            )

        if len(criteria_parameters) != 2:
            raise SDKUsageError("Range parameters must be a tuple of exactly 2 values")

        # Declare validated_range_type with explicit type annotation
        validated_range_type: RangeTypeParsedType

        # Use explicit types if provided, otherwise infer from parameters
        if explicit_types is not None:
            min_type, max_type = explicit_types
            validated_min_type = _try_parse_and_validate_value_type(min_type)
            validated_max_type = _try_parse_and_validate_value_type(max_type)
            validated_range_type = (validated_min_type, validated_max_type)

            min_value, max_value = criteria_parameters
            validated_min_value = _try_parse_and_validate_value(
                min_value, validated_min_type
            )
            validated_max_value = _try_parse_and_validate_value(
                max_value, validated_max_type
            )
            validated_range = (validated_min_value, validated_max_value)
        else:
            inferred_range_type = self._infer_criteria_type_from_parameters(
                criteria_parameters
            )
            # Type narrowing: inferred_range_type should be a tuple for range parameters
            if not isinstance(inferred_range_type, tuple):
                raise SDKUsageError(
                    "Expected tuple type for range parameters, but got "
                    f"{type(inferred_range_type).__name__}"
                )
            validated_range_type = _try_parse_and_validate_range_type(
                inferred_range_type
            )
            validated_range = _try_parse_and_validate_range(
                criteria_parameters, validated_range_type, self.operator
            )

        self.criteria_parameters = validated_range
        self.criteria_type = validated_range_type

    def _process_single_value_parameters(
        self,
        criteria_parameters: Union[str, int, float],
        explicit_type: Optional[models.AssertionStdParameterTypeClass] = None,
    ) -> None:
        """Process single value criteria_parameters.

        Args:
            criteria_parameters: The single value parameter.
            explicit_type: Optional explicit type from GMS. If provided, this type
                is used directly. Otherwise, the type is inferred from the parameter.
        """
        if self.operator in NO_PARAMETER_OPERATORS:
            raise SDKUsageError(
                f"Value parameters should not be provided for operator {self.operator}"
            )
        if self.operator not in SINGLE_VALUE_OPERATORS:
            raise SDKUsageError(
                f"Operator {self.operator} does not support value parameters. "
                "Use criteria_parameters=None or omit criteria_parameters."
            )

        # Use explicit type if provided, otherwise infer from parameters
        if explicit_type is not None:
            validated_value_type = _try_parse_and_validate_value_type(explicit_type)
        else:
            inferred_value_type = self._infer_criteria_type_from_parameters(
                criteria_parameters
            )
            if isinstance(inferred_value_type, tuple):
                raise SDKUsageError("Single value type expected, not a tuple type")
            validated_value_type = _try_parse_and_validate_value_type(
                inferred_value_type
            )

        validated_value = _try_parse_and_validate_value(
            criteria_parameters, validated_value_type
        )

        self.criteria_parameters = validated_value
        self.criteria_type = validated_value_type

    def _create_monitor_info(
        self,
        assertion_urn: AssertionUrn,
        status: models.MonitorStatusClass,
        schedule: models.CronScheduleClass,
    ) -> models.MonitorInfoClass:
        """Create a MonitorInfoClass with all the necessary components."""
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
                settings=None,
            ),
        )

    def _create_assertion_info(
        self, filter: Optional[models.DatasetFilterClass]
    ) -> AssertionInfoInputType:
        """Create a FieldAssertionInfoClass for a column value assertion."""
        field_spec = self._get_schema_field_spec(self.column_name)

        field_values_assertion = models.FieldValuesAssertionClass(
            field=field_spec,
            operator=self.operator,
            parameters=self._create_assertion_parameters(),
            transform=self._create_field_transform(),
            failThreshold=models.FieldValuesFailThresholdClass(
                type=self._convert_fail_threshold_type(),
                value=self.fail_threshold_value,
            ),
            excludeNulls=self.exclude_nulls,
        )

        return models.FieldAssertionInfoClass(
            type=models.FieldAssertionTypeClass.FIELD_VALUES,
            entity=str(self.dataset_urn),
            filter=filter,
            fieldValuesAssertion=field_values_assertion,
            fieldMetricAssertion=None,
        )

    def _convert_fail_threshold_type(self) -> str:
        """Convert the fail threshold type to the model class."""
        if self.fail_threshold_type == FailThresholdType.COUNT:
            return models.FieldValuesFailThresholdTypeClass.COUNT
        else:
            return models.FieldValuesFailThresholdTypeClass.PERCENTAGE

    def _create_field_transform(self) -> Optional[models.FieldTransformClass]:
        """Create the field transform if specified."""
        if self.transform is None:
            return None
        return models.FieldTransformClass(type=self.transform)

    def _convert_schedule(self) -> models.CronScheduleClass:
        """Create a schedule for a column value assertion."""
        if self.schedule is None:
            return DEFAULT_EVERY_SIX_HOURS_SCHEDULE

        return models.CronScheduleClass(
            cron=self.schedule.cron,
            timezone=self.schedule.timezone,
        )

    def _convert_schema_field_spec_to_freshness_field_spec(
        self, field_spec: models.SchemaFieldSpecClass
    ) -> models.FreshnessFieldSpecClass:
        """Convert a SchemaFieldSpecClass to a FreshnessFieldSpecClass."""
        return models.FreshnessFieldSpecClass(
            path=field_spec.path,
            type=field_spec.type,
            nativeType=field_spec.nativeType,
            kind=models.FreshnessFieldKindClass.HIGH_WATERMARK,
        )

    def _get_assertion_evaluation_parameters(
        self, source_type: str, field: Optional[FieldSpecType]
    ) -> models.AssertionEvaluationParametersClass:
        """Get evaluation parameters for a column value assertion."""
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
        """Convert detection mechanism into source type and field specification."""
        source_type = models.DatasetFieldAssertionSourceTypeClass.ALL_ROWS_QUERY
        field = None
        SUPPORTED_DETECTION_MECHANISMS = [
            _AllRowsQuery().type,
            _AllRowsQueryDataHubDatasetProfile().type,
            _ChangedRowsQuery(column_name="").type,
        ]

        if isinstance(self.detection_mechanism, _ChangedRowsQuery):
            source_type = models.DatasetFieldAssertionSourceTypeClass.CHANGED_ROWS_QUERY
            column_name = self._try_parse_and_validate_column_name_is_valid_type(
                self.detection_mechanism.column_name,
                allowed_column_types=[
                    models.NumberTypeClass(),
                    models.DateTypeClass(),
                    models.TimeTypeClass(),
                ],
            )
            field = self._get_schema_field_spec(column_name)
        elif isinstance(self.detection_mechanism, _AllRowsQuery):
            source_type = models.DatasetFieldAssertionSourceTypeClass.ALL_ROWS_QUERY
        elif isinstance(
            self.detection_mechanism,
            (_AllRowsQueryDataHubDatasetProfile, _DatasetProfile),
        ):
            source_type = (
                models.DatasetFieldAssertionSourceTypeClass.DATAHUB_DATASET_PROFILE
            )
        else:
            raise SDKNotYetSupportedError(
                f"Detection mechanism {self.detection_mechanism} is not supported for "
                f"column value assertions, please use a supported detection mechanism: "
                f"{', '.join(SUPPORTED_DETECTION_MECHANISMS)}"
            )

        return source_type, field

    def _create_assertion_parameters(self) -> models.AssertionStdParametersClass:
        """Create assertion parameters based on the operator type and provided values."""
        if self.operator in SINGLE_VALUE_OPERATORS:
            if self.criteria_parameters is None or isinstance(
                self.criteria_parameters, tuple
            ):
                raise SDKUsageError(
                    f"Single value is required for operator {self.operator}"
                )
            if self.criteria_type is None or isinstance(self.criteria_type, tuple):
                raise SDKUsageError(
                    f"Single value type is required for operator {self.operator}"
                )
            return models.AssertionStdParametersClass(
                value=models.AssertionStdParameterClass(
                    value=str(self.criteria_parameters),
                    type=self.criteria_type,
                ),
            )
        elif self.operator in RANGE_OPERATORS:
            if not isinstance(self.criteria_parameters, tuple):
                raise SDKUsageError(
                    f"Range parameters are required for operator {self.operator}"
                )
            if not isinstance(self.criteria_type, tuple):
                raise SDKUsageError(
                    f"Range type is required for operator {self.operator}"
                )
            return models.AssertionStdParametersClass(
                minValue=models.AssertionStdParameterClass(
                    value=str(self.criteria_parameters[0]),
                    type=self.criteria_type[0],
                ),
                maxValue=models.AssertionStdParameterClass(
                    value=str(self.criteria_parameters[1]),
                    type=self.criteria_type[1],
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
        ] = ALLOWED_COLUMN_TYPES_FOR_COLUMN_METRIC_ASSERTION,
    ) -> str:
        """Parse and validate a column name and its type."""
        field_spec = self._get_schema_field_spec(column_name)
        self._validate_field_type(
            field_spec,
            column_name,
            allowed_column_types,
            "column value assertion",
        )
        return column_name

    def _assertion_type(self) -> str:
        """Get the assertion type."""
        return models.AssertionTypeClass.FIELD

    def _validate_field_type_and_operator_compatibility(
        self, column_name: str, operator: models.AssertionStdOperatorClass
    ) -> None:
        """Validate that the field type is compatible with the operator.

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
                f"Operator {operator} is not allowed for field type {field_spec.type} "
                f"for column '{column_name}'. Allowed operators: "
                f"{', '.join(str(op) for op in allowed_operators)}"
            )
