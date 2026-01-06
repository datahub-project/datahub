"""
Column value assertion module.

This module provides the ColumnValueAssertion class for representing column value
assertions that validate individual row values against semantic constraints.
"""

import logging
from datetime import datetime
from typing import Optional, Union

from typing_extensions import Self

from acryl_datahub_cloud.sdk.assertion.assertion_base import (
    AssertionMode,
    _AssertionPublic,
    _HasSchedule,
)
from acryl_datahub_cloud.sdk.assertion_input.assertion_input import (
    DEFAULT_DETECTION_MECHANISM,
    DEFAULT_SCHEDULE,
    AssertionIncidentBehavior,
    DetectionMechanism,
    _DetectionMechanismTypes,
)
from acryl_datahub_cloud.sdk.assertion_input.column_metric_constants import (
    OperatorInputType,
)
from acryl_datahub_cloud.sdk.assertion_input.column_value_assertion_input import (
    ColumnValueAssertionParameters,
    FailThresholdType,
    FieldTransformInputType,
)
from acryl_datahub_cloud.sdk.entities.assertion import Assertion
from acryl_datahub_cloud.sdk.entities.monitor import (
    Monitor,
    _get_nested_field_for_entity_with_default,
)
from acryl_datahub_cloud.sdk.errors import SDKUsageError
from datahub.metadata import schema_classes as models
from datahub.metadata.urns import AssertionUrn, CorpUserUrn, DatasetUrn, TagUrn

logger = logging.getLogger(__name__)


class ColumnValueAssertion(
    _HasSchedule,
    _AssertionPublic,
):
    """
    A class that represents a column value assertion.

    This assertion validates individual row values in a column against semantic
    constraints (e.g., "all values in column X must match pattern Y" or
    "no NULL values allowed").

    Key differences from ColumnMetricAssertion (FIELD_METRIC):
    - FIELD_METRIC: Validates aggregated metrics (NULL_COUNT, MEAN, MIN, etc.)
    - FIELD_VALUES: Validates each individual row value against an operator/predicate
    """

    def __init__(
        self,
        *,
        urn: AssertionUrn,
        dataset_urn: DatasetUrn,
        column_name: str,
        operator: OperatorInputType,
        criteria_parameters: Optional[ColumnValueAssertionParameters] = None,
        transform: Optional[FieldTransformInputType] = None,
        fail_threshold_type: FailThresholdType = FailThresholdType.COUNT,
        fail_threshold_value: int = 0,
        exclude_nulls: bool = True,
        display_name: str,
        mode: AssertionMode,
        schedule: models.CronScheduleClass = DEFAULT_SCHEDULE,
        incident_behavior: list[AssertionIncidentBehavior],
        detection_mechanism: Optional[
            _DetectionMechanismTypes
        ] = DEFAULT_DETECTION_MECHANISM,
        tags: list[TagUrn],
        created_by: Optional[CorpUserUrn] = None,
        created_at: Union[datetime, None] = None,
        updated_by: Optional[CorpUserUrn] = None,
        updated_at: Optional[datetime] = None,
    ):
        """
        Initialize a column value assertion.

        Args:
            urn: The URN of the assertion.
            dataset_urn: The URN of the dataset to monitor.
            column_name: The name of the column to validate.
            operator: The operator to use for validation.
            criteria_parameters: The criteria parameters for the operator.
            transform: Optional transform to apply to field values before evaluation.
            fail_threshold_type: The type of failure threshold (COUNT or PERCENTAGE).
            fail_threshold_value: The failure threshold value.
            exclude_nulls: Whether to exclude nulls when evaluating.
            display_name: The display name of the assertion.
            mode: The mode of the assertion (active/inactive).
            schedule: The evaluation schedule.
            incident_behavior: The behavior when incidents occur.
            detection_mechanism: The mechanism used to detect changes.
            tags: The tags to apply to the assertion.
            created_by: The URN of the user who created the assertion.
            created_at: The timestamp when the assertion was created.
            updated_by: The URN of the user who last updated the assertion.
            updated_at: The timestamp when the assertion was last updated.
        """
        _AssertionPublic.__init__(
            self,
            urn=urn,
            dataset_urn=dataset_urn,
            display_name=display_name,
            mode=mode,
            tags=tags,
            incident_behavior=incident_behavior,
            detection_mechanism=detection_mechanism,
            created_by=created_by,
            created_at=created_at,
            updated_by=updated_by,
            updated_at=updated_at,
        )
        _HasSchedule.__init__(
            self,
            schedule=schedule,
        )

        self._column_name = column_name
        self._operator = operator
        self._criteria_parameters = criteria_parameters
        self._transform = transform
        self._fail_threshold_type = fail_threshold_type
        self._fail_threshold_value = fail_threshold_value
        self._exclude_nulls = exclude_nulls

    @property
    def column_name(self) -> str:
        return self._column_name

    @property
    def operator(self) -> OperatorInputType:
        return self._operator

    @property
    def criteria_parameters(self) -> Optional[ColumnValueAssertionParameters]:
        return self._criteria_parameters

    @property
    def transform(self) -> Optional[FieldTransformInputType]:
        return self._transform

    @property
    def fail_threshold_type(self) -> FailThresholdType:
        return self._fail_threshold_type

    @property
    def fail_threshold_value(self) -> int:
        return self._fail_threshold_value

    @property
    def exclude_nulls(self) -> bool:
        return self._exclude_nulls

    @classmethod
    def _from_entities(cls, assertion: Assertion, monitor: Monitor) -> Self:
        """
        Create a ColumnValueAssertion from an Assertion and Monitor entity.

        Args:
            assertion: The Assertion entity.
            monitor: The Monitor entity.

        Returns:
            A ColumnValueAssertion instance.
        """
        return cls(
            urn=assertion.urn,
            dataset_urn=assertion.dataset,
            column_name=cls._get_column_name(assertion),
            operator=cls._get_operator(assertion),
            criteria_parameters=cls._get_criteria_parameters(assertion),
            transform=cls._get_transform(assertion),
            fail_threshold_type=cls._get_fail_threshold_type(assertion),
            fail_threshold_value=cls._get_fail_threshold_value(assertion),
            exclude_nulls=cls._get_exclude_nulls(assertion),
            display_name=assertion.description or "",
            mode=cls._get_mode(monitor),
            schedule=cls._get_schedule(monitor),
            incident_behavior=cls._get_incident_behavior(assertion),
            detection_mechanism=cls._get_detection_mechanism(assertion, monitor),
            tags=cls._get_tags(assertion),
            created_by=cls._get_created_by(assertion),
            created_at=cls._get_created_at(assertion),
            updated_by=cls._get_updated_by(assertion),
            updated_at=cls._get_updated_at(assertion),
        )

    @staticmethod
    def _get_column_name(assertion: Assertion) -> str:
        """Get the column name from the assertion."""
        column_name = _get_nested_field_for_entity_with_default(
            assertion,
            field_path="info.fieldValuesAssertion.field.path",
            default=None,
        )
        if column_name is None:
            raise SDKUsageError(
                f"Column name is required for column value assertions. "
                f"Assertion {assertion.urn} does not have a column name"
            )
        return column_name

    @staticmethod
    def _get_operator(assertion: Assertion) -> OperatorInputType:
        """Get the operator from the assertion."""
        operator = _get_nested_field_for_entity_with_default(
            assertion,
            field_path="info.fieldValuesAssertion.operator",
            default=None,
        )
        if operator is None:
            raise SDKUsageError(
                f"Operator is required for column value assertions. "
                f"Assertion {assertion.urn} does not have an operator"
            )
        return operator

    @staticmethod
    def _convert_param_value(
        value: str, param_type: Optional[str]
    ) -> Union[str, int, float]:
        """Convert a string value to the appropriate type based on param_type."""
        if param_type == models.AssertionStdParameterTypeClass.NUMBER:
            # Try to convert to int first, then float
            try:
                if "." in value:
                    return float(value)
                return int(value)
            except (ValueError, TypeError) as e:
                logger.warning(
                    f"Failed to convert value '{value}' to number (type: {param_type}). "
                    f"Returning as string. Error: {e}"
                )
                return value
        # For STRING and other types, return as-is
        return value

    @staticmethod
    def _get_criteria_parameters(
        assertion: Assertion,
    ) -> Optional[ColumnValueAssertionParameters]:
        """Get the criteria parameters from the assertion."""
        # First check if there's a single value parameter
        value_param = _get_nested_field_for_entity_with_default(
            assertion,
            field_path="info.fieldValuesAssertion.parameters.value",
            default=None,
        )
        if value_param is not None:
            return ColumnValueAssertion._convert_param_value(
                value_param.value, getattr(value_param, "type", None)
            )

        # Then check for range parameters
        min_param = _get_nested_field_for_entity_with_default(
            assertion,
            field_path="info.fieldValuesAssertion.parameters.minValue",
            default=None,
        )
        max_param = _get_nested_field_for_entity_with_default(
            assertion,
            field_path="info.fieldValuesAssertion.parameters.maxValue",
            default=None,
        )

        if min_param is not None and max_param is not None:
            min_value = ColumnValueAssertion._convert_param_value(
                min_param.value if hasattr(min_param, "value") else min_param,
                getattr(min_param, "type", None),
            )
            max_value = ColumnValueAssertion._convert_param_value(
                max_param.value if hasattr(max_param, "value") else max_param,
                getattr(max_param, "type", None),
            )
            return (min_value, max_value)

        return None

    @staticmethod
    def _get_criteria_parameters_with_type(
        assertion: Assertion,
    ) -> Optional[tuple]:
        """
        Get criteria parameters along with their type information from the backend.

        Returns:
            For single values: (value, type)
            For ranges: ((min_value, max_value), (min_type, max_type))
            None if no parameters found
        """
        # First check if there's a single value parameter
        value_param = _get_nested_field_for_entity_with_default(
            assertion,
            field_path="info.fieldValuesAssertion.parameters.value",
            default=None,
        )
        if value_param is not None:
            return (value_param.value, value_param.type)

        # Then check for range parameters
        min_param = _get_nested_field_for_entity_with_default(
            assertion,
            field_path="info.fieldValuesAssertion.parameters.minValue",
            default=None,
        )
        max_param = _get_nested_field_for_entity_with_default(
            assertion,
            field_path="info.fieldValuesAssertion.parameters.maxValue",
            default=None,
        )

        if min_param is not None and max_param is not None:
            return (
                (min_param.value, max_param.value),
                (min_param.type, max_param.type),
            )

        return None

    @staticmethod
    def _get_transform(assertion: Assertion) -> Optional[FieldTransformInputType]:
        """Get the transform from the assertion."""
        transform = _get_nested_field_for_entity_with_default(
            assertion,
            field_path="info.fieldValuesAssertion.transform",
            default=None,
        )
        if transform is None:
            return None
        # Return the type from the transform class
        if hasattr(transform, "type"):
            return transform.type
        return None

    @staticmethod
    def _get_fail_threshold_type(assertion: Assertion) -> FailThresholdType:
        """Get the fail threshold type from the assertion."""
        threshold_type = _get_nested_field_for_entity_with_default(
            assertion,
            field_path="info.fieldValuesAssertion.failThreshold.type",
            default=models.FieldValuesFailThresholdTypeClass.COUNT,
        )
        if threshold_type == models.FieldValuesFailThresholdTypeClass.PERCENTAGE:
            return FailThresholdType.PERCENTAGE
        return FailThresholdType.COUNT

    @staticmethod
    def _get_fail_threshold_value(assertion: Assertion) -> int:
        """Get the fail threshold value from the assertion."""
        value = _get_nested_field_for_entity_with_default(
            assertion,
            field_path="info.fieldValuesAssertion.failThreshold.value",
            default=0,
        )
        return int(value)

    @staticmethod
    def _get_exclude_nulls(assertion: Assertion) -> bool:
        """Get the exclude_nulls setting from the assertion."""
        return _get_nested_field_for_entity_with_default(
            assertion,
            field_path="info.fieldValuesAssertion.excludeNulls",
            default=True,
        )

    @staticmethod
    def _get_detection_mechanism(
        assertion: Assertion,
        monitor: Monitor,
        default: Optional[_DetectionMechanismTypes] = DEFAULT_DETECTION_MECHANISM,
    ) -> Optional[_DetectionMechanismTypes]:
        """Get the detection mechanism for column value assertions."""
        parameters = _AssertionPublic._get_validated_detection_context(
            monitor,
            assertion,
            models.AssertionEvaluationParametersTypeClass.DATASET_FIELD,
            models.FieldAssertionInfoClass,
            default,
        )
        if parameters is None:
            return default
        if parameters.datasetFieldParameters is None:
            logger.warning(
                f"Monitor does not have datasetFieldParameters, "
                f"defaulting detection mechanism to {default}"
            )
            return default
        source_type = parameters.datasetFieldParameters.sourceType
        if source_type == models.DatasetFieldAssertionSourceTypeClass.ALL_ROWS_QUERY:
            additional_filter = _AssertionPublic._get_additional_filter(assertion)
            return DetectionMechanism.ALL_ROWS_QUERY(
                additional_filter=additional_filter
            )
        elif (
            source_type
            == models.DatasetFieldAssertionSourceTypeClass.CHANGED_ROWS_QUERY
        ):
            if parameters.datasetFieldParameters.changedRowsField is None:
                logger.warning(
                    f"Monitor has CHANGED_ROWS_QUERY source type but no changedRowsField, "
                    f"defaulting detection mechanism to {default}"
                )
                return default
            column_name = parameters.datasetFieldParameters.changedRowsField.path
            additional_filter = _AssertionPublic._get_additional_filter(assertion)
            return DetectionMechanism.CHANGED_ROWS_QUERY(
                column_name=column_name, additional_filter=additional_filter
            )
        elif (
            source_type
            == models.DatasetFieldAssertionSourceTypeClass.DATAHUB_DATASET_PROFILE
        ):
            return DetectionMechanism.ALL_ROWS_QUERY_DATAHUB_DATASET_PROFILE
        else:
            logger.warning(
                f"Unsupported DatasetFieldAssertionSourceType {source_type}, "
                f"defaulting detection mechanism to {default}"
            )
            return default
