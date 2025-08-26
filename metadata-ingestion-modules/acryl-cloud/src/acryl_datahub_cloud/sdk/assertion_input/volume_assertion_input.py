from datetime import datetime
from enum import Enum
from typing import Any, Optional, Tuple, Union

from pydantic import BaseModel, Extra

from acryl_datahub_cloud.sdk.assertion_input.assertion_input import (
    DEFAULT_EVERY_SIX_HOURS_SCHEDULE,
    AssertionIncidentBehaviorInputTypes,
    DetectionMechanismInputTypes,
    FieldSpecType,
    _AssertionInput,
    _DatasetProfile,
    _InformationSchema,
    _Query,
)
from acryl_datahub_cloud.sdk.entities.assertion import (
    Assertion,
    AssertionInfoInputType,
    TagsInputType,
)
from acryl_datahub_cloud.sdk.errors import SDKNotYetSupportedError, SDKUsageError
from datahub.metadata import schema_classes as models
from datahub.metadata.urns import (
    AssertionUrn,
    CorpUserUrn,
    DatasetUrn,
)
from datahub.sdk.entity_client import EntityClient

# Type aliases and enums for volume assertions


class VolumeAssertionCondition(Enum):
    """Valid conditions for volume assertions combining type, operator, and change kind."""

    # Row count total conditions
    ROW_COUNT_IS_LESS_THAN_OR_EQUAL_TO = "ROW_COUNT_IS_LESS_THAN_OR_EQUAL_TO"  # models.VolumeAssertionTypeClass.ROW_COUNT_TOTAL + models.AssertionStdOperatorClass.LESS_THAN_OR_EQUAL_TO
    ROW_COUNT_IS_GREATER_THAN_OR_EQUAL_TO = "ROW_COUNT_IS_GREATER_THAN_OR_EQUAL_TO"  # models.VolumeAssertionTypeClass.ROW_COUNT_TOTAL + models.AssertionStdOperatorClass.GREATER_THAN_OR_EQUAL_TO
    ROW_COUNT_IS_WITHIN_A_RANGE = "ROW_COUNT_IS_WITHIN_A_RANGE"  # models.VolumeAssertionTypeClass.ROW_COUNT_TOTAL + models.AssertionStdOperatorClass.BETWEEN

    # Row count change conditions - absolute
    ROW_COUNT_GROWS_BY_AT_MOST_ABSOLUTE = "ROW_COUNT_GROWS_BY_AT_MOST_ABSOLUTE"  # models.VolumeAssertionTypeClass.ROW_COUNT_CHANGE + models.AssertionStdOperatorClass.LESS_THAN_OR_EQUAL_TO + models.AssertionValueChangeTypeClass.ABSOLUTE
    ROW_COUNT_GROWS_BY_AT_LEAST_ABSOLUTE = "ROW_COUNT_GROWS_BY_AT_LEAST_ABSOLUTE"  # models.VolumeAssertionTypeClass.ROW_COUNT_CHANGE + models.AssertionStdOperatorClass.GREATER_THAN_OR_EQUAL_TO + models.AssertionValueChangeTypeClass.ABSOLUTE
    ROW_COUNT_GROWS_WITHIN_A_RANGE_ABSOLUTE = "ROW_COUNT_GROWS_WITHIN_A_RANGE_ABSOLUTE"  # models.VolumeAssertionTypeClass.ROW_COUNT_CHANGE + models.AssertionStdOperatorClass.BETWEEN + models.AssertionValueChangeTypeClass.ABSOLUTE

    # Row count change conditions - percentage
    ROW_COUNT_GROWS_BY_AT_MOST_PERCENTAGE = "ROW_COUNT_GROWS_BY_AT_MOST_PERCENTAGE"  # models.VolumeAssertionTypeClass.ROW_COUNT_CHANGE + models.AssertionStdOperatorClass.LESS_THAN_OR_EQUAL_TO + models.AssertionValueChangeTypeClass.PERCENTAGE
    ROW_COUNT_GROWS_BY_AT_LEAST_PERCENTAGE = "ROW_COUNT_GROWS_BY_AT_LEAST_PERCENTAGE"  # models.VolumeAssertionTypeClass.ROW_COUNT_CHANGE + models.AssertionStdOperatorClass.GREATER_THAN_OR_EQUAL_TO + models.AssertionValueChangeTypeClass.PERCENTAGE
    ROW_COUNT_GROWS_WITHIN_A_RANGE_PERCENTAGE = "ROW_COUNT_GROWS_WITHIN_A_RANGE_PERCENTAGE"  # models.VolumeAssertionTypeClass.ROW_COUNT_CHANGE + models.AssertionStdOperatorClass.BETWEEN + models.AssertionValueChangeTypeClass.PERCENTAGE


VolumeAssertionDefinitionParameters = Union[float, Tuple[float, float]]

VolumeAssertionCriteriaInputTypes = Union[dict[str, Any], "VolumeAssertionCriteria"]


class VolumeAssertionCriteria(BaseModel):
    condition: VolumeAssertionCondition
    parameters: VolumeAssertionDefinitionParameters

    class Config:
        extra = Extra.forbid

    @staticmethod
    def parse(criteria: VolumeAssertionCriteriaInputTypes) -> "VolumeAssertionCriteria":
        """Parse and validate volume assertion criteria.

        This method converts dictionary-based volume assertion criteria into typed volume
        assertion objects, or validates already instantiated volume assertion objects. It
        supports nine volume assertion conditions covering both total row count checks and
        row count change monitoring with absolute or percentage thresholds.

        Args:
            criteria: A volume assertion criteria that can be either:
                - A dictionary containing volume assertion configuration with keys:
                  - condition: Must be one of the VolumeAssertionCondition enum values:
                    - "ROW_COUNT_IS_LESS_THAN_OR_EQUAL_TO": Total row count threshold (upper bound)
                    - "ROW_COUNT_IS_GREATER_THAN_OR_EQUAL_TO": Total row count threshold (lower bound)
                    - "ROW_COUNT_IS_WITHIN_A_RANGE": Total row count within specified range
                    - "ROW_COUNT_GROWS_BY_AT_MOST_ABSOLUTE": Row count change upper bound (absolute)
                    - "ROW_COUNT_GROWS_BY_AT_LEAST_ABSOLUTE": Row count change lower bound (absolute)
                    - "ROW_COUNT_GROWS_WITHIN_A_RANGE_ABSOLUTE": Row count change within range (absolute)
                    - "ROW_COUNT_GROWS_BY_AT_MOST_PERCENTAGE": Row count change upper bound (percentage)
                    - "ROW_COUNT_GROWS_BY_AT_LEAST_PERCENTAGE": Row count change lower bound (percentage)
                    - "ROW_COUNT_GROWS_WITHIN_A_RANGE_PERCENTAGE": Row count change within range (percentage)
                  - parameters: Numeric threshold(s) for the condition:
                    - Single number (int/float) for single-bound conditions
                    - Tuple of two numbers for range conditions (WITHIN_A_RANGE)
                - An already instantiated VolumeAssertionCriteria object

        Returns:
            A validated VolumeAssertionCriteria object with the specified condition and parameters.

        Raises:
            SDKUsageError: If the criteria is invalid, including:
                - Invalid input type (not dict or VolumeAssertionCriteria object)
                - Missing required fields (condition or parameters)
                - Invalid condition value (not in VolumeAssertionCondition enum)
                - Invalid parameter structure for condition:
                  - Single-bound conditions require a single number
                  - Range conditions require a tuple of two numbers
                  - Parameters must be numeric (int or float)
                - Parameter validation failures (negative values, invalid ranges)

        Examples:
            Parse a total row count assertion with single threshold:
            >>> criteria = {
            ...     "condition": "ROW_COUNT_IS_GREATER_THAN_OR_EQUAL_TO",
            ...     "parameters": 100
            ... }
            >>> result = VolumeAssertionCriteria.parse(criteria)
            >>> result.condition.value
            "ROW_COUNT_IS_GREATER_THAN_OR_EQUAL_TO"

            Parse a row count change assertion with range:
            >>> criteria = {
            ...     "condition": "ROW_COUNT_GROWS_WITHIN_A_RANGE_PERCENTAGE",
            ...     "parameters": (10.0, 50.0)
            ... }
            >>> result = VolumeAssertionCriteria.parse(criteria)
            >>> result.parameters
            (10.0, 50.0)

            Parse an already instantiated object:
            >>> obj = VolumeAssertionCriteria(
            ...     condition=VolumeAssertionCondition.ROW_COUNT_IS_LESS_THAN_OR_EQUAL_TO,
            ...     parameters=200
            ... )
            >>> result = VolumeAssertionCriteria.parse(obj)
            >>> result == obj
            True
        """
        if isinstance(criteria, VolumeAssertionCriteria):
            return criteria

        if isinstance(criteria, dict):
            condition = criteria.get("condition")
            parameters = criteria.get("parameters")

            if condition is None:
                raise SDKUsageError(
                    "Volume assertion criteria must include a 'condition' field"
                )
            if parameters is None:
                raise SDKUsageError(
                    "Volume assertion criteria must include a 'parameters' field"
                )

            # Validate condition and parameters compatibility
            VolumeAssertionCriteria._validate_condition_and_parameters(
                condition, parameters
            )

            return VolumeAssertionCriteria(condition=condition, parameters=parameters)

        raise SDKUsageError(
            f"Volume assertion criteria must be a dict or VolumeAssertionCriteria object, got: {type(criteria)}"
        )

    @staticmethod
    def build_model_volume_info(
        criteria: "VolumeAssertionCriteria",
        dataset_urn: str,
        filter: Optional[models.DatasetFilterClass] = None,
    ) -> models.VolumeAssertionInfoClass:
        """Build a DataHub VolumeAssertionInfoClass from volume assertion criteria."""
        condition = criteria.condition
        parameters = criteria.parameters

        # Convert condition to DataHub models based on condition type
        if condition.value.startswith("ROW_COUNT_IS_"):
            volume_info = VolumeAssertionCriteria._build_row_count_total_info(
                condition, parameters, dataset_urn
            )
        elif condition.value.startswith("ROW_COUNT_GROWS_"):
            volume_info = VolumeAssertionCriteria._build_row_count_change_info(
                condition, parameters, dataset_urn
            )
        else:
            raise SDKUsageError(f"Unsupported volume assertion condition: {condition}")

        if filter is not None:
            volume_info.filter = filter
        return volume_info

    @staticmethod
    def _build_row_count_total_info(
        condition: VolumeAssertionCondition,
        parameters: VolumeAssertionDefinitionParameters,
        dataset_urn: str,
    ) -> models.VolumeAssertionInfoClass:
        """Build VolumeAssertionInfoClass for row count total assertions."""
        if condition.value.endswith("_WITHIN_A_RANGE"):
            operator = models.AssertionStdOperatorClass.BETWEEN
        elif condition.value.endswith("_LESS_THAN_OR_EQUAL_TO"):
            operator = models.AssertionStdOperatorClass.LESS_THAN_OR_EQUAL_TO
        elif condition.value.endswith("_GREATER_THAN_OR_EQUAL_TO"):
            operator = models.AssertionStdOperatorClass.GREATER_THAN_OR_EQUAL_TO
        else:
            raise SDKUsageError(f"Unknown row count condition: {condition}")

        return models.VolumeAssertionInfoClass(
            type=models.VolumeAssertionTypeClass.ROW_COUNT_TOTAL,
            entity=dataset_urn,
            rowCountTotal=models.RowCountTotalClass(
                operator=operator,
                parameters=VolumeAssertionCriteria._build_assertion_parameters(
                    operator, parameters
                ),
            ),
        )

    @staticmethod
    def _build_row_count_change_info(
        condition: VolumeAssertionCondition,
        parameters: VolumeAssertionDefinitionParameters,
        dataset_urn: str,
    ) -> models.VolumeAssertionInfoClass:
        """Build VolumeAssertionInfoClass for row count change assertions."""
        # Determine operator
        if condition.value.endswith(
            "_WITHIN_A_RANGE_ABSOLUTE"
        ) or condition.value.endswith("_WITHIN_A_RANGE_PERCENTAGE"):
            operator = models.AssertionStdOperatorClass.BETWEEN
        elif condition.value.endswith("_AT_MOST_ABSOLUTE") or condition.value.endswith(
            "_AT_MOST_PERCENTAGE"
        ):
            operator = models.AssertionStdOperatorClass.LESS_THAN_OR_EQUAL_TO
        elif condition.value.endswith("_AT_LEAST_ABSOLUTE") or condition.value.endswith(
            "_AT_LEAST_PERCENTAGE"
        ):
            operator = models.AssertionStdOperatorClass.GREATER_THAN_OR_EQUAL_TO
        else:
            raise SDKUsageError(f"Unknown row count change condition: {condition}")

        # Determine change type
        if condition.value.endswith("_ABSOLUTE") or condition.value.endswith(
            "_ABSOLUTE_WITHIN_A_RANGE"
        ):
            change_type = models.AssertionValueChangeTypeClass.ABSOLUTE
        elif condition.value.endswith("_PERCENTAGE") or condition.value.endswith(
            "_PERCENTAGE_WITHIN_A_RANGE"
        ):
            change_type = models.AssertionValueChangeTypeClass.PERCENTAGE
        else:
            raise SDKUsageError(
                f"Cannot determine change type for condition: {condition}"
            )

        return models.VolumeAssertionInfoClass(
            type=models.VolumeAssertionTypeClass.ROW_COUNT_CHANGE,
            entity=dataset_urn,
            rowCountChange=models.RowCountChangeClass(
                type=change_type,
                operator=operator,
                parameters=VolumeAssertionCriteria._build_assertion_parameters(
                    operator, parameters
                ),
            ),
        )

    @staticmethod
    def from_assertion(assertion: Assertion) -> "VolumeAssertionCriteria":
        """Create volume assertion criteria from a DataHub assertion entity."""
        VolumeAssertionCriteria._validate_assertion_info(assertion)

        # Type narrowing: we know assertion.info is VolumeAssertionInfoClass after validation
        assert isinstance(assertion.info, models.VolumeAssertionInfoClass)

        if assertion.info.type == models.VolumeAssertionTypeClass.ROW_COUNT_TOTAL:
            return VolumeAssertionCriteria._extract_row_count_total_criteria(assertion)
        elif assertion.info.type == models.VolumeAssertionTypeClass.ROW_COUNT_CHANGE:
            return VolumeAssertionCriteria._extract_row_count_change_criteria(assertion)
        else:
            raise SDKNotYetSupportedError(
                f"Unsupported volume assertion type: {assertion.info.type}"
            )

    @staticmethod
    def _validate_assertion_info(assertion: Assertion) -> None:
        """Validate that assertion has valid volume assertion info."""
        if assertion.info is None:
            raise SDKNotYetSupportedError(
                f"Assertion {assertion.urn} does not have a volume assertion info, which is not supported"
            )
        if not isinstance(assertion.info, models.VolumeAssertionInfoClass):
            raise SDKNotYetSupportedError(
                f"Assertion {assertion.urn} is not a volume assertion"
            )

    @staticmethod
    def _extract_row_count_total_criteria(
        assertion: Assertion,
    ) -> "VolumeAssertionCriteria":
        """Extract criteria from row count total assertion."""
        # Type narrowing: we know assertion.info is VolumeAssertionInfoClass
        assert isinstance(assertion.info, models.VolumeAssertionInfoClass)

        if assertion.info.rowCountTotal is None:
            raise SDKNotYetSupportedError(
                f"Volume assertion {assertion.urn} has ROW_COUNT_TOTAL type but no rowCountTotal"
            )

        operator = assertion.info.rowCountTotal.operator
        parameters = VolumeAssertionCriteria._extract_volume_parameters(
            str(assertion.urn), str(operator), assertion.info.rowCountTotal.parameters
        )

        if operator == models.AssertionStdOperatorClass.LESS_THAN_OR_EQUAL_TO:
            condition = VolumeAssertionCondition.ROW_COUNT_IS_LESS_THAN_OR_EQUAL_TO
        elif operator == models.AssertionStdOperatorClass.GREATER_THAN_OR_EQUAL_TO:
            condition = VolumeAssertionCondition.ROW_COUNT_IS_GREATER_THAN_OR_EQUAL_TO
        elif operator == models.AssertionStdOperatorClass.BETWEEN:
            condition = VolumeAssertionCondition.ROW_COUNT_IS_WITHIN_A_RANGE
        else:
            raise SDKNotYetSupportedError(
                f"Unsupported operator for row count total: {operator}"
            )

        return VolumeAssertionCriteria(condition=condition, parameters=parameters)

    @staticmethod
    def _extract_row_count_change_criteria(
        assertion: Assertion,
    ) -> "VolumeAssertionCriteria":
        """Extract criteria from row count change assertion."""
        # Type narrowing: we know assertion.info is VolumeAssertionInfoClass
        assert isinstance(assertion.info, models.VolumeAssertionInfoClass)

        if assertion.info.rowCountChange is None:
            raise SDKNotYetSupportedError(
                f"Volume assertion {assertion.urn} has ROW_COUNT_CHANGE type but no rowCountChange"
            )

        operator = assertion.info.rowCountChange.operator
        change_type = assertion.info.rowCountChange.type
        parameters = VolumeAssertionCriteria._extract_volume_parameters(
            str(assertion.urn), str(operator), assertion.info.rowCountChange.parameters
        )

        # Determine condition based on operator and change type
        if operator == models.AssertionStdOperatorClass.LESS_THAN_OR_EQUAL_TO:
            condition = (
                VolumeAssertionCondition.ROW_COUNT_GROWS_BY_AT_MOST_ABSOLUTE
                if change_type == models.AssertionValueChangeTypeClass.ABSOLUTE
                else VolumeAssertionCondition.ROW_COUNT_GROWS_BY_AT_MOST_PERCENTAGE
            )
        elif operator == models.AssertionStdOperatorClass.GREATER_THAN_OR_EQUAL_TO:
            condition = (
                VolumeAssertionCondition.ROW_COUNT_GROWS_BY_AT_LEAST_ABSOLUTE
                if change_type == models.AssertionValueChangeTypeClass.ABSOLUTE
                else VolumeAssertionCondition.ROW_COUNT_GROWS_BY_AT_LEAST_PERCENTAGE
            )
        elif operator == models.AssertionStdOperatorClass.BETWEEN:
            condition = (
                VolumeAssertionCondition.ROW_COUNT_GROWS_WITHIN_A_RANGE_ABSOLUTE
                if change_type == models.AssertionValueChangeTypeClass.ABSOLUTE
                else VolumeAssertionCondition.ROW_COUNT_GROWS_WITHIN_A_RANGE_PERCENTAGE
            )
        else:
            raise SDKNotYetSupportedError(
                f"Unsupported operator for row count change: {operator}"
            )

        return VolumeAssertionCriteria(condition=condition, parameters=parameters)

    @staticmethod
    def _extract_volume_parameters(
        assertion_urn: str,
        operator: str,
        parameters: models.AssertionStdParametersClass,
    ) -> VolumeAssertionDefinitionParameters:
        """Extract parameters from assertion based on operator type."""
        if operator == "BETWEEN":
            if parameters.minValue is None or parameters.maxValue is None:
                raise SDKNotYetSupportedError(
                    f"Volume assertion {assertion_urn} has BETWEEN operator but missing min/max values"
                )
            return (float(parameters.minValue.value), float(parameters.maxValue.value))
        else:
            if parameters.value is None:
                raise SDKNotYetSupportedError(
                    f"Volume assertion {assertion_urn} has {operator} operator but missing value"
                )
            return float(parameters.value.value)

    @staticmethod
    def _validate_between_parameters(
        parameters: VolumeAssertionDefinitionParameters, condition: str
    ) -> None:
        """Validate parameters for WITHIN_A_RANGE conditions."""
        if not isinstance(parameters, tuple) or len(parameters) != 2:
            raise SDKUsageError(
                f"For WITHIN_A_RANGE condition {condition}, parameters must be a tuple of two numbers (min_value, max_value)."
            )

    @staticmethod
    def _validate_single_value_parameters(
        parameters: VolumeAssertionDefinitionParameters,
        condition: str,
    ) -> None:
        """Validate parameters for single-value conditions."""
        if not isinstance(parameters, (int, float)):
            if isinstance(parameters, tuple):
                raise SDKUsageError(
                    f"For condition {condition}, parameters must be a single number, not a tuple."
                )
            else:
                raise SDKUsageError(
                    f"For condition {condition}, parameters must be a single number."
                )

    @staticmethod
    def _parse_condition(
        condition: Union[str, VolumeAssertionCondition],
    ) -> VolumeAssertionCondition:
        """Parse and validate condition input, converting string to enum if needed."""
        if isinstance(condition, str):
            try:
                return VolumeAssertionCondition(condition)
            except ValueError as e:
                valid_conditions = ", ".join(
                    [cond.value for cond in VolumeAssertionCondition]
                )
                raise SDKUsageError(
                    f"Invalid condition '{condition}'. Valid conditions: {valid_conditions}"
                ) from e
        return condition

    @staticmethod
    def _validate_condition_and_parameters(
        condition: Union[str, VolumeAssertionCondition],
        parameters: VolumeAssertionDefinitionParameters,
    ) -> None:
        """Validate that condition and parameters are compatible for volume assertions."""
        condition_enum = VolumeAssertionCriteria._parse_condition(condition)

        # Validate parameter structure based on condition
        if (
            condition_enum.value.endswith("_WITHIN_A_RANGE")
            or condition_enum.value.endswith("_WITHIN_A_RANGE_ABSOLUTE")
            or condition_enum.value.endswith("_WITHIN_A_RANGE_PERCENTAGE")
        ):
            VolumeAssertionCriteria._validate_between_parameters(
                parameters, condition_enum.value
            )
        else:
            VolumeAssertionCriteria._validate_single_value_parameters(
                parameters, condition_enum.value
            )

    @staticmethod
    def _format_number_value(value: Union[int, float]) -> str:
        """Format number value for DataHub parameter strings.

        Converts whole numbers to integers (100.0 -> "100") and keeps decimals (100.5 -> "100.5").
        """
        if isinstance(value, float) and value.is_integer():
            return str(int(value))
        return str(value)

    @staticmethod
    def _build_assertion_parameters(
        operator: str,
        parameters: VolumeAssertionDefinitionParameters,
    ) -> models.AssertionStdParametersClass:
        """Build assertion parameters for DataHub model classes.

        Args:
            operator: The assertion operator (from models.AssertionStdOperatorClass).
            parameters: The parameters (int for single value, tuple for BETWEEN).

        Returns:
            AssertionStdParametersClass with appropriate parameter structure.
        """
        if operator == models.AssertionStdOperatorClass.BETWEEN:
            assert isinstance(parameters, tuple) and len(parameters) == 2, (
                f"BETWEEN operator requires tuple of two numbers, got: {parameters}"
            )
            # Sort values to ensure minValue is actually the minimum and maxValue is the maximum
            min_val, max_val = sorted(parameters)
            return models.AssertionStdParametersClass(
                minValue=models.AssertionStdParameterClass(
                    value=VolumeAssertionCriteria._format_number_value(min_val),
                    type=models.AssertionStdParameterTypeClass.NUMBER,
                ),
                maxValue=models.AssertionStdParameterClass(
                    value=VolumeAssertionCriteria._format_number_value(max_val),
                    type=models.AssertionStdParameterTypeClass.NUMBER,
                ),
            )
        else:
            # Single value operators
            assert isinstance(parameters, (int, float)), (
                f"Single value operator {operator} requires number parameter, got: {parameters}"
            )
            return models.AssertionStdParametersClass(
                value=models.AssertionStdParameterClass(
                    value=VolumeAssertionCriteria._format_number_value(parameters),
                    type=models.AssertionStdParameterTypeClass.NUMBER,
                ),
            )


class _VolumeAssertionInput(_AssertionInput):
    def __init__(
        self,
        *,
        # Required fields
        dataset_urn: Union[str, DatasetUrn],
        entity_client: EntityClient,  # Needed to get the schema field spec for the detection mechanism if needed
        criteria: VolumeAssertionCriteriaInputTypes,
        urn: Optional[Union[str, AssertionUrn]] = None,
        # Optional fields
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
    ):
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
            source_type=models.AssertionSourceTypeClass.NATIVE,  # Native assertions are of type native, not inferred
            created_by=created_by,
            created_at=created_at,
            updated_by=updated_by,
            updated_at=updated_at,
        )

        self.criteria = VolumeAssertionCriteria.parse(criteria)

    def _assertion_type(self) -> str:
        return models.AssertionTypeClass.VOLUME

    def _create_assertion_info(
        self, filter: Optional[models.DatasetFilterClass]
    ) -> AssertionInfoInputType:
        """
        Create a VolumeAssertionInfoClass for a volume assertion.

        Args:
            filter: Optional filter to apply to the assertion.

        Returns:
            A VolumeAssertionInfoClass configured for volume assertions.
        """
        return VolumeAssertionCriteria.build_model_volume_info(
            self.criteria, str(self.dataset_urn), filter
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
                    )
                ]
            ),
        )

    def _convert_schedule(self) -> models.CronScheduleClass:
        """Create a schedule for a volume assertion.

        Returns:
            A CronScheduleClass with appropriate schedule settings.
        """
        if self.schedule is None:
            return DEFAULT_EVERY_SIX_HOURS_SCHEDULE

        return models.CronScheduleClass(
            cron=self.schedule.cron,
            timezone=self.schedule.timezone,
        )

    def _get_assertion_evaluation_parameters(
        self, source_type: str, field: Optional[FieldSpecType]
    ) -> models.AssertionEvaluationParametersClass:
        return models.AssertionEvaluationParametersClass(
            type=models.AssertionEvaluationParametersTypeClass.DATASET_VOLUME,
            datasetVolumeParameters=models.DatasetVolumeAssertionParametersClass(
                sourceType=source_type
            ),
        )

    def _convert_assertion_source_type_and_field(
        self,
    ) -> tuple[str, Optional[FieldSpecType]]:
        """Convert the detection mechanism to source type and field."""
        source_type = models.DatasetVolumeSourceTypeClass.INFORMATION_SCHEMA
        field = None

        if self.detection_mechanism is None:
            return source_type, field

        if isinstance(self.detection_mechanism, _Query):
            source_type = models.DatasetVolumeSourceTypeClass.QUERY
        elif isinstance(self.detection_mechanism, _InformationSchema):
            source_type = models.DatasetVolumeSourceTypeClass.INFORMATION_SCHEMA
        elif isinstance(self.detection_mechanism, _DatasetProfile):
            source_type = models.DatasetVolumeSourceTypeClass.DATAHUB_DATASET_PROFILE
        else:
            raise SDKNotYetSupportedError(
                f"Detection mechanism {self.detection_mechanism} not yet supported for volume assertions"
            )

        return source_type, field
