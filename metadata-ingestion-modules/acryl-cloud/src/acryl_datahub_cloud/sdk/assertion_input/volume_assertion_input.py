from abc import ABC
from datetime import datetime
from enum import Enum
from typing import Any, Optional, Tuple, Union

from pydantic import BaseModel, Extra

from acryl_datahub_cloud.sdk.assertion_input.assertion_input import (
    DEFAULT_EVERY_SIX_HOURS_SCHEDULE,
    AssertionIncidentBehavior,
    DetectionMechanismInputTypes,
    FieldSpecType,
    _AssertionInput,
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

# TODO: better naming for "volume assertion definition"


# Type aliases  and enums for volume assertions


class VolumeAssertionDefinitionType(str, Enum):
    ROW_COUNT_TOTAL = models.VolumeAssertionTypeClass.ROW_COUNT_TOTAL
    ROW_COUNT_CHANGE = models.VolumeAssertionTypeClass.ROW_COUNT_CHANGE


# Currently supported volume assertion definition types
CURRENTLY_SUPPORTED_VOLUME_ASSERTION_DEFINITIONS = [
    VolumeAssertionDefinitionType.ROW_COUNT_TOTAL,
    VolumeAssertionDefinitionType.ROW_COUNT_CHANGE,
]


class VolumeAssertionDefinitionChangeKind(str, Enum):
    ABSOLUTE = models.AssertionValueChangeTypeClass.ABSOLUTE
    PERCENTAGE = models.AssertionValueChangeTypeClass.PERCENTAGE


VolumeAssertionDefinitionParameters = Union[float, Tuple[float, float]]


class VolumeAssertionOperator(str, Enum):
    """Valid operators for volume assertions."""

    LESS_THAN_OR_EQUAL_TO = models.AssertionStdOperatorClass.LESS_THAN_OR_EQUAL_TO
    GREATER_THAN_OR_EQUAL_TO = models.AssertionStdOperatorClass.GREATER_THAN_OR_EQUAL_TO
    BETWEEN = models.AssertionStdOperatorClass.BETWEEN


class _AbstractVolumeAssertionDefinition(BaseModel, ABC):
    type: str

    class Config:
        extra = Extra.forbid


class RowCountTotal(_AbstractVolumeAssertionDefinition):
    type: VolumeAssertionDefinitionType = VolumeAssertionDefinitionType.ROW_COUNT_TOTAL
    operator: VolumeAssertionOperator
    parameters: VolumeAssertionDefinitionParameters


class RowCountChange(_AbstractVolumeAssertionDefinition):
    type: VolumeAssertionDefinitionType = VolumeAssertionDefinitionType.ROW_COUNT_CHANGE
    kind: VolumeAssertionDefinitionChangeKind
    operator: VolumeAssertionOperator
    parameters: VolumeAssertionDefinitionParameters


_VOLUME_ASSERTION_DEFINITION_CONCRETE_TYPES = (
    RowCountTotal,
    RowCountChange,
)
_VolumeAssertionDefinitionTypes = Union[
    RowCountTotal,
    RowCountChange,
]

VolumeAssertionDefinitionInputTypes = Union[
    dict[str, Any], _VolumeAssertionDefinitionTypes
]


class VolumeAssertionDefinition:
    ROW_COUNT_TOTAL = RowCountTotal
    ROW_COUNT_CHANGE = RowCountChange

    @staticmethod
    def _validate_between_parameters(
        parameters: VolumeAssertionDefinitionParameters, assertion_type: str
    ) -> None:
        """Validate parameters for BETWEEN operator."""
        if not isinstance(parameters, tuple) or len(parameters) != 2:
            raise SDKUsageError(
                f"For BETWEEN operator in {assertion_type}, parameters must be a tuple of two numbers (min_value, max_value)."
            )

    @staticmethod
    def _validate_single_value_parameters(
        parameters: VolumeAssertionDefinitionParameters,
        operator_enum: VolumeAssertionOperator,
        assertion_type: str,
    ) -> None:
        """Validate parameters for single-value operators."""
        if not isinstance(parameters, (int, float)):
            if isinstance(parameters, tuple):
                raise SDKUsageError(
                    f"For {operator_enum.value} operator in {assertion_type}, parameters must be a single number, not a tuple."
                )
            else:
                raise SDKUsageError(
                    f"For {operator_enum.value} operator in {assertion_type}, parameters must be a single number."
                )

    @staticmethod
    def _parse_operator(
        operator: Union[str, VolumeAssertionOperator],
    ) -> VolumeAssertionOperator:
        """Parse and validate operator input, converting string to enum if needed."""
        if isinstance(operator, str):
            try:
                return VolumeAssertionOperator(operator)
            except ValueError as e:
                valid_operators = ", ".join(
                    [op.value for op in VolumeAssertionOperator]
                )
                raise SDKUsageError(
                    f"Invalid operator '{operator}'. Valid operators: {valid_operators}"
                ) from e
        return operator

    @staticmethod
    def _validate_operator_and_parameters(
        operator: Union[str, VolumeAssertionOperator],
        parameters: VolumeAssertionDefinitionParameters,
        assertion_type: str,
    ) -> None:
        """Validate that operator and parameters are compatible for volume assertions."""
        operator_enum = VolumeAssertionDefinition._parse_operator(operator)

        # Validate parameter structure based on operator
        if operator_enum == VolumeAssertionOperator.BETWEEN:
            VolumeAssertionDefinition._validate_between_parameters(
                parameters, assertion_type
            )
        else:
            VolumeAssertionDefinition._validate_single_value_parameters(
                parameters, operator_enum, assertion_type
            )

    @staticmethod
    def _parse_instantiated_object(
        definition: _VolumeAssertionDefinitionTypes,
    ) -> _VolumeAssertionDefinitionTypes:
        """Parse and validate already instantiated volume assertion objects."""
        VolumeAssertionDefinition._validate_operator_and_parameters(
            definition.operator, definition.parameters, definition.type
        )
        return definition

    @staticmethod
    def _parse_dict_definition(
        definition_dict: dict[str, Any],
    ) -> _VolumeAssertionDefinitionTypes:
        """Parse and validate dictionary-based volume assertion definitions."""
        try:
            assertion_type = definition_dict.pop("type")
        except KeyError as e:
            raise SDKUsageError(
                "Volume assertion definition must include a 'type' field"
            ) from e

        # Check for valid assertion type first
        if assertion_type not in CURRENTLY_SUPPORTED_VOLUME_ASSERTION_DEFINITIONS:
            supported_types = ", ".join(
                [t.value for t in CURRENTLY_SUPPORTED_VOLUME_ASSERTION_DEFINITIONS]
            )
            raise SDKUsageError(
                f"Unknown volume assertion type: {assertion_type}. Supported types: {supported_types}"
            )

        # Extract operator and parameters for validation
        operator = definition_dict.get("operator")
        parameters = definition_dict.get("parameters")

        if operator is None:
            raise SDKUsageError(
                f"Missing required 'operator' field for {assertion_type}"
            )
        if parameters is None:
            raise SDKUsageError(
                f"Missing required 'parameters' field for {assertion_type}"
            )

        # Validate basic parameter type first
        if not isinstance(parameters, (int, float, tuple)):
            raise SDKUsageError(
                f"For {assertion_type}, parameters must be a number or a tuple of two numbers, got: {type(parameters)}"
            )

        # Validate operator and parameters before object creation
        VolumeAssertionDefinition._validate_operator_and_parameters(
            operator, parameters, assertion_type
        )

        # Convert string operator to enum for object creation
        if isinstance(operator, str):
            definition_dict["operator"] = VolumeAssertionDefinition._parse_operator(
                operator
            )

        if assertion_type == VolumeAssertionDefinitionType.ROW_COUNT_TOTAL:
            try:
                return RowCountTotal(**definition_dict)
            except Exception as e:
                raise SDKUsageError(
                    f"Failed to create {VolumeAssertionDefinitionType.ROW_COUNT_TOTAL.value} volume assertion: {str(e)}"
                ) from e
        else:  # assertion_type == VolumeAssertionDefinitionType.ROW_COUNT_CHANGE
            try:
                return RowCountChange(**definition_dict)
            except Exception as e:
                raise SDKUsageError(
                    f"Failed to create {VolumeAssertionDefinitionType.ROW_COUNT_CHANGE.value} volume assertion: {str(e)}"
                ) from e

    @staticmethod
    def parse(
        definition: VolumeAssertionDefinitionInputTypes,
    ) -> _VolumeAssertionDefinitionTypes:
        """Parse and validate a volume assertion definition.

        This method converts dictionary-based volume assertion definitions into typed volume
        assertion objects, or validates already instantiated volume assertion objects. It
        supports two volume assertion types: row_count_total and row_count_change.

        Args:
            definition: A volume assertion definition that can be either:
                - A dictionary containing volume assertion configuration with keys:
                  - type: Must be "row_count_total" or "row_count_change"
                  - operator: Must be "LESS_THAN_OR_EQUAL_TO", "GREATER_THAN_OR_EQUAL_TO", or "BETWEEN"
                  - parameters: Number for single-value operators, tuple of two numbers for BETWEEN
                  - kind: Required for "row_count_change", must be "absolute" or "percent"
                - An already instantiated RowCountTotal or RowCountChange object

        Returns:
            A validated volume assertion definition object (RowCountTotal or RowCountChange).

        Raises:
            SDKUsageError: If the definition is invalid, including:
                - Invalid input type (not dict or volume assertion object)
                - Missing required fields (type, operator, parameters, kind for row_count_change)
                - Unknown assertion type (not row_count_total or row_count_change)
                - Invalid operator (not in allowed operators)
                - Invalid parameter structure for operator:
                  - Single-value operators require number parameters
                  - BETWEEN operator requires tuple of two numbers
                - Object construction failures (extra fields, validation errors)

        Examples:
            Parse a row count total assertion:
            >>> definition = {
            ...     "type": "row_count_total",
            ...     "operator": "GREATER_THAN_OR_EQUAL_TO",
            ...     "parameters": 100
            ... }
            >>> result = VolumeAssertionDefinition.parse(definition)
            >>> isinstance(result, RowCountTotal)
            True

            Parse a row count change assertion with BETWEEN operator:
            >>> definition = {
            ...     "type": "row_count_change",
            ...     "kind": "absolute",
            ...     "operator": "BETWEEN",
            ...     "parameters": (10, 50)
            ... }
            >>> result = VolumeAssertionDefinition.parse(definition)
            >>> isinstance(result, RowCountChange)
            True

            Parse an already instantiated object:
            >>> obj = RowCountTotal(
            ...     operator=VolumeAssertionOperator.LESS_THAN_OR_EQUAL_TO,
            ...     parameters=200
            ... )
            >>> result = VolumeAssertionDefinition.parse(obj)
            >>> result == obj
            True
        """
        # If already instantiated, validate and return
        if isinstance(definition, _VOLUME_ASSERTION_DEFINITION_CONCRETE_TYPES):
            return VolumeAssertionDefinition._parse_instantiated_object(definition)

        if not isinstance(definition, dict):
            raise SDKUsageError(
                f"Volume assertion definition must be a dict or a volume assertion definition object, got: {type(definition)}"
            )

        return VolumeAssertionDefinition._parse_dict_definition(definition.copy())

    @staticmethod
    def build_model_volume_info(
        definition: _VolumeAssertionDefinitionTypes,
        dataset_urn: str,
        filter: Optional[models.DatasetFilterClass] = None,
    ) -> models.VolumeAssertionInfoClass:
        """Build a DataHub VolumeAssertionInfoClass from a validated volume assertion definition.

        This method converts validated volume assertion definition objects into DataHub model
        classes suitable for creating volume assertions in the DataHub metadata service.

        Args:
            definition: A validated volume assertion definition object (RowCountTotal or RowCountChange).
                        This should be the output of VolumeAssertionDefinition.parse().
            dataset_urn: The dataset URN that this assertion applies to.
            filter: Optional filter to apply to the assertion.

        Returns:
            A VolumeAssertionInfoClass configured for the specific volume assertion type.

        Raises:
            SDKUsageError: If the definition type is not supported.
        """
        if isinstance(definition, RowCountTotal):
            volume_info = models.VolumeAssertionInfoClass(
                type=models.VolumeAssertionTypeClass.ROW_COUNT_TOTAL,
                entity=dataset_urn,
                rowCountTotal=models.RowCountTotalClass(
                    operator=definition.operator.value,
                    parameters=VolumeAssertionDefinition._build_assertion_parameters(
                        definition.operator, definition.parameters
                    ),
                ),
            )
            if filter is not None:
                volume_info.filter = filter
            return volume_info
        elif isinstance(definition, RowCountChange):
            # Map kind to DataHub assertion value change type
            change_type = (
                models.AssertionValueChangeTypeClass.ABSOLUTE
                if definition.kind == VolumeAssertionDefinitionChangeKind.ABSOLUTE
                else models.AssertionValueChangeTypeClass.PERCENTAGE
            )

            volume_info = models.VolumeAssertionInfoClass(
                type=models.VolumeAssertionTypeClass.ROW_COUNT_CHANGE,
                entity=dataset_urn,
                rowCountChange=models.RowCountChangeClass(
                    type=change_type,
                    operator=definition.operator.value,
                    parameters=VolumeAssertionDefinition._build_assertion_parameters(
                        definition.operator, definition.parameters
                    ),
                ),
            )
            if filter is not None:
                volume_info.filter = filter
            return volume_info
        else:
            raise SDKUsageError(
                f"Unsupported volume assertion definition type: {type(definition)}"
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
        operator: VolumeAssertionOperator,
        parameters: VolumeAssertionDefinitionParameters,
    ) -> models.AssertionStdParametersClass:
        """Build assertion parameters for DataHub model classes.

        Args:
            operator: The volume assertion operator.
            parameters: The parameters (int for single value, tuple for BETWEEN).

        Returns:
            AssertionStdParametersClass with appropriate parameter structure.
        """
        if operator == VolumeAssertionOperator.BETWEEN:
            assert isinstance(parameters, tuple) and len(parameters) == 2, (
                f"BETWEEN operator requires tuple of two numbers, got: {parameters}"
            )
            # Sort values to ensure minValue is actually the minimum and maxValue is the maximum
            min_val, max_val = sorted(parameters)
            return models.AssertionStdParametersClass(
                minValue=models.AssertionStdParameterClass(
                    value=VolumeAssertionDefinition._format_number_value(min_val),
                    type=models.AssertionStdParameterTypeClass.NUMBER,
                ),
                maxValue=models.AssertionStdParameterClass(
                    value=VolumeAssertionDefinition._format_number_value(max_val),
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
                    value=VolumeAssertionDefinition._format_number_value(parameters),
                    type=models.AssertionStdParameterTypeClass.NUMBER,
                ),
            )

    @staticmethod
    def _extract_volume_parameters(
        assertion_urn: str,
        operator: VolumeAssertionOperator,
        parameters: models.AssertionStdParametersClass,
    ) -> VolumeAssertionDefinitionParameters:
        """Extract parameters from assertion based on operator type."""
        if operator.value == "BETWEEN":
            if parameters.minValue is None or parameters.maxValue is None:
                raise SDKNotYetSupportedError(
                    f"Volume assertion {assertion_urn} has BETWEEN operator but missing min/max values"
                )
            return (float(parameters.minValue.value), float(parameters.maxValue.value))
        else:
            if parameters.value is None:
                raise SDKNotYetSupportedError(
                    f"Volume assertion {assertion_urn} has {operator.value} operator but missing value"
                )
            return float(parameters.value.value)

    @staticmethod
    def _get_row_count_total(assertion: Assertion) -> RowCountTotal:
        """Extract RowCountTotal from assertion."""
        assert isinstance(assertion.info, models.VolumeAssertionInfoClass)
        if assertion.info.rowCountTotal is None:
            raise SDKNotYetSupportedError(
                f"Volume assertion {assertion.urn} has ROW_COUNT_TOTAL type but no rowCountTotal, which is not supported"
            )
        row_count_total = assertion.info.rowCountTotal
        operator = VolumeAssertionOperator(row_count_total.operator)
        parameters = VolumeAssertionDefinition._extract_volume_parameters(
            str(assertion.urn), operator, row_count_total.parameters
        )
        return RowCountTotal(operator=operator, parameters=parameters)

    @staticmethod
    def _get_row_count_change(assertion: Assertion) -> RowCountChange:
        """Extract RowCountChange from assertion."""
        assert isinstance(assertion.info, models.VolumeAssertionInfoClass)
        if assertion.info.rowCountChange is None:
            raise SDKNotYetSupportedError(
                f"Volume assertion {assertion.urn} has ROW_COUNT_CHANGE type but no rowCountChange, which is not supported"
            )
        row_count_change = assertion.info.rowCountChange
        operator = VolumeAssertionOperator(row_count_change.operator)
        parameters = VolumeAssertionDefinition._extract_volume_parameters(
            str(assertion.urn), operator, row_count_change.parameters
        )
        kind: VolumeAssertionDefinitionChangeKind = (
            VolumeAssertionDefinitionChangeKind.ABSOLUTE
            if row_count_change.type == models.AssertionValueChangeTypeClass.ABSOLUTE
            else VolumeAssertionDefinitionChangeKind.PERCENTAGE
        )
        return RowCountChange(operator=operator, parameters=parameters, kind=kind)

    @staticmethod
    def from_assertion(assertion: Assertion) -> _VolumeAssertionDefinitionTypes:
        """Create a volume assertion definition from a DataHub assertion entity.

        Args:
            assertion: The DataHub assertion entity to extract the definition from.

        Returns:
            A volume assertion definition object (RowCountTotal or RowCountChange).

        Raises:
            SDKNotYetSupportedError: If the assertion is not a volume assertion or has
                unsupported configuration.
        """
        if assertion.info is None:
            raise SDKNotYetSupportedError(
                f"Assertion {assertion.urn} does not have a volume assertion info, which is not supported"
            )
        if not isinstance(assertion.info, models.VolumeAssertionInfoClass):
            raise SDKNotYetSupportedError(
                f"Assertion {assertion.urn} is not a volume assertion"
            )

        if assertion.info.type == models.VolumeAssertionTypeClass.ROW_COUNT_TOTAL:
            return VolumeAssertionDefinition._get_row_count_total(assertion)
        elif assertion.info.type == models.VolumeAssertionTypeClass.ROW_COUNT_CHANGE:
            return VolumeAssertionDefinition._get_row_count_change(assertion)
        else:
            raise SDKNotYetSupportedError(
                f"Volume assertion {assertion.urn} has unsupported type {assertion.info.type}"
            )


class _VolumeAssertionInput(_AssertionInput):
    def __init__(
        self,
        *,
        # Required fields
        dataset_urn: Union[str, DatasetUrn],
        entity_client: EntityClient,  # Needed to get the schema field spec for the detection mechanism if needed
        definition: VolumeAssertionDefinitionInputTypes,
        urn: Optional[Union[str, AssertionUrn]] = None,
        # Optional fields
        display_name: Optional[str] = None,
        enabled: bool = True,
        schedule: Optional[Union[str, models.CronScheduleClass]] = None,
        detection_mechanism: DetectionMechanismInputTypes = None,
        incident_behavior: Optional[
            Union[AssertionIncidentBehavior, list[AssertionIncidentBehavior]]
        ] = None,
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

        self.definition = VolumeAssertionDefinition.parse(definition)

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
        return VolumeAssertionDefinition.build_model_volume_info(
            self.definition, str(self.dataset_urn), filter
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
        default_source_type = models.DatasetVolumeSourceTypeClass.INFORMATION_SCHEMA

        if self.detection_mechanism is None:
            return default_source_type, None

        # Convert detection mechanism to volume source type
        if isinstance(self.detection_mechanism, str):
            if self.detection_mechanism == "information_schema":
                return models.DatasetVolumeSourceTypeClass.INFORMATION_SCHEMA, None
            elif self.detection_mechanism == "datahub_operation":
                return models.DatasetVolumeSourceTypeClass.OPERATION, None
            else:
                return default_source_type, None

        # For more complex detection mechanisms, we might need additional logic
        return default_source_type, None
