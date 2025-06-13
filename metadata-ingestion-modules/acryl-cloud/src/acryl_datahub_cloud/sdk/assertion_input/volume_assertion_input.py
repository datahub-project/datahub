from abc import ABC
from datetime import datetime
from enum import Enum
from typing import Any, Optional, Tuple, Union

from pydantic import BaseModel, Extra
from typing_extensions import Literal

from acryl_datahub_cloud.sdk.assertion_input.assertion_input import (
    AssertionIncidentBehavior,
    DetectionMechanismInputTypes,
    _AssertionInput,
)
from acryl_datahub_cloud.sdk.entities.assertion import (
    AssertionInfoInputType,
    TagsInputType,
)
from acryl_datahub_cloud.sdk.errors import SDKUsageError
from datahub.metadata import schema_classes as models
from datahub.metadata.urns import (
    AssertionUrn,
    CorpUserUrn,
    DatasetUrn,
)
from datahub.sdk.entity_client import EntityClient

# TODO: better naming for "volume assertion definition"


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
    type: Literal["row_count_total"] = "row_count_total"
    operator: VolumeAssertionOperator
    parameters: Union[int, Tuple[int, int]]


class RowCountChange(_AbstractVolumeAssertionDefinition):
    type: Literal["row_count_change"] = "row_count_change"
    kind: Literal["absolute", "percent"]
    operator: VolumeAssertionOperator
    parameters: Union[int, Tuple[int, int]]


_VOLUME_ASSERTION_DEFINITION_CONCRETE_TYPES = (
    RowCountTotal,
    RowCountChange,
)
_VolumeAssertionDefinitionTypes = Union[
    RowCountTotal,
    RowCountChange,
]


class VolumeAssertionDefinition:
    ROW_COUNT_TOTAL = RowCountTotal
    ROW_COUNT_CHANGE = RowCountChange

    @staticmethod
    def _validate_between_parameters(
        parameters: Union[int, Tuple[int, int]], assertion_type: str
    ) -> None:
        """Validate parameters for BETWEEN operator."""
        if not isinstance(parameters, tuple) or len(parameters) != 2:
            raise SDKUsageError(
                f"For BETWEEN operator in {assertion_type}, parameters must be a tuple of two integers (min_value, max_value)."
            )

    @staticmethod
    def _validate_single_value_parameters(
        parameters: Union[int, Tuple[int, int]],
        operator_enum: VolumeAssertionOperator,
        assertion_type: str,
    ) -> None:
        """Validate parameters for single-value operators."""
        if not isinstance(parameters, int):
            if isinstance(parameters, tuple):
                raise SDKUsageError(
                    f"For {operator_enum.value} operator in {assertion_type}, parameters must be a single integer, not a tuple."
                )
            else:
                raise SDKUsageError(
                    f"For {operator_enum.value} operator in {assertion_type}, parameters must be a single integer."
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
        parameters: Union[int, Tuple[int, int]],
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
        if assertion_type not in ["row_count_total", "row_count_change"]:
            raise SDKUsageError(
                f"Unknown volume assertion type: {assertion_type}. Supported types: row_count_total, row_count_change"
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
        if not isinstance(parameters, (int, tuple)):
            raise SDKUsageError(
                f"For {assertion_type}, parameters must be an integer or a tuple of two integers, got: {type(parameters)}"
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

        if assertion_type == "row_count_total":
            try:
                return RowCountTotal(**definition_dict)
            except Exception as e:
                raise SDKUsageError(
                    f"Failed to create row_count_total volume assertion: {str(e)}"
                ) from e
        else:  # assertion_type == "row_count_change"
            try:
                return RowCountChange(**definition_dict)
            except Exception as e:
                raise SDKUsageError(
                    f"Failed to create row_count_change volume assertion: {str(e)}"
                ) from e

    @staticmethod
    def parse(
        definition: Union[dict[str, Any], _VolumeAssertionDefinitionTypes],
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
                  - parameters: Integer for single-value operators, tuple of two integers for BETWEEN
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
                  - Single-value operators require integer parameters
                  - BETWEEN operator requires tuple of two integers
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
                if definition.kind == "absolute"
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
    def _build_assertion_parameters(
        operator: VolumeAssertionOperator,
        parameters: Union[int, Tuple[int, int]],
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
                f"BETWEEN operator requires tuple of two integers, got: {parameters}"
            )
            # Sort values to ensure minValue is actually the minimum and maxValue is the maximum
            min_val, max_val = sorted(parameters)
            return models.AssertionStdParametersClass(
                minValue=models.AssertionStdParameterClass(
                    value=str(min_val),
                    type=models.AssertionStdParameterTypeClass.NUMBER,
                ),
                maxValue=models.AssertionStdParameterClass(
                    value=str(max_val),
                    type=models.AssertionStdParameterTypeClass.NUMBER,
                ),
            )
        else:
            # Single value operators
            assert isinstance(parameters, int), (
                f"Single value operator {operator} requires integer parameter, got: {parameters}"
            )
            return models.AssertionStdParametersClass(
                value=models.AssertionStdParameterClass(
                    value=str(parameters),
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
        # volume assertion fields
        definition: Optional[
            _VolumeAssertionDefinitionTypes
        ] = None,  # TBC: default value does not make sense
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

        if definition is None:
            raise SDKUsageError("Volume assertion definition is required")
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
