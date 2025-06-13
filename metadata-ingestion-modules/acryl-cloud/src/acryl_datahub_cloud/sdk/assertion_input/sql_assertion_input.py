from datetime import datetime
from enum import Enum
from typing import Optional, Union

from pydantic import BaseModel

from acryl_datahub_cloud.sdk.assertion_input.assertion_input import (
    DEFAULT_EVERY_SIX_HOURS_SCHEDULE,
    RANGE_OPERATORS,
    SINGLE_VALUE_NUMERIC_OPERATORS,
    AssertionIncidentBehavior,
    FieldSpecType,
    _AssertionInput,
    _try_parse_and_validate_schema_classes_enum,
)
from acryl_datahub_cloud.sdk.entities.assertion import (
    AssertionInfoInputType,
    TagsInputType,
)
from acryl_datahub_cloud.sdk.errors import SDKUsageError
from datahub.metadata import schema_classes as models
from datahub.metadata.urns import AssertionUrn, CorpUserUrn, DatasetUrn
from datahub.sdk.entity_client import EntityClient


class SqlAssertionType(Enum):
    METRIC_CHANGE = models.SqlAssertionTypeClass.METRIC_CHANGE
    METRIC = models.SqlAssertionTypeClass.METRIC


class SqlAssertionChangeType(Enum):
    ABSOLUTE = models.AssertionValueChangeTypeClass.ABSOLUTE
    PERCENTAGE = models.AssertionValueChangeTypeClass.PERCENTAGE


class SqlAssertionOperator(Enum):
    EQUAL_TO = models.AssertionStdOperatorClass.EQUAL_TO
    NOT_EQUAL_TO = models.AssertionStdOperatorClass.NOT_EQUAL_TO
    GREATER_THAN = models.AssertionStdOperatorClass.GREATER_THAN
    LESS_THAN = models.AssertionStdOperatorClass.LESS_THAN
    GREATER_THAN_OR_EQUAL_TO = models.AssertionStdOperatorClass.GREATER_THAN_OR_EQUAL_TO
    LESS_THAN_OR_EQUAL_TO = models.AssertionStdOperatorClass.LESS_THAN_OR_EQUAL_TO
    BETWEEN = models.AssertionStdOperatorClass.BETWEEN


class SqlAssertionCriteria(BaseModel):
    type: Union[SqlAssertionType, str]
    change_type: Optional[Union[SqlAssertionChangeType, str]]
    operator: Union[SqlAssertionOperator, str]
    # Either a single value or a range must be provided
    parameters: Union[Union[float, int], tuple[Union[float, int], Union[float, int]]]

    @classmethod
    def get_type(
        cls, type: Union[SqlAssertionType, str]
    ) -> models.SqlAssertionTypeClass:
        return _try_parse_and_validate_schema_classes_enum(
            type if isinstance(type, str) else str(type.value),
            models.SqlAssertionTypeClass,
        )

    @classmethod
    def get_change_type(
        cls, change_type: Optional[Union[SqlAssertionChangeType, str]]
    ) -> Optional[models.AssertionValueChangeTypeClass]:
        if change_type is None:
            return None
        return _try_parse_and_validate_schema_classes_enum(
            change_type if isinstance(change_type, str) else str(change_type.value),
            models.AssertionValueChangeTypeClass,
        )

    @classmethod
    def get_operator(
        cls, operator: Union[SqlAssertionOperator, str]
    ) -> models.AssertionStdOperatorClass:
        return _try_parse_and_validate_schema_classes_enum(
            operator if isinstance(operator, str) else str(operator.value),
            models.AssertionStdOperatorClass,
        )

    @classmethod
    def get_parameters(
        cls,
        parameters: Union[
            Union[float, int], tuple[Union[float, int], Union[float, int]]
        ],
    ) -> models.AssertionStdParametersClass:
        if isinstance(parameters, (float, int)):
            return models.AssertionStdParametersClass(
                value=models.AssertionStdParameterClass(
                    type=models.AssertionStdParameterTypeClass.NUMBER,
                    value=str(parameters),
                ),
            )
        elif isinstance(parameters, tuple):
            return models.AssertionStdParametersClass(
                minValue=models.AssertionStdParameterClass(
                    type=models.AssertionStdParameterTypeClass.NUMBER,
                    value=str(min(parameters[0], parameters[1])),
                ),
                maxValue=models.AssertionStdParameterClass(
                    type=models.AssertionStdParameterTypeClass.NUMBER,
                    value=str(max(parameters[0], parameters[1])),
                ),
            )
        else:
            raise SDKUsageError(
                "Either a single value or a range must be provided for the parameters of SqlAssertionCriteria"
            )

    @staticmethod
    def validate(criteria: "SqlAssertionCriteria") -> "SqlAssertionCriteria":
        operator = SqlAssertionCriteria.get_operator(criteria.operator)
        if operator in SINGLE_VALUE_NUMERIC_OPERATORS:
            if not isinstance(criteria.parameters, float) and not isinstance(
                criteria.parameters, int
            ):
                raise SDKUsageError(
                    f"The parameter value of SqlAssertionCriteria must be a single value numeric for operator {str(operator)}"
                )
        elif operator in RANGE_OPERATORS:
            if not isinstance(criteria.parameters, tuple):
                raise SDKUsageError(
                    f"The parameter value of SqlAssertionCriteria must be a tuple range for operator {str(operator)}"
                )
        else:
            raise SDKUsageError(
                f"Operator {str(operator)} is not supported for SqlAssertionCriteria"
            )

        if (
            criteria.type == SqlAssertionType.METRIC_CHANGE
            and criteria.change_type is None
        ):
            raise SDKUsageError("Change type is required for metric change assertions")

        if (
            criteria.type == SqlAssertionType.METRIC
            and criteria.change_type is not None
        ):
            raise SDKUsageError("Change type is not allowed for metric assertions")

        return criteria


class _SqlAssertionInput(_AssertionInput):
    def _assertion_type(self) -> str:
        """Get the assertion type."""
        return models.AssertionTypeClass.SQL

    def __init__(
        self,
        *,
        # Required fields
        dataset_urn: Union[str, DatasetUrn],
        entity_client: EntityClient,  # Needed to get the schema field spec for the detection mechanism if needed
        urn: Optional[Union[str, AssertionUrn]] = None,
        criteria: SqlAssertionCriteria,
        statement: str,
        # Optional fields
        display_name: Optional[str] = None,
        enabled: bool = True,
        schedule: Optional[Union[str, models.CronScheduleClass]] = None,
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
            incident_behavior=incident_behavior,
            tags=tags,
            source_type=models.AssertionSourceTypeClass.NATIVE,  # Native assertions are of type native, not inferred
            created_by=created_by,
            created_at=created_at,
            updated_by=updated_by,
            updated_at=updated_at,
        )
        self.criteria = criteria
        self.statement = statement

    def _create_monitor_info(
        self,
        assertion_urn: AssertionUrn,
        status: models.MonitorStatusClass,
        schedule: models.CronScheduleClass,
    ) -> models.MonitorInfoClass:
        """
        Create a MonitorInfoClass with all the necessary components.
        """
        return models.MonitorInfoClass(
            type=models.MonitorTypeClass.ASSERTION,
            status=status,
            assertionMonitor=models.AssertionMonitorClass(
                assertions=[
                    models.AssertionEvaluationSpecClass(
                        assertion=str(assertion_urn),
                        schedule=schedule,
                        parameters=self._get_assertion_evaluation_parameters(
                            "None",
                            None,  # Not used for sql assertions
                        ),
                    )
                ]
            ),
        )

    def _create_assertion_info(
        self,
        filter: Optional[models.DatasetFilterClass],  # Not used for sql assertions
    ) -> AssertionInfoInputType:
        """
        Create a SqlAssertionInfoClass for a sql assertion.

        Returns:
            A SqlAssertionInfoClass configured for sql.
        """
        SqlAssertionCriteria.validate(self.criteria)

        return models.SqlAssertionInfoClass(
            type=SqlAssertionCriteria.get_type(self.criteria.type),
            entity=str(self.dataset_urn),
            statement=self.statement,
            changeType=SqlAssertionCriteria.get_change_type(self.criteria.change_type),
            operator=SqlAssertionCriteria.get_operator(self.criteria.operator),
            parameters=SqlAssertionCriteria.get_parameters(self.criteria.parameters),
        )

    def _get_assertion_evaluation_parameters(
        self,
        source_type: str,  # Not used for sql assertions
        field: Optional[FieldSpecType],  # Not used for sql assertions
    ) -> models.AssertionEvaluationParametersClass:
        return models.AssertionEvaluationParametersClass(
            type=models.AssertionEvaluationParametersTypeClass.DATASET_SQL,
        )

    # Not used for sql assertions
    def _convert_assertion_source_type_and_field(
        self,
    ) -> tuple[str, Optional[FieldSpecType]]:
        return "None", None

    # Not used for sql assertions
    def _create_filter_from_detection_mechanism(
        self,
    ) -> Optional[models.DatasetFilterClass]:
        return None

    def _convert_schedule(self) -> models.CronScheduleClass:
        """Create a schedule for a sql assertion.

        Returns:
            A CronScheduleClass with appropriate schedule settings.
        """
        print(f"self.schedule: {self.schedule}")
        if self.schedule is None:
            return DEFAULT_EVERY_SIX_HOURS_SCHEDULE

        return models.CronScheduleClass(
            cron=self.schedule.cron,
            timezone=self.schedule.timezone,
        )
