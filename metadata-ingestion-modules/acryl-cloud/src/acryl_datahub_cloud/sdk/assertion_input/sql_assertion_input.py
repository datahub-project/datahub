from datetime import datetime
from enum import Enum
from typing import Optional, Union

from pydantic import BaseModel

from acryl_datahub_cloud.sdk.assertion_input.assertion_input import (
    DEFAULT_EVERY_SIX_HOURS_SCHEDULE,
    RANGE_OPERATORS,
    SINGLE_VALUE_NUMERIC_OPERATORS,
    AssertionIncidentBehaviorInputTypes,
    FieldSpecType,
    _AssertionInput,
)
from acryl_datahub_cloud.sdk.entities.assertion import (
    AssertionInfoInputType,
    TagsInputType,
)
from acryl_datahub_cloud.sdk.errors import SDKUsageError
from datahub.metadata import schema_classes as models
from datahub.metadata.urns import AssertionUrn, CorpUserUrn, DatasetUrn
from datahub.sdk.entity_client import EntityClient


class SqlAssertionCondition(Enum):
    IS_EQUAL_TO = "IS_EQUAL_TO"  # models.SqlAssertionTypeClass.METRIC + models.AssertionStdOperatorClass.EQUAL_TO
    IS_NOT_EQUAL_TO = "IS_NOT_EQUAL_TO"  # models.SqlAssertionTypeClass.METRIC + models.AssertionStdOperatorClass.NOT_EQUAL_TO
    IS_GREATER_THAN = "IS_GREATER_THAN"  # models.SqlAssertionTypeClass.METRIC + models.AssertionStdOperatorClass.GREATER_THAN
    IS_LESS_THAN = "IS_LESS_THAN"  # models.SqlAssertionTypeClass.METRIC + models.AssertionStdOperatorClass.LESS_THAN
    IS_WITHIN_A_RANGE = "IS_WITHIN_A_RANGE"  # models.SqlAssertionTypeClass.METRIC + models.AssertionStdOperatorClass.BETWEEN
    GROWS_AT_MOST_ABSOLUTE = "GROWS_AT_MOST_ABSOLUTE"  # models.SqlAssertionTypeClass.METRIC_CHANGE + models.AssertionStdOperatorClass.LESS_THAN_OR_EQUAL_TO + models.AssertionValueChangeTypeClass.ABSOLUTE
    GROWS_AT_MOST_PERCENTAGE = "GROWS_AT_MOST_PERCENTAGE"  # models.SqlAssertionTypeClass.METRIC_CHANGE + models.AssertionStdOperatorClass.LESS_THAN_OR_EQUAL_TO + models.AssertionValueChangeTypeClass.PERCENTAGE
    GROWS_AT_LEAST_ABSOLUTE = "GROWS_AT_LEAST_ABSOLUTE"  # models.SqlAssertionTypeClass.METRIC_CHANGE + models.AssertionStdOperatorClass.GREATER_THAN_OR_EQUAL_TO + models.AssertionValueChangeTypeClass.ABSOLUTE
    GROWS_AT_LEAST_PERCENTAGE = "GROWS_AT_LEAST_PERCENTAGE"  # models.SqlAssertionTypeClass.METRIC_CHANGE + models.AssertionStdOperatorClass.GREATER_THAN_OR_EQUAL_TO + models.AssertionValueChangeTypeClass.PERCENTAGE
    GROWS_WITHIN_A_RANGE_ABSOLUTE = "GROWS_WITHIN_A_RANGE_ABSOLUTE"  # models.SqlAssertionTypeClass.METRIC_CHANGE + models.AssertionStdOperatorClass.BETWEEN + models.AssertionValueChangeTypeClass.ABSOLUTE
    GROWS_WITHIN_A_RANGE_PERCENTAGE = "GROWS_WITHIN_A_RANGE_PERCENTAGE"  # models.SqlAssertionTypeClass.METRIC_CHANGE + models.AssertionStdOperatorClass.BETWEEN + models.AssertionValueChangeTypeClass.PERCENTAGE


class SqlAssertionCriteria(BaseModel):
    condition: Union[SqlAssertionCondition, str]
    # Either a single value or a range must be provided
    parameters: Union[Union[float, int], tuple[Union[float, int], Union[float, int]]]

    @classmethod
    def get_type_from_condition(
        cls, condition: Union[SqlAssertionCondition, str]
    ) -> models.SqlAssertionTypeClass:
        condition_enum = (
            condition
            if isinstance(condition, SqlAssertionCondition)
            else SqlAssertionCondition(condition)
        )

        if condition_enum in [
            SqlAssertionCondition.IS_EQUAL_TO,
            SqlAssertionCondition.IS_NOT_EQUAL_TO,
            SqlAssertionCondition.IS_GREATER_THAN,
            SqlAssertionCondition.IS_LESS_THAN,
            SqlAssertionCondition.IS_WITHIN_A_RANGE,
        ]:
            return models.SqlAssertionTypeClass.METRIC  # type: ignore[return-value]
        elif condition_enum in [
            SqlAssertionCondition.GROWS_AT_MOST_ABSOLUTE,
            SqlAssertionCondition.GROWS_AT_MOST_PERCENTAGE,
            SqlAssertionCondition.GROWS_AT_LEAST_ABSOLUTE,
            SqlAssertionCondition.GROWS_AT_LEAST_PERCENTAGE,
            SqlAssertionCondition.GROWS_WITHIN_A_RANGE_ABSOLUTE,
            SqlAssertionCondition.GROWS_WITHIN_A_RANGE_PERCENTAGE,
        ]:
            return models.SqlAssertionTypeClass.METRIC_CHANGE  # type: ignore[return-value]
        else:
            raise SDKUsageError(f"Unknown condition: {condition_enum}")

    @classmethod
    def get_operator_from_condition(
        cls, condition: Union[SqlAssertionCondition, str]
    ) -> models.AssertionStdOperatorClass:
        condition_enum = (
            condition
            if isinstance(condition, SqlAssertionCondition)
            else SqlAssertionCondition(condition)
        )

        condition_to_operator = {
            SqlAssertionCondition.IS_EQUAL_TO: models.AssertionStdOperatorClass.EQUAL_TO,
            SqlAssertionCondition.IS_NOT_EQUAL_TO: models.AssertionStdOperatorClass.NOT_EQUAL_TO,
            SqlAssertionCondition.IS_GREATER_THAN: models.AssertionStdOperatorClass.GREATER_THAN,
            SqlAssertionCondition.IS_LESS_THAN: models.AssertionStdOperatorClass.LESS_THAN,
            SqlAssertionCondition.IS_WITHIN_A_RANGE: models.AssertionStdOperatorClass.BETWEEN,
            SqlAssertionCondition.GROWS_AT_MOST_ABSOLUTE: models.AssertionStdOperatorClass.LESS_THAN_OR_EQUAL_TO,
            SqlAssertionCondition.GROWS_AT_MOST_PERCENTAGE: models.AssertionStdOperatorClass.LESS_THAN_OR_EQUAL_TO,
            SqlAssertionCondition.GROWS_AT_LEAST_ABSOLUTE: models.AssertionStdOperatorClass.GREATER_THAN_OR_EQUAL_TO,
            SqlAssertionCondition.GROWS_AT_LEAST_PERCENTAGE: models.AssertionStdOperatorClass.GREATER_THAN_OR_EQUAL_TO,
            SqlAssertionCondition.GROWS_WITHIN_A_RANGE_ABSOLUTE: models.AssertionStdOperatorClass.BETWEEN,
            SqlAssertionCondition.GROWS_WITHIN_A_RANGE_PERCENTAGE: models.AssertionStdOperatorClass.BETWEEN,
        }

        if condition_enum not in condition_to_operator:
            raise SDKUsageError(f"Unknown condition: {condition_enum}")

        return condition_to_operator[condition_enum]  # type: ignore[return-value]

    @classmethod
    def get_change_type_from_condition(
        cls, condition: Union[SqlAssertionCondition, str]
    ) -> Optional[models.AssertionValueChangeTypeClass]:
        condition_enum = (
            condition
            if isinstance(condition, SqlAssertionCondition)
            else SqlAssertionCondition(condition)
        )

        if condition_enum in [
            SqlAssertionCondition.GROWS_AT_MOST_ABSOLUTE,
            SqlAssertionCondition.GROWS_AT_LEAST_ABSOLUTE,
            SqlAssertionCondition.GROWS_WITHIN_A_RANGE_ABSOLUTE,
        ]:
            return models.AssertionValueChangeTypeClass.ABSOLUTE  # type: ignore[return-value]
        elif condition_enum in [
            SqlAssertionCondition.GROWS_AT_MOST_PERCENTAGE,
            SqlAssertionCondition.GROWS_AT_LEAST_PERCENTAGE,
            SqlAssertionCondition.GROWS_WITHIN_A_RANGE_PERCENTAGE,
        ]:
            return models.AssertionValueChangeTypeClass.PERCENTAGE  # type: ignore[return-value]
        else:
            # Value-based conditions don't have a change type
            return None

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
        operator = SqlAssertionCriteria.get_operator_from_condition(criteria.condition)
        condition_enum = (
            criteria.condition
            if isinstance(criteria.condition, SqlAssertionCondition)
            else SqlAssertionCondition(criteria.condition)
        )

        if operator in SINGLE_VALUE_NUMERIC_OPERATORS:
            if not isinstance(criteria.parameters, float) and not isinstance(
                criteria.parameters, int
            ):
                raise SDKUsageError(
                    f"The parameter value of SqlAssertionCriteria must be a single value numeric for condition {str(condition_enum)}"
                )
        elif operator in RANGE_OPERATORS:
            if not isinstance(criteria.parameters, tuple):
                raise SDKUsageError(
                    f"The parameter value of SqlAssertionCriteria must be a tuple range for condition {str(condition_enum)}"
                )
        else:
            raise SDKUsageError(
                f"Condition {str(condition_enum)} is not supported for SqlAssertionCriteria"
            )

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
            type=SqlAssertionCriteria.get_type_from_condition(self.criteria.condition),
            entity=str(self.dataset_urn),
            statement=self.statement,
            changeType=SqlAssertionCriteria.get_change_type_from_condition(
                self.criteria.condition
            ),
            operator=SqlAssertionCriteria.get_operator_from_condition(
                self.criteria.condition
            ),
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
        if self.schedule is None:
            return DEFAULT_EVERY_SIX_HOURS_SCHEDULE

        return models.CronScheduleClass(
            cron=self.schedule.cron,
            timezone=self.schedule.timezone,
        )
