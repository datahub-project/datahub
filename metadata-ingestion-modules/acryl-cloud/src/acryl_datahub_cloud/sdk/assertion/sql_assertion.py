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
    DEFAULT_EVERY_SIX_HOURS_SCHEDULE,
    AssertionIncidentBehavior,
    _DetectionMechanismTypes,
)
from acryl_datahub_cloud.sdk.assertion_input.sql_assertion_input import (
    SqlAssertionCondition,
    SqlAssertionCriteria,
)
from acryl_datahub_cloud.sdk.entities.assertion import Assertion
from acryl_datahub_cloud.sdk.entities.monitor import Monitor
from acryl_datahub_cloud.sdk.errors import SDKNotYetSupportedError
from datahub.metadata import schema_classes as models
from datahub.metadata.urns import AssertionUrn, CorpUserUrn, DatasetUrn, TagUrn

logger = logging.getLogger(__name__)


class SqlAssertion(_AssertionPublic, _HasSchedule):
    """
    A class that represents a SQL assertion.
    """

    def __init__(
        self,
        *,
        urn: AssertionUrn,
        dataset_urn: DatasetUrn,
        display_name: str,
        mode: AssertionMode,
        statement: str,
        criteria: SqlAssertionCriteria,
        schedule: models.CronScheduleClass,
        tags: list[TagUrn],
        incident_behavior: list[AssertionIncidentBehavior],
        created_by: Optional[CorpUserUrn] = None,
        created_at: Union[datetime, None] = None,
        updated_by: Optional[CorpUserUrn] = None,
        updated_at: Optional[datetime] = None,
    ):
        """
        Initialize a SQL assertion.

        Note: Values can be accessed, but not set on the assertion object.
        To update an assertion, use the `upsert_*` method.
        Args:
            urn: The urn of the assertion.
            dataset_urn: The urn of the dataset that the assertion is for.
            display_name: The display name of the assertion.
            mode: The mode of the assertion (active, inactive).
            statement: The SQL statement to be used for the assertion.
            criteria: The criteria to be used for the assertion.
            schedule: The schedule of the assertion.
            tags: The tags applied to the assertion.
            incident_behavior: Whether to raise or resolve an incident when the assertion fails / passes.
            created_by: The urn of the user that created the assertion.
            created_at: The timestamp of when the assertion was created.
            updated_by: The urn of the user that updated the assertion.
            updated_at: The timestamp of when the assertion was updated.
        """
        # Initialize the mixins first
        _AssertionPublic.__init__(
            self,
            urn=urn,
            dataset_urn=dataset_urn,
            display_name=display_name,
            mode=mode,
            tags=tags,
            incident_behavior=incident_behavior,
            created_by=created_by,
            created_at=created_at,
            updated_by=updated_by,
            updated_at=updated_at,
        )
        _HasSchedule.__init__(self, schedule=schedule)
        # Then initialize the parent class
        self._statement = statement
        self._criteria = criteria

    @property
    def statement(self) -> str:
        return self._statement

    @property
    def criteria_condition(self) -> Union[SqlAssertionCondition, str]:
        return self._criteria.condition

    @property
    def criteria_parameters(
        self,
    ) -> Union[Union[float, int], tuple[Union[float, int], Union[float, int]]]:
        return self._criteria.parameters

    @staticmethod
    def _get_detection_mechanism(
        assertion: Assertion,
        monitor: Monitor,
        default: Optional[_DetectionMechanismTypes] = DEFAULT_DETECTION_MECHANISM,
    ) -> Optional[_DetectionMechanismTypes]:
        """Sql assertions do not have a detection mechanism."""
        return None

    @staticmethod
    def _get_statement(assertion: Assertion) -> str:
        if assertion.info is None:
            raise SDKNotYetSupportedError(
                f"Assertion {assertion.urn} does not have a SQL assertion info, which is not supported"
            )
        if isinstance(assertion.info, models.SqlAssertionInfoClass):
            return assertion.info.statement
        else:
            raise SDKNotYetSupportedError(
                f"Assertion {assertion.urn} is not a SQL assertion"
            )

    @staticmethod
    def _get_condition_from_model_assertion_info(
        assertion_info: models.SqlAssertionInfoClass,
    ) -> SqlAssertionCondition:
        """Convert stored assertion info to condition enum."""
        # Handle value-based conditions (no change type)
        if str(assertion_info.type) == str(models.SqlAssertionTypeClass.METRIC):
            value_conditions = {
                str(
                    models.AssertionStdOperatorClass.EQUAL_TO
                ): SqlAssertionCondition.IS_EQUAL_TO,
                str(
                    models.AssertionStdOperatorClass.NOT_EQUAL_TO
                ): SqlAssertionCondition.IS_NOT_EQUAL_TO,
                str(
                    models.AssertionStdOperatorClass.GREATER_THAN
                ): SqlAssertionCondition.IS_GREATER_THAN,
                str(
                    models.AssertionStdOperatorClass.LESS_THAN
                ): SqlAssertionCondition.IS_LESS_THAN,
                str(
                    models.AssertionStdOperatorClass.BETWEEN
                ): SqlAssertionCondition.IS_WITHIN_A_RANGE,
            }
            if str(assertion_info.operator) in value_conditions:
                return value_conditions[str(assertion_info.operator)]

        # Handle growth-based conditions (with change type)
        elif str(assertion_info.type) == str(
            models.SqlAssertionTypeClass.METRIC_CHANGE
        ):
            assert assertion_info.changeType is not None, (
                "changeType must be present for METRIC_CHANGE assertions"
            )

            growth_conditions = {
                (
                    str(models.AssertionStdOperatorClass.LESS_THAN_OR_EQUAL_TO),
                    str(models.AssertionValueChangeTypeClass.ABSOLUTE),
                ): SqlAssertionCondition.GROWS_AT_MOST_ABSOLUTE,
                (
                    str(models.AssertionStdOperatorClass.LESS_THAN_OR_EQUAL_TO),
                    str(models.AssertionValueChangeTypeClass.PERCENTAGE),
                ): SqlAssertionCondition.GROWS_AT_MOST_PERCENTAGE,
                (
                    str(models.AssertionStdOperatorClass.GREATER_THAN_OR_EQUAL_TO),
                    str(models.AssertionValueChangeTypeClass.ABSOLUTE),
                ): SqlAssertionCondition.GROWS_AT_LEAST_ABSOLUTE,
                (
                    str(models.AssertionStdOperatorClass.GREATER_THAN_OR_EQUAL_TO),
                    str(models.AssertionValueChangeTypeClass.PERCENTAGE),
                ): SqlAssertionCondition.GROWS_AT_LEAST_PERCENTAGE,
                (
                    str(models.AssertionStdOperatorClass.BETWEEN),
                    str(models.AssertionValueChangeTypeClass.ABSOLUTE),
                ): SqlAssertionCondition.GROWS_WITHIN_A_RANGE_ABSOLUTE,
                (
                    str(models.AssertionStdOperatorClass.BETWEEN),
                    str(models.AssertionValueChangeTypeClass.PERCENTAGE),
                ): SqlAssertionCondition.GROWS_WITHIN_A_RANGE_PERCENTAGE,
            }

            key = (str(assertion_info.operator), str(assertion_info.changeType))
            if key in growth_conditions:
                return growth_conditions[key]

        raise ValueError(
            f"Unsupported combination: type={assertion_info.type}, operator={assertion_info.operator}, changeType={assertion_info.changeType}"
        )

    @staticmethod
    def _get_criteria(assertion: Assertion) -> SqlAssertionCriteria:
        if assertion.info is None:
            raise SDKNotYetSupportedError(
                f"Assertion {assertion.urn} does not have a SQL assertion info, which is not supported"
            )
        if isinstance(assertion.info, models.SqlAssertionInfoClass):
            parameters: Union[float, tuple[float, float]]
            if assertion.info.parameters.value is not None:
                parameters = float(assertion.info.parameters.value.value)
            elif (
                assertion.info.parameters.maxValue is not None
                and assertion.info.parameters.minValue is not None
            ):
                # min and max values are in the order of min, max
                parameters = (
                    float(assertion.info.parameters.minValue.value),
                    float(assertion.info.parameters.maxValue.value),
                )
            else:
                raise SDKNotYetSupportedError(
                    f"Assertion {assertion.urn} does not have a valid parameters for the SQL assertion"
                )

            condition = SqlAssertion._get_condition_from_model_assertion_info(
                assertion.info
            )

            return SqlAssertionCriteria(
                condition=condition,
                parameters=parameters,
            )
        else:
            raise SDKNotYetSupportedError(
                f"Assertion {assertion.urn} is not a SQL assertion"
            )

    @classmethod
    def _from_entities(cls, assertion: Assertion, monitor: Monitor) -> Self:
        """
        Create a SQL assertion from the assertion and monitor entities.
        """
        return cls(
            urn=assertion.urn,
            dataset_urn=assertion.dataset,
            display_name=assertion.description or "",
            mode=cls._get_mode(monitor),
            statement=cls._get_statement(assertion),
            criteria=cls._get_criteria(assertion),
            schedule=cls._get_schedule(
                monitor, default=DEFAULT_EVERY_SIX_HOURS_SCHEDULE
            ),
            tags=cls._get_tags(assertion),
            incident_behavior=cls._get_incident_behavior(assertion),
            created_by=cls._get_created_by(assertion),
            created_at=cls._get_created_at(assertion),
            updated_by=cls._get_updated_by(assertion),
            updated_at=cls._get_updated_at(assertion),
        )
