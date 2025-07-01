import logging
from datetime import datetime
from typing import Optional, Union

from typing_extensions import Self

from acryl_datahub_cloud.sdk.assertion.assertion_base import (
    AssertionMode,
    _AssertionPublic,
    _HasColumnMetricFunctionality,
    _HasSchedule,
)
from acryl_datahub_cloud.sdk.assertion_input.assertion_input import (
    DEFAULT_DETECTION_MECHANISM,
    DEFAULT_SCHEDULE,
    AssertionIncidentBehavior,
    DetectionMechanism,
    _DetectionMechanismTypes,
)
from acryl_datahub_cloud.sdk.assertion_input.column_metric_assertion_input import (
    ColumnMetricAssertionParameters,
    MetricInputType,
    OperatorInputType,
)
from acryl_datahub_cloud.sdk.entities.assertion import Assertion
from acryl_datahub_cloud.sdk.entities.monitor import Monitor
from datahub.metadata import schema_classes as models
from datahub.metadata.urns import AssertionUrn, CorpUserUrn, DatasetUrn, TagUrn

logger = logging.getLogger(__name__)


class ColumnMetricAssertion(
    _HasColumnMetricFunctionality,
    _HasSchedule,
    _AssertionPublic,
):
    """
    A class that represents a column metric assertion.
    This assertion is used to validate the value of a common field / column metric (e.g. aggregation) such as null count + percentage,
    min, max, median, and more. It uses native source types without AI inference.
    """

    def __init__(
        self,
        *,
        urn: AssertionUrn,
        dataset_urn: DatasetUrn,
        column_name: str,
        metric_type: MetricInputType,
        operator: OperatorInputType,
        # Consolidated criteria parameters
        criteria_parameters: Optional[ColumnMetricAssertionParameters] = None,
        # Standard assertion parameters:
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
        Initialize a column metric assertion.

        Args:
            urn: The URN of the assertion.
            dataset_urn: The URN of the dataset to monitor.
            display_name: The display name of the assertion.
            mode: The mode of the assertion (active/inactive).
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
        _HasColumnMetricFunctionality.__init__(
            self,
            column_name=column_name,
            metric_type=metric_type,
            operator=operator,
            criteria_parameters=criteria_parameters,
        )

    @classmethod
    def _from_entities(cls, assertion: Assertion, monitor: Monitor) -> Self:
        """
        Create a ColumnMetricAssertion from an Assertion and Monitor entity.

        Args:
            assertion: The Assertion entity.
            monitor: The Monitor entity.

        Returns:
            A ColumnMetricAssertion instance.
        """
        return cls(
            urn=assertion.urn,
            dataset_urn=assertion.dataset,
            column_name=cls._get_column_name(assertion),
            metric_type=cls._get_metric_type(assertion),
            operator=cls._get_operator(assertion),
            criteria_parameters=cls._get_criteria_parameters(assertion),
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
    def _get_detection_mechanism(
        assertion: Assertion,
        monitor: Monitor,
        default: Optional[_DetectionMechanismTypes] = DEFAULT_DETECTION_MECHANISM,
    ) -> Optional[_DetectionMechanismTypes]:
        """Get the detection mechanism for column metric assertions."""
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
                f"Monitor does not have datasetFieldParameters, defaulting detection mechanism to {default}"
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
                    f"Monitor has CHANGED_ROWS_QUERY source type but no changedRowsField, defaulting detection mechanism to {default}"
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
                f"Unsupported DatasetFieldAssertionSourceType {source_type}, defaulting detection mechanism to {default}"
            )
            return default
