import logging
from datetime import datetime
from typing import Optional, Union

from typing_extensions import Self

from acryl_datahub_cloud.sdk.assertion.assertion_base import (
    AssertionMode,
    _AssertionPublic,
    _HasColumnMetricFunctionality,
    _HasSchedule,
    _HasSmartFunctionality,
)
from acryl_datahub_cloud.sdk.assertion_input.assertion_input import (
    ASSERTION_MONITOR_DEFAULT_TRAINING_LOOKBACK_WINDOW_DAYS,
    DEFAULT_DETECTION_MECHANISM,
    DEFAULT_SCHEDULE,
    DEFAULT_SENSITIVITY,
    AssertionIncidentBehavior,
    DetectionMechanism,
    ExclusionWindowTypes,
    InferenceSensitivity,
    _DetectionMechanismTypes,
)
from acryl_datahub_cloud.sdk.assertion_input.smart_column_metric_assertion_input import (
    MetricInputType,
    OperatorInputType,
    RangeInputType,
    RangeTypeInputType,
    ValueInputType,
    ValueTypeInputType,
)
from acryl_datahub_cloud.sdk.entities.assertion import Assertion
from acryl_datahub_cloud.sdk.entities.monitor import Monitor
from datahub.metadata import schema_classes as models
from datahub.metadata.urns import AssertionUrn, CorpUserUrn, DatasetUrn, TagUrn

logger = logging.getLogger(__name__)


class SmartColumnMetricAssertion(
    _HasColumnMetricFunctionality,
    _HasSmartFunctionality,
    _HasSchedule,
    _AssertionPublic,
):
    """
    A class that represents a smart column metric assertion.
    This assertion is used to validate the value of a common field / column metric (e.g. aggregation) such as null count + percentage,
    min, max, median, and more. It uses AI to infer the assertion parameters.
    """

    def __init__(
        self,
        *,
        urn: AssertionUrn,
        dataset_urn: DatasetUrn,
        column_name: str,
        metric_type: MetricInputType,
        operator: OperatorInputType,
        # Depending on the operator, value, range (and corresponding type) or no parameters are required:
        value: Optional[ValueInputType] = None,
        value_type: Optional[ValueTypeInputType] = None,
        range: Optional[RangeInputType] = None,
        range_type: Optional[RangeTypeInputType] = None,
        # TODO: Evaluate these params:
        display_name: str,
        mode: AssertionMode,
        schedule: models.CronScheduleClass = DEFAULT_SCHEDULE,
        sensitivity: InferenceSensitivity = DEFAULT_SENSITIVITY,
        exclusion_windows: list[ExclusionWindowTypes],
        training_data_lookback_days: int = ASSERTION_MONITOR_DEFAULT_TRAINING_LOOKBACK_WINDOW_DAYS,
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
        Initialize a smart column metric assertion.

        Args:
            urn: The URN of the assertion.
            dataset_urn: The URN of the dataset to monitor.
            display_name: The display name of the assertion.
            mode: The mode of the assertion (active/inactive).
            sensitivity: The sensitivity of the assertion (low/medium/high).
            exclusion_windows: The exclusion windows to apply to the assertion.
            training_data_lookback_days: The number of days of data to use for training.
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
        _HasSmartFunctionality.__init__(
            self,
            sensitivity=sensitivity,
            exclusion_windows=exclusion_windows,
            training_data_lookback_days=training_data_lookback_days,
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
            value=value,
            value_type=value_type,
            range=range,
            range_type=range_type,
        )

    @classmethod
    def _from_entities(cls, assertion: Assertion, monitor: Monitor) -> Self:
        """
        Create a SmartColumnMetricAssertion from an Assertion and Monitor entity.

        Args:
            assertion: The Assertion entity.
            monitor: The Monitor entity.

        Returns:
            A SmartColumnMetricAssertion instance.
        """
        return cls(
            urn=assertion.urn,
            dataset_urn=assertion.dataset,
            column_name=cls._get_column_name(assertion),
            metric_type=cls._get_metric_type(assertion),
            operator=cls._get_operator(assertion),
            value=cls._get_value(assertion),
            value_type=cls._get_value_type(assertion),
            range=cls._get_range(assertion),
            range_type=cls._get_range_type(assertion),
            display_name=assertion.description or "",
            mode=cls._get_mode(monitor),
            schedule=cls._get_schedule(monitor),
            sensitivity=cls._get_sensitivity(monitor),
            exclusion_windows=cls._get_exclusion_windows(monitor),
            training_data_lookback_days=cls._get_training_data_lookback_days(monitor),
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
