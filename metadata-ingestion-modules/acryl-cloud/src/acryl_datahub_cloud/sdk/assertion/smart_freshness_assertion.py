import logging
from datetime import datetime
from typing import Optional, Union

from typing_extensions import Self

from acryl_datahub_cloud.sdk.assertion.assertion_base import (
    AssertionMode,
    _AssertionPublic,
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
from acryl_datahub_cloud.sdk.entities.assertion import Assertion
from acryl_datahub_cloud.sdk.entities.monitor import Monitor
from acryl_datahub_cloud.sdk.errors import SDKNotYetSupportedError
from datahub.metadata import schema_classes as models
from datahub.metadata.urns import AssertionUrn, CorpUserUrn, DatasetUrn, TagUrn

logger = logging.getLogger(__name__)


class SmartFreshnessAssertion(_HasSchedule, _HasSmartFunctionality, _AssertionPublic):
    """
    A class that represents a smart freshness assertion.
    """

    def __init__(
        self,
        *,
        urn: AssertionUrn,
        dataset_urn: DatasetUrn,
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
        Initialize a smart freshness assertion.

        Note: Values can be accessed, but not set on the assertion object.
        To update an assertion, use the `upsert_*` method.
        Args:
            urn: The urn of the assertion.
            dataset_urn: The urn of the dataset that the assertion is for.
            display_name: The display name of the assertion.
            mode: The mode of the assertion (active, inactive).
            schedule: The schedule of the assertion.
            sensitivity: The sensitivity of the assertion (low, medium, high).
            exclusion_windows: The exclusion windows of the assertion.
            training_data_lookback_days: The max number of days of data to use for training the assertion.
            incident_behavior: Whether to raise or resolve an incident when the assertion fails / passes.
            detection_mechanism: The detection mechanism of the assertion.
            tags: The tags applied to the assertion.
            created_by: The urn of the user that created the assertion.
            created_at: The timestamp of when the assertion was created.
            updated_by: The urn of the user that updated the assertion.
            updated_at: The timestamp of when the assertion was updated.
        """
        # Initialize the mixins first
        _HasSchedule.__init__(self, schedule=schedule)
        _HasSmartFunctionality.__init__(
            self,
            sensitivity=sensitivity,
            exclusion_windows=exclusion_windows,
            training_data_lookback_days=training_data_lookback_days,
        )
        # Then initialize the parent class
        _AssertionPublic.__init__(
            self,
            urn=urn,
            dataset_urn=dataset_urn,
            display_name=display_name,
            mode=mode,
            incident_behavior=incident_behavior,
            detection_mechanism=detection_mechanism,
            created_by=created_by,
            created_at=created_at,
            updated_by=updated_by,
            updated_at=updated_at,
            tags=tags,
        )

    @classmethod
    def _from_entities(cls, assertion: Assertion, monitor: Monitor) -> Self:
        """
        Create a smart freshness assertion from the assertion and monitor entities.

        Note: This is a private method since it is intended to be called internally by the client.
        """
        return cls(
            urn=assertion.urn,
            dataset_urn=assertion.dataset,
            display_name=assertion.description or "",
            mode=cls._get_mode(monitor),
            schedule=cls._get_schedule(monitor),
            sensitivity=cls._get_sensitivity(monitor),
            exclusion_windows=cls._get_exclusion_windows(monitor),
            training_data_lookback_days=cls._get_training_data_lookback_days(monitor),
            incident_behavior=cls._get_incident_behavior(assertion),
            detection_mechanism=cls._get_detection_mechanism(assertion, monitor),
            created_by=cls._get_created_by(assertion),
            created_at=cls._get_created_at(assertion),
            updated_by=cls._get_updated_by(assertion),
            updated_at=cls._get_updated_at(assertion),
            tags=cls._get_tags(assertion),
        )

    @staticmethod
    def _get_detection_mechanism(
        assertion: Assertion,
        monitor: Monitor,
        default: Optional[_DetectionMechanismTypes] = DEFAULT_DETECTION_MECHANISM,
    ) -> Optional[_DetectionMechanismTypes]:
        """Get the detection mechanism for freshness assertions."""
        parameters = _AssertionPublic._get_validated_detection_context(
            monitor,
            assertion,
            models.AssertionEvaluationParametersTypeClass.DATASET_FRESHNESS,
            models.FreshnessAssertionInfoClass,
            default,
        )
        if parameters is None:
            return default
        if parameters.datasetFreshnessParameters is None:
            logger.warning(
                f"Monitor does not have datasetFreshnessParameters, defaulting detection mechanism to {DEFAULT_DETECTION_MECHANISM}"
            )
            return default
        source_type = parameters.datasetFreshnessParameters.sourceType
        if source_type == models.DatasetFreshnessSourceTypeClass.INFORMATION_SCHEMA:
            return DetectionMechanism.INFORMATION_SCHEMA
        elif source_type == models.DatasetFreshnessSourceTypeClass.AUDIT_LOG:
            return DetectionMechanism.AUDIT_LOG
        elif source_type == models.DatasetFreshnessSourceTypeClass.FIELD_VALUE:
            return _AssertionPublic._get_field_value_detection_mechanism(
                assertion, parameters
            )
        elif source_type == models.DatasetFreshnessSourceTypeClass.DATAHUB_OPERATION:
            return DetectionMechanism.DATAHUB_OPERATION
        elif source_type == models.DatasetFreshnessSourceTypeClass.FILE_METADATA:
            raise SDKNotYetSupportedError("FILE_METADATA DatasetFreshnessSourceType")
        else:
            raise SDKNotYetSupportedError(f"DatasetFreshnessSourceType {source_type}")
