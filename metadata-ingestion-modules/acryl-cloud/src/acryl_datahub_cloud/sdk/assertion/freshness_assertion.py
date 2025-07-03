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
    AssertionIncidentBehavior,
    DetectionMechanism,
    TimeWindowSizeInputTypes,
    _DetectionMechanismTypes,
)
from acryl_datahub_cloud.sdk.entities.assertion import Assertion
from acryl_datahub_cloud.sdk.entities.monitor import Monitor
from acryl_datahub_cloud.sdk.errors import SDKNotYetSupportedError
from datahub.metadata import schema_classes as models
from datahub.metadata.urns import AssertionUrn, CorpUserUrn, DatasetUrn, TagUrn

logger = logging.getLogger(__name__)


class FreshnessAssertion(_HasSchedule, _AssertionPublic):
    """
    A class that represents a freshness assertion.
    """

    def __init__(
        self,
        *,
        urn: AssertionUrn,
        dataset_urn: DatasetUrn,
        display_name: str,
        mode: AssertionMode,
        schedule: models.CronScheduleClass,
        freshness_schedule_check_type: Union[
            str, models.FreshnessAssertionScheduleTypeClass
        ],
        lookback_window: Optional[TimeWindowSizeInputTypes],
        tags: list[TagUrn],
        incident_behavior: list[AssertionIncidentBehavior],
        detection_mechanism: Optional[
            _DetectionMechanismTypes
        ] = DEFAULT_DETECTION_MECHANISM,
        created_by: Optional[CorpUserUrn] = None,
        created_at: Union[datetime, None] = None,
        updated_by: Optional[CorpUserUrn] = None,
        updated_at: Optional[datetime] = None,
    ):
        """
        Initialize a freshness assertion.

        Note: Values can be accessed, but not set on the assertion object.
        To update an assertion, use the `upsert_*` method.
        Args:
            urn: The urn of the assertion.
            dataset_urn: The urn of the dataset that the assertion is for.
            display_name: The display name of the assertion.
            mode: The mode of the assertion (active, inactive).
            schedule: The schedule of the assertion.
            freshness_schedule_check_type: The type of freshness schedule check to be used for the assertion.
            lookback_window: The lookback window to be used for the assertion.
            tags: The tags applied to the assertion.
            incident_behavior: Whether to raise or resolve an incident when the assertion fails / passes.
            detection_mechanism: The detection mechanism of the assertion.
            created_by: The urn of the user that created the assertion.
            created_at: The timestamp of when the assertion was created.
            updated_by: The urn of the user that updated the assertion.
            updated_at: The timestamp of when the assertion was updated.
        """
        _HasSchedule.__init__(self, schedule=schedule)
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
        self._freshness_schedule_check_type = freshness_schedule_check_type
        self._lookback_window = lookback_window

    @property
    def freshness_schedule_check_type(
        self,
    ) -> Union[str, models.FreshnessAssertionScheduleTypeClass]:
        return self._freshness_schedule_check_type

    @property
    def lookback_window(self) -> Optional[TimeWindowSizeInputTypes]:
        return self._lookback_window

    @staticmethod
    def _get_freshness_schedule_check_type(
        assertion: Assertion,
    ) -> Union[str, models.FreshnessAssertionScheduleTypeClass]:
        if assertion.info is None:
            raise SDKNotYetSupportedError(
                f"Assertion {assertion.urn} does not have a freshness assertion info, which is not supported"
            )
        if isinstance(assertion.info, models.FreshnessAssertionInfoClass):
            if assertion.info.schedule is None:
                raise SDKNotYetSupportedError(
                    f"Traditional freshness assertion {assertion.urn} does not have a schedule, which is not supported"
                )
            return assertion.info.schedule.type
        else:
            raise SDKNotYetSupportedError(
                f"Assertion {assertion.urn} is not a freshness assertion"
            )

    @staticmethod
    def _get_lookback_window(
        assertion: Assertion,
    ) -> Optional[models.FixedIntervalScheduleClass]:
        if assertion.info is None:
            raise SDKNotYetSupportedError(
                f"Assertion {assertion.urn} does not have a freshness assertion info, which is not supported"
            )
        if isinstance(assertion.info, models.FreshnessAssertionInfoClass):
            if assertion.info.schedule is None:
                raise SDKNotYetSupportedError(
                    f"Traditional freshness assertion {assertion.urn} does not have a schedule, which is not supported"
                )
            return assertion.info.schedule.fixedInterval
        else:
            raise SDKNotYetSupportedError(
                f"Assertion {assertion.urn} is not a freshness assertion"
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

    @classmethod
    def _from_entities(cls, assertion: Assertion, monitor: Monitor) -> Self:
        """
        Create a freshness assertion from the assertion and monitor entities.
        """
        return cls(
            urn=assertion.urn,
            dataset_urn=assertion.dataset,
            display_name=assertion.description or "",
            mode=cls._get_mode(monitor),
            schedule=cls._get_schedule(monitor),
            freshness_schedule_check_type=cls._get_freshness_schedule_check_type(
                assertion
            ),
            lookback_window=cls._get_lookback_window(assertion),
            incident_behavior=cls._get_incident_behavior(assertion),
            detection_mechanism=cls._get_detection_mechanism(assertion, monitor),
            created_by=cls._get_created_by(assertion),
            created_at=cls._get_created_at(assertion),
            updated_by=cls._get_updated_by(assertion),
            updated_at=cls._get_updated_at(assertion),
            tags=cls._get_tags(assertion),
        )
