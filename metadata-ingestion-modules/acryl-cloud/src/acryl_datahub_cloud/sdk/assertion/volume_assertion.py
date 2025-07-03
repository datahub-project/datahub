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
    _DetectionMechanismTypes,
)
from acryl_datahub_cloud.sdk.assertion_input.volume_assertion_input import (
    VolumeAssertionCriteria,
)
from acryl_datahub_cloud.sdk.entities.assertion import Assertion
from acryl_datahub_cloud.sdk.entities.monitor import Monitor
from acryl_datahub_cloud.sdk.errors import SDKNotYetSupportedError
from datahub.metadata import schema_classes as models
from datahub.metadata.urns import AssertionUrn, CorpUserUrn, DatasetUrn, TagUrn

logger = logging.getLogger(__name__)


class VolumeAssertion(_HasSchedule, _AssertionPublic):
    """
    A class that represents a volume assertion.
    """

    def __init__(
        self,
        *,
        urn: AssertionUrn,
        dataset_urn: DatasetUrn,
        display_name: str,
        mode: AssertionMode,
        schedule: models.CronScheduleClass,
        criteria: VolumeAssertionCriteria,
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
        Initialize a volume assertion.

        Note: Values can be accessed, but not set on the assertion object.
        To update an assertion, use the `upsert_*` method.
        Args:
            urn: The urn of the assertion.
            dataset_urn: The urn of the dataset that the assertion is for.
            display_name: The display name of the assertion.
            mode: The mode of the assertion (active, inactive).
            schedule: The schedule of the assertion.
            criteria: The volume assertion criteria.
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
        self._criteria = criteria

    @property
    def criteria(self) -> VolumeAssertionCriteria:
        return self._criteria

    @staticmethod
    def _get_volume_definition(
        assertion: Assertion,
    ) -> VolumeAssertionCriteria:
        """Get volume assertion definition from a DataHub assertion entity."""
        return VolumeAssertionCriteria.from_assertion(assertion)

    @staticmethod
    def _get_detection_mechanism(
        assertion: Assertion,
        monitor: Monitor,
        default: Optional[_DetectionMechanismTypes] = DEFAULT_DETECTION_MECHANISM,
    ) -> Optional[_DetectionMechanismTypes]:
        """Get the detection mechanism for volume assertions."""
        parameters = _AssertionPublic._get_validated_detection_context(
            monitor,
            assertion,
            models.AssertionEvaluationParametersTypeClass.DATASET_VOLUME,
            models.VolumeAssertionInfoClass,
            default,
        )
        if parameters is None:
            return default
        if parameters.datasetVolumeParameters is None:
            logger.warning(
                f"Monitor does not have datasetVolumeParameters, defaulting detection mechanism to {DEFAULT_DETECTION_MECHANISM}"
            )
            if default is None:
                return DEFAULT_DETECTION_MECHANISM
            else:
                return default
        source_type = parameters.datasetVolumeParameters.sourceType
        if source_type == models.DatasetVolumeSourceTypeClass.INFORMATION_SCHEMA:
            return DetectionMechanism.INFORMATION_SCHEMA
        elif source_type == models.DatasetVolumeSourceTypeClass.QUERY:
            additional_filter = _AssertionPublic._get_additional_filter(assertion)
            return DetectionMechanism.QUERY(additional_filter=additional_filter)
        elif source_type == models.DatasetVolumeSourceTypeClass.DATAHUB_DATASET_PROFILE:
            return DetectionMechanism.DATASET_PROFILE
        else:
            raise SDKNotYetSupportedError(f"DatasetVolumeSourceType {source_type}")

    @classmethod
    def _from_entities(cls, assertion: Assertion, monitor: Monitor) -> Self:
        """
        Create a volume assertion from the assertion and monitor entities.
        """
        return cls(
            urn=assertion.urn,
            dataset_urn=assertion.dataset,
            display_name=assertion.description or "",
            mode=cls._get_mode(monitor),
            schedule=cls._get_schedule(monitor),
            criteria=cls._get_volume_definition(assertion),
            incident_behavior=cls._get_incident_behavior(assertion),
            detection_mechanism=cls._get_detection_mechanism(assertion, monitor),
            created_by=cls._get_created_by(assertion),
            created_at=cls._get_created_at(assertion),
            updated_by=cls._get_updated_by(assertion),
            updated_at=cls._get_updated_at(assertion),
            tags=cls._get_tags(assertion),
        )
