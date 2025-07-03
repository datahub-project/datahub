"""
Smart Volume Assertion Input classes for DataHub.

This module contains the _SmartVolumeAssertionInput class that handles
input validation and entity creation for smart volume assertions.
"""

from datetime import datetime
from typing import Optional, Union

from acryl_datahub_cloud.sdk.assertion_input.assertion_input import (
    DEFAULT_HOURLY_SCHEDULE,
    AssertionIncidentBehaviorInputTypes,
    DetectionMechanismInputTypes,
    ExclusionWindowInputTypes,
    FieldSpecType,
    InferenceSensitivity,
    _AssertionInput,
    _DatasetProfile,
    _HasSmartAssertionInputs,
    _InformationSchema,
    _Query,
)
from acryl_datahub_cloud.sdk.entities.assertion import (
    AssertionInfoInputType,
    TagsInputType,
)
from acryl_datahub_cloud.sdk.errors import SDKNotYetSupportedError
from datahub.metadata import schema_classes as models
from datahub.metadata.urns import AssertionUrn, CorpUserUrn, DatasetUrn
from datahub.sdk.entity_client import EntityClient


class _SmartVolumeAssertionInput(_AssertionInput, _HasSmartAssertionInputs):
    def __init__(
        self,
        *,
        # Required fields
        dataset_urn: Union[str, DatasetUrn],
        entity_client: EntityClient,  # Needed to get the schema field spec for the detection mechanism if needed
        # Optional fields
        urn: Optional[Union[str, AssertionUrn]] = None,
        display_name: Optional[str] = None,
        enabled: bool = True,
        schedule: Optional[Union[str, models.CronScheduleClass]] = None,
        detection_mechanism: DetectionMechanismInputTypes = None,
        sensitivity: Optional[Union[str, InferenceSensitivity]] = None,
        exclusion_windows: Optional[ExclusionWindowInputTypes] = None,
        training_data_lookback_days: Optional[int] = None,
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
            detection_mechanism=detection_mechanism,
            incident_behavior=incident_behavior,
            tags=tags,
            source_type=models.AssertionSourceTypeClass.INFERRED,  # Smart assertions are of type inferred, not native
            created_by=created_by,
            created_at=created_at,
            updated_by=updated_by,
            updated_at=updated_at,
        )
        _HasSmartAssertionInputs.__init__(
            self,
            sensitivity=sensitivity,
            exclusion_windows=exclusion_windows,
            training_data_lookback_days=training_data_lookback_days,
        )

    def _create_assertion_info(
        self, filter: Optional[models.DatasetFilterClass]
    ) -> AssertionInfoInputType:
        """
        Create a VolumeAssertionInfoClass for a smart volume assertion.

        Args:
            filter: Optional filter to apply to the assertion.

        Returns:
            A VolumeAssertionInfoClass configured for smart volume.
        """
        return models.VolumeAssertionInfoClass(
            type=models.VolumeAssertionTypeClass.ROW_COUNT_TOTAL,  # Currently only ROW_COUNT_TOTAL is supported for smart volume
            entity=str(self.dataset_urn),
            filter=filter,
        )

    def _convert_schedule(self) -> models.CronScheduleClass:
        """Create a schedule for a smart volume assertion.

        Returns:
            A CronScheduleClass with appropriate schedule settings.
        """
        if self.schedule is None:
            return DEFAULT_HOURLY_SCHEDULE

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
                sourceType=source_type,
            ),
        )

    def _convert_assertion_source_type_and_field(
        self,
    ) -> tuple[str, Optional[FieldSpecType]]:
        """
        Convert detection mechanism into source type and field specification for volume assertions.

        Returns:
            A tuple of (source_type, field) where field may be None.
            Note that the source_type is a string, not a models.DatasetFreshnessSourceTypeClass (or other assertion source type) since
            the source type is not a enum in the code generated from the DatasetFreshnessSourceType enum in the PDL.

        Raises:
            SDKNotYetSupportedError: If the detection mechanism is not supported.
            SDKUsageError: If the field (column) is not found in the dataset,
            and the detection mechanism requires a field. Also if the field
            is not an allowed type for the detection mechanism.
        """
        source_type = models.DatasetVolumeSourceTypeClass.INFORMATION_SCHEMA
        field = None

        if isinstance(self.detection_mechanism, _Query):
            source_type = models.DatasetVolumeSourceTypeClass.QUERY
        elif isinstance(self.detection_mechanism, _InformationSchema):
            source_type = models.DatasetVolumeSourceTypeClass.INFORMATION_SCHEMA
        elif isinstance(self.detection_mechanism, _DatasetProfile):
            source_type = models.DatasetVolumeSourceTypeClass.DATAHUB_DATASET_PROFILE
        else:
            raise SDKNotYetSupportedError(
                f"Detection mechanism {self.detection_mechanism} not yet supported for smart volume assertions"
            )

        return source_type, field

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
                    ),
                ],
                settings=models.AssertionMonitorSettingsClass(
                    adjustmentSettings=models.AssertionAdjustmentSettingsClass(
                        sensitivity=self._convert_sensitivity(),
                        exclusionWindows=self._convert_exclusion_windows(),
                        trainingDataLookbackWindowDays=self.training_data_lookback_days,
                    ),
                ),
            ),
        )

    def _assertion_type(self) -> str:
        """Get the assertion type."""
        return models.AssertionTypeClass.VOLUME
