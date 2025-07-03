"""
Smart Freshness Assertion Input classes for DataHub.

This module contains the _SmartFreshnessAssertionInput class that handles
input validation and entity creation for smart freshness assertions.
"""

from datetime import datetime
from typing import Optional, Union

from acryl_datahub_cloud.sdk.assertion_input.assertion_input import (
    DEFAULT_HOURLY_SCHEDULE,
    LAST_MODIFIED_ALLOWED_FIELD_TYPES,
    AssertionIncidentBehaviorInputTypes,
    DetectionMechanismInputTypes,
    ExclusionWindowInputTypes,
    FieldSpecType,
    InferenceSensitivity,
    _AssertionInput,
    _AuditLog,
    _DataHubOperation,
    _HasFreshnessFeatures,
    _HasSmartAssertionInputs,
    _InformationSchema,
    _LastModifiedColumn,
)
from acryl_datahub_cloud.sdk.entities.assertion import (
    AssertionInfoInputType,
    TagsInputType,
)
from acryl_datahub_cloud.sdk.errors import (
    SDKNotYetSupportedError,
    SDKUsageError,
)
from datahub.metadata import schema_classes as models
from datahub.metadata.urns import AssertionUrn, CorpUserUrn, DatasetUrn
from datahub.sdk.entity_client import EntityClient


class _SmartFreshnessAssertionInput(
    _AssertionInput, _HasSmartAssertionInputs, _HasFreshnessFeatures
):
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
            schedule=schedule
            if schedule is not None
            else DEFAULT_HOURLY_SCHEDULE,  # Use provided schedule or default for create case
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

    def _assertion_type(self) -> str:
        """Get the assertion type."""
        return models.AssertionTypeClass.FRESHNESS

    def _create_assertion_info(
        self, filter: Optional[models.DatasetFilterClass]
    ) -> AssertionInfoInputType:
        """
        Create a FreshnessAssertionInfoClass for a smart freshness assertion.

        Args:
            filter: Optional filter to apply to the assertion.

        Returns:
            A FreshnessAssertionInfoClass configured for smart freshness.
        """
        return models.FreshnessAssertionInfoClass(
            type=models.FreshnessAssertionTypeClass.DATASET_CHANGE,  # Currently only dataset change is supported
            entity=str(self.dataset_urn),
            # schedule (optional, must be left empty for smart freshness assertions - managed by the AI inference engine)
            filter=filter,
        )

    def _convert_schedule(self) -> models.CronScheduleClass:
        """Create a schedule for a smart freshness assertion.

        For create case, uses DEFAULT_HOURLY_SCHEDULE. For update case, preserves existing schedule.

        Returns:
            A CronScheduleClass with appropriate schedule settings.
        """
        assert self.schedule is not None, (
            "Schedule should never be None due to constructor logic"
        )
        return self.schedule

    def _get_assertion_evaluation_parameters(
        self, source_type: str, field: Optional[FieldSpecType]
    ) -> models.AssertionEvaluationParametersClass:
        # Ensure field is either None or FreshnessFieldSpecClass
        freshness_field = None
        if field is not None:
            if not isinstance(field, models.FreshnessFieldSpecClass):
                raise SDKUsageError(
                    f"Expected FreshnessFieldSpecClass for freshness assertion, got {type(field).__name__}"
                )
            freshness_field = field

        return models.AssertionEvaluationParametersClass(
            type=models.AssertionEvaluationParametersTypeClass.DATASET_FRESHNESS,
            datasetFreshnessParameters=models.DatasetFreshnessAssertionParametersClass(
                sourceType=source_type, field=freshness_field
            ),
        )

    def _convert_assertion_source_type_and_field(
        self,
    ) -> tuple[str, Optional[FieldSpecType]]:
        """
        Convert detection mechanism into source type and field specification for freshness assertions.

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
        source_type = models.DatasetFreshnessSourceTypeClass.INFORMATION_SCHEMA
        field = None

        if isinstance(self.detection_mechanism, _LastModifiedColumn):
            source_type = models.DatasetFreshnessSourceTypeClass.FIELD_VALUE
            field = self._create_field_spec(
                self.detection_mechanism.column_name,
                LAST_MODIFIED_ALLOWED_FIELD_TYPES,
                "last modified column",
                models.FreshnessFieldKindClass.LAST_MODIFIED,
                self._get_schema_field_spec,
                self._validate_field_type,
            )
        elif isinstance(self.detection_mechanism, _InformationSchema):
            source_type = models.DatasetFreshnessSourceTypeClass.INFORMATION_SCHEMA
        elif isinstance(self.detection_mechanism, _DataHubOperation):
            source_type = models.DatasetFreshnessSourceTypeClass.DATAHUB_OPERATION
        elif isinstance(self.detection_mechanism, _AuditLog):
            source_type = models.DatasetFreshnessSourceTypeClass.AUDIT_LOG
        else:
            raise SDKNotYetSupportedError(
                f"Detection mechanism {self.detection_mechanism} not yet supported for smart freshness assertions"
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
