from datetime import datetime
from typing import Optional, Union

from acryl_datahub_cloud.sdk.assertion_input.assertion_input import (
    DEFAULT_DAILY_SCHEDULE,
    HIGH_WATERMARK_ALLOWED_FIELD_TYPES,
    LAST_MODIFIED_ALLOWED_FIELD_TYPES,
    AssertionIncidentBehavior,
    DetectionMechanismInputTypes,
    FieldSpecType,
    TimeWindowSizeInputTypes,
    _AssertionInput,
    _AuditLog,
    _DataHubOperation,
    _HasFreshnessFeatures,
    _HighWatermarkColumn,
    _InformationSchema,
    _LastModifiedColumn,
    _try_parse_and_validate_schema_classes_enum,
    _try_parse_time_window_size,
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


class _FreshnessAssertionInput(_AssertionInput, _HasFreshnessFeatures):
    def _assertion_type(self) -> str:
        """Get the assertion type."""
        return models.AssertionTypeClass.FRESHNESS

    def __init__(
        self,
        *,
        # Required fields
        dataset_urn: Union[str, DatasetUrn],
        entity_client: EntityClient,  # Needed to get the schema field spec for the detection mechanism if needed
        urn: Optional[Union[str, AssertionUrn]] = None,
        # Optional fields
        display_name: Optional[str] = None,
        enabled: bool = True,
        schedule: Optional[Union[str, models.CronScheduleClass]] = None,
        detection_mechanism: DetectionMechanismInputTypes = None,
        incident_behavior: Optional[
            Union[AssertionIncidentBehavior, list[AssertionIncidentBehavior]]
        ] = None,
        tags: Optional[TagsInputType] = None,
        created_by: Union[str, CorpUserUrn],
        created_at: datetime,
        updated_by: Union[str, CorpUserUrn],
        updated_at: datetime,
        freshness_schedule_check_type: Optional[
            Union[str, models.FreshnessAssertionScheduleTypeClass]
        ] = None,
        lookback_window: Optional[TimeWindowSizeInputTypes] = None,
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
            source_type=models.AssertionSourceTypeClass.NATIVE,  # Native assertions are of type native, not inferred
            created_by=created_by,
            created_at=created_at,
            updated_by=updated_by,
            updated_at=updated_at,
        )

        self.freshness_schedule_check_type = (
            _try_parse_and_validate_schema_classes_enum(
                freshness_schedule_check_type
                or models.FreshnessAssertionScheduleTypeClass.SINCE_THE_LAST_CHECK,
                models.FreshnessAssertionScheduleTypeClass,
            )
        )
        self.lookback_window = (
            _try_parse_time_window_size(lookback_window) if lookback_window else None
        )
        if (
            self.freshness_schedule_check_type
            is models.FreshnessAssertionScheduleTypeClass.FIXED_INTERVAL
            and lookback_window is None
        ):
            raise SDKUsageError(
                "Fixed interval freshness assertions must have a lookback_window provided."
            )
        if (
            self.freshness_schedule_check_type
            is models.FreshnessAssertionScheduleTypeClass.SINCE_THE_LAST_CHECK
            and lookback_window is not None
        ):
            raise SDKUsageError(
                "Since the last check freshness assertions cannot have a lookback_window provided."
            )

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
                    )
                ]
            ),
        )

    def _create_assertion_info(
        self, filter: Optional[models.DatasetFilterClass]
    ) -> AssertionInfoInputType:
        """
        Create a FreshnessAssertionInfoClass for a freshness assertion.

        Args:
            filter: Optional filter to apply to the assertion. Only relevant for QUERY detection mechanism.

        Returns:
            A FreshnessAssertionInfoClass configured for freshness.
        """
        schedule = self._convert_schedule()
        return models.FreshnessAssertionInfoClass(
            type=models.FreshnessAssertionTypeClass.DATASET_CHANGE,  # Currently only dataset change is supported
            entity=str(self.dataset_urn),
            schedule=models.FreshnessAssertionScheduleClass(
                type=self.freshness_schedule_check_type
                or models.FreshnessAssertionScheduleTypeClass.SINCE_THE_LAST_CHECK,
                cron=models.FreshnessCronScheduleClass(
                    cron=schedule.cron,
                    timezone=schedule.timezone,
                ),
                fixedInterval=models.FixedIntervalScheduleClass(
                    multiple=self.lookback_window.multiple,
                    unit=self.lookback_window.unit,
                )
                if self.lookback_window
                else None,
            ),
            filter=filter,
        )

    def _convert_schedule(self) -> models.CronScheduleClass:
        """Create a schedule for a freshness assertion.

        Returns:
            A CronScheduleClass with appropriate schedule settings.
        """
        if self.schedule is None:
            return DEFAULT_DAILY_SCHEDULE

        return models.CronScheduleClass(
            cron=self.schedule.cron,
            timezone=self.schedule.timezone,
        )

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

        if isinstance(self.detection_mechanism, _InformationSchema):
            source_type = models.DatasetFreshnessSourceTypeClass.INFORMATION_SCHEMA
        elif isinstance(self.detection_mechanism, _DataHubOperation):
            source_type = models.DatasetFreshnessSourceTypeClass.DATAHUB_OPERATION
        elif isinstance(self.detection_mechanism, _AuditLog):
            source_type = models.DatasetFreshnessSourceTypeClass.AUDIT_LOG
        elif isinstance(self.detection_mechanism, _LastModifiedColumn):
            source_type = models.DatasetFreshnessSourceTypeClass.FIELD_VALUE
            field = self._create_field_spec(
                self.detection_mechanism.column_name,
                LAST_MODIFIED_ALLOWED_FIELD_TYPES,
                "last modified column",
                models.FreshnessFieldKindClass.LAST_MODIFIED,
                self._get_schema_field_spec,
                self._validate_field_type,
            )
        elif isinstance(self.detection_mechanism, _HighWatermarkColumn):
            if (
                self.freshness_schedule_check_type
                is models.FreshnessAssertionScheduleTypeClass.FIXED_INTERVAL
            ):
                raise SDKUsageError(
                    "Fixed interval freshness assertions cannot have a high watermark column provided."
                )
            source_type = models.DatasetFreshnessSourceTypeClass.FIELD_VALUE
            field = self._create_field_spec(
                self.detection_mechanism.column_name,
                HIGH_WATERMARK_ALLOWED_FIELD_TYPES,
                "high watermark column",
                models.FreshnessFieldKindClass.HIGH_WATERMARK,
                self._get_schema_field_spec,
                self._validate_field_type,
            )
        else:
            raise SDKNotYetSupportedError(
                f"Detection mechanism {self.detection_mechanism} not yet supported for freshness assertions"
            )

        return source_type, field
