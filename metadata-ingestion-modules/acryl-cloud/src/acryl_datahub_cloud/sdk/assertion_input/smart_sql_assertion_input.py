"""
Smart SQL Assertion Input classes for DataHub.

This module contains the _SmartSqlAssertionInput class that handles
input validation and entity creation for smart SQL assertions.
"""

from datetime import datetime
from typing import Optional, Union

from acryl_datahub_cloud.sdk.assertion_input.assertion_input import (
    DEFAULT_EVERY_SIX_HOURS_SCHEDULE,
    AssertionIncidentBehaviorInputTypes,
    ExclusionWindowInputTypes,
    FieldSpecType,
    InferenceSensitivity,
    _AssertionInput,
    _HasSmartAssertionInputs,
)
from acryl_datahub_cloud.sdk.entities.assertion import (
    AssertionInfoInputType,
    TagsInputType,
)
from datahub.metadata import schema_classes as models
from datahub.metadata.urns import AssertionUrn, CorpUserUrn, DatasetUrn
from datahub.sdk.entity_client import EntityClient


class _SmartSqlAssertionInput(_AssertionInput, _HasSmartAssertionInputs):
    """Input class for smart SQL assertions with AI-powered inference."""

    def __init__(
        self,
        *,
        dataset_urn: Union[str, DatasetUrn],
        entity_client: EntityClient,
        statement: Optional[str],
        created_by: Union[str, CorpUserUrn],
        created_at: datetime,
        updated_by: Union[str, CorpUserUrn],
        updated_at: datetime,
        urn: Optional[Union[str, AssertionUrn]] = None,
        display_name: Optional[str] = None,
        enabled: bool = True,
        schedule: Optional[Union[str, models.CronScheduleClass]] = None,
        sensitivity: Optional[Union[str, InferenceSensitivity]] = None,
        exclusion_windows: Optional[ExclusionWindowInputTypes] = None,
        training_data_lookback_days: Optional[int] = None,
        incident_behavior: Optional[AssertionIncidentBehaviorInputTypes] = None,
        tags: Optional[TagsInputType] = None,
    ):
        _AssertionInput.__init__(
            self,
            dataset_urn=dataset_urn,
            entity_client=entity_client,
            urn=urn,
            display_name=display_name,
            enabled=enabled,
            schedule=schedule,
            detection_mechanism=None,  # Smart SQL assertions don't use detection mechanisms
            incident_behavior=incident_behavior,
            tags=tags,
            source_type=models.AssertionSourceTypeClass.INFERRED,  # Smart assertions are of type inferred
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
        self.statement = statement

    def _create_assertion_info(
        self, filter: Optional[models.DatasetFilterClass]
    ) -> AssertionInfoInputType:
        """
        Create a SqlAssertionInfoClass for a smart SQL assertion.

        For smart SQL assertions, we use METRIC type with a placeholder operator/parameters
        since the actual thresholds are inferred by AI.

        Args:
            filter: Optional filter (not used for SQL assertions).

        Returns:
            A SqlAssertionInfoClass configured for smart SQL.
        """

        assert self.statement is not None, (
            "statement is required for Smart SQL assertions"
        )

        # For smart SQL assertions, we use METRIC type with placeholder values
        # The actual thresholds are inferred by the AI model
        return models.SqlAssertionInfoClass(
            type=models.SqlAssertionTypeClass.METRIC,
            entity=str(self.dataset_urn),
            statement=self.statement,
            changeType=None,
            # Use GREATER_THAN_OR_EQUAL_TO with 0 as a placeholder - AI will infer actual thresholds
            operator=models.AssertionStdOperatorClass.GREATER_THAN_OR_EQUAL_TO,
            parameters=models.AssertionStdParametersClass(
                value=models.AssertionStdParameterClass(
                    type=models.AssertionStdParameterTypeClass.NUMBER,
                    value="0",
                ),
            ),
        )

    def _convert_schedule(self) -> models.CronScheduleClass:
        """Create a schedule for a smart SQL assertion.

        Returns:
            A CronScheduleClass with appropriate schedule settings.
        """
        if self.schedule is None:
            return DEFAULT_EVERY_SIX_HOURS_SCHEDULE

        return models.CronScheduleClass(
            cron=self.schedule.cron,
            timezone=self.schedule.timezone,
        )

    def _get_assertion_evaluation_parameters(
        self, source_type: str, field: Optional[FieldSpecType]
    ) -> models.AssertionEvaluationParametersClass:
        return models.AssertionEvaluationParametersClass(
            type=models.AssertionEvaluationParametersTypeClass.DATASET_SQL,
        )

    def _convert_assertion_source_type_and_field(
        self,
    ) -> tuple[str, None]:
        """SQL assertions do not have source type or field."""
        return "None", None

    def _create_filter_from_detection_mechanism(
        self,
    ) -> Optional[models.DatasetFilterClass]:
        """SQL assertions do not use filters."""
        return None

    def _create_monitor_info(
        self,
        assertion_urn: AssertionUrn,
        status: models.MonitorStatusClass,
        schedule: models.CronScheduleClass,
    ) -> models.MonitorInfoClass:
        """
        Create a MonitorInfoClass with all the necessary components for smart SQL assertion.
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
                            "None", None
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
        return models.AssertionTypeClass.SQL
