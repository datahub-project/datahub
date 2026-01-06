"""
Smart SQL Assertion class for DataHub.

This module contains the SmartSqlAssertion class that represents
a smart SQL assertion with AI-powered inference.
"""

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
    DEFAULT_DAILY_SCHEDULE,
    AssertionIncidentBehavior,
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


class SmartSqlAssertion(_AssertionPublic, _HasSchedule, _HasSmartFunctionality):
    """
    A class that represents a Smart SQL assertion with AI-powered inference.

    Smart SQL assertions use machine learning to infer appropriate thresholds
    for the SQL query result instead of requiring fixed threshold values.
    """

    def __init__(
        self,
        *,
        urn: AssertionUrn,
        dataset_urn: DatasetUrn,
        display_name: str,
        mode: AssertionMode,
        statement: str,
        schedule: models.CronScheduleClass,
        sensitivity: InferenceSensitivity,
        exclusion_windows: list[ExclusionWindowTypes],
        training_data_lookback_days: int,
        tags: list[TagUrn],
        incident_behavior: list[AssertionIncidentBehavior],
        created_by: Optional[CorpUserUrn] = None,
        created_at: Union[datetime, None] = None,
        updated_by: Optional[CorpUserUrn] = None,
        updated_at: Optional[datetime] = None,
    ):
        """
        Initialize a Smart SQL assertion.

        Note: Values can be accessed, but not set on the assertion object.
        To update an assertion, use the `sync_smart_sql_assertion` method.

        Args:
            urn: The urn of the assertion.
            dataset_urn: The urn of the dataset that the assertion is for.
            display_name: The display name of the assertion.
            mode: The mode of the assertion (active, inactive).
            statement: The SQL statement to be used for the assertion.
            schedule: The schedule of the assertion.
            sensitivity: The sensitivity level for AI inference (low, medium, high).
            exclusion_windows: Time windows to exclude from assertion evaluation.
            training_data_lookback_days: Number of days of data to use for training.
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
        _HasSmartFunctionality.__init__(
            self,
            sensitivity=sensitivity,
            exclusion_windows=exclusion_windows,
            training_data_lookback_days=training_data_lookback_days,
        )
        self._statement = statement

    @property
    def statement(self) -> str:
        return self._statement

    @staticmethod
    def _get_detection_mechanism(
        assertion: Assertion,
        monitor: Monitor,
        default: Optional[_DetectionMechanismTypes] = None,
    ) -> Optional[_DetectionMechanismTypes]:
        """Smart SQL assertions do not have a detection mechanism."""
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

    @classmethod
    def _from_entities(cls, assertion: Assertion, monitor: Monitor) -> Self:
        """
        Create a Smart SQL assertion from the assertion and monitor entities.
        """
        return cls(
            urn=assertion.urn,
            dataset_urn=assertion.dataset,
            display_name=assertion.description or "",
            mode=cls._get_mode(monitor),
            statement=cls._get_statement(assertion),
            schedule=cls._get_schedule(monitor, default=DEFAULT_DAILY_SCHEDULE),
            sensitivity=cls._get_sensitivity(monitor),
            exclusion_windows=cls._get_exclusion_windows(monitor),
            training_data_lookback_days=cls._get_training_data_lookback_days(monitor),
            tags=cls._get_tags(assertion),
            incident_behavior=cls._get_incident_behavior(assertion),
            created_by=cls._get_created_by(assertion),
            created_at=cls._get_created_at(assertion),
            updated_by=cls._get_updated_by(assertion),
            updated_at=cls._get_updated_at(assertion),
        )
