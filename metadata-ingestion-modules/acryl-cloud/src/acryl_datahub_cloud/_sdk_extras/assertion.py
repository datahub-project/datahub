"""
This module contains the classes that represent assertions. These
classes are used to provide a user-friendly interface for creating and
managing assertions.

The actual Assertion Entity classes are defined in `metadata-ingestion/src/datahub/sdk`.
"""

from abc import ABC, abstractmethod
from datetime import datetime
from enum import Enum
from typing import Union

from typing_extensions import Self

from acryl_datahub_cloud._sdk_extras.assertion_input import (
    AssertionIncidentBehavior,
    DetectionMechanism,
    ExclusionWindowTypes,
    FixedRangeExclusionWindow,
    InferenceSensitivity,
)
from datahub.metadata.urns import AssertionUrn, DatasetUrn
from datahub.utilities.urns.urn import Urn


class AssertionMode(Enum):
    """
    The mode of an assertion, e.g. whether it is active or inactive.
    """

    # Note: Modeled here after MonitorStatus but called AssertionMode in this user facing interface
    # to keep all naming related to assertions.
    ACTIVE = "ACTIVE"
    INACTIVE = "INACTIVE"
    # PASSIVE = "PASSIVE" # Not supported in the user facing interface.


class _Assertion(ABC):
    """
    Abstract base class that represents an assertion and contains the common properties of all assertions.
    """

    def __init__(
        self,
        urn: AssertionUrn,
        dataset_urn: DatasetUrn,
        display_name: str,
        mode: AssertionMode,
        created_by: Urn,
        created_at: datetime,
        updated_by: Urn,
        updated_at: datetime,
        tags: list[Urn],
    ):
        """
        Initialize the base assertion class.

        Args:
            urn: The urn of the assertion.
            dataset_urn: The urn of the dataset that the assertion is for.
            display_name: The display name of the assertion.
            mode: The mode of the assertion (active, inactive).
            created_by: The urn of the user that created the assertion.
            created_at: The timestamp of when the assertion was created.
            updated_by: The urn of the user that updated the assertion.
            updated_at: The timestamp of when the assertion was updated.
            tags: The tags of the assertion.
        """
        self._urn = urn
        self._dataset_urn = dataset_urn
        self._display_name = display_name
        self._mode = mode
        self._created_by = created_by
        self._created_at = created_at
        self._updated_by = updated_by
        self._updated_at = updated_at
        self._tags = tags

    @property
    def urn(self) -> AssertionUrn:
        return self._urn

    @property
    def dataset_urn(self) -> DatasetUrn:
        return self._dataset_urn

    @property
    def display_name(self) -> str:
        return self._display_name

    @property
    def mode(self) -> AssertionMode:
        return self._mode

    @property
    def created_by(self) -> Urn:
        return self._created_by

    @property
    def created_at(self) -> datetime:
        return self._created_at

    @property
    def updated_by(self) -> Urn:
        return self._updated_by

    @property
    def updated_at(self) -> datetime:
        return self._updated_at

    @property
    def tags(self) -> list[Urn]:
        return self._tags

    @abstractmethod
    def from_entities(
        cls,
    ) -> (
        Self
    ):  # TODO: add these properties: , assertion: Assertion, monitor: Monitor) -> Self:
        """
        Create an assertion from the assertion and monitor entities.
        """
        pass


class SmartFreshnessAssertion(_Assertion):
    """
    A class that represents a smart freshness assertion.
    """

    def __init__(
        self,
        urn: AssertionUrn,
        dataset_urn: DatasetUrn,
        display_name: str,
        mode: AssertionMode,
        sensitivity: InferenceSensitivity,
        exclusion_windows: list[ExclusionWindowTypes],
        training_data_lookback_days: int,
        incident_behavior: list[AssertionIncidentBehavior],
        detection_mechanism: DetectionMechanism.DETECTION_MECHANISM_TYPES,
        created_by: Urn,
        created_at: datetime,
        updated_by: Urn,
        updated_at: datetime,
        tags: list[Urn],
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
            sensitivity: The sensitivity of the assertion (low, medium, high).
            exclusion_windows: The exclusion windows of the assertion.
            training_data_lookback_days: The max number of days of data to use for training the assertion.
            incident_behavior: Whether to raise or resolve an incident when the assertion fails / passes.
            detection_mechanism: The detection mechanism of the assertion.
            created_by: The urn of the user that created the assertion.
            created_at: The timestamp of when the assertion was created.
            updated_by: The urn of the user that updated the assertion.
            updated_at: The timestamp of when the assertion was updated.
            tags: The tags of the assertion.
        """
        super().__init__(
            urn=urn,
            dataset_urn=dataset_urn,
            display_name=display_name,
            mode=mode,
            created_by=created_by,
            created_at=created_at,
            updated_by=updated_by,
            updated_at=updated_at,
            tags=tags,
        )

        self._sensitivity = sensitivity
        self._exclusion_windows = exclusion_windows
        self._training_data_lookback_days = training_data_lookback_days
        self._incident_behavior = incident_behavior
        self._detection_mechanism = detection_mechanism

    @property
    def sensitivity(self) -> InferenceSensitivity:
        return self._sensitivity

    @property
    def exclusion_windows(self) -> list[ExclusionWindowTypes]:
        return self._exclusion_windows

    @property
    def training_data_lookback_days(self) -> int:
        return self._training_data_lookback_days

    @property
    def incident_behavior(self) -> list[AssertionIncidentBehavior]:
        return self._incident_behavior

    @property
    def detection_mechanism(self) -> DetectionMechanism.DETECTION_MECHANISM_TYPES:
        return self._detection_mechanism

    # TODO: Implement creation of this user facing assertion from the assertion and monitor entity in from_entities()
    @classmethod
    def from_entities(
        cls,
    ) -> (
        "SmartFreshnessAssertion"
    ):  # TODO: add params -> assertion: Assertion, monitor: Monitor) -> Self:
        """
        Create a smart freshness assertion from the assertion and monitor entities.
        """
        # TODO: Find these fields in the data model and then translate them to the user facing assertion fields:
        # - dataset_urn
        # - urn

        # TODO: Translate the following fields from the monitor or assertion entity to the user facing assertion fields:
        # From AssertionInfo:
        # - display_name -> comes from the description field in AssertionInfo

        # From status: optional MonitorStatus mode: in MonitorInfo:
        # Note: Modeled here after MonitorStatus but called AssertionStatus in this user facing interface.
        # - status

        # From settings: optional AssertionMonitorSettings, AssertionAdjustmentSettings in AssertionMonitor:
        # There is a capabilities field in AssertionMonitorSettings that can be used to determine which
        # settings are available for the assertion, we won't need to use that here we can just use the
        # fields directly that we know are applicable to SmartFreshnessAssertion.
        # - sensitivity
        # - exclusion_windows
        # - training_data_lookback_days

        # From AssertionEvaluationSpec -> AssertionEvaluationParameters -> DatasetFreshnessAssertionParameters.sourceType
        # And related fields if applicable.
        # - detection_mechanism

        # From AssertionActions: onFailure, onSuccess
        # - incident_behavior

        # AssertionInfo -> source: optional AssertionSource -> created: from created: optional AuditStamp
        # - created_by
        # - created_at

        # AssertionInfo -> lastUpdated: optional AuditStamp
        # - updated_by
        # - updated_at

        # Assertion -> globalTags: optional GlobalTags
        # - tags

        # TODO: Retrieve the fields from the assertion and monitor entities, not hardcoded as below:
        return SmartFreshnessAssertion(
            urn=AssertionUrn("urn:li:assertion:smart_freshness_assertion"),
            dataset_urn=DatasetUrn.from_string(
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,table_name,PROD)"
            ),
            display_name="Smart Freshness Assertion",
            mode=AssertionMode.ACTIVE,
            sensitivity=InferenceSensitivity.LOW,
            exclusion_windows=[
                FixedRangeExclusionWindow(
                    start=datetime(2021, 1, 1), end=datetime(2021, 1, 2)
                )
            ],
            training_data_lookback_days=30,
            incident_behavior=[AssertionIncidentBehavior.RAISE_ON_FAIL],
            detection_mechanism=DetectionMechanism.INFORMATION_SCHEMA,
            created_by=Urn.from_string("urn:li:corpuser:acryl-cloud-user"),
            created_at=datetime(2021, 1, 1),
            updated_by=Urn.from_string("urn:li:corpuser:acryl-cloud-user"),
            updated_at=datetime(2021, 1, 1),
            tags=[],
        )


AssertionTypes = Union[
    SmartFreshnessAssertion,
    # TODO: Add other assertion types here as we add them.
]
