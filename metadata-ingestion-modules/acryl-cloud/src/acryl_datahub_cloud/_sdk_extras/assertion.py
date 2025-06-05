"""
This module contains the classes that represent assertions. These
classes are used to provide a user-friendly interface for creating and
managing assertions.

The actual Assertion Entity classes are defined in `metadata-ingestion/src/datahub/sdk`.
"""

import logging
from abc import ABC, abstractmethod
from datetime import datetime
from enum import Enum
from typing import Any, Optional, Union

from typing_extensions import Self

from acryl_datahub_cloud._sdk_extras.assertion_input import (
    ASSERTION_MONITOR_DEFAULT_TRAINING_LOOKBACK_WINDOW_DAYS,
    DEFAULT_DETECTION_MECHANISM,
    DEFAULT_SENSITIVITY,
    AssertionIncidentBehavior,
    DetectionMechanism,
    ExclusionWindowTypes,
    FixedRangeExclusionWindow,
    InferenceSensitivity,
    _DetectionMechanismTypes,
)
from acryl_datahub_cloud._sdk_extras.entities.assertion import Assertion
from acryl_datahub_cloud._sdk_extras.entities.monitor import Monitor
from acryl_datahub_cloud._sdk_extras.errors import SDKNotYetSupportedError
from datahub.emitter.mce_builder import parse_ts_millis
from datahub.metadata import schema_classes as models
from datahub.metadata.urns import AssertionUrn, CorpUserUrn, DatasetUrn, TagUrn
from datahub.sdk.entity import Entity

logger = logging.getLogger(__name__)


class AssertionMode(Enum):
    """
    The mode of an assertion, e.g. whether it is active or inactive.
    """

    # Note: Modeled here after MonitorStatus but called AssertionMode in this user facing interface
    # to keep all naming related to assertions.
    ACTIVE = "ACTIVE"
    INACTIVE = "INACTIVE"
    # PASSIVE = "PASSIVE" # Not supported in the user facing interface.


def _get_nested_field_for_entity_with_default(
    entity: Entity,
    field_path: str,
    default: Any = None,
) -> Any:
    """
    Get a nested field from an Entity object, and warn and return default if not found.

    Args:
        entity: The entity to get the nested field from.
        field_path: The path to the nested field.
        default: The default value to return if the field is not found.
    """
    fields = field_path.split(".")
    current = entity
    last_valid_path = entity.entity_type_name()

    for field in fields:
        try:
            current = getattr(current, field)
            last_valid_path = f"{last_valid_path}.{field}"
        except AttributeError:
            logger.warning(
                f"{entity.entity_type_name().capitalize()} {entity.urn} does not have an `{last_valid_path}` field, defaulting to {default}"
            )
            return default

    return current


class _HasSmartFunctionality:
    """
    Mixin class that provides smart functionality for assertions.
    """

    def __init__(
        self,
        *,
        sensitivity: InferenceSensitivity = DEFAULT_SENSITIVITY,
        exclusion_windows: list[ExclusionWindowTypes],
        training_data_lookback_days: int = ASSERTION_MONITOR_DEFAULT_TRAINING_LOOKBACK_WINDOW_DAYS,
        incident_behavior: list[AssertionIncidentBehavior],
        detection_mechanism: Optional[
            _DetectionMechanismTypes
        ] = DEFAULT_DETECTION_MECHANISM,
    ) -> None:
        """
        Initialize the smart functionality mixin.

        Args:
            sensitivity: The sensitivity of the assertion (low, medium, high).
            exclusion_windows: The exclusion windows of the assertion.
            training_data_lookback_days: The max number of days of data to use for training the assertion.
            incident_behavior: Whether to raise or resolve an incident when the assertion fails / passes.
            detection_mechanism: The detection mechanism of the assertion.
            **kwargs: Additional arguments to pass to the parent class (_Assertion).
        """
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
    def detection_mechanism(self) -> Optional[_DetectionMechanismTypes]:
        return self._detection_mechanism

    @staticmethod
    def _get_sensitivity(monitor: Monitor) -> InferenceSensitivity:
        # 1. Check if the monitor has a sensitivity field
        raw_sensitivity = _get_nested_field_for_entity_with_default(
            monitor,
            "info.assertionMonitor.settings.adjustmentSettings.sensitivity.level",
            DEFAULT_SENSITIVITY,
        )

        # 2. Convert the raw sensitivity to the SDK sensitivity enum (1-3: LOW, 4-6: MEDIUM, 7-10: HIGH)
        return InferenceSensitivity.parse(raw_sensitivity)

    @staticmethod
    def _get_exclusion_windows(monitor: Monitor) -> list[ExclusionWindowTypes]:
        # 1. Check if the monitor has an exclusion windows field
        raw_windows = monitor.exclusion_windows or []

        # 2. Convert the raw exclusion windows to the SDK exclusion windows
        exclusion_windows = []
        for raw_window in raw_windows:
            if raw_window.type == models.AssertionExclusionWindowTypeClass.FIXED_RANGE:
                if raw_window.fixedRange is None:
                    logger.warning(
                        f"Monitor {monitor.urn} has a fixed range exclusion window with no fixed range, skipping"
                    )
                    continue
                exclusion_windows.append(
                    FixedRangeExclusionWindow(
                        start=parse_ts_millis(raw_window.fixedRange.startTimeMillis),
                        end=parse_ts_millis(raw_window.fixedRange.endTimeMillis),
                    )
                )
            else:
                raise SDKNotYetSupportedError(
                    f"AssertionExclusionWindowType {raw_window.type}"
                )
        return exclusion_windows

    @staticmethod
    def _get_training_data_lookback_days(monitor: Monitor) -> int:
        retrieved = monitor.training_data_lookback_days
        if (
            retrieved is None
        ):  # Explicitly check for None since retrieved can be 0 which is falsy
            return ASSERTION_MONITOR_DEFAULT_TRAINING_LOOKBACK_WINDOW_DAYS
        assert isinstance(retrieved, int)
        return retrieved

    @staticmethod
    def _get_detection_mechanism(
        assertion: Assertion,
        monitor: Monitor,
        default: Optional[_DetectionMechanismTypes] = DEFAULT_DETECTION_MECHANISM,
    ) -> Optional[_DetectionMechanismTypes]:
        """Get the detection mechanism from the monitor and assertion."""
        if not _HasSmartFunctionality._has_valid_monitor_info(monitor):
            return default

        # 1. Check if the assertion has a parameters field
        def _warn_and_return_default_detection_mechanism(
            field_name: str,
            default: Optional[_DetectionMechanismTypes] = DEFAULT_DETECTION_MECHANISM,
        ) -> Optional[_DetectionMechanismTypes]:
            logger.warning(
                f"Monitor {monitor.urn} does not have an `{field_name}` field, defaulting detection mechanism to {default}"
            )
            return default

        parameters = _HasSmartFunctionality._get_assertion_parameters(monitor, default)
        if parameters is None:
            return _warn_and_return_default_detection_mechanism("parameters", default)

        # 2. Convert the raw detection mechanism to the SDK detection mechanism
        if (
            parameters.type
            == models.AssertionEvaluationParametersTypeClass.DATASET_FRESHNESS
        ):
            # TODO: Add support for other detection mechanisms when other assertion types are supported
            return _HasSmartFunctionality._get_freshness_detection_mechanism(
                assertion, parameters, default
            )
        else:
            raise SDKNotYetSupportedError(
                f"AssertionEvaluationParametersType {parameters.type}"
            )

    @staticmethod
    def _has_valid_monitor_info(monitor: Monitor) -> bool:
        """Check if monitor has valid info and assertion monitor."""

        def _warn_and_return_false(field_name: str) -> bool:
            logger.warning(
                f"Monitor {monitor.urn} does not have an `{field_name}` field, defaulting detection mechanism to {DEFAULT_DETECTION_MECHANISM}"
            )
            return False

        if monitor.info is None:
            return _warn_and_return_false("info")
        if monitor.info.assertionMonitor is None:
            return _warn_and_return_false("assertionMonitor")
        if (
            monitor.info.assertionMonitor.assertions is None
            or len(monitor.info.assertionMonitor.assertions) == 0
        ):
            return _warn_and_return_false("assertionMonitor.assertions")

        return True

    @staticmethod
    def _get_assertion_parameters(
        monitor: Monitor,
        default: Optional[_DetectionMechanismTypes] = DEFAULT_DETECTION_MECHANISM,
    ) -> Optional[models.AssertionEvaluationParametersClass]:
        """Get the assertion parameters from the monitor."""
        # We know these are not None from _has_valid_monitor_info check
        assert (
            monitor is not None
            and monitor.info is not None
            and monitor.info.assertionMonitor is not None
        )
        assertion_monitor = monitor.info.assertionMonitor
        assert (
            assertion_monitor is not None and assertion_monitor.assertions is not None
        )
        assertions = assertion_monitor.assertions

        if assertions[0].parameters is None:
            logger.warning(
                f"Monitor {monitor.urn} does not have a assertionMonitor.assertions[0].parameters, defaulting detection mechanism to {default}"
            )
            return None
        return assertions[0].parameters

    @staticmethod
    def _get_freshness_detection_mechanism(
        assertion: Assertion,
        parameters: models.AssertionEvaluationParametersClass,
        default: Optional[_DetectionMechanismTypes] = DEFAULT_DETECTION_MECHANISM,
    ) -> Optional[_DetectionMechanismTypes]:
        """Get the detection mechanism for freshness assertions."""
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
            return _HasSmartFunctionality._get_field_value_detection_mechanism(
                assertion, parameters
            )
        elif source_type == models.DatasetFreshnessSourceTypeClass.DATAHUB_OPERATION:
            return DetectionMechanism.DATAHUB_OPERATION
        elif source_type == models.DatasetFreshnessSourceTypeClass.FILE_METADATA:
            raise SDKNotYetSupportedError("FILE_METADATA DatasetFreshnessSourceType")
        else:
            raise SDKNotYetSupportedError(f"DatasetFreshnessSourceType {source_type}")

    @staticmethod
    def _get_field_value_detection_mechanism(
        assertion: Assertion,
        parameters: models.AssertionEvaluationParametersClass,
    ) -> _DetectionMechanismTypes:
        """Get the detection mechanism for field value based freshness."""
        # We know datasetFreshnessParameters is not None from _get_freshness_detection_mechanism check
        assert parameters.datasetFreshnessParameters is not None
        field = parameters.datasetFreshnessParameters.field

        if field is None or field.kind is None:
            logger.warning(
                f"Monitor does not have valid field info, defaulting detection mechanism to {DEFAULT_DETECTION_MECHANISM}"
            )
            return DEFAULT_DETECTION_MECHANISM

        column_name = field.path
        additional_filter = _HasSmartFunctionality._get_additional_filter(assertion)

        if field.kind == models.FreshnessFieldKindClass.LAST_MODIFIED:
            return DetectionMechanism.LAST_MODIFIED_COLUMN(
                column_name=column_name, additional_filter=additional_filter
            )
        elif field.kind == models.FreshnessFieldKindClass.HIGH_WATERMARK:
            return DetectionMechanism.HIGH_WATERMARK_COLUMN(
                column_name=column_name, additional_filter=additional_filter
            )
        else:
            raise SDKNotYetSupportedError(f"FreshnessFieldKind {field.kind}")

    @staticmethod
    def _get_additional_filter(assertion: Assertion) -> Optional[str]:
        """Get the additional filter SQL from the assertion."""
        if assertion.info is None:
            logger.warning(
                f"Assertion {assertion.urn} does not have an info, defaulting additional filter to None"
            )
            return None
        if (
            not isinstance(assertion.info, models.FreshnessAssertionInfoClass)
            or assertion.info.filter is None
        ):
            logger.warning(
                f"Assertion {assertion.urn} does not have a filter, defaulting additional filter to None"
            )
            return None
        if assertion.info.filter.type != models.DatasetFilterTypeClass.SQL:
            raise SDKNotYetSupportedError(
                f"DatasetFilterType {assertion.info.filter.type}"
            )
        return assertion.info.filter.sql


class _AssertionPublic(ABC):
    """
    Abstract base class that represents a public facing assertion and contains the common properties of all assertions.
    """

    def __init__(
        self,
        *,
        urn: AssertionUrn,
        dataset_urn: DatasetUrn,
        display_name: str,
        mode: AssertionMode,
        tags: list[TagUrn],
        created_by: Optional[CorpUserUrn] = None,
        created_at: Union[datetime, None] = None,
        updated_by: Optional[CorpUserUrn] = None,
        updated_at: Optional[datetime] = None,
    ):
        """
        Initialize the public facing assertion class.

        Args:
            urn: The urn of the assertion.
            dataset_urn: The urn of the dataset that the assertion is for.
            display_name: The display name of the assertion.
            mode: The mode of the assertion (active, inactive).
            tags: The tags of the assertion.
            created_by: The urn of the user that created the assertion.
            created_at: The timestamp of when the assertion was created.
            updated_by: The urn of the user that updated the assertion.
            updated_at: The timestamp of when the assertion was updated.
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
    def created_by(self) -> Optional[CorpUserUrn]:
        return self._created_by

    @property
    def created_at(self) -> Union[datetime, None]:
        return self._created_at

    @property
    def updated_by(self) -> Optional[CorpUserUrn]:
        return self._updated_by

    @property
    def updated_at(self) -> Union[datetime, None]:
        return self._updated_at

    @property
    def tags(self) -> list[TagUrn]:
        return self._tags

    @staticmethod
    def _get_incident_behavior(assertion: Assertion) -> list[AssertionIncidentBehavior]:
        incident_behaviors = []
        for action in assertion.on_failure + assertion.on_success:
            if action.type == models.AssertionActionTypeClass.RAISE_INCIDENT:
                incident_behaviors.append(AssertionIncidentBehavior.RAISE_ON_FAIL)
            elif action.type == models.AssertionActionTypeClass.RESOLVE_INCIDENT:
                incident_behaviors.append(AssertionIncidentBehavior.RESOLVE_ON_PASS)

        return incident_behaviors

    @staticmethod
    def _get_created_by(assertion: Assertion) -> Optional[CorpUserUrn]:
        if assertion.source is None:
            logger.warning(f"Assertion {assertion.urn} does not have a source")
            return None
        if isinstance(assertion.source, models.AssertionSourceClass):
            if assertion.source.created is None:
                logger.warning(
                    f"Assertion {assertion.urn} does not have a created by in the source"
                )
                return None
            return CorpUserUrn.from_string(assertion.source.created.actor)
        elif isinstance(assertion.source, models.AssertionSourceTypeClass):
            logger.warning(
                f"Assertion {assertion.urn} has a source type with no created by"
            )
            return None
        return None

    @staticmethod
    def _get_created_at(assertion: Assertion) -> Union[datetime, None]:
        if assertion.source is None:
            logger.warning(f"Assertion {assertion.urn} does not have a source")
            return None
        if isinstance(assertion.source, models.AssertionSourceClass):
            if assertion.source.created is None:
                logger.warning(
                    f"Assertion {assertion.urn} does not have a created by in the source"
                )
                return None
            return parse_ts_millis(assertion.source.created.time)
        elif isinstance(assertion.source, models.AssertionSourceTypeClass):
            logger.warning(
                f"Assertion {assertion.urn} has a source type with no created by"
            )
            return None
        return None

    @staticmethod
    def _get_updated_by(assertion: Assertion) -> Optional[CorpUserUrn]:
        if assertion.last_updated is None:
            logger.warning(f"Assertion {assertion.urn} does not have a last updated")
            return None
        return CorpUserUrn.from_string(assertion.last_updated.actor)

    @staticmethod
    def _get_updated_at(assertion: Assertion) -> Union[datetime, None]:
        if assertion.last_updated is None:
            logger.warning(f"Assertion {assertion.urn} does not have a last updated")
            return None
        return parse_ts_millis(assertion.last_updated.time)

    @staticmethod
    def _get_tags(assertion: Assertion) -> list[TagUrn]:
        return [TagUrn.from_string(t.tag) for t in assertion.tags or []]

    @staticmethod
    def _get_mode(monitor: Monitor) -> AssertionMode:
        if monitor.info is None:
            logger.warning(
                f"Monitor {monitor.urn} does not have a info, defaulting status to INACTIVE"
            )
            return AssertionMode.INACTIVE
        return AssertionMode(monitor.info.status.mode)

    @abstractmethod
    def from_entities(
        cls,
        assertion: Assertion,
        monitor: Monitor,
    ) -> Self:
        """
        Create an assertion from the assertion and monitor entities.
        """
        pass


class SmartFreshnessAssertion(_HasSmartFunctionality, _AssertionPublic):
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
        # Initialize the mixin first
        _HasSmartFunctionality.__init__(
            self,
            sensitivity=sensitivity,
            exclusion_windows=exclusion_windows,
            training_data_lookback_days=training_data_lookback_days,
            incident_behavior=incident_behavior,
            detection_mechanism=detection_mechanism,
        )
        # Then initialize the parent class
        _AssertionPublic.__init__(
            self,
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

    @classmethod
    def from_entities(cls, assertion: Assertion, monitor: Monitor) -> Self:
        """
        Create a smart freshness assertion from the assertion and monitor entities.
        """
        return cls(
            urn=assertion.urn,
            dataset_urn=assertion.dataset,
            display_name=assertion.description or "",
            mode=cls._get_mode(monitor),
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


AssertionTypes = Union[
    SmartFreshnessAssertion,
    # TODO: Add other assertion types here as we add them.
]
