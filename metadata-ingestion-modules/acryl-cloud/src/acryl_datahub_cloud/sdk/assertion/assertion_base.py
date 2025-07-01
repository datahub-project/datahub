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
from typing import Optional, Union

from typing_extensions import Self

from acryl_datahub_cloud.sdk.assertion_input.assertion_input import (
    ASSERTION_MONITOR_DEFAULT_TRAINING_LOOKBACK_WINDOW_DAYS,
    DEFAULT_DETECTION_MECHANISM,
    DEFAULT_EVERY_SIX_HOURS_SCHEDULE,
    DEFAULT_SCHEDULE,
    DEFAULT_SENSITIVITY,
    AssertionIncidentBehavior,
    DetectionMechanism,
    ExclusionWindowTypes,
    FixedRangeExclusionWindow,
    InferenceSensitivity,
    TimeWindowSizeInputTypes,
    _DetectionMechanismTypes,
)
from acryl_datahub_cloud.sdk.assertion_input.column_metric_constants import (
    MetricInputType,
    OperatorInputType,
)
from acryl_datahub_cloud.sdk.assertion_input.smart_column_metric_assertion_input import (
    SmartColumnMetricAssertionParameters,
)
from acryl_datahub_cloud.sdk.assertion_input.sql_assertion_input import (
    SqlAssertionCondition,
    SqlAssertionCriteria,
)
from acryl_datahub_cloud.sdk.assertion_input.volume_assertion_input import (
    VolumeAssertionCriteria,
)
from acryl_datahub_cloud.sdk.entities.assertion import Assertion
from acryl_datahub_cloud.sdk.entities.monitor import (
    Monitor,
    _get_nested_field_for_entity_with_default,
)
from acryl_datahub_cloud.sdk.errors import SDKNotYetSupportedError, SDKUsageError
from datahub.emitter.mce_builder import parse_ts_millis
from datahub.metadata import schema_classes as models
from datahub.metadata.urns import AssertionUrn, CorpUserUrn, DatasetUrn, TagUrn

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


class _HasSchedule:
    """
    Mixin class that provides schedule functionality for assertions.
    """

    def __init__(self, schedule: models.CronScheduleClass) -> None:
        self._schedule = schedule

    @property
    def schedule(self) -> models.CronScheduleClass:
        return self._schedule

    @staticmethod
    def _get_schedule(
        monitor: Monitor, default: models.CronScheduleClass = DEFAULT_SCHEDULE
    ) -> models.CronScheduleClass:
        """Get the schedule from the monitor."""
        assertion_evaluation_specs = _get_nested_field_for_entity_with_default(
            monitor,
            "info.assertionMonitor.assertions",
            [],
        )
        if len(assertion_evaluation_specs) == 0:
            return default
        assertion_evaluation_spec = assertion_evaluation_specs[0]
        schedule = assertion_evaluation_spec.schedule
        if schedule is None:
            return default
        return schedule


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

    @property
    def sensitivity(self) -> InferenceSensitivity:
        return self._sensitivity

    @property
    def exclusion_windows(self) -> list[ExclusionWindowTypes]:
        return self._exclusion_windows

    @property
    def training_data_lookback_days(self) -> int:
        return self._training_data_lookback_days

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


class _HasColumnMetricFunctionality:
    """
    Mixin class that provides column metric functionality for assertions.
    """

    def __init__(
        self,
        column_name: str,
        metric_type: MetricInputType,
        operator: OperatorInputType,
        criteria_parameters: Optional[SmartColumnMetricAssertionParameters] = None,
    ):
        self._column_name = column_name
        self._metric_type = metric_type
        self._operator = operator
        self._criteria_parameters = criteria_parameters

    @property
    def column_name(self) -> str:
        return self._column_name

    @property
    def metric_type(self) -> MetricInputType:
        return self._metric_type

    @property
    def operator(self) -> OperatorInputType:
        return self._operator

    @property
    def criteria_parameters(self) -> Optional[SmartColumnMetricAssertionParameters]:
        return self._criteria_parameters

    @staticmethod
    def _get_column_name(assertion: Assertion) -> str:
        column_name = _get_nested_field_for_entity_with_default(
            assertion,
            field_path="info.fieldMetricAssertion.field.path",
            default=None,
        )
        if column_name is None:
            raise SDKUsageError(
                f"Column name is required for column metric assertions. Assertion {assertion.urn} does not have a column name"
            )
        return column_name

    @staticmethod
    def _get_metric_type(assertion: Assertion) -> MetricInputType:
        metric_type = _get_nested_field_for_entity_with_default(
            assertion,
            field_path="info.fieldMetricAssertion.metric",
            default=None,
        )
        if metric_type is None:
            raise SDKUsageError(
                f"Metric type is required for column metric assertions. Assertion {assertion.urn} does not have a metric type"
            )
        return metric_type

    @staticmethod
    def _get_operator(assertion: Assertion) -> OperatorInputType:
        operator = _get_nested_field_for_entity_with_default(
            assertion,
            field_path="info.fieldMetricAssertion.operator",
            default=None,
        )
        if operator is None:
            raise SDKUsageError(
                f"Operator is required for column metric assertions. Assertion {assertion.urn} does not have an operator"
            )
        return operator

    @staticmethod
    def _get_criteria_parameters(
        assertion: Assertion,
    ) -> Optional[SmartColumnMetricAssertionParameters]:
        # First check if there's a single value parameter
        value_param = _get_nested_field_for_entity_with_default(
            assertion,
            field_path="info.fieldMetricAssertion.parameters.value",
            default=None,
        )
        if value_param is not None:
            return value_param.value

        # Then check for range parameters
        min_value = _get_nested_field_for_entity_with_default(
            assertion,
            field_path="info.fieldMetricAssertion.parameters.minValue",
            default=None,
        )
        max_value = _get_nested_field_for_entity_with_default(
            assertion,
            field_path="info.fieldMetricAssertion.parameters.maxValue",
            default=None,
        )

        # If both range values exist, extract their values and return as tuple
        if min_value is not None and max_value is not None:
            if hasattr(min_value, "value"):
                min_value = min_value.value
            if hasattr(max_value, "value"):
                max_value = max_value.value
            return (min_value, max_value)

        # If no parameters found, return None
        return None

    @staticmethod
    def _get_criteria_parameters_with_type(
        assertion: Assertion,
    ) -> Optional[tuple]:
        """
        Get criteria parameters along with their type information from the backend.

        Returns:
            For single values: (value, type)
            For ranges: ((min_value, max_value), (min_type, max_type))
            None if no parameters found
        """
        # First check if there's a single value parameter
        value_param = _get_nested_field_for_entity_with_default(
            assertion,
            field_path="info.fieldMetricAssertion.parameters.value",
            default=None,
        )
        if value_param is not None:
            return (value_param.value, value_param.type)

        # Then check for range parameters
        min_param = _get_nested_field_for_entity_with_default(
            assertion,
            field_path="info.fieldMetricAssertion.parameters.minValue",
            default=None,
        )
        max_param = _get_nested_field_for_entity_with_default(
            assertion,
            field_path="info.fieldMetricAssertion.parameters.maxValue",
            default=None,
        )

        # If both range parameters exist, return values and types
        if min_param is not None and max_param is not None:
            return (
                (min_param.value, max_param.value),
                (min_param.type, max_param.type),
            )

        # If no parameters found, return None
        return None


class _AssertionPublic(ABC):
    """
    Abstract base class that represents a public facing assertion and contains the common properties of all assertions.
    """

    # TODO: have the individual classes self-declare this
    _SUPPORTED_WITH_FILTER_ASSERTION_TYPES = (
        models.FreshnessAssertionInfoClass,
        models.VolumeAssertionInfoClass,
        models.FieldAssertionInfoClass,
    )

    def __init__(
        self,
        *,
        urn: AssertionUrn,
        dataset_urn: DatasetUrn,
        display_name: str,
        mode: AssertionMode,
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
        self._incident_behavior = incident_behavior
        self._detection_mechanism = detection_mechanism
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
    def incident_behavior(self) -> list[AssertionIncidentBehavior]:
        return self._incident_behavior

    @property
    def detection_mechanism(self) -> Optional[_DetectionMechanismTypes]:
        return self._detection_mechanism

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
    @abstractmethod
    def _get_detection_mechanism(
        assertion: Assertion,
        monitor: Monitor,
        default: Optional[_DetectionMechanismTypes] = DEFAULT_DETECTION_MECHANISM,
    ) -> Optional[_DetectionMechanismTypes]:
        """Get the detection mechanism from the monitor and assertion.

        This method should be implemented by each assertion class to handle
        its specific detection mechanism logic.

        Args:
            assertion: The assertion entity
            monitor: The monitor entity
            default: Default detection mechanism to return if none is found

        Returns:
            The detection mechanism or default if none is found
        """
        pass

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

    @classmethod
    @abstractmethod
    def _from_entities(
        cls,
        assertion: Assertion,
        monitor: Monitor,
    ) -> Self:
        """
        Create an assertion from the assertion and monitor entities.

        Note: This is a private method since it is intended to be called internally by the client.
        """
        pass

    @staticmethod
    def _get_additional_filter(assertion: Assertion) -> Optional[str]:
        """Get the additional filter SQL from the assertion."""
        if assertion.info is None:
            logger.warning(
                f"Assertion {assertion.urn} does not have an info, defaulting additional filter to None"
            )
            return None
        if (
            not isinstance(
                assertion.info,
                _AssertionPublic._SUPPORTED_WITH_FILTER_ASSERTION_TYPES,
            )
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
        additional_filter = _AssertionPublic._get_additional_filter(assertion)

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
    def _warn_and_return_default_detection_mechanism(
        monitor: Monitor,
        field_name: str,
        default: Optional[_DetectionMechanismTypes] = DEFAULT_DETECTION_MECHANISM,
    ) -> Optional[_DetectionMechanismTypes]:
        """Helper method to log a warning and return default detection mechanism."""
        logger.warning(
            f"Monitor {monitor.urn} does not have an `{field_name}` field, defaulting detection mechanism to {default}"
        )
        return default

    @staticmethod
    def _check_valid_monitor_info(
        monitor: Monitor,
        default: Optional[_DetectionMechanismTypes] = DEFAULT_DETECTION_MECHANISM,
    ) -> Optional[models.AssertionEvaluationParametersClass]:
        """Check if monitor has valid info and get assertion parameters.

        Returns:
            The assertion parameters if monitor info is valid, None otherwise.
        """
        if not _AssertionPublic._has_valid_monitor_info(monitor):
            return None

        parameters = _AssertionPublic._get_assertion_parameters(monitor)
        if parameters is None:
            return None

        return parameters

    @staticmethod
    def _get_validated_detection_context(
        monitor: Monitor,
        assertion: Assertion,
        expected_parameters_type: str,
        expected_info_class: type,
        default: Optional[_DetectionMechanismTypes] = DEFAULT_DETECTION_MECHANISM,
    ) -> Optional[models.AssertionEvaluationParametersClass]:
        """
        Validate and extract the detection context (parameters) for detection mechanism logic.
        Returns the parameters if all checks pass, otherwise None.
        """
        parameters = _AssertionPublic._check_valid_monitor_info(monitor, default)
        if parameters is None:
            return None
        if parameters.type != expected_parameters_type:
            logger.warning(
                f"Expected {expected_parameters_type} parameters type, got {parameters.type}, defaulting detection mechanism to {default}"
            )
            return None
        if assertion.info is None:
            _AssertionPublic._warn_and_return_default_detection_mechanism(
                monitor, "info", default
            )
            return None
        if not isinstance(assertion.info, expected_info_class):
            logger.warning(
                f"Expected {expected_info_class.__name__}, got {type(assertion.info).__name__}, defaulting detection mechanism to {default}"
            )
            return None
        return parameters


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


class SmartVolumeAssertion(_HasSchedule, _HasSmartFunctionality, _AssertionPublic):
    """
    A class that represents a smart volume assertion.
    """

    def __init__(
        self,
        *,
        urn: AssertionUrn,
        dataset_urn: DatasetUrn,
        display_name: str,
        mode: AssertionMode,
        schedule: models.CronScheduleClass,
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
        Initialize a smart volume assertion.

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


class SqlAssertion(_AssertionPublic, _HasSchedule):
    """
    A class that represents a SQL assertion.
    """

    def __init__(
        self,
        *,
        urn: AssertionUrn,
        dataset_urn: DatasetUrn,
        display_name: str,
        mode: AssertionMode,
        statement: str,
        criteria: SqlAssertionCriteria,
        schedule: models.CronScheduleClass,
        tags: list[TagUrn],
        incident_behavior: list[AssertionIncidentBehavior],
        created_by: Optional[CorpUserUrn] = None,
        created_at: Union[datetime, None] = None,
        updated_by: Optional[CorpUserUrn] = None,
        updated_at: Optional[datetime] = None,
    ):
        """
        Initialize a SQL assertion.

        Note: Values can be accessed, but not set on the assertion object.
        To update an assertion, use the `upsert_*` method.
        Args:
            urn: The urn of the assertion.
            dataset_urn: The urn of the dataset that the assertion is for.
            display_name: The display name of the assertion.
            mode: The mode of the assertion (active, inactive).
            statement: The SQL statement to be used for the assertion.
            criteria: The criteria to be used for the assertion.
            schedule: The schedule of the assertion.
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
        # Then initialize the parent class
        self._statement = statement
        self._criteria = criteria

    @property
    def statement(self) -> str:
        return self._statement

    @property
    def criteria_condition(self) -> Union[SqlAssertionCondition, str]:
        return self._criteria.condition

    @property
    def criteria_parameters(
        self,
    ) -> Union[Union[float, int], tuple[Union[float, int], Union[float, int]]]:
        return self._criteria.parameters

    @staticmethod
    def _get_detection_mechanism(
        assertion: Assertion,
        monitor: Monitor,
        default: Optional[_DetectionMechanismTypes] = DEFAULT_DETECTION_MECHANISM,
    ) -> Optional[_DetectionMechanismTypes]:
        """Sql assertions do not have a detection mechanism."""
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

    @staticmethod
    def _get_condition_from_model_assertion_info(
        assertion_info: models.SqlAssertionInfoClass,
    ) -> SqlAssertionCondition:
        """Convert stored assertion info to condition enum."""
        # Handle value-based conditions (no change type)
        if str(assertion_info.type) == str(models.SqlAssertionTypeClass.METRIC):
            value_conditions = {
                str(
                    models.AssertionStdOperatorClass.EQUAL_TO
                ): SqlAssertionCondition.IS_EQUAL_TO,
                str(
                    models.AssertionStdOperatorClass.NOT_EQUAL_TO
                ): SqlAssertionCondition.IS_NOT_EQUAL_TO,
                str(
                    models.AssertionStdOperatorClass.GREATER_THAN
                ): SqlAssertionCondition.IS_GREATER_THAN,
                str(
                    models.AssertionStdOperatorClass.LESS_THAN
                ): SqlAssertionCondition.IS_LESS_THAN,
                str(
                    models.AssertionStdOperatorClass.BETWEEN
                ): SqlAssertionCondition.IS_WITHIN_A_RANGE,
            }
            if str(assertion_info.operator) in value_conditions:
                return value_conditions[str(assertion_info.operator)]

        # Handle growth-based conditions (with change type)
        elif str(assertion_info.type) == str(
            models.SqlAssertionTypeClass.METRIC_CHANGE
        ):
            assert assertion_info.changeType is not None, (
                "changeType must be present for METRIC_CHANGE assertions"
            )

            growth_conditions = {
                (
                    str(models.AssertionStdOperatorClass.LESS_THAN_OR_EQUAL_TO),
                    str(models.AssertionValueChangeTypeClass.ABSOLUTE),
                ): SqlAssertionCondition.GROWS_AT_MOST_ABSOLUTE,
                (
                    str(models.AssertionStdOperatorClass.LESS_THAN_OR_EQUAL_TO),
                    str(models.AssertionValueChangeTypeClass.PERCENTAGE),
                ): SqlAssertionCondition.GROWS_AT_MOST_PERCENTAGE,
                (
                    str(models.AssertionStdOperatorClass.GREATER_THAN_OR_EQUAL_TO),
                    str(models.AssertionValueChangeTypeClass.ABSOLUTE),
                ): SqlAssertionCondition.GROWS_AT_LEAST_ABSOLUTE,
                (
                    str(models.AssertionStdOperatorClass.GREATER_THAN_OR_EQUAL_TO),
                    str(models.AssertionValueChangeTypeClass.PERCENTAGE),
                ): SqlAssertionCondition.GROWS_AT_LEAST_PERCENTAGE,
                (
                    str(models.AssertionStdOperatorClass.BETWEEN),
                    str(models.AssertionValueChangeTypeClass.ABSOLUTE),
                ): SqlAssertionCondition.GROWS_WITHIN_A_RANGE_ABSOLUTE,
                (
                    str(models.AssertionStdOperatorClass.BETWEEN),
                    str(models.AssertionValueChangeTypeClass.PERCENTAGE),
                ): SqlAssertionCondition.GROWS_WITHIN_A_RANGE_PERCENTAGE,
            }

            key = (str(assertion_info.operator), str(assertion_info.changeType))
            if key in growth_conditions:
                return growth_conditions[key]

        raise ValueError(
            f"Unsupported combination: type={assertion_info.type}, operator={assertion_info.operator}, changeType={assertion_info.changeType}"
        )

    @staticmethod
    def _get_criteria(assertion: Assertion) -> SqlAssertionCriteria:
        if assertion.info is None:
            raise SDKNotYetSupportedError(
                f"Assertion {assertion.urn} does not have a SQL assertion info, which is not supported"
            )
        if isinstance(assertion.info, models.SqlAssertionInfoClass):
            parameters: Union[float, tuple[float, float]]
            if assertion.info.parameters.value is not None:
                parameters = float(assertion.info.parameters.value.value)
            elif (
                assertion.info.parameters.maxValue is not None
                and assertion.info.parameters.minValue is not None
            ):
                # min and max values are in the order of min, max
                parameters = (
                    float(assertion.info.parameters.minValue.value),
                    float(assertion.info.parameters.maxValue.value),
                )
            else:
                raise SDKNotYetSupportedError(
                    f"Assertion {assertion.urn} does not have a valid parameters for the SQL assertion"
                )

            condition = SqlAssertion._get_condition_from_model_assertion_info(
                assertion.info
            )

            return SqlAssertionCriteria(
                condition=condition,
                parameters=parameters,
            )
        else:
            raise SDKNotYetSupportedError(
                f"Assertion {assertion.urn} is not a SQL assertion"
            )

    @classmethod
    def _from_entities(cls, assertion: Assertion, monitor: Monitor) -> Self:
        """
        Create a SQL assertion from the assertion and monitor entities.
        """
        return cls(
            urn=assertion.urn,
            dataset_urn=assertion.dataset,
            display_name=assertion.description or "",
            mode=cls._get_mode(monitor),
            statement=cls._get_statement(assertion),
            criteria=cls._get_criteria(assertion),
            schedule=cls._get_schedule(
                monitor, default=DEFAULT_EVERY_SIX_HOURS_SCHEDULE
            ),
            tags=cls._get_tags(assertion),
            incident_behavior=cls._get_incident_behavior(assertion),
            created_by=cls._get_created_by(assertion),
            created_at=cls._get_created_at(assertion),
            updated_by=cls._get_updated_by(assertion),
            updated_at=cls._get_updated_at(assertion),
        )
