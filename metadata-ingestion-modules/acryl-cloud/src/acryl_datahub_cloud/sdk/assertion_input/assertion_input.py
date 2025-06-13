"""
This file contains the AssertionInput class and related classes, which are used to
validate and represent the input for creating an Assertion in DataHub.
"""

import random
import string
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Callable, Literal, Optional, Type, TypeAlias, TypeVar, Union

import pydantic
import pytz
import tzlocal
from avrogen.dict_wrapper import DictWrapper
from croniter import croniter
from pydantic import BaseModel, Extra, ValidationError

from acryl_datahub_cloud.sdk.entities.assertion import (
    Assertion,
    AssertionActionsInputType,
    AssertionInfoInputType,
    TagsInputType,
)
from acryl_datahub_cloud.sdk.entities.monitor import Monitor
from acryl_datahub_cloud.sdk.errors import (
    SDKNotYetSupportedError,
    SDKUsageError,
    SDKUsageErrorWithExamples,
)
from datahub.emitter.enum_helpers import get_enum_options
from datahub.emitter.mce_builder import make_ts_millis, parse_ts_millis
from datahub.metadata import schema_classes as models
from datahub.metadata.urns import AssertionUrn, CorpUserUrn, DatasetUrn
from datahub.sdk import Dataset
from datahub.sdk.entity_client import EntityClient

# TODO: Import ASSERTION_MONITOR_DEFAULT_TRAINING_LOOKBACK_WINDOW_DAYS from datahub_executor.config
ASSERTION_MONITOR_DEFAULT_TRAINING_LOOKBACK_WINDOW_DAYS = 60

DEFAULT_NAME_PREFIX = "New Assertion"
DEFAULT_NAME_SUFFIX_LENGTH = 8


DEFAULT_HOURLY_SCHEDULE: models.CronScheduleClass = models.CronScheduleClass(
    cron="0 * * * *",  # Every hour, matches the UI default
    timezone=str(
        tzlocal.get_localzone()
    ),  # User local timezone, matches the UI default
)
DEFAULT_SCHEDULE: models.CronScheduleClass = DEFAULT_HOURLY_SCHEDULE

DEFAULT_DAILY_SCHEDULE = models.CronScheduleClass(
    cron="0 0 * * *",  # Every day at midnight, matches the UI default
    timezone=str(
        tzlocal.get_localzone()
    ),  # User local timezone, matches the UI default
)

DEFAULT_EVERY_SIX_HOURS_SCHEDULE = models.CronScheduleClass(
    cron="0 */6 * * *",  # Every 6 hours, matches the UI default
    timezone=str(
        tzlocal.get_localzone()
    ),  # User local timezone, matches the UI default
)


class AbstractDetectionMechanism(BaseModel, ABC):
    type: str

    class Config:
        extra = Extra.forbid


class _InformationSchema(AbstractDetectionMechanism):
    type: Literal["information_schema"] = "information_schema"


class _AuditLog(AbstractDetectionMechanism):
    type: Literal["audit_log"] = "audit_log"


# Keep this in sync with the allowed field types in the UI, currently in
# datahub-web-react/src/app/entity/shared/tabs/Dataset/Validations/assertion/builder/constants.ts: LAST_MODIFIED_FIELD_TYPES
LAST_MODIFIED_ALLOWED_FIELD_TYPES = [models.DateTypeClass(), models.TimeTypeClass()]


class _LastModifiedColumn(AbstractDetectionMechanism):
    type: Literal["last_modified_column"] = "last_modified_column"
    column_name: str
    additional_filter: Optional[str] = None


# Keep this in sync with the allowed field types in the UI, currently in
# datahub-web-react/src/app/entity/shared/tabs/Dataset/Validations/assertion/builder/constants.ts: HIGH_WATERMARK_FIELD_TYPES
HIGH_WATERMARK_ALLOWED_FIELD_TYPES = [
    models.NumberTypeClass(),
    models.DateTypeClass(),
    models.TimeTypeClass(),
]


class _HighWatermarkColumn(AbstractDetectionMechanism):
    type: Literal["high_watermark_column"] = "high_watermark_column"
    column_name: str
    additional_filter: Optional[str] = None


class _DataHubOperation(AbstractDetectionMechanism):
    type: Literal["datahub_operation"] = "datahub_operation"


class _Query(AbstractDetectionMechanism):
    # COUNT(*) query
    type: Literal["query"] = "query"
    additional_filter: Optional[str] = None


class _AllRowsQuery(AbstractDetectionMechanism):
    # For column-based assertions, this is the default detection mechanism.
    type: Literal["all_rows_query"] = "all_rows_query"
    additional_filter: Optional[str] = None


class _AllRowsQueryDataHubDatasetProfile(AbstractDetectionMechanism):
    # Used for column-based assertions.
    type: Literal["all_rows_query_datahub_dataset_profile"] = (
        "all_rows_query_datahub_dataset_profile"
    )


class _ChangedRowsQuery(AbstractDetectionMechanism):
    # Used for column-based assertions.
    type: Literal["changed_rows_query"] = "changed_rows_query"
    column_name: str
    additional_filter: Optional[str] = None


class _DatasetProfile(AbstractDetectionMechanism):
    type: Literal["dataset_profile"] = "dataset_profile"


# Operators that require a single value numeric parameter
SINGLE_VALUE_NUMERIC_OPERATORS = [
    models.AssertionStdOperatorClass.EQUAL_TO,
    models.AssertionStdOperatorClass.NOT_EQUAL_TO,
    models.AssertionStdOperatorClass.GREATER_THAN,
    models.AssertionStdOperatorClass.LESS_THAN,
    models.AssertionStdOperatorClass.GREATER_THAN_OR_EQUAL_TO,
    models.AssertionStdOperatorClass.LESS_THAN_OR_EQUAL_TO,
]

# Operators that require a single value parameter
SINGLE_VALUE_OPERATORS = [
    models.AssertionStdOperatorClass.CONTAIN,
    models.AssertionStdOperatorClass.END_WITH,
    models.AssertionStdOperatorClass.START_WITH,
    models.AssertionStdOperatorClass.REGEX_MATCH,
    models.AssertionStdOperatorClass.IN,
    models.AssertionStdOperatorClass.NOT_IN,
] + SINGLE_VALUE_NUMERIC_OPERATORS

# Operators that require a numeric range parameter
RANGE_OPERATORS = [
    models.AssertionStdOperatorClass.BETWEEN,
]

# Operators that require no parameters
NO_PARAMETER_OPERATORS = [
    models.AssertionStdOperatorClass.NULL,
    models.AssertionStdOperatorClass.NOT_NULL,
    models.AssertionStdOperatorClass.IS_TRUE,
    models.AssertionStdOperatorClass.IS_FALSE,
]


# Keep these two lists in sync:
_DETECTION_MECHANISM_CONCRETE_TYPES = (
    _InformationSchema,
    _AuditLog,
    _LastModifiedColumn,
    _HighWatermarkColumn,
    _DataHubOperation,
    _Query,
    _DatasetProfile,
    _AllRowsQuery,
    _ChangedRowsQuery,
    _AllRowsQueryDataHubDatasetProfile,
)
_DetectionMechanismTypes = Union[
    _InformationSchema,
    _AuditLog,
    _LastModifiedColumn,
    _HighWatermarkColumn,
    _DataHubOperation,
    _Query,
    _DatasetProfile,
    _AllRowsQuery,
    _ChangedRowsQuery,
    _AllRowsQueryDataHubDatasetProfile,
]

_DETECTION_MECHANISM_TYPES_WITH_ADDITIONAL_FILTER = (
    _LastModifiedColumn,
    _HighWatermarkColumn,
    _Query,
    _AllRowsQuery,
    _ChangedRowsQuery,
)

DEFAULT_DETECTION_MECHANISM: _DetectionMechanismTypes = _InformationSchema()


class DetectionMechanism:
    # To have a more enum-like user experience even with sub parameters, we define the detection mechanisms as class attributes.
    # The options with sub parameters are the classes themselves so that parameters can be applied, and the rest are already instantiated instances of the classes.
    INFORMATION_SCHEMA = _InformationSchema()
    AUDIT_LOG = _AuditLog()
    LAST_MODIFIED_COLUMN = _LastModifiedColumn
    HIGH_WATERMARK_COLUMN = _HighWatermarkColumn
    DATAHUB_OPERATION = _DataHubOperation()
    QUERY = _Query
    ALL_ROWS_QUERY = _AllRowsQuery
    CHANGED_ROWS_QUERY = _ChangedRowsQuery
    ALL_ROWS_QUERY_DATAHUB_DATASET_PROFILE = _AllRowsQueryDataHubDatasetProfile()
    DATASET_PROFILE = _DatasetProfile()

    _DETECTION_MECHANISM_EXAMPLES = {
        "Information Schema from string": "information_schema",
        "Information Schema from DetectionMechanism": "DetectionMechanism.INFORMATION_SCHEMA",
        "Audit Log from string": "audit_log",
        "Audit Log from DetectionMechanism": "DetectionMechanism.AUDIT_LOG",
        "Last Modified Column from dict": {
            "type": "last_modified_column",
            "column_name": "last_modified",
            "additional_filter": "last_modified > '2021-01-01'",
        },
        "Last Modified Column from DetectionMechanism": "DetectionMechanism.LAST_MODIFIED_COLUMN(column_name='last_modified', additional_filter='last_modified > 2021-01-01')",
        "High Watermark Column from dict": {
            "type": "high_watermark_column",
            "column_name": "id",
            "additional_filter": "id > 1000",
        },
        "High Watermark Column from DetectionMechanism": "DetectionMechanism.HIGH_WATERMARK_COLUMN(column_name='id', additional_filter='id > 1000')",
        "DataHub Operation from string": "datahub_operation",
        "DataHub Operation from DetectionMechanism": "DetectionMechanism.DATAHUB_OPERATION",
        "Query from string": "query",
        "Query from dict": {
            "type": "query",
            "additional_filter": "id > 1000",
        },
        "Query from DetectionMechanism (with optional additional filter)": "DetectionMechanism.QUERY(additional_filter='id > 1000')",
        "Dataset Profile from string": "dataset_profile",
        "Dataset Profile from DetectionMechanism": "DetectionMechanism.DATASET_PROFILE",
        "All Rows Query from string": "all_rows_query",
        "All Rows Query from DetectionMechanism": "DetectionMechanism.ALL_ROWS_QUERY",
        "All Rows Query from DetectionMechanism (with optional additional filter)": "DetectionMechanism.ALL_ROWS_QUERY(additional_filter='id > 1000')",
        "Changed Rows Query from dict (with optional additional filter)": {
            "type": "changed_rows_query",
            "column_name": "id",
            "additional_filter": "id > 1000",
        },
        "Changed Rows Query from DetectionMechanism": "DetectionMechanism.CHANGED_ROWS_QUERY(column_name='id')",
        "Changed Rows Query from DetectionMechanism (with optional additional filter)": "DetectionMechanism.CHANGED_ROWS_QUERY(column_name='id', additional_filter='id > 1000')",
        "All Rows Query DataHub Dataset Profile from string": "all_rows_query_datahub_dataset_profile",
        "All Rows Query DataHub Dataset Profile from DetectionMechanism": "DetectionMechanism.ALL_ROWS_QUERY_DATAHUB_DATASET_PROFILE",
    }

    @staticmethod
    def parse(
        detection_mechanism_config: Optional[
            Union[str, dict[str, str], _DetectionMechanismTypes]
        ] = None,
        default_detection_mechanism: _DetectionMechanismTypes = DEFAULT_DETECTION_MECHANISM,
    ) -> _DetectionMechanismTypes:
        if detection_mechanism_config is None:
            return default_detection_mechanism
        if isinstance(detection_mechanism_config, _DETECTION_MECHANISM_CONCRETE_TYPES):
            return detection_mechanism_config
        elif isinstance(detection_mechanism_config, str):
            return DetectionMechanism._try_parse_from_string(detection_mechanism_config)
        elif isinstance(detection_mechanism_config, dict):
            return DetectionMechanism._try_parse_from_dict(detection_mechanism_config)
        else:
            raise SDKUsageErrorWithExamples(
                msg=f"Invalid detection mechanism: {detection_mechanism_config}",
                examples=DetectionMechanism._DETECTION_MECHANISM_EXAMPLES,
            )

    @staticmethod
    def _try_parse_from_string(
        detection_mechanism_config: str,
    ) -> _DetectionMechanismTypes:
        try:
            return_value = getattr(
                DetectionMechanism, detection_mechanism_config.upper()
            )
            if isinstance(return_value, pydantic.main.ModelMetaclass):
                try:
                    # We try to instantiate here to let pydantic raise a helpful error
                    # about which parameters are missing
                    return_value = return_value()
                except ValidationError as e:
                    raise SDKUsageErrorWithExamples(
                        msg=f"Detection mechanism type '{detection_mechanism_config}' requires additional parameters: {e}",
                        examples=DetectionMechanism._DETECTION_MECHANISM_EXAMPLES,
                    ) from e
            return return_value
        except AttributeError as e:
            raise SDKUsageErrorWithExamples(
                msg=f"Invalid detection mechanism type: {detection_mechanism_config}",
                examples=DetectionMechanism._DETECTION_MECHANISM_EXAMPLES,
            ) from e

    @staticmethod
    def _try_parse_from_dict(
        detection_mechanism_config: dict[str, str],
    ) -> _DetectionMechanismTypes:
        try:
            detection_mechanism_type = detection_mechanism_config.pop("type")
        except KeyError as e:
            raise SDKUsageErrorWithExamples(
                msg="Detection mechanism type is required if using a dict to create a DetectionMechanism",
                examples=DetectionMechanism._DETECTION_MECHANISM_EXAMPLES,
            ) from e
        try:
            detection_mechanism_obj = getattr(
                DetectionMechanism, detection_mechanism_type.upper()
            )
        except AttributeError as e:
            raise SDKUsageErrorWithExamples(
                msg=f"Invalid detection mechanism type: {detection_mechanism_type}",
                examples=DetectionMechanism._DETECTION_MECHANISM_EXAMPLES,
            ) from e

        try:
            return detection_mechanism_obj(**detection_mechanism_config)
        except TypeError as e:
            if "object is not callable" not in e.args[0]:
                raise e
            if detection_mechanism_config:
                # If we are here in the TypeError case, the detection mechanism is an instance of a class,
                # not a class itself, so we can't instantiate it with the config dict.
                # In this case, the config dict should be empty after the type is popped.
                # If it is not empty, we raise an error.
                raise SDKUsageErrorWithExamples(
                    msg=f"Invalid additional fields specified for detection mechanism '{detection_mechanism_type}': {detection_mechanism_config}",
                    examples=DetectionMechanism._DETECTION_MECHANISM_EXAMPLES,
                ) from e
            return detection_mechanism_obj
        except ValidationError as e:
            raise SDKUsageErrorWithExamples(
                msg=f"Invalid detection mechanism type '{detection_mechanism_type}': {detection_mechanism_config} {e}",
                examples=DetectionMechanism._DETECTION_MECHANISM_EXAMPLES,
            ) from e


DetectionMechanismInputTypes: TypeAlias = Union[
    str, dict[str, str], _DetectionMechanismTypes, None
]


class InferenceSensitivity(Enum):
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"

    @staticmethod
    def parse(
        sensitivity: Optional[
            Union[
                str,
                int,
                "InferenceSensitivity",
                models.AssertionMonitorSensitivityClass,
            ]
        ],
    ) -> "InferenceSensitivity":
        if sensitivity is None:
            return DEFAULT_SENSITIVITY
        EXAMPLES = {
            "High sensitivity from string": "high",
            "High sensitivity from enum": "InferenceSensitivity.HIGH",
            "Medium sensitivity from string": "medium",
            "Medium sensitivity from enum": "InferenceSensitivity.MEDIUM",
            "Low sensitivity from string": "low",
            "Low sensitivity from enum": "InferenceSensitivity.LOW",
            "Sensitivity from int (1-3: low, 4-6: medium, 7-10: high)": "10",
        }

        if isinstance(sensitivity, InferenceSensitivity):
            return sensitivity
        if isinstance(sensitivity, models.AssertionMonitorSensitivityClass):
            sensitivity = sensitivity.level
        if isinstance(sensitivity, int):
            if (sensitivity < 1) or (sensitivity > 10):
                raise SDKUsageErrorWithExamples(
                    msg=f"Invalid inference sensitivity: {sensitivity}",
                    examples=EXAMPLES,
                )
            elif sensitivity < 4:
                return InferenceSensitivity.LOW
            elif sensitivity < 7:
                return InferenceSensitivity.MEDIUM
            else:
                return InferenceSensitivity.HIGH
        try:
            return InferenceSensitivity(sensitivity)
        except ValueError as e:
            raise SDKUsageErrorWithExamples(
                msg=f"Invalid inference sensitivity: {sensitivity}",
                examples=EXAMPLES,
            ) from e

    @staticmethod
    def to_int(sensitivity: "InferenceSensitivity") -> int:
        return {
            InferenceSensitivity.HIGH: 10,
            InferenceSensitivity.MEDIUM: 5,
            InferenceSensitivity.LOW: 1,
        }[sensitivity]


DEFAULT_SENSITIVITY: InferenceSensitivity = InferenceSensitivity.MEDIUM

TIME_WINDOW_SIZE_EXAMPLES = {
    "Time window size from models.TimeWindowSizeClass": "models.TimeWindowSizeClass(unit='MINUTE', multiple=10)",
    "Time window size from object": "TimeWindowSize(unit='MINUTE', multiple=10)",
}


class CalendarInterval(Enum):
    MINUTE = "MINUTE"
    HOUR = "HOUR"
    DAY = "DAY"


class TimeWindowSize(BaseModel):
    unit: Union[CalendarInterval, str]
    multiple: int


TimeWindowSizeInputTypes: TypeAlias = Union[
    models.TimeWindowSizeClass,
    models.FixedIntervalScheduleClass,
    TimeWindowSize,
]


def _try_parse_time_window_size(
    config: TimeWindowSizeInputTypes,
) -> models.TimeWindowSizeClass:
    if isinstance(config, models.TimeWindowSizeClass):
        return config
    elif isinstance(config, models.FixedIntervalScheduleClass):
        return models.TimeWindowSizeClass(
            unit=_try_parse_and_validate_schema_classes_enum(
                config.unit, models.CalendarIntervalClass
            ),
            multiple=config.multiple,
        )
    elif isinstance(config, TimeWindowSize):
        return models.TimeWindowSizeClass(
            unit=_try_parse_and_validate_schema_classes_enum(
                _try_parse_and_validate_schema_classes_enum(
                    config.unit, CalendarInterval
                ).value,
                models.CalendarIntervalClass,
            ),
            multiple=config.multiple,
        )
    else:
        raise SDKUsageErrorWithExamples(
            msg=f"Invalid time window size: {config}",
            examples=TIME_WINDOW_SIZE_EXAMPLES,
        )


class FixedRangeExclusionWindow(BaseModel):
    type: Literal["fixed_range_exclusion_window"] = "fixed_range_exclusion_window"
    start: datetime
    end: datetime


ExclusionWindowTypes: TypeAlias = Union[
    FixedRangeExclusionWindow,
    # Add other exclusion window types here as they are added to the SDK.
]

FIXED_RANGE_EXCLUSION_WINDOW_EXAMPLES = {
    "Exclusion Window from datetimes": {
        "start": "datetime(2025, 1, 1, 0, 0, 0)",
        "end": "datetime(2025, 1, 2, 0, 0, 0)",
    },
    "Exclusion Window from strings": {
        "start": "2025-01-01T00:00:00",
        "end": "2025-01-02T00:00:00",
    },
    "Exclusion Window from object": "ExclusionWindow(start=datetime(2025, 1, 1, 0, 0, 0), end=datetime(2025, 1, 2, 0, 0, 0))",
}
FixedRangeExclusionWindowInputTypes: TypeAlias = Union[
    dict[str, datetime],
    dict[str, str],
    list[dict[str, datetime]],
    list[dict[str, str]],
    FixedRangeExclusionWindow,
    list[FixedRangeExclusionWindow],
]

ExclusionWindowInputTypes: TypeAlias = Union[
    models.AssertionExclusionWindowClass,
    list[models.AssertionExclusionWindowClass],
    FixedRangeExclusionWindowInputTypes,
    # Add other exclusion window types here as they are added to the SDK.
]

IterableExclusionWindowInputTypes: TypeAlias = Union[
    list[dict[str, datetime]],
    list[dict[str, str]],
    list[FixedRangeExclusionWindow],
    list[models.AssertionExclusionWindowClass],
]


def _try_parse_exclusion_window(
    config: Optional[ExclusionWindowInputTypes],
) -> Union[FixedRangeExclusionWindow, list[FixedRangeExclusionWindow], None]:
    if config is None:
        return []
    if isinstance(config, dict):
        return [FixedRangeExclusionWindow(**config)]
    if isinstance(config, FixedRangeExclusionWindow):
        return [config]
    elif isinstance(config, models.AssertionExclusionWindowClass):
        assert config.fixedRange is not None
        return [
            FixedRangeExclusionWindow(
                start=parse_ts_millis(config.fixedRange.startTimeMillis),
                end=parse_ts_millis(config.fixedRange.endTimeMillis),
            )
        ]
    elif isinstance(config, list):
        return _try_parse_list_of_exclusion_windows(config)
    else:
        raise SDKUsageErrorWithExamples(
            msg=f"Invalid exclusion window: {config}",
            examples=FIXED_RANGE_EXCLUSION_WINDOW_EXAMPLES,
        )


def _try_parse_list_of_exclusion_windows(
    config: IterableExclusionWindowInputTypes,
) -> Union[list[FixedRangeExclusionWindow], None]:
    if all(isinstance(item, models.AssertionExclusionWindowClass) for item in config):
        exclusion_windows = []
        for item in config:
            assert isinstance(item, models.AssertionExclusionWindowClass)
            assert item.fixedRange is not None
            exclusion_windows.append(
                FixedRangeExclusionWindow(
                    start=parse_ts_millis(item.fixedRange.startTimeMillis),
                    end=parse_ts_millis(item.fixedRange.endTimeMillis),
                )
            )
        return exclusion_windows
    else:
        exclusion_windows = []
        for item in config:
            if isinstance(item, dict):
                try:
                    exclusion_windows.append(FixedRangeExclusionWindow(**item))
                except ValidationError as e:
                    raise SDKUsageErrorWithExamples(
                        msg=f"Invalid exclusion window: {item}",
                        examples=FIXED_RANGE_EXCLUSION_WINDOW_EXAMPLES,
                    ) from e
            elif isinstance(item, FixedRangeExclusionWindow):
                exclusion_windows.append(item)
            elif item is None:
                pass
            else:
                raise SDKUsageErrorWithExamples(
                    msg=f"Invalid exclusion window: {item}",
                    examples=FIXED_RANGE_EXCLUSION_WINDOW_EXAMPLES,
                )
    return exclusion_windows


class AssertionIncidentBehavior(Enum):
    RAISE_ON_FAIL = "raise_on_fail"
    RESOLVE_ON_PASS = "resolve_on_pass"


ASSERTION_INCIDENT_BEHAVIOR_EXAMPLES = {
    "Raise on fail from string": "raise_on_fail",
    "Raise on fail from enum": "AssertionIncidentBehavior.RAISE_ON_FAIL",
    "Resolve on pass from string": "resolve_on_pass",
    "Resolve on pass from enum": "AssertionIncidentBehavior.RESOLVE_ON_PASS",
}

AssertionIncidentBehaviorInputTypes: TypeAlias = Union[
    str,
    list[str],
    AssertionIncidentBehavior,
    list[AssertionIncidentBehavior],
    None,
]


def _try_parse_incident_behavior(
    config: AssertionIncidentBehaviorInputTypes,
) -> Union[AssertionIncidentBehavior, list[AssertionIncidentBehavior], None]:
    if config is None:
        return []
    if isinstance(config, str):
        try:
            return [AssertionIncidentBehavior(config)]
        except ValueError as e:
            raise SDKUsageErrorWithExamples(
                msg=f"Invalid incident behavior: {config}",
                examples=ASSERTION_INCIDENT_BEHAVIOR_EXAMPLES,
            ) from e
    if isinstance(config, AssertionIncidentBehavior):
        return [config]
    elif isinstance(config, list):
        incident_behaviors = []
        for item in config:
            if isinstance(item, str):
                try:
                    incident_behaviors.append(AssertionIncidentBehavior(item))
                except ValueError as e:
                    raise SDKUsageErrorWithExamples(
                        msg=f"Invalid incident behavior: {item}",
                        examples=ASSERTION_INCIDENT_BEHAVIOR_EXAMPLES,
                    ) from e
            elif isinstance(item, AssertionIncidentBehavior):
                incident_behaviors.append(item)
            else:
                raise SDKUsageErrorWithExamples(
                    msg=f"Invalid incident behavior: {item}",
                    examples=ASSERTION_INCIDENT_BEHAVIOR_EXAMPLES,
                )
        return incident_behaviors
    else:
        raise SDKUsageErrorWithExamples(
            msg=f"Invalid incident behavior: {config}",
            examples=ASSERTION_INCIDENT_BEHAVIOR_EXAMPLES,
        )


def _generate_default_name(prefix: str, suffix_length: int) -> str:
    return f"{prefix}-{''.join(random.choices(string.ascii_letters + string.digits, k=suffix_length))}"


TRAINING_DATA_LOOKBACK_DAYS_EXAMPLES = {
    "Training data lookback days from int": ASSERTION_MONITOR_DEFAULT_TRAINING_LOOKBACK_WINDOW_DAYS,
    f"Training data lookback days from None (uses default of {ASSERTION_MONITOR_DEFAULT_TRAINING_LOOKBACK_WINDOW_DAYS} days)": None,
}


def _try_parse_training_data_lookback_days(
    training_data_lookback_days: Optional[int],
) -> int:
    if training_data_lookback_days is None:
        return ASSERTION_MONITOR_DEFAULT_TRAINING_LOOKBACK_WINDOW_DAYS
    if isinstance(training_data_lookback_days, str):
        try:
            training_data_lookback_days = int(training_data_lookback_days)
        except ValueError as e:
            raise SDKUsageErrorWithExamples(
                msg=f"Invalid training data lookback days: {training_data_lookback_days}",
                examples=TRAINING_DATA_LOOKBACK_DAYS_EXAMPLES,
            ) from e
    if not isinstance(training_data_lookback_days, int):
        raise SDKUsageErrorWithExamples(
            msg=f"Invalid training data lookback days: {training_data_lookback_days}",
            examples=TRAINING_DATA_LOOKBACK_DAYS_EXAMPLES,
        )
    if training_data_lookback_days < 0:
        raise SDKUsageError("Training data lookback days must be non-negative")
    return training_data_lookback_days


def _validate_cron_schedule(schedule: str, timezone: str) -> None:
    """We are using the POSIX.1-2017 standard for cron expressions.

    Note: We are using the croniter library for cron parsing which is different from executor, which uses apscheduler, so there is a risk of mismatch here.
    """
    try:
        # Validate timezone - pytz.timezone() raises UnknownTimeZoneError for invalid timezones
        # Skip timezone validation when empty
        if timezone:
            pytz.timezone(timezone)

        # Validate 5-field cron expression only (POSIX.1-2017 standard)
        fields = schedule.strip().split()
        if len(fields) != 5:
            raise ValueError("POSIX.1-2017 requires exactly 5 fields")

        # POSIX.1-2017 specific validation: Sunday must be 0, not 7
        # However croniter accepts 7 as Sunday, so custom check is needed here.
        # Check the day-of-week field (5th field, index 4)
        dow_field = fields[4]
        if "7" in dow_field:
            # Check if 7 appears as a standalone value or in ranges
            import re

            # Match 7 as standalone, in lists, or in ranges
            if re.search(r"\b7\b|7-|,7,|^7,|,7$|-7\b", dow_field):
                raise ValueError(
                    "POSIX.1-2017 standard: Sunday must be represented as 0, not 7"
                )

        # Validate cron expression - croniter constructor validates the expression
        croniter(schedule)

    except Exception as e:
        raise SDKUsageError(
            f"Invalid cron expression or timezone: {schedule} {timezone}, please use a POSIX.1-2017 compatible cron expression and timezone."
        ) from e


def _try_parse_schedule(
    schedule: Optional[Union[str, models.CronScheduleClass]],
) -> Optional[models.CronScheduleClass]:
    if schedule is None:
        return None
    if isinstance(schedule, str):
        _validate_cron_schedule(schedule, "UTC")
        return models.CronScheduleClass(
            cron=schedule,
            timezone="UTC",
        )
    if isinstance(schedule, models.CronScheduleClass):
        _validate_cron_schedule(schedule.cron, schedule.timezone)
        return schedule


FieldSpecType = Union[models.FreshnessFieldSpecClass, models.SchemaFieldSpecClass]


T = TypeVar("T")


def _try_parse_and_validate_schema_classes_enum(
    value: Union[str, T],
    enum_class: Type[T],
) -> T:
    if isinstance(value, enum_class):
        return value
    assert isinstance(value, str)
    if value.upper() not in get_enum_options(enum_class):
        raise SDKUsageError(
            f"Invalid value for {enum_class.__name__}: {value}, valid options are {get_enum_options(enum_class)}"
        )
    return getattr(enum_class, value.upper())


@dataclass(frozen=True)
class DatasetSourceType:
    """
    DatasetSourceType is used to represent a dataset source type.
    It is used to check if a source type is valid for a dataset type and assertion type.

    Args:
        source_type: The source type (e.g. information schema, field value, etc. aka detection mechanism)
        platform: The platform of the dataset as a string OR "all" for all platforms.
        assertion_type: The assertion type as a models.AssertionTypeClass string e.g. models.AssertionTypeClass.FRESHNESS OR "all" for all assertion types.

    Example:
    DatasetSourceType(
        source_type=_InformationSchema,
        platform="databricks",
        assertion_type="all",
    )
    This means that the source type _InformationSchema is invalid for the dataset type "databricks" and assertion type "all".
    "all" in this example means that the source type is invalid for all assertion types.
    """

    source_type: Type[_DetectionMechanismTypes]
    platform: str
    assertion_type: Union[models.AssertionTypeClass, str]


INVALID_SOURCE_TYPES = {
    # Add exceptions here if a source type (detection mechanism) is invalid for a dataset type and assertion type.
    DatasetSourceType(
        source_type=_InformationSchema,
        platform="databricks",
        assertion_type="all",
    )
}


def _is_source_type_valid(
    dataset_source_type: DatasetSourceType,
    invalid_source_types: set[DatasetSourceType] = INVALID_SOURCE_TYPES,
) -> bool:
    for invalid in invalid_source_types:
        if invalid.source_type == dataset_source_type.source_type:
            # If both platform and assertion type are "all", the source type is invalid for all combinations
            if invalid.platform == "all" and invalid.assertion_type == "all":
                return False
            # If platform matches and assertion type is "all", the source type is invalid for all assertion types on that platform
            if (
                invalid.platform == dataset_source_type.platform
                and invalid.assertion_type == "all"
            ):
                return False
            # If platform is "all" and assertion type matches, the source type is invalid for all platforms for that assertion type
            if (
                invalid.platform == "all"
                and invalid.assertion_type == dataset_source_type.assertion_type
            ):
                return False
            # If both platform and assertion type match exactly, the source type is invalid
            if (
                invalid.platform == dataset_source_type.platform
                and invalid.assertion_type == dataset_source_type.assertion_type
            ):
                return False
    return True


class _HasSmartAssertionInputs:
    """
    A class that contains the common inputs for smart assertions.
    This is used to avoid code duplication in the smart assertion inputs.

    Args:
        sensitivity: The sensitivity to be applied to the assertion.
        exclusion_windows: The exclusion windows to be applied to the assertion. If not provided, no exclusion windows will be applied.
        training_data_lookback_days: The training data lookback days to be applied to the assertion.
    """

    def __init__(
        self,
        *,
        sensitivity: Optional[Union[str, InferenceSensitivity]] = None,
        exclusion_windows: Optional[ExclusionWindowInputTypes] = None,
        training_data_lookback_days: Optional[int] = None,
    ):
        self.sensitivity = InferenceSensitivity.parse(sensitivity)
        self.exclusion_windows = _try_parse_exclusion_window(exclusion_windows)
        self.training_data_lookback_days = _try_parse_training_data_lookback_days(
            training_data_lookback_days
        )

    def _convert_exclusion_windows(
        self,
    ) -> list[models.AssertionExclusionWindowClass]:
        """
        Convert exclusion windows into AssertionExclusionWindowClass objects including generating display names for them.

        Returns:
            A list of AssertionExclusionWindowClass objects.

        Raises:
            SDKUsageErrorWithExamples: If an exclusion window is of an invalid type.
        """
        exclusion_windows: list[models.AssertionExclusionWindowClass] = []
        if self.exclusion_windows:
            for window in self.exclusion_windows:
                if not isinstance(window, FixedRangeExclusionWindow):
                    raise SDKUsageErrorWithExamples(
                        msg=f"Invalid exclusion window type: {window}",
                        examples=FIXED_RANGE_EXCLUSION_WINDOW_EXAMPLES,
                    )
                # To match the UI, we generate a display name for the exclusion window.
                # See here for the UI code: https://github.com/acryldata/datahub-fork/blob/acryl-main/datahub-web-react/src/app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/steps/inferred/common/ExclusionWindowAdjuster.tsx#L31
                # Copied here for reference: displayName: `${dayjs(startTime).format('MMM D, h:mm A')} - ${dayjs(endTime).format('MMM D, h:mm A')}`,
                generated_display_name = f"{window.start.strftime('%b %-d, %-I:%M %p')} - {window.end.strftime('%b %-d, %-I:%M %p')}"
                exclusion_windows.append(
                    models.AssertionExclusionWindowClass(
                        type=models.AssertionExclusionWindowTypeClass.FIXED_RANGE,  # Currently only fixed range is supported
                        displayName=generated_display_name,
                        fixedRange=models.AbsoluteTimeWindowClass(
                            startTimeMillis=make_ts_millis(window.start),
                            endTimeMillis=make_ts_millis(window.end),
                        ),
                    )
                )
        return exclusion_windows

    def _convert_sensitivity(self) -> models.AssertionMonitorSensitivityClass:
        """
        Convert sensitivity into an AssertionMonitorSensitivityClass.

        Returns:
            An AssertionMonitorSensitivityClass with the appropriate sensitivity.
        """
        return models.AssertionMonitorSensitivityClass(
            level=InferenceSensitivity.to_int(self.sensitivity),
        )


class _AssertionInput(ABC):
    def __init__(
        self,
        *,
        # Required fields
        dataset_urn: Union[str, DatasetUrn],
        entity_client: EntityClient,  # Needed to get the schema field spec for the detection mechanism if needed
        # Optional fields
        urn: Optional[
            Union[str, AssertionUrn]
        ] = None,  # Can be None if the assertion is not yet created
        display_name: Optional[str] = None,
        enabled: bool = True,
        schedule: Optional[Union[str, models.CronScheduleClass]] = None,
        detection_mechanism: DetectionMechanismInputTypes = None,
        incident_behavior: Optional[
            Union[AssertionIncidentBehavior, list[AssertionIncidentBehavior]]
        ] = None,
        tags: Optional[TagsInputType] = None,
        source_type: str = models.AssertionSourceTypeClass.NATIVE,  # Verified on init to be a valid enum value
        created_by: Union[str, CorpUserUrn],
        created_at: datetime,
        updated_by: Union[str, CorpUserUrn],
        updated_at: datetime,
        default_detection_mechanism: _DetectionMechanismTypes = DEFAULT_DETECTION_MECHANISM,
    ):
        """
        Create an AssertionInput object.

        Args:
            dataset_urn: The urn of the dataset to be monitored.
            entity_client: The entity client to be used for creating the assertion.
            urn: The urn of the assertion. If not provided, a random urn will be generated.
            display_name: The display name of the assertion. If not provided, a random display name will be generated.
            enabled: Whether the assertion is enabled. Defaults to True.
            detection_mechanism: The detection mechanism to be used for the assertion.
            incident_behavior: The incident behavior to be applied to the assertion.
            tags: The tags to be applied to the assertion.
            source_type: The source type of the assertion. Defaults to models.AssertionSourceTypeClass.NATIVE.
            created_by: The actor that created the assertion.
            created_at: The timestamp of the assertion creation.
            updated_by: The actor that last updated the assertion.
            updated_at: The timestamp of the assertion last update.
        """
        self.dataset_urn = DatasetUrn.from_string(dataset_urn)
        self.entity_client = entity_client
        self.urn = AssertionUrn(urn) if urn else None
        self.display_name = (
            display_name
            if display_name is not None
            else _generate_default_name(DEFAULT_NAME_PREFIX, DEFAULT_NAME_SUFFIX_LENGTH)
        )
        self.enabled = enabled
        self.schedule = _try_parse_schedule(schedule)
        self.detection_mechanism = DetectionMechanism.parse(
            detection_mechanism, default_detection_mechanism
        )
        if not _is_source_type_valid(
            DatasetSourceType(
                source_type=type(self.detection_mechanism),
                platform=self.dataset_urn.platform,
                assertion_type=self._assertion_type(),
            )
        ):
            raise SDKUsageError(
                f"Invalid source type: {self.detection_mechanism} for dataset type: {self.dataset_urn.platform} and assertion type: {self._assertion_type()}"
            )
        self.incident_behavior = _try_parse_incident_behavior(incident_behavior)
        self.tags = tags
        if source_type not in get_enum_options(models.AssertionSourceTypeClass):
            raise SDKUsageError(
                msg=f"Invalid source type: {source_type}, valid options are {get_enum_options(models.AssertionSourceTypeClass)}",
            )
        self.source_type = source_type
        self.created_by = created_by
        self.created_at = created_at
        self.updated_by = updated_by
        self.updated_at = updated_at
        self.cached_dataset: Optional[Dataset] = None

    def to_assertion_and_monitor_entities(self) -> tuple[Assertion, Monitor]:
        """
        Convert the assertion input to an assertion and monitor entity.

        Returns:
            A tuple of (assertion, monitor) entities.
        """
        assertion = self.to_assertion_entity()
        monitor = self.to_monitor_entity(assertion.urn)
        return assertion, monitor

    def to_assertion_entity(self) -> Assertion:
        """
        Convert the assertion input to an assertion entity.

        Returns:
            The created assertion entity.
        """
        on_success, on_failure = self._convert_incident_behavior()
        filter = self._create_filter_from_detection_mechanism()

        return Assertion(
            id=self.urn,
            info=self._create_assertion_info(filter),
            description=self.display_name,
            on_success=on_success,
            on_failure=on_failure,
            tags=self._convert_tags(),
            source=self._convert_source(),
            last_updated=self._convert_last_updated(),
        )

    def _convert_incident_behavior(
        self,
    ) -> tuple[
        Optional[AssertionActionsInputType],
        Optional[AssertionActionsInputType],
    ]:
        """
        Convert incident behavior to on_success and on_failure actions.

        Returns:
            A tuple of (on_success, on_failure) actions.
        """
        if not self.incident_behavior:
            return None, None

        behaviors = (
            [self.incident_behavior]
            if isinstance(self.incident_behavior, AssertionIncidentBehavior)
            else self.incident_behavior
        )

        on_success: Optional[AssertionActionsInputType] = [
            models.AssertionActionClass(
                type=models.AssertionActionTypeClass.RESOLVE_INCIDENT
            )
            for behavior in behaviors
            if behavior == AssertionIncidentBehavior.RESOLVE_ON_PASS
        ] or None

        on_failure: Optional[AssertionActionsInputType] = [
            models.AssertionActionClass(
                type=models.AssertionActionTypeClass.RAISE_INCIDENT
            )
            for behavior in behaviors
            if behavior == AssertionIncidentBehavior.RAISE_ON_FAIL
        ] or None

        return on_success, on_failure

    def _create_filter_from_detection_mechanism(
        self,
    ) -> Optional[models.DatasetFilterClass]:
        """
        Create a filter from the detection mechanism if it has an additional filter.

        Returns:
            A DatasetFilterClass if the detection mechanism has an additional filter, None otherwise.
        """
        if not isinstance(
            self.detection_mechanism,
            _DETECTION_MECHANISM_TYPES_WITH_ADDITIONAL_FILTER,
        ):
            return None

        additional_filter = self.detection_mechanism.additional_filter
        if not additional_filter:
            return None

        return models.DatasetFilterClass(
            type=models.DatasetFilterTypeClass.SQL,
            sql=additional_filter,
        )

    def _convert_tags(self) -> Optional[TagsInputType]:
        """
        Convert the tags input into a standardized format.

        Returns:
            A list of tags or None if no tags are provided.

        Raises:
            SDKUsageErrorWithExamples: If the tags input is invalid.
        """
        if not self.tags:
            return None

        if isinstance(self.tags, str):
            return [self.tags]
        elif isinstance(self.tags, list):
            return self.tags
        else:
            raise SDKUsageErrorWithExamples(
                msg=f"Invalid tags: {self.tags}",
                examples={
                    "Tags from string": "urn:li:tag:my_tag_1",
                    "Tags from list": [
                        "urn:li:tag:my_tag_1",
                        "urn:li:tag:my_tag_2",
                    ],
                },
            )

    def _convert_source(self) -> models.AssertionSourceClass:
        """
        Convert the source input into a models.AssertionSourceClass.
        """
        return models.AssertionSourceClass(
            type=self.source_type,
            created=models.AuditStampClass(
                time=make_ts_millis(self.created_at),
                actor=str(self.created_by),
            ),
        )

    def _convert_last_updated(self) -> tuple[datetime, str]:
        """
        Convert the last updated input into a tuple of (datetime, str).

        Validation is handled in the Assertion entity constructor.
        """
        return (self.updated_at, str(self.updated_by))

    def to_monitor_entity(self, assertion_urn: AssertionUrn) -> Monitor:
        """
        Convert the assertion input to a monitor entity.

        Args:
            assertion_urn: The URN of the assertion to monitor.

        Returns:
            A Monitor entity configured with the assertion input parameters.
        """
        return Monitor(
            id=(self.dataset_urn, assertion_urn),
            info=self._create_monitor_info(
                assertion_urn=assertion_urn,
                status=self._convert_monitor_status(),
                schedule=self._convert_schedule(),
            ),
        )

    def _convert_monitor_status(self) -> models.MonitorStatusClass:
        """
        Convert the enabled flag into a MonitorStatusClass.

        Returns:
            A MonitorStatusClass with ACTIVE or INACTIVE mode based on the enabled flag.
        """
        return models.MonitorStatusClass(
            mode=models.MonitorModeClass.ACTIVE
            if self.enabled
            else models.MonitorModeClass.INACTIVE,
        )

    def _get_schema_field_spec(self, column_name: str) -> models.SchemaFieldSpecClass:
        """
        Get the schema field spec for the detection mechanism if needed.
        """
        # Only fetch the dataset if it's not already cached.
        # Also we only fetch the dataset if it's needed for the detection mechanism.
        if self.cached_dataset is None:
            self.cached_dataset = self.entity_client.get(self.dataset_urn)

        # Handle case where dataset doesn't exist
        if self.cached_dataset is None:
            raise SDKUsageError(
                f"Dataset {self.dataset_urn} not found. Cannot validate column {column_name}."
            )

        # TODO: Make a public accessor for _schema_dict in the SDK
        schema_fields = self.cached_dataset._schema_dict()
        field = schema_fields.get(column_name)
        if field:
            return models.SchemaFieldSpecClass(
                path=field.fieldPath,
                type=field.type.type.__class__.__name__,
                nativeType=field.nativeDataType,
            )
        else:
            raise SDKUsageError(
                msg=f"Column {column_name} not found in dataset {self.dataset_urn}",
            )

    def _validate_field_type(
        self,
        field_spec: models.SchemaFieldSpecClass,
        column_name: str,
        allowed_types: list[DictWrapper],
        field_type_name: str,
    ) -> None:
        """
        Validate that a field has an allowed type.

        Args:
            field_spec: The field specification to validate
            column_name: The name of the column for error messages
            allowed_types: List of allowed field types
            field_type_name: Human-readable name of the field type for error messages

        Raises:
            SDKUsageError: If the field has an invalid type
        """
        allowed_type_names = [t.__class__.__name__ for t in allowed_types]
        if field_spec.type not in allowed_type_names:
            raise SDKUsageError(
                msg=f"Column {column_name} with type {field_spec.type} does not have an allowed type for a {field_type_name} in dataset {self.dataset_urn}. "
                f"Allowed types are {allowed_type_names}.",
            )

    @abstractmethod
    def _create_monitor_info(
        self,
        assertion_urn: AssertionUrn,
        status: models.MonitorStatusClass,
        schedule: models.CronScheduleClass,
    ) -> models.MonitorInfoClass:
        """
        Create a MonitorInfoClass with all the necessary components.

        Args:
            status: The monitor status.
            schedule: The monitor schedule.
        Returns:
            A MonitorInfoClass configured with all the provided components.
        """
        pass

    @abstractmethod
    def _assertion_type(self) -> str:
        """Get the assertion type."""
        pass

    @abstractmethod
    def _create_assertion_info(
        self, filter: Optional[models.DatasetFilterClass]
    ) -> AssertionInfoInputType:
        """Create assertion info specific to the assertion type."""
        pass

    @abstractmethod
    def _convert_schedule(self) -> models.CronScheduleClass:
        """Convert schedule to appropriate format for the assertion type."""
        pass

    @abstractmethod
    def _get_assertion_evaluation_parameters(
        self, source_type: str, field: Optional[FieldSpecType]
    ) -> models.AssertionEvaluationParametersClass:
        """Get evaluation parameters specific to the assertion type."""
        pass

    @abstractmethod
    def _convert_assertion_source_type_and_field(
        self,
    ) -> tuple[str, Optional[FieldSpecType]]:
        """Convert detection mechanism to source type and field spec."""
        pass


class _HasFreshnessFeatures:
    def _create_field_spec(
        self,
        column_name: str,
        allowed_types: list[DictWrapper],  # TODO: Use the type from the PDL
        field_type_name: str,
        kind: str,
        get_schema_field_spec: Callable[[str], models.SchemaFieldSpecClass],
        validate_field_type: Callable[
            [models.SchemaFieldSpecClass, str, list[DictWrapper], str], None
        ],
    ) -> models.FreshnessFieldSpecClass:
        """
        Create a field specification for a column, validating its type.

        Args:
            column_name: The name of the column to create a spec for
            allowed_types: List of allowed field types
            field_type_name: Human-readable name of the field type for error messages
            kind: The kind of field to create

        Returns:
            A FreshnessFieldSpecClass for the column

        Raises:
            SDKUsageError: If the column is not found or has an invalid type
        """
        SUPPORTED_KINDS = [
            models.FreshnessFieldKindClass.LAST_MODIFIED,
            models.FreshnessFieldKindClass.HIGH_WATERMARK,
        ]
        if kind not in SUPPORTED_KINDS:
            raise SDKUsageError(
                msg=f"Invalid kind: {kind}. Must be one of {SUPPORTED_KINDS}",
            )

        field_spec = get_schema_field_spec(column_name)
        validate_field_type(field_spec, column_name, allowed_types, field_type_name)
        return models.FreshnessFieldSpecClass(
            path=field_spec.path,
            type=field_spec.type,
            nativeType=field_spec.nativeType,
            kind=kind,
        )


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
        incident_behavior: Optional[
            Union[AssertionIncidentBehavior, list[AssertionIncidentBehavior]]
        ] = None,
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
        incident_behavior: Optional[
            Union[AssertionIncidentBehavior, list[AssertionIncidentBehavior]]
        ] = None,
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
