"""
This file contains the AssertionInput class and related classes, which are used to
validate and represent the input for creating an Assertion in DataHub.
"""

import random
import string
from abc import ABC
from datetime import datetime
from enum import Enum
from typing import Literal, TypeAlias, Union

import pydantic
from pydantic import BaseModel, Extra, ValidationError

from acryl_datahub_cloud._sdk_extras.errors import SDKUsageErrorWithExamples
from datahub.metadata.urns import AssertionUrn, DatasetUrn

# TODO: Import ASSERTION_MONITOR_DEFAULT_TRAINING_LOOKBACK_WINDOW_DAYS from datahub_executor.config
ASSERTION_MONITOR_DEFAULT_TRAINING_LOOKBACK_WINDOW_DAYS = 60

DEFAULT_NAME_PREFIX = "New Assertion"
DEFAULT_NAME_SUFFIX_LENGTH = 8


class AbstractDetectionMechanism(BaseModel, ABC):
    type: str

    class Config:
        extra = Extra.forbid


class _InformationSchema(AbstractDetectionMechanism):
    type: Literal["information_schema"] = "information_schema"


class _AuditLog(AbstractDetectionMechanism):
    type: Literal["audit_log"] = "audit_log"


class _LastModifiedColumn(AbstractDetectionMechanism):
    type: Literal["last_modified_column"] = "last_modified_column"
    column: str
    additional_filter: Union[str, None] = None


class _HighWaterMarkColumn(AbstractDetectionMechanism):
    type: Literal["high_water_mark_column"] = "high_water_mark_column"
    high_water_mark_column: str
    additional_filter: Union[str, None] = None


class _DataHubOperation(AbstractDetectionMechanism):
    type: Literal["datahub_operation"] = "datahub_operation"


class DetectionMechanism:
    INFORMATION_SCHEMA = _InformationSchema()
    AUDIT_LOG = _AuditLog()
    LAST_MODIFIED_COLUMN = _LastModifiedColumn
    HIGH_WATER_MARK_COLUMN = _HighWaterMarkColumn
    DATAHUB_OPERATION = _DataHubOperation()

    DETECTION_MECHANISM_TYPES = Union[
        _InformationSchema,
        _AuditLog,
        _LastModifiedColumn,
        _HighWaterMarkColumn,
        _DataHubOperation,
    ]

    DETECTION_MECHANISM_EXAMPLES = {
        "Information Schema from string": "information_schema",
        "Information Schema from DetectionMechanism": "DetectionMechanism.INFORMATION_SCHEMA",
        "Audit Log from string": "audit_log",
        "Audit Log from DetectionMechanism": "DetectionMechanism.AUDIT_LOG",
        "Last Modified Column from dict": {
            "type": "last_modified_column",
            "column": "last_modified",
            "additional_filter": "last_modified > '2021-01-01'",
        },
        "Last Modified Column from DetectionMechanism": "DetectionMechanism.LAST_MODIFIED_COLUMN(column='last_modified', additional_filter='last_modified > 2021-01-01')",
        "High Water Mark Column from dict": {
            "type": "high_water_mark_column",
            "high_water_mark_column": "id",
            "additional_filter": "id > 1000",
        },
        "High Water Mark Column from DetectionMechanism": "DetectionMechanism.HIGH_WATER_MARK_COLUMN(high_water_mark_column='id', additional_filter='id > 1000')",
        "DataHub Operation from string": "datahub_operation",
        "DataHub Operation from DetectionMechanism": "DetectionMechanism.DATAHUB_OPERATION",
    }

    @staticmethod
    def parse(
        detection_mechanism_config: Union[
            str, dict[str, str], DETECTION_MECHANISM_TYPES, None
        ],
    ) -> DETECTION_MECHANISM_TYPES:
        if detection_mechanism_config is None:
            return DEFAULT_DETECTION_MECHANISM
        if isinstance(detection_mechanism_config, str):
            return DetectionMechanism._try_parse_from_string(detection_mechanism_config)
        elif isinstance(detection_mechanism_config, dict):
            return DetectionMechanism._try_parse_from_dict(detection_mechanism_config)
        elif issubclass(type(detection_mechanism_config), AbstractDetectionMechanism):
            return detection_mechanism_config
        else:
            raise SDKUsageErrorWithExamples(
                msg=f"Invalid detection mechanism: {detection_mechanism_config}",
                examples=DetectionMechanism.DETECTION_MECHANISM_EXAMPLES,
            )

    @staticmethod
    def _try_parse_from_string(
        detection_mechanism_config: str,
    ) -> DETECTION_MECHANISM_TYPES:
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
                        examples=DetectionMechanism.DETECTION_MECHANISM_EXAMPLES,
                    ) from e
            return return_value
        except AttributeError as e:
            raise SDKUsageErrorWithExamples(
                msg=f"Invalid detection mechanism type: {detection_mechanism_config}",
                examples=DetectionMechanism.DETECTION_MECHANISM_EXAMPLES,
            ) from e

    @staticmethod
    def _try_parse_from_dict(
        detection_mechanism_config: dict[str, str],
    ) -> DETECTION_MECHANISM_TYPES:
        try:
            detection_mechanism_type = detection_mechanism_config.pop("type")
        except KeyError as e:
            raise SDKUsageErrorWithExamples(
                msg="Detection mechanism type is required if using a dict to create a DetectionMechanism",
                examples=DetectionMechanism.DETECTION_MECHANISM_EXAMPLES,
            ) from e
        try:
            detection_mechanism_obj = getattr(
                DetectionMechanism, detection_mechanism_type.upper()
            )
        except AttributeError as e:
            raise SDKUsageErrorWithExamples(
                msg=f"Invalid detection mechanism type: {detection_mechanism_type}",
                examples=DetectionMechanism.DETECTION_MECHANISM_EXAMPLES,
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
                    examples=DetectionMechanism.DETECTION_MECHANISM_EXAMPLES,
                ) from e
            return detection_mechanism_obj
        except ValidationError as e:
            raise SDKUsageErrorWithExamples(
                msg=f"Invalid detection mechanism type '{detection_mechanism_type}': {detection_mechanism_config} {e}",
                examples=DetectionMechanism.DETECTION_MECHANISM_EXAMPLES,
            ) from e


DEFAULT_DETECTION_MECHANISM = DetectionMechanism.INFORMATION_SCHEMA

DetectionMechanismInputTypes: TypeAlias = Union[
    str, dict[str, str], DetectionMechanism.DETECTION_MECHANISM_TYPES, None
]


class InferenceSensitivity(Enum):
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"

    @staticmethod
    def parse(
        sensitivity: Union[str, "InferenceSensitivity", None],
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
        }

        if isinstance(sensitivity, InferenceSensitivity):
            return sensitivity
        try:
            return InferenceSensitivity(sensitivity)
        except ValueError as e:
            raise SDKUsageErrorWithExamples(
                msg=f"Invalid inference sensitivity: {sensitivity}",
                examples=EXAMPLES,
            ) from e


DEFAULT_SENSITIVITY = InferenceSensitivity.MEDIUM


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
    FixedRangeExclusionWindowInputTypes,
    # Add other exclusion window types here as they are added to the SDK.
    None,
]


def _try_parse_exclusion_window(
    config: ExclusionWindowInputTypes,
) -> Union[FixedRangeExclusionWindow, list[FixedRangeExclusionWindow], None]:
    if config is None:
        return []
    if isinstance(config, dict):
        return [FixedRangeExclusionWindow(**config)]
    if isinstance(config, FixedRangeExclusionWindow):
        return [config]
    elif isinstance(config, list):
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
    else:
        raise SDKUsageErrorWithExamples(
            msg=f"Invalid exclusion window: {config}",
            examples=FIXED_RANGE_EXCLUSION_WINDOW_EXAMPLES,
        )


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
    training_data_lookback_days: Union[int, None],
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
    return training_data_lookback_days


class _AssertionInput:
    def __init__(
        self,
        *,
        # Required fields
        dataset_urn: Union[str, DatasetUrn],
        # Optional fields
        urn: Union[
            str, None, AssertionUrn
        ] = None,  # Can be None if the assertion is not yet created
        display_name: Union[str, None] = None,
        detection_mechanism: DetectionMechanismInputTypes = None,
        sensitivity: Union[str, InferenceSensitivity, None] = None,
        exclusion_windows: ExclusionWindowInputTypes = None,
        training_data_lookback_days: Union[int, None] = None,
        incident_behavior: Union[
            AssertionIncidentBehavior, list[AssertionIncidentBehavior], None
        ] = None,
    ):
        """
        Create an AssertionInput object.

        Args:
            urn: The urn of the assertion.
            dataset_urn: The urn of the dataset to be monitored.
            display_name: The display name of the assertion, which will be generated randomly if not provided.
            detection_mechanism: The detection mechanism to be used for the assertion.
            sensitivity: The sensitivity to be applied to the assertion.
            exclusion_windows: The exclusion windows to be applied to the assertion.
            training_data_lookback_days: The training data lookback days to be applied to the assertion.
            incident_behavior: The incident behavior to be applied to the assertion.
        """
        self.dataset_urn = DatasetUrn.from_string(dataset_urn)
        self.urn = AssertionUrn(urn) if urn else None
        self.name = display_name or _generate_default_name(
            DEFAULT_NAME_PREFIX, DEFAULT_NAME_SUFFIX_LENGTH
        )
        self.detection_mechanism = DetectionMechanism.parse(detection_mechanism)
        self.sensitivity = InferenceSensitivity.parse(sensitivity)
        self.exclusion_windows = _try_parse_exclusion_window(exclusion_windows)
        self.training_data_lookback_days = _try_parse_training_data_lookback_days(
            training_data_lookback_days
        )
        self.incident_behavior = _try_parse_incident_behavior(incident_behavior)

    # TODO: Implement the following methods:
    # def to_assertion_entity(self) -> Assertion:
    #     pass

    # def to_monitor_entity(self) -> Monitor:
    #     pass
