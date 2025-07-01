import datetime
import logging
from typing import Any, Dict, Optional

import cachetools
import pydantic
from pydantic.fields import Field

from datahub.configuration.common import ConfigModel
from datahub.utilities.cachetools_keys import self_methodkey

logger = logging.getLogger(__name__)


class OperationConfig(ConfigModel):
    lower_freq_profile_enabled: bool = Field(
        default=False,
        description="Whether to do profiling at lower freq or not. This does not do any scheduling just adds additional checks to when not to run profiling.",
    )
    profile_day_of_week: Optional[int] = Field(
        default=None,
        description="Number between 0 to 6 for day of week (both inclusive). 0 is Monday and 6 is Sunday. If not specified, defaults to Nothing and this field does not take affect.",
    )
    profile_date_of_month: Optional[int] = Field(
        default=None,
        description="Number between 1 to 31 for date of month (both inclusive). If not specified, defaults to Nothing and this field does not take affect.",
    )

    @pydantic.root_validator(pre=True)
    def lower_freq_configs_are_set(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        lower_freq_profile_enabled = values.get("lower_freq_profile_enabled")
        profile_day_of_week = values.get("profile_day_of_week")
        profile_date_of_month = values.get("profile_date_of_month")
        if (
            lower_freq_profile_enabled
            and profile_day_of_week is None
            and profile_date_of_month is None
        ):
            raise ValueError(
                "Lower freq profiling setting is enabled but no day of week or date of month is specified. Profiling will be done.",
            )
        return values

    @pydantic.validator("profile_day_of_week")
    def validate_profile_day_of_week(cls, v: Optional[int]) -> Optional[int]:
        profile_day_of_week = v
        if profile_day_of_week is None:
            return None
        if profile_day_of_week < 0 or profile_day_of_week > 6:
            raise ValueError(
                f"Invalid value {profile_day_of_week} for profile_day_of_week. Must be between 0 to 6 (both inclusive)."
            )
        return profile_day_of_week

    @pydantic.validator("profile_date_of_month")
    def validate_profile_date_of_month(cls, v: Optional[int]) -> Optional[int]:
        profile_date_of_month = v
        if profile_date_of_month is None:
            return None
        if profile_date_of_month < 1 or profile_date_of_month > 31:
            raise ValueError(
                f"Invalid value {profile_date_of_month} for profile_date_of_month. Must be between 1 to 31 (both inclusive)."
            )
        return profile_date_of_month


# TRICKY: The operation_config is time-dependent. Because we don't want to change
# whether or not we're running profiling mid-ingestion, we cache the result of this method.
# An additional benefit is that we only print the log lines on the first call.
@cachetools.cached(
    cache=cachetools.LRUCache(maxsize=1),
    key=self_methodkey,
)
def is_profiling_enabled(operation_config: OperationConfig) -> bool:
    if operation_config.lower_freq_profile_enabled is False:
        return True
    logger.info("Lower freq profiling setting is enabled.")
    today = datetime.date.today()
    if (
        operation_config.profile_day_of_week is not None
        and operation_config.profile_day_of_week != today.weekday()
    ):
        logger.info(
            "Profiling won't be done because weekday does not match config profile_day_of_week.",
        )
        return False
    if (
        operation_config.profile_date_of_month is not None
        and operation_config.profile_date_of_month != today.day
    ):
        logger.info(
            "Profiling won't be done because date of month does not match config profile_date_of_month.",
        )
        return False
    return True
