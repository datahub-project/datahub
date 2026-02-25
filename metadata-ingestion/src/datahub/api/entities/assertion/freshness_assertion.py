from datetime import timedelta
from enum import Enum
from typing import Optional, Union

import humanfriendly
from pydantic import Field, field_validator
from typing_extensions import Literal

from datahub.api.entities.assertion.assertion import (
    BaseEntityAssertion,
)
from datahub.api.entities.assertion.assertion_trigger import AssertionTrigger
from datahub.api.entities.assertion.filter import DatasetFilter
from datahub.emitter.mce_builder import datahub_guid
from datahub.metadata.com.linkedin.pegasus2avro.assertion import (
    AssertionInfo,
    AssertionType,
    FixedIntervalSchedule,
    FreshnessAssertionInfo,
    FreshnessAssertionSchedule,
    FreshnessAssertionScheduleType,
    FreshnessAssertionType,
    FreshnessCronSchedule,
)
from datahub.metadata.com.linkedin.pegasus2avro.timeseries import CalendarInterval


class FreshnessSourceType(Enum):
    LAST_MODIFIED_COLUMN = "last_modified_column"


class CronFreshnessAssertion(BaseEntityAssertion):
    type: Literal["freshness"]
    cron: str = Field(
        description="The cron expression to use. See https://crontab.guru/ for help."
    )
    timezone: str = Field(
        "UTC",
        description="The timezone to use for the cron schedule. Defaults to UTC.",
    )
    source_type: FreshnessSourceType = Field(
        default=FreshnessSourceType.LAST_MODIFIED_COLUMN
    )
    last_modified_field: str
    filters: Optional[DatasetFilter] = Field(default=None)

    def get_assertion_info(
        self,
    ) -> AssertionInfo:
        return AssertionInfo(
            description=self.description,
            type=AssertionType.FRESHNESS,
            freshnessAssertion=FreshnessAssertionInfo(
                type=FreshnessAssertionType.DATASET_CHANGE,
                entity=self.entity,
                schedule=FreshnessAssertionSchedule(
                    type=FreshnessAssertionScheduleType.CRON,
                    cron=FreshnessCronSchedule(cron=self.cron, timezone=self.timezone),
                ),
            ),
        )

    def get_id(self) -> str:
        guid_dict = {
            "entity": self.entity,
            "type": self.type,
            "id_raw": self.id_raw,
        }
        return self.id or datahub_guid(guid_dict)

    def get_assertion_info_aspect(self) -> AssertionInfo:
        return self.get_assertion_info()

    def get_assertion_trigger(self) -> Optional[AssertionTrigger]:
        return self.trigger


class FixedIntervalFreshnessAssertion(BaseEntityAssertion):
    type: Literal["freshness"]
    lookback_interval: timedelta
    filters: Optional[DatasetFilter] = Field(default=None)
    source_type: FreshnessSourceType = Field(
        default=FreshnessSourceType.LAST_MODIFIED_COLUMN
    )
    last_modified_field: str

    @field_validator("lookback_interval", mode="before")
    @classmethod
    def lookback_interval_to_timedelta(cls, v):
        if isinstance(v, str):
            seconds = humanfriendly.parse_timespan(v)
            return timedelta(seconds=seconds)
        raise ValueError("Invalid value.")

    def get_assertion_info(
        self,
    ) -> AssertionInfo:
        return AssertionInfo(
            description=self.description,
            type=AssertionType.FRESHNESS,
            freshnessAssertion=FreshnessAssertionInfo(
                type=FreshnessAssertionType.DATASET_CHANGE,
                entity=self.entity,
                schedule=FreshnessAssertionSchedule(
                    type=FreshnessAssertionScheduleType.FIXED_INTERVAL,
                    fixedInterval=FixedIntervalSchedule(
                        unit=CalendarInterval.SECOND,
                        multiple=self.lookback_interval.seconds,
                    ),
                ),
            ),
        )

    def get_id(self) -> str:
        guid_dict = {
            "entity": self.entity,
            "type": self.type,
            "id_raw": self.id_raw,
        }
        return self.id or datahub_guid(guid_dict)

    def get_assertion_info_aspect(self) -> AssertionInfo:
        return self.get_assertion_info()

    def get_assertion_trigger(self) -> Optional[AssertionTrigger]:
        return self.trigger


# Pydantic v2 smart union: automatically discriminates based on presence of
# unique fields (eg lookback_interval vs cron)
FreshnessAssertion = Union[FixedIntervalFreshnessAssertion, CronFreshnessAssertion]
