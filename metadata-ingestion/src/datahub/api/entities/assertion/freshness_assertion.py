from datetime import timedelta
from typing import Optional, Union

from typing_extensions import Literal

from datahub.api.entities.assertion.assertion import (
    BaseAssertionProtocol,
    BaseEntityAssertion,
)
from datahub.api.entities.assertion.assertion_trigger import AssertionTrigger
from datahub.api.entities.assertion.filter import DatasetFilter
from datahub.configuration.pydantic_migration_helpers import v1_Field
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


class CronFreshnessAssertion(BaseEntityAssertion):
    type: Literal["freshness"]
    freshness_schedule_type: Literal["cron"]
    cron: str = v1_Field(
        description="The cron expression to use. See https://crontab.guru/ for help."
    )
    timezone: str = v1_Field(
        "UTC",
        description="The timezone to use for the cron schedule. Defaults to UTC.",
    )
    filter: Optional[DatasetFilter] = v1_Field(default=None)

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


class FixedIntervalFreshnessAssertion(BaseEntityAssertion):
    type: Literal["freshness"]
    freshness_schedule_type: Literal["interval"]
    interval: timedelta
    filter: Optional[DatasetFilter] = v1_Field(default=None)

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
                        unit=CalendarInterval.SECOND, multiple=self.interval.seconds
                    ),
                ),
            ),
        )


class FreshnessAssertion(BaseAssertionProtocol):
    __root__: Union[CronFreshnessAssertion, FixedIntervalFreshnessAssertion] = v1_Field(
        discriminator="freshness_schedule_type"
    )

    @property
    def assertion(self):
        return self.__root__

    def get_id(self) -> str:
        guid_dict = {
            "entity": self.__root__.entity,
            "type": self.__root__.type,
            "id_raw": self.__root__.id_raw,
        }
        return self.__root__.id or datahub_guid(guid_dict)

    def get_assertion_info_aspect(
        self,
    ) -> AssertionInfo:
        return self.__root__.get_assertion_info()

    def get_assertion_trigger(self) -> Optional[AssertionTrigger]:
        return self.__root__.trigger
