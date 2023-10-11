from __future__ import annotations

from datetime import timedelta
from typing import List, Union

import pydantic
from typing_extensions import Literal

from datahub.configuration.common import ConfigModel
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.schema_classes import (
    AssertionInfoClass,
    AssertionTypeClass,
    CalendarIntervalClass,
    FixedIntervalScheduleClass,
    FreshnessAssertionInfoClass,
    FreshnessAssertionScheduleClass,
    FreshnessAssertionScheduleTypeClass,
    FreshnessAssertionTypeClass,
    FreshnessCronScheduleClass,
)


class CronFreshnessAssertion(ConfigModel):
    type: Literal["cron"]

    cron: str = pydantic.Field(
        description="The cron expression to use. See https://crontab.guru/ for help."
    )
    timezone: str = pydantic.Field(
        "UTC",
        description="The timezone to use for the cron schedule. Defaults to UTC.",
    )


class FixedIntervalFreshnessAssertion(ConfigModel):
    type: Literal["interval"]

    interval: timedelta


class FreshnessAssertion(ConfigModel):
    __root__: Union[
        CronFreshnessAssertion, FixedIntervalFreshnessAssertion
    ] = pydantic.Field(discriminator="type")

    @property
    def id(self):
        return self.__root__.type

    def generate_mcp(
        self, assertion_urn: str, entity_urn: str
    ) -> List[MetadataChangeProposalWrapper]:
        freshness = self.__root__

        if isinstance(freshness, CronFreshnessAssertion):
            schedule = FreshnessAssertionScheduleClass(
                type=FreshnessAssertionScheduleTypeClass.CRON,
                cron=FreshnessCronScheduleClass(
                    cron=freshness.cron,
                    timezone=freshness.timezone,
                ),
            )
        elif isinstance(freshness, FixedIntervalFreshnessAssertion):
            schedule = FreshnessAssertionScheduleClass(
                type=FreshnessAssertionScheduleTypeClass.FIXED_INTERVAL,
                fixedInterval=FixedIntervalScheduleClass(
                    unit=CalendarIntervalClass.SECOND,
                    multiple=int(freshness.interval.total_seconds()),
                ),
            )
        else:
            raise ValueError(f"Unknown freshness type {freshness}")

        assertionInfo = AssertionInfoClass(
            type=AssertionTypeClass.FRESHNESS,
            freshnessAssertion=FreshnessAssertionInfoClass(
                entity=entity_urn,
                type=FreshnessAssertionTypeClass.DATASET_CHANGE,
                schedule=schedule,
            ),
        )

        return [
            MetadataChangeProposalWrapper(entityUrn=assertion_urn, aspect=assertionInfo)
        ]
