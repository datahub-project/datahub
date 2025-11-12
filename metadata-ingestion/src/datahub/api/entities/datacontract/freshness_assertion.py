from __future__ import annotations

from datetime import timedelta
from typing import List, Union

from pydantic import Field, RootModel
from typing_extensions import Literal

from datahub.api.entities.datacontract.assertion import BaseAssertion
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


class CronFreshnessAssertion(BaseAssertion):
    type: Literal["cron"]

    cron: str = Field(
        description="The cron expression to use. See https://crontab.guru/ for help."
    )
    timezone: str = Field(
        "UTC",
        description="The timezone to use for the cron schedule. Defaults to UTC.",
    )

    def generate_freshness_assertion_schedule(self) -> FreshnessAssertionScheduleClass:
        return FreshnessAssertionScheduleClass(
            type=FreshnessAssertionScheduleTypeClass.CRON,
            cron=FreshnessCronScheduleClass(
                cron=self.cron,
                timezone=self.timezone,
            ),
        )


class FixedIntervalFreshnessAssertion(BaseAssertion):
    type: Literal["interval"]

    interval: timedelta

    def generate_freshness_assertion_schedule(self) -> FreshnessAssertionScheduleClass:
        return FreshnessAssertionScheduleClass(
            type=FreshnessAssertionScheduleTypeClass.FIXED_INTERVAL,
            fixedInterval=FixedIntervalScheduleClass(
                unit=CalendarIntervalClass.SECOND,
                multiple=int(self.interval.total_seconds()),
            ),
        )


class FreshnessAssertion(
    RootModel[Union[CronFreshnessAssertion, FixedIntervalFreshnessAssertion]]
):
    root: Union[CronFreshnessAssertion, FixedIntervalFreshnessAssertion] = Field(
        discriminator="type"
    )

    @property
    def id(self):
        return self.root.type

    def generate_mcp(
        self, assertion_urn: str, entity_urn: str
    ) -> List[MetadataChangeProposalWrapper]:
        aspect = AssertionInfoClass(
            type=AssertionTypeClass.FRESHNESS,
            freshnessAssertion=FreshnessAssertionInfoClass(
                entity=entity_urn,
                type=FreshnessAssertionTypeClass.DATASET_CHANGE,
                schedule=self.root.generate_freshness_assertion_schedule(),
            ),
            description=self.root.description,
        )
        return [MetadataChangeProposalWrapper(entityUrn=assertion_urn, aspect=aspect)]
