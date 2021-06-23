from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Iterable, Optional

import pydantic

from datahub.ingestion.api.source import Source, SourceReport
from datahub.ingestion.api.workunit import UsageStatsWorkUnit
from datahub.ingestion.source.snowflake import SnowflakeConfig


class SnowflakeUsageConfig(SnowflakeConfig):
    # start_time and end_time will be populated by the validators.
    bucket_duration: _BucketDuration = _BucketDuration.DAY
    end_time: datetime = None  # type: ignore
    start_time: datetime = None  # type: ignore

    top_n_queries: Optional[pydantic.PositiveInt] = 10

    @pydantic.validator("end_time", pre=True, always=True)
    def default_end_time(cls, v, *, values, **kwargs):
        return v or get_time_bucket(
            datetime.now(tz=timezone.utc), values["bucket_duration"]
        )

    pass


@dataclass
class SnowflakeUsageSource(Source):
    config: SnowflakeUsageConfig
    report: SourceReport = field(default_factory=SourceReport)

    @classmethod
    def create(cls, config_dict, ctx):
        config = SnowflakeUsageConfig.parse_obj(config_dict)
        return cls(ctx, config)

    def get_workunits(self) -> Iterable[UsageStatsWorkUnit]:
        pass

    def get_report(self):
        return self.report

    def close(self):
        pass
