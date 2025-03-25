from dataclasses import dataclass, field
from datetime import timedelta
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field


class ConfidenceScoreInfo(BaseModel):
    score: Optional[float] = Field(
        default=None,
        description="overall confidence score of the inference",
    )


@dataclass
class Bucket:
    bucket_number: int
    bucket_size: str
    bucket_start_time: float
    bucket_end_time: float
    bucket_id: str
    min_value: Optional[float] = field(default=None)
    max_value: Optional[float] = field(default=None)
    min_value_with_buffer: Optional[float] = field(default=None)
    max_value_with_buffer: Optional[float] = field(default=None)


@dataclass
class MetricForecastInfo:
    confidence_score_info: Optional[ConfidenceScoreInfo] = field(default=None)
    best_params: Optional[dict] = field(default=None)
    data_standard_deviation: Optional[float] = field(default=None)


class TimePeriod(Enum):
    HOUR = "H"
    DAY = "D"
    WEEK = "W"
    MONTH = "M"

    @staticmethod
    def from_timedelta(delta: timedelta) -> "TimePeriod":
        if delta <= timedelta(hours=1):
            return TimePeriod.HOUR
        elif delta <= timedelta(days=1):
            return TimePeriod.DAY
        elif delta <= timedelta(days=7):
            return TimePeriod.WEEK
        else:
            return TimePeriod.MONTH
