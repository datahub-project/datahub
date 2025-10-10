from dataclasses import dataclass, field
from typing import Optional

# import numpy
from pydantic import BaseModel, Field


class ConsistencyScoreInfo(BaseModel):
    score: Optional[float] = Field(
        default=None,
        description="overall confidence score of the max_interval",
    )
    anomaly_confidence_penalty: Optional[float] = Field(
        default=None,
        description="penalty on the number of disagreements between used anomaly"
        "detection techniques",
    )
    num_anomalies_penalty: Optional[float] = Field(
        default=None,
        description="penalty number of anomalies detected",
    )
    cluster_index_penalty: Optional[float] = Field(
        default=None,
        description="penalty based on the relative index of the max normal cluster",
    )
    inter_cluster_penalty: Optional[float] = Field(
        default=None,
        description="penalty accounting for the variation of "
        "the intervals within the max normal cluster",
    )
    missing_days_penalty: Optional[float] = Field(
        default=None,
        description="penalty accounting for the missing days in the data",
    )
    time_span_penalty: Optional[float] = Field(
        default=None,
        description="penalty based on the size of data",
    )


@dataclass
class MaxNormalIntervalResult:
    unit: Optional[str] = field(default=None)
    max_normal_interval: Optional[int] = field(default=None)
    max_normal_interval_with_buffer: Optional[int] = field(default=None)
    consistency_info: Optional[ConsistencyScoreInfo] = field(default=None)
