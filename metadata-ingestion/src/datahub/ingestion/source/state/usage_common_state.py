from typing import Optional

import pydantic

from datahub.configuration.time_window_config import BucketDuration
from datahub.ingestion.source.state.checkpoint import CheckpointStateBase


class BaseTimeWindowCheckpointState(CheckpointStateBase):
    """
    Base class for representing the checkpoint state for all time window based ingestion stages.
    Stores the last successful run's begin and end timestamps.
    Subclasses can define additional state as appropriate.
    """

    begin_timestamp_millis: pydantic.NonNegativeInt
    end_timestamp_millis: pydantic.NonNegativeInt

    # Required for time bucket based aggregations -  e.g. Usage
    bucket_duration: Optional[BucketDuration] = None
