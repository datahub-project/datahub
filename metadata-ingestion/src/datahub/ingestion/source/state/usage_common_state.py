# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

from typing import Optional

import pydantic

from datahub.configuration.time_window_config import BucketDuration
from datahub.ingestion.source.state.checkpoint import CheckpointStateBase
from datahub.utilities.time import TimeWindow, ts_millis_to_datetime


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

    def to_time_interval(self) -> TimeWindow:
        return TimeWindow(
            ts_millis_to_datetime(self.begin_timestamp_millis),
            ts_millis_to_datetime(self.end_timestamp_millis),
        )
