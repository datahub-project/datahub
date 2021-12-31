import pydantic

from datahub.ingestion.source.state.checkpoint import CheckpointStateBase


class BaseUsageCheckpointState(CheckpointStateBase):
    """
    Base class for representing the checkpoint state for all usage based sources.
    Stores the last successful run's begin and end timestamps.
    Subclasses can define additional state as appropriate.
    """

    begin_timestamp_millis: pydantic.PositiveInt
    end_timestamp_millis: pydantic.PositiveInt
