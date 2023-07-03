from typing import Dict

import pydantic

from datahub.ingestion.source.state.checkpoint import CheckpointStateBase


class ProfilingCheckpointState(CheckpointStateBase):
    """
    Base class for representing the checkpoint state for all profiling based sources.
    Stores the last successful profiling time per urn.
    Subclasses can define additional state as appropriate.
    """

    # Last profiled stores urn, last_profiled timestamp millis in a dict
    last_profiled: Dict[str, pydantic.PositiveInt]
