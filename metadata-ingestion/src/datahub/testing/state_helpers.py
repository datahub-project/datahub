from typing import Optional, cast

from datahub.ingestion.run.pipeline import Pipeline
from datahub.ingestion.source.state.checkpoint import Checkpoint
from datahub.ingestion.source.state.entity_removal_state import GenericCheckpointState
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalHandler,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionSourceBase,
)


def get_current_checkpoint_from_pipeline(
    pipeline: Pipeline,
) -> Optional[Checkpoint[GenericCheckpointState]]:
    """
    Helper method to retrieve the current checkpoint from a pipeline.
    """
    stateful_source = cast(StatefulIngestionSourceBase, pipeline.source)
    return stateful_source.state_provider.get_current_checkpoint(
        StaleEntityRemovalHandler.compute_job_id(
            getattr(stateful_source, "platform", "default")
        )
    )
