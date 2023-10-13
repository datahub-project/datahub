from typing import List

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.ingestion_job_checkpointing_provider_base import JobId
from datahub.ingestion.source.state.checkpoint import Checkpoint
from datahub.ingestion.source.state.sql_common_state import (
    BaseSQLAlchemyCheckpointState,
)
from datahub.ingestion.source.state.usage_common_state import (
    BaseTimeWindowCheckpointState,
)
from datahub.ingestion.source.state_provider.file_ingestion_checkpointing_provider import (
    FileIngestionCheckpointingProvider,
)
from tests.test_helpers.type_helpers import assert_not_null

pipeline_name: str = "test_pipeline"
job_names: List[JobId] = [JobId("job1"), JobId("job2")]
run_id: str = "test_run"


def test_file_ingestion_checkpointing_provider(tmp_path):
    ctx: PipelineContext = PipelineContext(run_id=run_id, pipeline_name=pipeline_name)
    provider = FileIngestionCheckpointingProvider.create(
        {"filename": str(tmp_path / "checkpoint_mces.json")},
        ctx,
        name=FileIngestionCheckpointingProvider.__name__,
    )
    # 1. Create the individual job checkpoints with appropriate states.
    # Job1 - Checkpoint with a BaseSQLAlchemyCheckpointState state
    job1_state_obj = BaseSQLAlchemyCheckpointState()
    job1_checkpoint = Checkpoint(
        job_name=job_names[0],
        pipeline_name=pipeline_name,
        run_id=run_id,
        state=job1_state_obj,
    )
    # Job2 - Checkpoint with a BaseTimeWindowCheckpointState state
    job2_state_obj = BaseTimeWindowCheckpointState(
        begin_timestamp_millis=10, end_timestamp_millis=100
    )
    job2_checkpoint = Checkpoint(
        job_name=job_names[1],
        pipeline_name=pipeline_name,
        run_id=run_id,
        state=job2_state_obj,
    )

    # 2. Set the provider's state_to_commit.
    provider.state_to_commit = {
        # NOTE: state_to_commit accepts only the aspect version of the checkpoint.
        job_names[0]: assert_not_null(
            job1_checkpoint.to_checkpoint_aspect(max_allowed_state_size=2**20)
        ),
        job_names[1]: assert_not_null(
            job2_checkpoint.to_checkpoint_aspect(max_allowed_state_size=2**20)
        ),
    }

    # 3. Perform the commit
    # NOTE: This will commit the state to the in-memory mcps_emitted because of the monkey-patching.
    provider.commit()
    assert provider.committed

    # 4. Get last committed state. This must match what has been committed earlier.
    # NOTE: This will retrieve from in-memory mcps_emitted because of the monkey-patching.
    job1_last_state = provider.get_latest_checkpoint(pipeline_name, job_names[0])
    job2_last_state = provider.get_latest_checkpoint(pipeline_name, job_names[1])

    # 5. Validate individual job checkpoint state values that have been committed and retrieved
    # against the original values.
    assert job1_last_state is not None
    job1_last_checkpoint = Checkpoint.create_from_checkpoint_aspect(
        job_name=job_names[0],
        checkpoint_aspect=job1_last_state,
        state_class=type(job1_state_obj),
    )
    assert job1_last_checkpoint == job1_checkpoint

    assert job2_last_state is not None
    job2_last_checkpoint = Checkpoint.create_from_checkpoint_aspect(
        job_name=job_names[1],
        checkpoint_aspect=job2_last_state,
        state_class=type(job2_state_obj),
    )
    assert job2_last_checkpoint == job2_checkpoint
