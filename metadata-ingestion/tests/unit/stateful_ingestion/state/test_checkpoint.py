from datetime import datetime
from typing import Dict

import pytest

from datahub.emitter.mce_builder import make_dataset_urn
from datahub.ingestion.source.sql.mysql import MySQLConfig
from datahub.ingestion.source.sql.sql_common import BasicSQLAlchemyConfig
from datahub.ingestion.source.state.checkpoint import Checkpoint, CheckpointStateBase
from datahub.ingestion.source.state.sql_common_state import (
    BaseSQLAlchemyCheckpointState,
)
from datahub.ingestion.source.state.usage_common_state import BaseUsageCheckpointState
from datahub.metadata.schema_classes import (
    DatahubIngestionCheckpointClass,
    IngestionCheckpointStateClass,
)

# 1. Setup common test param values.
test_pipeline_name: str = "test_pipeline"
test_platform_instance_id: str = "test_platform_instance_1"
test_job_name: str = "test_job_1"
test_run_id: str = "test_run_1"
test_source_config: BasicSQLAlchemyConfig = MySQLConfig()

# 2. Create the params for parametrized tests.

#   2.1 Create and add an instance of BaseSQLAlchemyCheckpointState.
test_checkpoint_serde_params: Dict[str, CheckpointStateBase] = {}
base_sql_alchemy_checkpoint_state_obj = BaseSQLAlchemyCheckpointState()
base_sql_alchemy_checkpoint_state_obj.add_table_urn(
    make_dataset_urn("mysql", "db1.t1", "prod")
)
base_sql_alchemy_checkpoint_state_obj.add_view_urn(
    make_dataset_urn("mysql", "db1.v1", "prod")
)
test_checkpoint_serde_params[
    "BaseSQLAlchemyCheckpointState"
] = base_sql_alchemy_checkpoint_state_obj

#   2.2 Create and add an instance of BaseUsageCheckpointState.
base_usage_checkpoint_state_obj = BaseUsageCheckpointState(
    version="2.0", begin_timestamp_millis=1, end_timestamp_millis=100
)
test_checkpoint_serde_params[
    "BaseUsageCheckpointState"
] = base_usage_checkpoint_state_obj


# 3. Define the test with the params


@pytest.mark.parametrize(
    "state_obj",
    test_checkpoint_serde_params.values(),
    ids=test_checkpoint_serde_params.keys(),
)
def test_create_from_checkpoint_aspect(state_obj):
    """
    Tests the Checkpoint class API 'create_from_checkpoint_aspect' with the state_obj parameter as the state.
    """
    # 1. Construct the raw aspect object with the state
    checkpoint_state = IngestionCheckpointStateClass(
        formatVersion=state_obj.version,
        serde=state_obj.serde,
        payload=state_obj.to_bytes(),
    )
    checkpoint_aspect = DatahubIngestionCheckpointClass(
        timestampMillis=int(datetime.utcnow().timestamp() * 1000),
        pipelineName=test_pipeline_name,
        platformInstanceId=test_platform_instance_id,
        config=test_source_config.json(),
        state=checkpoint_state,
        runId=test_run_id,
    )

    # 2. Create the checkpoint from the raw checkpoint aspect and validate.
    checkpoint_obj = Checkpoint.create_from_checkpoint_aspect(
        job_name=test_job_name,
        checkpoint_aspect=checkpoint_aspect,
        state_class=type(state_obj),
        config_class=MySQLConfig,
    )

    expected_checkpoint_obj = Checkpoint(
        job_name=test_job_name,
        pipeline_name=test_pipeline_name,
        platform_instance_id=test_platform_instance_id,
        run_id=test_run_id,
        config=test_source_config,
        state=state_obj,
    )
    assert checkpoint_obj == expected_checkpoint_obj


@pytest.mark.parametrize(
    "state_obj",
    test_checkpoint_serde_params.values(),
    ids=test_checkpoint_serde_params.keys(),
)
def test_serde_idempotence(state_obj):
    """
    Verifies that Serialization + Deserialization reconstructs the original object fully.
    """
    # 1. Construct the initial checkpoint object
    orig_checkpoint_obj = Checkpoint(
        job_name=test_job_name,
        pipeline_name=test_pipeline_name,
        platform_instance_id=test_platform_instance_id,
        run_id=test_run_id,
        config=test_source_config,
        state=state_obj,
    )

    # 2. Convert it to the aspect form.
    checkpoint_aspect = orig_checkpoint_obj.to_checkpoint_aspect(
        # fmt: off
        max_allowed_state_size=2**20
        # fmt: on
    )
    assert checkpoint_aspect is not None

    # 3. Reconstruct from the aspect form and verify that it matches the original.
    serde_checkpoint_obj = Checkpoint.create_from_checkpoint_aspect(
        job_name=test_job_name,
        checkpoint_aspect=checkpoint_aspect,
        state_class=type(state_obj),
        config_class=MySQLConfig,
    )
    assert orig_checkpoint_obj == serde_checkpoint_obj
