import types
import unittest
from typing import Dict, List, Optional, Type
from unittest.mock import MagicMock, patch

from avrogen.dict_wrapper import DictWrapper

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.ingestion_job_checkpointing_provider_base import (
    CheckpointJobStatesMap,
    CheckpointJobStateType,
    IngestionCheckpointingProviderBase,
    JobId,
    JobStateKey,
)
from datahub.ingestion.source.sql.postgres import PostgresConfig
from datahub.ingestion.source.state.checkpoint import Checkpoint
from datahub.ingestion.source.state.sql_common_state import (
    BaseSQLAlchemyCheckpointState,
)
from datahub.ingestion.source.state.usage_common_state import BaseUsageCheckpointState
from datahub.ingestion.source.state_provider.datahub_ingestion_checkpointing_provider import (
    DatahubIngestionCheckpointingProvider,
)


class TestDatahubIngestionCheckpointProvider(unittest.TestCase):
    # Static members for the tests
    pipeline_name: str = "test_pipeline"
    platform_instance_id: str = "test_platform_instance_1"
    job_names: List[JobId] = [JobId("job1"), JobId("job2")]
    run_id: str = "test_run"
    job_state_key: JobStateKey = JobStateKey(
        pipeline_name=pipeline_name,
        platform_instance_id=platform_instance_id,
        job_names=job_names,
    )

    def setUp(self) -> None:
        self._setup_mock_graph()
        self.provider = self._create_provider()
        assert self.provider

    def _setup_mock_graph(self) -> None:
        """
        Setup monkey-patched graph client.
        """
        self.patcher = patch(
            "datahub.ingestion.graph.client.DataHubGraph", autospec=True
        )
        self.addCleanup(self.patcher.stop)
        self.mock_graph = self.patcher.start()
        # Make server stateful ingestion capable
        self.mock_graph.get_config.return_value = {"statefulIngestionCapable": True}
        # Bind mock_graph's emit_mcp to testcase's monkey_patch_emit_mcp so that we can emulate emits.
        self.mock_graph.emit_mcp = types.MethodType(
            self.monkey_patch_emit_mcp, self.mock_graph
        )
        # Bind mock_graph's get_latest_timeseries_value to monkey_patch_get_latest_timeseries_value
        self.mock_graph.get_latest_timeseries_value = types.MethodType(
            self.monkey_patch_get_latest_timeseries_value, self.mock_graph
        )
        # Tracking for emitted mcps.
        self.mcps_emitted: Dict[str, MetadataChangeProposalWrapper] = {}

    def _create_provider(self) -> IngestionCheckpointingProviderBase:
        ctx: PipelineContext = PipelineContext(
            run_id=self.run_id, pipeline_name=self.pipeline_name
        )
        ctx.graph = self.mock_graph
        return DatahubIngestionCheckpointingProvider.create(
            {}, ctx, name=DatahubIngestionCheckpointingProvider.__name__
        )

    def monkey_patch_emit_mcp(
        self, graph_ref: MagicMock, mcpw: MetadataChangeProposalWrapper
    ) -> None:
        """
        Mockey patched implementation of DatahubGraph.emit_mcp that caches the mcp locally in memory.
        """
        self.assertIsNotNone(graph_ref)
        self.assertEqual(mcpw.entityType, "dataJob")
        self.assertEqual(mcpw.aspectName, "datahubIngestionCheckpoint")
        # Cache the mcpw against the entityUrn
        assert mcpw.entityUrn is not None
        self.mcps_emitted[mcpw.entityUrn] = mcpw

    def monkey_patch_get_latest_timeseries_value(
        self,
        graph_ref: MagicMock,
        entity_urn: str,
        aspect_name: str,
        aspect_type: Type[DictWrapper],
        filter_criteria_map: Dict[str, str],
    ) -> Optional[DictWrapper]:
        """
        Monkey patched implementation of DatahubGraph.get_latest_timeseries_value that returns the latest cached aspect
        for a given entity urn.
        """
        self.assertIsNotNone(graph_ref)
        self.assertEqual(aspect_name, "datahubIngestionCheckpoint")
        self.assertEqual(aspect_type, CheckpointJobStateType)
        self.assertEqual(
            filter_criteria_map,
            {
                "pipelineName": self.pipeline_name,
                "platformInstanceId": self.platform_instance_id,
            },
        )
        # Retrieve the cached mcpw and return its aspect value.
        mcpw = self.mcps_emitted.get(entity_urn)
        if mcpw:
            return mcpw.aspect
        return None

    def test_provider(self):

        # 1. Create the individual job checkpoints with appropriate states.
        # Job1 - Checkpoint with a BaseSQLAlchemyCheckpointState state
        job1_state_obj = BaseSQLAlchemyCheckpointState()
        job1_checkpoint = Checkpoint(
            job_name=self.job_names[0],
            pipeline_name=self.pipeline_name,
            platform_instance_id=self.platform_instance_id,
            run_id=self.run_id,
            config=PostgresConfig(host_port="localhost:5432"),
            state=job1_state_obj,
        )
        # Job2 - Checkpoint with a BaseUsageCheckpointState state
        job2_state_obj = BaseUsageCheckpointState(
            begin_timestamp_millis=10, end_timestamp_millis=100
        )
        job2_checkpoint = Checkpoint(
            job_name=self.job_names[1],
            pipeline_name=self.pipeline_name,
            platform_instance_id=self.platform_instance_id,
            run_id=self.run_id,
            config=PostgresConfig(host_port="localhost:5432"),
            state=job2_state_obj,
        )

        # 2. Set the provider's state_to_commit.
        self.provider.state_to_commit = {
            # NOTE: state_to_commit accepts only the aspect version of the checkpoint.
            self.job_names[0]: job1_checkpoint.to_checkpoint_aspect(
                max_allowed_state_size=2**20
            ),
            self.job_names[1]: job2_checkpoint.to_checkpoint_aspect(
                max_allowed_state_size=2**20
            ),
        }

        # 3. Perform the commit
        # NOTE: This will commit the state to the in-memory self.mcps_emitted because of the monkey-patching.
        self.provider.commit()
        self.assertTrue(self.provider.committed)

        # 4. Get last committed state. This must match what has been committed earlier.
        # NOTE: This will retrieve from in-memory self.mcps_emitted because of the monkey-patching.
        last_state: Optional[CheckpointJobStatesMap] = self.provider.get_last_state(
            self.job_state_key
        )
        assert last_state is not None
        self.assertEqual(len(last_state), 2)

        # 5. Validate individual job checkpoint state values that have been committed and retrieved
        # against the original values.
        self.assertIsNotNone(last_state[self.job_names[0]])
        job1_last_checkpoint = Checkpoint.create_from_checkpoint_aspect(
            job_name=self.job_names[0],
            checkpoint_aspect=last_state[self.job_names[0]],
            state_class=type(job1_state_obj),
            config_class=type(job1_checkpoint.config),
        )
        self.assertEqual(job1_last_checkpoint, job1_checkpoint)

        self.assertIsNotNone(last_state[self.job_names[1]])
        job2_last_checkpoint = Checkpoint.create_from_checkpoint_aspect(
            job_name=self.job_names[1],
            checkpoint_aspect=last_state[self.job_names[1]],
            state_class=type(job2_state_obj),
            config_class=type(job2_checkpoint.config),
        )
        self.assertEqual(job2_last_checkpoint, job2_checkpoint)
