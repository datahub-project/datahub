import tempfile
import types
import unittest
from typing import Dict, List, Optional, Type
from unittest.mock import MagicMock, patch

from avrogen.dict_wrapper import DictWrapper

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.ingestion_job_checkpointing_provider_base import (
    CheckpointJobStateType,
    IngestionCheckpointingProviderBase,
    JobId,
)
from datahub.ingestion.source.state.checkpoint import Checkpoint
from datahub.ingestion.source.state.sql_common_state import (
    BaseSQLAlchemyCheckpointState,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfig,
    StateProviderWrapper,
)
from datahub.ingestion.source.state.usage_common_state import (
    BaseTimeWindowCheckpointState,
)
from datahub.ingestion.source.state_provider.datahub_ingestion_checkpointing_provider import (
    DatahubIngestionCheckpointingProvider,
)
from datahub.ingestion.source.state_provider.file_ingestion_checkpointing_provider import (
    FileIngestionCheckpointingProvider,
)
from tests.test_helpers.type_helpers import assert_not_null


class TestIngestionCheckpointProviders(unittest.TestCase):
    # Static members for the tests
    pipeline_name: str = "test_pipeline"
    job_names: List[JobId] = [JobId("job1"), JobId("job2")]
    run_id: str = "test_run"

    def setUp(self) -> None:
        self._setup_mock_graph()
        self._create_providers()

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

    def _create_providers(self) -> None:
        ctx: PipelineContext = PipelineContext(
            run_id=self.run_id, pipeline_name=self.pipeline_name
        )
        ctx.graph = self.mock_graph
        self.providers: List[IngestionCheckpointingProviderBase] = [
            DatahubIngestionCheckpointingProvider.create({}, ctx),
            FileIngestionCheckpointingProvider.create(
                {"filename": f"{tempfile.mkdtemp()}/checkpoint_mces.json"},
                ctx,
            ),
        ]

    def monkey_patch_emit_mcp(
        self, graph_ref: MagicMock, mcpw: MetadataChangeProposalWrapper
    ) -> None:
        """
        Mockey patched implementation of DatahubGraph.emit_mcp that caches the mcp locally in memory.
        """
        self.assertIsNotNone(graph_ref)
        if mcpw.aspectName != "status":
            self.assertEqual(mcpw.entityType, "dataJob")
            self.assertEqual(mcpw.aspectName, "datahubIngestionCheckpoint")
        # Cache the mcpw against the entityUrn
        assert mcpw.entityUrn is not None
        self.mcps_emitted[mcpw.entityUrn] = mcpw

    def monkey_patch_get_latest_timeseries_value(
        self,
        graph_ref: MagicMock,
        entity_urn: str,
        aspect_type: Type[DictWrapper],
        filter_criteria_map: Dict[str, str],
    ) -> Optional[DictWrapper]:
        """
        Monkey patched implementation of DatahubGraph.get_latest_timeseries_value that returns the latest cached aspect
        for a given entity urn.
        """
        self.assertIsNotNone(graph_ref)
        self.assertEqual(aspect_type, CheckpointJobStateType)
        self.assertEqual(
            filter_criteria_map,
            {
                "pipelineName": self.pipeline_name,
            },
        )
        # Retrieve the cached mcpw and return its aspect value.
        mcpw = self.mcps_emitted.get(entity_urn)
        if mcpw:
            return mcpw.aspect
        return None

    def test_providers(self):
        self.assertEqual(len(self.providers), 2)
        for provider in self.providers:
            assert provider
            # 1. Create the individual job checkpoints with appropriate states.
            # Job1 - Checkpoint with a BaseSQLAlchemyCheckpointState state
            job1_state_obj = BaseSQLAlchemyCheckpointState()
            job1_checkpoint = Checkpoint(
                job_name=self.job_names[0],
                pipeline_name=self.pipeline_name,
                run_id=self.run_id,
                state=job1_state_obj,
            )
            # Job2 - Checkpoint with a BaseTimeWindowCheckpointState state
            job2_state_obj = BaseTimeWindowCheckpointState(
                begin_timestamp_millis=10, end_timestamp_millis=100
            )
            job2_checkpoint = Checkpoint(
                job_name=self.job_names[1],
                pipeline_name=self.pipeline_name,
                run_id=self.run_id,
                state=job2_state_obj,
            )

            # 2. Set the provider's state_to_commit.
            provider.state_to_commit = {
                # NOTE: state_to_commit accepts only the aspect version of the checkpoint.
                self.job_names[0]: assert_not_null(
                    job1_checkpoint.to_checkpoint_aspect(max_allowed_state_size=2**20)
                ),
                self.job_names[1]: assert_not_null(
                    job2_checkpoint.to_checkpoint_aspect(max_allowed_state_size=2**20)
                ),
            }

            # 3. Perform the commit
            # NOTE: This will commit the state to
            # In-memory self.mcps_emitted because of the monkey-patching for datahub ingestion checkpointer provider.
            # And to temp directory json file for file ingestion checkpointer provider.
            provider.commit()
            self.assertTrue(provider.committed)

            # 4. Get last committed state. This must match what has been committed earlier.
            # NOTE: This will retrieve the state form where it is committed.
            job1_last_state = provider.get_latest_checkpoint(
                self.pipeline_name, self.job_names[0]
            )
            job2_last_state = provider.get_latest_checkpoint(
                self.pipeline_name, self.job_names[1]
            )

            # 5. Validate individual job checkpoint state values that have been committed and retrieved
            # against the original values.
            self.assertIsNotNone(job1_last_state)
            job1_last_checkpoint = Checkpoint.create_from_checkpoint_aspect(
                job_name=self.job_names[0],
                checkpoint_aspect=job1_last_state,
                state_class=type(job1_state_obj),
            )
            self.assertEqual(job1_last_checkpoint, job1_checkpoint)

            self.assertIsNotNone(job2_last_state)
            job2_last_checkpoint = Checkpoint.create_from_checkpoint_aspect(
                job_name=self.job_names[1],
                checkpoint_aspect=job2_last_state,
                state_class=type(job2_state_obj),
            )
            self.assertEqual(job2_last_checkpoint, job2_checkpoint)

    def test_state_provider_wrapper_with_config_provided(self):
        # stateful_ingestion_config.enabled as true
        ctx = PipelineContext(run_id=self.run_id, pipeline_name=self.pipeline_name)
        ctx.graph = self.mock_graph
        state_provider = StateProviderWrapper(
            StatefulIngestionConfig(enabled=True), ctx
        )
        assert state_provider.stateful_ingestion_config
        assert state_provider.ingestion_checkpointing_state_provider
        # stateful_ingestion_config.enabled as false
        ctx = PipelineContext(run_id=self.run_id, pipeline_name=self.pipeline_name)
        ctx.graph = self.mock_graph
        state_provider = StateProviderWrapper(
            StatefulIngestionConfig(enabled=False), ctx
        )
        assert state_provider.stateful_ingestion_config
        assert not state_provider.ingestion_checkpointing_state_provider

    def test_state_provider_wrapper_with_config_not_provided(self):
        # graph object is present
        ctx = PipelineContext(run_id=self.run_id, pipeline_name=self.pipeline_name)
        ctx.graph = self.mock_graph
        state_provider = StateProviderWrapper(None, ctx)
        assert state_provider.stateful_ingestion_config
        assert state_provider.ingestion_checkpointing_state_provider
        # graph object is none
        ctx = PipelineContext(run_id=self.run_id, pipeline_name=self.pipeline_name)
        state_provider = StateProviderWrapper(None, ctx)
        assert not state_provider.stateful_ingestion_config
        assert not state_provider.ingestion_checkpointing_state_provider
