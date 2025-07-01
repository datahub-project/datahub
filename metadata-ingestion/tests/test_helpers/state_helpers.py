import types
from typing import Any, Callable, Dict, Optional, Type, cast
from unittest.mock import MagicMock, create_autospec

import pytest
from avrogen.dict_wrapper import DictWrapper

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.ingestion_job_checkpointing_provider_base import (
    IngestionCheckpointingProviderBase,
)
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.graph.config import DatahubClientConfig
from datahub.ingestion.run.pipeline import Pipeline
from datahub.ingestion.source.state.checkpoint import Checkpoint
from datahub.ingestion.source.state.entity_removal_state import GenericCheckpointState
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalHandler,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionSourceBase,
)


def validate_all_providers_have_committed_successfully(
    pipeline: Pipeline, expected_providers: int
) -> None:
    """
    makes sure the pipeline includes the desired number of providers
    and for each of them verify it has successfully committed
    """
    provider_count: int = 0
    for _, provider in pipeline.ctx.get_committables():
        provider_count += 1
        assert isinstance(provider, IngestionCheckpointingProviderBase)
        stateful_committable = cast(IngestionCheckpointingProviderBase, provider)
        assert stateful_committable.has_successfully_committed()
        assert stateful_committable.state_to_commit
    assert provider_count == expected_providers


def run_and_get_pipeline(pipeline_config_dict: Dict[str, Any]) -> Pipeline:
    pipeline = Pipeline.create(pipeline_config_dict)
    pipeline.run()
    pipeline.raise_from_status()
    return pipeline


@pytest.fixture
def mock_datahub_graph():
    class MockDataHubGraphContext:
        pipeline_name: str = "test_pipeline"
        run_id: str = "test_run"

        def __init__(self) -> None:
            """
            Create a new monkey-patched instance of the DataHubGraph graph client.
            """
            # ensure this mock keeps the same api of the original class
            self.mock_graph = create_autospec(DataHubGraph)
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

        def monkey_patch_emit_mcp(
            self, graph_ref: MagicMock, mcpw: MetadataChangeProposalWrapper
        ) -> None:
            """
            Mockey patched implementation of DatahubGraph.emit_mcp that caches the mcp locally in memory.
            """
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
            # Retrieve the cached mcpw and return its aspect value.
            mcpw = self.mcps_emitted.get(entity_urn)
            if mcpw:
                return mcpw.aspect
            return None

    mock_datahub_graph_ctx = MockDataHubGraphContext()
    return mock_datahub_graph_ctx.mock_graph


@pytest.fixture
def mock_datahub_graph_instance(
    mock_datahub_graph: Callable[[DatahubClientConfig], DataHubGraph],
) -> DataHubGraph:
    return mock_datahub_graph(DatahubClientConfig(server="http://fake.domain.local"))


def get_current_checkpoint_from_pipeline(
    pipeline: Pipeline,
) -> Optional[Checkpoint[GenericCheckpointState]]:
    # TODO: This only works for stale entity removal. We need to generalize this.

    stateful_source = cast(StatefulIngestionSourceBase, pipeline.source)
    return stateful_source.state_provider.get_current_checkpoint(
        StaleEntityRemovalHandler.compute_job_id(
            getattr(stateful_source, "platform", "default")
        )
    )
