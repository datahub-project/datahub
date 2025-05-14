import logging
import os
import tempfile
from random import randint

import pytest

from datahub.emitter.mce_builder import make_ml_model_group_urn, make_ml_model_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext, RecordEnvelope
from datahub.ingestion.api.sink import NoopWriteCallback
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.sink.file import FileSink, FileSinkConfig
from datahub.metadata.schema_classes import (
    MLModelGroupPropertiesClass,
    MLModelPropertiesClass,
)
from tests.utils import (
    delete_urns_from_file,
    get_sleep_info,
    ingest_file_via_rest,
    wait_for_writes_to_sync,
)

logger = logging.getLogger(__name__)

# Generate unique model names for testing
start_index = randint(10, 10000)
model_names = [f"test_model_{i}" for i in range(start_index, start_index + 3)]
model_group_urn = make_ml_model_group_urn("workbench", "test_group", "DEV")
model_urns = [make_ml_model_urn("workbench", name, "DEV") for name in model_names]


class FileEmitter:
    def __init__(self, filename: str) -> None:
        self.sink: FileSink = FileSink(
            ctx=PipelineContext(run_id="create_test_data"),
            config=FileSinkConfig(filename=filename),
        )

    def emit(self, event):
        self.sink.write_record_async(
            record_envelope=RecordEnvelope(record=event, metadata={}),
            write_callback=NoopWriteCallback(),
        )

    def close(self):
        self.sink.close()


def create_test_data(filename: str):
    # Create model group
    model_group_mcp = MetadataChangeProposalWrapper(
        entityUrn=str(model_group_urn),
        aspect=MLModelGroupPropertiesClass(
            description="Test model group for integration testing",
            trainingJobs=["urn:li:dataProcessInstance:test_job"],
        ),
    )

    # Create models that belong to the group
    model_mcps = [
        MetadataChangeProposalWrapper(
            entityUrn=model_urn,
            aspect=MLModelPropertiesClass(
                name=f"Test Model ({model_urn})",
                description=f"Test model {model_urn}",
                groups=[str(model_group_urn)],
                trainingJobs=["urn:li:dataProcessInstance:test_job"],
            ),
        )
        for model_urn in model_urns
    ]

    file_emitter = FileEmitter(filename)
    for mcps in [model_group_mcp] + model_mcps:
        file_emitter.emit(mcps)

    file_emitter.close()


sleep_sec, sleep_times = get_sleep_info()


@pytest.fixture(scope="module", autouse=False)
def ingest_cleanup_data(auth_session, graph_client, request):
    new_file, filename = tempfile.mkstemp(suffix=".json")
    try:
        create_test_data(filename)
        print("ingesting ml model test data")
        ingest_file_via_rest(auth_session, filename)
        wait_for_writes_to_sync()
        yield
        print("removing ml model test data")
        delete_urns_from_file(graph_client, filename)
        wait_for_writes_to_sync()
    finally:
        os.remove(filename)


@pytest.mark.integration
def test_create_ml_models(graph_client: DataHubGraph, ingest_cleanup_data):
    """Test creation and validation of ML models and model groups."""

    # Validate model group properties
    fetched_group_props = graph_client.get_aspect(
        str(model_group_urn), MLModelGroupPropertiesClass
    )
    assert fetched_group_props is not None
    assert fetched_group_props.description == "Test model group for integration testing"
    assert fetched_group_props.trainingJobs == ["urn:li:dataProcessInstance:test_job"]

    # Validate individual models
    for model_urn in model_urns:
        fetched_model_props = graph_client.get_aspect(model_urn, MLModelPropertiesClass)
        assert fetched_model_props is not None
        assert fetched_model_props.name == f"Test Model ({model_urn})"
        assert fetched_model_props.description == f"Test model {model_urn}"
        assert str(model_group_urn) in (fetched_model_props.groups or [])
        assert fetched_model_props.trainingJobs == [
            "urn:li:dataProcessInstance:test_job"
        ]

    # Validate relationships between models and group
    related_models = set()
    for e in graph_client.get_related_entities(
        str(model_group_urn),
        relationship_types=["MemberOf"],
        direction=DataHubGraph.RelationshipDirection.INCOMING,
    ):
        related_models.add(e.urn)

    assert set(model_urns) == related_models
