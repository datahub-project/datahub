import logging
import os
import tempfile
from random import randint

import pytest
import tenacity

from conftest import _ingest_cleanup_data_impl
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


@pytest.fixture(scope="module", autouse=False)
def ingest_cleanup_data(auth_session, graph_client):
    _, filename = tempfile.mkstemp(suffix=".json")
    try:
        create_test_data(filename)
        yield from _ingest_cleanup_data_impl(
            auth_session, graph_client, filename, "ml_models"
        )
    finally:
        os.remove(filename)


@pytest.mark.integration
def test_create_ml_models(graph_client: DataHubGraph, ingest_cleanup_data):
    """Test creation and validation of ML models and model groups."""

    # Retry aspect fetch with exponential backoff for eventual consistency
    @tenacity.retry(
        stop=tenacity.stop_after_attempt(10),
        wait=tenacity.wait_exponential(multiplier=1, min=2, max=20),
        retry=tenacity.retry_if_exception_type(AssertionError),
        reraise=True,
    )
    def get_and_validate_group_properties():
        fetched_group_props = graph_client.get_aspect(
            str(model_group_urn), MLModelGroupPropertiesClass
        )
        assert fetched_group_props is not None, (
            f"Failed to fetch ML Model Group properties for {model_group_urn}"
        )
        return fetched_group_props

    # Validate model group properties
    fetched_group_props = get_and_validate_group_properties()
    assert fetched_group_props.description == "Test model group for integration testing"
    assert fetched_group_props.trainingJobs == ["urn:li:dataProcessInstance:test_job"]

    # Retry aspect fetch for individual models with exponential backoff
    @tenacity.retry(
        stop=tenacity.stop_after_attempt(10),
        wait=tenacity.wait_exponential(multiplier=1, min=2, max=20),
        retry=tenacity.retry_if_exception_type(AssertionError),
        reraise=True,
    )
    def get_and_validate_model_properties(model_urn):
        fetched_model_props = graph_client.get_aspect(model_urn, MLModelPropertiesClass)
        assert fetched_model_props is not None, (
            f"Failed to fetch ML Model properties for {model_urn}"
        )
        return fetched_model_props

    # Validate individual models
    for model_urn in model_urns:
        fetched_model_props = get_and_validate_model_properties(model_urn)
        assert fetched_model_props.name == f"Test Model ({model_urn})"
        assert fetched_model_props.description == f"Test model {model_urn}"
        assert str(model_group_urn) in (fetched_model_props.groups or [])
        assert fetched_model_props.trainingJobs == [
            "urn:li:dataProcessInstance:test_job"
        ]

    # Retry relationship validation with exponential backoff
    @tenacity.retry(
        stop=tenacity.stop_after_attempt(10),
        wait=tenacity.wait_exponential(multiplier=1, min=2, max=20),
        retry=tenacity.retry_if_exception_type(AssertionError),
        reraise=True,
    )
    def validate_relationships():
        related_models = set()
        for e in graph_client.get_related_entities(
            str(model_group_urn),
            relationship_types=["MemberOf"],
            direction=DataHubGraph.RelationshipDirection.INCOMING,
        ):
            related_models.add(e.urn)

        assert set(model_urns) == related_models, (
            f"Expected {set(model_urns)}, got {related_models}"
        )

    # Validate relationships between models and group
    validate_relationships()
