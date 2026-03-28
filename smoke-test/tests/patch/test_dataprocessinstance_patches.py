# ABOUTME: Smoke tests for DataProcessInstance patch operations.
# ABOUTME: Tests input/output lineage patching via Python SDK v2.
import uuid

import pytest

from datahub.emitter.mce_builder import make_dataset_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.schema_classes import (
    DataProcessInstanceInputClass,
    DataProcessInstanceOutputClass,
    EdgeClass,
)
from datahub.specific.dataprocessinstance import DataProcessInstancePatchBuilder


def _make_test_dataprocessinstance_urn():
    return f"urn:li:dataProcessInstance:test-instance-{uuid.uuid4()}"


# Input lineage
@pytest.mark.parametrize(
    "client_fixture_name", ["graph_client", "openapi_graph_client"]
)
def test_dataprocessinstance_input_patch(request, client_fixture_name):
    """Test adding and removing inputs using patch operations."""
    graph_client = request.getfixturevalue(client_fixture_name)
    dpi_urn = _make_test_dataprocessinstance_urn()

    initial_input_urn = make_dataset_urn(
        platform="hive", name=f"SampleHiveDataset-{uuid.uuid4()}", env="PROD"
    )
    patch_input_urn = make_dataset_urn(
        platform="hive", name=f"SampleHiveDataset2-{uuid.uuid4()}", env="PROD"
    )

    # Create initial input aspect
    initial_input = DataProcessInstanceInputClass(
        inputs=[initial_input_urn],
    )
    mcpw = MetadataChangeProposalWrapper(entityUrn=dpi_urn, aspect=initial_input)
    graph_client.emit_mcp(mcpw)

    # Verify initial state
    input_read = graph_client.get_aspect(
        entity_urn=dpi_urn,
        aspect_type=DataProcessInstanceInputClass,
    )
    assert input_read is not None
    assert input_read.inputs is not None
    assert initial_input_urn in input_read.inputs

    # Add another input using patch
    for patch_mcp in (
        DataProcessInstancePatchBuilder(dpi_urn).add_input(patch_input_urn).build()
    ):
        graph_client.emit_mcp(patch_mcp)

    input_read = graph_client.get_aspect(
        entity_urn=dpi_urn,
        aspect_type=DataProcessInstanceInputClass,
    )
    assert input_read is not None
    assert input_read.inputs is not None
    assert len(input_read.inputs) == 2
    assert initial_input_urn in input_read.inputs
    assert patch_input_urn in input_read.inputs

    # Remove the patched input
    for patch_mcp in (
        DataProcessInstancePatchBuilder(dpi_urn).remove_input(patch_input_urn).build()
    ):
        graph_client.emit_mcp(patch_mcp)

    input_read = graph_client.get_aspect(
        entity_urn=dpi_urn,
        aspect_type=DataProcessInstanceInputClass,
    )
    assert input_read is not None
    assert input_read.inputs is not None
    assert len(input_read.inputs) == 1
    assert initial_input_urn in input_read.inputs


# Input Edge lineage
@pytest.mark.parametrize(
    "client_fixture_name", ["graph_client", "openapi_graph_client"]
)
def test_dataprocessinstance_input_edge_patch(request, client_fixture_name):
    """Test adding and removing input edges using patch operations."""
    graph_client = request.getfixturevalue(client_fixture_name)
    dpi_urn = _make_test_dataprocessinstance_urn()

    initial_input_urn = make_dataset_urn(
        platform="hive", name=f"SampleHiveDataset-{uuid.uuid4()}", env="PROD"
    )
    patch_input_urn = make_dataset_urn(
        platform="hive", name=f"SampleHiveDataset2-{uuid.uuid4()}", env="PROD"
    )

    # Create initial input edges aspect
    initial_input = DataProcessInstanceInputClass(
        inputs=[],
        inputEdges=[EdgeClass(destinationUrn=initial_input_urn)],
    )
    mcpw = MetadataChangeProposalWrapper(entityUrn=dpi_urn, aspect=initial_input)
    graph_client.emit_mcp(mcpw)

    # Verify initial state
    input_read = graph_client.get_aspect(
        entity_urn=dpi_urn,
        aspect_type=DataProcessInstanceInputClass,
    )
    assert input_read is not None
    assert input_read.inputEdges is not None
    assert input_read.inputEdges[0].destinationUrn == initial_input_urn

    # Add another input edge using patch
    for patch_mcp in (
        DataProcessInstancePatchBuilder(dpi_urn)
        .add_input_edge(EdgeClass(destinationUrn=patch_input_urn))
        .build()
    ):
        graph_client.emit_mcp(patch_mcp)

    input_read = graph_client.get_aspect(
        entity_urn=dpi_urn,
        aspect_type=DataProcessInstanceInputClass,
    )
    assert input_read is not None
    assert input_read.inputEdges is not None
    assert len(input_read.inputEdges) == 2
    edge_urns = {edge.destinationUrn for edge in input_read.inputEdges}
    assert initial_input_urn in edge_urns
    assert patch_input_urn in edge_urns

    # Remove the patched input edge
    for patch_mcp in (
        DataProcessInstancePatchBuilder(dpi_urn)
        .remove_input_edge(patch_input_urn)
        .build()
    ):
        graph_client.emit_mcp(patch_mcp)

    input_read = graph_client.get_aspect(
        entity_urn=dpi_urn,
        aspect_type=DataProcessInstanceInputClass,
    )
    assert input_read is not None
    assert input_read.inputEdges is not None
    assert len(input_read.inputEdges) == 1
    assert input_read.inputEdges[0].destinationUrn == initial_input_urn


# Output lineage
@pytest.mark.parametrize(
    "client_fixture_name", ["graph_client", "openapi_graph_client"]
)
def test_dataprocessinstance_output_patch(request, client_fixture_name):
    """Test adding and removing outputs using patch operations."""
    graph_client = request.getfixturevalue(client_fixture_name)
    dpi_urn = _make_test_dataprocessinstance_urn()

    initial_output_urn = make_dataset_urn(
        platform="hive", name=f"SampleHiveDataset-{uuid.uuid4()}", env="PROD"
    )
    patch_output_urn = make_dataset_urn(
        platform="hive", name=f"SampleHiveDataset2-{uuid.uuid4()}", env="PROD"
    )

    # Create initial output aspect
    initial_output = DataProcessInstanceOutputClass(
        outputs=[initial_output_urn],
    )
    mcpw = MetadataChangeProposalWrapper(entityUrn=dpi_urn, aspect=initial_output)
    graph_client.emit_mcp(mcpw)

    # Verify initial state
    output_read = graph_client.get_aspect(
        entity_urn=dpi_urn,
        aspect_type=DataProcessInstanceOutputClass,
    )
    assert output_read is not None
    assert output_read.outputs is not None
    assert initial_output_urn in output_read.outputs

    # Add another output using patch
    for patch_mcp in (
        DataProcessInstancePatchBuilder(dpi_urn).add_output(patch_output_urn).build()
    ):
        graph_client.emit_mcp(patch_mcp)

    output_read = graph_client.get_aspect(
        entity_urn=dpi_urn,
        aspect_type=DataProcessInstanceOutputClass,
    )
    assert output_read is not None
    assert output_read.outputs is not None
    assert len(output_read.outputs) == 2
    assert initial_output_urn in output_read.outputs
    assert patch_output_urn in output_read.outputs

    # Remove the patched output
    for patch_mcp in (
        DataProcessInstancePatchBuilder(dpi_urn).remove_output(patch_output_urn).build()
    ):
        graph_client.emit_mcp(patch_mcp)

    output_read = graph_client.get_aspect(
        entity_urn=dpi_urn,
        aspect_type=DataProcessInstanceOutputClass,
    )
    assert output_read is not None
    assert output_read.outputs is not None
    assert len(output_read.outputs) == 1
    assert initial_output_urn in output_read.outputs


# Output Edge lineage
@pytest.mark.parametrize(
    "client_fixture_name", ["graph_client", "openapi_graph_client"]
)
def test_dataprocessinstance_output_edge_patch(request, client_fixture_name):
    """Test adding and removing output edges using patch operations."""
    graph_client = request.getfixturevalue(client_fixture_name)
    dpi_urn = _make_test_dataprocessinstance_urn()

    initial_output_urn = make_dataset_urn(
        platform="hive", name=f"SampleHiveDataset-{uuid.uuid4()}", env="PROD"
    )
    patch_output_urn = make_dataset_urn(
        platform="hive", name=f"SampleHiveDataset2-{uuid.uuid4()}", env="PROD"
    )

    # Create initial output edges aspect
    initial_output = DataProcessInstanceOutputClass(
        outputs=[],
        outputEdges=[EdgeClass(destinationUrn=initial_output_urn)],
    )
    mcpw = MetadataChangeProposalWrapper(entityUrn=dpi_urn, aspect=initial_output)
    graph_client.emit_mcp(mcpw)

    # Verify initial state
    output_read = graph_client.get_aspect(
        entity_urn=dpi_urn,
        aspect_type=DataProcessInstanceOutputClass,
    )
    assert output_read is not None
    assert output_read.outputEdges is not None
    assert output_read.outputEdges[0].destinationUrn == initial_output_urn

    # Add another output edge using patch
    for patch_mcp in (
        DataProcessInstancePatchBuilder(dpi_urn)
        .add_output_edge(EdgeClass(destinationUrn=patch_output_urn))
        .build()
    ):
        graph_client.emit_mcp(patch_mcp)

    output_read = graph_client.get_aspect(
        entity_urn=dpi_urn,
        aspect_type=DataProcessInstanceOutputClass,
    )
    assert output_read is not None
    assert output_read.outputEdges is not None
    assert len(output_read.outputEdges) == 2
    edge_urns = {edge.destinationUrn for edge in output_read.outputEdges}
    assert initial_output_urn in edge_urns
    assert patch_output_urn in edge_urns

    # Remove the patched output edge
    for patch_mcp in (
        DataProcessInstancePatchBuilder(dpi_urn)
        .remove_output_edge(patch_output_urn)
        .build()
    ):
        graph_client.emit_mcp(patch_mcp)

    output_read = graph_client.get_aspect(
        entity_urn=dpi_urn,
        aspect_type=DataProcessInstanceOutputClass,
    )
    assert output_read is not None
    assert output_read.outputEdges is not None
    assert len(output_read.outputEdges) == 1
    assert output_read.outputEdges[0].destinationUrn == initial_output_urn


# Multiple operations test
@pytest.mark.parametrize(
    "client_fixture_name", ["graph_client", "openapi_graph_client"]
)
def test_dataprocessinstance_multiple_input_output_patch(request, client_fixture_name):
    """Test adding multiple inputs and outputs in a single patch operation."""
    graph_client = request.getfixturevalue(client_fixture_name)
    dpi_urn = _make_test_dataprocessinstance_urn()

    input_urns = [
        make_dataset_urn(platform="hive", name=f"input_{i}_{uuid.uuid4()}", env="PROD")
        for i in range(3)
    ]
    output_urns = [
        make_dataset_urn(platform="hive", name=f"output_{i}_{uuid.uuid4()}", env="PROD")
        for i in range(3)
    ]

    # Add multiple inputs and outputs using patch
    patch_builder = DataProcessInstancePatchBuilder(dpi_urn)
    for input_urn in input_urns:
        patch_builder.add_input_edge(EdgeClass(destinationUrn=input_urn))
    for output_urn in output_urns:
        patch_builder.add_output_edge(EdgeClass(destinationUrn=output_urn))

    for patch_mcp in patch_builder.build():
        graph_client.emit_mcp(patch_mcp)

    # Verify inputs
    input_read = graph_client.get_aspect(
        entity_urn=dpi_urn,
        aspect_type=DataProcessInstanceInputClass,
    )
    assert input_read is not None
    assert input_read.inputEdges is not None
    assert len(input_read.inputEdges) == 3
    input_edge_urns = {edge.destinationUrn for edge in input_read.inputEdges}
    for input_urn in input_urns:
        assert input_urn in input_edge_urns

    # Verify outputs
    output_read = graph_client.get_aspect(
        entity_urn=dpi_urn,
        aspect_type=DataProcessInstanceOutputClass,
    )
    assert output_read is not None
    assert output_read.outputEdges is not None
    assert len(output_read.outputEdges) == 3
    output_edge_urns = {edge.destinationUrn for edge in output_read.outputEdges}
    for output_urn in output_urns:
        assert output_urn in output_edge_urns


# Idempotency test
@pytest.mark.parametrize(
    "client_fixture_name", ["graph_client", "openapi_graph_client"]
)
def test_dataprocessinstance_patch_idempotency(request, client_fixture_name):
    """Test that patching the same values multiple times is idempotent."""
    graph_client = request.getfixturevalue(client_fixture_name)
    dpi_urn = _make_test_dataprocessinstance_urn()

    input_urn = make_dataset_urn(
        platform="hive", name=f"idempotent_input_{uuid.uuid4()}", env="PROD"
    )

    # Apply the same patch twice
    for _ in range(2):
        for patch_mcp in (
            DataProcessInstancePatchBuilder(dpi_urn)
            .add_input_edge(EdgeClass(destinationUrn=input_urn))
            .build()
        ):
            graph_client.emit_mcp(patch_mcp)

    # Verify only one input was added
    input_read = graph_client.get_aspect(
        entity_urn=dpi_urn,
        aspect_type=DataProcessInstanceInputClass,
    )
    assert input_read is not None
    assert input_read.inputEdges is not None
    assert len(input_read.inputEdges) == 1
    assert input_read.inputEdges[0].destinationUrn == input_urn
