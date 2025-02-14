import time
import uuid

import datahub.metadata.schema_classes as models
from datahub.emitter.mce_builder import make_data_job_urn, make_dataset_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.schema_classes import (
    DataJobInfoClass,
    DataJobInputOutputClass,
    EdgeClass,
)
from datahub.specific.datajob import DataJobPatchBuilder
from tests.patch.common_patch_tests import (
    helper_test_custom_properties_patch,
    helper_test_dataset_tags_patch,
    helper_test_entity_terms_patch,
    helper_test_ownership_patch,
)


def _make_test_datajob_urn(
    seedFlow: str = "SampleAirflowDag", seedTask: str = "SampleAirflowTask"
):
    return make_data_job_urn(
        orchestrator="airflow",
        flow_id=f"{seedFlow}{uuid.uuid4()}",
        job_id=f"{seedTask}{uuid.uuid4()}",
    )


# Common Aspect Patch Tests
# Ownership
def test_datajob_ownership_patch(graph_client):
    datajob_urn = _make_test_datajob_urn()
    helper_test_ownership_patch(graph_client, datajob_urn, DataJobPatchBuilder)


# Tags
def test_datajob_tags_patch(graph_client):
    helper_test_dataset_tags_patch(
        graph_client, _make_test_datajob_urn(), DataJobPatchBuilder
    )


# Terms
def test_dataset_terms_patch(graph_client):
    helper_test_entity_terms_patch(
        graph_client, _make_test_datajob_urn(), DataJobPatchBuilder
    )


# Custom Properties
def test_custom_properties_patch(graph_client):
    orig_datajob_info = DataJobInfoClass(name="test_name", type="TestJobType")
    helper_test_custom_properties_patch(
        graph_client,
        test_entity_urn=_make_test_datajob_urn(),
        patch_builder_class=DataJobPatchBuilder,
        custom_properties_aspect_class=DataJobInfoClass,
        base_aspect=orig_datajob_info,
    )


# Specific Aspect Patch Tests
# Input/Output
def test_datajob_inputoutput_dataset_patch(graph_client):
    datajob_urn = _make_test_datajob_urn()

    other_dataset_urn = make_dataset_urn(
        platform="hive", name=f"SampleHiveDataset2-{uuid.uuid4()}", env="PROD"
    )

    patch_dataset_urn = make_dataset_urn(
        platform="hive", name=f"SampleHiveDataset3-{uuid.uuid4()}", env="PROD"
    )

    inputoutput_lineage = DataJobInputOutputClass(
        inputDatasets=[],
        outputDatasets=[],
        inputDatasetEdges=[EdgeClass(destinationUrn=other_dataset_urn)],
    )
    dataset_input_lineage_to_add = EdgeClass(destinationUrn=patch_dataset_urn)
    mcpw = MetadataChangeProposalWrapper(
        entityUrn=datajob_urn, aspect=inputoutput_lineage
    )

    graph_client.emit_mcp(mcpw)
    inputoutput_lineage_read = graph_client.get_aspect(
        entity_urn=datajob_urn,
        aspect_type=DataJobInputOutputClass,
    )
    assert inputoutput_lineage_read is not None
    assert inputoutput_lineage_read.inputDatasetEdges is not None
    assert (
        inputoutput_lineage_read.inputDatasetEdges[0].destinationUrn
        == other_dataset_urn
    )

    for patch_mcp in (
        DataJobPatchBuilder(datajob_urn)
        .add_input_dataset(dataset_input_lineage_to_add)
        .build()
    ):
        graph_client.emit_mcp(patch_mcp)
        pass

    inputoutput_lineage_read = graph_client.get_aspect(
        entity_urn=datajob_urn,
        aspect_type=DataJobInputOutputClass,
    )
    assert inputoutput_lineage_read is not None
    assert inputoutput_lineage_read.inputDatasetEdges is not None
    assert len(inputoutput_lineage_read.inputDatasetEdges) == 2
    assert (
        inputoutput_lineage_read.inputDatasetEdges[0].destinationUrn
        == other_dataset_urn
    )
    assert (
        inputoutput_lineage_read.inputDatasetEdges[1].destinationUrn
        == patch_dataset_urn
    )

    for patch_mcp in (
        DataJobPatchBuilder(datajob_urn).remove_input_dataset(patch_dataset_urn).build()
    ):
        graph_client.emit_mcp(patch_mcp)
        pass

    inputoutput_lineage_read = graph_client.get_aspect(
        entity_urn=datajob_urn,
        aspect_type=DataJobInputOutputClass,
    )
    assert inputoutput_lineage_read is not None
    assert inputoutput_lineage_read.inputDatasetEdges is not None
    assert len(inputoutput_lineage_read.inputDatasetEdges) == 1
    assert (
        inputoutput_lineage_read.inputDatasetEdges[0].destinationUrn
        == other_dataset_urn
    )


def test_datajob_multiple_inputoutput_dataset_patch(graph_client):
    """Test creating a data job with multiple input and output datasets and verifying the aspects."""
    # Create the data job
    datajob_urn = "urn:li:dataJob:(urn:li:dataFlow:(airflow,training,default),training)"

    # Create input and output dataset URNs
    input_datasets = ["input_data_1", "input_data_2"]
    output_datasets = ["output_data_1", "output_data_2"]

    input_dataset_urns = [
        make_dataset_urn(platform="s3", name=f"test_patch_{dataset}", env="PROD")
        for dataset in input_datasets
    ]
    output_dataset_urns = [
        make_dataset_urn(platform="s3", name=f"test_patch_{dataset}", env="PROD")
        for dataset in output_datasets
    ]

    # Create edges for datasets
    def make_edge(urn, generate_auditstamp=False):
        audit_stamp = models.AuditStampClass(
            time=int(time.time() * 1000.0),
            actor="urn:li:corpuser:datahub",
        )
        return EdgeClass(
            destinationUrn=str(urn),
            lastModified=audit_stamp if generate_auditstamp else None,
        )

    # Initialize empty input/output lineage
    initial_lineage = DataJobInputOutputClass(
        inputDatasets=[], outputDatasets=[], inputDatasetEdges=[], outputDatasetEdges=[]
    )

    # Emit initial lineage
    mcpw = MetadataChangeProposalWrapper(entityUrn=datajob_urn, aspect=initial_lineage)
    graph_client.emit_mcp(mcpw)

    # Create patches for input and output datasets
    patch_builder = DataJobPatchBuilder(datajob_urn)
    for input_urn in input_dataset_urns:
        patch_builder.add_input_dataset(make_edge(input_urn))
    for output_urn in output_dataset_urns:
        patch_builder.add_output_dataset(make_edge(output_urn))

    # Apply patches
    for patch_mcp in patch_builder.build():
        graph_client.emit_mcp(patch_mcp)

    # Verify the lineage was correctly applied
    lineage_aspect = graph_client.get_aspect(
        entity_urn=datajob_urn,
        aspect_type=DataJobInputOutputClass,
    )

    # Assert lineage was created
    assert lineage_aspect is not None
    assert lineage_aspect.inputDatasetEdges is not None
    assert lineage_aspect.outputDatasetEdges is not None

    # Verify input datasets
    assert len(lineage_aspect.inputDatasetEdges) == len(input_datasets)
    input_urns = {edge.destinationUrn for edge in lineage_aspect.inputDatasetEdges}
    expected_input_urns = {str(urn) for urn in input_dataset_urns}
    assert input_urns == expected_input_urns

    # Verify output datasets
    assert len(lineage_aspect.outputDatasetEdges) == len(output_datasets)
    output_urns = {edge.destinationUrn for edge in lineage_aspect.outputDatasetEdges}
    expected_output_urns = {str(urn) for urn in output_dataset_urns}
    assert output_urns == expected_output_urns

    # Test updating the same datasets again (idempotency)
    patch_builder = DataJobPatchBuilder(datajob_urn)
    for input_urn in input_dataset_urns:
        patch_builder.add_input_dataset(make_edge(input_urn))
    for output_urn in output_dataset_urns:
        patch_builder.add_output_dataset(make_edge(output_urn))

    for patch_mcp in patch_builder.build():
        graph_client.emit_mcp(patch_mcp)

    # Verify the aspect hasn't changed
    updated_lineage_aspect = graph_client.get_aspect(
        entity_urn=datajob_urn,
        aspect_type=DataJobInputOutputClass,
    )

    assert updated_lineage_aspect is not None
    assert updated_lineage_aspect.to_obj() == lineage_aspect.to_obj()
