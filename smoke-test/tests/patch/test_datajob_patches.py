import uuid

from datahub.emitter.mce_builder import make_data_job_urn, make_dataset_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import DataHubGraph, DataHubGraphConfig
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
def test_datajob_ownership_patch(wait_for_healthchecks):
    datajob_urn = _make_test_datajob_urn()
    helper_test_ownership_patch(datajob_urn, DataJobPatchBuilder)


# Tags
def test_datajob_tags_patch(wait_for_healthchecks):
    helper_test_dataset_tags_patch(
        _make_test_datajob_urn(), DataJobPatchBuilder
    )


# Terms
def test_dataset_terms_patch(wait_for_healthchecks):
    helper_test_entity_terms_patch(
        _make_test_datajob_urn(), DataJobPatchBuilder
    )


# Custom Properties
def test_custom_properties_patch(wait_for_healthchecks):
    orig_datajob_info = DataJobInfoClass(name="test_name", type="TestJobType")
    helper_test_custom_properties_patch(
        test_entity_urn=_make_test_datajob_urn(),
        patch_builder_class=DataJobPatchBuilder,
        custom_properties_aspect_class=DataJobInfoClass,
        base_aspect=orig_datajob_info,
    )


# Specific Aspect Patch Tests
# Input/Output
def test_datajob_inputoutput_dataset_patch(wait_for_healthchecks):
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

    with DataHubGraph(DataHubGraphConfig()) as graph:
        graph.emit_mcp(mcpw)
        inputoutput_lineage_read: DataJobInputOutputClass = graph.get_aspect(
            entity_urn=datajob_urn,
            aspect_type=DataJobInputOutputClass,
        )
        assert (
            inputoutput_lineage_read.inputDatasetEdges[0].destinationUrn
            == other_dataset_urn
        )

        for patch_mcp in (
            DataJobPatchBuilder(datajob_urn)
            .add_input_dataset(dataset_input_lineage_to_add)
            .build()
        ):
            graph.emit_mcp(patch_mcp)
            pass

        inputoutput_lineage_read = graph.get_aspect(
            entity_urn=datajob_urn,
            aspect_type=DataJobInputOutputClass,
        )
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
            DataJobPatchBuilder(datajob_urn)
            .remove_input_dataset(patch_dataset_urn)
            .build()
        ):
            graph.emit_mcp(patch_mcp)
            pass

        inputoutput_lineage_read = graph.get_aspect(
            entity_urn=datajob_urn,
            aspect_type=DataJobInputOutputClass,
        )
        assert len(inputoutput_lineage_read.inputDatasetEdges) == 1
        assert (
            inputoutput_lineage_read.inputDatasetEdges[0].destinationUrn
            == other_dataset_urn
        )
