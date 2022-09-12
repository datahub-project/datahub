from datahub.emitter.mce_builder import make_dataset_urn, make_user_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import RecordEnvelope
from datahub.ingestion.graph.client import DataHubGraph, DataHubGraphConfig
from datahub.ingestion.run.pipeline import PipelineContext
from datahub.metadata.schema_classes import (
    OwnerClass,
    OwnershipClass,
    OwnershipTypeClass,
)
from datahub.specific.dataset import DatasetPatchBuilder


def test_dataset_ownership_patch(wait_for_healthchecks):
    dataset_urn = make_dataset_urn(
        platform="hive", name="SampleHiveDataset", env="PROD"
    )
    owner_to_set = OwnerClass(
        owner=make_user_urn("jdoe"), type=OwnershipTypeClass.DATAOWNER
    )
    ownership_to_set = OwnershipClass(owners=[owner_to_set])

    owner_to_add = OwnerClass(
        owner=make_user_urn("gdoe"), type=OwnershipTypeClass.DATAOWNER
    )
    mcpw = MetadataChangeProposalWrapper(entityUrn=dataset_urn, aspect=ownership_to_set)
    dp_builder = DatasetPatchBuilder(dataset_urn)
    from datahub.ingestion.sink.file import FileSink, FileSinkConfig

    file_sink = FileSink(
        PipelineContext(run_id="test"),
        config=FileSinkConfig(filename="test_patch.json"),
    )
    with DataHubGraph(DataHubGraphConfig()) as graph:
        graph.emit_mcp(mcpw)
        owner = graph.get_aspect_v2(
            entity_urn=dataset_urn, aspect_type=OwnershipClass, aspect="ownership"
        )
        assert owner.owners[0].owner == make_user_urn("jdoe")

        for patch_mcp in dp_builder.add_owner(owner_to_add).build():
            file_sink.write_record_async(
                RecordEnvelope(record=patch_mcp, metadata={}), write_callback=None
            )
            graph.emit_mcp(patch_mcp)
            
        file_sink.close()
        owner = graph.get_aspect_v2(
            entity_urn=dataset_urn, aspect_type=OwnershipClass, aspect="ownership"
        )
        assert len(owner.owners) == 2
