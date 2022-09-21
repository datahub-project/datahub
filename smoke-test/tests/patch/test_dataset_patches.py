import uuid

from datahub.emitter.mce_builder import make_dataset_urn, make_user_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import RecordEnvelope
from datahub.ingestion.graph.client import DataHubGraph, DataHubGraphConfig
from datahub.ingestion.run.pipeline import PipelineContext
from datahub.metadata.schema_classes import (
    DatasetLineageTypeClass,
    OwnerClass,
    OwnershipClass,
    OwnershipTypeClass,
    UpstreamClass,
    UpstreamLineageClass,
)
from datahub.specific.dataset import DatasetPatchBuilder


def test_dataset_ownership_patch(wait_for_healthchecks):
    dataset_urn = make_dataset_urn(
        platform="hive", name=f"SampleHiveDataset{uuid.uuid4()}", env="PROD"
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
    with DataHubGraph(DataHubGraphConfig()) as graph:
        graph.emit_mcp(mcpw)
        owner = graph.get_aspect_v2(
            entity_urn=dataset_urn, aspect_type=OwnershipClass, aspect="ownership"
        )
        assert owner.owners[0].owner == make_user_urn("jdoe")

        for patch_mcp in dp_builder.add_owner(owner_to_add).build():
            graph.emit_mcp(patch_mcp)

        owner = graph.get_aspect_v2(
            entity_urn=dataset_urn, aspect_type=OwnershipClass, aspect="ownership"
        )
        assert len(owner.owners) == 2

        for patch_mcp in dp_builder.remove_owner(make_user_urn("gdoe")).build():
            graph.emit_mcp(patch_mcp)

        owner = graph.get_aspect_v2(
            entity_urn=dataset_urn, aspect_type=OwnershipClass, aspect="ownership"
        )
        assert len(owner.owners) == 1
        assert owner.owners[0].owner == make_user_urn("jdoe")


def test_dataset_upstream_lineage_patch(wait_for_healthchecks):
    dataset_urn = make_dataset_urn(
        platform="hive", name=f"SampleHiveDataset-{uuid.uuid4()}", env="PROD"
    )

    other_dataset_urn = make_dataset_urn(
        platform="hive", name=f"SampleHiveDataset2-{uuid.uuid4()}", env="PROD"
    )

    patch_dataset_urn = make_dataset_urn(
        platform="hive", name=f"SampleHiveDataset3-{uuid.uuid4()}", env="PROD"
    )

    upstream_lineage = UpstreamLineageClass(
        upstreams=[
            UpstreamClass(dataset=other_dataset_urn, type=DatasetLineageTypeClass.VIEW)
        ]
    )
    upstream_lineage_to_add = UpstreamClass(
        dataset=patch_dataset_urn, type=DatasetLineageTypeClass.VIEW
    )
    mcpw = MetadataChangeProposalWrapper(entityUrn=dataset_urn, aspect=upstream_lineage)

    dp_builder = DatasetPatchBuilder(dataset_urn)
    with DataHubGraph(DataHubGraphConfig()) as graph:
        graph.emit_mcp(mcpw)
        upstream_lineage_read = graph.get_aspect_v2(
            entity_urn=dataset_urn,
            aspect_type=UpstreamLineageClass,
            aspect="upstreamLineage",
        )
        assert upstream_lineage_read.upstreams[0].dataset == other_dataset_urn

        for patch_mcp in dp_builder.add_upstream_lineage(
            upstream_lineage_to_add
        ).build():
            graph.emit_mcp(patch_mcp)
            pass

        upstream_lineage_read = graph.get_aspect_v2(
            entity_urn=dataset_urn,
            aspect_type=UpstreamLineageClass,
            aspect="upstreamLineage",
        )
        assert len(upstream_lineage_read.upstreams) == 2
        assert upstream_lineage_read.upstreams[0].dataset == other_dataset_urn
        assert upstream_lineage_read.upstreams[1].dataset == patch_dataset_urn

        for patch_mcp in dp_builder.remove_upstream_lineage(
            upstream_lineage_to_add
        ).build():
            graph.emit_mcp(patch_mcp)
            pass

        upstream_lineage_read = graph.get_aspect_v2(
            entity_urn=dataset_urn,
            aspect_type=UpstreamLineageClass,
            aspect="upstreamLineage",
        )
        assert len(upstream_lineage_read.upstreams) == 1
        assert upstream_lineage_read.upstreams[0].dataset == other_dataset_urn
