from datahub.emitter.mce_builder import make_dataset_urn, make_schema_field_urn
from datahub.ingestion.graph.client import DataHubGraph, DataHubGraphConfig
from datahub.metadata.schema_classes import (
    DatasetLineageTypeClass,
    FineGrainedLineageClass,
    FineGrainedLineageUpstreamTypeClass,
    UpstreamClass,
)
from datahub.specific.dataset import DatasetPatchBuilder

# Create DataHub Client
datahub_client = DataHubGraph(DataHubGraphConfig(server="http://localhost:8080"))

# Create Dataset URN
dataset_urn = make_dataset_urn(
    platform="snowflake", name="fct_users_created", env="PROD"
)
upstream_to_remove_urn = make_dataset_urn(
    platform="s3", name="fct_users_old", env="PROD"
)
upstream_to_add_urn = make_dataset_urn(platform="s3", name="fct_users_new", env="PROD")

# Create Dataset Patch to Add & Remove Upstream Lineage Edges
patch_builder = DatasetPatchBuilder(dataset_urn)
patch_builder.remove_upstream_lineage(upstream_to_remove_urn)
patch_builder.add_upstream_lineage(
    UpstreamClass(upstream_to_add_urn, DatasetLineageTypeClass.TRANSFORMED)
)

# ...And also include schema field lineage
upstream_field_to_add_urn = make_schema_field_urn(upstream_to_add_urn, "profile_id")
downstream_field_to_add_urn = make_schema_field_urn(dataset_urn, "profile_id")

patch_builder.add_fine_grained_upstream_lineage(
    FineGrainedLineageClass(
        FineGrainedLineageUpstreamTypeClass.FIELD_SET,
        FineGrainedLineageUpstreamTypeClass.FIELD_SET,
        [upstream_field_to_add_urn],
        [downstream_field_to_add_urn],
    )
)

upstream_field_to_remove_urn = make_schema_field_urn(
    upstream_to_remove_urn, "profile_id"
)
downstream_field_to_remove_urn = make_schema_field_urn(dataset_urn, "profile_id")

patch_builder.remove_fine_grained_upstream_lineage(
    FineGrainedLineageClass(
        FineGrainedLineageUpstreamTypeClass.FIELD_SET,
        FineGrainedLineageUpstreamTypeClass.FIELD_SET,
        [upstream_field_to_remove_urn],
        [downstream_field_to_remove_urn],
    )
)

patch_mcps = patch_builder.build()


# Emit Dataset Patch
for patch_mcp in patch_mcps:
    datahub_client.emit(patch_mcp)
