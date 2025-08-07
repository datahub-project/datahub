from datahub.emitter import mce_builder
from datahub.ingestion.graph.client import DataHubGraph, DataHubGraphConfig
from datahub.metadata.schema_classes import (
    FineGrainedLineageClass as FineGrainedLineage,
    FineGrainedLineageDownstreamTypeClass as FineGrainedLineageDownstreamType,
    FineGrainedLineageUpstreamTypeClass as FineGrainedLineageUpstreamType,
)
from datahub.specific.datajob import DataJobPatchBuilder

# Create DataJobPatchBuilder instance
job_urn = mce_builder.make_data_job_urn("airflow", "data_pipeline", "transform_task")
patch_builder = DataJobPatchBuilder(job_urn)

# Add input and output datasets
patch_builder.add_input_dataset(
    mce_builder.make_dataset_urn("postgres", "raw_data.users")
)
patch_builder.add_output_dataset(
    mce_builder.make_dataset_urn("postgres", "analytics.user_metrics")
)

# Add input data job dependency
patch_builder.add_input_datajob(
    mce_builder.make_data_job_urn("airflow", "data_pipeline", "extract_users")
)

# Add input/output fields
patch_builder.add_input_dataset_field(
    mce_builder.make_schema_field_urn(
        mce_builder.make_dataset_urn("postgres", "raw_data.users"), "user_id"
    )
)
patch_builder.add_output_dataset_field(
    mce_builder.make_schema_field_urn(
        mce_builder.make_dataset_urn("postgres", "analytics.user_metrics"), "user_id"
    )
)

# Update column-level lineage through the Data Job
lineage1 = FineGrainedLineage(
    upstreamType=FineGrainedLineageUpstreamType.FIELD_SET,
    upstreams=[
        mce_builder.make_schema_field_urn(
            mce_builder.make_dataset_urn("postgres", "raw_data.users"), "user_id"
        )
    ],
    downstreamType=FineGrainedLineageDownstreamType.FIELD,
    downstreams=[
        mce_builder.make_schema_field_urn(
            mce_builder.make_dataset_urn("postgres", "analytics.user_metrics"),
            "user_id",
        )
    ],
    transformOperation="IDENTITY",
    confidenceScore=1.0,
)
patch_builder.add_fine_grained_lineage(lineage1)
patch_builder.remove_fine_grained_lineage(lineage1)
patch_builder.set_fine_grained_lineages(
    [lineage1]
)  # Replaces all existing fine-grained lineages

# Remove entities
patch_builder.remove_input_dataset(
    mce_builder.make_dataset_urn("postgres", "raw_data.users")
)

# Create DataHub Client
datahub_client = DataHubGraph(DataHubGraphConfig(server="http://localhost:8080"))

# Emit patches
for patch in patch_builder.build():
    datahub_client.emit(patch)
