import datahub.emitter.mce_builder as builder
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import (
    DatasetLineageTypeClass,
    UpstreamClass,
    UpstreamLineageClass,
)

# Construct a lineage object.
lineage_mcp = MetadataChangeProposalWrapper(
    entityUrn=builder.make_dataset_urn("hive", "logging_events"),  # Downstream
    aspect=UpstreamLineageClass(
        upstreams=[
            UpstreamClass(
                dataset=builder.make_dataset_urn("hive", "fct_users_deleted"),
                type=DatasetLineageTypeClass.TRANSFORMED,
            ),
        ]
    ),
)

# Create an emitter to the GMS REST API.
emitter = DatahubRestEmitter("http://localhost:8080")

# Emit metadata!
emitter.emit(lineage_mcp)
