from typing import List

import datahub.emitter.mce_builder as builder
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.com.linkedin.pegasus2avro.dashboard import DashboardInfoClass
from datahub.metadata.schema_classes import ChangeAuditStampsClass, MLFeatureTablePropertiesClass

# Construct the DashboardInfo aspect with the charts -> dashboard lineage.
models: List[str] = [
    builder.make_ml_feature_table_urn(platform="feast", feature_table_name="user_features")
]

last_modified = ChangeAuditStampsClass()


model_info = MLFeatureTablePropertiesClass(
    downstreamJobs=models
)

# Construct a MetadataChangeProposalWrapper object with the DashboardInfo aspect.
# NOTE: This will overwrite all of the existing dashboard aspect information associated with this dashboard.
chart_info_mcp = MetadataChangeProposalWrapper(
    entityUrn=builder.make_ml_feature_table_urn(platform="feast", feature_table_name="user_analytics"),
    aspect=model_info,
)

# Create an emitter to the GMS REST API.
emitter = DatahubRestEmitter("http://localhost:8080")

# Emit metadata!
emitter.emit_mcp(chart_info_mcp)
