from typing import List

import datahub.emitter.mce_builder as builder
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.com.linkedin.pegasus2avro.chart import ChartInfoClass
from datahub.metadata.schema_classes import ChangeAuditStampsClass

# Construct the ChartInfo aspect with the input_datasets lineage.
input_datasets: List[str] = [
    builder.make_dataset_urn(platform="hdfs", name="dataset1", env="PROD"),
    builder.make_dataset_urn(platform="hdfs", name="dataset2", env="PROD"),
]

last_modified = ChangeAuditStampsClass()

chart_info = ChartInfoClass(
    title="Baz Chart 1",
    description="Sample Baz chart",
    lastModified=last_modified,
    inputs=input_datasets,
)

# Construct a MetadataChangeProposalWrapper object with the ChartInfo aspect.
# NOTE: This will overwrite all of the existing chartInfo aspect information associated with this chart.
chart_info_mcp = MetadataChangeProposalWrapper(
    entityUrn=builder.make_chart_urn(platform="looker", name="my_chart_1"),
    aspect=chart_info,
)

# Create an emitter to the GMS REST API.
emitter = DatahubRestEmitter("http://localhost:8080")

# Emit metadata!
emitter.emit_mcp(chart_info_mcp)
