from typing import List

import datahub.emitter.mce_builder as builder
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.com.linkedin.pegasus2avro.dashboard import DashboardInfoClass
from datahub.metadata.schema_classes import ChangeAuditStampsClass

# Construct the DashboardInfo aspect with the charts -> dashboard lineage.
charts_in_dashboard: List[str] = [
    builder.make_chart_urn(platform="looker", name="chart_1"),
    builder.make_chart_urn(platform="looker", name="chart_2"),
]

last_modified = ChangeAuditStampsClass()


dashboard_info = DashboardInfoClass(
    title="My Dashboard 1",
    description="Sample dashboard",
    lastModified=last_modified,
    charts=charts_in_dashboard,
)

# Construct a MetadataChangeProposalWrapper object with the DashboardInfo aspect.
# NOTE: This will overwrite all of the existing dashboard aspect information associated with this dashboard.
chart_info_mcp = MetadataChangeProposalWrapper(
    entityUrn=builder.make_dashboard_urn(platform="looker", name="my_dashboard_1"),
    aspect=dashboard_info,
)

# Create an emitter to the GMS REST API.
emitter = DatahubRestEmitter("http://localhost:8080")

# Emit metadata!
emitter.emit_mcp(chart_info_mcp)
