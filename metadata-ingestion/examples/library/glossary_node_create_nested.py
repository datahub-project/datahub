import logging
import os

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata._urns.urn_defs import GlossaryNodeUrn
from datahub.metadata.schema_classes import GlossaryNodeInfoClass

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# First, ensure the parent node exists (Finance)
parent_node_urn = GlossaryNodeUrn("Finance")
parent_node_info = GlossaryNodeInfoClass(
    definition="Top-level category for financial metrics and terms",
    name="Finance",
)

parent_event = MetadataChangeProposalWrapper(
    entityUrn=str(parent_node_urn),
    aspect=parent_node_info,
)

# Create a nested child node under Finance
child_node_urn = GlossaryNodeUrn("RevenueMetrics")
child_node_info = GlossaryNodeInfoClass(
    definition="Metrics related to revenue recognition and reporting",
    name="Revenue Metrics",
    parentNode=str(parent_node_urn),  # Set the parent relationship
)

child_event = MetadataChangeProposalWrapper(
    entityUrn=str(child_node_urn),
    aspect=child_node_info,
)

# Emit both to DataHub
rest_emitter = DatahubRestEmitter(
    gms_server=os.getenv("DATAHUB_GMS_URL", "http://localhost:8080"),
    token=os.getenv("DATAHUB_GMS_TOKEN"),
)
rest_emitter.emit(parent_event)
rest_emitter.emit(child_event)

log.info(f"Created parent glossary node {parent_node_urn}")
log.info(f"Created child glossary node {child_node_urn} under {parent_node_urn}")
