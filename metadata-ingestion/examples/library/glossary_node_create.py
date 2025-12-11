# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

import logging
import os

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata._urns.urn_defs import GlossaryNodeUrn
from datahub.metadata.schema_classes import GlossaryNodeInfoClass

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# Create a GlossaryNode URN
node_urn = GlossaryNodeUrn("Finance")

# Create the glossary node info with definition and display name
node_info = GlossaryNodeInfoClass(
    definition="Category for all financial and accounting-related business terms including revenue, costs, and profitability measures.",
    name="Financial Metrics",
)

# Create metadata change proposal
event: MetadataChangeProposalWrapper = MetadataChangeProposalWrapper(
    entityUrn=str(node_urn),
    aspect=node_info,
)

# Emit to DataHub
gms_server = os.getenv("DATAHUB_GMS_URL", "http://localhost:8080")
token = os.getenv("DATAHUB_GMS_TOKEN")
rest_emitter = DatahubRestEmitter(gms_server=gms_server, token=token)
rest_emitter.emit(event)

log.info(f"Created glossary node {node_urn}")
