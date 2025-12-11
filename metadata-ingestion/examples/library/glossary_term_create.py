# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

import os

from datahub.emitter.mce_builder import make_term_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import GlossaryTermInfoClass

# Get DataHub connection details from environment
gms_server = os.getenv("DATAHUB_GMS_URL", "http://localhost:8080")
token = os.getenv("DATAHUB_GMS_TOKEN")

# Create a term URN - the unique identifier for the glossary term
term_urn = make_term_urn("CustomerLifetimeValue")

# Define the term's core information
term_info = GlossaryTermInfoClass(
    name="Customer Lifetime Value",
    definition="The total revenue a business can expect from a single customer account throughout the business relationship. This metric helps prioritize customer retention efforts and marketing spend.",
    termSource="INTERNAL",
)

# Create a metadata change proposal
event = MetadataChangeProposalWrapper(
    entityUrn=term_urn,
    aspect=term_info,
)

# Emit the metadata
rest_emitter = DatahubRestEmitter(gms_server=gms_server, token=token)
rest_emitter.emit(event)
print(f"Created glossary term: {term_urn}")
