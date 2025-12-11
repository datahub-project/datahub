# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

import os

from datahub.api.entities.structuredproperties.structuredproperties import (
    AllowedValue,
    StructuredProperties,
)
from datahub.ingestion.graph.client import DataHubGraph, DataHubGraphConfig

# Create a DataHub client
client = DataHubGraph(
    DataHubGraphConfig(
        server=os.getenv("DATAHUB_GMS_URL", "http://localhost:8080"),
        token=os.getenv("DATAHUB_GMS_TOKEN"),
    )
)

# Define a structured property for data retention time
retention_property = StructuredProperties(
    id="io.acryl.privacy.retentionTime",
    qualified_name="io.acryl.privacy.retentionTime",
    display_name="Retention Time",
    type="number",
    description="Number of days to retain data based on privacy and compliance requirements",
    entity_types=["dataset", "dataFlow"],
    cardinality="SINGLE",
    allowed_values=[
        AllowedValue(
            value=30.0,
            description="30 days - for ephemeral data containing PII",
        ),
        AllowedValue(
            value=90.0,
            description="90 days - for monthly reporting data with PII",
        ),
        AllowedValue(
            value=365.0,
            description="365 days - for non-sensitive data",
        ),
    ],
)

# Emit the structured property to DataHub
for mcp in retention_property.generate_mcps():
    client.emit(mcp)

print(f"Created structured property: {retention_property.urn}")
