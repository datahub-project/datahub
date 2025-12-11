# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

from datahub.api.entities.structuredproperties.structuredproperties import (
    StructuredProperties,
    TypeQualifierAllowedTypes,
)
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph

# Create a DataHub client
client = DataHubGraph(DatahubClientConfig(server="http://localhost:8080"))

# Define a structured property that references DataHub entities
# This property can only reference CorpUser or CorpGroup entities
data_steward_property = StructuredProperties(
    id="io.acryl.governance.dataSteward",
    qualified_name="io.acryl.governance.dataSteward",
    display_name="Data Steward",
    type="urn",
    description="The designated data steward responsible for this asset's governance and quality",
    entity_types=["dataset", "dashboard", "chart", "dataJob"],
    cardinality="SINGLE",
    type_qualifier=TypeQualifierAllowedTypes(
        allowed_types=[
            "urn:li:entityType:datahub.corpuser",
            "urn:li:entityType:datahub.corpGroup",
        ]
    ),
)

# Emit the structured property to DataHub
for mcp in data_steward_property.generate_mcps():
    client.emit_mcp(mcp)

print(f"Created structured property: {data_steward_property.urn}")

# Example: Create a multi-value property for related datasets
related_datasets_property = StructuredProperties(
    id="io.acryl.lineage.relatedDatasets",
    qualified_name="io.acryl.lineage.relatedDatasets",
    display_name="Related Datasets",
    type="urn",
    description="Other datasets that are semantically or functionally related to this asset",
    entity_types=["dataset"],
    cardinality="MULTIPLE",
    type_qualifier=TypeQualifierAllowedTypes(
        allowed_types=["urn:li:entityType:datahub.dataset"]
    ),
)

# Emit the second structured property
for mcp in related_datasets_property.generate_mcps():
    client.emit_mcp(mcp)

print(f"Created structured property: {related_datasets_property.urn}")
