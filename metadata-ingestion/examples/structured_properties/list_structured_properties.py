# Usage: python3 list_structured_properties.py
# Expected Output: List of structured properties
# This script lists all structured properties in DataHub
from datahub.api.entities.structuredproperties.structuredproperties import (
    StructuredProperties,
)
from datahub.ingestion.graph.client import get_default_graph

with get_default_graph() as graph:
    structuredproperties = StructuredProperties.list(graph)
    for structuredproperty in structuredproperties:
        print(structuredproperty.dict())
