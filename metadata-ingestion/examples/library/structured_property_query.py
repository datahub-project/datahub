from datahub.api.entities.structuredproperties.structuredproperties import (
    StructuredProperties,
)
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph

# Create a DataHub client
client = DataHubGraph(DatahubClientConfig(server="http://localhost:8080"))

# List all structured properties in the system
print("Listing all structured properties:")
print("-" * 80)

for structured_property in StructuredProperties.list(client):
    print(f"\nURN: {structured_property.urn}")
    print(f"Display Name: {structured_property.display_name}")
    print(f"Type: {structured_property.type}")
    print(f"Cardinality: {structured_property.cardinality}")
    print(f"Entity Types: {', '.join(structured_property.entity_types or [])}")

    if structured_property.allowed_values:
        print("Allowed Values:")
        for av in structured_property.allowed_values:
            print(f"  - {av.value}: {av.description}")

# Retrieve a specific structured property by URN
print("\n" + "=" * 80)
print("Retrieving specific structured property:")
print("-" * 80)

property_urn = "urn:li:structuredProperty:io.acryl.privacy.retentionTime"

try:
    specific_property = StructuredProperties.from_datahub(client, property_urn)

    print(f"\nURN: {specific_property.urn}")
    print(f"Qualified Name: {specific_property.qualified_name}")
    print(f"Display Name: {specific_property.display_name}")
    print(f"Description: {specific_property.description}")
    print(f"Type: {specific_property.type}")
    print(f"Cardinality: {specific_property.cardinality}")
    print(f"Immutable: {specific_property.immutable}")
    print(f"Entity Types: {', '.join(specific_property.entity_types or [])}")

    if specific_property.allowed_values:
        print("\nAllowed Values:")
        for av in specific_property.allowed_values:
            print(f"  - {av.value}: {av.description}")

    if specific_property.type_qualifier:
        print("\nType Qualifier - Allowed Entity Types:")
        for entity_type in specific_property.type_qualifier.allowed_types:
            print(f"  - {entity_type}")

except Exception as e:
    print(f"Error retrieving structured property: {e}")

# Example: List just the URNs (for scripting)
print("\n" + "=" * 80)
print("All structured property URNs:")
print("-" * 80)

for urn in StructuredProperties.list_urns(client):
    print(urn)
