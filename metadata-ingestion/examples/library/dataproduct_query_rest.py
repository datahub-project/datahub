from datahub.ingestion.graph.client import get_default_graph
from datahub.metadata.schema_classes import (
    DataProductPropertiesClass,
    DomainsClass,
    GlobalTagsClass,
    GlossaryTermsClass,
    OwnershipClass,
)
from datahub.metadata.urns import DataProductUrn

graph = get_default_graph()

data_product_urn = DataProductUrn("pet_of_the_week")

# get_entity_raw() cannot be used to test existence -- entitiesV2 synthesizes the key
# aspect for an entity that was never created, so it always returns a non-empty payload.
if not graph.exists(str(data_product_urn)):
    raise SystemExit(f"Data Product not found: {data_product_urn}")

print(f"Successfully retrieved Data Product: {data_product_urn}")

properties = graph.get_aspect(
    entity_urn=str(data_product_urn), aspect_type=DataProductPropertiesClass
)
if properties is not None:
    print(f"Name: {properties.name}")
    print(f"Description: {properties.description}")

    assets = properties.assets or []
    print(f"Number of assets: {len(assets)}")
    for asset in assets:
        print(f"  - Asset: {asset.destinationUrn} (Output Port: {asset.outputPort})")

domains = graph.get_aspect(entity_urn=str(data_product_urn), aspect_type=DomainsClass)
if domains is not None:
    print(f"Domain: {domains.domains}")

ownership = graph.get_aspect(
    entity_urn=str(data_product_urn), aspect_type=OwnershipClass
)
if ownership is not None:
    print(f"Number of owners: {len(ownership.owners)}")
    for owner in ownership.owners:
        print(f"  - Owner: {owner.owner} (Type: {owner.type})")

tags = graph.get_aspect(entity_urn=str(data_product_urn), aspect_type=GlobalTagsClass)
if tags is not None:
    print(f"Tags: {[t.tag for t in tags.tags]}")

terms = graph.get_aspect(
    entity_urn=str(data_product_urn), aspect_type=GlossaryTermsClass
)
if terms is not None:
    print(f"Glossary Terms: {[t.urn for t in terms.terms]}")
