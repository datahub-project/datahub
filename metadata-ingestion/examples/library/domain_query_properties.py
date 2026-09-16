from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph
from datahub.metadata.schema_classes import (
    DomainPropertiesClass,
    GlobalTagsClass,
    GlossaryTermsClass,
    OwnershipClass,
)
from datahub.metadata.urns import DomainUrn

graph = DataHubGraph(DatahubClientConfig(server="http://localhost:8080"))

domain_urn = DomainUrn(id="marketing")

# Access domain properties
print(f"Domain URN: {domain_urn}")

properties = graph.get_aspect(str(domain_urn), DomainPropertiesClass)
if properties:
    print(f"Domain Name: {properties.name}")
    if properties.description:
        print(f"Domain Description: {properties.description}")
    if properties.parentDomain:
        print(f"Parent Domain: {properties.parentDomain}")

# Check ownership
ownership = graph.get_aspect(str(domain_urn), OwnershipClass)
if ownership and ownership.owners:
    print("Domain Owners:")
    for owner in ownership.owners:
        print(f"  - {owner.owner} ({owner.type})")

# Check tags
tags = graph.get_aspect(str(domain_urn), GlobalTagsClass)
if tags and tags.tags:
    print("Tags:")
    for tag in tags.tags:
        print(f"  - {tag.tag}")

# Check glossary terms
terms = graph.get_aspect(str(domain_urn), GlossaryTermsClass)
if terms and terms.terms:
    print("Glossary Terms:")
    for term in terms.terms:
        print(f"  - {term.urn}")
