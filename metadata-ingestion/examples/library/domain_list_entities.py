from datahub.sdk import DataHubClient, DomainUrn
from datahub.sdk.search_filters import FilterDsl as F

client = DataHubClient.from_env()

domain_urn = DomainUrn(id="marketing")

# Search for all entities in the domain

results = list(client.search.get_urns(filter=F.domain(str(domain_urn))))

print(f"Found {len(results)} entities in domain {domain_urn}")
for entity_urn in results:
    print(f"  - {entity_urn}")

# You can also search for specific entity types within a domain
dataset_results = list(
    client.search.get_urns(
        filter=F.and_(F.domain(str(domain_urn)), F.entity_type("dataset"))
    )
)

print(f"\nFound {len(dataset_results)} datasets in domain {domain_urn}")
for dataset_urn in dataset_results:
    print(f"  - {dataset_urn}")
