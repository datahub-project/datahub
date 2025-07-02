from datahub.metadata.urns import DatasetUrn, DomainUrn
from datahub.sdk import DataHubClient

client = DataHubClient.from_env()

dataset = client.entities.get(DatasetUrn(platform="snowflake", name="example_dataset"))

# if you don't know the domain id, you can get it from resolve client by name
# domain_urn = client.resolve.domain(name="marketing")

# NOTE : This will overwrite the existing domain
dataset.set_domain(DomainUrn(id="marketing"))

client.entities.update(dataset)
