from datahub.metadata.schema_classes import DomainsClass
from datahub.sdk import DataHubClient, DatasetUrn

client = DataHubClient.from_env()

dataset = client.entities.get(DatasetUrn(platform="snowflake", name="example_dataset"))

# Remove the dataset from its current domain by setting an empty domains list
dataset._set_aspect(DomainsClass(domains=[]))

client.entities.update(dataset)
