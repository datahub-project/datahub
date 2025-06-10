from datahub.metadata.urns import DatasetUrn, DomainUrn
from datahub.sdk import DataHubClient

client = DataHubClient.from_env()

dataset = client.entities.get(DatasetUrn(platform="hive", name="fct_users_created"))
# NOTE : this will overwrite the existing domain
dataset.set_domain(DomainUrn(id="marketing"))

client.entities.upsert(dataset)
