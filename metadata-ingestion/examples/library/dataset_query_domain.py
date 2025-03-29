from datahub.sdk import DataHubClient, DatasetUrn

client = DataHubClient.from_env()

dataset = client.entities.get(
    DatasetUrn(platform="hive", name="fct_users_created", env="PROD")
)

# Print the dataset domain
print(dataset.domain)
