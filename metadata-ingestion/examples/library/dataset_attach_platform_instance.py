from datahub.sdk import DataHubClient, DatasetUrn

client = DataHubClient.from_env()

dataset = client.entities.get(
    DatasetUrn(
        platform="mysql",
        name="production-mysql-cluster.ecommerce.customers",
        env="PROD",
    )
)

dataset._set_platform_instance(platform="mysql", instance="production-mysql-cluster")

client.entities.update(dataset)

print("Attached platform instance 'production-mysql-cluster'")
print(f"to dataset {dataset.urn}")
