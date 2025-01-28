from datahub import DataHubClient, DatasetUrn, TagUrn

# Reads config from DATAHUB_GMS_URL and DATAHUB_GMS_TOKEN, or from ~/.datahubenv.
client = DataHubClient.from_env()

dataset = client.get(
    DatasetUrn(platform="hive", name="realestate_db.sales", env="PROD")
)

dataset.set_tags([TagUrn("purchase")])

client.put(dataset)
