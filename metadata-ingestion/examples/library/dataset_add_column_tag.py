from datahub.sdk import DataHubClient, DatasetUrn, TagUrn

client = DataHubClient.from_env()

dataset = client.entities.get(
    DatasetUrn(platform="hive", name="fct_users_created", env="PROD")
)

dataset["user_name"].add_tag(TagUrn("deprecated"))

client.entities.update(dataset)
