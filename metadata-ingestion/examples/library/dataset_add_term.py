from datahub.sdk import DataHubClient, DatasetUrn, GlossaryTermUrn

client = DataHubClient.from_env()

dataset = client.entities.get(
    DatasetUrn(platform="hive", name="realestate_db.sales", env="PROD")
)
dataset.add_term(GlossaryTermUrn("Classification.HighlyConfidential"))

client.entities.update(dataset)
