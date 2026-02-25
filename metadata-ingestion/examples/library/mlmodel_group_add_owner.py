from datahub.sdk import CorpUserUrn, DataHubClient, MlModelGroupUrn

client = DataHubClient.from_env()

group = client.entities.get(
    MlModelGroupUrn(platform="mlflow", name="recommendation-models", env="PROD")
)

group.add_owner(CorpUserUrn("data_science_team"))

client.entities.update(group)
