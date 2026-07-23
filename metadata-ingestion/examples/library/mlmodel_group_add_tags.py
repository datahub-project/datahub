from datahub.metadata.urns import MlModelGroupUrn, TagUrn
from datahub.sdk import DataHubClient

client = DataHubClient.from_env()

group = client.entities.get(
    MlModelGroupUrn(platform="mlflow", name="recommendation-models", env="PROD")
)
group.add_tag(TagUrn("production-ready"))

client.entities.update(group)
