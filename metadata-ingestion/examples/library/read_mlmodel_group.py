from datahub.metadata.urns import MlModelGroupUrn
from datahub.sdk import DataHubClient

client = DataHubClient.from_env()

mlmodel_group = client.entities.get(
    MlModelGroupUrn(platform="mlflow", name="my-recommendations-model-group")
)

print(mlmodel_group)
