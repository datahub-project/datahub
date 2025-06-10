from datahub.metadata.urns import MlModelUrn
from datahub.sdk import DataHubClient

client = DataHubClient.from_env()

mlmodel = client.entities.get(
    MlModelUrn(platform="mlflow", name="my-recommendations-model")
)

print(mlmodel)
