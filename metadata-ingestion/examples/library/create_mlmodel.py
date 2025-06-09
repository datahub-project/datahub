from datahub.sdk import DataHubClient
from datahub.sdk.mlmodel import MLModel

client = DataHubClient.from_env()

mlmodel = MLModel(
    id="forecast-model",
    name="Forecast Model",
    platform="mlflow",
)

client.entities.upsert(mlmodel)
