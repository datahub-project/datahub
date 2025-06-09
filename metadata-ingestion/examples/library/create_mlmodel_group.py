from datahub.sdk import DataHubClient
from datahub.sdk.mlmodelgroup import MLModelGroup

client = DataHubClient.from_env()

mlmodel_group = MLModelGroup(
    id="forecast-model-group",
    name="Forecast Model Group",
    platform="mlflow",
)

client.entities.upsert(mlmodel_group)
