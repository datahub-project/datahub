from datahub.metadata.urns import MlModelGroupUrn
from datahub.sdk import DataHubClient
from datahub.sdk.mlmodel import MLModel

client = DataHubClient.from_env()

model = MLModel(
    id="forecast-model",
    platform="mlflow",
)

model.set_model_group(
    MlModelGroupUrn(
        platform="mlflow",
        name="forecast-model-group",
    )
)

client.entities.upsert(model)
