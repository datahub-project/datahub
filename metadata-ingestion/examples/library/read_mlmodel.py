from datahub.metadata.urns import MlModelGroupUrn, MlModelUrn
from datahub.sdk import DataHubClient
from datahub.sdk.mlmodel import MLModel

client = DataHubClient.from_env()

mlmodel = MLModel(
    id="my-recommendations-model",
    name="My Recommendations Model",
    description="A model for recommending products to users",
    platform="mlflow",
    model_group=MlModelGroupUrn(platform="mlflow", name="my-recommendations-model"),
    hyper_params={
        "learning_rate": "0.01",
        "num_epochs": "100",
        "batch_size": "32",
    },
)

client.entities.upsert(mlmodel)

mlmodel = client.entities.get(
    MlModelUrn(platform="mlflow", name="my-recommendations-model")
)

print("Model Name: ", mlmodel.name)
print("Model Description: ", mlmodel.description)
print("Model Group: ", mlmodel.model_group)
print("Model Hyper Parameters: ", mlmodel.hyper_params)
