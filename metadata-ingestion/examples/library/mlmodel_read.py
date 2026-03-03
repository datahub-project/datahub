from datahub.metadata.urns import MlModelUrn
from datahub.sdk import DataHubClient

client = DataHubClient.from_env()

# Or get this from the UI (share -> copy urn) and use MlModelUrn.from_string(...)
mlmodel_urn = MlModelUrn(platform="mlflow", name="my-recommendations-model")

mlmodel_entity = client.entities.get(mlmodel_urn)
print("Model Name: ", mlmodel_entity.name)
print("Model Description: ", mlmodel_entity.description)
print("Model Group: ", mlmodel_entity.model_group)
print("Model Hyper Parameters: ", mlmodel_entity.hyper_params)
