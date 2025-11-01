from datahub.metadata.urns import MlModelGroupUrn
from datahub.sdk import DataHubClient

client = DataHubClient.from_env()

# Or get this from the UI (share -> copy urn) and use MlModelGroupUrn.from_string(...)
mlmodel_group_urn = MlModelGroupUrn(
    platform="mlflow", name="my-recommendations-model-group"
)

mlmodel_group_entity = client.entities.get(mlmodel_group_urn)
print("Model Group Name: ", mlmodel_group_entity.name)
print("Model Group Description: ", mlmodel_group_entity.description)
print("Model Group Custom Properties: ", mlmodel_group_entity.custom_properties)
