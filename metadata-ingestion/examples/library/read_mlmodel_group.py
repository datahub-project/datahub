from datahub.metadata.urns import MlModelGroupUrn
from datahub.sdk import DataHubClient
from datahub.sdk.mlmodelgroup import MLModelGroup

client = DataHubClient.from_env()

mlmodel_group = MLModelGroup(
    id="my-recommendations-model-group",
    name="My Recommendations Model Group",
    description="A group for recommendations models",
    platform="mlflow",
    custom_properties={
        "owner": "John Doe",
        "team": "recommendations",
        "domain": "marketing",
    },
)

client.entities.upsert(mlmodel_group)
mlmodel_group = client.entities.get(
    MlModelGroupUrn(platform="mlflow", name="my-recommendations-model-group")
)

print("Model Group Name: ", mlmodel_group.name)
print("Model Group Description: ", mlmodel_group.description)
print("Model Group Custom Properties: ", mlmodel_group.custom_properties)
