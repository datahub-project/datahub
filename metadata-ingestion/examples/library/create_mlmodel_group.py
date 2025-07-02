from datahub.sdk import DataHubClient
from datahub.sdk.mlmodelgroup import MLModelGroup

client = DataHubClient.from_env()

mlmodel_group = MLModelGroup(
    id="my-recommendations-model-group",
    name="My Recommendations Model Group",
    platform="mlflow",
    description="Grouping of ml model related to home page recommendations",
    custom_properties={
        "framework": "pytorch",
    },
)

client.entities.update(mlmodel_group)
