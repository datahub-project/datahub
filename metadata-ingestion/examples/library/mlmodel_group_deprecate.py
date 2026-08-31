from datetime import datetime

import datahub.metadata.schema_classes as models
from datahub.sdk import DataHubClient, MlModelGroupUrn

client = DataHubClient.from_env()

group_urn = MlModelGroupUrn(
    platform="mlflow",
    name="legacy-recommendation-models",
    env="PROD",
)

mlmodel_group = client.entities.get(group_urn)

deprecation_aspect = models.DeprecationClass(
    deprecated=True,
    note="This model group has been replaced by the new transformer-based recommendation models",
    decommissionTime=int(datetime.now().timestamp() * 1000),
    actor="urn:li:corpuser:datahub",
)

mlmodel_group._set_aspect(deprecation_aspect)

client.entities.update(mlmodel_group)
print(f"Deprecated ML model group: {group_urn}")
