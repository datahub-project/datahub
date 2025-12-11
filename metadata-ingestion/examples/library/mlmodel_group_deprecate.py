# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

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
