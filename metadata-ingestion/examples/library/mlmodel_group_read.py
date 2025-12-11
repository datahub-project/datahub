# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

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
