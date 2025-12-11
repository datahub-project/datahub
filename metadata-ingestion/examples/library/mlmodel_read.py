# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

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
