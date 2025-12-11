# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

from datahub.emitter import mce_builder
from datahub.metadata.urns import MlModelUrn
from datahub.sdk import DataHubClient

client = DataHubClient.from_env()

mlmodel_urn = MlModelUrn(platform="sagemaker", name="recommendation-model")

deployment_urn = mce_builder.make_ml_model_deployment_urn(
    platform="sagemaker",
    deployment_name="recommendation-endpoint",
    env="PROD",
)

mlmodel = client.entities.get(mlmodel_urn)
mlmodel.add_deployment(deployment_urn)

client.entities.update(mlmodel)
