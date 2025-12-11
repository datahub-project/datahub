# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

from datahub.metadata.urns import MlModelGroupUrn
from datahub.sdk import DataHubClient
from datahub.sdk.mlmodel import MLModel

client = DataHubClient.from_env()

mlmodel = MLModel(
    id="customer-churn-predictor",
    name="Customer Churn Prediction Model",
    platform="mlflow",
    description="A gradient boosting model that predicts customer churn based on usage patterns and engagement metrics",
    custom_properties={
        "framework": "xgboost",
        "framework_version": "1.7.0",
        "model_format": "pickle",
    },
    model_group=MlModelGroupUrn(platform="mlflow", name="customer-churn-models"),
)

client.entities.upsert(mlmodel)
