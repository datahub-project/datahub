# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

import datahub.metadata.schema_classes as models
from datahub.metadata.urns import MlFeatureUrn, MlModelGroupUrn
from datahub.sdk import DataHubClient
from datahub.sdk.mlmodel import MLModel

client = DataHubClient.from_env()

mlmodel = MLModel(
    id="my-recommendations-model",
    name="My Recommendations Model",
    platform="mlflow",
    model_group=MlModelGroupUrn(
        platform="mlflow", name="my-recommendations-model-group"
    ),
    custom_properties={
        "framework": "pytorch",
    },
    extra_aspects=[
        models.MLModelPropertiesClass(
            mlFeatures=[
                str(
                    MlFeatureUrn(
                        feature_namespace="users_feature_table", name="user_signup_date"
                    )
                ),
                str(
                    MlFeatureUrn(
                        feature_namespace="users_feature_table",
                        name="user_last_active_date",
                    )
                ),
            ]
        )
    ],
    training_metrics={
        "accuracy": "1.0",
        "precision": "0.95",
        "recall": "0.90",
        "f1_score": "0.92",
    },
    hyper_params={
        "learning_rate": "0.01",
        "num_epochs": "100",
        "batch_size": "32",
    },
)

client.entities.update(mlmodel)
