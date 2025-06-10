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
    training_metrics=[
        models.MLMetricClass(
            name="accuracy", description="accuracy of the model", value="1.0"
        ),
    ],
    hyper_params=[
        models.MLHyperParamClass(name="hyper_1", description="hyper_1", value="0.102"),
    ],
)

client.entities.update(mlmodel)
