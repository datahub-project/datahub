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
