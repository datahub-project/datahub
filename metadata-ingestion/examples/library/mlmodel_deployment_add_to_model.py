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
