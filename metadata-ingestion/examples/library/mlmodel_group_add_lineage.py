from datahub.emitter import mce_builder
from datahub.metadata.urns import MlModelGroupUrn
from datahub.sdk import DataHubClient

client = DataHubClient.from_env()

group_urn = MlModelGroupUrn(
    platform="mlflow",
    name="recommendation-models",
    env="PROD",
)

training_job_urn = mce_builder.make_data_job_urn(
    orchestrator="airflow",
    flow_id="train_recommendation_model",
    job_id="training_task",
)

group = client.entities.get(group_urn)

group.add_training_job(training_job_urn)

client.entities.update(group)

print(f"Added training job {training_job_urn} to ML model group {group_urn}")
