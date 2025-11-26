import os
import time

from datahub.api.entities.dataprocess.dataprocess_instance import (
    DataProcessInstance,
    InstanceRunResult,
)
from datahub.emitter.mcp_builder import ContainerKey
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import DataProcessTypeClass
from datahub.utilities.urns.dataset_urn import DatasetUrn

emitter = DatahubRestEmitter(
    gms_server=os.getenv("DATAHUB_GMS_URL", "http://localhost:8080"),
    token=os.getenv("DATAHUB_GMS_TOKEN"),
)

# Define the ML experiment container
experiment_container = ContainerKey(
    platform="urn:li:dataPlatform:mlflow",
    name="customer_churn_experiment",
    env="PROD",
)

# Create a process instance for a training run
training_run = DataProcessInstance.from_container(
    container_key=experiment_container, id="run_abc123def456"
)

# Set training-specific properties
training_run.type = DataProcessTypeClass.BATCH_AD_HOC
training_run.properties = {
    "model_type": "RandomForestClassifier",
    "hyperparameters": "n_estimators=100,max_depth=10",
    "framework": "scikit-learn",
    "framework_version": "1.3.0",
}
training_run.url = "https://mlflow.company.com/experiments/5/runs/abc123def456"

# Set training data inputs
training_run.inlets = [
    DatasetUrn.create_from_string(
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,ml.training_data,PROD)"
    )
]

# Set model output
training_run.outlets = [
    DatasetUrn.create_from_string(
        "urn:li:mlModel:(urn:li:dataPlatform:mlflow,customer_churn_model_v2,PROD)"
    )
]

# Track training start
start_time = int(time.time() * 1000)
training_run.emit_process_start(
    emitter=emitter,
    start_timestamp_millis=start_time,
    attempt=1,
    emit_template=False,
    materialize_iolets=True,
)
print("ML training run started...")

# Simulate training
time.sleep(5)

# Track training completion
end_time = int(time.time() * 1000)
training_run.emit_process_end(
    emitter=emitter,
    end_timestamp_millis=end_time,
    result=InstanceRunResult.SUCCESS,
    result_type="mlflow",
    attempt=1,
    start_timestamp_millis=start_time,
)
print(f"ML training completed in {(end_time - start_time) / 1000:.2f}s")
