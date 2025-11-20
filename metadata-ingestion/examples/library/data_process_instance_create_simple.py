import os
import time

from datahub.api.entities.dataprocess.dataprocess_instance import (
    DataProcessInstance,
    InstanceRunResult,
)
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import DataProcessTypeClass
from datahub.utilities.urns.data_job_urn import DataJobUrn
from datahub.utilities.urns.dataset_urn import DatasetUrn

# Create REST emitter
emitter = DatahubRestEmitter(
    gms_server=os.getenv("DATAHUB_GMS_URL", "http://localhost:8080"),
    token=os.getenv("DATAHUB_GMS_TOKEN"),
)

# Define the parent DataJob that this instance is executing
parent_job_urn = DataJobUrn.create_from_string(
    "urn:li:dataJob:(urn:li:dataFlow:(airflow,sales_pipeline,prod),process_sales_data)"
)

# Create a process instance for a specific execution
# This might represent an Airflow task run on 2024-01-15 at 10:00:00
instance = DataProcessInstance(
    id="scheduled__2024-01-15T10:00:00+00:00",
    orchestrator="airflow",
    cluster="prod",
    template_urn=parent_job_urn,
    type=DataProcessTypeClass.BATCH_SCHEDULED,
    properties={
        "airflow_version": "2.7.0",
        "executor": "CeleryExecutor",
        "pool": "default_pool",
    },
    url="https://airflow.company.com/dags/sales_pipeline/grid?dag_run_id=scheduled__2024-01-15T10:00:00+00:00&task_id=process_sales_data",
    inlets=[
        DatasetUrn.create_from_string(
            "urn:li:dataset:(urn:li:dataPlatform:postgres,sales_db.raw_orders,PROD)"
        )
    ],
    outlets=[
        DatasetUrn.create_from_string(
            "urn:li:dataset:(urn:li:dataPlatform:postgres,sales_db.processed_orders,PROD)"
        )
    ],
)

# Record the start of execution
start_time = int(time.time() * 1000)
instance.emit_process_start(
    emitter=emitter,
    start_timestamp_millis=start_time,
    attempt=1,
    emit_template=True,
    materialize_iolets=True,
)

print(f"Started tracking process instance: {instance.urn}")

# Simulate process execution
print("Process is running...")
time.sleep(2)

# Record the end of execution
end_time = int(time.time() * 1000)
instance.emit_process_end(
    emitter=emitter,
    end_timestamp_millis=end_time,
    result=InstanceRunResult.SUCCESS,
    result_type="airflow",
    attempt=1,
    start_timestamp_millis=start_time,
)

print("Completed tracking process instance with result: SUCCESS")
print(f"Duration: {end_time - start_time}ms")
