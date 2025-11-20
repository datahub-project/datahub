import os
import time

from datahub.api.entities.datajob import DataFlow, DataJob
from datahub.api.entities.dataprocess.dataprocess_instance import (
    DataProcessInstance,
    InstanceRunResult,
)
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.utilities.urns.dataset_urn import DatasetUrn

emitter = DatahubRestEmitter(
    gms_server=os.getenv("DATAHUB_GMS_URL", "http://localhost:8080"),
    token=os.getenv("DATAHUB_GMS_TOKEN"),
)

# Define the DataFlow (Airflow DAG)
dataflow = DataFlow(
    orchestrator="airflow",
    id="etl_pipeline",
    env="prod",
    description="ETL pipeline with multiple tasks",
)

# Define DataJobs (Tasks in the DAG)
extract_job = DataJob(
    id="extract_data",
    flow_urn=dataflow.urn,
    description="Extract data from source",
)

transform_job = DataJob(
    id="transform_data",
    flow_urn=dataflow.urn,
    description="Transform extracted data",
    upstream_urns=[extract_job.urn],
)

# Create a DAG run instance (parent)
dag_run_id = "scheduled__2024-01-15T12:00:00+00:00"
dag_run_instance = DataProcessInstance.from_dataflow(dataflow=dataflow, id=dag_run_id)

# Track DAG run start
dag_start_time = int(time.time() * 1000)
dag_run_instance.emit_process_start(
    emitter=emitter,
    start_timestamp_millis=dag_start_time,
    attempt=1,
    emit_template=True,
    materialize_iolets=False,
)
print(f"DAG run started: {dag_run_instance.urn}")

# Create task instance for extract_data (child of DAG run)
extract_instance = DataProcessInstance.from_datajob(
    datajob=extract_job,
    id=f"{dag_run_id}__extract_data",
    clone_inlets=False,
    clone_outlets=False,
)
extract_instance.parent_instance = dag_run_instance.urn
extract_instance.inlets = [
    DatasetUrn.create_from_string(
        "urn:li:dataset:(urn:li:dataPlatform:postgres,raw_db.orders,PROD)"
    )
]
extract_instance.outlets = [
    DatasetUrn.create_from_string(
        "urn:li:dataset:(urn:li:dataPlatform:s3,staging/orders,PROD)"
    )
]

# Track extract task start
extract_start_time = int(time.time() * 1000)
extract_instance.emit_process_start(
    emitter=emitter,
    start_timestamp_millis=extract_start_time,
    attempt=1,
    emit_template=True,
    materialize_iolets=True,
)
print(f"  Extract task started: {extract_instance.urn}")

time.sleep(1)

# Track extract task completion
extract_end_time = int(time.time() * 1000)
extract_instance.emit_process_end(
    emitter=emitter,
    end_timestamp_millis=extract_end_time,
    result=InstanceRunResult.SUCCESS,
    attempt=1,
    start_timestamp_millis=extract_start_time,
)
print("  Extract task completed successfully")

# Create task instance for transform_data (child of DAG run, depends on extract)
transform_instance = DataProcessInstance.from_datajob(
    datajob=transform_job,
    id=f"{dag_run_id}__transform_data",
    clone_inlets=False,
    clone_outlets=False,
)
transform_instance.parent_instance = dag_run_instance.urn
transform_instance.upstream_urns = [extract_instance.urn]
transform_instance.inlets = [
    DatasetUrn.create_from_string(
        "urn:li:dataset:(urn:li:dataPlatform:s3,staging/orders,PROD)"
    )
]
transform_instance.outlets = [
    DatasetUrn.create_from_string(
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,analytics.orders,PROD)"
    )
]

# Track transform task start
transform_start_time = int(time.time() * 1000)
transform_instance.emit_process_start(
    emitter=emitter,
    start_timestamp_millis=transform_start_time,
    attempt=1,
    emit_template=True,
    materialize_iolets=True,
)
print(f"  Transform task started: {transform_instance.urn}")

time.sleep(1)

# Track transform task completion
transform_end_time = int(time.time() * 1000)
transform_instance.emit_process_end(
    emitter=emitter,
    end_timestamp_millis=transform_end_time,
    result=InstanceRunResult.SUCCESS,
    attempt=1,
    start_timestamp_millis=transform_start_time,
)
print("  Transform task completed successfully")

# Track DAG run completion
dag_end_time = int(time.time() * 1000)
dag_run_instance.emit_process_end(
    emitter=emitter,
    end_timestamp_millis=dag_end_time,
    result=InstanceRunResult.SUCCESS,
    attempt=1,
    start_timestamp_millis=dag_start_time,
)
print(f"DAG run completed successfully in {dag_end_time - dag_start_time}ms")
print("\nHierarchy:")
print(f"  DAG Run: {dag_run_instance.urn}")
print(f"    ├─ Extract Task: {extract_instance.urn}")
print(f"    └─ Transform Task: {transform_instance.urn} (depends on Extract)")
