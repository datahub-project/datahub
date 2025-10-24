import os
import time

from datahub.api.entities.datajob import DataFlow
from datahub.api.entities.dataprocess.dataprocess_instance import (
    DataProcessInstance,
    InstanceRunResult,
)
from datahub.emitter.rest_emitter import DatahubRestEmitter

emitter = DatahubRestEmitter(
    gms_server=os.getenv("DATAHUB_GMS_URL", "http://localhost:8080"),
    token=os.getenv("DATAHUB_GMS_TOKEN"),
)

# Define the DataFlow (Airflow DAG)
dataflow = DataFlow(
    orchestrator="airflow",
    id="daily_reporting_pipeline",
    env="prod",
    description="Daily reporting pipeline that aggregates metrics",
)

# Create a DataProcessInstance for a specific DAG run
dag_run_instance = DataProcessInstance.from_dataflow(
    dataflow=dataflow, id="scheduled__2024-01-15T00:00:00+00:00"
)

# Set properties specific to this DAG run
dag_run_instance.properties = {
    "execution_date": "2024-01-15",
    "run_type": "scheduled",
    "external_trigger": "false",
}
dag_run_instance.url = "https://airflow.company.com/dags/daily_reporting_pipeline/grid?dag_run_id=scheduled__2024-01-15T00:00:00+00:00"

# Track DAG run start
start_time = int(time.time() * 1000)
dag_run_instance.emit_process_start(
    emitter=emitter,
    start_timestamp_millis=start_time,
    attempt=1,
    emit_template=True,
    materialize_iolets=True,
)
print(f"DAG run started: {dag_run_instance.urn}")

# Simulate DAG execution
time.sleep(3)

# Track DAG run completion
end_time = int(time.time() * 1000)
dag_run_instance.emit_process_end(
    emitter=emitter,
    end_timestamp_millis=end_time,
    result=InstanceRunResult.SUCCESS,
    result_type="airflow",
    attempt=1,
    start_timestamp_millis=start_time,
)
print(f"DAG run completed successfully in {end_time - start_time}ms")
