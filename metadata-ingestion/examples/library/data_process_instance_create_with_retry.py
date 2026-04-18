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

emitter = DatahubRestEmitter(
    gms_server=os.getenv("DATAHUB_GMS_URL", "http://localhost:8080"),
    token=os.getenv("DATAHUB_GMS_TOKEN"),
)

parent_job_urn = DataJobUrn.create_from_string(
    "urn:li:dataJob:(urn:li:dataFlow:(airflow,etl_pipeline,prod),load_customer_data)"
)

instance = DataProcessInstance(
    id="scheduled__2024-01-15T14:30:00+00:00",
    orchestrator="airflow",
    cluster="prod",
    template_urn=parent_job_urn,
    type=DataProcessTypeClass.BATCH_SCHEDULED,
    inlets=[
        DatasetUrn.create_from_string(
            "urn:li:dataset:(urn:li:dataPlatform:s3,customer_exports,PROD)"
        )
    ],
    outlets=[
        DatasetUrn.create_from_string(
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,analytics.customers,PROD)"
        )
    ],
)

# First attempt
start_time_attempt1 = int(time.time() * 1000)
instance.emit_process_start(
    emitter=emitter,
    start_timestamp_millis=start_time_attempt1,
    attempt=1,
    emit_template=True,
    materialize_iolets=True,
)
print("Attempt 1 started...")

time.sleep(1)

# First attempt fails
end_time_attempt1 = int(time.time() * 1000)
instance.emit_process_end(
    emitter=emitter,
    end_timestamp_millis=end_time_attempt1,
    result=InstanceRunResult.UP_FOR_RETRY,
    result_type="airflow",
    attempt=1,
    start_timestamp_millis=start_time_attempt1,
)
print("Attempt 1 failed, will retry...")

time.sleep(2)

# Second attempt (retry)
start_time_attempt2 = int(time.time() * 1000)
instance.emit_process_start(
    emitter=emitter,
    start_timestamp_millis=start_time_attempt2,
    attempt=2,
    emit_template=False,
    materialize_iolets=False,
)
print("Attempt 2 started (retry)...")

time.sleep(1)

# Second attempt succeeds
end_time_attempt2 = int(time.time() * 1000)
instance.emit_process_end(
    emitter=emitter,
    end_timestamp_millis=end_time_attempt2,
    result=InstanceRunResult.SUCCESS,
    result_type="airflow",
    attempt=2,
    start_timestamp_millis=start_time_attempt2,
)
print("Attempt 2 succeeded!")
