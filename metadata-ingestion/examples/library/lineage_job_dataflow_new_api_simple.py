import uuid
from datetime import datetime, timezone

from datahub.api.entities.datajob import DataFlow, DataJob
from datahub.api.entities.dataprocess.dataprocess_instance import (
    DataProcessInstance,
    InstanceRunResult,
)
from datahub.emitter.rest_emitter import DatahubRestEmitter

emitter = DatahubRestEmitter("http://localhost:8080")

jobFlow = DataFlow(env="prod", orchestrator="airflow", id="flow_api_simple")
jobFlow.emit(emitter)

# Flowurn as constructor
dataJob = DataJob(flow_urn=jobFlow.urn, id="job1", name="My Job 1")
dataJob.properties["custom_properties"] = "test"
dataJob.emit(emitter)

dataJob2 = DataJob(flow_urn=jobFlow.urn, id="job2", name="My Job 2")
dataJob2.upstream_urns.append(dataJob.urn)
dataJob2.tags.add("TestTag")
dataJob2.owners.add("test@test.com")
dataJob2.emit(emitter)

dataJob3 = DataJob(flow_urn=jobFlow.urn, id="job3", name="My Job 3")
dataJob3.upstream_urns.append(dataJob.urn)
dataJob3.emit(emitter)

dataJob4 = DataJob(flow_urn=jobFlow.urn, id="job4", name="My Job 4")
dataJob4.upstream_urns.append(dataJob2.urn)
dataJob4.upstream_urns.append(dataJob3.urn)
dataJob4.emit(emitter)

jobFlowRun = DataProcessInstance.from_dataflow(
    dataflow=jobFlow, id=f"{jobFlow.id}-{uuid.uuid4()}"
)
jobFlowRun.emit_process_start(
    emitter, int(datetime.now(timezone.utc).timestamp() * 1000)
)


jobRun = DataProcessInstance.from_datajob(
    datajob=dataJob, id=f"{jobFlow.id}-{uuid.uuid4()}"
)
jobRun.emit_process_start(emitter, int(datetime.now(timezone.utc).timestamp() * 1000))

jobRun.emit_process_end(
    emitter,
    int(datetime.now(timezone.utc).timestamp() * 1000),
    result=InstanceRunResult.SUCCESS,
)


job2Run = DataProcessInstance.from_datajob(
    datajob=dataJob2, id=f"{jobFlow.id}-{uuid.uuid4()}"
)
job2Run.emit_process_start(emitter, int(datetime.now(timezone.utc).timestamp() * 1000))

job2Run.emit_process_end(
    emitter,
    int(datetime.now(timezone.utc).timestamp() * 1000),
    result=InstanceRunResult.SUCCESS,
)


job3Run = DataProcessInstance.from_datajob(
    datajob=dataJob3, id=f"{jobFlow.id}-{uuid.uuid4()}"
)
job3Run.emit_process_start(emitter, int(datetime.now(timezone.utc).timestamp() * 1000))

job3Run.emit_process_end(
    emitter,
    int(datetime.now(timezone.utc).timestamp() * 1000),
    result=InstanceRunResult.SUCCESS,
)


job4Run = DataProcessInstance.from_datajob(
    datajob=dataJob4, id=f"{jobFlow.id}-{uuid.uuid4()}"
)
job4Run.emit_process_start(emitter, int(datetime.now(timezone.utc).timestamp() * 1000))

job4Run.emit_process_end(
    emitter,
    int(datetime.now(timezone.utc).timestamp() * 1000),
    result=InstanceRunResult.SUCCESS,
)


jobFlowRun.emit_process_end(
    emitter,
    int(datetime.now(timezone.utc).timestamp() * 1000),
    result=InstanceRunResult.SUCCESS,
)
