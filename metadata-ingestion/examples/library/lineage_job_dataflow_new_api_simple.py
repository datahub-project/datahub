from datetime import datetime
import uuid

from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import RunResultTypeClass
from datahub.api.dataflow import DataFlow
from datahub.api.datajob import DataJob
from datahub.api.dataprocess_instance import DataProcessInstance

emitter = DatahubRestEmitter("http://localhost:8080")

jobFlow = DataFlow(cluster="prod", orchestrator="airflow", id="flow_api_simple")
jobFlow.emit(emitter)

# Flowurn as constructor
dataJob = DataJob(flow_urn=jobFlow.urn, id="job1", name="My Job 1")
dataJob.properties["custom_properties"] = "test"
dataJob.emit(emitter)

dataJob2 = DataJob(flow_urn=jobFlow.urn, id="job2", name="My Job 2")
dataJob2.input_datajob_urns.append(dataJob.urn)
dataJob2.tags.add("TestTag")
dataJob2.owners.add("test@test.com")
dataJob2.emit(emitter)

dataJob3 = DataJob(flow_urn=jobFlow.urn, id="job3", name="My Job 3")
dataJob3.input_datajob_urns.append(dataJob.urn)
dataJob3.emit(emitter)

dataJob4 = DataJob(flow_urn=jobFlow.urn, id="job4", name="My Job 4")
dataJob4.input_datajob_urns.append(dataJob2.urn)
dataJob4.input_datajob_urns.append(dataJob3.urn)
dataJob4.emit(emitter)

jobFlowRun = DataProcessInstance.from_dataflow(dataflow=jobFlow, id=f"{jobFlow.id}-{uuid.uuid4()}")
jobFlowRun.emit_process_start(emitter, int(datetime.utcnow().timestamp() * 1000))

jobRun = DataProcessInstance.from_datajob(datajob=dataJob, id=f"{jobFlow.id}-{uuid.uuid4()}")
jobRun.emit_process_start(emitter, int(datetime.utcnow().timestamp() * 1000))
jobRun.emit_process_end(emitter, int(datetime.utcnow().timestamp() * 1000), result=RunResultTypeClass.SUCCESS)

job2Run = DataProcessInstance.from_datajob(datajob=dataJob2, id=f"{jobFlow.id}-{uuid.uuid4()}")
job2Run.emit_process_start(emitter, int(datetime.utcnow().timestamp() * 1000))
job2Run.emit_process_end(emitter, int(datetime.utcnow().timestamp() * 1000), result=RunResultTypeClass.SUCCESS)

job3Run = DataProcessInstance.from_datajob(datajob=dataJob3, id=f"{jobFlow.id}-{uuid.uuid4()}")
job3Run.emit_process_start(emitter, int(datetime.utcnow().timestamp() * 1000))
job3Run.emit_process_end(emitter, int(datetime.utcnow().timestamp() * 1000), result=RunResultTypeClass.SUCCESS)

job4Run = DataProcessInstance.from_datajob(datajob=dataJob4, id=f"{jobFlow.id}-{uuid.uuid4()}")
job4Run.emit_process_start(emitter, int(datetime.utcnow().timestamp() * 1000))
job4Run.emit_process_end(emitter, int(datetime.utcnow().timestamp() * 1000), result=RunResultTypeClass.SUCCESS)

jobFlowRun.emit_process_end(emitter, int(datetime.utcnow().timestamp() * 1000), result=RunResultTypeClass.SUCCESS)