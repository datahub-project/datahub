import time
import uuid

from datahub.api.entities.corpgroup.corpgroup import CorpGroup
from datahub.api.entities.corpuser.corpuser import CorpUser
from datahub.api.entities.datajob.dataflow import DataFlow
from datahub.api.entities.datajob.datajob import DataJob
from datahub.api.entities.dataprocess.dataprocess_instance import (
    DataProcessInstance,
    InstanceRunResult,
)
from datahub.emitter.rest_emitter import DatahubRestEmitter

emitter = DatahubRestEmitter("http://localhost:8080")

jobFlow = DataFlow(cluster="prod", orchestrator="airflow", id="flow2")
jobFlow.emit(emitter)

# Flowurn as constructor
dataJob = DataJob(flow_urn=jobFlow.urn, id="job1", name="My Job 1")
dataJob.properties["custom_properties"] = "test"
dataJob.emit(emitter)

dataJob2 = DataJob(flow_urn=jobFlow.urn, id="job2", name="My Job 2")
dataJob2.upstream_urns.append(dataJob.urn)
dataJob2.tags.add("TestTag")
dataJob2.owners.add("testUser")
dataJob2.emit(emitter)

dataJob3 = DataJob(flow_urn=jobFlow.urn, id="job3", name="My Job 3")
dataJob3.upstream_urns.append(dataJob.urn)
dataJob3.emit(emitter)

dataJob4 = DataJob(flow_urn=jobFlow.urn, id="job4", name="My Job 4")
dataJob4.upstream_urns.append(dataJob2.urn)
dataJob4.upstream_urns.append(dataJob3.urn)
dataJob4.group_owners.add("testGroup")
dataJob4.emit(emitter)

# Hello World
jobFlowRun: DataProcessInstance = DataProcessInstance(
    orchestrator="airflow", cluster="prod", id=f"{jobFlow.id}-{uuid.uuid4()}"
)
jobRun1: DataProcessInstance = DataProcessInstance(
    orchestrator="airflow",
    cluster="prod",
    id=f"{jobFlow.id}-{dataJob.id}-{uuid.uuid4()}",
)
jobRun1.parent_instance = jobFlowRun.urn
jobRun1.template_urn = dataJob.urn
jobRun1.emit_process_start(
    emitter=emitter, start_timestamp_millis=int(time.time() * 1000), emit_template=False
)
jobRun1.emit_process_end(
    emitter=emitter,
    end_timestamp_millis=int(time.time() * 1000),
    result=InstanceRunResult.SUCCESS,
)

jobRun2: DataProcessInstance = DataProcessInstance(
    orchestrator="airflow",
    cluster="prod",
    id=f"{jobFlow.id}-{dataJob2.id}-{uuid.uuid4()}",
)
jobRun2.template_urn = dataJob2.urn
jobRun2.parent_instance = jobFlowRun.urn
jobRun2.upstream_urns = [jobRun1.urn]
jobRun2.emit_process_start(
    emitter=emitter, start_timestamp_millis=int(time.time() * 1000), emit_template=False
)
jobRun2.emit_process_end(
    emitter=emitter,
    end_timestamp_millis=int(time.time() * 1000),
    result=InstanceRunResult.SUCCESS,
)


jobRun3: DataProcessInstance = DataProcessInstance(
    orchestrator="airflow",
    cluster="prod",
    id=f"{jobFlow.id}-{dataJob3.id}-{uuid.uuid4()}",
)
jobRun3.parent_instance = jobFlowRun.urn
jobRun3.template_urn = dataJob3.urn
jobRun3.upstream_urns = [jobRun1.urn]
jobRun3.emit_process_start(
    emitter=emitter, start_timestamp_millis=int(time.time() * 1000), emit_template=False
)
jobRun3.emit_process_end(
    emitter=emitter,
    end_timestamp_millis=int(time.time() * 1000),
    result=InstanceRunResult.SUCCESS,
)

jobRun4: DataProcessInstance = DataProcessInstance(
    orchestrator="airflow",
    cluster="prod",
    id=f"{jobFlow.id}-{dataJob4.id}-{uuid.uuid4()}",
)
jobRun4.parent_instance = jobFlowRun.urn
jobRun4.template_urn = dataJob4.urn
jobRun4.upstream_urns = [jobRun2.urn, jobRun3.urn]
jobRun4.emit_process_start(
    emitter=emitter, start_timestamp_millis=int(time.time() * 1000), emit_template=False
)
jobRun4.emit_process_end(
    emitter=emitter,
    end_timestamp_millis=int(time.time() * 1000),
    result=InstanceRunResult.SUCCESS,
)

user1 = CorpUser(
    id="testUser",
    display_name="Test User",
    email="test-user@test.com",
    groups=["testGroup"],
)
user1.emit(emitter)

group1 = CorpGroup(
    id="testGroup",
    display_name="Test Group",
    email="test-group@test.com",
    slack="#test-group",
    overrideEditable=True,
)
group1.emit(emitter)
