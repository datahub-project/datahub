from datahub.api.entities.datajob import DataFlow, DataJob
from datahub.emitter.rest_emitter import DatahubRestEmitter

# Create an emitter to the GMS REST API.

emitter = DatahubRestEmitter("http://localhost:8080")

jobFlow = DataFlow(cluster="prod", orchestrator="airflow", id="flow_new_api")
jobFlow.emit(emitter)

dataJob = DataJob(flow_urn=jobFlow.urn, id="job1")
dataJob.emit(emitter)
